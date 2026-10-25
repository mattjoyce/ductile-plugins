"""Tests for the agentmail plugin, against a fake AgentMail inbox served over HTTP."""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, unquote, urlparse

from run import (
    TMP_PREFIX,
    health_command,
    poll_command,
    safe_name,
    stem,
    unique_name,
)

KEY = "test-agentmail-key"
INBOX = "me@agentmail.to"
PDF = b"%PDF-1.4\n" + b"x" * 200 + b"\n%%EOF\n"


def message(i: int, attachments: List[Dict[str, Any]] = ()) -> Dict[str, Any]:
    return {
        "message_id": f"<msg{i}@mail.example.com>",
        "inbox_id": INBOX,
        "timestamp": f"2026-10-2{i}T01:00:00Z",
        "from": "Matt <matt@example.com>",
        "subject": f"invoice {i}",
        "text": "save this pdf\u0001 as a purchase",
        "html": "<p>save this pdf</p>",
        "authentication_results": "spf=pass dkim=pass",
        "attachments": list(attachments),
    }


def attachment(aid: str, filename: str, body: bytes, size: Any = None) -> Dict[str, Any]:
    return {"attachment_id": aid, "filename": filename, "content_type": "application/pdf",
            "size": len(body) if size is None else size, "_body": body}


class FakeInbox:
    """Serves the list, message, attachment and signed-download calls AgentMail answers."""

    def __init__(self) -> None:
        self.messages: List[Dict[str, Any]] = []
        self.page_size_seen: List[int] = []
        self.download_auth: List[str] = []
        self.fail_download: set = set()
        self.status = 200
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # noqa: D401
                pass

            def send_json(self, obj: Any) -> None:
                body = json.dumps(obj).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):  # noqa: N802
                url = urlparse(self.path)
                parts = [unquote(p) for p in url.path.split("/") if p]
                if parts and parts[0] == "download":
                    outer.download_auth.append(self.headers.get("Authorization") or "")
                    aid = parts[1]
                    if aid in outer.fail_download:
                        self.send_response(500)
                        self.end_headers()
                        return
                    for m in outer.messages:
                        for a in m["attachments"]:
                            if a["attachment_id"] == aid:
                                self.send_response(200)
                                self.send_header("Content-Length", str(len(a["_body"])))
                                self.end_headers()
                                self.wfile.write(a["_body"])
                                return
                    self.send_response(404)
                    self.end_headers()
                    return
                if outer.status != 200:
                    self.send_response(outer.status)
                    self.end_headers()
                    return
                if self.headers.get("Authorization") != f"Bearer {KEY}":
                    self.send_response(401)
                    self.end_headers()
                    return
                # /v0/inboxes/<inbox>/messages[/<id>[/attachments/<aid>]]
                if parts[:2] != ["v0", "inboxes"] or parts[2] != INBOX or parts[3] != "messages":
                    self.send_response(404)
                    self.end_headers()
                    return
                public = [{k: v for k, v in m.items() if k not in ("text", "html")} for m in outer.messages]
                for m in public:
                    m["attachments"] = [{k: v for k, v in a.items() if k != "_body"} for a in m["attachments"]]
                if len(parts) == 4:
                    q = parse_qs(url.query)
                    limit = int(q.get("limit", ["50"])[0])
                    start = int(q.get("page_token", ["0"])[0])
                    outer.page_size_seen.append(limit)
                    page = public[start:start + limit]
                    nxt = str(start + limit) if start + limit < len(public) else None
                    self.send_json({"count": len(page), "messages": page, "next_page_token": nxt})
                    return
                mid = parts[4]
                full = next((m for m in outer.messages if m["message_id"] == mid), None)
                if full is None:
                    self.send_response(404)
                    self.end_headers()
                    return
                if len(parts) == 5:
                    out = dict(full)
                    out["attachments"] = [{k: v for k, v in a.items() if k != "_body"} for a in full["attachments"]]
                    self.send_json(out)
                    return
                aid = parts[6]
                host, port = self.server.server_address[:2]
                self.send_json({"attachment_id": aid, "download_url": f"http://{host}:{port}/download/{aid}?sig=x"})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


class Base(unittest.TestCase):
    def setUp(self) -> None:
        self.fake = FakeInbox()
        self.tmp = tempfile.TemporaryDirectory()
        self.spool = Path(self.tmp.name) / "spool"

    def tearDown(self) -> None:
        self.fake.close()
        self.tmp.cleanup()

    def request(self, command: str = "poll", **config: Any) -> Dict[str, Any]:
        cfg = {"inbox": INBOX, "spool_dir": str(self.spool), "base_url": self.fake.url}
        cfg.update(config)
        return {"command": command, "config": cfg, "secrets": {"agentmail_api_key": KEY}}

    def folder(self, i: int) -> Path:
        return self.spool / stem(f"<msg{i}@mail.example.com>")


class PollTest(Base):
    def test_saves_message_attachments_and_manifest(self) -> None:
        self.fake.messages = [message(1, [attachment("a1", "Invoice-1.pdf", PDF)])]
        out = poll_command(self.request())
        self.assertEqual(out["status"], "ok", out)
        folder = self.folder(1)
        saved = json.loads((folder / "message.json").read_text())
        self.assertEqual(saved["text"], "save this pdf\u0001 as a purchase")
        self.assertEqual(saved["authentication_results"], "spf=pass dkim=pass")
        self.assertEqual((folder / "attachments" / "Invoice-1.pdf").read_bytes(), PDF)
        manifest = json.loads((folder / "manifest.json").read_text())
        self.assertEqual(manifest["attachments"][0]["sha256"], hashlib.sha256(PDF).hexdigest())
        self.assertEqual(manifest["message_id"], "<msg1@mail.example.com>")

    def test_event_carries_ids_not_content(self) -> None:
        self.fake.messages = [message(1, [attachment("a1", "x.pdf", PDF)])]
        out = poll_command(self.request())
        self.assertEqual(len(out["events"]), 1)
        event = out["events"][0]
        self.assertEqual(event["type"], "agentmail.message.saved")
        self.assertEqual(event["payload"]["folder"], self.folder(1).name)
        self.assertEqual(event["payload"]["attachments"], 1)
        text = json.dumps(out)
        self.assertNotIn("invoice 1", text)
        self.assertNotIn("save this pdf", text)
        self.assertNotIn("matt@example.com", text)

    def test_second_poll_is_a_no_op(self) -> None:
        self.fake.messages = [message(1, [attachment("a1", "x.pdf", PDF)])]
        poll_command(self.request())
        before = len(self.fake.download_auth)
        out = poll_command(self.request())
        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["events"], [])
        self.assertEqual(len(self.fake.download_auth), before)

    def test_message_without_attachments_is_saved(self) -> None:
        self.fake.messages = [message(1)]
        out = poll_command(self.request())
        self.assertEqual(len(out["events"]), 1)
        self.assertTrue((self.folder(1) / "message.json").is_file())
        self.assertEqual(list((self.folder(1) / "attachments").iterdir()), [])

    def test_key_never_goes_to_the_signed_download(self) -> None:
        self.fake.messages = [message(1, [attachment("a1", "x.pdf", PDF)])]
        poll_command(self.request())
        self.assertEqual(self.fake.download_auth, [""])

    def test_wrong_size_leaves_no_folder_and_others_still_save(self) -> None:
        self.fake.messages = [message(1, [attachment("a1", "x.pdf", PDF, size=5)]),
                              message(2, [attachment("a2", "y.pdf", PDF)])]
        out = poll_command(self.request())
        self.assertEqual(out["status"], "ok")
        self.assertFalse(self.folder(1).exists())
        self.assertTrue(self.folder(2).exists())
        self.assertEqual([p for p in self.spool.iterdir() if p.name.startswith(TMP_PREFIX)], [])

    def test_not_a_pdf_is_refused(self) -> None:
        self.fake.messages = [message(1, [attachment("a1", "x.pdf", b"<html>login</html>")])]
        out = poll_command(self.request())
        self.assertEqual(out["status"], "error")
        self.assertTrue(out["retry"])
        self.assertFalse(self.folder(1).exists())

    def test_failed_download_is_retried_next_poll(self) -> None:
        self.fake.messages = [message(1, [attachment("a1", "x.pdf", PDF)])]
        self.fake.fail_download = {"a1"}
        self.assertEqual(poll_command(self.request())["status"], "error")
        self.fake.fail_download = set()
        out = poll_command(self.request())
        self.assertEqual(len(out["events"]), 1)

    def test_half_built_folder_is_cleared(self) -> None:
        self.spool.mkdir(parents=True)
        (self.spool / f"{TMP_PREFIX}dead-1").mkdir()
        self.fake.messages = []
        out = poll_command(self.request())
        self.assertFalse((self.spool / f"{TMP_PREFIX}dead-1").exists())
        self.assertIn("half-built", json.dumps(out["logs"]))

    def test_pages_and_max_messages(self) -> None:
        self.fake.messages = [message(i) for i in range(1, 6)]
        out = poll_command(self.request(page_size=2, max_messages=3))
        self.assertEqual(len(out["events"]), 3)
        self.assertIn("waiting 2", out["result"])
        self.assertEqual(set(self.fake.page_size_seen), {2})
        out = poll_command(self.request(page_size=2, max_messages=3))
        self.assertEqual(len(out["events"]), 2)

    def test_two_attachments_with_one_name_stay_two_files(self) -> None:
        other = PDF + b"2"
        self.fake.messages = [message(1, [attachment("a1", "inv.pdf", PDF), attachment("a2", "inv.pdf", other)])]
        poll_command(self.request())
        files = sorted(p.name for p in (self.folder(1) / "attachments").iterdir())
        self.assertEqual(files, ["inv-2.pdf", "inv.pdf"])
        self.assertEqual((self.folder(1) / "attachments" / "inv-2.pdf").read_bytes(), other)

    def test_hostile_filename_stays_in_its_folder(self) -> None:
        self.fake.messages = [message(1, [attachment("a1", "../../etc/passwd.pdf", PDF)])]
        poll_command(self.request())
        self.assertTrue((self.folder(1) / "attachments" / "passwd.pdf").is_file())

    def test_refused_key_is_not_retried(self) -> None:
        req = self.request()
        req["secrets"] = {"agentmail_api_key": "wrong"}
        out = poll_command(req)
        self.assertEqual(out["status"], "error")
        self.assertFalse(out["retry"])

    def test_missing_secret_names_it(self) -> None:
        req = self.request()
        req["secrets"] = {}
        out = poll_command(req)
        self.assertIn("agentmail_api_key", out["error"])

    def test_bad_config_numbers(self) -> None:
        out = poll_command(self.request(page_size=500))
        self.assertEqual(out["status"], "error")
        self.assertFalse(out["retry"])


class HealthTest(Base):
    def test_healthy(self) -> None:
        self.spool.mkdir(parents=True)
        out = health_command(self.request("health"))
        payload = out["events"][0]["payload"]
        self.assertTrue(payload["healthy"])
        self.assertEqual(payload["spool"]["folders"], 0)

    def test_inbox_down(self) -> None:
        self.spool.mkdir(parents=True)
        self.fake.status = 503
        payload = health_command(self.request("health"))["events"][0]["payload"]
        self.assertFalse(payload["healthy"])
        self.assertIn("503", payload["inbox"]["problem"])


class NamesTest(unittest.TestCase):
    def test_safe_name(self) -> None:
        self.assertEqual(safe_name("a/b\\c.pdf"), "c.pdf")
        self.assertEqual(safe_name("\x00.hidden"), "hidden")
        self.assertEqual(safe_name(""), "attachment")
        long = safe_name("x" * 300 + ".pdf")
        self.assertTrue(long.endswith(".pdf"))
        self.assertLessEqual(len(long.encode()), 200)

    def test_unique_name(self) -> None:
        self.assertEqual(unique_name("a.pdf", {"a.pdf", "a-2.pdf"}), "a-3.pdf")


class ProtocolTest(Base):
    def test_stdin_stdout(self) -> None:
        self.fake.messages = [message(1)]
        run = Path(__file__).with_name("run.py")
        proc = subprocess.run([sys.executable, str(run)], input=json.dumps(self.request()),
                              capture_output=True, text=True, timeout=30)
        out = json.loads(proc.stdout)
        self.assertEqual(out["status"], "ok", proc.stderr)
        self.assertEqual(len(out["events"]), 1)

    def test_unknown_command(self) -> None:
        run = Path(__file__).with_name("run.py")
        proc = subprocess.run([sys.executable, str(run)], input=json.dumps({"command": "nope"}),
                              capture_output=True, text=True, timeout=30)
        self.assertEqual(json.loads(proc.stdout)["status"], "error")


if __name__ == "__main__":
    unittest.main()
