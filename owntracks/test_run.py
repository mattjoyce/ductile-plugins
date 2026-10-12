"""Tests for the owntracks plugin, against a fake export endpoint."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

from run import (
    health_command,
    poll_command,
    safe_component,
    spool_cursor,
)

TOKEN = "test-pull-token"
JAN = 1767225600  # 2026-01-01T00:00:00Z
FEB = 1769904000  # 2026-02-01T00:00:00Z
LAT = "-33.8688197"


def row(i: int, received_at: int = JAN, device: str = "phone", kind: str = "location") -> Dict[str, Any]:
    body = json.dumps({"_type": kind, "tst": received_at - 2, "lat": float(LAT), "lon": 151.2, "acc": 12})
    return {"id": i, "user": "u", "device": device, "type": kind, "tst": received_at - 2,
            "received_at": received_at, "body": body}


class FakeExport:
    """Serves rows like the edge Worker: after/limit, bearer token, rows in id order."""

    def __init__(self) -> None:
        self.rows: List[Dict[str, Any]] = []
        self.status = 200
        self.requests = 0
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                outer.requests += 1
                if outer.status != 200:
                    self.send_response(outer.status)
                    self.end_headers()
                    return
                if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                    self.send_response(401)
                    self.end_headers()
                    return
                q = parse_qs(urlparse(self.path).query)
                after = int(q.get("after", ["0"])[0])
                limit = int(q.get("limit", ["500"])[0])
                rows = [r for r in outer.rows if r["id"] > after][:limit]
                body = json.dumps({"rows": rows, "next_after": rows[-1]["id"] if rows else after,
                                   "more": len(rows) == limit}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}/owntracks/export"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


class OwnTracksTest(unittest.TestCase):
    def setUp(self) -> None:
        self.export = FakeExport()
        self.tmp = tempfile.TemporaryDirectory()
        self.spool = Path(self.tmp.name) / "spool"

    def tearDown(self) -> None:
        self.export.close()
        self.tmp.cleanup()

    def request(self, command: str = "poll", **config: Any) -> Dict[str, Any]:
        cfg = {"export_url": self.export.url, "spool_dir": str(self.spool)}
        cfg.update(config)
        return {"protocol": 2, "command": command, "config": cfg, "secrets": {"owntracks_pull_token": TOKEN}}

    def spooled_ids(self) -> List[int]:
        ids = []
        for path in sorted(self.spool.glob("*/*.jsonl")):
            ids += [json.loads(line)["id"] for line in path.read_text().splitlines()]
        return sorted(ids)

    def test_pulls_appends_and_emits_without_positions(self):
        self.export.rows = [row(1), row(2), row(3, kind="status")]
        resp = poll_command(self.request())
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(self.spooled_ids(), [1, 2, 3])
        event = resp["events"][0]
        self.assertEqual(event["type"], "location.new_data")
        p = event["payload"]
        self.assertEqual((p["rows"], p["first_id"], p["last_id"]), (3, 1, 3))
        self.assertEqual(p["types"], {"location": 2, "status": 1})
        self.assertEqual(p["files"], ["phone/2026-01.jsonl"])
        self.assertNotIn(LAT, json.dumps(resp))
        self.assertNotIn('"lat"', json.dumps(resp))

    def test_body_is_kept_byte_exact(self):
        self.export.rows = [row(1)]
        poll_command(self.request())
        line = json.loads((self.spool / "phone" / "2026-01.jsonl").read_text())
        self.assertEqual(line["body"], self.export.rows[0]["body"])

    def test_second_poll_is_a_no_op(self):
        self.export.rows = [row(1)]
        poll_command(self.request())
        resp = poll_command(self.request())
        self.assertEqual(resp["events"], [])
        self.assertEqual(self.spooled_ids(), [1])

    def test_pages_until_drained(self):
        self.export.rows = [row(i) for i in range(1, 6)]
        resp = poll_command(self.request(page_limit=2))
        self.assertEqual(self.spooled_ids(), [1, 2, 3, 4, 5])
        self.assertFalse(resp["events"][0]["payload"]["backlog"])

    def test_page_budget_leaves_the_rest_for_the_next_poll(self):
        self.export.rows = [row(i) for i in range(1, 6)]
        resp = poll_command(self.request(page_limit=2, max_pages=1))
        self.assertEqual(self.spooled_ids(), [1, 2])
        self.assertTrue(resp["events"][0]["payload"]["backlog"])
        poll_command(self.request(page_limit=2, max_pages=1))
        self.assertEqual(self.spooled_ids(), [1, 2, 3, 4])

    def test_files_by_device_and_received_month(self):
        self.export.rows = [row(1, JAN), row(2, FEB), row(3, FEB, device="tablet")]
        poll_command(self.request())
        files = sorted(str(p.relative_to(self.spool)) for p in self.spool.glob("*/*.jsonl"))
        self.assertEqual(files, ["phone/2026-01.jsonl", "phone/2026-02.jsonl", "tablet/2026-02.jsonl"])
        self.assertEqual(spool_cursor(self.spool), 3)

    def test_cursor_comes_from_the_spool(self):
        self.export.rows = [row(1), row(2)]
        poll_command(self.request())
        for path in self.spool.glob("*/*.jsonl"):
            path.unlink()
        poll_command(self.request())
        self.assertEqual(self.spooled_ids(), [1, 2])

    def test_torn_tail_is_repaired_and_re_pulled(self):
        self.export.rows = [row(1), row(2)]
        poll_command(self.request())
        path = self.spool / "phone" / "2026-01.jsonl"
        lines = path.read_text().splitlines(keepends=True)
        path.write_text(lines[0] + lines[1][:20])  # crash mid-append of row 2
        self.assertEqual(spool_cursor(self.spool), 1)
        poll_command(self.request())
        self.assertEqual(self.spooled_ids(), [1, 2])

    def test_refused_token_is_permanent(self):
        req = self.request()
        req["secrets"]["owntracks_pull_token"] = "wrong"
        resp = poll_command(req)
        self.assertEqual((resp["status"], resp["retry"]), ("error", False))

    def test_server_error_is_retried(self):
        self.export.status = 503
        resp = poll_command(self.request())
        self.assertEqual((resp["status"], resp["retry"]), ("error", True))

    def test_unreachable_is_retried(self):
        resp = poll_command(self.request(export_url="http://127.0.0.1:9/owntracks/export",
                                         request_timeout_seconds=2))
        self.assertEqual((resp["status"], resp["retry"]), ("error", True))

    def test_missing_secret_is_permanent(self):
        req = self.request()
        req["secrets"] = {}
        resp = poll_command(req)
        self.assertEqual(resp["retry"], False)
        self.assertIn("owntracks_pull_token", resp["error"])

    def test_out_of_order_rows_are_refused(self):
        self.export.rows = [row(2), row(1)]
        resp = poll_command(self.request())
        self.assertEqual((resp["status"], resp["retry"]), ("error", False))
        self.assertEqual(self.spooled_ids(), [])

    def test_device_names_stay_inside_the_spool(self):
        self.assertEqual(safe_component("../../etc"), "etc")
        self.assertEqual(safe_component(".."), "unknown")
        self.export.rows = [row(1, device="../../etc")]
        poll_command(self.request())
        files = [p.resolve() for p in self.spool.rglob("*.jsonl")]
        self.assertTrue(all(self.spool.resolve() in p.parents for p in files))

    def test_health_reports_cursor_and_backlog(self):
        self.export.rows = [row(1), row(2)]
        poll_command(self.request(page_limit=1, max_pages=1))
        resp = health_command(self.request("health"))
        p = resp["events"][0]["payload"]
        self.assertTrue(p["healthy"])
        self.assertEqual(p["spool"]["cursor"], 1)
        self.assertTrue(p["buffer"]["rows_waiting"])
        self.assertNotIn(LAT, json.dumps(resp))

    def test_entrypoint_speaks_the_protocol(self):
        self.export.rows = [row(1)]
        out = subprocess.run([sys.executable, str(Path(__file__).with_name("run.py"))],
                             input=json.dumps(self.request()), capture_output=True, text=True, check=True)
        self.assertEqual(json.loads(out.stdout)["status"], "ok")


if __name__ == "__main__":
    unittest.main()
