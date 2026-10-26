"""Tests for pdf_text. PDFs are built here by hand; no real documents."""
from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from run import handle_command, health_command

HAVE_PDFTOTEXT = shutil.which("pdftotext") is not None


def make_pdf(lines):
    """A one-page PDF whose text layer is lines; with no lines, a page with no text."""
    ops = "".join(f"BT /F1 12 Tf 72 {700 - 20 * i} Td ({t}) Tj ET\n" for i, t in enumerate(lines))
    stream = ops.encode("latin-1")
    objs = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R "
        b"/Resources << /Font << /F1 5 0 R >> >> >>",
        b"<< /Length %d >>\nstream\n" % len(stream) + stream + b"endstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out = b"%PDF-1.4\n"
    offsets = []
    for n, body in enumerate(objs, start=1):
        offsets.append(len(out))
        out += b"%d 0 obj\n" % n + body + b"\nendobj\n"
    xref = len(out)
    out += b"xref\n0 %d\n0000000000 65535 f \n" % (len(objs) + 1)
    for off in offsets:
        out += b"%010d 00000 n \n" % off
    out += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (len(objs) + 1, xref)
    return out


class Base(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name).resolve()
        self.root = self.dir / "root"
        self.root.mkdir()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def pdf(self, name, lines=("Invoice total 42.00", "Thank you")):
        p = self.root / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(make_pdf(lines))
        return p

    def run_handle(self, payload, **config):
        cfg = {"root": str(self.root)}
        cfg.update(config)
        return handle_command(cfg, {}, {"payload": payload}, {})


@unittest.skipUnless(HAVE_PDFTOTEXT, "pdftotext not installed")
class Extract(Base):
    def test_writes_text_beside_pdf_and_emits(self) -> None:
        pdf = self.pdf("a/in.pdf")
        out = self.run_handle({"pdf": str(pdf), "payload_dir": "/x", "dedupe": 1})
        self.assertEqual(out["status"], "ok", out)
        txt = Path(str(pdf) + ".txt")
        self.assertIn("Invoice total 42.00", txt.read_text())
        ev = out["events"][0]
        self.assertEqual(ev["type"], "pdf_text.completed")
        self.assertEqual(ev["payload"]["input_file"], str(txt))
        self.assertEqual(ev["payload"]["pdf"], str(pdf))
        self.assertIs(ev["payload"]["empty"], False)
        self.assertEqual(ev["payload"]["payload_dir"], "/x")
        self.assertEqual(ev["payload"]["dedupe"], 1)
        self.assertEqual(out["state_updates"]["executions_count"], 1)

    def test_relative_path_and_input_file_alias(self) -> None:
        self.pdf("in.pdf")
        out = self.run_handle({"input_file": "in.pdf"})
        self.assertEqual(out["status"], "ok", out)
        self.assertTrue((self.root / "in.pdf.txt").exists())

    def test_output_file(self) -> None:
        pdf = self.pdf("in.pdf")
        out = self.run_handle({"pdf": str(pdf), "output_file": "o/out.txt"})
        # o/ does not exist: the write fails and says so, with retry
        self.assertEqual(out["status"], "error")
        (self.root / "o").mkdir()
        out = self.run_handle({"pdf": str(pdf), "output_file": "o/out.txt"})
        self.assertEqual(out["status"], "ok", out)
        self.assertEqual(out["events"][0]["payload"]["input_file"], str(self.root / "o" / "out.txt"))
        self.assertFalse((self.root / "in.pdf.txt").exists())

    def test_layout_off_still_extracts(self) -> None:
        pdf = self.pdf("in.pdf")
        out = self.run_handle({"pdf": str(pdf)}, layout=False)
        self.assertEqual(out["status"], "ok", out)
        self.assertIn("Thank you", Path(str(pdf) + ".txt").read_text())

    def test_no_text_layer_is_empty_true(self) -> None:
        pdf = self.pdf("scan.pdf", lines=())
        out = self.run_handle({"pdf": str(pdf)})
        self.assertEqual(out["status"], "ok", out)
        self.assertIs(out["events"][0]["payload"]["empty"], True)
        self.assertTrue(Path(str(pdf) + ".txt").exists())

    def test_not_a_pdf_names_exit_code(self) -> None:
        bad = self.root / "bad.pdf"
        bad.write_text("not a pdf")
        out = self.run_handle({"pdf": str(bad)})
        self.assertEqual(out["status"], "error")
        self.assertIn("exited 1", out["error"])
        self.assertFalse(out["retry"])
        self.assertFalse((self.root / "bad.pdf.txt").exists())

    def test_health_ok(self) -> None:
        self.assertEqual(health_command({"root": str(self.root)})["status"], "ok")

    def test_stdin_stdout(self) -> None:
        pdf = self.pdf("in.pdf")
        req = {"command": "handle", "config": {"root": str(self.root)}, "event": {"payload": {"pdf": str(pdf)}}}
        proc = subprocess.run([sys.executable, str(Path(__file__).with_name("run.py"))],
                              input=json.dumps(req), capture_output=True, text=True, timeout=30)
        self.assertEqual(json.loads(proc.stdout)["status"], "ok", proc.stderr)


class Refusals(Base):
    def test_no_root_refuses(self) -> None:
        pdf = self.pdf("in.pdf")
        out = handle_command({}, {}, {"payload": {"pdf": str(pdf)}}, {})
        self.assertEqual(out["status"], "error")
        self.assertIn("Refused", out["error"])

    def test_outside_root_refuses(self) -> None:
        other = self.dir / "other.pdf"
        other.write_bytes(make_pdf(["x"]))
        out = self.run_handle({"pdf": str(other)})
        self.assertEqual(out["status"], "error")
        self.assertIn("outside root", out["error"])
        self.assertFalse((self.dir / "other.pdf.txt").exists())

    def test_dotdot_and_symlink_out_refuse(self) -> None:
        other = self.dir / "other.pdf"
        other.write_bytes(make_pdf(["x"]))
        self.assertEqual(self.run_handle({"pdf": "../other.pdf"})["status"], "error")
        (self.root / "link.pdf").symlink_to(other)
        self.assertEqual(self.run_handle({"pdf": "link.pdf"})["status"], "error")

    def test_output_outside_root_refuses(self) -> None:
        pdf = self.pdf("in.pdf")
        out = self.run_handle({"pdf": str(pdf), "output_file": str(self.dir / "o.txt")})
        self.assertEqual(out["status"], "error")
        self.assertFalse((self.dir / "o.txt").exists())

    def test_not_regular_file_refuses(self) -> None:
        (self.root / "d.pdf").mkdir()
        self.assertEqual(self.run_handle({"pdf": "d.pdf"})["status"], "error")
        fifo = self.root / "f.pdf"
        os.mkfifo(fifo)
        out = self.run_handle({"pdf": "f.pdf"})
        self.assertEqual(out["status"], "error")
        self.assertIn("not a regular file", out["error"])

    def test_missing_input_and_missing_file(self) -> None:
        self.assertEqual(self.run_handle({})["status"], "error")
        self.assertEqual(self.run_handle({"pdf": "nope.pdf"})["status"], "error")

    def test_bad_timeout(self) -> None:
        self.pdf("in.pdf")
        self.assertEqual(self.run_handle({"pdf": "in.pdf"}, timeout="soon")["status"], "error")
        self.assertEqual(self.run_handle({"pdf": "in.pdf"}, timeout=0)["status"], "error")


class StandIn(Base):
    """pdftotext absent, slow or failing, with PATH pointed at a stand-in."""

    def stand_in(self, body):
        d = self.dir / "bin"
        d.mkdir(exist_ok=True)
        exe = d / "pdftotext"
        exe.write_text(f"#!{sys.executable}\n{body}\n")
        exe.chmod(exe.stat().st_mode | stat.S_IXUSR)
        return mock.patch.dict(os.environ, {"PATH": str(d)})

    def test_missing_binary_retries(self) -> None:
        self.pdf("in.pdf")
        empty = self.dir / "empty"
        empty.mkdir()
        with mock.patch.dict(os.environ, {"PATH": str(empty)}):
            out = self.run_handle({"pdf": "in.pdf"})
            health = health_command({"root": str(self.root)})
        self.assertEqual(out["status"], "error")
        self.assertTrue(out["retry"])
        self.assertEqual(health["status"], "error")

    def test_timeout_retries(self) -> None:
        self.pdf("in.pdf")
        with self.stand_in("import time\ntime.sleep(30)"):
            out = self.run_handle({"pdf": "in.pdf"}, timeout=1)
        self.assertEqual(out["status"], "error")
        self.assertTrue(out["retry"])
        self.assertIn("timed out", out["error"])

    def test_exit_code_named_and_flags_passed(self) -> None:
        self.pdf("in.pdf")
        with self.stand_in("import sys\nsys.stderr.write('boom')\nsys.exit(3)"):
            out = self.run_handle({"pdf": "in.pdf"})
        self.assertEqual(out["status"], "error")
        self.assertIn("exited 3", out["error"])
        self.assertIn("boom", out["error"])
        argv_file = self.dir / "argv.json"
        body = f"import json,sys\njson.dump(sys.argv[1:], open({str(argv_file)!r}, 'w'))\nprint('hi')"
        with self.stand_in(body):
            self.assertEqual(self.run_handle({"pdf": "in.pdf"})["status"], "ok")
            on = json.loads(argv_file.read_text())
            self.run_handle({"pdf": "in.pdf"}, layout="false")
            off = json.loads(argv_file.read_text())
        self.assertEqual(on[:3], ["-layout", "-enc", "UTF-8"])
        self.assertNotIn("-layout", off)


if __name__ == "__main__":
    unittest.main()
