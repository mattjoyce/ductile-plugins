"""Tests for the fabric plugin, against a stand-in fabric binary that reports what it got."""
from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from run import handle_command, health_command, parse_json_output

# The stand-in prints FAKE_REPLY when set, else a JSON report of its argv, stdin and env.
FAKE = textwrap.dedent("""\
    #!{python}
    import json, os, sys
    if "--help" in sys.argv or "--listpatterns" in sys.argv:
        print("p1\\np2")
        sys.exit(0)
    data = sys.stdin.read()
    reply = os.environ.get("FAKE_REPLY")
    if reply is not None:
        sys.stdout.write(reply)
    else:
        print(json.dumps({{"argv": sys.argv[1:], "stdin": data,
                          "home": os.environ.get("HOME"), "vendor": os.environ.get("DEFAULT_VENDOR")}}))
    """)


class Base(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.bin = self.dir / "fabric"
        self.bin.write_text(FAKE.format(python=sys.executable))
        self.bin.chmod(self.bin.stat().st_mode | stat.S_IXUSR)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def run_handle(self, payload, **config):
        cfg = {"FABRIC_BIN_PATH": str(self.bin)}
        cfg.update(config)
        return handle_command(cfg, {}, {"payload": payload}, {})


class DefaultsUnchanged(Base):
    def test_text_mode_as_before(self) -> None:
        out = self.run_handle({"text": "hi", "pattern": "summarize"})
        self.assertEqual(out["status"], "ok", out)
        report = json.loads(out["events"][0]["payload"]["result"])
        self.assertEqual(report["argv"], ["--pattern", "summarize"])
        self.assertEqual(report["stdin"], "hi")
        self.assertNotIn("data", out["events"][0]["payload"])


class NewOptions(Base):
    def test_raw_flag(self) -> None:
        out = self.run_handle({"text": "hi", "pattern": "p"}, FABRIC_RAW=True)
        self.assertIn("--raw", json.loads(out["events"][0]["payload"]["result"])["argv"])

    def test_env_and_home(self) -> None:
        home = self.dir / "fhome"
        out = self.run_handle({"text": "hi"}, FABRIC_HOME=str(home), FABRIC_ENV={"DEFAULT_VENDOR": "OpenAI"})
        report = json.loads(out["events"][0]["payload"]["result"])
        self.assertEqual(report["home"], str(home))
        self.assertEqual(report["vendor"], "OpenAI")
        self.assertTrue((home / ".config" / "fabric" / ".env").is_file())

    def test_env_must_be_a_map(self) -> None:
        out = self.run_handle({"text": "hi"}, FABRIC_ENV="DEFAULT_VENDOR=OpenAI")
        self.assertEqual(out["status"], "error")
        self.assertFalse(out["retry"])

    def test_input_file(self) -> None:
        f = self.dir / "in.txt"
        f.write_text("from a file")
        out = self.run_handle({"input_file": str(f), "pattern": "p"})
        self.assertEqual(json.loads(out["events"][0]["payload"]["result"])["stdin"], "from a file")
        self.assertEqual(out["events"][0]["payload"]["input_file"], str(f))

    def test_input_file_too_big(self) -> None:
        f = self.dir / "in.txt"
        f.write_text("x" * 50)
        out = self.run_handle({"input_file": str(f)}, FABRIC_MAX_INPUT_BYTES=10)
        self.assertEqual(out["status"], "error")
        self.assertIn("limit", out["error"])

    def test_missing_input_file(self) -> None:
        out = self.run_handle({"input_file": str(self.dir / "nope.txt")})
        self.assertEqual(out["status"], "error")

    def test_json_output_fenced(self) -> None:
        os.environ["FAKE_REPLY"] = '```json\n{"total": "11.07", "currency": "USD"}\n```\n'
        try:
            out = self.run_handle({"text": "invoice", "output_format": "json"})
        finally:
            del os.environ["FAKE_REPLY"]
        self.assertEqual(out["events"][0]["payload"]["data"], {"total": "11.07", "currency": "USD"})

    def test_json_output_not_json_is_retried(self) -> None:
        os.environ["FAKE_REPLY"] = "Sure! Here is the data you asked for."
        try:
            out = self.run_handle({"text": "invoice"}, FABRIC_OUTPUT_FORMAT="json")
        finally:
            del os.environ["FAKE_REPLY"]
        self.assertEqual(out["status"], "error")
        self.assertTrue(out["retry"])

    def test_output_file_json(self) -> None:
        target = self.dir / "plan.json"
        os.environ["FAKE_REPLY"] = '{"a": 1}'
        try:
            out = self.run_handle({"text": "x", "output_format": "json", "output_file": str(target)})
        finally:
            del os.environ["FAKE_REPLY"]
        self.assertEqual(out["status"], "ok")
        self.assertEqual(json.loads(target.read_text()), {"a": 1})
        self.assertEqual([p.name for p in self.dir.iterdir() if p.name.startswith(".fabric-")], [])

    def test_bad_output_format(self) -> None:
        out = self.run_handle({"text": "x", "output_format": "yaml"})
        self.assertEqual(out["status"], "error")

    def test_folder_passes_through(self) -> None:
        out = self.run_handle({"text": "x", "folder": "abc123"})
        self.assertEqual(out["events"][0]["payload"]["folder"], "abc123")

    def test_timeout_is_configurable(self) -> None:
        slow = self.dir / "slow"
        slow.write_text(f"#!{sys.executable}\nimport time\ntime.sleep(5)\n")
        slow.chmod(0o755)
        out = handle_command({"FABRIC_BIN_PATH": str(slow), "FABRIC_TIMEOUT_SECONDS": 1}, {},
                             {"payload": {"text": "x"}}, {})
        self.assertEqual(out["status"], "error")
        self.assertIn("1s", out["error"])


class Health(Base):
    def test_health_with_home(self) -> None:
        out = health_command({"FABRIC_BIN_PATH": str(self.bin), "FABRIC_HOME": str(self.dir / "h")})
        self.assertEqual(out["status"], "ok")
        self.assertIn("2 patterns", out["result"])


class Fence(unittest.TestCase):
    def test_parse(self) -> None:
        self.assertEqual(parse_json_output('```\n[1]\n```'), [1])
        self.assertEqual(parse_json_output(' {"a": 2} '), {"a": 2})
        with self.assertRaises(ValueError):
            parse_json_output("```json\nnot json\n```")


class Protocol(Base):
    def test_stdin_stdout(self) -> None:
        req = {"command": "handle", "config": {"FABRIC_BIN_PATH": str(self.bin)},
               "event": {"payload": {"text": "hi"}}}
        proc = subprocess.run([sys.executable, str(Path(__file__).with_name("run.py"))],
                              input=json.dumps(req), capture_output=True, text=True, timeout=30)
        self.assertEqual(json.loads(proc.stdout)["status"], "ok", proc.stderr)


if __name__ == "__main__":
    unittest.main()
