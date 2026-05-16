"""Tests for the docling-pdf plugin.

docling and the LLM SDKs are heavy and not installed in CI, so the two
seam functions (run.docling_convert, run.llm_polish) are mocked. One
opt-in test exercises real docling against a fixture PDF and skips
cleanly when docling is not importable.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.dirname(__file__))

from run import (  # noqa: E402
    PolishError,
    atomic_write_outputs,
    handle_request,
)

_FAKE_RAW = "# Title\n\n| a | b |\n|-|-|\n| 1 | 2 |\n"
_FAKE_POLISHED = "# Title\n\n| a | b |\n| --- | --- |\n| 1 | 2 |\n"
_DOCLING_RV = (_FAKE_RAW, "2.0.0", 3)


def _handle(payload: dict, config: dict | None = None) -> dict:
    return handle_request(
        {
            "command": "handle",
            "config": config if config is not None else {"gemini_api_key": "g-x"},
            "event": {"payload": payload},
        }
    )


class TestHealthCommand(unittest.TestCase):
    def test_health_ok_when_gemini_key_set(self):
        with mock.patch.dict(sys.modules, {"docling": mock.MagicMock()}):
            resp = handle_request(
                {"command": "health", "config": {"gemini_api_key": "g-x"}}
            )
        self.assertEqual(resp["status"], "ok")

    def test_health_error_when_gemini_key_missing(self):
        with mock.patch.dict(sys.modules, {"docling": mock.MagicMock()}):
            resp = handle_request({"command": "health", "config": {}})
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])
        self.assertIn("API key", resp["error"])

    def test_health_none_provider_needs_no_key(self):
        with mock.patch.dict(sys.modules, {"docling": mock.MagicMock()}):
            resp = handle_request(
                {"command": "health", "config": {"llm_provider": "none"}}
            )
        self.assertEqual(resp["status"], "ok")

    def test_health_error_when_docling_missing(self):
        # No docling in sys.modules and not installed -> import fails.
        if importlib.util.find_spec("docling") is not None:
            self.skipTest("docling is installed; cannot test the missing path")
        resp = handle_request(
            {"command": "health", "config": {"gemini_api_key": "g-x"}}
        )
        self.assertEqual(resp["status"], "error")
        self.assertIn("docling", resp["error"])


class TestValidation(unittest.TestCase):
    def test_missing_source(self):
        with tempfile.TemporaryDirectory() as td:
            resp = _handle({"doc_id": "d1", "output_dir": td})
        self.assertEqual(resp["status"], "error")
        self.assertIn("source", resp["error"])
        self.assertFalse(resp["retry"])

    def test_missing_doc_id(self):
        with tempfile.TemporaryDirectory() as td:
            (Path(td) / "in.pdf").write_text("x")
            resp = _handle({"source": str(Path(td) / "in.pdf"), "output_dir": td})
        self.assertEqual(resp["status"], "error")
        self.assertIn("doc_id", resp["error"])

    def test_missing_output_dir(self):
        with tempfile.TemporaryDirectory() as td:
            (Path(td) / "in.pdf").write_text("x")
            resp = _handle({"source": str(Path(td) / "in.pdf"), "doc_id": "d1"})
        self.assertEqual(resp["status"], "error")
        self.assertIn("output_dir", resp["error"])

    def test_doc_id_rejects_traversal(self):
        with tempfile.TemporaryDirectory() as td:
            (Path(td) / "in.pdf").write_text("x")
            resp = _handle(
                {"source": str(Path(td) / "in.pdf"), "doc_id": "../escape", "output_dir": td}
            )
        self.assertEqual(resp["status"], "error")
        self.assertIn("illegal characters", resp["error"])

    def test_doc_id_rejects_slash(self):
        with tempfile.TemporaryDirectory() as td:
            (Path(td) / "in.pdf").write_text("x")
            resp = _handle(
                {"source": str(Path(td) / "in.pdf"), "doc_id": "a/b", "output_dir": td}
            )
        self.assertEqual(resp["status"], "error")

    def test_source_not_found(self):
        with tempfile.TemporaryDirectory() as td:
            resp = _handle(
                {"source": "/no/such.pdf", "doc_id": "d1", "output_dir": td}
            )
        self.assertEqual(resp["status"], "error")
        self.assertIn("source", resp["error"])
        self.assertFalse(resp["retry"])

    def test_output_dir_not_a_dir(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "in.pdf"
            src.write_text("x")
            resp = _handle(
                {"source": str(src), "doc_id": "d1", "output_dir": "/no/such/dir"}
            )
        self.assertEqual(resp["status"], "error")
        self.assertIn("output_dir", resp["error"])

    def test_missing_api_key_for_gemini(self):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "in.pdf"
            src.write_text("x")
            resp = _handle(
                {"source": str(src), "doc_id": "d1", "output_dir": td}, config={}
            )
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])
        self.assertIn("API key", resp["error"])


class TestHappyPath(unittest.TestCase):
    @mock.patch("run.llm_polish", return_value=_FAKE_POLISHED)
    @mock.patch("run.docling_convert", return_value=_DOCLING_RV)
    def test_writes_md_and_sidecar(self, _mock_dc, _mock_lp):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "in.pdf"
            src.write_text("%PDF-1.4 fake")
            resp = _handle(
                {"source": str(src), "doc_id": "abc123", "output_dir": td}
            )

            self.assertEqual(resp["status"], "ok")
            md_path = Path(td) / "abc123.md"
            json_path = Path(td) / "abc123.json"
            self.assertTrue(md_path.exists())
            self.assertTrue(json_path.exists())
            self.assertEqual(md_path.read_text(encoding="utf-8"), _FAKE_POLISHED)

            sc = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual(sc["doc_id"], "abc123")
            self.assertEqual(sc["docling_version"], "2.0.0")
            self.assertEqual(sc["llm_provider"], "gemini")
            self.assertEqual(sc["llm_model"], "gemini-2.5-pro")
            self.assertEqual(sc["polish_prompt_version"], "v1")
            self.assertEqual(sc["page_count"], 3)
            self.assertIsInstance(sc["parse_duration_seconds"], float)
            self.assertIsInstance(sc["polish_duration_seconds"], float)
            self.assertIsNone(sc["polish_skipped_reason"])
            self.assertEqual(sc["status"], "ready")

            self.assertEqual(len(resp["events"]), 1)
            evt = resp["events"][0]
            self.assertEqual(evt["type"], "content_ready")
            self.assertEqual(evt["payload"]["doc_id"], "abc123")
            self.assertEqual(evt["payload"]["output_path"], str(md_path))
            self.assertEqual(evt["payload"]["page_count"], 3)

    @mock.patch("run.llm_polish")
    @mock.patch("run.docling_convert", return_value=_DOCLING_RV)
    def test_provider_none_skips_polish(self, _mock_dc, mock_lp):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "in.pdf"
            src.write_text("%PDF-1.4 fake")
            resp = _handle(
                {"source": str(src), "doc_id": "d1", "output_dir": td},
                config={"llm_provider": "none"},
            )
            self.assertEqual(resp["status"], "ok")
            mock_lp.assert_not_called()
            sc = json.loads((Path(td) / "d1.json").read_text(encoding="utf-8"))
            self.assertEqual(sc["polish_skipped_reason"], "llm_provider=none")
            self.assertIsNone(sc["llm_model"])
            # parse-only -> the docling raw markdown is what lands
            self.assertEqual((Path(td) / "d1.md").read_text(encoding="utf-8"), _FAKE_RAW)

    @mock.patch("run.llm_polish", return_value=_FAKE_POLISHED)
    @mock.patch("run.docling_convert", return_value=_DOCLING_RV)
    def test_claude_provider_recorded(self, _mock_dc, _mock_lp):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "in.pdf"
            src.write_text("%PDF-1.4 fake")
            resp = _handle(
                {"source": str(src), "doc_id": "d1", "output_dir": td},
                config={"llm_provider": "claude", "anthropic_api_key": "sk-x"},
            )
            self.assertEqual(resp["status"], "ok")
            sc = json.loads((Path(td) / "d1.json").read_text(encoding="utf-8"))
            self.assertEqual(sc["llm_provider"], "claude")
            self.assertEqual(sc["llm_model"], "claude-sonnet-4-6")


class TestErrorMapping(unittest.TestCase):
    @mock.patch(
        "run.docling_convert",
        side_effect=RuntimeError("docling conversion failed: corrupt"),
    )
    def test_docling_failure_not_retryable(self, _mock_dc):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "in.pdf"
            src.write_text("%PDF-1.4 fake")
            resp = _handle(
                {"source": str(src), "doc_id": "d1", "output_dir": td}
            )
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])
        self.assertIn("docling", resp["error"])

    @mock.patch(
        "run.llm_polish",
        side_effect=PolishError("gemini call failed: 503", retry=True),
    )
    @mock.patch("run.docling_convert", return_value=_DOCLING_RV)
    def test_polish_transient_retryable(self, _mock_dc, _mock_lp):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "in.pdf"
            src.write_text("%PDF-1.4 fake")
            resp = _handle(
                {"source": str(src), "doc_id": "d1", "output_dir": td}
            )
        self.assertEqual(resp["status"], "error")
        self.assertTrue(resp["retry"])

    @mock.patch(
        "run.llm_polish",
        side_effect=PolishError("gemini call failed: 401", retry=False),
    )
    @mock.patch("run.docling_convert", return_value=_DOCLING_RV)
    def test_polish_auth_not_retryable(self, _mock_dc, _mock_lp):
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "in.pdf"
            src.write_text("%PDF-1.4 fake")
            resp = _handle(
                {"source": str(src), "doc_id": "d1", "output_dir": td}
            )
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])


class TestAtomicWriteOrder(unittest.TestCase):
    def test_md_path_returned_and_no_tmp_leak(self):
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            md_path = atomic_write_outputs(
                output_dir=td_path,
                doc_id="d1",
                markdown="content",
                sidecar={"doc_id": "d1"},
            )
            self.assertEqual(md_path, td_path / "d1.md")
            self.assertTrue((td_path / "d1.json").exists())
            self.assertEqual(list(td_path.glob(".*.tmp")), [])


class TestUnknownCommand(unittest.TestCase):
    def test_unknown_command(self):
        resp = handle_request({"command": "frobnicate", "config": {}})
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])
        self.assertIn("unknown command", resp["error"])


class TestRealDoclingIntegration(unittest.TestCase):
    """Opt-in: real docling, mocked LLM, against a fixture PDF.

    Skips cleanly when docling is not installed (the common dev/CI case).
    """

    def test_real_docling_mocked_polish(self):
        if importlib.util.find_spec("docling") is None:
            self.skipTest("docling not installed — integration test skipped")
        fixture = Path(__file__).with_name("tests") / "fixtures" / "sample.pdf"
        if not fixture.is_file():
            self.skipTest(f"fixture PDF missing: {fixture}")
        with mock.patch("run.llm_polish", return_value="# polished\n"):
            with tempfile.TemporaryDirectory() as td:
                resp = _handle(
                    {"source": str(fixture), "doc_id": "fix1", "output_dir": td}
                )
                self.assertEqual(resp["status"], "ok", resp.get("error"))
                sc = json.loads((Path(td) / "fix1.json").read_text(encoding="utf-8"))
                self.assertGreaterEqual(sc["page_count"], 1)
                self.assertNotEqual(sc["docling_version"], "unknown")


if __name__ == "__main__":
    unittest.main()
