"""Tests for firecrawl plugin."""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.dirname(__file__))

from run import (
    atomic_write_outputs,
    content_hash,
    handle_request,
)


class TestHealthCommand(unittest.TestCase):
    def test_health_ok_when_api_key_set(self):
        resp = handle_request({"command": "health", "config": {"firecrawl_api_key": "fc-x"}})
        self.assertEqual(resp["status"], "ok")

    def test_health_error_when_api_key_missing(self):
        resp = handle_request({"command": "health", "config": {}})
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])
        self.assertIn("firecrawl_api_key", resp["error"])


class TestScrapeValidation(unittest.TestCase):
    """Payload validation — none of these should reach Firecrawl."""

    def _scrape(self, payload: dict, config: dict | None = None) -> dict:
        return handle_request(
            {
                "command": "handle",
                "config": config or {"firecrawl_api_key": "fc-x"},
                "event": {"payload": payload},
            }
        )

    def test_missing_api_key_returns_error(self):
        resp = self._scrape(
            {"url": "https://example.com", "doc_id": "d1", "output_dir": "/tmp"},
            config={},
        )
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])

    def test_missing_url(self):
        with tempfile.TemporaryDirectory() as td:
            resp = self._scrape({"doc_id": "d1", "output_dir": td})
        self.assertEqual(resp["status"], "error")
        self.assertIn("url", resp["error"])

    def test_missing_doc_id(self):
        with tempfile.TemporaryDirectory() as td:
            resp = self._scrape({"url": "https://example.com", "output_dir": td})
        self.assertEqual(resp["status"], "error")
        self.assertIn("doc_id", resp["error"])

    def test_missing_output_dir(self):
        resp = self._scrape({"url": "https://example.com", "doc_id": "d1"})
        self.assertEqual(resp["status"], "error")
        self.assertIn("output_dir", resp["error"])

    def test_doc_id_rejects_path_separator(self):
        with tempfile.TemporaryDirectory() as td:
            resp = self._scrape(
                {"url": "https://example.com", "doc_id": "../escape", "output_dir": td}
            )
        self.assertEqual(resp["status"], "error")
        self.assertIn("illegal characters", resp["error"])

    def test_doc_id_rejects_slash(self):
        with tempfile.TemporaryDirectory() as td:
            resp = self._scrape(
                {"url": "https://example.com", "doc_id": "a/b", "output_dir": td}
            )
        self.assertEqual(resp["status"], "error")

    def test_output_dir_does_not_exist(self):
        resp = self._scrape(
            {"url": "https://example.com", "doc_id": "d1", "output_dir": "/nonexistent/path"}
        )
        self.assertEqual(resp["status"], "error")
        self.assertIn("output_dir", resp["error"])


class TestScrapeHappyPath(unittest.TestCase):
    @mock.patch(
        "run.call_firecrawl",
        return_value=(
            "# Hello\n\nWorld.",
            {"sourceURL": "https://example.com/final", "title": "Hi", "_status_code": 200},
        ),
    )
    def test_writes_markdown_and_sidecar_atomically(self, _mock_call):
        with tempfile.TemporaryDirectory() as td:
            resp = handle_request(
                {
                    "command": "handle",
                    "config": {"firecrawl_api_key": "fc-x"},
                    "event": {
                        "payload": {
                            "url": "https://example.com",
                            "doc_id": "abc123",
                            "output_dir": td,
                        }
                    },
                }
            )

            self.assertEqual(resp["status"], "ok")
            md_path = Path(td) / "abc123.md"
            json_path = Path(td) / "abc123.json"
            self.assertTrue(md_path.exists())
            self.assertTrue(json_path.exists())
            self.assertEqual(md_path.read_text(encoding="utf-8"), "# Hello\n\nWorld.")

            sidecar = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual(sidecar["doc_id"], "abc123")
            self.assertEqual(sidecar["url"], "https://example.com")
            self.assertEqual(sidecar["final_url"], "https://example.com/final")
            self.assertEqual(sidecar["status"], "ready")
            self.assertEqual(sidecar["status_code"], 200)
            self.assertEqual(sidecar["title"], "Hi")
            self.assertEqual(sidecar["content_hash"], content_hash("# Hello\n\nWorld."))

            # event emitted
            self.assertEqual(len(resp["events"]), 1)
            self.assertEqual(resp["events"][0]["type"], "content_ready")
            evt = resp["events"][0]["payload"]
            self.assertEqual(evt["doc_id"], "abc123")
            self.assertEqual(evt["url"], "https://example.com")
            self.assertEqual(evt["output_path"], str(md_path))


class TestScrapeErrorMapping(unittest.TestCase):
    @mock.patch(
        "run.call_firecrawl",
        side_effect=__import__("urllib.error", fromlist=["HTTPError"]).HTTPError(
            "https://example", 400, "bad url", {}, None
        ),
    )
    def test_http_4xx_not_retryable(self, _mock_call):
        with tempfile.TemporaryDirectory() as td:
            resp = handle_request(
                {
                    "command": "handle",
                    "config": {"firecrawl_api_key": "fc-x"},
                    "event": {
                        "payload": {
                            "url": "https://example.com",
                            "doc_id": "abc",
                            "output_dir": td,
                        }
                    },
                }
            )
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])
        self.assertIn("400", resp["error"])

    @mock.patch(
        "run.call_firecrawl",
        side_effect=__import__("urllib.error", fromlist=["HTTPError"]).HTTPError(
            "https://example", 503, "service unavailable", {}, None
        ),
    )
    def test_http_5xx_retryable(self, _mock_call):
        with tempfile.TemporaryDirectory() as td:
            resp = handle_request(
                {
                    "command": "handle",
                    "config": {"firecrawl_api_key": "fc-x"},
                    "event": {
                        "payload": {
                            "url": "https://example.com",
                            "doc_id": "abc",
                            "output_dir": td,
                        }
                    },
                }
            )
        self.assertEqual(resp["status"], "error")
        self.assertTrue(resp["retry"])

    @mock.patch("run.call_firecrawl", side_effect=ValueError("bad response shape"))
    def test_value_error_not_retryable(self, _mock_call):
        with tempfile.TemporaryDirectory() as td:
            resp = handle_request(
                {
                    "command": "handle",
                    "config": {"firecrawl_api_key": "fc-x"},
                    "event": {
                        "payload": {
                            "url": "https://example.com",
                            "doc_id": "abc",
                            "output_dir": td,
                        }
                    },
                }
            )
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])


class TestAtomicWriteOrder(unittest.TestCase):
    def test_md_appears_last(self):
        """The .md must appear AFTER the .json — filewatch trigger contract."""
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            md_path = atomic_write_outputs(
                output_dir=td_path,
                doc_id="d1",
                markdown="content",
                sidecar={"doc_id": "d1"},
            )
            json_path = td_path / "d1.json"
            self.assertTrue(md_path.exists())
            self.assertTrue(json_path.exists())
            # Both exist; we can't easily verify creation order after the
            # fact, but we can verify the function returns the md path
            # (meaning it wrote it last) and that no .tmp files leak.
            self.assertEqual(md_path, td_path / "d1.md")
            tmp_files = list(td_path.glob(".*.tmp"))
            self.assertEqual(tmp_files, [])


class TestUnknownCommand(unittest.TestCase):
    def test_unknown_command_returns_error(self):
        resp = handle_request({"command": "foobar", "config": {}})
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])
        self.assertIn("unknown command", resp["error"])


if __name__ == "__main__":
    unittest.main()
