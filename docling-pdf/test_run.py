"""Tests for the docling-pdf plugin — pure-stdlib unit tests, no real HTTP."""

from __future__ import annotations

import unittest
import urllib.error
from unittest import mock

from run import handle_request


def _req(payload, config=None):
    return {
        "command": "handle",
        "payload": payload,
        "config": config or {"satellite_url": "http://docling.local:8889"},
    }


def _convert_ok(output_path: str, page_count: int = 8) -> tuple[int, dict, str]:
    parsed = {
        "doc_id": "42",
        "input_path": "/library/originals/42/source.pdf",
        "output_path": output_path,
        "sidecar_path": output_path.replace(".md", ".json"),
        "page_count": page_count,
        "parse_duration_seconds": 51.2,
        "docling_version": "2.94.0",
        "started_at": "2026-05-24T10:00:00Z",
        "completed_at": "2026-05-24T10:00:51Z",
        "status": "ready",
    }
    import json as _j
    return 200, parsed, _j.dumps(parsed)


class TestHandle(unittest.TestCase):
    @mock.patch(
        "run.DoclingClient.post_convert",
        return_value=_convert_ok("/library/inbound/converted/42.md"),
    )
    def test_derives_output_path_from_doc_id(self, mock_post):
        resp = handle_request(
            _req({"source_path": "/library/originals/42/source.pdf", "doc_id": "42"})
        )
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["result"], "ready")
        self.assertEqual(
            resp["state_updates"]["output_path"], "/library/inbound/converted/42.md"
        )
        args, _ = mock_post.call_args
        self.assertEqual(args[1], "/library/originals/42/source.pdf")
        self.assertEqual(args[2], "/library/inbound/converted/42.md")

    @mock.patch(
        "run.DoclingClient.post_convert",
        return_value=_convert_ok("/custom/out/foo.md"),
    )
    def test_explicit_output_path_wins(self, _mock_post):
        resp = handle_request(
            _req(
                {
                    "source_path": "/library/originals/42/source.pdf",
                    "doc_id": "42",
                    "output_path": "/custom/out/foo.md",
                }
            )
        )
        self.assertEqual(resp["state_updates"]["output_path"], "/custom/out/foo.md")

    def test_missing_source_path_errors(self):
        resp = handle_request(_req({"doc_id": "42"}))
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])

    def test_relative_path_rejected(self):
        resp = handle_request(_req({"source_path": "relative/foo.pdf", "doc_id": "42"}))
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])

    def test_traversal_rejected(self):
        resp = handle_request(
            _req({"source_path": "/library/../etc/passwd.pdf", "doc_id": "42"})
        )
        self.assertEqual(resp["status"], "error")

    def test_non_md_output_rejected(self):
        resp = handle_request(
            _req(
                {
                    "source_path": "/library/originals/42/source.pdf",
                    "output_path": "/library/inbound/converted/42.txt",
                }
            )
        )
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])

    def test_missing_satellite_url_errors(self):
        resp = handle_request(
            {
                "command": "handle",
                "payload": {"source_path": "/library/originals/42/source.pdf"},
                "config": {},
            }
        )
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])

    @mock.patch(
        "run.DoclingClient.post_convert",
        side_effect=urllib.error.URLError("connection refused"),
    )
    def test_satellite_unreachable_retries(self, _mock_post):
        resp = handle_request(
            _req({"source_path": "/library/originals/42/source.pdf", "doc_id": "42"})
        )
        self.assertEqual(resp["status"], "error")
        self.assertTrue(resp["retry"])

    @mock.patch(
        "run.DoclingClient.post_convert",
        return_value=(400, {"detail": "input_path not found"}, '{"detail":"input_path not found"}'),
    )
    def test_400_no_retry(self, _mock_post):
        resp = handle_request(
            _req({"source_path": "/library/originals/42/source.pdf", "doc_id": "42"})
        )
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])

    @mock.patch(
        "run.DoclingClient.post_convert",
        return_value=(503, {"detail": "docling failure"}, '{"detail":"docling failure"}'),
    )
    def test_503_retries(self, _mock_post):
        resp = handle_request(
            _req({"source_path": "/library/originals/42/source.pdf", "doc_id": "42"})
        )
        self.assertEqual(resp["status"], "error")
        self.assertTrue(resp["retry"])


class TestHealth(unittest.TestCase):
    @mock.patch("run.DoclingClient.get_healthz", return_value=(200, '{"status":"ok"}'))
    def test_health_ok(self, _mock):
        resp = handle_request(
            {"command": "health", "config": {"satellite_url": "http://docling.local:8889"}}
        )
        self.assertEqual(resp["result"], "healthy")

    @mock.patch("run.DoclingClient.get_healthz", side_effect=urllib.error.URLError("nope"))
    def test_health_unreachable_degraded(self, _mock):
        resp = handle_request(
            {"command": "health", "config": {"satellite_url": "http://docling.local:8889"}}
        )
        self.assertEqual(resp["result"], "degraded")

    def test_health_no_url_degraded(self):
        resp = handle_request({"command": "health", "config": {}})
        self.assertEqual(resp["result"], "degraded")


class TestDispatch(unittest.TestCase):
    def test_unknown_command_errors(self):
        resp = handle_request({"command": "bogus", "config": {"satellite_url": "x"}})
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])

    @mock.patch(
        "run.DoclingClient.post_convert",
        return_value=_convert_ok("/library/inbound/converted/42.md"),
    )
    def test_payload_from_event_payload(self, _mock):
        resp = handle_request(
            {
                "command": "handle",
                "event": {
                    "type": "parsem.needs_docling",
                    "payload": {
                        "source_path": "/library/originals/42/source.pdf",
                        "doc_id": "42",
                    },
                },
                "config": {"satellite_url": "http://docling.local:8889"},
            }
        )
        self.assertEqual(resp["status"], "ok")


if __name__ == "__main__":
    unittest.main()
