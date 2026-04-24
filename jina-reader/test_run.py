"""Tests for jina-reader plugin."""
from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(__file__))

from run import handle_request


class TestPollStateSnapshot(unittest.TestCase):
    @mock.patch("run.fetch_via_jina", return_value=("hello world", False))
    def test_poll_changed_emits_event_and_snapshot(self, _mock_fetch):
        resp = handle_request(
            {
                "command": "poll",
                "config": {"url": "https://example.com"},
                "state": {"content_hash": "previous"},
            }
        )

        self.assertEqual(resp["status"], "ok")
        self.assertEqual(
            resp["state_updates"],
            {
                "content_hash": "b94d27b9934d3e08",
                "last_url": "https://example.com",
            },
        )
        self.assertEqual(len(resp["events"]), 1)
        self.assertEqual(resp["events"][0]["type"], "content_changed")

    @mock.patch("run.fetch_via_jina", return_value=("hello world", False))
    def test_poll_unchanged_keeps_snapshot_without_event(self, _mock_fetch):
        resp = handle_request(
            {
                "command": "poll",
                "config": {"url": "https://example.com"},
                "state": {"content_hash": "b94d27b9934d3e08"},
            }
        )

        self.assertEqual(resp["status"], "ok")
        self.assertEqual(
            resp["state_updates"],
            {
                "content_hash": "b94d27b9934d3e08",
                "last_url": "https://example.com",
            },
        )
        self.assertNotIn("events", resp)


if __name__ == "__main__":
    unittest.main()
