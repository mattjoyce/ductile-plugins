"""Tests for gmail_poller plugin."""
from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(__file__))

from run import cmd_poll


class TestPollStateSnapshot(unittest.TestCase):
    @mock.patch("run.now_iso", return_value="2026-04-24T00:00:00+00:00")
    @mock.patch("run.get_current_history_id", return_value="12345")
    def test_first_run_records_full_snapshot(self, _mock_history_id, _mock_now_iso):
        resp = cmd_poll({}, {})

        self.assertEqual(resp["status"], "ok")
        self.assertEqual(
            resp["state_updates"],
            {
                "last_history_id": "12345",
                "last_poll_at": "2026-04-24T00:00:00+00:00",
                "history_reset_count": 0,
            },
        )
        self.assertNotIn("events", resp)

    @mock.patch("run.now_iso", return_value="2026-04-24T00:05:00+00:00")
    @mock.patch("run.fetch_message_meta")
    @mock.patch("run.fetch_history")
    def test_message_poll_keeps_snapshot_shape(self, mock_fetch_history, mock_fetch_meta, _mock_now_iso):
        mock_fetch_history.return_value = ([{"id": "m-1", "threadId": "t-1"}], "23456")
        mock_fetch_meta.return_value = {
            "from": "sender@example.com",
            "subject": "Hello",
            "snippet": "Hi there",
            "label_ids": ["INBOX"],
            "received_at": "Wed, 1 Apr 2026 06:45:32 +1100",
        }

        resp = cmd_poll({}, {"last_history_id": "12345", "history_reset_count": 2})

        self.assertEqual(resp["status"], "ok")
        self.assertEqual(
            resp["state_updates"],
            {
                "last_history_id": "23456",
                "last_poll_at": "2026-04-24T00:05:00+00:00",
                "history_reset_count": 2,
            },
        )
        self.assertEqual(len(resp["events"]), 1)
        self.assertEqual(resp["events"][0]["type"], "gmail.new_message")


if __name__ == "__main__":
    unittest.main()
