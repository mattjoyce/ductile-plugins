"""Tests for youtube_playlist plugin."""
from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(__file__))

from run import handle_poll


class TestPollStateSnapshot(unittest.TestCase):
    @mock.patch("run.iso_now", return_value="2026-04-24T00:00:00+00:00")
    @mock.patch("run.fetch_playlist_via_ytdlp")
    def test_first_run_without_emit_existing_records_snapshot(self, mock_fetch_playlist, _mock_iso_now):
        mock_fetch_playlist.return_value = [
            {
                "video_id": "vid-1",
                "title": "Video One",
                "published": "2026-04-01T00:00:00+00:00",
                "video_url": "https://www.youtube.com/watch?v=vid-1",
            },
            {
                "video_id": "vid-2",
                "title": "Video Two",
                "published": "2026-04-02T00:00:00+00:00",
                "video_url": "https://www.youtube.com/watch?v=vid-2",
            },
        ]

        resp = handle_poll(
            {"playlist_id": "PL123", "emit_existing_on_first_run": False},
            {},
        )

        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["state_updates"]["last_checked"], "2026-04-24T00:00:00+00:00")
        self.assertCountEqual(resp["state_updates"]["seen_ids"], ["vid-1", "vid-2"])
        self.assertNotIn("events", resp)

    @mock.patch("run.iso_now", return_value="2026-04-24T00:05:00+00:00")
    @mock.patch("run.fetch_playlist_via_ytdlp")
    def test_emitting_new_items_keeps_snapshot_shape(self, mock_fetch_playlist, _mock_iso_now):
        mock_fetch_playlist.return_value = [
            {
                "video_id": "seen-1",
                "title": "Seen Video",
                "published": "2026-04-01T00:00:00+00:00",
                "video_url": "https://www.youtube.com/watch?v=seen-1",
            },
            {
                "video_id": "new-1",
                "title": "New Video",
                "published": "2026-04-02T00:00:00+00:00",
                "video_url": "https://www.youtube.com/watch?v=new-1",
            },
        ]

        resp = handle_poll({"playlist_id": "PL123"}, {"seen_ids": ["seen-1"]})

        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["state_updates"]["last_checked"], "2026-04-24T00:05:00+00:00")
        self.assertCountEqual(resp["state_updates"]["seen_ids"], ["seen-1", "new-1"])
        self.assertEqual(len(resp["events"]), 1)
        self.assertEqual(resp["events"][0]["type"], "youtube.playlist_item")


if __name__ == "__main__":
    unittest.main()
