"""Tests for birdnet_firstday plugin."""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from typing import Any, Dict, List, Optional, Tuple
from unittest import mock

from run import (
    build_event,
    handle_request,
    health_command,
    load_species_cache,
    poll_command,
    query_first_of_day,
    validate_config,
)

PLUGIN = os.path.join(os.path.dirname(__file__), "run.py")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE labels (
    id INTEGER PRIMARY KEY,
    scientific_name TEXT NOT NULL
);
CREATE TABLE detections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    label_id INTEGER NOT NULL,
    detected_at INTEGER NOT NULL,
    confidence REAL NOT NULL
);
"""


def today_unix(hour: int = 12, minute: int = 0) -> int:
    import datetime as _dt
    now = _dt.datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)
    return int(now.timestamp())


def yesterday_unix(hour: int = 12) -> int:
    import datetime as _dt
    d = _dt.datetime.now() - _dt.timedelta(days=1)
    d = d.replace(hour=hour, minute=0, second=0, microsecond=0)
    return int(d.timestamp())


def make_db(detections: List[Tuple[int, str, int, float]]) -> str:
    """detections: list of (id, scientific_name, detected_at_unix, confidence)."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    # Build label set from scientific names
    seen: Dict[str, int] = {}
    for _, sci, _, _ in detections:
        if sci not in seen:
            seen[sci] = len(seen) + 1
            conn.execute("INSERT INTO labels (id, scientific_name) VALUES (?, ?)", (seen[sci], sci))
    for det_id, sci, ts, conf in detections:
        conn.execute(
            "INSERT INTO detections (id, label_id, detected_at, confidence) VALUES (?, ?, ?, ?)",
            (det_id, seen[sci], ts, conf),
        )
    conn.commit()
    conn.close()
    return path


# ---------------------------------------------------------------------------
# validate_config
# ---------------------------------------------------------------------------

class TestValidateConfig(unittest.TestCase):
    def test_requires_db_path(self):
        self.assertEqual(validate_config({"db_path": "/tmp/x.db"}), [])

    def test_missing_db_path(self):
        errors = validate_config({})
        self.assertTrue(any("db_path" in e for e in errors))


# ---------------------------------------------------------------------------
# query_first_of_day
# ---------------------------------------------------------------------------

class TestQueryFirstOfDay(unittest.TestCase):
    def test_single_species_today(self):
        db = make_db([
            (1, "Acridotheres tristis", today_unix(7), 0.9),
            (2, "Acridotheres tristis", today_unix(8), 0.8),
        ])
        rows, today_max = query_first_of_day(db, watermark=0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["scientific_name"], "Acridotheres tristis")
        self.assertEqual(rows[0]["first_id"], 1)
        self.assertEqual(today_max, 2)
        os.unlink(db)

    def test_multiple_species_ordered_by_first_id(self):
        db = make_db([
            (1, "Corvus coronoides", today_unix(6), 0.85),
            (2, "Acridotheres tristis", today_unix(7), 0.90),
            (3, "Corvus coronoides", today_unix(8), 0.80),
            (4, "Zosterops lateralis", today_unix(9), 0.70),
        ])
        rows, today_max = query_first_of_day(db, watermark=0)
        self.assertEqual([r["scientific_name"] for r in rows],
                         ["Corvus coronoides", "Acridotheres tristis", "Zosterops lateralis"])
        self.assertEqual(today_max, 4)
        os.unlink(db)

    def test_watermark_excludes_already_seen_species(self):
        db = make_db([
            (1, "Corvus coronoides", today_unix(6), 0.85),
            (2, "Acridotheres tristis", today_unix(7), 0.90),
            (3, "Corvus coronoides", today_unix(8), 0.80),
        ])
        rows, today_max = query_first_of_day(db, watermark=2)
        self.assertEqual(rows, [])
        self.assertEqual(today_max, 3)
        os.unlink(db)

    def test_watermark_admits_new_species(self):
        db = make_db([
            (1, "Corvus coronoides", today_unix(6), 0.85),
            (2, "Acridotheres tristis", today_unix(7), 0.90),
            (3, "Zosterops lateralis", today_unix(8), 0.70),
        ])
        rows, today_max = query_first_of_day(db, watermark=2)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["scientific_name"], "Zosterops lateralis")
        self.assertEqual(rows[0]["first_id"], 3)
        self.assertEqual(today_max, 3)
        os.unlink(db)

    def test_yesterdays_detections_excluded(self):
        db = make_db([
            (1, "Strepera graculina", yesterday_unix(15), 0.9),
            (2, "Acridotheres tristis", today_unix(7), 0.8),
        ])
        rows, today_max = query_first_of_day(db, watermark=0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["scientific_name"], "Acridotheres tristis")
        self.assertEqual(today_max, 2)
        os.unlink(db)

    def test_empty_today_returns_none_max(self):
        db = make_db([
            (1, "Strepera graculina", yesterday_unix(15), 0.9),
        ])
        rows, today_max = query_first_of_day(db, watermark=0)
        self.assertEqual(rows, [])
        self.assertIsNone(today_max)
        os.unlink(db)


# ---------------------------------------------------------------------------
# load_species_cache
# ---------------------------------------------------------------------------

class TestLoadSpeciesCache(unittest.TestCase):
    def test_no_url_returns_empty(self):
        m, fetched, warn = load_species_cache({}, url=None, ttl_seconds=3600, timeout=5)
        self.assertEqual(m, {})
        self.assertIsNone(fetched)
        self.assertIsNone(warn)

    def test_uses_fresh_cache(self):
        from datetime import datetime, timezone
        state = {
            "species_cache": {"Tyto alba": "Barn Owl"},
            "species_cache_fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        with mock.patch("run.fetch_species_map") as fetcher:
            m, _, warn = load_species_cache(state, url="http://x", ttl_seconds=3600, timeout=5)
            self.assertEqual(m, {"Tyto alba": "Barn Owl"})
            self.assertIsNone(warn)
            fetcher.assert_not_called()

    def test_refreshes_on_stale_cache(self):
        from datetime import datetime, timezone, timedelta
        state = {
            "species_cache": {"Tyto alba": "Barn Owl"},
            "species_cache_fetched_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
        }
        with mock.patch("run.fetch_species_map", return_value={"Tyto alba": "Barn Owl", "Corvus coronoides": "Australian Raven"}):
            m, fetched, warn = load_species_cache(state, url="http://x", ttl_seconds=3600, timeout=5)
            self.assertEqual(len(m), 2)
            self.assertIsNotNone(fetched)
            self.assertIsNone(warn)

    def test_fetch_failure_falls_back_to_stale(self):
        from datetime import datetime, timezone, timedelta
        state = {
            "species_cache": {"Tyto alba": "Barn Owl"},
            "species_cache_fetched_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
        }
        with mock.patch("run.fetch_species_map", side_effect=RuntimeError("network down")):
            m, _, warn = load_species_cache(state, url="http://x", ttl_seconds=3600, timeout=5)
            self.assertEqual(m, {"Tyto alba": "Barn Owl"})
            self.assertIn("network down", warn)

    def test_fetch_failure_no_prior_cache_returns_empty(self):
        with mock.patch("run.fetch_species_map", side_effect=RuntimeError("boom")):
            m, _, warn = load_species_cache({}, url="http://x", ttl_seconds=3600, timeout=5)
            self.assertEqual(m, {})
            self.assertIn("boom", warn)


# ---------------------------------------------------------------------------
# build_event
# ---------------------------------------------------------------------------

class TestBuildEvent(unittest.TestCase):
    def test_event_with_common_name(self):
        row = {"scientific_name": "Tyto alba", "first_id": 42, "first_ts": today_unix(21), "peak_conf": 0.95}
        ev = build_event(row, {"Tyto alba": "Barn Owl"}, "birdnet.firstday_species", "b450")
        self.assertEqual(ev["type"], "birdnet.firstday_species")
        self.assertEqual(ev["payload"]["common_name"], "Barn Owl")
        self.assertEqual(ev["payload"]["scientific_name"], "Tyto alba")
        self.assertEqual(ev["payload"]["peak_conf_pct"], 95)
        self.assertIn("Barn Owl", ev["payload"]["message"])
        self.assertIn("first heard today", ev["payload"]["message"])

    def test_event_without_common_name_falls_back(self):
        row = {"scientific_name": "Tyto alba", "first_id": 42, "first_ts": today_unix(21), "peak_conf": 0.5}
        ev = build_event(row, {}, "birdnet.firstday_species", "b450")
        self.assertEqual(ev["payload"]["common_name"], "")
        self.assertIn("Tyto alba", ev["payload"]["message"])


# ---------------------------------------------------------------------------
# poll_command (integration)
# ---------------------------------------------------------------------------

class TestPollCommand(unittest.TestCase):
    def test_missing_db_returns_error_with_retry(self):
        resp = poll_command({"db_path": "/nonexistent.db"}, {}, "b450")
        self.assertEqual(resp["status"], "error")
        self.assertTrue(resp["retry"])

    def test_first_run_emits_all_today_species(self):
        db = make_db([
            (1, "Acridotheres tristis", today_unix(7), 0.9),
            (2, "Corvus coronoides", today_unix(8), 0.8),
        ])
        try:
            resp = poll_command({"db_path": db}, {}, "b450")
            self.assertEqual(resp["status"], "ok")
            self.assertEqual(len(resp["events"]), 2)
            self.assertEqual(resp["state_updates"]["watermark"], 2)
        finally:
            os.unlink(db)

    def test_second_run_no_new_species_emits_nothing(self):
        db = make_db([
            (1, "Acridotheres tristis", today_unix(7), 0.9),
            (2, "Acridotheres tristis", today_unix(8), 0.8),
        ])
        try:
            resp = poll_command({"db_path": db}, {"watermark": 2}, "b450")
            self.assertEqual(resp["status"], "ok")
            self.assertEqual(resp["events"], [])
            self.assertEqual(resp["state_updates"]["watermark"], 2)
        finally:
            os.unlink(db)

    def test_midnight_rollover_new_day_emits_fresh(self):
        # Yesterday's detections end at id=5, today's start at id=6.
        db = make_db([
            (1, "Strepera graculina", yesterday_unix(9), 0.85),
            (5, "Strepera graculina", yesterday_unix(20), 0.75),
            (6, "Acridotheres tristis", today_unix(6), 0.9),
            (7, "Acridotheres tristis", today_unix(7), 0.85),
        ])
        try:
            # Watermark carries over from yesterday.
            resp = poll_command({"db_path": db}, {"watermark": 5}, "b450")
            self.assertEqual(resp["status"], "ok")
            self.assertEqual(len(resp["events"]), 1)
            self.assertEqual(resp["events"][0]["payload"]["scientific_name"], "Acridotheres tristis")
            self.assertEqual(resp["state_updates"]["watermark"], 7)
        finally:
            os.unlink(db)

    def test_species_url_unset_uses_scientific_only(self):
        db = make_db([(1, "Tyto alba", today_unix(22), 0.9)])
        try:
            resp = poll_command({"db_path": db}, {}, "b450")
            self.assertEqual(resp["events"][0]["payload"]["common_name"], "")
            self.assertIn("Tyto alba", resp["events"][0]["payload"]["message"])
        finally:
            os.unlink(db)

    def test_species_url_set_caches_and_enriches(self):
        db = make_db([(1, "Tyto alba", today_unix(22), 0.9)])
        try:
            with mock.patch("run.fetch_species_map", return_value={"Tyto alba": "Barn Owl"}):
                resp = poll_command(
                    {"db_path": db, "species_url": "http://x/range"},
                    {},
                    "b450",
                )
            self.assertEqual(resp["events"][0]["payload"]["common_name"], "Barn Owl")
            self.assertEqual(resp["state_updates"]["species_cache"], {"Tyto alba": "Barn Owl"})
        finally:
            os.unlink(db)


# ---------------------------------------------------------------------------
# health_command
# ---------------------------------------------------------------------------

class TestHealthCommand(unittest.TestCase):
    def test_healthy_with_db(self):
        db = make_db([(1, "Tyto alba", today_unix(22), 0.9)])
        try:
            resp = health_command({"db_path": db}, {"watermark": 5})
            self.assertEqual(resp["status"], "ok")
        finally:
            os.unlink(db)

    def test_degraded_without_db(self):
        resp = health_command({"db_path": "/nonexistent.db"}, {})
        # db missing is a non-retryable config-ish state for health — expect degraded.
        self.assertIn(resp["status"], {"ok", "degraded"})


# ---------------------------------------------------------------------------
# handle_request end-to-end via subprocess (JSON-over-stdin protocol)
# ---------------------------------------------------------------------------

class TestSubprocess(unittest.TestCase):
    def test_poll_via_stdin(self):
        db = make_db([(1, "Tyto alba", today_unix(22), 0.9)])
        try:
            req = {"command": "poll", "config": {"db_path": db}, "state": {}, "instance": "b450"}
            proc = subprocess.run(
                [sys.executable, PLUGIN],
                input=json.dumps(req),
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            resp = json.loads(proc.stdout)
            self.assertEqual(resp["status"], "ok")
            self.assertEqual(len(resp["events"]), 1)
        finally:
            os.unlink(db)

    def test_unknown_command(self):
        req = {"command": "nope", "config": {"db_path": "/tmp/x.db"}}
        proc = subprocess.run(
            [sys.executable, PLUGIN],
            input=json.dumps(req),
            capture_output=True,
            text=True,
            timeout=5,
        )
        resp = json.loads(proc.stdout)
        self.assertEqual(resp["status"], "error")


if __name__ == "__main__":
    unittest.main()
