"""Tests for sqlite_change plugin."""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from typing import Any, Dict

from run import (
    evaluate_threshold,
    handle_request,
    poll_command,
    health_command,
    run_query,
    snapshot_state,
    validate_config,
)

PLUGIN = os.path.join(os.path.dirname(__file__), "run.py")

BASE_CONFIG = {
    "db_path": "/tmp/test.db",
    "query": "SELECT v FROM t",
    "event_type": "test.event",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_db(rows: list[tuple] = None, schema: str = "CREATE TABLE t (v INTEGER)") -> str:
    """Create a temp SQLite DB and return its path."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute(schema)
    for row in (rows or []):
        placeholders = ",".join("?" * len(row))
        conn.execute(f"INSERT INTO t VALUES ({placeholders})", row)
    conn.commit()
    conn.close()
    return path


# ---------------------------------------------------------------------------
# validate_config
# ---------------------------------------------------------------------------

class TestValidateConfig(unittest.TestCase):
    def test_valid_minimal(self):
        self.assertEqual(validate_config(BASE_CONFIG), [])

    def test_missing_db_path(self):
        cfg = {**BASE_CONFIG, "db_path": ""}
        errors = validate_config(cfg)
        self.assertTrue(any("db_path" in e for e in errors))

    def test_missing_query(self):
        cfg = {**BASE_CONFIG, "query": ""}
        errors = validate_config(cfg)
        self.assertTrue(any("query" in e for e in errors))

    def test_missing_event_type(self):
        cfg = {**BASE_CONFIG, "event_type": ""}
        errors = validate_config(cfg)
        self.assertTrue(any("event_type" in e for e in errors))

    def test_comparison_op_requires_threshold_value(self):
        for op in (">", ">=", "<", "<=", "=="):
            with self.subTest(op=op):
                cfg = {**BASE_CONFIG, "threshold_op": op}
                errors = validate_config(cfg)
                self.assertTrue(any("threshold_value" in e for e in errors), errors)

    def test_comparison_op_with_threshold_value_ok(self):
        for op in (">", ">=", "<", "<=", "=="):
            with self.subTest(op=op):
                cfg = {**BASE_CONFIG, "threshold_op": op, "threshold_value": 5}
                self.assertEqual(validate_config(cfg), [])

    def test_changed_op_no_threshold_needed(self):
        cfg = {**BASE_CONFIG, "threshold_op": "changed"}
        self.assertEqual(validate_config(cfg), [])

    def test_any_rows_op_no_threshold_needed(self):
        cfg = {**BASE_CONFIG, "threshold_op": "any_rows"}
        self.assertEqual(validate_config(cfg), [])

    def test_invalid_threshold_op(self):
        cfg = {**BASE_CONFIG, "threshold_op": "between"}
        errors = validate_config(cfg)
        self.assertTrue(any("threshold_op" in e for e in errors))

    def test_all_required_missing(self):
        errors = validate_config({})
        self.assertEqual(len(errors), 3)


# ---------------------------------------------------------------------------
# evaluate_threshold
# ---------------------------------------------------------------------------

class TestEvaluateThreshold(unittest.TestCase):
    def test_any_rows_true(self):
        self.assertTrue(evaluate_threshold("5", True, "any_rows", None, None))

    def test_any_rows_false_no_rows(self):
        self.assertFalse(evaluate_threshold(None, False, "any_rows", None, None))

    def test_changed_first_run_always_triggers(self):
        self.assertTrue(evaluate_threshold("5", True, "changed", None, None))

    def test_changed_same_value(self):
        self.assertFalse(evaluate_threshold("5", True, "changed", None, "5"))

    def test_changed_different_value(self):
        self.assertTrue(evaluate_threshold("6", True, "changed", None, "5"))

    def test_changed_null_to_value(self):
        self.assertTrue(evaluate_threshold("1", True, "changed", None, None))

    def test_changed_value_to_null(self):
        self.assertTrue(evaluate_threshold(None, False, "changed", None, "1"))

    def test_gt_met(self):
        self.assertTrue(evaluate_threshold("10", True, ">", 5.0, None))

    def test_gt_not_met(self):
        self.assertFalse(evaluate_threshold("3", True, ">", 5.0, None))

    def test_gt_equal_not_met(self):
        self.assertFalse(evaluate_threshold("5", True, ">", 5.0, None))

    def test_gte_equal_met(self):
        self.assertTrue(evaluate_threshold("5", True, ">=", 5.0, None))

    def test_lt_met(self):
        self.assertTrue(evaluate_threshold("3", True, "<", 5.0, None))

    def test_lt_not_met(self):
        self.assertFalse(evaluate_threshold("7", True, "<", 5.0, None))

    def test_lte_equal_met(self):
        self.assertTrue(evaluate_threshold("5", True, "<=", 5.0, None))

    def test_eq_met(self):
        self.assertTrue(evaluate_threshold("5", True, "==", 5.0, None))

    def test_eq_not_met(self):
        self.assertFalse(evaluate_threshold("6", True, "==", 5.0, None))

    def test_null_scalar_numeric_treated_as_zero(self):
        # No rows returned: scalar is None, numeric ops treat as 0.0
        self.assertFalse(evaluate_threshold(None, False, ">", 5.0, None))
        self.assertTrue(evaluate_threshold(None, False, "<", 5.0, None))
        self.assertTrue(evaluate_threshold(None, False, "==", 0.0, None))

    def test_float_scalar_comparison(self):
        self.assertTrue(evaluate_threshold("3.14", True, ">", 3.0, None))


# ---------------------------------------------------------------------------
# snapshot_state
# ---------------------------------------------------------------------------

class TestSnapshotState(unittest.TestCase):
    def test_returns_full_compatibility_snapshot(self):
        self.assertEqual(
            snapshot_state(
                last_result="5",
                last_checked_at="2026-04-24T00:00:00+00:00",
                last_triggered_at=None,
            ),
            {
                "last_result": "5",
                "last_checked_at": "2026-04-24T00:00:00+00:00",
                "last_triggered_at": None,
            },
        )


# ---------------------------------------------------------------------------
# run_query
# ---------------------------------------------------------------------------

class TestRunQuery(unittest.TestCase):
    def setUp(self):
        self.db = make_db(rows=[(42,)])

    def tearDown(self):
        os.unlink(self.db)

    def test_returns_scalar(self):
        scalar, had_rows = run_query(self.db, "SELECT v FROM t")
        self.assertEqual(scalar, "42")
        self.assertTrue(had_rows)

    def test_no_rows(self):
        scalar, had_rows = run_query(self.db, "SELECT v FROM t WHERE v = 999")
        self.assertIsNone(scalar)
        self.assertFalse(had_rows)

    def test_aggregate(self):
        scalar, had_rows = run_query(self.db, "SELECT COUNT(*) FROM t")
        self.assertEqual(scalar, "1")
        self.assertTrue(had_rows)

    def test_first_column_only(self):
        db = make_db(schema="CREATE TABLE t (a INTEGER, b TEXT)", rows=[(7, "ignored")])
        try:
            scalar, _ = run_query(db, "SELECT a, b FROM t")
            self.assertEqual(scalar, "7")
        finally:
            os.unlink(db)

    def test_null_cell_returns_none(self):
        db = make_db(schema="CREATE TABLE t (v INTEGER)")
        conn = sqlite3.connect(db)
        conn.execute("INSERT INTO t VALUES (NULL)")
        conn.commit()
        conn.close()
        try:
            scalar, had_rows = run_query(db, "SELECT v FROM t")
            self.assertIsNone(scalar)
            self.assertTrue(had_rows)
        finally:
            os.unlink(db)

    def test_read_only_prevents_write(self):
        with self.assertRaises(Exception):
            run_query(self.db, "INSERT INTO t VALUES (99)")


# ---------------------------------------------------------------------------
# poll_command
# ---------------------------------------------------------------------------

class TestPollCommand(unittest.TestCase):
    def setUp(self):
        self.db = make_db(rows=[(5,)])

    def tearDown(self):
        os.unlink(self.db)

    def cfg(self, **kwargs) -> Dict[str, Any]:
        return {**BASE_CONFIG, "db_path": self.db, **kwargs}

    def test_db_not_found_retry(self):
        r = poll_command({**BASE_CONFIG, "db_path": "/nonexistent/path.db"}, {}, "inst")
        self.assertEqual(r["status"], "error")
        self.assertTrue(r["retry"])

    def test_missing_config_key_no_retry(self):
        r = poll_command({"db_path": self.db, "query": "SELECT 1"}, {}, "inst")
        self.assertEqual(r["status"], "error")
        self.assertFalse(r["retry"])

    def test_threshold_not_met_no_events(self):
        r = poll_command(self.cfg(threshold_op=">", threshold_value=10), {}, "inst")
        self.assertEqual(r["status"], "ok")
        self.assertEqual(r["events"], [])

    def test_threshold_met_emits_event(self):
        r = poll_command(self.cfg(threshold_op=">", threshold_value=3), {}, "inst")
        self.assertEqual(r["status"], "ok")
        self.assertEqual(len(r["events"]), 1)
        event = r["events"][0]
        self.assertEqual(event["type"], "test.event")
        self.assertEqual(event["payload"]["source"], "sqlite_change")
        self.assertEqual(event["payload"]["instance"], "inst")
        self.assertEqual(event["payload"]["result"], "5")
        self.assertEqual(event["payload"]["threshold_op"], ">")
        self.assertEqual(event["payload"]["threshold_value"], 3.0)

    def test_event_payload_has_previous_result(self):
        r = poll_command(self.cfg(threshold_op="changed"), {"last_result": "3"}, "inst")
        self.assertEqual(r["events"][0]["payload"]["previous_result"], "3")

    def test_event_payload_previous_result_null_on_first_run(self):
        r = poll_command(self.cfg(threshold_op="changed"), {}, "inst")
        self.assertIsNone(r["events"][0]["payload"]["previous_result"])

    def test_state_updates_always_present_on_ok(self):
        # Sprint 7: poll emits a full snapshot containing all durable keys.
        r = poll_command(self.cfg(threshold_op=">", threshold_value=100), {}, "inst")
        self.assertIn("state_updates", r)
        su = r["state_updates"]
        self.assertIn("last_result", su)
        self.assertIn("last_checked_at", su)
        self.assertIn("last_triggered_at", su)

    def test_state_updates_triggered_at_when_met(self):
        r = poll_command(self.cfg(threshold_op="changed"), {}, "inst")
        self.assertIsNotNone(r["state_updates"]["last_triggered_at"])

    def test_state_updates_triggered_at_carried_forward_when_not_met(self):
        prior = "2024-01-01T00:00:00+00:00"
        r = poll_command(
            self.cfg(threshold_op="changed"),
            {"last_result": "5", "last_triggered_at": prior},
            "inst",
        )
        self.assertEqual(r["state_updates"]["last_triggered_at"], prior)

    def test_state_updates_triggered_at_null_when_never_triggered(self):
        r = poll_command(self.cfg(threshold_op="changed"), {"last_result": "5"}, "inst")
        self.assertIsNone(r["state_updates"]["last_triggered_at"])

    def test_changed_first_run_triggers(self):
        r = poll_command(self.cfg(threshold_op="changed"), {}, "inst")
        self.assertEqual(len(r["events"]), 1)

    def test_changed_same_value_no_event(self):
        r = poll_command(self.cfg(threshold_op="changed"), {"last_result": "5"}, "inst")
        self.assertEqual(r["events"], [])

    def test_any_rows_triggers_when_rows_exist(self):
        r = poll_command(self.cfg(threshold_op="any_rows"), {}, "inst")
        self.assertEqual(len(r["events"]), 1)

    def test_any_rows_no_trigger_when_empty(self):
        # COUNT(*) always returns a row — use a direct SELECT to get zero rows
        empty_db = make_db()
        try:
            r = poll_command(
                {**BASE_CONFIG, "db_path": empty_db, "query": "SELECT v FROM t", "threshold_op": "any_rows"},
                {}, "inst",
            )
            self.assertEqual(r["events"], [])
        finally:
            os.unlink(empty_db)

    def test_bad_query_retry(self):
        r = poll_command(self.cfg(query="SELECT * FROM nonexistent_table"), {}, "inst")
        self.assertEqual(r["status"], "error")
        self.assertTrue(r["retry"])

    def test_default_threshold_op_is_changed(self):
        # No threshold_op in config — should default to 'changed'
        r = poll_command(self.cfg(), {}, "inst")
        self.assertEqual(len(r["events"]), 1)  # first run → changed triggers


# ---------------------------------------------------------------------------
# health_command
# ---------------------------------------------------------------------------

class TestHealthCommand(unittest.TestCase):
    def setUp(self):
        self.db = make_db(rows=[(7,)])

    def tearDown(self):
        os.unlink(self.db)

    def test_ok_when_db_exists(self):
        r = health_command({**BASE_CONFIG, "db_path": self.db}, {})
        self.assertEqual(r["status"], "ok")
        self.assertEqual(r["result"], "ok")

    def test_degraded_when_db_missing(self):
        r = health_command({**BASE_CONFIG, "db_path": "/nonexistent.db"}, {})
        self.assertEqual(r["result"], "degraded")

    def test_health_does_not_write_durable_state(self):
        # Sprint 7: health is diagnostic-only and must not write state_updates.
        r = health_command({**BASE_CONFIG, "db_path": self.db}, {})
        self.assertNotIn("state_updates", r)

    def test_invalid_config_error(self):
        r = health_command({}, {})
        self.assertEqual(r["status"], "error")

    def test_bad_query_degraded(self):
        r = health_command({**BASE_CONFIG, "db_path": self.db, "query": "SELECT * FROM nope"}, {})
        self.assertEqual(r["result"], "degraded")

    def test_logs_contain_health_info(self):
        r = health_command({**BASE_CONFIG, "db_path": self.db}, {})
        log_text = " ".join(e["message"] for e in r.get("logs", []))
        self.assertIn("db_exists", log_text)


# ---------------------------------------------------------------------------
# handle_request dispatch
# ---------------------------------------------------------------------------

class TestHandleRequest(unittest.TestCase):
    def test_unknown_command(self):
        r = handle_request({"command": "bogus", "config": BASE_CONFIG, "state": {}})
        self.assertEqual(r["status"], "error")
        self.assertIn("unknown command", r["error"])

    def test_missing_command_key(self):
        r = handle_request({"config": BASE_CONFIG, "state": {}})
        self.assertEqual(r["status"], "error")

    def test_config_defaults_to_empty_dict(self):
        # Should not raise — config missing means empty dict, validation returns errors
        r = handle_request({"command": "poll", "state": {}})
        self.assertEqual(r["status"], "error")

    def test_state_defaults_to_empty_dict(self):
        db = make_db(rows=[(1,)])
        try:
            r = handle_request({"command": "poll", "config": {**BASE_CONFIG, "db_path": db}})
            self.assertEqual(r["status"], "ok")
        finally:
            os.unlink(db)


# ---------------------------------------------------------------------------
# Subprocess / wire protocol integration tests
# ---------------------------------------------------------------------------

class TestProtocol(unittest.TestCase):
    def _run(self, request: Dict[str, Any]) -> tuple[Dict[str, Any], int]:
        result = subprocess.run(
            [sys.executable, PLUGIN],
            input=json.dumps(request),
            capture_output=True,
            text=True,
        )
        return json.loads(result.stdout), result.returncode

    def test_invalid_json_exits_nonzero(self):
        result = subprocess.run(
            [sys.executable, PLUGIN],
            input="not json {{{",
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        r = json.loads(result.stdout)
        self.assertEqual(r["status"], "error")
        self.assertIn("Invalid JSON", r["error"])

    def test_unknown_command(self):
        r, _ = self._run({"command": "reticulate", "config": BASE_CONFIG, "state": {}})
        self.assertEqual(r["status"], "error")
        self.assertIn("unknown command", r["error"])

    def test_poll_full_roundtrip(self):
        db = make_db(rows=[(99,)])
        try:
            r, code = self._run({
                "command": "poll",
                "config": {**BASE_CONFIG, "db_path": db, "threshold_op": "changed"},
                "state": {},
                "instance": "roundtrip_test",
            })
            self.assertEqual(code, 0)
            self.assertEqual(r["status"], "ok")
            self.assertEqual(len(r["events"]), 1)
            self.assertEqual(r["events"][0]["payload"]["result"], "99")
            self.assertEqual(r["state_updates"]["last_result"], "99")
        finally:
            os.unlink(db)

    def test_poll_no_change_no_event(self):
        db = make_db(rows=[(42,)])
        try:
            r, _ = self._run({
                "command": "poll",
                "config": {**BASE_CONFIG, "db_path": db, "threshold_op": "changed"},
                "state": {"last_result": "42"},
                "instance": "no_change",
            })
            self.assertEqual(r["status"], "ok")
            self.assertEqual(r["events"], [])
        finally:
            os.unlink(db)

    def test_health_full_roundtrip(self):
        db = make_db(rows=[(1,)])
        try:
            r, code = self._run({
                "command": "health",
                "config": {**BASE_CONFIG, "db_path": db},
                "state": {"last_result": "1"},
            })
            self.assertEqual(code, 0)
            self.assertEqual(r["status"], "ok")
            # Sprint 7: health must not emit state_updates.
            self.assertNotIn("state_updates", r)
        finally:
            os.unlink(db)

    def test_output_is_valid_json(self):
        db = make_db(rows=[(1,)])
        try:
            result = subprocess.run(
                [sys.executable, PLUGIN],
                input=json.dumps({"command": "poll", "config": {**BASE_CONFIG, "db_path": db}, "state": {}}),
                capture_output=True,
                text=True,
            )
            # Must be parseable and have no stderr leakage into stdout
            parsed = json.loads(result.stdout)
            self.assertIn("status", parsed)
        finally:
            os.unlink(db)


if __name__ == "__main__":
    unittest.main()
