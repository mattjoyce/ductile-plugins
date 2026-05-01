"""Tests for email_pipeline_fetch plugin."""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pytest

import run as plugin
from run import DEDUPE_PREFIX, EVENT_TYPE, GWSError, cmd_health, cmd_poll, main

# ── poll: first run ───────────────────────────────────────────────────────────


def test_first_run_records_baseline_snapshot_and_emits_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(plugin, "now_iso", lambda: "2026-04-30T03:00:00+00:00")
    monkeypatch.setattr(plugin, "get_current_history_id", lambda _binary: "100000")

    resp = cmd_poll({}, {})

    assert resp["status"] == "ok"
    assert resp["state_updates"] == {
        "last_history_id": "100000",
        "last_poll_at": "2026-04-30T03:00:00+00:00",
        "history_reset_count": 0,
    }
    assert "events" not in resp


# ── poll: subsequent runs ─────────────────────────────────────────────────────


def test_emits_full_message_event_per_new_message(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(plugin, "now_iso", lambda: "2026-04-30T03:05:00+00:00")
    monkeypatch.setattr(
        plugin,
        "fetch_history",
        lambda *a, **kw: ([{"id": "msg-A", "threadId": "thr-A"}], "100050"),
    )
    monkeypatch.setattr(
        plugin,
        "fetch_full_message",
        lambda _binary, _msg_id: {
            "id": "msg-A",
            "threadId": "thr-A",
            "labelIds": ["INBOX"],
            "snippet": "Hello there",
            "payload": {"headers": [{"name": "From", "value": "x@y.z"}]},
            "internalDate": "1714435200000",
        },
    )

    resp = cmd_poll(
        {},
        {"last_history_id": "100000", "history_reset_count": 0},
    )

    assert resp["status"] == "ok"
    assert resp["state_updates"] == {
        "last_history_id": "100050",
        "last_poll_at": "2026-04-30T03:05:00+00:00",
        "history_reset_count": 0,
    }
    assert len(resp["events"]) == 1
    ev = resp["events"][0]
    assert ev["type"] == EVENT_TYPE
    assert ev["dedupe_key"] == f"{DEDUPE_PREFIX}msg-A"
    assert ev["payload"]["message_id"] == "msg-A"
    assert ev["payload"]["thread_id"] == "thr-A"
    assert ev["payload"]["raw_message_json"]["id"] == "msg-A"
    assert "payload" in ev["payload"]["raw_message_json"]


def test_no_messages_keeps_snapshot_shape_no_events(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(plugin, "now_iso", lambda: "2026-04-30T03:05:00+00:00")
    monkeypatch.setattr(plugin, "fetch_history", lambda *a, **kw: ([], "100050"))

    resp = cmd_poll(
        {},
        {"last_history_id": "100000", "history_reset_count": 0},
    )

    assert resp["status"] == "ok"
    assert resp["state_updates"]["last_history_id"] == "100050"
    assert resp["state_updates"]["history_reset_count"] == 0
    assert "events" not in resp


def test_history_gap_resets_and_increments_counter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(plugin, "now_iso", lambda: "2026-04-30T03:05:00+00:00")
    monkeypatch.setattr(plugin, "get_current_history_id", lambda _binary: "999999")

    def _gap(*_a, **_kw):
        from run import HistoryGapError as _HGE

        raise _HGE(
            "historyId '100000' is stale (purged by Gmail)",
            retry=False,
        )

    monkeypatch.setattr(plugin, "fetch_history", _gap)

    resp = cmd_poll(
        {},
        {"last_history_id": "100000", "history_reset_count": 2},
    )

    assert resp["status"] == "ok"
    assert resp["state_updates"]["last_history_id"] == "999999"
    assert resp["state_updates"]["history_reset_count"] == 3
    assert "events" not in resp


# ── health ────────────────────────────────────────────────────────────────────


def test_health_success_emits_no_state_updates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(plugin.shutil, "which", lambda _binary: "/opt/homebrew/bin/gws")
    monkeypatch.setattr(
        plugin,
        "gws_run",
        lambda *a, **kw: {"emailAddress": "splendidupdating@gmail.com"},
    )

    resp = cmd_health({})

    assert resp["status"] == "ok"
    assert "splendidupdating@gmail.com" in resp["result"]
    assert "state_updates" not in resp
    assert "events" not in resp


def test_health_fails_when_gws_missing_from_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(plugin.shutil, "which", lambda _binary: None)

    resp = cmd_health({})

    assert resp["status"] == "error"
    assert "gws binary not found" in resp["error"]
    assert "state_updates" not in resp


def test_health_propagates_gws_run_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(plugin.shutil, "which", lambda _binary: "/opt/homebrew/bin/gws")

    def _raise(*_a: object, **_kw: object) -> dict:
        raise GWSError("Gmail auth/permission error (401): bad creds", retry=False)

    monkeypatch.setattr(plugin, "gws_run", _raise)

    resp = cmd_health({})

    assert resp["status"] == "error"
    assert "Gmail health check failed" in resp["error"]
    assert "401" in resp["error"]


def test_gws_run_raises_on_invalid_json_output(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify gws_run rejects non-JSON stdout with a clear error."""

    class _FakeResult:
        stdout = "this is not json"
        returncode = 0

    monkeypatch.setattr(plugin.subprocess, "run", lambda *a, **kw: _FakeResult())

    with pytest.raises(GWSError, match="not valid JSON"):
        plugin.gws_run("gws", "gmail", "users", "getProfile")


# ── unknown command ──────────────────────────────────────────────────────────


def test_unknown_command_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    stdin = io.StringIO(json.dumps({"command": "frobnicate", "config": {}, "state": {}}))
    stdout = io.StringIO()
    monkeypatch.setattr(sys, "stdin", stdin)
    monkeypatch.setattr(sys, "stdout", stdout)

    main()

    out = json.loads(stdout.getvalue().strip())
    assert out["status"] == "error"
    assert "Unknown command" in out["error"]
