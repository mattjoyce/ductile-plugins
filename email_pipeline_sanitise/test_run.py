"""Tests for email_pipeline_sanitise plugin."""

from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

import pytest

import run as plugin
from run import (
    DEDUPE_PREFIX,
    DEFAULT_SCHEMA_VERSION,
    EVENT_TYPE,
    SanitiserError,
    cmd_handle,
    cmd_health,
    main,
)


def _facts_blob(
    message_id: str = "<m-1@x.y>",
    schema: str = DEFAULT_SCHEMA_VERSION,
) -> dict[str, Any]:
    return {
        "schema_version": schema,
        "message_id": message_id,
        "parts": [{"mime_type": "text/plain", "sanitised_text": "hello", "byte_count": 5}],
        "mime_summary": {
            "top_level_type": "text/plain",
            "part_count": 1,
            "has_plain_text": True,
            "has_html": False,
            "has_calendar": False,
        },
        "attachments": [],
        "unicode_normalisation": "nfkc",
    }


# ── handle ────────────────────────────────────────────────────────────────────


def test_handle_emits_email_sanitised_with_dedupe_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        plugin,
        "run_sanitise",
        lambda *a, **kw: _facts_blob(message_id="<abc@example.com>"),
    )
    event = {"payload": {"message_id": "M-1", "raw_message_json": {"id": "M-1"}}}

    resp = cmd_handle({}, event)

    assert resp["status"] == "ok"
    assert len(resp["events"]) == 1
    ev = resp["events"][0]
    assert ev["type"] == EVENT_TYPE
    assert ev["dedupe_key"] == f"{DEDUPE_PREFIX}M-1"
    assert ev["payload"]["schema_version"] == DEFAULT_SCHEMA_VERSION
    assert ev["payload"]["message_id"] == "<abc@example.com>"
    assert len(ev["payload"]["parts"]) == 1
    assert "state_updates" not in resp


def test_handle_defensive_message_id_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    facts = _facts_blob()
    facts["message_id"] = ""
    monkeypatch.setattr(plugin, "run_sanitise", lambda *a, **kw: facts)
    event = {"payload": {"message_id": "M-2", "raw_message_json": {"id": "M-2"}}}

    resp = cmd_handle({}, event)

    assert resp["status"] == "ok"
    assert resp["events"][0]["payload"]["message_id"] == "M-2"


def test_handle_missing_message_id_errors() -> None:
    event = {"payload": {"raw_message_json": {"id": "M-1"}}}

    resp = cmd_handle({}, event)

    assert resp["status"] == "error"
    assert "message_id" in resp["error"]
    assert "events" not in resp


def test_handle_missing_raw_message_json_errors() -> None:
    event = {"payload": {"message_id": "M-1"}}

    resp = cmd_handle({}, event)

    assert resp["status"] == "error"
    assert "raw_message_json" in resp["error"]


def test_handle_propagates_sanitiser_error_with_retry_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(*_a, **_kw):
        raise SanitiserError("sanitise exited 1: bad input", retry=False)

    monkeypatch.setattr(plugin, "run_sanitise", _raise)
    event = {"payload": {"message_id": "M-1", "raw_message_json": {"id": "M-1"}}}

    resp = cmd_handle({}, event)

    assert resp["status"] == "error"
    assert resp["retry"] is False
    assert "sanitise exited 1" in resp["error"]


def test_handle_rejects_schema_version_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(plugin, "run_sanitise", lambda *a, **kw: _facts_blob(schema="9.9.9"))
    event = {"payload": {"message_id": "M-1", "raw_message_json": {"id": "M-1"}}}

    resp = cmd_handle({}, event)

    assert resp["status"] == "error"
    assert "schema version mismatch" in resp["error"]


def test_handle_rejects_non_serialisable_raw_message_json() -> None:
    class NotJSON:
        pass

    event = {"payload": {"message_id": "M-1", "raw_message_json": {"x": NotJSON()}}}

    resp = cmd_handle({}, event)

    assert resp["status"] == "error"
    assert "not JSON-serialisable" in resp["error"]


def test_handle_propagates_timeout_with_retry_true(monkeypatch: pytest.MonkeyPatch) -> None:
    def _timeout(*_a, **_kw):
        raise SanitiserError("sanitise timed out after 25s", retry=True)

    monkeypatch.setattr(plugin, "run_sanitise", _timeout)
    event = {"payload": {"message_id": "M-1", "raw_message_json": {"id": "M-1"}}}

    resp = cmd_handle({}, event)

    assert resp["status"] == "error"
    assert resp["retry"] is True
    assert "timed out" in resp["error"]


# ── health ────────────────────────────────────────────────────────────────────


def test_health_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        plugin.subprocess,
        "run",
        lambda *a, **kw: subprocess.CompletedProcess(
            args=["sanitise", "--version"],
            returncode=0,
            stdout=b"0.1.0\n",
            stderr=b"",
        ),
    )

    resp = cmd_health({})

    assert resp["status"] == "ok"
    assert "0.1.0" in resp["result"]


def test_health_fails_when_binary_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    def _missing(*_a, **_kw):
        raise FileNotFoundError

    monkeypatch.setattr(plugin.subprocess, "run", _missing)

    resp = cmd_health({"sanitise_binary": "sanitise"})

    assert resp["status"] == "error"
    assert "not found" in resp["error"]


def test_health_fails_on_schema_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        plugin.subprocess,
        "run",
        lambda *a, **kw: subprocess.CompletedProcess(
            args=["sanitise", "--version"],
            returncode=0,
            stdout=b"0.0.1\n",
            stderr=b"",
        ),
    )

    resp = cmd_health({})

    assert resp["status"] == "error"
    assert "schema version mismatch" in resp["error"]


def test_health_fails_on_nonzero_version_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        plugin.subprocess,
        "run",
        lambda *a, **kw: subprocess.CompletedProcess(
            args=["sanitise", "--version"],
            returncode=2,
            stdout=b"",
            stderr=b"flag error\n",
        ),
    )

    resp = cmd_health({})

    assert resp["status"] == "error"
    assert "exited 2" in resp["error"]


# ── unknown command ──────────────────────────────────────────────────────────


def test_unknown_command_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    stdin = io.StringIO(json.dumps({"command": "frobnicate", "config": {}, "event": {}}))
    stdout = io.StringIO()
    monkeypatch.setattr(sys, "stdin", stdin)
    monkeypatch.setattr(sys, "stdout", stdout)

    main()

    out = json.loads(stdout.getvalue().strip())
    assert out["status"] == "error"
    assert "Unknown command" in out["error"]
