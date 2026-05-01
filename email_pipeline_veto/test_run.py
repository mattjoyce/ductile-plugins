"""Tests for email_pipeline_veto plugin."""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pytest

from run import (
    DECISION_PROCESS,
    DECISION_QUARANTINE,
    DEDUPE_PREFIX,
    DISP_CLASSIFIERS_ONLY,
    DISP_DISAGREEMENT,
    DISP_MISSING,
    DISP_REGEX_ONLY,
    DISP_SINGLE_CLASSIFIER,
    DISP_UNANIMOUS_BLOCK,
    DISP_UNANIMOUS_PASS,
    EVENT_TYPE,
    VERDICT_MISSING,
    cmd_handle,
    cmd_health,
    compute_disposition,
    fuse,
    main,
)


def _ctx(
    *,
    regex: str | None = "pass",
    promptguard: str | None = "pass",
    classifier_a: str | None = "pass",
    msg_id: str = "M-1",
) -> dict[str, object]:
    scorer: dict[str, object] = {}
    if regex is not None:
        scorer["regex"] = {"verdict": regex}
    if promptguard is not None:
        scorer["promptguard"] = {"verdict": promptguard}
    if classifier_a is not None:
        scorer["classifier_a"] = {"verdict": classifier_a}
    return {"mail": {"message_id": msg_id}, "scorer": scorer}


# ── fuse: pure logic ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("regex", "pg", "cls_a", "expected_decision", "expected_reasons"),
    [
        ("pass", "pass", "pass", DECISION_PROCESS, []),
        ("block", "pass", "pass", DECISION_QUARANTINE, ["regex"]),
        ("pass", "block", "pass", DECISION_QUARANTINE, ["promptguard"]),
        ("pass", "pass", "block", DECISION_QUARANTINE, ["classifier_a"]),
        ("block", "block", "pass", DECISION_QUARANTINE, ["regex", "promptguard"]),
        ("block", "pass", "block", DECISION_QUARANTINE, ["regex", "classifier_a"]),
        ("pass", "block", "block", DECISION_QUARANTINE, ["promptguard", "classifier_a"]),
        (
            "block",
            "block",
            "block",
            DECISION_QUARANTINE,
            ["regex", "promptguard", "classifier_a"],
        ),
        (
            "missing",
            "pass",
            "pass",
            DECISION_QUARANTINE,
            ["regex"],
        ),
        (
            "pass",
            "missing",
            "pass",
            DECISION_QUARANTINE,
            ["promptguard"],
        ),
    ],
)
def test_fuse_decision_table(
    regex: str,
    pg: str,
    cls_a: str,
    expected_decision: str,
    expected_reasons: list[str],
) -> None:
    decision, reasons = fuse({"regex": regex, "promptguard": pg, "classifier_a": cls_a})
    assert decision == expected_decision
    assert reasons == expected_reasons


# ── compute_disposition ───────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("regex", "pg", "cls_a", "expected_disp"),
    [
        ("block", "block", "block", DISP_UNANIMOUS_BLOCK),
        ("pass", "pass", "pass", DISP_UNANIMOUS_PASS),
        ("block", "pass", "pass", DISP_REGEX_ONLY),
        ("pass", "block", "block", DISP_CLASSIFIERS_ONLY),
        ("pass", "block", "pass", DISP_SINGLE_CLASSIFIER),
        ("pass", "pass", "block", DISP_SINGLE_CLASSIFIER),
        ("block", "block", "pass", DISP_DISAGREEMENT),
        ("block", "pass", "block", DISP_DISAGREEMENT),
        ("missing", "pass", "pass", DISP_MISSING),
        ("pass", "missing", "block", DISP_MISSING),
        ("missing", "missing", "missing", DISP_MISSING),
    ],
)
def test_compute_disposition(regex: str, pg: str, cls_a: str, expected_disp: str) -> None:
    disp = compute_disposition({"regex": regex, "promptguard": pg, "classifier_a": cls_a})
    assert disp == expected_disp


# ── handle: end-to-end ────────────────────────────────────────────────────────


def test_handle_all_pass_emits_process() -> None:
    resp = cmd_handle({}, _ctx())

    assert resp["status"] == "ok"
    assert len(resp["events"]) == 1
    ev = resp["events"][0]
    assert ev["type"] == EVENT_TYPE
    assert ev["dedupe_key"] == f"{DEDUPE_PREFIX}M-1"
    assert ev["payload"]["decision"] == DECISION_PROCESS
    assert ev["payload"]["block_reasons"] == []
    assert ev["payload"]["disposition"] == DISP_UNANIMOUS_PASS
    assert ev["payload"]["scorer_verdicts"] == {
        "regex": "pass",
        "promptguard": "pass",
        "classifier_a": "pass",
    }


def test_handle_unanimous_block_emits_quarantine() -> None:
    resp = cmd_handle({}, _ctx(regex="block", promptguard="block", classifier_a="block"))

    assert resp["status"] == "ok"
    ev = resp["events"][0]
    assert ev["payload"]["decision"] == DECISION_QUARANTINE
    assert ev["payload"]["disposition"] == DISP_UNANIMOUS_BLOCK
    assert ev["payload"]["block_reasons"] == ["regex", "promptguard", "classifier_a"]


def test_handle_regex_only_block_quarantines_with_audit_tag() -> None:
    resp = cmd_handle({}, _ctx(regex="block", promptguard="pass", classifier_a="pass"))

    assert resp["status"] == "ok"
    ev = resp["events"][0]
    assert ev["payload"]["decision"] == DECISION_QUARANTINE
    assert ev["payload"]["disposition"] == DISP_REGEX_ONLY
    assert ev["payload"]["block_reasons"] == ["regex"]


def test_handle_classifiers_only_block() -> None:
    resp = cmd_handle({}, _ctx(regex="pass", promptguard="block", classifier_a="block"))

    ev = resp["events"][0]
    assert ev["payload"]["decision"] == DECISION_QUARANTINE
    assert ev["payload"]["disposition"] == DISP_CLASSIFIERS_ONLY


def test_handle_missing_scorer_quarantines() -> None:
    resp = cmd_handle({}, _ctx(promptguard=None))

    assert resp["status"] == "ok"
    ev = resp["events"][0]
    assert ev["payload"]["decision"] == DECISION_QUARANTINE
    assert ev["payload"]["disposition"] == DISP_MISSING
    assert ev["payload"]["scorer_verdicts"]["promptguard"] == VERDICT_MISSING
    assert "promptguard" in ev["payload"]["block_reasons"]
    assert any("strict-three" in log["message"] for log in resp["logs"])


def test_handle_invalid_verdict_value_treated_as_missing() -> None:
    """If a scorer emits something other than 'block'/'pass', strict-three triggers."""
    ctx = _ctx()
    ctx["scorer"]["regex"] = {"verdict": "WAT"}  # type: ignore[index]

    resp = cmd_handle({}, ctx)

    ev = resp["events"][0]
    assert ev["payload"]["scorer_verdicts"]["regex"] == VERDICT_MISSING
    assert ev["payload"]["decision"] == DECISION_QUARANTINE


def test_handle_missing_message_id_errors() -> None:
    resp = cmd_handle({}, {"mail": {}, "scorer": {}})

    assert resp["status"] == "error"
    assert "message_id" in resp["error"]


def test_handle_missing_mail_errors() -> None:
    resp = cmd_handle({}, {"scorer": {}})

    assert resp["status"] == "error"
    assert "context.mail" in resp["error"]


def test_handle_dedupe_key_uses_prefix() -> None:
    resp = cmd_handle({}, _ctx(msg_id="<abc@example.com>"))

    assert resp["status"] == "ok"
    assert resp["events"][0]["dedupe_key"] == f"{DEDUPE_PREFIX}<abc@example.com>"


# ── verdict normalisation ─────────────────────────────────────────────────────


def test_handle_verdict_uppercase_normalised() -> None:
    ctx = _ctx()
    ctx["scorer"]["regex"] = {"verdict": "BLOCK"}  # type: ignore[index]

    resp = cmd_handle({}, ctx)

    ev = resp["events"][0]
    assert ev["payload"]["scorer_verdicts"]["regex"] == "block"
    assert ev["payload"]["decision"] == DECISION_QUARANTINE


# ── health ────────────────────────────────────────────────────────────────────


def test_health() -> None:
    resp = cmd_health({})

    assert resp["status"] == "ok"
    assert "veto" in resp["result"]


# ── unknown / bad input ───────────────────────────────────────────────────────


def test_unknown_command_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    stdin = io.StringIO(json.dumps({"command": "frobnicate"}))
    stdout = io.StringIO()
    monkeypatch.setattr(sys, "stdin", stdin)
    monkeypatch.setattr(sys, "stdout", stdout)

    main()

    out = json.loads(stdout.getvalue().strip())
    assert out["status"] == "error"
    assert "Unknown command" in out["error"]


def test_main_invalid_json_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    stdin = io.StringIO("not json")
    stdout = io.StringIO()
    monkeypatch.setattr(sys, "stdin", stdin)
    monkeypatch.setattr(sys, "stdout", stdout)

    with pytest.raises(SystemExit) as ei:
        main()

    assert ei.value.code == 1
    out = json.loads(stdout.getvalue().strip())
    assert out["status"] == "error"
