"""Tests for email_pipeline_veto plugin (v0.3.0 — 5-input tiered fusion)."""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

import pytest

from run import (
    DECISION_PROCESS,
    DECISION_QUARANTINE,
    DEDUPE_PREFIX,
    EVENT_TYPE,
    FAST_BLOCK_THRESHOLD,
    FAST_PASS_THRESHOLD,
    LLM_BLOCK_THRESHOLD,
    TRUST_TRUSTED,
    TRUST_UNKNOWN,
    cmd_handle,
    cmd_health,
    main,
    read_scores,
)


def _ctx(
    *,
    regex: float = 0.05,
    promptguard: float = 0.05,
    sentinel: float = 0.05,
    classifier_a_verdict: str = "pass",
    classifier_a_confidence: float = 0.95,
    trust_level: str = TRUST_UNKNOWN,
    msg_id: str = "M-1",
) -> dict[str, Any]:
    """Build a baggage context. Defaults are all-low/benign + unknown sender."""
    return {
        "mail": {"message_id": msg_id},
        "scorer": {
            "regex": {"max_weight": regex},
            "promptguard": {"score": promptguard},
            "sentinel": {"score": sentinel},
            "classifier_a": {
                "verdict": classifier_a_verdict,
                "confidence": classifier_a_confidence,
            },
        },
        "sender": {"trust_level": trust_level},
    }


# ── read_scores ──────────────────────────────────────────────────────────────


def test_read_scores_includes_sentinel() -> None:
    scores = read_scores(_ctx(sentinel=0.42))
    assert scores["sentinel"] == pytest.approx(0.42)
    assert set(scores.keys()) == {"regex", "promptguard", "sentinel", "classifier_a"}


def test_read_scores_classifier_a_block_uses_confidence() -> None:
    scores = read_scores(_ctx(classifier_a_verdict="block", classifier_a_confidence=0.8))
    assert scores["classifier_a"] == pytest.approx(0.8)


def test_read_scores_classifier_a_pass_inverts_confidence() -> None:
    scores = read_scores(_ctx(classifier_a_verdict="pass", classifier_a_confidence=0.9))
    assert scores["classifier_a"] == pytest.approx(0.1)


# ── fast-pass tier ───────────────────────────────────────────────────────────


def test_all_four_low_fast_pass() -> None:
    """All four scorers ≤ FAST_PASS_THRESHOLD → process via fast_pass."""
    resp = cmd_handle(
        {},
        _ctx(
            regex=0.05,
            promptguard=0.05,
            sentinel=0.05,
            classifier_a_verdict="pass",
            classifier_a_confidence=0.95,  # block_prob = 0.05
        ),
    )

    assert resp["status"] == "ok"
    ev = resp["events"][0]
    assert ev["type"] == EVENT_TYPE
    assert ev["payload"]["decision"] == DECISION_PROCESS
    assert ev["payload"]["path"] == "fast_pass"
    assert ev["payload"]["llm_score"] is None


def test_sentinel_pass_with_others_pass_fast() -> None:
    """All four scorers ≤ 0.10, decision=process, path=fast_pass."""
    resp = cmd_handle(
        {},
        _ctx(
            regex=0.10,
            promptguard=0.10,
            sentinel=0.10,
            classifier_a_verdict="pass",
            classifier_a_confidence=0.90,  # block_prob = 0.10
        ),
    )

    assert resp["status"] == "ok"
    ev = resp["events"][0]
    assert ev["payload"]["decision"] == DECISION_PROCESS
    assert ev["payload"]["path"] == "fast_pass"
    assert ev["payload"]["llm_score"] is None


def test_one_score_above_fast_pass_threshold_skips_fast_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If any score > FAST_PASS_THRESHOLD (but none ≥ FAST_BLOCK), goes to LLM."""
    # Stub LLM to a low score so we don't need network + so we can detect llm path
    monkeypatch.setattr("run._llm_adjudicate", lambda *a, **kw: 0.2)
    resp = cmd_handle(
        {},
        _ctx(regex=0.05, promptguard=0.20, sentinel=0.05),
    )
    ev = resp["events"][0]
    assert ev["payload"]["path"] == "llm_adjudicated"


# ── fast-block tier ──────────────────────────────────────────────────────────


def test_sentinel_only_block_fast_path() -> None:
    """sentinel=0.95, others low, unknown sender → quarantine via fast_block."""
    resp = cmd_handle(
        {},
        _ctx(
            regex=0.05,
            promptguard=0.05,
            sentinel=0.95,
            classifier_a_verdict="pass",
            classifier_a_confidence=0.95,  # block_prob = 0.05
            trust_level=TRUST_UNKNOWN,
        ),
    )

    assert resp["status"] == "ok"
    ev = resp["events"][0]
    assert ev["payload"]["decision"] == DECISION_QUARANTINE
    assert ev["payload"]["path"] == "fast_block"
    assert ev["payload"]["trust_level"] == TRUST_UNKNOWN
    assert ev["payload"]["llm_score"] is None
    assert ev["payload"]["scores"]["sentinel"] == pytest.approx(0.95)


def test_regex_high_unknown_sender_fast_block() -> None:
    resp = cmd_handle({}, _ctx(regex=0.95, trust_level=TRUST_UNKNOWN))
    ev = resp["events"][0]
    assert ev["payload"]["decision"] == DECISION_QUARANTINE
    assert ev["payload"]["path"] == "fast_block"


def test_promptguard_high_unknown_sender_fast_block() -> None:
    resp = cmd_handle({}, _ctx(promptguard=0.95, trust_level=TRUST_UNKNOWN))
    ev = resp["events"][0]
    assert ev["payload"]["path"] == "fast_block"


def test_trusted_sender_skips_fast_block_routes_to_llm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Trusted sender with high score MUST escalate to LLM, not fast_block."""
    monkeypatch.setattr("run._llm_adjudicate", lambda *a, **kw: 0.3)
    resp = cmd_handle(
        {},
        _ctx(sentinel=0.95, trust_level=TRUST_TRUSTED),
    )
    ev = resp["events"][0]
    assert ev["payload"]["path"] == "llm_adjudicated"
    assert ev["payload"]["trust_level"] == TRUST_TRUSTED


# ── LLM path ─────────────────────────────────────────────────────────────────


def test_llm_high_score_quarantines(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("run._llm_adjudicate", lambda *a, **kw: 0.85)
    resp = cmd_handle({}, _ctx(promptguard=0.40, sentinel=0.40))
    ev = resp["events"][0]
    assert ev["payload"]["decision"] == DECISION_QUARANTINE
    assert ev["payload"]["path"] == "llm_adjudicated"
    assert ev["payload"]["llm_score"] == pytest.approx(0.85)


def test_llm_low_score_processes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("run._llm_adjudicate", lambda *a, **kw: 0.20)
    resp = cmd_handle({}, _ctx(promptguard=0.40, sentinel=0.40))
    ev = resp["events"][0]
    assert ev["payload"]["decision"] == DECISION_PROCESS
    assert ev["payload"]["path"] == "llm_adjudicated"


def test_llm_failure_falls_back_to_quarantine(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(*_a: Any, **_kw: Any) -> float:
        import urllib.error

        raise urllib.error.URLError("boom")

    monkeypatch.setattr("run._llm_adjudicate", _raise)
    resp = cmd_handle({}, _ctx(promptguard=0.40, sentinel=0.40))
    ev = resp["events"][0]
    assert ev["payload"]["decision"] == DECISION_QUARANTINE
    assert ev["payload"]["path"] == "llm_fallback"
    assert ev["payload"]["llm_score"] is None


def test_llm_unparseable_response_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("run._llm_adjudicate", lambda *a, **kw: None)
    resp = cmd_handle({}, _ctx(promptguard=0.40, sentinel=0.40))
    ev = resp["events"][0]
    assert ev["payload"]["path"] == "llm_fallback"
    assert ev["payload"]["decision"] == DECISION_QUARANTINE


# ── emitted payload structure ────────────────────────────────────────────────


def test_sentinel_in_emitted_scores() -> None:
    """Verify the emitted event payload has scores.sentinel with correct value."""
    resp = cmd_handle({}, _ctx(sentinel=0.07))
    ev = resp["events"][0]
    assert "sentinel" in ev["payload"]["scores"]
    assert ev["payload"]["scores"]["sentinel"] == pytest.approx(0.07)
    # All four keys present
    assert set(ev["payload"]["scores"].keys()) == {
        "regex",
        "promptguard",
        "sentinel",
        "classifier_a",
    }


def test_dedupe_key_uses_prefix() -> None:
    resp = cmd_handle({}, _ctx(msg_id="<abc@example.com>"))
    assert resp["events"][0]["dedupe_key"] == f"{DEDUPE_PREFIX}<abc@example.com>"


# ── error paths ──────────────────────────────────────────────────────────────


def test_handle_missing_message_id_errors() -> None:
    resp = cmd_handle({}, {"mail": {}, "scorer": {}})
    assert resp["status"] == "error"
    assert "message_id" in resp["error"]


def test_handle_missing_mail_errors() -> None:
    resp = cmd_handle({}, {"scorer": {}})
    assert resp["status"] == "error"
    assert "context.mail" in resp["error"]


# ── thresholds & defaults pinned (regression guards) ─────────────────────────


def test_thresholds_unchanged() -> None:
    assert FAST_PASS_THRESHOLD == 0.15
    assert FAST_BLOCK_THRESHOLD == 0.90
    assert LLM_BLOCK_THRESHOLD == 0.5


def test_health_reports_5_input_fusion() -> None:
    resp = cmd_health({})
    assert resp["status"] == "ok"
    assert "5-input" in resp["result"]


# ── stdin entrypoint ─────────────────────────────────────────────────────────


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
