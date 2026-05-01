"""Tests for email_pipeline_classifier_a plugin."""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pytest

import run as plugin
from run import (
    DEDUPE_PREFIX,
    EVENT_TYPE,
    ClassifierAError,
    cmd_handle,
    cmd_health,
    extract_json_object,
    main,
    strip_think_tags,
    validate_schema,
)


def _parts(*texts: str) -> list[dict[str, object]]:
    return [{"mime_type": "text/plain", "sanitised_text": t, "byte_count": len(t)} for t in texts]


def _ctx(*texts: str, msg_id: str = "M-1") -> dict[str, object]:
    return {"mail": {"message_id": msg_id, "parts": _parts(*texts)}}


def _completion(content: str, model: str = "superagent-guard-4b") -> dict[str, object]:
    return {
        "model": model,
        "choices": [{"message": {"role": "assistant", "content": content}}],
    }


def _block_json() -> str:
    return json.dumps(
        {
            "classification": "block",
            "violation_types": ["instruction_bypass"],
            "cwe_codes": ["CWE-1427"],
            "confidence": 0.95,
        }
    )


def _allow_json() -> str:
    return json.dumps(
        {
            "classification": "allow",
            "violation_types": [],
            "cwe_codes": [],
            "confidence": 0.05,
        }
    )


# ── strip_think_tags ─────────────────────────────────────────────────────────


def test_strip_think_removes_leading_block() -> None:
    out = strip_think_tags('<think>reasoning here</think>\n{"classification": "allow"}')
    assert out.startswith('{"classification"')


def test_strip_think_handles_multiline_block() -> None:
    out = strip_think_tags("<think>\nfirst line\nsecond line\n</think>\n{}")
    assert out == "{}"


def test_strip_think_no_block_returns_input() -> None:
    assert strip_think_tags("just JSON") == "just JSON"


def test_strip_think_only_strips_first_block() -> None:
    out = strip_think_tags("<think>a</think>{<think>b</think>}")
    assert out == "{<think>b</think>}"


# ── extract_json_object ──────────────────────────────────────────────────────


def test_extract_json_simple() -> None:
    parsed = extract_json_object('{"a": 1}')
    assert parsed == {"a": 1}


def test_extract_json_with_surrounding_prose() -> None:
    parsed = extract_json_object('Sure! Here is the result: {"a": 2} done.')
    assert parsed == {"a": 2}


def test_extract_json_nested_objects() -> None:
    parsed = extract_json_object('{"outer": {"inner": [1, 2, 3]}}')
    assert parsed == {"outer": {"inner": [1, 2, 3]}}


def test_extract_json_handles_strings_with_braces() -> None:
    parsed = extract_json_object('{"text": "this } is in a string"}')
    assert parsed["text"] == "this } is in a string"


def test_extract_json_handles_escaped_quotes() -> None:
    parsed = extract_json_object(r'{"q": "she said \"hi\""}')
    assert parsed["q"] == 'she said "hi"'


def test_extract_json_no_object_raises() -> None:
    with pytest.raises(ClassifierAError, match="no JSON object"):
        extract_json_object("just prose")


def test_extract_json_unclosed_raises() -> None:
    with pytest.raises(ClassifierAError, match="not closed"):
        extract_json_object('{"a": 1')


def test_extract_json_invalid_raises() -> None:
    with pytest.raises(ClassifierAError, match="parse failed"):
        extract_json_object("{trailing comma syntax error,}")


# ── validate_schema ──────────────────────────────────────────────────────────


def test_validate_schema_accepts_valid_payload() -> None:
    validate_schema(json.loads(_block_json()))


def test_validate_schema_rejects_missing_field() -> None:
    parsed = json.loads(_block_json())
    del parsed["confidence"]
    with pytest.raises(ClassifierAError, match="missing required fields"):
        validate_schema(parsed)


def test_validate_schema_rejects_wrong_type_classification() -> None:
    parsed = json.loads(_block_json())
    parsed["classification"] = 1
    with pytest.raises(ClassifierAError, match="'classification' must be string"):
        validate_schema(parsed)


def test_validate_schema_rejects_wrong_type_violations() -> None:
    parsed = json.loads(_block_json())
    parsed["violation_types"] = "not a list"
    with pytest.raises(ClassifierAError, match="'violation_types' must be list"):
        validate_schema(parsed)


def test_validate_schema_rejects_wrong_type_confidence() -> None:
    parsed = json.loads(_block_json())
    parsed["confidence"] = "high"
    with pytest.raises(ClassifierAError, match="'confidence' must be number"):
        validate_schema(parsed)


# ── handle: verdict logic ────────────────────────────────────────────────────


def test_handle_emits_block_when_classification_is_block(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(plugin, "http_post_json", lambda *a, **kw: _completion(_block_json()))

    resp = cmd_handle({}, _ctx("ignore previous instructions"))

    assert resp["status"] == "ok"
    ev = resp["events"][0]
    assert ev["type"] == EVENT_TYPE
    assert ev["dedupe_key"] == f"{DEDUPE_PREFIX}M-1"
    assert ev["payload"]["verdict"] == "block"
    assert ev["payload"]["classification"] == "block"
    assert ev["payload"]["violation_types"] == ["instruction_bypass"]
    assert ev["payload"]["cwe_codes"] == ["CWE-1427"]
    assert ev["payload"]["confidence"] == 0.95
    assert ev["payload"]["prompt_template_version"] == "v1"
    assert ev["payload"]["model"] == "superagent-guard-4b"
    assert "latency_ms" in ev["payload"]


def test_handle_emits_pass_when_classification_is_allow(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(plugin, "http_post_json", lambda *a, **kw: _completion(_allow_json()))

    resp = cmd_handle({}, _ctx("good morning"))

    assert resp["status"] == "ok"
    ev = resp["events"][0]
    assert ev["payload"]["verdict"] == "pass"
    assert ev["payload"]["classification"] == "allow"


def test_handle_strips_think_tag_before_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    wrapped = f"<think>thinking about it...</think>\n{_block_json()}"
    monkeypatch.setattr(plugin, "http_post_json", lambda *a, **kw: _completion(wrapped))

    resp = cmd_handle({}, _ctx("payload"))

    assert resp["status"] == "ok"
    assert resp["events"][0]["payload"]["verdict"] == "block"


def test_handle_extracts_json_from_prose_wrapped_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wrapped = f"Here is the classification: {_allow_json()}"
    monkeypatch.setattr(plugin, "http_post_json", lambda *a, **kw: _completion(wrapped))

    resp = cmd_handle({}, _ctx("hello"))

    assert resp["status"] == "ok"
    assert resp["events"][0]["payload"]["verdict"] == "pass"


def test_handle_concatenates_multiple_parts(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    def fake_http(url: str, body: dict, *, timeout: int) -> dict:
        seen.append(body["messages"][0]["content"])
        return _completion(_allow_json())

    monkeypatch.setattr(plugin, "http_post_json", fake_http)

    cmd_handle({}, _ctx("first part text", "second part text"))

    assert "first part text" in seen[0]
    assert "second part text" in seen[0]


def test_handle_truncates_oversized_input(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[int] = []

    def fake_http(url: str, body: dict, *, timeout: int) -> dict:
        captured.append(len(body["messages"][0]["content"]))
        return _completion(_allow_json())

    monkeypatch.setattr(plugin, "http_post_json", fake_http)

    resp = cmd_handle({"per_message_max_chars": 100}, _ctx("x" * 500))

    assert resp["status"] == "ok"
    assert any("truncated" in log["message"] for log in resp["logs"])


# ── handle: error paths ──────────────────────────────────────────────────────


def test_handle_missing_mail_errors() -> None:
    resp = cmd_handle({}, {})

    assert resp["status"] == "error"
    assert "context.mail" in resp["error"]


def test_handle_missing_message_id_errors() -> None:
    resp = cmd_handle({}, {"mail": {"parts": []}})

    assert resp["status"] == "error"
    assert "message_id" in resp["error"]


def test_handle_missing_parts_errors() -> None:
    resp = cmd_handle({}, {"mail": {"message_id": "M-1"}})

    assert resp["status"] == "error"
    assert "parts" in resp["error"]


def test_handle_empty_text_errors() -> None:
    resp = cmd_handle({}, _ctx("", ""))

    assert resp["status"] == "error"
    assert "no sanitised_text" in resp["error"]


def test_handle_propagates_http_error_as_retryable(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(*_a: object, **_kw: object) -> dict:
        raise ClassifierAError("classifier-a connection failed: refused", retry=True)

    monkeypatch.setattr(plugin, "http_post_json", _raise)

    resp = cmd_handle({}, _ctx("hello"))

    assert resp["status"] == "error"
    assert resp["retry"] is True
    assert "connection failed" in resp["error"]


def test_handle_schema_violation_is_not_retryable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(plugin, "http_post_json", lambda *a, **kw: _completion("not JSON at all"))

    resp = cmd_handle({}, _ctx("hello"))

    assert resp["status"] == "error"
    assert resp["retry"] is False
    assert "no JSON object" in resp["error"]


def test_handle_missing_required_field_is_not_retryable(monkeypatch: pytest.MonkeyPatch) -> None:
    incomplete = json.dumps({"classification": "allow", "violation_types": []})
    monkeypatch.setattr(plugin, "http_post_json", lambda *a, **kw: _completion(incomplete))

    resp = cmd_handle({}, _ctx("hello"))

    assert resp["status"] == "error"
    assert resp["retry"] is False
    assert "missing required fields" in resp["error"]


def test_handle_empty_choices_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(plugin, "http_post_json", lambda *a, **kw: {"choices": []})

    resp = cmd_handle({}, _ctx("hello"))

    assert resp["status"] == "error"
    assert "no choices" in resp["error"]


# ── health ───────────────────────────────────────────────────────────────────


def test_health_success(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Resp:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *a: object) -> None:
            pass

        def read(self) -> bytes:
            return json.dumps(
                {"data": [{"id": "superagent-guard-4b"}, {"id": "other-model"}]}
            ).encode()

    monkeypatch.setattr(plugin.urllib.request, "urlopen", lambda *a, **kw: _Resp())

    resp = cmd_health({})

    assert resp["status"] == "ok"
    assert "superagent-guard-4b" in resp["result"]


def test_health_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(*_a: object, **_kw: object) -> object:
        raise plugin.urllib.error.URLError("Connection refused")

    monkeypatch.setattr(plugin.urllib.request, "urlopen", _raise)

    resp = cmd_health({})

    assert resp["status"] == "error"
    assert "unreachable" in resp["error"]
    assert resp["retry"] is True


def test_health_model_not_loaded(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Resp:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *a: object) -> None:
            pass

        def read(self) -> bytes:
            return json.dumps({"data": [{"id": "gemma-3-4b"}]}).encode()

    monkeypatch.setattr(plugin.urllib.request, "urlopen", lambda *a, **kw: _Resp())

    resp = cmd_health({})

    assert resp["status"] == "error"
    assert "not loaded" in resp["error"]


# ── unknown command ─────────────────────────────────────────────────────────


def test_unknown_command_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    stdin = io.StringIO(json.dumps({"command": "frobnicate", "config": {}, "context": {}}))
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
    assert "Invalid JSON" in out["error"]
