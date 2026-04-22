#!/usr/bin/env python3
"""Agent handshake plugin for Ductile Gateway (protocol v2).

Validates a challenge-response proof-of-work from an autonomous agent
and logs the agent's identity. Designed to be deployed as an alias with
instance-specific challenge/salt configuration.

Proof algorithm: SHA256(challenge + salt) must equal the submitted proof.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict


def error_response(message: str, retry: bool = False) -> Dict[str, Any]:
    return {
        "status": "error",
        "error": message,
        "retry": retry,
        "logs": [{"level": "error", "message": message}],
    }


def ok_response(result: str, events: list | None = None, state_updates: dict | None = None) -> Dict[str, Any]:
    resp: Dict[str, Any] = {
        "status": "ok",
        "result": result,
        "logs": [{"level": "info", "message": result}],
    }
    if events:
        resp["events"] = events
    if state_updates:
        resp["state_updates"] = state_updates
    return resp


def compute_proof(challenge: str, salt: str) -> str:
    return hashlib.sha256((challenge + salt).encode()).hexdigest()


def handle_command(config: Dict[str, Any], payload: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    challenge = str(config.get("challenge") or "").strip()
    salt = str(config.get("salt") or "").strip()
    log_path = str(config.get("log_path") or "").strip()

    if not challenge or not salt:
        return error_response("Plugin misconfigured: challenge and salt are required", retry=False)

    # Validate required fields
    email = str(payload.get("email") or "").strip()
    submitted_challenge = str(payload.get("challenge") or "").strip()
    proof = str(payload.get("proof") or "").strip()
    consent = payload.get("consent")
    agent = str(payload.get("agent") or "unknown").strip()

    if not email:
        return error_response("Missing required field: email", retry=False)
    if not submitted_challenge:
        return error_response("Missing required field: challenge", retry=False)
    if not proof:
        return error_response("Missing required field: proof", retry=False)
    if consent is not True:
        return error_response("consent must be true", retry=False)

    # Validate challenge matches
    if submitted_challenge != challenge:
        return error_response(f"Challenge mismatch", retry=False)

    # Validate proof
    expected = compute_proof(challenge, salt)
    if proof != expected:
        return error_response("Proof verification failed", retry=False)

    # Log the registration
    timestamp = datetime.now(timezone.utc).isoformat()
    log_entry = {
        "timestamp": timestamp,
        "email": email,
        "agent": agent,
        "challenge": challenge,
    }

    if log_path:
        try:
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            with open(log_path, "a") as f:
                f.write(json.dumps(log_entry) + "\n")
        except OSError as e:
            # Log failure is non-fatal — registration still succeeds
            pass

    result = f"Agent registered: {email} (agent: {agent})"

    try:
        prior_total = int(state.get("total_registrations") or 0)
    except (TypeError, ValueError):
        prior_total = 0
    total_registrations = prior_total + 1

    return ok_response(
        result=result,
        events=[{
            "type": "agent_handshake.registered",
            "payload": {
                "email": email,
                "agent": agent,
                "timestamp": timestamp,
                "challenge": challenge,
                "message": result,
                "text": result,
            },
        }],
        state_updates={
            "last_registration": timestamp,
            "total_registrations": total_registrations,
        },
    )


def handle_health(config: Dict[str, Any]) -> Dict[str, Any]:
    challenge = str(config.get("challenge") or "").strip()
    salt = str(config.get("salt") or "").strip()
    if not challenge or not salt:
        return error_response("Missing required config: challenge and/or salt", retry=False)
    return ok_response(f"agent_handshake configured, challenge={challenge!r}")


def main() -> None:
    try:
        request = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        json.dump(error_response(f"Invalid JSON input: {e}", retry=False), sys.stdout)
        sys.stdout.write("\n")
        sys.stdout.flush()
        sys.exit(1)

    command = request.get("command", "")
    config = request.get("config") if isinstance(request.get("config"), dict) else {}
    state = request.get("state") if isinstance(request.get("state"), dict) else {}
    event = request.get("event", {})
    payload = event.get("payload", {}) if isinstance(event, dict) else {}

    if command == "handle":
        response = handle_command(config, payload, state)
    elif command == "health":
        response = handle_health(config)
    else:
        response = error_response(f"Unknown command: '{command}'. Supported: handle, health")

    json.dump(response, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
