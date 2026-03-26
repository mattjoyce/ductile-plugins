#!/usr/bin/env python3
"""claude_harvest plugin for Ductile Gateway (protocol v2).

Harvests Zettelkasten notes from Claude Code sessions using the /learn command.

Commands:
  poll   — Scanner: find projects with sessions changed since last run,
            trigger one claude-harvest-worker pipeline job per project.
  handle — Worker: process unprocessed sessions for a specific project
            (project/cwd from payload), output Discord-ready summary.
  health — Verify dependencies are in place.

Protocol v2: reads JSON from stdin, writes JSON to stdout.
"""
from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Protocol helpers
# ---------------------------------------------------------------------------

def ok(result: str, logs: list[dict] | None = None) -> dict:
    return {
        "status": "ok",
        "result": result,
        "logs": logs or [{"level": "info", "message": result[:200]}],
    }


def error(message: str, retry: bool = False) -> dict:
    return {
        "status": "error",
        "error": message,
        "retry": retry,
        "logs": [{"level": "error", "message": message}],
    }


# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------

def cfg(config: dict, key: str, default: Any) -> Any:
    return config.get(key, default)


def resolve_paths(config: dict) -> dict:
    home = Path.home()
    return {
        "projects_dir": home / ".claude" / "projects",
        "state_file": Path(cfg(config, "state_file", str(home / ".config/claude-harvest/state.json"))).expanduser(),
        "learn_cmd": Path(cfg(config, "learn_cmd", str(home / ".claude/commands/learn.md"))).expanduser(),
        "zk_env": Path(cfg(config, "zk_env", str(home / ".config/ZK/.env"))).expanduser(),
        "api_url": cfg(config, "api_url", "http://localhost:8081"),
        "api_token": cfg(config, "api_token", os.environ.get("DUCTILE_LOCAL_TOKEN", "")),
        "worker_pipeline": cfg(config, "worker_pipeline", "claude-harvest-worker"),
        "min_size_bytes": int(cfg(config, "min_size_bytes", 50_000)),
        "max_age_days": int(cfg(config, "max_age_days", 30)),
        "min_age_minutes": int(cfg(config, "min_age_minutes", 60)),
        "max_per_project": int(cfg(config, "max_per_project", 5)),
        "session_timeout": int(cfg(config, "session_timeout_seconds", 120)),
        "model": cfg(config, "model", "claude-sonnet-4-6"),
    }


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def load_state(state_file: Path) -> dict:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    if state_file.exists():
        try:
            return json.loads(state_file.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {"processed": {}, "last_scan_at": None}


def save_session(state_file: Path, session_id: str, entry: dict) -> None:
    """Append one processed session to state with file locking for concurrency safety."""
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with open(state_file, "a+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            f.seek(0)
            content = f.read()
            try:
                state = json.loads(content) if content.strip() else {"processed": {}}
            except json.JSONDecodeError:
                state = {"processed": {}}
            state.setdefault("processed", {})[session_id] = entry
            f.seek(0)
            f.truncate()
            f.write(json.dumps(state, indent=2))
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def save_last_scan(state_file: Path, ts: str) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with open(state_file, "a+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            f.seek(0)
            content = f.read()
            try:
                state = json.loads(content) if content.strip() else {}
            except json.JSONDecodeError:
                state = {}
            state["last_scan_at"] = ts
            f.seek(0)
            f.truncate()
            f.write(json.dumps(state, indent=2))
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


# ---------------------------------------------------------------------------
# ZK helpers
# ---------------------------------------------------------------------------

def get_zk_notes_dir(zk_env: Path) -> Path | None:
    if not zk_env.exists():
        return None
    for line in zk_env.read_text().splitlines():
        line = line.strip()
        if line.startswith("ZK_VAULT_PATH="):
            vault = Path(line.split("=", 1)[1].strip().strip('"').strip("'")).expanduser()
            return vault / "notes"
    return None


def count_notes(notes_dir: Path | None) -> int:
    if notes_dir is None or not notes_dir.is_dir():
        return 0
    return sum(1 for _ in notes_dir.glob("*.md"))


# ---------------------------------------------------------------------------
# Session discovery
# ---------------------------------------------------------------------------

def get_session_cwd(jsonl_path: Path) -> str | None:
    try:
        with open(jsonl_path) as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    cwd = obj.get("cwd")
                    if cwd:
                        return cwd
                except json.JSONDecodeError:
                    continue
    except OSError:
        pass
    return None


def find_changed_projects(p: dict, processed: dict, last_scan: datetime | None) -> dict[str, str]:
    """Return {slug: cwd} for projects with new unprocessed sessions since last_scan."""
    now = datetime.now()
    max_age_cutoff = now - timedelta(days=p["max_age_days"])
    min_age_cutoff = now - timedelta(minutes=p["min_age_minutes"])
    changed: dict[str, str] = {}

    for project_dir in p["projects_dir"].iterdir():
        if not project_dir.is_dir():
            continue
        slug = project_dir.name
        for jsonl in project_dir.glob("*.jsonl"):
            session_id = jsonl.stem
            if session_id in processed:
                continue
            stat = jsonl.stat()
            if stat.st_size < p["min_size_bytes"]:
                continue
            mtime = datetime.fromtimestamp(stat.st_mtime)
            if mtime < max_age_cutoff or mtime > min_age_cutoff:
                continue
            if last_scan and mtime <= last_scan:
                continue
            if slug not in changed:
                cwd = get_session_cwd(jsonl) or str(Path.home())
                changed[slug] = cwd
    return changed


def find_project_sessions(p: dict, slug: str, processed: dict) -> list[tuple[str, datetime]]:
    """Return unprocessed (session_id, mtime) for a project, oldest first."""
    project_dir = p["projects_dir"] / slug
    if not project_dir.is_dir():
        return []
    now = datetime.now()
    max_age_cutoff = now - timedelta(days=p["max_age_days"])
    min_age_cutoff = now - timedelta(minutes=p["min_age_minutes"])
    sessions = []
    for jsonl in project_dir.glob("*.jsonl"):
        session_id = jsonl.stem
        if session_id in processed:
            continue
        stat = jsonl.stat()
        if stat.st_size < p["min_size_bytes"]:
            continue
        mtime = datetime.fromtimestamp(stat.st_mtime)
        if mtime < max_age_cutoff or mtime > min_age_cutoff:
            continue
        sessions.append((session_id, mtime))
    return sorted(sessions, key=lambda x: x[1])


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def handle_poll(config: dict) -> dict:
    """Scanner: find changed projects, trigger one worker pipeline job each."""
    p = resolve_paths(config)
    state = load_state(p["state_file"])
    processed = state.get("processed", {})
    last_scan_raw = state.get("last_scan_at")
    last_scan = datetime.fromisoformat(last_scan_raw) if last_scan_raw else None

    changed = find_changed_projects(p, processed, last_scan)
    now = datetime.now()

    if not changed:
        save_last_scan(p["state_file"], now.isoformat())
        return ok("Claude Harvest Scan: no changed projects since last run.")

    logs = []
    queued = []
    for slug, cwd in sorted(changed.items()):
        label = cwd.replace(str(Path.home()), "~")
        triggered, msg = trigger_worker_pipeline(p, slug, cwd)
        level = "info" if triggered else "warn"
        logs.append({"level": level, "message": f"{label}: {msg}"})
        if triggered:
            queued.append(label)

    save_last_scan(p["state_file"], now.isoformat())

    summary = f"Claude Harvest Scan: queued {len(queued)}/{len(changed)} project(s): {', '.join(queued)}"
    return ok(summary, logs)


def trigger_worker_pipeline(p: dict, project: str, cwd: str) -> tuple[bool, str]:
    url = f"{p['api_url']}/pipeline/{p['worker_pipeline']}"
    payload = json.dumps({"payload": {"project": project, "cwd": cwd}}).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {p['api_token']}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
            job_id = body.get("job_id", "?")[:8]
            return True, f"queued job {job_id}..."
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}: {e.read().decode()[:100]}"
    except Exception as e:
        return False, f"error: {e}"


def handle_handle(config: dict, payload: dict) -> dict:
    """Worker: process sessions for one project, return Discord-ready summary."""
    p = resolve_paths(config)
    project = payload.get("project", "")
    cwd = payload.get("cwd", str(Path.home()))

    if not project:
        return error("payload.project is required")
    if not p["learn_cmd"].exists():
        return error(f"learn command not found at {p['learn_cmd']}")

    learn_prompt = p["learn_cmd"].read_text()
    notes_dir = get_zk_notes_dir(p["zk_env"])
    state = load_state(p["state_file"])
    processed = state.get("processed", {})

    sessions = find_project_sessions(p, project, processed)[: p["max_per_project"]]
    project_label = cwd.replace(str(Path.home()), "~")
    date_str = datetime.now().strftime("%Y-%m-%d")

    if not sessions:
        msg = f"**Claude Harvest** — {project_label} — {date_str}\nNo new sessions."
        return ok(msg)

    results = []
    work_dir = cwd if Path(cwd).is_dir() else str(Path.home())
    logs = []

    for session_id, mtime in sessions:
        notes_before = count_notes(notes_dir)
        success, output = run_learn(session_id, work_dir, learn_prompt, p["session_timeout"], p["model"])
        notes_after = count_notes(notes_dir)
        notes_written = max(0, notes_after - notes_before)

        results.append({
            "session_id": session_id[:8],
            "mtime": mtime.strftime("%Y-%m-%d %H:%M"),
            "notes_written": notes_written,
            "success": success,
        })
        detail = f" ({output[:120]})" if not success and output else ""
        logs.append({
            "level": "info" if success else "warn",
            "message": f"{session_id[:8]}: {'ok' if success else 'failed'}, +{notes_written} notes{detail}",
        })

        save_session(p["state_file"], session_id, {
            "processed_at": datetime.now().isoformat(),
            "project": project,
            "cwd": cwd,
            "success": success,
            "notes_written": notes_written,
        })

    # Remaining sessions after this run
    updated_state = load_state(p["state_file"])
    remaining = len(find_project_sessions(p, project, updated_state.get("processed", {})))

    total_notes = sum(r["notes_written"] for r in results)
    success_count = sum(1 for r in results if r["success"])

    lines = [f"**Claude Harvest** — {project_label} — {date_str}"]
    lines.append(f"{success_count}/{len(results)} sessions · {total_notes} ZK notes")
    if remaining:
        lines.append(f"({remaining} more queued for next run)")
    lines.append("")
    for r in results:
        status = "✓" if r["success"] else "✗"
        note_str = f"+{r['notes_written']} notes" if r["notes_written"] else "no new notes"
        lines.append(f"{status} `{r['session_id']}` {r['mtime']} — {note_str}")

    summary = "\n".join(lines)
    if success_count == 0:
        return {
            "status": "error",
            "error": f"all {len(results)} session(s) failed — check PATH/tooling in the ductile service environment",
            "retry": False,
            "result": summary,
            "logs": logs,
        }
    return ok(summary, logs)


def run_learn(session_id: str, work_dir: str, learn_prompt: str, timeout: int, model: str) -> tuple[bool, str]:
    cmd = [
        "claude",
        "--resume", session_id,
        "--fork-session",
        "--no-session-persistence",
        "--dangerously-skip-permissions",
        "--model", model,
        "-p", learn_prompt,
    ]
    try:
        result = subprocess.run(cmd, cwd=work_dir, capture_output=True, text=True, timeout=timeout)
        if result.returncode == 0:
            return True, result.stdout.strip()
        return False, result.stderr.strip() or f"exit code {result.returncode}"
    except subprocess.TimeoutExpired:
        return False, f"timed out after {timeout}s"


def find_bun() -> str | None:
    """Return path to bun binary, checking known install locations before PATH."""
    known = [
        Path.home() / ".bun" / "bin" / "bun",
        Path("/usr/local/bin/bun"),
        Path("/usr/bin/bun"),
    ]
    for candidate in known:
        if candidate.exists() and os.access(str(candidate), os.X_OK):
            return str(candidate)
    import shutil
    return shutil.which("bun")


def handle_health(config: dict) -> dict:
    p = resolve_paths(config)
    issues = []

    if not p["learn_cmd"].exists():
        issues.append(f"learn command not found: {p['learn_cmd']}")
    if not p["zk_env"].exists():
        issues.append(f"ZK env not found: {p['zk_env']}")
    if not p["api_token"]:
        issues.append("api_token not configured and DUCTILE_LOCAL_TOKEN not set")

    bun_path = find_bun()
    if not bun_path:
        issues.append("bun not found — required by SessionEnd hook (checked ~/.bun/bin/bun, /usr/local/bin/bun, /usr/bin/bun, PATH)")

    # Quick API ping
    try:
        req = urllib.request.Request(
            f"{p['api_url']}/healthz",
            headers={"Authorization": f"Bearer {p['api_token']}"},
        )
        urllib.request.urlopen(req, timeout=3)
    except Exception as e:
        issues.append(f"ductile API unreachable at {p['api_url']}: {e}")

    if issues:
        return error("claude_harvest health check failed: " + "; ".join(issues))
    return ok("claude_harvest ok — learn command, ZK vault, and ductile API all reachable")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    try:
        request = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        json.dump(error(f"Invalid JSON input: {e}"), sys.stdout)
        sys.stdout.write("\n")
        sys.exit(1)

    command = request.get("command", "")
    config = request.get("config") or {}
    event = request.get("event") or {}
    payload = event.get("payload") or {}

    if command == "poll":
        response = handle_poll(config)
    elif command == "handle":
        response = handle_handle(config, payload)
    elif command == "health":
        response = handle_health(config)
    else:
        response = error(f"Unknown command: '{command}'. Supported: poll, handle, health")

    json.dump(response, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
