#!/usr/bin/env python3
"""owntracks plugin for Ductile (protocol v2).

Pulls OwnTracks points from an edge buffer (an HTTP export that returns rows after a
cursor) and appends them, raw, to a local spool: one JSON line per row in
<spool_dir>/<device>/<YYYY-MM>.jsonl, the month being when the buffer received the row.
The spool is the source of record; the buffer can be pruned once a row is here.

The cursor is not kept in plugin state. It is the highest buffer id already in the
spool, so a crash between appending and returning can neither skip nor duplicate rows,
and deleting the spool re-pulls whatever the buffer still holds.

Positions never leave the spool: no coordinates in events, logs or state.

Commands:
- poll   : pull every row after the cursor, append, emit location.new_data if any
- health : spool reachability, cursor, file count, and whether the buffer answers
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

SECRET_NAME = "owntracks_pull_token"
DEFAULT_PAGE_LIMIT = 500
DEFAULT_MAX_PAGES = 20
DEFAULT_TIMEOUT_SECONDS = 30
EVENT_TYPE = "location.new_data"

_UNSAFE = re.compile(r"[^A-Za-z0-9._-]")


class PullError(Exception):
    def __init__(self, message: str, retry: bool) -> None:
        super().__init__(message)
        self.retry = retry


# --- envelope -------------------------------------------------------------------------


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(level: str, message: str) -> Dict[str, str]:
    return {"level": level, "message": message}


def ok_response(result: str, events=None, logs=None) -> Dict[str, Any]:
    return {"status": "ok", "result": result, "events": events or [], "logs": logs or []}


def error_response(message: str, retry: bool) -> Dict[str, Any]:
    return {
        "status": "error",
        "error": message,
        "retry": retry,
        "events": [],
        "logs": [log("error", message)],
    }


# --- pure: rows to spool lines ----------------------------------------------------------


def safe_component(name: Any) -> str:
    """A device name as a single path component."""
    cleaned = _UNSAFE.sub("_", str(name or "")).strip("._")
    return cleaned or "unknown"


def month_of(received_at: Any) -> str:
    return datetime.fromtimestamp(int(received_at), tz=timezone.utc).strftime("%Y-%m")


def spool_path(spool_dir: Path, row: Dict[str, Any]) -> Path:
    return spool_dir / safe_component(row.get("device")) / f"{month_of(row['received_at'])}.jsonl"


def spool_line(row: Dict[str, Any]) -> str:
    """One row as one line. body stays the raw string the phone posted."""
    keep = {k: row.get(k) for k in ("id", "user", "device", "type", "tst", "received_at", "body")}
    return json.dumps(keep, separators=(",", ":"), ensure_ascii=False) + "\n"


def validate_rows(rows: Any, after: int) -> List[Dict[str, Any]]:
    if not isinstance(rows, list):
        raise PullError("export answered without a rows list", retry=True)
    last = after
    for row in rows:
        if not isinstance(row, dict):
            raise PullError("export row is not an object", retry=False)
        for key in ("id", "received_at", "body"):
            if key not in row:
                raise PullError(f"export row lacks {key}", retry=False)
        if not isinstance(row["id"], int) or row["id"] <= last:
            raise PullError("export rows are not in ascending id order after the cursor", retry=False)
        last = row["id"]
    return rows


# --- spool edge ---------------------------------------------------------------------------


def repair_tail(path: Path) -> None:
    """Cut a torn last line (a crash mid-append) back to the last newline."""
    with open(path, "rb+") as fh:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        if size == 0:
            return
        fh.seek(size - 1)
        if fh.read(1) == b"\n":
            return
        chunk = 65536
        pos = size
        while pos > 0:
            start = max(0, pos - chunk)
            fh.seek(start)
            data = fh.read(pos - start)
            cut = data.rfind(b"\n")
            if cut >= 0:
                fh.truncate(start + cut + 1)
                break
            pos = start
        else:
            fh.truncate(0)
        fh.flush()
        os.fsync(fh.fileno())


def last_id_in(path: Path) -> Optional[int]:
    """The id on a file's last line. Reads backwards in growing chunks."""
    with open(path, "rb") as fh:
        fh.seek(0, os.SEEK_END)
        end = fh.tell()
        if end == 0:
            return None
        size = min(end, 4096)
        while True:
            fh.seek(end - size)
            body = fh.read(size).rstrip(b"\n")
            newline = body.rfind(b"\n")
            if newline >= 0 or size == end:
                last = body[newline + 1:]
                return int(json.loads(last)["id"]) if last.strip() else None
            size = min(end, size * 2)


def spool_files(spool_dir: Path) -> List[Path]:
    if not spool_dir.is_dir():
        return []
    return sorted(p for p in spool_dir.glob("*/*.jsonl") if p.is_file())


def spool_cursor(spool_dir: Path) -> int:
    """The highest buffer id in the spool, after repairing torn tails.

    Rows are appended in id order, so each file's last line is its highest id. Two newest
    files per device are read, in case a row stamped just before a month boundary got a
    higher id than one stamped just after it on another edge node.
    """
    cursor = 0
    if not spool_dir.is_dir():
        return cursor
    for device_dir in sorted(p for p in spool_dir.iterdir() if p.is_dir()):
        for path in sorted(device_dir.glob("*.jsonl"))[-2:]:
            repair_tail(path)
            last = last_id_in(path)
            if last is not None and last > cursor:
                cursor = last
    return cursor


def append_rows(spool_dir: Path, rows: Iterable[Dict[str, Any]]) -> List[Path]:
    """Append rows in id order. A file is fsynced before another is written to, so what
    survives a crash is always a prefix of the rows."""
    touched: List[Path] = []
    current: Optional[Path] = None
    fh = None
    try:
        for row in rows:
            path = spool_path(spool_dir, row)
            if path != current:
                if fh is not None:
                    fh.flush()
                    os.fsync(fh.fileno())
                    fh.close()
                created_dir = not path.parent.exists()
                path.parent.mkdir(parents=True, exist_ok=True)
                if created_dir:
                    fsync_dir(path.parent.parent)
                created_file = not path.exists()
                fh = open(path, "a", encoding="utf-8")
                if created_file:
                    fsync_dir(path.parent)
                current = path
                if path not in touched:
                    touched.append(path)
            fh.write(spool_line(row))
        if fh is not None:
            fh.flush()
            os.fsync(fh.fileno())
    finally:
        if fh is not None:
            fh.close()
    return touched


def fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


# --- buffer edge --------------------------------------------------------------------------


def fetch_page(export_url: str, token: str, after: int, limit: int, timeout: int) -> Dict[str, Any]:
    query = urllib.parse.urlencode({"after": after, "limit": limit})
    sep = "&" if "?" in export_url else "?"
    req = urllib.request.Request(
        f"{export_url}{sep}{query}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json",
                 "User-Agent": "ductile-owntracks/0.1"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            raise PullError(f"export refused the pull token (HTTP {exc.code})", retry=False)
        if exc.code == 400:
            raise PullError("export rejected the cursor or limit (HTTP 400)", retry=False)
        raise PullError(f"export answered HTTP {exc.code}", retry=exc.code >= 500 or exc.code == 429)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise PullError(f"export unreachable: {exc}", retry=True)
    except ValueError:
        raise PullError("export answered with something that is not JSON", retry=True)


# --- commands -----------------------------------------------------------------------------


def read_settings(request: Dict[str, Any]) -> Tuple[str, Path, str, int, int, int]:
    config = request.get("config") or {}
    secrets = request.get("secrets") if isinstance(request.get("secrets"), dict) else {}
    export_url = str(config.get("export_url", "")).strip()
    spool_dir = str(config.get("spool_dir", "")).strip()
    if not export_url:
        raise PullError("Missing required config key: export_url", retry=False)
    if not spool_dir:
        raise PullError("Missing required config key: spool_dir", retry=False)
    token = str(secrets.get(SECRET_NAME) or "").strip()
    if not token:
        raise PullError(f"No pull token: grant the '{SECRET_NAME}' vault secret to this plugin",
                        retry=False)
    limit = int(config.get("page_limit", DEFAULT_PAGE_LIMIT))
    max_pages = int(config.get("max_pages", DEFAULT_MAX_PAGES))
    timeout = int(config.get("request_timeout_seconds", DEFAULT_TIMEOUT_SECONDS))
    return export_url, Path(spool_dir), token, limit, max_pages, timeout


def poll_command(request: Dict[str, Any]) -> Dict[str, Any]:
    try:
        export_url, spool_dir, token, limit, max_pages, timeout = read_settings(request)
        spool_dir.mkdir(parents=True, exist_ok=True)
        start = spool_cursor(spool_dir)
        cursor = start
        pulled = 0
        pages = 0
        more = True
        devices: List[str] = []
        files: List[str] = []
        types: Dict[str, int] = {}
        first_id = first_received = last_received = None
        while more and pages < max_pages:
            page = fetch_page(export_url, token, cursor, limit, timeout)
            rows = validate_rows(page.get("rows"), cursor)
            pages += 1
            if not rows:
                break
            for path in append_rows(spool_dir, rows):
                rel = str(path.relative_to(spool_dir))
                if rel not in files:
                    files.append(rel)
            for row in rows:
                device = safe_component(row.get("device"))
                if device not in devices:
                    devices.append(device)
                kind = str(row.get("type") or "unknown")
                types[kind] = types.get(kind, 0) + 1
                if first_id is None:
                    first_id, first_received = row["id"], row["received_at"]
                last_received = row["received_at"]
            pulled += len(rows)
            cursor = rows[-1]["id"]
            more = bool(page.get("more"))
    except PullError as exc:
        return error_response(str(exc), retry=exc.retry)
    except OSError as exc:
        return error_response(f"spool write failed: {exc}", retry=True)

    summary = f"pulled {pulled} row(s) in {pages} page(s), cursor {start} -> {cursor}"
    logs = [log("info", summary)]
    if more and pages >= max_pages:
        logs.append(log("info", "page budget spent with rows remaining; the next poll continues"))
    if pulled == 0:
        return ok_response(f"no new rows, cursor {cursor}", logs=logs)

    payload = {
        "source": "owntracks",
        "rows": pulled,
        "types": types,
        "first_id": first_id,
        "last_id": cursor,
        "devices": devices,
        "files": files,
        "received_from": datetime.fromtimestamp(first_received, tz=timezone.utc).isoformat(),
        "received_to": datetime.fromtimestamp(last_received, tz=timezone.utc).isoformat(),
        "backlog": more,
        "detected_at": now_iso(),
    }
    return ok_response(summary, events=[{"type": EVENT_TYPE, "payload": payload}], logs=logs)


def health_command(request: Dict[str, Any]) -> Dict[str, Any]:
    try:
        export_url, spool_dir, token, _, _, timeout = read_settings(request)
    except PullError as exc:
        return error_response(str(exc), retry=exc.retry)

    files = spool_files(spool_dir)
    writable = spool_dir.is_dir() and os.access(spool_dir, os.W_OK)
    cursor = 0
    try:
        cursor = spool_cursor(spool_dir)
    except (OSError, ValueError, KeyError) as exc:
        return error_response(f"spool unreadable: {exc}", retry=True)

    buffer_ok = True
    backlog = None
    problem = None
    try:
        page = fetch_page(export_url, token, cursor, 1, timeout)
        backlog = bool(page.get("rows"))
    except PullError as exc:
        buffer_ok = False
        problem = str(exc)

    healthy = writable and buffer_ok
    payload = {
        "healthy": healthy,
        "checked_at": now_iso(),
        "spool": {"writable": writable, "files": len(files), "cursor": cursor},
        "buffer": {"reachable": buffer_ok, "rows_waiting": backlog, "problem": problem},
    }
    result = f"healthy={healthy} cursor={cursor} files={len(files)} rows_waiting={backlog}"
    return ok_response(result, events=[{"type": "owntracks.health", "payload": payload}],
                       logs=[log("info", result)])


def main() -> None:
    try:
        request = json.load(sys.stdin)
    except Exception as exc:  # noqa: BLE001
        json.dump(error_response(f"Invalid request JSON: {exc}", retry=False), sys.stdout)
        return

    command = str(request.get("command", "")).strip().lower()
    if command == "poll":
        response = poll_command(request)
    elif command == "health":
        response = health_command(request)
    else:
        response = error_response(f"Unknown command: {command}", retry=False)
    json.dump(response, sys.stdout)


if __name__ == "__main__":
    main()
