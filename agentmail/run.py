#!/usr/bin/env python3
"""agentmail plugin for Ductile (protocol v2).

Saves the messages that arrive in an AgentMail inbox (agentmail.to) into a local spool,
one folder per message, and stops there. What a message means, and whether to act on it,
is a later step's business: this plugin reads the inbox and writes only under spool_dir.

A message's folder, <spool_dir>/<stem>, where <stem> is the first 16 hex digits of the
SHA-256 of the message id, holds:

    message.json    the message exactly as AgentMail returns it: headers, from, to,
                    subject, text, html, extracted_text, attachments, and the
                    authentication results, recorded as data and not judged here
    attachments/    each attachment, byte-exact
    manifest.json   the message id, when it was saved, and each attachment's
                    name, type, size and sha256

A folder is built under a temporary name and renamed into place only when every part of
it is written and checked, so a reader never sees half a message. The folder is the
plugin's memory: a message whose folder exists is not fetched again, so retries and
crashes neither skip nor duplicate one. Nothing in the inbox is deleted or marked.

Message content stays in the spool: events, logs and results carry ids, counts and
folder names only.

Commands:
- poll   : save every message not yet in the spool, emit agentmail.message.saved per new folder
- health : spool writability, folder count, and whether the inbox answers the key
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

SECRET_NAME = "agentmail_api_key"
DEFAULT_BASE_URL = "https://api.agentmail.to"
# 50 is the most AgentMail returns a page, so the fewest round trips.
DEFAULT_PAGE_SIZE = 50
# A poll that saves at most this many messages stays short; the next poll continues.
DEFAULT_MAX_MESSAGES = 20
# Every call fails fast rather than hanging; 30 s outlasts a slow PDF over a slow link.
DEFAULT_TIMEOUT_SECONDS = 30
EVENT_TYPE = "agentmail.message.saved"
MESSAGE_FILE = "message.json"
MANIFEST_FILE = "manifest.json"
ATTACHMENT_DIR = "attachments"
TMP_PREFIX = ".partial-"
# A filesystem's name limit is 255 bytes; this keeps room for a suffix.
MAX_NAME_BYTES = 200


class MailError(Exception):
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


# --- pure -------------------------------------------------------------------------------


def stem(message_id: str) -> str:
    """A message's folder name: no character of the id can reach a path."""
    return hashlib.sha256(message_id.encode("utf-8")).hexdigest()[:16]


def safe_name(name: Any) -> str:
    """A filename with no directory, control character or excess length in it, so an
    attachment cannot write outside its folder or make a write fail. A long name keeps
    its extension and gains a hash of the whole name, so two long names stay two files."""
    base = Path(str(name or "").replace("\\", "/")).name
    base = "".join(c for c in base if c.isprintable()).strip().lstrip(".")
    if len(base.encode("utf-8")) > MAX_NAME_BYTES:
        ext = Path(base).suffix[:16]
        digest = hashlib.sha256(base.encode("utf-8")).hexdigest()[:12]
        head = base[: -len(ext)] if ext else base
        head = head.encode("utf-8")[: MAX_NAME_BYTES - len(ext) - 13].decode("utf-8", "ignore")
        base = f"{head}-{digest}{ext}"
    return base or "attachment"


def unique_name(name: str, taken: set) -> str:
    """name, or name with -2, -3 ... before its extension when another attachment has it."""
    if name not in taken:
        return name
    path = Path(name)
    n = 2
    while f"{path.stem}-{n}{path.suffix}" in taken:
        n += 1
    return f"{path.stem}-{n}{path.suffix}"


def check(body: bytes, expected: Any, content_type: str, filename: str) -> Optional[str]:
    """Why these bytes are not the attachment, or None when they are."""
    if isinstance(expected, int) and len(body) != expected:
        return f"{len(body)} bytes, the inbox says {expected}"
    if (content_type == "application/pdf" or filename.lower().endswith(".pdf")) and not body.startswith(b"%PDF"):
        return "not a PDF: no %PDF header"
    return None


# --- inbox edge -------------------------------------------------------------------------


class Inbox:
    """The few AgentMail calls the plugin needs, each with a deadline."""

    def __init__(self, base_url: str, inbox: str, key: str, timeout: int) -> None:
        self.base = base_url.rstrip("/")
        self.inbox = urllib.parse.quote(inbox, safe="@")
        self.key = key
        self.timeout = timeout

    def _get(self, url: str, auth: bool = True) -> bytes:
        headers = {"User-Agent": "ductile-agentmail/0.1"}
        if auth:
            headers["Authorization"] = f"Bearer {self.key}"
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            exc.close()
            if exc.code in (401, 403):
                raise MailError(f"AgentMail refused the key (HTTP {exc.code})", retry=False)
            raise MailError(f"AgentMail answered HTTP {exc.code}", retry=exc.code >= 500 or exc.code == 429)
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise MailError(f"AgentMail unreachable: {exc}", retry=True)

    def _json(self, url: str) -> Dict[str, Any]:
        try:
            # strict=False: a message body may carry control characters inside its strings.
            return json.loads(self._get(url).decode("utf-8"), strict=False)
        except (ValueError, UnicodeDecodeError):
            raise MailError("AgentMail answered with something that is not JSON", retry=True)

    def message_ids(self, page_size: int) -> Iterator[str]:
        token: Optional[str] = None
        while True:
            query = {"limit": page_size}
            if token:
                query["page_token"] = token
            page = self._json(f"{self.base}/v0/inboxes/{self.inbox}/messages?{urllib.parse.urlencode(query)}")
            for msg in page.get("messages") or []:
                if isinstance(msg, dict) and msg.get("message_id"):
                    yield str(msg["message_id"])
            token = page.get("next_page_token")
            if not token:
                return

    def message(self, message_id: str) -> Tuple[bytes, Dict[str, Any]]:
        """The message's raw JSON bytes, kept as AgentMail sent them, and the parsed form."""
        mid = urllib.parse.quote(message_id, safe="")
        raw = self._get(f"{self.base}/v0/inboxes/{self.inbox}/messages/{mid}")
        try:
            return raw, json.loads(raw.decode("utf-8"), strict=False)
        except (ValueError, UnicodeDecodeError):
            raise MailError("AgentMail answered a message with something that is not JSON", retry=True)

    def attachment(self, message_id: str, attachment_id: str) -> bytes:
        mid = urllib.parse.quote(message_id, safe="")
        aid = urllib.parse.quote(attachment_id, safe="")
        meta = self._json(f"{self.base}/v0/inboxes/{self.inbox}/messages/{mid}/attachments/{aid}")
        url = meta.get("download_url")
        if not url:
            raise MailError("AgentMail gave no download link for an attachment", retry=True)
        # The download link is signed and carries its own authority: the key never goes to it.
        return self._get(str(url), auth=False)


# --- spool edge ---------------------------------------------------------------------------


def fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_file(path: Path, body: bytes) -> None:
    with open(path, "wb") as fh:
        fh.write(body)
        fh.flush()
        os.fsync(fh.fileno())


def clear_partials(spool_dir: Path) -> int:
    """Remove folders a crashed poll left half built. Nothing reads them."""
    removed = 0
    for path in spool_dir.glob(f"{TMP_PREFIX}*"):
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
            removed += 1
    return removed


def saved_folders(spool_dir: Path) -> List[Path]:
    if not spool_dir.is_dir():
        return []
    return sorted(p for p in spool_dir.iterdir() if p.is_dir() and not p.name.startswith(TMP_PREFIX))


def save_message(inbox: Inbox, spool_dir: Path, message_id: str) -> Dict[str, Any]:
    """Build one message's folder under a temporary name, then rename it into place.
    Raises MailError, leaving nothing behind, when any part cannot be fetched or checked."""
    raw, msg = inbox.message(message_id)
    final = spool_dir / stem(message_id)
    tmp = spool_dir / f"{TMP_PREFIX}{stem(message_id)}-{os.getpid()}"
    shutil.rmtree(tmp, ignore_errors=True)
    try:
        (tmp / ATTACHMENT_DIR).mkdir(parents=True)
        write_file(tmp / MESSAGE_FILE, raw)
        taken: set = set()
        files: List[Dict[str, Any]] = []
        for att in msg.get("attachments") or []:
            if not isinstance(att, dict) or not att.get("attachment_id"):
                raise MailError("a message lists an attachment with no id", retry=False)
            name = unique_name(safe_name(att.get("filename") or "attachment"), taken)
            taken.add(name)
            body = inbox.attachment(message_id, str(att["attachment_id"]))
            why = check(body, att.get("size"), str(att.get("content_type") or ""), name)
            if why:
                raise MailError(f"attachment {name}: {why}", retry=True)
            write_file(tmp / ATTACHMENT_DIR / name, body)
            files.append({
                "attachment_id": att["attachment_id"],
                "filename": name,
                "original_filename": att.get("filename"),
                "content_type": att.get("content_type"),
                "bytes": len(body),
                "sha256": hashlib.sha256(body).hexdigest(),
            })
        manifest = {
            "message_id": message_id,
            "inbox_id": msg.get("inbox_id"),
            "saved_at": now_iso(),
            "message_sha256": hashlib.sha256(raw).hexdigest(),
            "attachments": files,
        }
        write_file(tmp / MANIFEST_FILE, (json.dumps(manifest, indent=2) + "\n").encode("utf-8"))
        fsync_dir(tmp / ATTACHMENT_DIR)
        fsync_dir(tmp)
        if final.exists():
            # Another poll saved it first; its folder stands.
            shutil.rmtree(tmp, ignore_errors=True)
            return {}
        os.rename(tmp, final)
        fsync_dir(spool_dir)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    return {"folder": final.name, "attachments": len(files), "timestamp": msg.get("timestamp")}


# --- commands -----------------------------------------------------------------------------


def read_settings(request: Dict[str, Any]) -> Tuple[Inbox, Path, int, int]:
    config = request.get("config") or {}
    secrets = request.get("secrets") if isinstance(request.get("secrets"), dict) else {}
    inbox = str(config.get("inbox", "")).strip()
    spool_dir = str(config.get("spool_dir", "")).strip()
    if not inbox:
        raise MailError("Missing required config key: inbox", retry=False)
    if not spool_dir:
        raise MailError("Missing required config key: spool_dir", retry=False)
    key = str(secrets.get(SECRET_NAME) or "").strip()
    if not key:
        raise MailError(f"No API key: grant the '{SECRET_NAME}' vault secret to this plugin", retry=False)
    try:
        base_url = str(config.get("base_url") or DEFAULT_BASE_URL)
        page_size = int(config.get("page_size", DEFAULT_PAGE_SIZE))
        max_messages = int(config.get("max_messages", DEFAULT_MAX_MESSAGES))
        timeout = int(config.get("request_timeout_seconds", DEFAULT_TIMEOUT_SECONDS))
    except (TypeError, ValueError):
        raise MailError("page_size, max_messages and request_timeout_seconds must be whole numbers", retry=False)
    if not 1 <= page_size <= 100 or max_messages < 1 or timeout < 1:
        raise MailError("page_size must be 1..100, max_messages and request_timeout_seconds at least 1",
                        retry=False)
    return Inbox(base_url, inbox, key, timeout), Path(spool_dir), page_size, max_messages


def poll_command(request: Dict[str, Any]) -> Dict[str, Any]:
    try:
        inbox, spool_dir, page_size, max_messages = read_settings(request)
        spool_dir.mkdir(parents=True, exist_ok=True)
        cleared = clear_partials(spool_dir)
        have = {p.name for p in saved_folders(spool_dir)}
        waiting = [mid for mid in inbox.message_ids(page_size) if stem(mid) not in have]
    except MailError as exc:
        return error_response(str(exc), retry=exc.retry)
    except OSError as exc:
        return error_response(f"spool unusable: {exc}", retry=True)

    events: List[Dict[str, Any]] = []
    logs: List[Dict[str, str]] = []
    if cleared:
        logs.append(log("warn", f"removed {cleared} half-built folder(s) from an earlier poll"))
    failed = 0
    retry = False
    for message_id in waiting[:max_messages]:
        try:
            saved = save_message(inbox, spool_dir, message_id)
        except MailError as exc:
            failed += 1
            retry = retry or exc.retry
            logs.append(log("error", f"{stem(message_id)}: {exc}"))
            continue
        except OSError as exc:
            failed += 1
            retry = True
            logs.append(log("error", f"{stem(message_id)}: spool write failed: {exc}"))
            continue
        if not saved:
            continue
        events.append({"type": EVENT_TYPE, "payload": {
            "inbox": urllib.parse.unquote(inbox.inbox),
            "message_id": message_id,
            "folder": saved["folder"],
            "spool_dir": str(spool_dir),
            "attachments": saved["attachments"],
            "timestamp": saved["timestamp"],
            "saved_at": now_iso(),
        }})

    backlog = max(0, len(waiting) - max_messages)
    summary = f"saved {len(events)}, failed {failed}, waiting {backlog}"
    logs.insert(0, log("info", summary))
    if failed and not events:
        return {**error_response(summary, retry=retry), "logs": logs}
    return ok_response(summary, events=events, logs=logs)


def health_command(request: Dict[str, Any]) -> Dict[str, Any]:
    try:
        inbox, spool_dir, _, _ = read_settings(request)
    except MailError as exc:
        return error_response(str(exc), retry=exc.retry)

    writable = spool_dir.is_dir() and os.access(spool_dir, os.W_OK)
    folders = len(saved_folders(spool_dir))
    reachable = True
    problem = None
    try:
        inbox._json(f"{inbox.base}/v0/inboxes/{inbox.inbox}/messages?limit=1")
    except MailError as exc:
        reachable = False
        problem = str(exc)

    healthy = writable and reachable
    payload = {
        "healthy": healthy,
        "checked_at": now_iso(),
        "spool": {"writable": writable, "folders": folders},
        "inbox": {"reachable": reachable, "problem": problem},
    }
    result = f"healthy={healthy} folders={folders} inbox_reachable={reachable}"
    return ok_response(result, events=[{"type": "agentmail.health", "payload": payload}],
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
