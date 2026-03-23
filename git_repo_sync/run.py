#!/usr/bin/env python3
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse


def respond(payload: Dict[str, Any]) -> None:
    json.dump(payload, sys.stdout)


def git_command(args, cwd: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(args, cwd=str(cwd) if cwd else None, capture_output=True, text=True)


def rewrite_ssh_url(url: str | None, alias_host: str) -> str | None:
    if not url:
        return None
    if not alias_host or alias_host == "github.com":
        return url
    if url.startswith("git@"):
        if ":" in url:
            user_host, path = url.split(":", 1)
            user, _host = user_host.split("@", 1)
            return f"{user}@{alias_host}:{path}"
    if url.startswith("ssh://"):
        parsed = urlparse(url)
        if parsed.hostname:
            user = parsed.username or "git"
            path = parsed.path.lstrip("/")
            return f"ssh://{user}@{alias_host}/{path}"
    return url


def get_ref_hash(repo_dir: Path, ref: str) -> Optional[str]:
    """Return the commit hash for a ref, or None if it doesn't exist."""
    result = git_command(["git", "rev-parse", ref], cwd=repo_dir)
    h = result.stdout.strip()
    return h if result.returncode == 0 and h else None


def count_new_commits(repo_dir: Path, before_hash: str, after_hash: str) -> int:
    """Count commits reachable from after_hash but not before_hash."""
    result = git_command(
        ["git", "rev-list", "--count", f"{before_hash}..{after_hash}"],
        cwd=repo_dir,
    )
    try:
        return int(result.stdout.strip()) if result.returncode == 0 else 0
    except ValueError:
        return 0


def advance_local_branch(repo_dir: Path, default_branch: str) -> Tuple[bool, str]:
    """Fast-forward local branch to match origin/{default_branch}.

    Returns (success, error_message).
    """
    result = git_command(
        ["git", "merge", "--ff-only", f"origin/{default_branch}"],
        cwd=repo_dir,
    )
    if result.returncode == 0:
        return True, ""
    return False, result.stderr.strip()


def main() -> None:
    request = json.load(sys.stdin)
    command = request.get("command") or "handle"
    config = request.get("config") or {}

    if command == "health":
        respond(
            {
                "status": "ok",
                "result": "git_repo_sync healthy",
                "logs": [{"level": "info", "message": "git_repo_sync healthy"}],
            }
        )
        return

    event = request.get("event") or {}
    payload = event.get("payload") or {}
    context = request.get("context") or {}

    def field(name: str):
        return payload.get(name) or context.get(name)

    owner = field("owner")
    repo_name = field("repo_name")
    clone_url = field("clone_url")
    ssh_url = field("ssh_url")
    default_branch = field("default_branch") or "main"
    clone_dir_raw = field("clone_dir") or "~/github.mattjoyce"
    prefer_ssh = bool(config.get("prefer_ssh", False))
    ssh_alias_host = config.get("ssh_alias_host", "github.com-ductile")
    ssh_url_effective = rewrite_ssh_url(ssh_url, ssh_alias_host) if prefer_ssh else ssh_url

    if not owner or not repo_name or not (clone_url or ssh_url):
        respond(
            {
                "status": "error",
                "error": "missing required payload fields (owner, repo_name, clone_url/ssh_url)",
                "retry": False,
                "logs": [
                    {
                        "level": "error",
                        "message": "missing required payload fields (owner, repo_name, clone_url/ssh_url)",
                    }
                ],
            }
        )
        return

    clone_dir = Path(os.path.expanduser(clone_dir_raw))
    repo_dir = clone_dir / owner / repo_name
    repo_dir.parent.mkdir(parents=True, exist_ok=True)

    action = "fetched"
    new_commits = False
    commit_count = 0
    before_sha: Optional[str] = None
    after_sha: Optional[str] = None
    logs = []

    def set_remote_to_ssh() -> bool:
        if prefer_ssh and ssh_url_effective:
            result = git_command(
                ["git", "-C", str(repo_dir), "remote", "set-url", "origin", ssh_url_effective]
            )
            if result.returncode != 0:
                logs.append(
                    {
                        "level": "error",
                        "message": f"failed to set SSH remote for {repo_name}: {result.stderr.strip()}",
                    }
                )
                return False
        elif prefer_ssh and not ssh_url_effective:
            logs.append(
                {
                    "level": "warn",
                    "message": f"prefer_ssh enabled but ssh_url missing for {repo_name}",
                }
            )
        return True

    if repo_dir.exists() and (repo_dir / ".git").exists():
        if not set_remote_to_ssh():
            respond(
                {
                    "status": "error",
                    "error": "failed to set SSH remote",
                    "retry": True,
                    "logs": logs,
                }
            )
            return

        remote_ref = f"origin/{default_branch}"
        before_sha = get_ref_hash(repo_dir, remote_ref)

        result = git_command(["git", "-C", str(repo_dir), "fetch", "--prune", "--quiet"])
        if result.returncode != 0:
            respond(
                {
                    "status": "error",
                    "error": f"fetch failed: {result.stderr.strip()}",
                    "retry": True,
                    "logs": [
                        {
                            "level": "error",
                            "message": f"fetch failed for {repo_name}: {result.stderr.strip()}",
                        }
                    ],
                }
            )
            return

        after_sha = get_ref_hash(repo_dir, remote_ref)
        new_commits = bool(before_sha and after_sha and before_sha != after_sha)
        if new_commits:
            commit_count = count_new_commits(repo_dir, before_sha, after_sha)
            ok, msg = advance_local_branch(repo_dir, default_branch)
            if not ok:
                logs.append(
                    {
                        "level": "warn",
                        "message": f"ff-merge skipped for {repo_name}: {msg}",
                    }
                )

    elif repo_dir.exists():
        respond(
            {
                "status": "error",
                "error": f"path exists but is not a git repo: {repo_dir}",
                "retry": False,
                "logs": [
                    {
                        "level": "error",
                        "message": f"path exists but is not a git repo: {repo_dir}",
                    }
                ],
            }
        )
        return
    else:
        action = "cloned"
        new_commits = True
        clone_source = ssh_url_effective if prefer_ssh and ssh_url_effective else (clone_url or ssh_url)
        result = git_command(["git", "clone", "--quiet", clone_source, str(repo_dir)])
        if result.returncode != 0:
            respond(
                {
                    "status": "error",
                    "error": f"clone failed: {result.stderr.strip()}",
                    "retry": True,
                    "logs": [
                        {
                            "level": "error",
                            "message": f"clone failed for {repo_name}: {result.stderr.strip()}",
                        }
                    ],
                }
            )
            return
        after_sha = get_ref_hash(repo_dir, f"origin/{default_branch}")

    summary = f"Repo sync {action}: {owner}/{repo_name}"

    respond(
        {
            "status": "ok",
            "result": summary,
            "events": [
                {
                    "type": "git_repo_sync.completed",
                    "payload": {
                        "message": summary,
                        "text": summary,
                        "result": summary,
                        "owner": owner,
                        "repo_name": repo_name,
                        "path": str(repo_dir),
                        "action": action,
                        "new_commits": new_commits,
                        "commit_count": commit_count,
                        "before_sha": before_sha or "",
                        "after_sha": after_sha or "",
                        "default_branch": default_branch,
                        "clone_url": clone_url or "",
                        "ssh_url": (ssh_url_effective or ssh_url or ""),
                    },
                }
            ],
            "logs": logs + [{"level": "info", "message": summary}],
        }
    )


if __name__ == "__main__":
    main()
