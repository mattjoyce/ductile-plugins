"""Tests for git_repo_sync plugin."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict

from run import (
    advance_local_branch,
    count_new_commits,
    get_ref_hash,
    rewrite_ssh_url,
)

PLUGIN = os.path.join(os.path.dirname(__file__), "run.py")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _git(args, **kwargs):
    """Run a git command with capture_output=True."""
    return subprocess.run(args, capture_output=True, text=True, **kwargs)


def make_upstream(tmpdir: Path) -> Path:
    """Create a bare upstream repo with one initial commit."""
    upstream = tmpdir / "upstream.git"
    upstream.mkdir()
    _git([
        "git", "-c", "init.defaultBranch=main", "init", "--bare", str(upstream)
    ])

    # Create a working clone to populate it
    work = tmpdir / "work"
    _git(["git", "-c", "init.defaultBranch=main", "clone", str(upstream), str(work)])
    _git(["git", "-C", str(work), "config", "user.email", "test@test.com"])
    _git(["git", "-C", str(work), "config", "user.name", "Test"])
    (work / "README.md").write_text("# Test\n")
    _git(["git", "-C", str(work), "add", "."])
    _git(["git", "-C", str(work), "commit", "-m", "Initial commit"])
    _git(["git", "-C", str(work), "push", "-u", "origin", "HEAD"])
    return upstream


def add_upstream_commit(tmpdir: Path, upstream: Path, message: str, filename: str = None) -> None:
    """Add a commit to the upstream repo."""
    work = tmpdir / "work"
    fname = filename or f"file_{message[:10].replace(' ', '_')}.txt"
    (work / fname).write_text(f"{message}\n")
    _git(["git", "-C", str(work), "add", "."])
    _git(["git", "-C", str(work), "commit", "-m", message])
    _git(["git", "-C", str(work), "push", "origin", "HEAD"])


def make_local_clone(tmpdir: Path, upstream: Path) -> Path:
    """Clone upstream into a local mirror (simulating an existing sync)."""
    local = tmpdir / "local"
    _git(["git", "clone", str(upstream), str(local)])
    _git(["git", "-C", str(local), "config", "user.email", "test@test.com"])
    _git(["git", "-C", str(local), "config", "user.name", "Test"])
    return local


def make_request(owner: str, repo_name: str, clone_url: str, clone_dir: str, default_branch: str = "main") -> Dict[str, Any]:
    return {
        "command": "handle",
        "config": {},
        "event": {
            "type": "github_repo_sync.repo_discovered",
            "payload": {
                "owner": owner,
                "repo_name": repo_name,
                "clone_url": clone_url,
                "ssh_url": "",
                "clone_dir": clone_dir,
                "default_branch": default_branch,
            },
        },
        "context": {},
    }


def run_plugin(request: Dict[str, Any]) -> Dict[str, Any]:
    result = subprocess.run(
        [sys.executable, PLUGIN],
        input=json.dumps(request),
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


# ---------------------------------------------------------------------------
# get_ref_hash
# ---------------------------------------------------------------------------

class TestGetRefHash(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        upstream = make_upstream(self.tmp)
        self.repo = make_local_clone(self.tmp, upstream)

    def tearDown(self):
        self._tmp.cleanup()

    def test_returns_hash_for_valid_ref(self):
        h = get_ref_hash(self.repo, "HEAD")
        self.assertIsNotNone(h)
        self.assertEqual(len(h), 40)

    def test_returns_none_for_invalid_ref(self):
        h = get_ref_hash(self.repo, "refs/nonexistent/branch")
        self.assertIsNone(h)

    def test_origin_main_hash(self):
        h = get_ref_hash(self.repo, "origin/main")
        self.assertIsNotNone(h)


# ---------------------------------------------------------------------------
# count_new_commits
# ---------------------------------------------------------------------------

class TestCountNewCommits(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.upstream = make_upstream(self.tmp)
        self.repo = make_local_clone(self.tmp, self.upstream)

    def tearDown(self):
        self._tmp.cleanup()

    def test_zero_when_same_hash(self):
        h = get_ref_hash(self.repo, "HEAD")
        self.assertEqual(count_new_commits(self.repo, h, h), 0)

    def test_counts_new_commits(self):
        before = get_ref_hash(self.repo, "origin/main")
        add_upstream_commit(self.tmp, self.upstream, "Add feature A")
        add_upstream_commit(self.tmp, self.upstream, "Add feature B")
        subprocess.run(["git", "-C", str(self.repo), "fetch"], capture_output=True)
        after = get_ref_hash(self.repo, "origin/main")
        self.assertEqual(count_new_commits(self.repo, before, after), 2)


# ---------------------------------------------------------------------------
# advance_local_branch
# ---------------------------------------------------------------------------

class TestAdvanceLocalBranch(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.upstream = make_upstream(self.tmp)
        self.repo = make_local_clone(self.tmp, self.upstream)

    def tearDown(self):
        self._tmp.cleanup()

    def test_advance_with_new_commits(self):
        add_upstream_commit(self.tmp, self.upstream, "New commit upstream")
        subprocess.run(["git", "-C", str(self.repo), "fetch"], capture_output=True)
        before_head = get_ref_hash(self.repo, "HEAD")
        ok, _ = advance_local_branch(self.repo, "main")
        self.assertTrue(ok)
        after_head = get_ref_hash(self.repo, "HEAD")
        self.assertNotEqual(before_head, after_head)

    def test_advance_when_already_up_to_date(self):
        ok, _ = advance_local_branch(self.repo, "main")
        self.assertTrue(ok)

    def test_returns_false_on_bad_branch(self):
        ok, msg = advance_local_branch(self.repo, "nonexistent-branch")
        self.assertFalse(ok)
        self.assertGreater(len(msg), 0)


# ---------------------------------------------------------------------------
# rewrite_ssh_url
# ---------------------------------------------------------------------------

class TestRewriteSshUrl(unittest.TestCase):
    def test_no_alias_returns_original(self):
        self.assertEqual(rewrite_ssh_url("git@github.com:user/repo.git", "github.com"), "git@github.com:user/repo.git")

    def test_rewrites_git_at_url(self):
        result = rewrite_ssh_url("git@github.com:user/repo.git", "github.com-ductile")
        self.assertEqual(result, "git@github.com-ductile:user/repo.git")

    def test_none_returns_none(self):
        self.assertIsNone(rewrite_ssh_url(None, "github.com-ductile"))

    def test_empty_alias_returns_original(self):
        self.assertEqual(rewrite_ssh_url("git@github.com:u/r.git", ""), "git@github.com:u/r.git")


# ---------------------------------------------------------------------------
# Integration: fetch with no new commits
# ---------------------------------------------------------------------------

class TestFetchNoNewCommits(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.upstream = make_upstream(self.tmp)
        # Pre-clone so the plugin sees an existing repo
        self.local = make_local_clone(self.tmp, self.upstream)
        # clone_dir is structured as clone_dir/owner/repo_name
        self.clone_dir = self.tmp / "mirrors"
        owner_dir = self.clone_dir / "testowner"
        owner_dir.mkdir(parents=True)
        (owner_dir / "testrepo").symlink_to(self.local)

    def tearDown(self):
        self._tmp.cleanup()

    def _request(self):
        return make_request("testowner", "testrepo", str(self.upstream), str(self.clone_dir))

    def test_status_ok(self):
        r = run_plugin(self._request())
        self.assertEqual(r["status"], "ok")

    def test_action_is_fetched(self):
        r = run_plugin(self._request())
        self.assertEqual(r["events"][0]["payload"]["action"], "fetched")

    def test_new_commits_false(self):
        r = run_plugin(self._request())
        self.assertFalse(r["events"][0]["payload"]["new_commits"])

    def test_commit_count_zero(self):
        r = run_plugin(self._request())
        self.assertEqual(r["events"][0]["payload"]["commit_count"], 0)

    def test_event_type(self):
        r = run_plugin(self._request())
        self.assertEqual(r["events"][0]["type"], "git_repo_sync.completed")


# ---------------------------------------------------------------------------
# Integration: fetch with new commits
# ---------------------------------------------------------------------------

class TestFetchWithNewCommits(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.upstream = make_upstream(self.tmp)
        # Pre-clone
        self.local = make_local_clone(self.tmp, self.upstream)
        self.clone_dir = self.tmp / "mirrors"
        owner_dir = self.clone_dir / "testowner"
        owner_dir.mkdir(parents=True)
        (owner_dir / "testrepo").symlink_to(self.local)
        # Add commits to upstream AFTER the local clone was made
        add_upstream_commit(self.tmp, self.upstream, "Add new feature X")
        add_upstream_commit(self.tmp, self.upstream, "Fix critical bug Y")

    def tearDown(self):
        self._tmp.cleanup()

    def _request(self):
        return make_request("testowner", "testrepo", str(self.upstream), str(self.clone_dir))

    def test_new_commits_true(self):
        r = run_plugin(self._request())
        self.assertTrue(r["events"][0]["payload"]["new_commits"])

    def test_commit_count_is_two(self):
        r = run_plugin(self._request())
        self.assertEqual(r["events"][0]["payload"]["commit_count"], 2)

    def test_before_and_after_sha_differ(self):
        r = run_plugin(self._request())
        p = r["events"][0]["payload"]
        self.assertNotEqual(p["before_sha"], p["after_sha"])
        self.assertEqual(len(p["before_sha"]), 40)
        self.assertEqual(len(p["after_sha"]), 40)

    def test_local_head_advanced(self):
        """After plugin runs, local HEAD should point to latest upstream commit."""
        r = run_plugin(self._request())
        self.assertEqual(r["status"], "ok")
        local_head = get_ref_hash(self.local, "HEAD")
        upstream_head = get_ref_hash(self.local, "origin/main")
        self.assertEqual(local_head, upstream_head)

    def test_default_branch_in_payload(self):
        r = run_plugin(self._request())
        self.assertEqual(r["events"][0]["payload"]["default_branch"], "main")


# ---------------------------------------------------------------------------
# Integration: fresh clone
# ---------------------------------------------------------------------------

class TestFreshClone(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.upstream = make_upstream(self.tmp)
        self.clone_dir = self.tmp / "mirrors"
        self.clone_dir.mkdir()

    def tearDown(self):
        self._tmp.cleanup()

    def _request(self):
        return make_request("testowner", "testrepo", str(self.upstream), str(self.clone_dir))

    def test_status_ok(self):
        r = run_plugin(self._request())
        self.assertEqual(r["status"], "ok")

    def test_action_is_cloned(self):
        r = run_plugin(self._request())
        self.assertEqual(r["events"][0]["payload"]["action"], "cloned")

    def test_new_commits_true_on_clone(self):
        r = run_plugin(self._request())
        self.assertTrue(r["events"][0]["payload"]["new_commits"])

    def test_repo_dir_created(self):
        run_plugin(self._request())
        self.assertTrue((self.clone_dir / "testowner" / "testrepo" / ".git").exists())


# ---------------------------------------------------------------------------
# Integration: error cases
# ---------------------------------------------------------------------------

class TestErrorCases(unittest.TestCase):
    def test_missing_owner_returns_error(self):
        r = run_plugin({
            "command": "handle",
            "config": {},
            "event": {"type": "x", "payload": {"repo_name": "r", "clone_url": "x"}},
            "context": {},
        })
        self.assertEqual(r["status"], "error")
        self.assertFalse(r["retry"])

    def test_missing_clone_url_and_ssh_url_returns_error(self):
        r = run_plugin({
            "command": "handle",
            "config": {},
            "event": {"type": "x", "payload": {"owner": "u", "repo_name": "r"}},
            "context": {},
        })
        self.assertEqual(r["status"], "error")

    def test_health_command(self):
        r = run_plugin({"command": "health", "config": {}})
        self.assertEqual(r["status"], "ok")

    def test_path_exists_but_not_git_repo(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            clone_dir = Path(tmpdir) / "mirrors"
            repo_dir = clone_dir / "u" / "r"
            repo_dir.mkdir(parents=True)
            r = run_plugin(make_request("u", "r", "file:///nonexistent", str(clone_dir)))
            self.assertEqual(r["status"], "error")
            self.assertFalse(r["retry"])


if __name__ == "__main__":
    unittest.main()
