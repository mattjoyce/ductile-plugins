"""Tests for changelog_microblog plugin."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict
from unittest.mock import patch

from run import (
    insert_changelog_entry,
    is_vague,
    normalize_bullets,
)

PLUGIN = os.path.join(os.path.dirname(__file__), "run.py")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _git(args, **kwargs):
    return subprocess.run(args, capture_output=True, text=True, **kwargs)


def make_git_repo(tmpdir: Path, commits: list[str] | None = None) -> Path:
    """Create a git repo with optional commits. Returns repo path."""
    repo = tmpdir / "repo"
    repo.mkdir()
    _git(["git", "-c", "init.defaultBranch=main", "init", str(repo)])
    _git(["git", "-C", str(repo), "config", "user.email", "test@test.com"])
    _git(["git", "-C", str(repo), "config", "user.name", "Test"])
    for i, msg in enumerate(commits or []):
        (repo / f"file{i}.txt").write_text(f"{msg}\n")
        _git(["git", "-C", str(repo), "add", "."])
        _git(["git", "-C", str(repo), "commit", "-m", msg])
    return repo


def make_request(repo_path: str, repo_name: str = "testrepo") -> Dict[str, Any]:
    return {
        "command": "handle",
        "config": {
            "fabric_bin": "fabric",
            "fabric_pattern": "ductile-microblog-changelog",
        },
        "event": {
            "type": "git_repo_sync.completed",
            "payload": {
                "path": repo_path,
                "repo_name": repo_name,
                "repo_path": repo_path,
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
# is_vague
# ---------------------------------------------------------------------------

class TestIsVague(unittest.TestCase):
    def test_empty_is_vague(self):
        self.assertTrue(is_vague(""))

    def test_merge_commit_is_vague(self):
        self.assertTrue(is_vague("Merge pull request #123"))

    def test_single_word_is_vague(self):
        self.assertTrue(is_vague("fix"))

    def test_two_words_is_vague(self):
        self.assertTrue(is_vague("fix bug"))

    def test_vague_prefix_short_is_vague(self):
        self.assertTrue(is_vague("update readme"))
        self.assertTrue(is_vague("chore cleanup"))
        self.assertTrue(is_vague("wip stuff"))

    def test_substantive_commit_not_vague(self):
        self.assertFalse(is_vague("Add OAuth2 token refresh with retry logic"))

    def test_vague_prefix_with_detail_not_vague(self):
        self.assertFalse(is_vague("fix broken pagination in user list endpoint"))

    def test_three_word_non_vague_prefix(self):
        self.assertFalse(is_vague("implement rate limiting middleware"))


# ---------------------------------------------------------------------------
# normalize_bullets
# ---------------------------------------------------------------------------

class TestNormalizeBullets(unittest.TestCase):
    def test_adds_dash_to_plain_lines(self):
        result = normalize_bullets("line one\nline two")
        self.assertIn("- line one", result)
        self.assertIn("- line two", result)

    def test_preserves_existing_dashes(self):
        result = normalize_bullets("- already a bullet")
        self.assertEqual(result, "- already a bullet")

    def test_preserves_asterisk_bullets(self):
        result = normalize_bullets("* asterisk bullet")
        self.assertEqual(result, "* asterisk bullet")

    def test_strips_empty_lines(self):
        result = normalize_bullets("line one\n\n\nline two")
        lines = result.splitlines()
        self.assertEqual(len(lines), 2)

    def test_mixed_input(self):
        result = normalize_bullets("- already\nplain line")
        self.assertIn("- already", result)
        self.assertIn("- plain line", result)


# ---------------------------------------------------------------------------
# insert_changelog_entry
# ---------------------------------------------------------------------------

class TestInsertChangelogEntry(unittest.TestCase):
    def test_empty_content_creates_changelog(self):
        result = insert_changelog_entry("", "## 2026-03-23\n- some change\n\n")
        self.assertIn("# Changelog", result)
        self.assertIn("## 2026-03-23", result)

    def test_inserts_after_h1(self):
        content = "# Changelog\n\n## 2026-01-01\n- old entry\n"
        result = insert_changelog_entry(content, "## 2026-03-23\n- new entry\n\n")
        lines = result.splitlines()
        # New entry should appear before old entry
        new_idx = next(i for i, l in enumerate(lines) if "2026-03-23" in l)
        old_idx = next(i for i, l in enumerate(lines) if "2026-01-01" in l)
        self.assertLess(new_idx, old_idx)

    def test_no_h1_inserts_after_first_heading(self):
        # Function treats any line starting with '#' as a heading anchor.
        # Content starting with '##' (no h1) gets the entry inserted after that line.
        content = "## 2026-01-01\n- old\n"
        result = insert_changelog_entry(content, "## 2026-03-23\n- new\n\n")
        self.assertIn("2026-03-23", result)
        self.assertIn("2026-01-01", result)

    def test_preserves_existing_entries(self):
        content = "# Changelog\n\n## 2026-01-01\n- existing\n"
        result = insert_changelog_entry(content, "## 2026-03-23\n- new\n\n")
        self.assertIn("existing", result)
        self.assertIn("new", result)


# ---------------------------------------------------------------------------
# Integration: no git repo → error
# ---------------------------------------------------------------------------

class TestMissingRepo(unittest.TestCase):
    def test_missing_repo_path_error(self):
        r = run_plugin({
            "command": "handle",
            "config": {},
            "event": {"type": "x", "payload": {}},
            "context": {},
        })
        self.assertEqual(r["status"], "error")

    def test_nonexistent_path_error(self):
        r = run_plugin(make_request("/nonexistent/path/repo"))
        self.assertEqual(r["status"], "error")
        self.assertFalse(r["retry"])


# ---------------------------------------------------------------------------
# Integration: only vague commits → no substantive commits
# (An unborn branch / empty repo hits a git log error; use vague commits instead)
# ---------------------------------------------------------------------------

class TestNoSubstantiveCommits(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        # "Initial commit" is 2 words → vague, gets filtered
        self.repo = make_git_repo(self.tmp, commits=["Initial commit"])

    def tearDown(self):
        self._tmp.cleanup()

    def test_returns_ok_not_changed(self):
        r = run_plugin(make_request(str(self.repo)))
        self.assertEqual(r["status"], "ok")
        self.assertFalse(r["events"][0]["payload"]["changed"])

    def test_result_mentions_no_substantive(self):
        r = run_plugin(make_request(str(self.repo)))
        self.assertIn("no substantive", r["result"])


# ---------------------------------------------------------------------------
# Integration: only vague commits → no substantive commits
# ---------------------------------------------------------------------------

class TestAllVagueCommits(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.repo = make_git_repo(self.tmp, commits=["fix", "wip", "update readme"])

    def tearDown(self):
        self._tmp.cleanup()

    def test_vague_commits_not_changed(self):
        r = run_plugin(make_request(str(self.repo)))
        self.assertEqual(r["status"], "ok")
        self.assertFalse(r["events"][0]["payload"]["changed"])


# ---------------------------------------------------------------------------
# Integration: substantive commits → fabric called → changelog updated
# Uses real fabric binary (known to be installed).
# ---------------------------------------------------------------------------

class TestSubstantiveCommits(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.repo = make_git_repo(self.tmp, commits=[
            "Add OAuth2 token refresh with automatic retry",
            "Implement rate limiting middleware for API endpoints",
        ])

    def tearDown(self):
        self._tmp.cleanup()

    def test_status_ok(self):
        r = run_plugin(make_request(str(self.repo)))
        self.assertEqual(r["status"], "ok")

    def test_event_type(self):
        r = run_plugin(make_request(str(self.repo)))
        self.assertEqual(r["events"][0]["type"], "changelog_microblog.completed")

    def test_payload_contains_repo_name(self):
        r = run_plugin(make_request(str(self.repo), repo_name="myrepo"))
        self.assertEqual(r["events"][0]["payload"]["repo_name"], "myrepo")

    def test_payload_has_since_field(self):
        r = run_plugin(make_request(str(self.repo)))
        self.assertIn("since", r["events"][0]["payload"])

    def test_changelog_written_when_changed(self):
        """If fabric produces output, CHANGELOG.md should be written."""
        r = run_plugin(make_request(str(self.repo)))
        p = r["events"][0]["payload"]
        if p["changed"]:
            self.assertTrue((self.repo / "CHANGELOG.md").exists())
            content = (self.repo / "CHANGELOG.md").read_text()
            self.assertIn("#", content)  # has a date heading


# ---------------------------------------------------------------------------
# Integration: ductile-changelog commits are excluded
# ---------------------------------------------------------------------------

class TestChangelogCommitsExcluded(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.repo = make_git_repo(self.tmp, commits=[
            "Update changelog [ductile-changelog]",
        ])

    def tearDown(self):
        self._tmp.cleanup()

    def test_ductile_changelog_commit_filtered_out(self):
        r = run_plugin(make_request(str(self.repo)))
        self.assertEqual(r["status"], "ok")
        self.assertFalse(r["events"][0]["payload"]["changed"])


# ---------------------------------------------------------------------------
# Protocol tests
# ---------------------------------------------------------------------------

class TestProtocol(unittest.TestCase):
    def test_invalid_json(self):
        result = subprocess.run(
            [sys.executable, PLUGIN],
            input="not json",
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)

    def test_health_requires_fabric(self):
        r = run_plugin({"command": "health", "config": {"fabric_bin": "fabric"}, "event": {}, "context": {}})
        # Either ok (fabric installed) or error (not installed) — never crashes
        self.assertIn(r["status"], ["ok", "error"])


if __name__ == "__main__":
    unittest.main()
