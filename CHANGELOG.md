# Changelog

## 2026-04-01
- Adds a new gmail_poller plugin to the project for polling Gmail accounts.

## 2026-03-29
- Adds agent_handshake plugin to implement a challenge-response barrier for autonomous agents.

## 2026-03-26
- Add claude_harvest manifest and README; also add a health check to look for bun at known install locations.
- Fix daily repo sync by including pushed_at in the dedupe key to avoid silently skipping repos.
- Before scanning, fetch and reset to origin in changelog_microblog to ensure it uses the latest changes.
- Restore execute permissions on all plugin run.py files so they can run.
- If all harvest sessions fail, escalate to status:error.

## 2026-03-24
- bd init initializes beads issue tracking.
- claude_harvest now supports --no-session-persistence and --model options, and provides more detailed error information.
- Add CI workflow and fix ruff lint warnings across all plugins.
- git_repo_sync detects new commits and advances the local HEAD.

## 2026-03-13
- Bring in the integration plugins from the ductile project into this repo.
- Introduce a plugin manifest schema for reference and validation.
- Add README, Apache 2.0 LICENSE, and .gitignore; remove pycache.

