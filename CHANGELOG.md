# Changelog

## 2026-04-20
- Fix email_handler to pass Gmail call parameters as JSON via the --params flag, standardizing configuration.

## 2026-04-19
- Implemented Sprint 3 names-only value contracts and added explicit durability annotations to manifests.
- Reconnected to the shared Dolt server and updated hooks to v1.0.2.
- Applied Sprint 3 values contracts to the remaining manifest commands.
- Completed Sprint 3 values contracts so all 20 plugins conform.
- Made entrypoints executable to satisfy trust validation.

## 2026-04-16
- Broadcasts the first bird species detected each day to Discord.

## 2026-04-08
- Add email_handler plugin; ignore .beads in git.
- claude_harvest now uses find_claude() and exits with status 1 on total failure.

## 2026-04-06
- Adds a GPU-accelerated BirdNET bird detection plugin for birda.
- Switches birda's find_docker to rely exclusively on shutil.which.
- Adds 51 unit tests to cover the birda plugin.
- Adds a README for birda with commands, payload, config, and infrastructure requirements.

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

