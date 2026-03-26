# claude_harvest

Harvests Zettelkasten notes from Claude Code sessions using the `/learn` command.

Runs nightly (via `poll` schedule), scans `~/.claude/projects/` for sessions that have changed since the last run, and triggers one worker job per project. Each worker resumes unprocessed sessions, invokes `/learn`, and counts new notes written to the ZK vault.

## Commands

- **`poll`** — Scanner. Finds projects with new unprocessed sessions since last run. Triggers one `handle` job per project via the ductile pipeline API.
- **`handle`** — Worker. Processes up to `max_per_project` sessions for a single project. Outputs a Discord-ready summary of sessions processed and notes written.
- **`health`** — Verifies the learn command, ZK vault path, and ductile API are all reachable.

## Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `api_url` | `http://localhost:8081` | Ductile API base URL |
| `api_token` | `$DUCTILE_LOCAL_TOKEN` | API token for triggering worker pipelines |
| `worker_pipeline` | `claude-harvest-worker` | Pipeline name triggered per project |
| `state_file` | `~/.config/claude-harvest/state.json` | Tracks processed sessions and last scan time |
| `learn_cmd` | `~/.claude/commands/learn.md` | Path to the /learn prompt file |
| `zk_env` | `~/.config/ZK/.env` | ZK vault config (reads `ZK_VAULT_PATH`) |
| `min_size_bytes` | `50000` | Skip sessions smaller than this (trivial/aborted) |
| `max_age_days` | `30` | Ignore sessions older than this |
| `min_age_minutes` | `60` | Skip sessions younger than this (may still be active) |
| `max_per_project` | `5` | Max sessions processed per worker run |
| `session_timeout_seconds` | `120` | Timeout per `claude --resume` invocation |

## Example Config

```yaml
plugins:
  claude_harvest:
    enabled: true
    timeout: 2m
    schedules:
      - cron: "0 7 * * *"
        timezone: "Australia/Sydney"
        command: poll
        jitter: 15m
    config:
      api_url: http://localhost:8081
      api_token: ${DUCTILE_LOCAL_TOKEN}

  claude_harvest_worker:
    uses: claude_harvest
    enabled: true
    timeout: 12m
    config:
      api_url: http://localhost:8081
      api_token: ${DUCTILE_LOCAL_TOKEN}
      session_timeout_seconds: 120
      max_per_project: 5
```

## Required Pipelines

```yaml
pipelines:
  - name: claude-harvest-worker
    on: claude.harvest.project
    steps:
      - id: harvest
        uses: claude_harvest_worker
      - id: notify
        uses: claude_harvest_notify
```

## Discord Notification

The worker outputs a summary that `discord_notify` picks up via `context.result`:

```
**Claude Harvest** — ~/admin — 2026-03-15
3/3 sessions · 7 ZK notes

✓ `d56a6a42` 2026-03-14 17:38 — +3 notes
✓ `461691ae` 2026-03-14 10:21 — +2 notes
✗ `3aaf8f60` 2026-03-14 09:32 — no new notes
```

## State File

Stored at `~/.config/claude-harvest/state.json`. Tracks:
- `processed`: map of session ID → `{processed_at, project, cwd, success, notes_written}`
- `last_scan_at`: ISO timestamp of last successful scan (used to filter changed projects)

File writes are protected with `fcntl` locking so parallel worker jobs don't corrupt state.

## How It Works

```
7am cron
  → claude_harvest poll
    → scans ~/.claude/projects/*/
    → filters: size > 50KB, age 1h–30d, mtime > last_scan_at, not in processed set
    → POST /pipeline/claude-harvest-worker per changed project
      → claude_harvest handle (payload: {project, cwd})
        → finds unprocessed sessions for that project
        → for each: claude --resume <id> --fork-session --dangerously-skip-permissions -p <learn prompt>
        → counts new ZK notes written
        → saves to state file
        → outputs Discord summary
      → discord_notify posts per-project message
```
