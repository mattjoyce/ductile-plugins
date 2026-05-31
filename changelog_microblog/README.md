# changelog_microblog

Generate a micro-blog style CHANGELOG.md entry from recent commit messages in a git repository using fabric. Fetches origin, resets HEAD to `origin/<default_branch>` (destructive — local changes are lost), runs fabric against the resulting `git log`, filters vague messages, and prepends the formatted entry to CHANGELOG.md.

Stateful per-repo — declares `fact_outputs` so `last_run` and `last_changelog_at` survive restarts. Pair with a downstream `git_commit_push` to materialise the CHANGELOG.md write into a commit.

## Commands

- `handle` (write): Generate and append a changelog entry for one repo.
- `health` (read): Verify the fabric binary is callable.

## Input Payload (`handle`)

Accepts payload or context fields:

| Field | Purpose |
|---|---|
| `repo_path` / `path` | Local filesystem path to the git repo. One is required. |
| `repo_name` | Display name. Defaults to `basename(repo_path)`. |
| `ssh_url` | Optional SSH clone URL — carried into the completion event. |
| `clone_url` | Optional HTTPS clone URL — carried into the completion event. |
| `default_branch` | Branch to fetch/reset against. Defaults to `main`. |

## Configuration

All optional:

| Key | Default | Purpose |
|---|---|---|
| `fabric_bin` | `fabric` | Path to the fabric binary |
| `fabric_pattern` | `ductile-microblog-changelog` | Fabric pattern name to apply |
| `patterns_path` | `~/.config/ductile/patterns` | Path to the fabric patterns directory |

## Behaviour

- **Destructive first step**: `git reset --hard origin/<default_branch>` before anything else. Local working-tree changes are overwritten.
- Lookback starts at the most recent of (7 days ago) or the last `CHANGELOG.md` commit timestamp.
- Skips vague commits and commits tagged `[ductile-changelog]`.
- Uses the configured fabric pattern to turn commits into micro-blog bullets.
- Prepends the rendered entry to CHANGELOG.md; if the vague-filter rejects all content, returns ok with `changed: false`.

## Events

Emits `changelog_microblog.completed` with payload: `repo_path`, `repo_name`, `changed`, `entry_date`, `entry_text`, `since`, `ssh_url`, `clone_url`.

## Durable state

On successful `handle` that actually updated CHANGELOG.md, returns `state_updates`:

```json
{
  "last_run": "<iso-8601>",
  "last_changelog_at": "<entry_date>"
}
```

Snapshotted as `changelog_microblog.snapshot` via `fact_outputs` (mirror_object). The "did this repo get a changelog today?" question is answered from `plugin_facts`.
