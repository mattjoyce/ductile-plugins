# health_weekly_report

Assemble a weekly personal health report from a `daily_health_summary` SQLite table, generate sectioned narratives via fabric/Ollama, write an Obsidian archive copy, and emit an event for downstream email handoff.

## Pipeline shape

```
scheduled cron (Mon 07:00 local)
    │
    ▼
health_weekly_report.assemble
    │ — read summary.db for ISO week + 4 prior weeks
    │ — compute deterministic numeric briefs (sleep / recovery / activity / body / cardio / stress)
    │ — fabric × 6 (one per section, pattern: health_section_narrative)
    │ — fabric × 1 (exec_summary, pattern: health_exec_summary)
    │ — assemble markdown + plain-text fallback
    │ — write {obsidian_archive_dir}/{YYYY}-W{WW}.md
    ▼
emit health_weekly_report.assembled
    │ payload: { week_iso, period_start, period_end, body_markdown, body_text,
    │            archive_path, sections_completed, sections_skipped, source_type }
    ▼
pipeline routes to email_send.send (separate plugin)
```

## Commands

| Command | Type | What it does |
|---------|------|--------------|
| `assemble` | write | Build the report for the most recently completed ISO week (override via `payload.week_iso` or `payload.target_date`). Writes Obsidian archive + emits event. |
| `health` | read  | Probe summary.db readability, archive dir writability, fabric binary. **Emits no `state_updates`.** |

## Trigger contract

The plugin doesn't subscribe to an upstream event by default — it's scheduled. Configure in `plugins.yaml`:

```yaml
plugins:
  health_weekly_report:
    enabled: true
    timeout: 300s
    max_attempts: 1
    concurrency_safe: false
    schedules:
      - id: weekly
        command: assemble
        cron: "0 7 * * MON"
        timezone: "Australia/Sydney"
    config:
      summary_db_path: /app/data/healthdata/summary/summary.db
      obsidian_archive_dir: /app/data/healthdata/obsidian/Health/Weekly
      fabric_bin: /usr/local/bin/fabric
      model: gemma3:4b
      section_timeout_sec: 90
      prior_weeks: 4
      timezone: Australia/Sydney
```

You can also trigger ad-hoc via the API to re-run for a specific week:

```bash
curl -X POST http://localhost:8081/plugin/health_weekly_report/assemble \
  -H "Authorization: Bearer $DUCTILE_LOCAL_TOKEN" \
  -d '{"payload": {"week_iso": "2026-W16"}}'
```

## Config keys

| Key | Required | Default | Purpose |
|---|---|---|---|
| `summary_db_path` | yes | — | Path to summary.db produced by `health_data_summary` |
| `obsidian_archive_dir` | yes | — | Directory the archive markdown is written into; created if absent |
| `fabric_bin` | no | `fabric` | Path to fabric CLI |
| `model` | no | _(fabric default)_ | Ollama model to pass via `--model` |
| `fabric_pattern_section` | no | `health_section_narrative` | Pattern name for per-section narratives |
| `fabric_pattern_exec_summary` | no | `health_exec_summary` | Pattern name for the exec summary call |
| `section_timeout_sec` | no | `90` | Per-fabric-call subprocess timeout |
| `prior_weeks` | no | `4` | Number of prior weeks to include in the trailing table |
| `timezone` | no | `Australia/Sydney` | Used to resolve "the most recent completed ISO week" |

## Sections

Six fixed sections, each with a deterministic numeric brief and an optional LLM narrative:

| Section | Source columns |
|---|---|
| sleep | sleep_total_min, sleep_deep_min, sleep_light_min, sleep_rem_min, sleep_score |
| recovery | resting_hr, body_battery_max, body_battery_min |
| activity | steps, distance_km, moderate_activity_min, vigorous_activity_min, calories_active |
| body | weight_kg, fat_ratio, muscle_mass_kg |
| cardio | systolic_bp, diastolic_bp, hr_min, hr_max |
| stress | stress_avg, spo2_avg, spo2_min, rr_avg |

If a fabric call fails or times out, the numeric brief is retained and the narrative is omitted; the section is reported in `sections_skipped` on the emitted event.

## Fabric patterns

Two patterns ship with this plugin under `patterns/`:
- `health_section_narrative/system.md`
- `health_exec_summary/system.md`

Operators install them by symlinking or copying into the fabric patterns dir (typically `~/.config/fabric/patterns/`):

```bash
ln -s $PWD/patterns/health_section_narrative ~/.config/fabric/patterns/
ln -s $PWD/patterns/health_exec_summary    ~/.config/fabric/patterns/
```

## Durable state

Plugin emits a presence-stable two-key snapshot on every successful `assemble`:

```json
{
  "latest_week_iso": "2026-W17",
  "last_assembled_at": "2026-04-27T07:00:14+00:00"
}
```

Both fields are derived from the **filesystem** state of `obsidian_archive_dir` (not from action bookkeeping), so they reflect what is actually durable on disk. Backed by a `fact_outputs` rule producing `health_weekly_report.snapshot`.

## Testing

Smoke test against a synthetic summary.db:

```bash
python3 -c "
import sqlite3, datetime
from pathlib import Path
db = Path('/tmp/test_summary.db'); db.unlink(missing_ok=True)
conn = sqlite3.connect(db)
conn.executescript(open('../health_data_summary/run.py').read().split('SUMMARY_SCHEMA = \"\"\"')[1].split('\"\"\"')[0])
# … insert 7 days of fake data …
"
```

Or invoke directly via stdin:

```bash
echo '{
  \"command\": \"assemble\",
  \"config\": {
    \"summary_db_path\": \"/tmp/test_summary.db\",
    \"obsidian_archive_dir\": \"/tmp/health_weekly\",
    \"model\": \"gpt-oss:20b\"
  },
  \"event\": {\"payload\": {\"week_iso\": \"2026-W17\"}}
}' | ./run.py | jq .
```

## Doctrine compliance

- protocol 2 ✓
- `values.consume` / `values.emit` per command ✓
- `health` emits no `state_updates` ✓
- snapshot is observed-state (filesystem-derived), presence-stable ✓
- `fact_outputs` rule for `health_weekly_report.snapshot` ✓
- `concurrency_safe: false` (owns the archive directory as single-writer) ✓
- `config_keys.required` matches the actual hard-fail set ✓
