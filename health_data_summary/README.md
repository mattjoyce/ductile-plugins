# health_data_summary

Joins per-day Garmin and Withings health data into a unified `daily_health_summary` table on receipt of `health.new_data` events from upstream collector plugins.

## Contract

### Trigger

`health.new_data` event with payload:

```yaml
source: string             # e.g. "garmin", "withings"
dirty_periods:             # list of YYYY-MM-DD strings
  - "2026-04-23"
  - "2026-04-24"
detected_at: string        # ISO8601 (informational)
```

### Commands

| Command | Type | Description |
|---|---|---|
| `summarize` | read | Consume the event, join garmin+withings data per dirty day, upsert into `daily_health_summary`. Emits `health.summary_updated`. |
| `query` | read | Return rows from `daily_health_summary` for a date range. Emits `health.query_result`. |
| `health` | read | Report DB stats and reachability. Emits `health_data_summary.health`. |

### Config

```yaml
plugins:
  health_data_summary:
    enabled: true
    config:
      garmin_db_path: /path/to/garmin.db
      withings_db_path: /path/to/withings.db
      summary_db_path: /path/to/summary.db
```

All three paths required. The plugin owns `summary_db_path` for write; reads the other two read-only.

### Durable state

Declared via `fact_outputs` as `health_data_summary.snapshot`. The snapshot is observed from `summary.db` after each `summarize`:

```yaml
latest_summarized_day: "2026-04-24"   # max(day) FROM daily_health_summary
summary_row_count: 743                # count(*) FROM daily_health_summary
```

Both fields are observations of the durable summary DB, not action bookkeeping. Action provenance is in `job_log`.

### Example pipeline

```yaml
pipelines:
  - name: health-summary
    on: health.new_data
    steps:
      - id: summarize
        uses: health_data_summary
```

Any upstream plugin emitting `health.new_data` with the documented payload shape will trigger summarization.

## Source-side schema requirements

### garmin.db (read-only)
- `daily_summary` — keyed on `day` (YYYY-MM-DD); columns referenced: `steps`, `calories_active`, `calories_total`, `distance`, `floors_up`, `stress_avg`, `moderate_activity_time`, `vigorous_activity_time`, `spo2_avg`, `spo2_min`, `rr_waking_avg`, `bb_max`, `bb_min`, `hr_min`, `hr_max`.
- `sleep` — keyed on `day`; columns `total_sleep`, `deep_sleep`, `light_sleep`, `rem_sleep`, `score`, `qualifier`.
- `resting_hr` — keyed on `day`; column `resting_heart_rate`.

This shape matches GarminDB's standard layout.

### withings.db (read-only)
- `measurements` — `(date TIMESTAMP UTC, type INTEGER, value REAL, unit INTEGER)`.
- Withings type IDs mapped:
  | Type | Field |
  |---|---|
  | 1 | weight_kg |
  | 5 | fat_free_mass_kg |
  | 6 | fat_ratio |
  | 8 | fat_mass_kg |
  | 9 | diastolic_bp |
  | 10 | systolic_bp |
  | 11 | heart_rate_bpm |
  | 76 | muscle_mass_kg |
  | 77 | hydration_kg |
  | 88 | bone_mass_kg |
  | 226 | bmr_kcal |

For each (day, type), the most recent measurement of the day is taken.

## Notes

- `concurrency_safe: false` — owns single-writer access to summary.db.
- `summarize` is `type: read` because all writes are to a local SQLite the plugin owns; no external state mutation.
- `health` and `query` emit no `state_updates` per ductile §5.
- COALESCE upserts mean re-summarizing a day overwrites only fields that have new non-null values, preserving anything not present in the current source data.
