# sqlite_change

Poll a SQLite database with a configured query and emit an event when the
result crosses a configured threshold.

## Plugin Facts

`poll` declares `sqlite_change.snapshot` as a fact output from
`state_updates`. Ductile records that snapshot append-only in `plugin_facts`
and keeps `plugin_state` as the compatibility/current-view row via
`mirror_object`.

The compatibility snapshot is:

```json
{
  "last_result": "5",
  "last_checked_at": "2026-04-24T00:00:00+00:00",
  "last_triggered_at": "2026-04-24T00:00:00+00:00"
}
```

This plugin keeps `last_checked_at` inside the snapshot deliberately. It is
the timestamp of the most recent completed observation, not transient debug
noise, and existing operators already read it as part of the current state
contract.

## Commands

- `poll` (write): Run the configured query, emit the configured event if the
  threshold is met, and return the full compatibility snapshot.
- `health` (read): Validate configuration, database reachability, and current
  query result without mutating durable state.

## Configuration

- `db_path`: SQLite database path.
- `query`: SQL query to execute. The first column of the first row is used as
  the scalar result.
- `event_type`: Event to emit when the threshold is met.
- `threshold_op`: One of `changed`, `any_rows`, `>`, `>=`, `<`, `<=`, `==`.
  Defaults to `changed`.
- `threshold_value`: Required for numeric comparison operators.
- `message_template`: Optional event message template.

## Events

Emits the configured `event_type` with payload fields including `result`,
`previous_result`, `threshold_op`, `threshold_value`, `detected_at`, and
`message`.
