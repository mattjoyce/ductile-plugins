# describe_image

Writes a Markdown sidecar next to an image describing what is in it, so an AI or `grep` can find pictures by text.

Pairs with `folder_watch` (`emit_mode: per_file`). Trusted-tier plugin: it must read the watched folder in the operator's home and write the sidecar beside the image, which a confined account cannot do.

## Sidecar format

`PXL_20260905_120544756.jpg` -> `PXL_20260905_120544756.jpg.md`

```markdown
---
image: "PXL_20260905_120544756.jpg"
sha256: "…"
size_bytes: 2061517
file_modified: "2026-09-05T02:05:44+00:00"
described_at: "2026-09-06T00:00:00+00:00"
model: "claude-opus-5"
generator: "ductile/describe_image"
---

Underside of a Seeed Studio XIAO ESP32S3 board …
```

The `sha256` line makes the plugin idempotent: a re-fired event for an unchanged image is skipped without an API call. Delete the sidecar to force a re-describe.

## Commands

- `handle` (write): describe one image from a folder_watch per-file event.
- `poll` (write): no-op.
- `health` (read): config and secret delivery check, no API call.

## Configuration

| Key | Default | Purpose |
|---|---|---|
| `model` | `claude-opus-5` | Claude model id |
| `effort` | `medium` | `output_config.effort` |
| `max_tokens` | `4096` | response cap |
| `prompt` | catalogue prompt | instruction sent with the image |
| `max_image_bytes` | `5242880` | larger files are skipped (API limit) |
| `sidecar_suffix` | `.md` | appended to the full image filename |
| `secret_name` | `anthropic-api-key` | key looked up in `request.secrets` |
| `delete_orphans` | `true` | remove the sidecar when the image is deleted |
| `timeout_seconds` | `120` | API timeout |

## Secrets

The API key is delivered by the vault as `anthropic-api-key`. Register the plugin as a principal and grant the secret:

```bash
ductile vault register-principal --api-url http://127.0.0.1:8081 --token "$DUCTILE_VAULT_TOKEN" --name describe-image --kind plugin
printf '%s' "$ANTHROPIC_API_KEY" | ductile vault set --api-url http://127.0.0.1:8081 --token "$DUCTILE_VAULT_TOKEN" --name anthropic-api-key --pattern manual --principal describe-image
```

## Why raw HTTP

The runtime contract forbids fetching dependencies at spawn and the `anthropic` package is not installed on the host, so the plugin calls `POST /v1/messages` with `urllib`. Server-side refusal fallbacks (`fallbacks: "default"`) are enabled.

## Example config

See `config-fragment.yaml` in this directory.
