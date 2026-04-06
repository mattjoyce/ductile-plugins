# birda

GPU-accelerated bird species detection using BirdNET v24 (ONNX/CUDA). Runs the [birda](https://github.com/tphakala/birda) Docker container on Unraid via the Docker socket, analyzes a WAV file, and returns structured detections from the Raven selection table output.

Requires: Docker socket mounted into Ductile container, Nvidia container toolkit on the host (RTX GPU), and the `birda` image built on Unraid.

## Commands

- `handle` (write): Analyze a WAV file for bird species. Runs `docker run --rm --gpus all birda` and parses the resulting `.BirdNET.selection.table.txt`.
- `health` (read): Verify the Docker binary is reachable and the `birda` image exists.

## Input (`handle`)

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `wav_path` | string | yes | — | Absolute Unraid host path to WAV file (`/mnt/user/...`) |
| `lat` | number | yes | — | Latitude for species range filtering |
| `lon` | number | yes | — | Longitude for species range filtering |
| `min_conf` | number | no | 0.7 | Minimum confidence threshold (0.0–1.0) |
| `week` | integer | no | -1 | Week number 1–48 for seasonal filtering; -1 disables |

## Output (`handle`)

| Field | Type | Description |
|-------|------|-------------|
| `output_path` | string | Path to `.BirdNET.selection.table.txt` written next to the WAV file |
| `detections` | array | List of `{start_s, end_s, common_name, scientific_name, confidence}` dicts, sorted by `start_s` |
| `detection_count` | integer | Total detections above threshold |
| `duration_s` | number | Audio duration in seconds (from birda ndjson output) |
| `realtime_factor` | number | Processing speed ratio (e.g. `237` = 237× realtime) |

> **Note:** `scientific_name` is always empty — birda's Raven format outputs `Species Code` not scientific name. Use `common_name` for species identification.

## Configuration

| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `docker_bin` | no | auto-detected | Override path to docker binary |
| `default_min_conf` | no | 0.7 | Default confidence threshold if not in payload |

## Example

```yaml
plugins:
  birda:
    enabled: true
    timeout: 300s
    max_attempts: 1
    concurrency_safe: false
    config:
      default_min_conf: 0.7
```

Example payload:
```json
{
  "wav_path": "/mnt/user/field_Recording/F3/Orig/260329/290326_001.WAV",
  "lat": -34.0,
  "lon": 150.5,
  "min_conf": 0.7,
  "week": 13
}
```

## Infrastructure requirements

- Ductile container must mount `/var/run/docker.sock:/var/run/docker.sock`
- Ductile container must mount the field recording path (e.g. `/mnt/user/field_Recording:/mnt/user/field_Recording`) to read birda's output file
- `docker-cli` must be available in the Ductile container (`apk add docker-cli` in Dockerfile)
- Nvidia container toolkit installed on Unraid host
- `birda` Docker image built: `cd /mnt/user/appdata/birda && docker build -t birda .`
