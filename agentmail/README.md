# agentmail

Saves the messages that arrive in an [AgentMail](https://agentmail.to) inbox into a local
spool, one folder per message, and stops there. Reading what a message means, and whether to
act on it, is a later step's job.

```text
mail ──▶ AgentMail inbox ──GET messages──▶ agentmail.poll ──▶ <spool_dir>/<stem>/
                                                  │
                                                  └─▶ agentmail.message.saved
```

## What it does

- **`poll`** lists the inbox and saves every message whose folder is not yet in the spool, at
  most `max_messages` a poll. Each folder, `<spool_dir>/<stem>` (the first 16 hex digits of the
  SHA-256 of the message id), holds:
  - `message.json`: the message exactly as AgentMail returns it, including `text`, `html`,
    `extracted_text`, `headers` and `authentication_results`
  - `attachments/`: each attachment, byte-exact, under a safe file name
  - `manifest.json`: the message id, when it was saved, the sha256 of `message.json`, and each
    attachment's id, name, original name, type, size and sha256

  It emits one `agentmail.message.saved` per folder saved.
- **`health`** reports whether the spool is writable, how many messages it holds, and whether
  the inbox answers the key.

## Design notes

- **The folder is the memory.** A message whose folder exists is not fetched again; there is no
  plugin state. Deleting a folder re-fetches that message.
- **Whole or nothing.** A folder is built as `.partial-<stem>-<pid>` and renamed into place only
  when every file is written, fsynced and checked. A crashed poll's partial folders are removed
  on the next poll. A reader that ignores dot-folders never sees half a message.
- **Checked bytes.** An attachment whose size differs from what the inbox reports, or a `.pdf`
  without a `%PDF` header, fails the message; its folder is not created and the next poll tries
  again. Other messages in the same poll still save.
- **The key stays with AgentMail.** It goes only to the API. The attachment download link is
  signed and is fetched without it.
- **Nothing is judged here.** Authentication results are kept as data. Who sent a message and
  whether to trust it is for the consumer to decide.
- **Content stays in the spool.** Events, logs and results carry ids, counts, timestamps and
  folder names, never a subject, sender or body.
- **The inbox is left alone.** Nothing is deleted, labelled or marked read.
- Run with `concurrency_safe: false` and `parallelism: 1`.

## Configuration

| Key | Required | Default | Notes |
|---|---|---|---|
| `inbox` | yes | | The inbox address, e.g. `me@agentmail.to` |
| `spool_dir` | yes | | Folder the messages are saved under |
| `base_url` | no | `https://api.agentmail.to` | |
| `page_size` | no | 50 | 1 to 100; 50 is AgentMail's page maximum |
| `max_messages` | no | 20 | Messages saved per poll; the next poll continues |
| `request_timeout_seconds` | no | 30 | Per call |

The API key is vault-native: grant the `agentmail_api_key` secret to this plugin. It arrives
over stdin in `secrets`, never in config.

```yaml
plugins:
  agentmail:
    enabled: true
    schedules:
      - every: 5m
    timeout: 120s
    max_attempts: 2
    config:
      inbox: me@agentmail.to
      spool_dir: /app/data/agentmail
```

## Tests

Stdlib only. `python3 -m unittest -v test_run` runs 21 tests against a fake AgentMail inbox
served over HTTP on localhost.
