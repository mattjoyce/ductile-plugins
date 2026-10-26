# pdf_text

Turns a PDF into a text file beside it with `pdftotext`, so any pipeline can chain extraction instead of each plugin shelling out on its own. Needs `poppler-utils` on the gateway's path.

## Commands

- `handle` (write): extract one PDF and write the text.
- `health` (read): `pdftotext -v` runs and every `root` is a folder.

## Configuration

| Key | Default | Purpose |
|---|---|---|
| `root` | none, required | A folder, or a list of folders, a PDF or output file may live under. Unset, every path is refused. A relative path is taken against the first |
| `output_dir` | none | Write the text here as `<pdf name>.txt` instead of beside the PDF, for a source folder that is read-only. Must lie under a root |
| `layout` | `true` | Pass `-layout`, which keeps columns and tables aligned. `false` gives reading order |
| `timeout` | `60` | Seconds one `pdftotext` run may take |

## Input (`handle`)

| Field | Purpose |
|---|---|
| `pdf` (or `input_file`) | Path of the PDF, under a root. Refused outside them, or when not a regular file |
| `output_dir` | As the config key, for this one PDF; wins over the config |
| `output_file` | Exact path for the text, under a root; wins over `output_dir`. Default `<pdf>.txt` |

The text is written whole or not at all (temporary file, then rename).

## Output

One `pdf_text.completed` event:

| Field | Value |
|---|---|
| `input_file` | the text file written, so the next step reads it as its input |
| `pdf` | the PDF |
| `empty` | `true` when pdftotext succeeded but found no text (a scan); route those to OCR |
| everything else | the incoming payload fields, unchanged |

Failures: a nonzero `pdftotext` exit is an error naming the code (no retry, the file will not heal). A missing binary or a timeout is an error with `retry: true`.

`fact_outputs` stores `pdf_text.snapshot` with `last_run`, `executions_count`, `last_pdf` and `last_empty`.

## Pipeline example

```yaml
- on: agentmail.pdf
  call: pdf_text.handle
- on: pdf_text.completed
  call: fabric_mail.handle
```
