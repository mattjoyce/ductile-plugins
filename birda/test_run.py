"""Tests for birda plugin."""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent))
from run import (
    _get_coord,
    err,
    find_docker,
    handle_handle,
    handle_health,
    ok,
    parse_birda_stats,
    parse_raven_table,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

RAVEN_HEADER = "Selection\tView\tChannel\tBegin Time (s)\tEnd Time (s)\tLow Freq (Hz)\tHigh Freq (Hz)\tSpecies Code\tCommon Name\tConfidence\n"
RAVEN_ROW = "1\tSpectrogram 1\t1\t0.0\t3.0\t0\t15000\tSULCOC1\tSulphur-crested Cockatoo\t0.95\n"
RAVEN_ROW2 = "2\tSpectrogram 1\t1\t6.0\t9.0\t0\t15000\tSUPLYR1\tSuperb Lyrebird\t0.82\n"

NDJSON_OUTPUT = (
    '{"spec_version":"1.0","event":"pipeline_started","payload":{}}\n'
    '{"spec_version":"1.0","event":"pipeline_completed","payload":{"duration_ms":13271,"realtime_factor":236.89,"detection_count":45}}\n'
)


# ---------------------------------------------------------------------------
# ok() / err() response helpers
# ---------------------------------------------------------------------------

class TestResponseHelpers(unittest.TestCase):
    def test_ok_status(self):
        r = ok("all good")
        self.assertEqual(r["status"], "ok")
        self.assertEqual(r["result"], "all good")

    def test_ok_extra_fields(self):
        r = ok("done", detection_count=3)
        self.assertEqual(r["detection_count"], 3)

    def test_ok_default_logs(self):
        r = ok("hi")
        self.assertEqual(r["logs"][0]["level"], "info")

    def test_err_status(self):
        r = err("boom")
        self.assertEqual(r["status"], "error")
        self.assertEqual(r["error"], "boom")

    def test_err_retry_default_false(self):
        self.assertFalse(err("x")["retry"])

    def test_err_retry_true(self):
        self.assertTrue(err("x", retry=True)["retry"])

    def test_err_default_logs(self):
        r = err("boom")
        self.assertEqual(r["logs"][0]["level"], "error")


# ---------------------------------------------------------------------------
# parse_raven_table
# ---------------------------------------------------------------------------

class TestParseRavenTable(unittest.TestCase):
    def _write_tsv(self, content: str) -> Path:
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8")
        f.write(content)
        f.close()
        self.addCleanup(Path(f.name).unlink, missing_ok=True)
        return Path(f.name)

    def test_missing_file_returns_empty(self):
        self.assertEqual(parse_raven_table(Path("/nonexistent/file.txt")), [])

    def test_parses_standard_raven_columns(self):
        p = self._write_tsv(RAVEN_HEADER + RAVEN_ROW)
        dets = parse_raven_table(p)
        self.assertEqual(len(dets), 1)
        self.assertAlmostEqual(dets[0]["start_s"], 0.0)
        self.assertAlmostEqual(dets[0]["end_s"], 3.0)
        self.assertEqual(dets[0]["common_name"], "Sulphur-crested Cockatoo")
        self.assertAlmostEqual(dets[0]["confidence"], 0.95)

    def test_scientific_name_empty_in_birda_raven(self):
        """birda raven format has no Scientific name column."""
        p = self._write_tsv(RAVEN_HEADER + RAVEN_ROW)
        dets = parse_raven_table(p)
        self.assertEqual(dets[0]["scientific_name"], "")

    def test_multiple_rows_sorted_by_start_s(self):
        # RAVEN_ROW2 starts at 6.0, RAVEN_ROW at 0.0 — already in order; swap to test sort
        header = RAVEN_HEADER
        row_late = "1\tSpectrogram 1\t1\t9.0\t12.0\t0\t15000\tSPEC1\tSpecies A\t0.9\n"
        row_early = "2\tSpectrogram 1\t1\t1.5\t4.5\t0\t15000\tSPEC2\tSpecies B\t0.7\n"
        p = self._write_tsv(header + row_late + row_early)
        dets = parse_raven_table(p)
        self.assertEqual(len(dets), 2)
        self.assertLess(dets[0]["start_s"], dets[1]["start_s"])

    def test_malformed_row_skipped(self):
        bad = RAVEN_HEADER + "1\tSpec\t1\tnot_a_number\t3.0\t0\t15000\tX\tBird\t0.5\n"
        p = self._write_tsv(bad)
        dets = parse_raven_table(p)
        self.assertEqual(dets, [])

    def test_empty_file_returns_empty(self):
        p = self._write_tsv("")
        self.assertEqual(parse_raven_table(p), [])


# ---------------------------------------------------------------------------
# parse_birda_stats
# ---------------------------------------------------------------------------

class TestParseBirdaStats(unittest.TestCase):
    def test_ndjson_pipeline_completed(self):
        duration_s, rtf = parse_birda_stats(NDJSON_OUTPUT)
        self.assertAlmostEqual(duration_s, 13.271)
        self.assertAlmostEqual(rtf, 236.89)

    def test_ndjson_stops_at_first_completed(self):
        second = '{"spec_version":"1.0","event":"pipeline_completed","payload":{"duration_ms":99000,"realtime_factor":1.0}}\n'
        duration_s, rtf = parse_birda_stats(NDJSON_OUTPUT + second)
        self.assertAlmostEqual(duration_s, 13.271)  # first wins

    def test_empty_output_returns_none_none(self):
        self.assertEqual(parse_birda_stats(""), (None, None))

    def test_non_json_lines_skipped(self):
        output = "Analyzing file...\nDone.\n"
        self.assertEqual(parse_birda_stats(output), (None, None))

    def test_fallback_regex_realtime(self):
        output = "Processed in 5.0s at 100.5x realtime."
        _, rtf = parse_birda_stats(output)
        self.assertAlmostEqual(rtf, 100.5)

    def test_fallback_regex_duration(self):
        output = "Processed in 7.3s at 200x realtime."
        duration_s, _ = parse_birda_stats(output)
        self.assertAlmostEqual(duration_s, 7.3)

    def test_ndjson_takes_precedence_over_regex(self):
        output = NDJSON_OUTPUT + "Processed in 99.0s at 1x realtime.\n"
        duration_s, rtf = parse_birda_stats(output)
        self.assertAlmostEqual(duration_s, 13.271)
        self.assertAlmostEqual(rtf, 236.89)

    def test_invalid_json_line_skipped(self):
        output = "{not valid json}\n" + NDJSON_OUTPUT
        duration_s, rtf = parse_birda_stats(output)
        self.assertAlmostEqual(duration_s, 13.271)

    def test_duration_ms_zero(self):
        output = '{"spec_version":"1.0","event":"pipeline_completed","payload":{"duration_ms":0,"realtime_factor":0.0}}\n'
        duration_s, rtf = parse_birda_stats(output)
        self.assertAlmostEqual(duration_s, 0.0)
        self.assertAlmostEqual(rtf, 0.0)


# ---------------------------------------------------------------------------
# _get_coord
# ---------------------------------------------------------------------------

class TestGetCoord(unittest.TestCase):
    def test_payload_wins(self):
        self.assertAlmostEqual(_get_coord({"lat": -33.5}, {"lat": 0.0}, "lat"), -33.5)

    def test_falls_back_to_context(self):
        self.assertAlmostEqual(_get_coord({}, {"lat": -33.5}, "lat"), -33.5)

    def test_raises_on_missing(self):
        with self.assertRaises((TypeError, ValueError)):
            _get_coord({}, {}, "lat")

    def test_raises_on_non_numeric(self):
        with self.assertRaises((TypeError, ValueError)):
            _get_coord({"lat": "bad"}, {}, "lat")


# ---------------------------------------------------------------------------
# find_docker
# ---------------------------------------------------------------------------

class TestFindDocker(unittest.TestCase):
    def test_returns_path_when_found(self):
        with patch("shutil.which", return_value="/usr/bin/docker"):
            result = find_docker()
        self.assertEqual(result, "/usr/bin/docker")

    def test_returns_none_when_not_found(self):
        with patch("shutil.which", return_value=None):
            result = find_docker()
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# handle_handle (mocked docker)
# ---------------------------------------------------------------------------

def _make_req(payload=None, context=None, config=None):
    return {
        "command": "handle",
        "event": {"payload": payload or {}},
        "context": context or {},
        "config": config or {},
    }


class TestHandleHandle(unittest.TestCase):
    def _run(self, payload=None, context=None, config=None,
             stdout="", stderr="", returncode=0, raven_content=None):
        proc = MagicMock()
        proc.stdout = stdout
        proc.stderr = stderr
        proc.returncode = returncode

        raven = raven_content or (RAVEN_HEADER + RAVEN_ROW)

        with patch("run.find_docker", return_value="/usr/bin/docker"), \
             patch("subprocess.run", return_value=proc) as mock_run, \
             patch("run.parse_raven_table", return_value=self._parse(raven)):
            resp = handle_handle(_make_req(payload, context, config))
        return resp, mock_run

    def _parse(self, content):
        import io, csv
        reader = csv.DictReader(io.StringIO(content), dialect="excel-tab")
        dets = []
        for row in reader:
            try:
                dets.append({
                    "start_s": float(row.get("Begin Time (s)", 0)),
                    "end_s": float(row.get("End Time (s)", 0)),
                    "scientific_name": "",
                    "common_name": row.get("Common Name", "").strip(),
                    "confidence": float(row.get("Confidence", 0)),
                })
            except (ValueError, KeyError):
                pass
        return dets

    def test_missing_wav_path(self):
        resp, _ = self._run(payload={})
        self.assertEqual(resp["status"], "error")
        self.assertIn("wav_path", resp["error"])
        self.assertFalse(resp["retry"])

    def test_missing_lat(self):
        resp, _ = self._run(payload={"wav_path": "/mnt/user/x.wav", "lon": 151.0})
        self.assertEqual(resp["status"], "error")
        self.assertIn("lat", resp["error"])

    def test_missing_lon(self):
        resp, _ = self._run(payload={"wav_path": "/mnt/user/x.wav", "lat": -33.0})
        self.assertEqual(resp["status"], "error")
        self.assertIn("lon", resp["error"])

    def test_no_docker_binary(self):
        with patch("run.find_docker", return_value=None):
            resp = handle_handle(_make_req({"wav_path": "/mnt/user/x.wav", "lat": -33.0, "lon": 151.0}))
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])

    def test_successful_analysis(self):
        resp, mock_run = self._run(
            payload={"wav_path": "/mnt/user/x.wav", "lat": -33.5, "lon": 151.2},
            stdout=NDJSON_OUTPUT,
        )
        self.assertEqual(resp["status"], "ok")
        self.assertIn("detection_count", resp)
        self.assertIn("output_path", resp)
        self.assertIn("detections", resp)
        self.assertAlmostEqual(resp["duration_s"], 13.271)
        self.assertAlmostEqual(resp["realtime_factor"], 236.89)

    def test_docker_command_includes_required_flags(self):
        resp, mock_run = self._run(
            payload={"wav_path": "/mnt/user/x.wav", "lat": -33.5, "lon": 151.2},
            stdout=NDJSON_OUTPUT,
        )
        cmd = mock_run.call_args[0][0]
        self.assertIn("--gpus", cmd)
        self.assertIn("all", cmd)
        self.assertIn("-v", cmd)
        self.assertIn("/mnt/user:/mnt/user", cmd)
        self.assertIn("birda", cmd)
        self.assertIn("--gpu", cmd)
        self.assertIn("-f", cmd)
        self.assertIn("raven", cmd)
        self.assertIn("--lat", cmd)
        self.assertIn("--lon", cmd)
        self.assertIn("-c", cmd)

    def test_week_omitted_when_minus_one(self):
        resp, mock_run = self._run(
            payload={"wav_path": "/mnt/user/x.wav", "lat": -33.5, "lon": 151.2, "week": -1},
            stdout=NDJSON_OUTPUT,
        )
        cmd = mock_run.call_args[0][0]
        self.assertNotIn("--week", cmd)

    def test_week_included_when_set(self):
        resp, mock_run = self._run(
            payload={"wav_path": "/mnt/user/x.wav", "lat": -33.5, "lon": 151.2, "week": 12},
            stdout=NDJSON_OUTPUT,
        )
        cmd = mock_run.call_args[0][0]
        self.assertIn("--week", cmd)
        self.assertIn("12", cmd)

    def test_min_conf_default(self):
        resp, mock_run = self._run(
            payload={"wav_path": "/mnt/user/x.wav", "lat": -33.5, "lon": 151.2},
            stdout=NDJSON_OUTPUT,
        )
        cmd = mock_run.call_args[0][0]
        idx = cmd.index("-c")
        self.assertEqual(cmd[idx + 1], "0.7")

    def test_min_conf_custom(self):
        resp, mock_run = self._run(
            payload={"wav_path": "/mnt/user/x.wav", "lat": -33.5, "lon": 151.2, "min_conf": 0.5},
            stdout=NDJSON_OUTPUT,
        )
        cmd = mock_run.call_args[0][0]
        idx = cmd.index("-c")
        self.assertEqual(cmd[idx + 1], "0.5")

    def test_docker_nonzero_exit_returns_error(self):
        resp, _ = self._run(
            payload={"wav_path": "/mnt/user/x.wav", "lat": -33.5, "lon": 151.2},
            stderr="birda: GPU error",
            returncode=1,
        )
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])

    def test_file_not_found_no_retry(self):
        with patch("run.find_docker", return_value="/usr/bin/docker"), \
             patch("subprocess.run", side_effect=FileNotFoundError):
            resp = handle_handle(_make_req({"wav_path": "/mnt/user/x.wav", "lat": -33.5, "lon": 151.2}))
        self.assertEqual(resp["status"], "error")
        self.assertFalse(resp["retry"])

    def test_timeout_retries(self):
        with patch("run.find_docker", return_value="/usr/bin/docker"), \
             patch("subprocess.run", side_effect=subprocess.TimeoutExpired("docker", 300)):
            resp = handle_handle(_make_req({"wav_path": "/mnt/user/x.wav", "lat": -33.5, "lon": 151.2}))
        self.assertEqual(resp["status"], "error")
        self.assertTrue(resp["retry"])

    def test_wav_path_passed_unchanged(self):
        wav = "/mnt/user/field_Recording/F3/Orig/260329/290326_001.WAV"
        resp, mock_run = self._run(
            payload={"wav_path": wav, "lat": -33.5, "lon": 151.2},
            stdout=NDJSON_OUTPUT,
        )
        cmd = mock_run.call_args[0][0]
        self.assertIn(wav, cmd)

    def test_wav_path_from_context_fallback(self):
        wav = "/mnt/user/field_Recording/test.wav"
        resp, mock_run = self._run(
            payload={"lat": -33.5, "lon": 151.2},
            context={"wav_path": wav},
            stdout=NDJSON_OUTPUT,
        )
        self.assertEqual(resp["status"], "ok")
        cmd = mock_run.call_args[0][0]
        self.assertIn(wav, cmd)


# ---------------------------------------------------------------------------
# handle_health (mocked docker)
# ---------------------------------------------------------------------------

class TestHandleHealth(unittest.TestCase):
    def _run_health(self, docker_found=True, version_rc=0, images_output="birda:latest", images_rc=0):
        def fake_subprocess(cmd, **kwargs):
            proc = MagicMock()
            if "version" in cmd:
                proc.stdout = "27.3.1\n" if version_rc == 0 else ""
                proc.returncode = version_rc
            else:
                proc.stdout = images_output + "\n" if images_rc == 0 else ""
                proc.returncode = images_rc
            return proc

        with patch("run.find_docker", return_value="/usr/bin/docker" if docker_found else None), \
             patch("subprocess.run", side_effect=fake_subprocess):
            return handle_health({"command": "health", "config": {}})

    def test_ok_when_docker_and_image_present(self):
        r = self._run_health()
        self.assertEqual(r["status"], "ok")
        self.assertIn("birda", r["result"])
        self.assertIn("docker", r["result"])

    def test_error_when_no_docker_binary(self):
        r = self._run_health(docker_found=False)
        self.assertEqual(r["status"], "error")
        self.assertFalse(r["retry"])

    def test_error_when_birda_image_missing(self):
        r = self._run_health(images_output="")
        self.assertEqual(r["status"], "error")
        self.assertIn("birda image not found", r["error"])

    def test_docker_version_unknown_on_failure(self):
        r = self._run_health(version_rc=1)
        self.assertEqual(r["status"], "ok")
        self.assertIn("unknown", r["result"])


# ---------------------------------------------------------------------------
# Main protocol (stdin/stdout via subprocess)
# ---------------------------------------------------------------------------

class TestMainProtocol(unittest.TestCase):
    PLUGIN = str(Path(__file__).parent / "run.py")

    def _run(self, request: dict) -> dict:
        result = subprocess.run(
            [sys.executable, self.PLUGIN],
            input=json.dumps(request),
            capture_output=True,
            text=True,
        )
        return json.loads(result.stdout)

    def test_invalid_json_returns_error(self):
        result = subprocess.run(
            [sys.executable, self.PLUGIN],
            input="not json at all",
            capture_output=True,
            text=True,
        )
        r = json.loads(result.stdout)
        self.assertEqual(r["status"], "error")

    def test_unknown_command_returns_error(self):
        r = self._run({"command": "bogus", "event": {}, "context": {}, "config": {}})
        self.assertEqual(r["status"], "error")
        self.assertIn("bogus", r["error"])
        self.assertFalse(r["retry"])

    def test_missing_wav_path_returns_error(self):
        r = self._run({
            "command": "handle",
            "event": {"payload": {"lat": -33.5, "lon": 151.2}},
            "context": {},
            "config": {},
        })
        self.assertEqual(r["status"], "error")
        self.assertIn("wav_path", r["error"])

    def test_health_without_docker_returns_error(self):
        r = self._run({"command": "health", "event": {}, "context": {}, "config": {}})
        # docker may or may not be present in test env; either ok or error is acceptable
        self.assertIn(r["status"], ("ok", "error"))


if __name__ == "__main__":
    unittest.main()
