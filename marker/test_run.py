"""Tests for marker plugin."""
from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(__file__))

from run import handle_request


def _exited_inspect(exit_code: int = 0, output_path: str = "/data/library/doc-1.md") -> dict:
    return {
        "State": {"Status": "exited", "ExitCode": exit_code},
        "Config": {"Labels": {"marker.output_path": output_path, "marker.doc_id": "doc-1"}},
    }


def _running_inspect() -> dict:
    return {
        "State": {"Status": "running", "ExitCode": 0},
        "Config": {"Labels": {"marker.output_path": "/data/library/doc-1.md", "marker.doc_id": "doc-1"}},
    }


def _created_inspect() -> dict:
    return {
        "State": {"Status": "created", "ExitCode": 0},
        "Config": {"Labels": {"marker.output_path": "/data/library/doc-1.md", "marker.doc_id": "doc-1"}},
    }


class TestHealth(unittest.TestCase):
    @mock.patch("run.DockerRunner.image_exists", return_value=True)
    def test_health_ok_when_image_present(self, _mock_exists):
        resp = handle_request({"command": "health"})
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["result"], "healthy")

    @mock.patch("run.DockerRunner.image_exists", return_value=False)
    def test_health_degraded_when_image_missing(self, _mock_exists):
        resp = handle_request({"command": "health"})
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["result"], "degraded")
        self.assertTrue(any("not present" in log["message"] for log in resp["logs"]))


class TestSubmit(unittest.TestCase):
    BASE_PAYLOAD = {
        "source": "/data/library/in.pdf",
        "output_dir": "/data/library/out",
        "doc_id": "doc-1",
    }

    @mock.patch("run.DockerRunner.run", return_value="abc123def456")
    def test_submit_returns_running_with_job_id(self, _mock_run):
        resp = handle_request({"command": "submit", "payload": dict(self.BASE_PAYLOAD)})
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["result"], "running")
        self.assertEqual(resp["state_updates"]["state"], "running")
        self.assertEqual(resp["state_updates"]["job_id"], "abc123def456")

    def test_submit_missing_source_returns_error(self):
        payload = dict(self.BASE_PAYLOAD)
        del payload["source"]
        resp = handle_request({"command": "submit", "payload": payload})
        self.assertEqual(resp["status"], "error")
        self.assertIn("source", resp["error"])

    def test_submit_missing_output_dir_returns_error(self):
        payload = dict(self.BASE_PAYLOAD)
        del payload["output_dir"]
        resp = handle_request({"command": "submit", "payload": payload})
        self.assertEqual(resp["status"], "error")
        self.assertIn("output_dir", resp["error"])

    def test_submit_missing_doc_id_returns_error(self):
        payload = dict(self.BASE_PAYLOAD)
        del payload["doc_id"]
        resp = handle_request({"command": "submit", "payload": payload})
        self.assertEqual(resp["status"], "error")
        self.assertIn("doc_id", resp["error"])

    @mock.patch("run.DockerRunner.run", side_effect=RuntimeError("permission denied on socket"))
    def test_submit_docker_error_returns_error_response(self, mock_run):
        # OOM happens INSIDE the detached container — not detectable here.
        # `submit` only surfaces docker-daemon-level errors. No retries at this layer.
        resp = handle_request({"command": "submit", "payload": dict(self.BASE_PAYLOAD)})
        self.assertEqual(resp["status"], "error")
        self.assertEqual(mock_run.call_count, 1)
        self.assertIn("permission denied", resp["error"])


class TestStatus(unittest.TestCase):
    def test_status_missing_job_id_returns_error(self):
        resp = handle_request({"command": "status", "payload": {}})
        self.assertEqual(resp["status"], "error")

    @mock.patch("run.DockerRunner.inspect", return_value=_created_inspect())
    def test_status_created_returns_queued(self, _mock_inspect):
        resp = handle_request({"command": "status", "payload": {"job_id": "abc"}})
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["result"], "queued")
        self.assertEqual(resp["state_updates"]["state"], "queued")

    @mock.patch("run.DockerRunner.inspect", return_value=_running_inspect())
    def test_status_running_returns_running(self, _mock_inspect):
        resp = handle_request({"command": "status", "payload": {"job_id": "abc"}})
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["result"], "running")
        self.assertEqual(resp["state_updates"]["state"], "running")

    @mock.patch("run.DockerRunner.rm")
    @mock.patch("run.DockerRunner.inspect", return_value=_exited_inspect(exit_code=0))
    def test_status_exited_zero_returns_ready(self, _mock_inspect, _mock_rm):
        # Plugin trusts marker's exit code per the marker contract; it does
        # NOT verify the file exists on disk (the plugin runs inside ductile,
        # which has a different filesystem view than the marker host anyway).
        resp = handle_request({"command": "status", "payload": {"job_id": "abc"}})
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["result"], "ready")
        self.assertEqual(resp["state_updates"]["state"], "ready")
        self.assertEqual(resp["state_updates"]["output_path"], "/data/library/doc-1.md")
        _mock_rm.assert_called_once_with("abc")

    @mock.patch("run.DockerRunner.rm")
    @mock.patch("run.DockerRunner.logs", return_value="Traceback: marker crashed")
    @mock.patch("run.DockerRunner.inspect", return_value=_exited_inspect(exit_code=1))
    def test_status_exited_nonzero_returns_failed_with_logs(self, _mock_inspect, _mock_logs, _mock_rm):
        resp = handle_request({"command": "status", "payload": {"job_id": "abc"}})
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["result"], "failed")
        self.assertIn("marker exited 1", resp["state_updates"]["error"])
        self.assertIn("marker crashed", resp["state_updates"]["error"])
        self.assertEqual(resp["state_updates"]["error_type"], "exit_failure")

    @mock.patch("run.DockerRunner.rm")
    @mock.patch(
        "run.DockerRunner.logs",
        return_value="RuntimeError: CUDA out of memory. Tried to allocate 4.20 GiB",
    )
    @mock.patch("run.DockerRunner.inspect", return_value=_exited_inspect(exit_code=1))
    def test_status_exited_nonzero_with_oom_logs_tagged_cuda_oom(
        self, _mock_inspect, _mock_logs, _mock_rm
    ):
        # OOM inside the detached container surfaces here. error_type lets the
        # caller decide to retry-with-backoff (e.g. wait for llama-swap eviction).
        resp = handle_request({"command": "status", "payload": {"job_id": "abc"}})
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["result"], "failed")
        self.assertEqual(resp["state_updates"]["error_type"], "cuda_oom")

    @mock.patch("run.DockerRunner.rm")
    @mock.patch("run.DockerRunner.logs", return_value="ran out of memory mid-page")
    @mock.patch("run.DockerRunner.inspect", return_value=_exited_inspect(exit_code=137))
    def test_status_exit_137_with_oom_text_tagged_cuda_oom(
        self, _mock_inspect, _mock_logs, _mock_rm
    ):
        # OOM-killer exit code 137 is also OOM territory; classification driven
        # by log text not exit code.
        resp = handle_request({"command": "status", "payload": {"job_id": "abc"}})
        self.assertEqual(resp["state_updates"]["error_type"], "cuda_oom")

    @mock.patch("run.DockerRunner.inspect", side_effect=RuntimeError("No such container: deadbeef"))
    def test_status_unknown_job_id_returns_error(self, _mock_inspect):
        resp = handle_request({"command": "status", "payload": {"job_id": "deadbeef"}})
        self.assertEqual(resp["status"], "error")
        self.assertIn("No such container", resp["error"])


class TestUnknownCommand(unittest.TestCase):
    def test_unknown_command_returns_error(self):
        resp = handle_request({"command": "frobnicate"})
        self.assertEqual(resp["status"], "error")
        self.assertIn("unknown command", resp["error"])


if __name__ == "__main__":
    unittest.main()
