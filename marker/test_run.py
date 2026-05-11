"""Tests for marker plugin."""
from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(__file__))

import run
from run import handle_request


def _exited_inspect(exit_code: int = 0, output_path: str = "/library/inbound/converted/doc-1.md") -> dict:
    return {
        "State": {"Status": "exited", "ExitCode": exit_code},
        "Config": {"Labels": {"marker.output_path": output_path, "marker.doc_id": "doc-1"}},
    }


def _running_inspect() -> dict:
    return {
        "State": {"Status": "running", "ExitCode": 0},
        "Config": {"Labels": {"marker.output_path": "/library/inbound/converted/doc-1.md", "marker.doc_id": "doc-1"}},
    }


def _created_inspect() -> dict:
    return {
        "State": {"Status": "created", "ExitCode": 0},
        "Config": {"Labels": {"marker.output_path": "/library/inbound/converted/doc-1.md", "marker.doc_id": "doc-1"}},
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


class TestHandle(unittest.TestCase):
    GET = "/library/inbound/raw/foo.pdf"
    PUT = "/library/inbound/converted/doc-1.md"

    def setUp(self):
        # handle() sweeps stale exited marker containers first — stub it out so
        # tests don't shell out to docker.
        p = mock.patch("run.DockerRunner.reap_exited", return_value=[])
        self.mock_reap = p.start()
        self.addCleanup(p.stop)

    @mock.patch("run.DockerRunner.run", return_value="abc123def456")
    def test_handle_get_put_spawns_and_returns_running(self, mock_run):
        resp = handle_request(
            {"command": "handle", "payload": {"get": self.GET, "put": self.PUT, "doc_id": "doc-1"}}
        )
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["result"], "running")
        self.assertEqual(resp["state_updates"]["state"], "running")
        self.assertEqual(resp["state_updates"]["job_id"], "abc123def456")
        self.assertEqual(resp["state_updates"]["output_path"], self.PUT)
        # marker image is called with get/put as its two positional args
        _image, mounts, args = mock_run.call_args.args[0:3]
        self.assertEqual(args, [self.GET, self.PUT])
        # the library volume is mounted at /library (default name)
        vols = {(v, t): ro for v, t, ro in mounts}
        self.assertIn(("parsem_library", "/library"), vols)
        self.assertIn(("marker_models", "/root/.cache/datalab"), vols)
        # passed as a label too, for status
        labels = mock_run.call_args.kwargs["labels"]
        self.assertEqual(labels["marker.output_path"], self.PUT)
        self.assertEqual(labels["marker.doc_id"], "doc-1")

    @mock.patch("run.DockerRunner.run", return_value="vol0vol0vol0")
    def test_handle_library_volume_from_config(self, mock_run):
        resp = handle_request(
            {
                "command": "handle",
                "config": {"library_volume": "my_lib_vol"},
                "payload": {"get": self.GET, "put": self.PUT},
            }
        )
        self.assertEqual(resp["status"], "ok")
        _image, mounts, _args = mock_run.call_args.args[0:3]
        self.assertIn(("my_lib_vol", "/library"), {(v, t) for v, t, _ in mounts})

    @mock.patch("run.DockerRunner.run", return_value="vol1vol1vol1")
    def test_handle_library_volume_payload_overrides_default_but_not_config(self, mock_run):
        # config wins over payload wins over default
        resp = handle_request(
            {
                "command": "handle",
                "config": {"library_volume": "cfg_vol"},
                "payload": {"get": self.GET, "put": self.PUT, "library_volume": "payload_vol"},
            }
        )
        self.assertEqual(resp["status"], "ok")
        _image, mounts, _args = mock_run.call_args.args[0:3]
        self.assertIn(("cfg_vol", "/library"), {(v, t) for v, t, _ in mounts})

    @mock.patch("run.DockerRunner.run", return_value="bc0bc0bc0bc0")
    def test_handle_backcompat_source_output_dir_doc_id(self, mock_run):
        resp = handle_request(
            {
                "command": "handle",
                "payload": {
                    "source": "/library/originals/7.pdf",
                    "output_dir": "/library/inbound/converted",
                    "doc_id": "7",
                },
            }
        )
        self.assertEqual(resp["status"], "ok")
        _image, _mounts, args = mock_run.call_args.args[0:3]
        self.assertEqual(args, ["/library/originals/7.pdf", "/library/inbound/converted/7.md"])
        self.assertEqual(mock_run.call_args.kwargs["labels"]["marker.doc_id"], "7")

    @mock.patch("run.DockerRunner.run", return_value="evt0evt0evt0")
    def test_handle_reads_inputs_from_event_payload(self, mock_run):
        # Pipeline-dispatched `handle` jobs receive the with:-remapped inputs
        # under request["event"]["payload"], not the top-level "payload" key.
        resp = handle_request(
            {
                "command": "handle",
                "payload": {"type": "parsem.needs_marker", "payload": {"doc_id": "doc-1"}},
                "event": {"type": "parsem.needs_marker", "payload": {"get": self.GET, "put": self.PUT}},
            }
        )
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["result"], "running")
        _image, _mounts, args = mock_run.call_args.args[0:3]
        self.assertEqual(args, [self.GET, self.PUT])

    @mock.patch("run.DockerRunner.run", return_value="evt1evt1evt1")
    def test_event_payload_overrides_raw_payload(self, mock_run):
        resp = handle_request(
            {
                "command": "handle",
                "payload": {"get": "/library/wrong.pdf", "put": "/library/wrong.md"},
                "event": {"payload": {"get": self.GET, "put": self.PUT}},
            }
        )
        self.assertEqual(resp["status"], "ok")
        _image, _mounts, args = mock_run.call_args.args[0:3]
        self.assertEqual(args, [self.GET, self.PUT])

    def test_handle_missing_get_returns_error(self):
        resp = handle_request({"command": "handle", "payload": {"put": self.PUT}})
        self.assertEqual(resp["status"], "error")
        self.assertIn("get", resp["error"])

    def test_handle_missing_put_returns_error(self):
        resp = handle_request({"command": "handle", "payload": {"get": self.GET}})
        self.assertEqual(resp["status"], "error")
        self.assertIn("put", resp["error"])

    def test_handle_relative_path_returns_error(self):
        resp = handle_request({"command": "handle", "payload": {"get": "rel/foo.pdf", "put": self.PUT}})
        self.assertEqual(resp["status"], "error")
        self.assertIn("invalid get", resp["error"])

    def test_handle_traversal_path_returns_error(self):
        resp = handle_request(
            {"command": "handle", "payload": {"get": "/library/../etc/shadow", "put": self.PUT}}
        )
        self.assertEqual(resp["status"], "error")
        self.assertIn("invalid get", resp["error"])

    @mock.patch("run.DockerRunner.run", return_value="warn0warn0wa")
    def test_handle_path_outside_library_warns_but_spawns(self, _mock_run):
        resp = handle_request(
            {"command": "handle", "payload": {"get": "/other/foo.pdf", "put": "/other/foo.md"}}
        )
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["result"], "running")
        self.assertTrue(any("not under /library/" in log["message"] for log in resp["logs"]))

    @mock.patch("run.DockerRunner.run", side_effect=RuntimeError("no such volume: parsem_library"))
    def test_handle_docker_error_returns_error_response(self, mock_run):
        resp = handle_request({"command": "handle", "payload": {"get": self.GET, "put": self.PUT}})
        self.assertEqual(resp["status"], "error")
        self.assertEqual(mock_run.call_count, 1)
        self.assertIn("no such volume", resp["error"])

    @mock.patch("run.DockerRunner.run", return_value="reap0reap0re")
    def test_handle_reaps_exited_marker_containers_first(self, _mock_run):
        self.mock_reap.return_value = ["old1", "old2"]
        resp = handle_request({"command": "handle", "payload": {"get": self.GET, "put": self.PUT}})
        self.assertEqual(resp["status"], "ok")
        self.mock_reap.assert_called_once_with("marker:latest")
        self.assertTrue(any("reaped 2 exited marker" in log["message"] for log in resp["logs"]))


class TestDockerRunnerReap(unittest.TestCase):
    @mock.patch("run.DockerRunner.rm")
    @mock.patch("run.subprocess.run")
    def test_reap_exited_removes_each(self, mock_sp, mock_rm):
        mock_sp.return_value = mock.Mock(returncode=0, stdout="cidA\ncidB\n", stderr="")
        out = run.DockerRunner.reap_exited("marker:latest")
        self.assertEqual(out, ["cidA", "cidB"])
        argv = mock_sp.call_args.args[0]
        self.assertIn("ancestor=marker:latest", argv)
        self.assertIn("status=exited", argv)
        self.assertEqual(mock_rm.call_args_list, [mock.call("cidA"), mock.call("cidB")])

    @mock.patch("run.DockerRunner.rm")
    @mock.patch("run.subprocess.run")
    def test_reap_exited_empty_when_none(self, mock_sp, mock_rm):
        mock_sp.return_value = mock.Mock(returncode=0, stdout="\n", stderr="")
        self.assertEqual(run.DockerRunner.reap_exited("marker:latest"), [])
        mock_rm.assert_not_called()

    @mock.patch("run.subprocess.run", side_effect=FileNotFoundError)
    def test_reap_exited_tolerates_no_docker(self, _mock_sp):
        self.assertEqual(run.DockerRunner.reap_exited("marker:latest"), [])


class TestDockerRunnerMountFlags(unittest.TestCase):
    @mock.patch("run.subprocess.run")
    def test_run_emits_mount_type_volume_flags(self, mock_sp):
        mock_sp.return_value = mock.Mock(returncode=0, stdout="cid123\n", stderr="")
        cid = run.DockerRunner.run(
            "img:latest",
            [("marker_models", "/root/.cache/datalab", False), ("parsem_library", "/library", False)],
            ["/library/in.pdf", "/library/out.md"],
            labels={"marker.doc_id": "x"},
        )
        self.assertEqual(cid, "cid123")
        argv = mock_sp.call_args.args[0]
        joined = " ".join(argv)
        self.assertIn("--mount type=volume,source=marker_models,target=/root/.cache/datalab", joined)
        self.assertIn("--mount type=volume,source=parsem_library,target=/library", joined)
        self.assertNotIn("-v ", joined)  # no legacy -v binds

    @mock.patch("run.subprocess.run")
    def test_run_readonly_mount_adds_readonly(self, mock_sp):
        mock_sp.return_value = mock.Mock(returncode=0, stdout="cid\n", stderr="")
        run.DockerRunner.run("img", [("vol", "/m", True)], ["a"])
        argv = mock_sp.call_args.args[0]
        self.assertIn("type=volume,source=vol,target=/m,readonly", " ".join(argv))


class TestStatus(unittest.TestCase):
    def test_status_missing_job_id_returns_error(self):
        resp = handle_request({"command": "status", "payload": {}})
        self.assertEqual(resp["status"], "error")

    @mock.patch("run.DockerRunner.inspect", return_value=_created_inspect())
    def test_status_created_returns_queued(self, _mock_inspect):
        resp = handle_request({"command": "status", "payload": {"job_id": "abc"}})
        self.assertEqual(resp["status"], "ok")
        self.assertEqual(resp["result"], "queued")

    @mock.patch("run.DockerRunner.inspect", return_value=_running_inspect())
    def test_status_running_returns_running(self, _mock_inspect):
        resp = handle_request({"command": "status", "payload": {"job_id": "abc"}})
        self.assertEqual(resp["result"], "running")

    @mock.patch("run.DockerRunner.rm")
    @mock.patch("run.DockerRunner.inspect", return_value=_exited_inspect(exit_code=0))
    def test_status_exited_zero_returns_ready_with_realpath(self, _mock_inspect, _mock_rm):
        resp = handle_request({"command": "status", "payload": {"job_id": "abc"}})
        self.assertEqual(resp["result"], "ready")
        self.assertEqual(resp["state_updates"]["state"], "ready")
        # realpath of a path with no symlink components is the path itself
        self.assertEqual(
            resp["state_updates"]["output_path"],
            os.path.realpath("/library/inbound/converted/doc-1.md"),
        )
        _mock_rm.assert_called_once_with("abc")

    @mock.patch("run.DockerRunner.rm")
    @mock.patch("run.DockerRunner.logs", return_value="Traceback: marker crashed")
    @mock.patch("run.DockerRunner.inspect", return_value=_exited_inspect(exit_code=1))
    def test_status_exited_nonzero_returns_failed_with_logs(self, _mock_inspect, _mock_logs, _mock_rm):
        resp = handle_request({"command": "status", "payload": {"job_id": "abc"}})
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
    def test_status_exited_nonzero_with_oom_logs_tagged_cuda_oom(self, _mi, _ml, _mr):
        resp = handle_request({"command": "status", "payload": {"job_id": "abc"}})
        self.assertEqual(resp["state_updates"]["error_type"], "cuda_oom")

    @mock.patch("run.DockerRunner.rm")
    @mock.patch("run.DockerRunner.logs", return_value="ran out of memory mid-page")
    @mock.patch("run.DockerRunner.inspect", return_value=_exited_inspect(exit_code=137))
    def test_status_exit_137_with_oom_text_tagged_cuda_oom(self, _mi, _ml, _mr):
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
