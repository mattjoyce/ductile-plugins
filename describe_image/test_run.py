#!/usr/bin/env python3
"""Offline tests for describe_image: no network, call_claude is stubbed."""
import json, os, subprocess, sys, tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import run  # noqa: E402

PNG = bytes.fromhex("89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4890000000d4944415478da63f8cfc0f01f000501020034b0c7f80000000049454e44ae426082")


def test_describe_skip_delete(tmp: Path):
    img = tmp / "a.png"
    img.write_bytes(PNG)
    run.call_claude = lambda *a, **k: "A tiny red pixel on a transparent ground."
    ev = {"payload": {"root": str(tmp), "path": "a.png", "change_type": "created"}}
    r = run.handle({}, ev, {"anthropic-api-key": "k"})
    assert r["status"] == "ok", r
    assert r["events"][0]["type"] == "image.described"
    sidecar = tmp / "a.png.md"
    text = sidecar.read_text()
    assert text.startswith("---\n") and "tiny red pixel" in text and "sha256:" in text

    r = run.handle({}, ev, {"anthropic-api-key": "k"})
    assert r["events"][0]["payload"]["reason"] == "unchanged", r

    ev["payload"]["change_type"] = "deleted"
    r = run.handle({}, ev, {"anthropic-api-key": "k"})
    assert r["events"][0]["type"] == "image.orphan_removed" and not sidecar.exists()


def test_missing_secret(tmp: Path):
    (tmp / "b.jpg").write_bytes(b"\xff\xd8\xff")
    ev = {"payload": {"root": str(tmp), "path": "b.jpg"}}
    r = run.handle({}, ev, {})
    assert r["status"] == "error" and "not delivered" in r["error"]


def test_downscale(tmp: Path):
    from PIL import Image
    img = tmp / "big.jpg"
    Image.new("RGB", (4000, 3000), (200, 120, 40)).save(img, "JPEG", quality=95)
    seen = {}
    def fake(api_key, model, effort, max_tokens, prompt, media_type, data_b64, timeout):
        seen["media_type"] = media_type; seen["bytes"] = len(data_b64) * 3 // 4
        return "An orange rectangle."
    run.call_claude = fake
    ev = {"payload": {"root": str(tmp), "path": "big.jpg", "change_type": "created"}}
    r = run.handle({"max_edge": 1000}, ev, {"anthropic-api-key": "k"})
    assert r["status"] == "ok", r
    assert seen["media_type"] == "image/jpeg" and seen["bytes"] < img.stat().st_size
    assert any("downscaled 4000x3000" in l["message"] for l in r["logs"]), r["logs"]
    # sha in the sidecar is of the original file, so a re-fire is still a no-op
    r = run.handle({"max_edge": 1000}, ev, {"anthropic-api-key": "k"})
    assert r["events"][0]["payload"]["reason"] == "unchanged"
    # byte cap that no encoding can meet -> skipped, no API call
    run.call_claude = lambda *a, **k: (_ for _ in ()).throw(AssertionError("should not call"))
    (tmp / "big.jpg.md").unlink()
    r = run.handle({"max_edge": 1000, "max_image_bytes": 10}, ev, {"anthropic-api-key": "k"})
    assert r["events"][0]["payload"]["reason"] == "too_large_after_downscale", r


def test_protocol_health():
    req = {"command": "health", "config": {}, "secrets": {"anthropic-api-key": "k"}}
    out = subprocess.run([sys.executable, str(HERE / "run.py")], input=json.dumps(req), capture_output=True, text=True, check=True)
    assert json.loads(out.stdout)["status"] == "ok"


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as d:
        test_describe_skip_delete(Path(d))
    with tempfile.TemporaryDirectory() as d:
        test_missing_secret(Path(d))
    with tempfile.TemporaryDirectory() as d:
        test_downscale(Path(d))
    test_protocol_health()
    print("all tests passed")
