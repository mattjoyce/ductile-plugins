"""Tests for hibp_radar plugin."""
from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(__file__))

from run import handle_request, id_hash


# ── fixtures ──────────────────────────────────────────────────────────────────

ADOBE_BREACH = {
    "Name": "Adobe",
    "BreachDate": "2013-10-04",
    "DataClasses": ["Email addresses", "Password hints", "Passwords"],
}
LINKEDIN_BREACH = {
    "Name": "LinkedIn",
    "BreachDate": "2012-05-05",
    "DataClasses": ["Email addresses", "Passwords"],
}


def make_config(*, api_key: str = "test-key", with_domain: bool = False) -> dict:
    cfg = {
        "hibp_api_key": api_key,
        "user_agent_email": "test@example.com",
        "identities": [
            {"label": "primary", "email": "test@example.com"},
        ],
        "rate_limit_ms": 0,  # no real sleep in tests
        "timeout_seconds": 5,
    }
    if with_domain:
        cfg["domains"] = ["example.com"]
    return cfg


# ── tests ─────────────────────────────────────────────────────────────────────

class TestHealth(unittest.TestCase):
    def test_health_no_api_key_reports_disabled(self):
        resp = handle_request({"command": "health", "config": {}})
        self.assertEqual(resp["status"], "ok")
        self.assertIn("paid_api=DISABLED", resp["result"])

    def test_health_with_api_key_reports_ready(self):
        resp = handle_request({"command": "health", "config": make_config()})
        self.assertEqual(resp["status"], "ok")
        self.assertIn("paid_api=ready", resp["result"])


class TestPollNoKey(unittest.TestCase):
    def test_poll_without_api_key_skips_and_preserves_state(self):
        prior = {"identities": {"abc": {"label": "x", "breaches_seen": ["Old"], "last_polled_at": "t"}}}
        resp = handle_request({"command": "poll", "config": {"identities": []}, "state": prior})
        self.assertEqual(resp["status"], "ok")
        self.assertNotIn("events", resp)
        # State must be preserved (not zeroed out)
        self.assertEqual(resp["state_updates"]["identities"], prior["identities"])


class TestPollIdentityFirstRun(unittest.TestCase):
    @mock.patch("run.hibp_get", return_value=[ADOBE_BREACH, LINKEDIN_BREACH])
    def test_first_run_emits_event_per_breach_and_persists(self, _mock_get):
        resp = handle_request({
            "command": "poll",
            "config": make_config(),
            "state": {},
        })

        self.assertEqual(resp["status"], "ok")
        self.assertEqual(len(resp["events"]), 2)
        names = sorted(e["payload"]["breach_name"] for e in resp["events"])
        self.assertEqual(names, ["Adobe", "LinkedIn"])

        h = id_hash("test@example.com")
        self.assertIn(h, resp["state_updates"]["identities"])
        seen = resp["state_updates"]["identities"][h]["breaches_seen"]
        self.assertEqual(sorted(seen), ["Adobe", "LinkedIn"])

        # Privacy: label is stored, plaintext email is not in any state value
        identity_record = resp["state_updates"]["identities"][h]
        self.assertEqual(identity_record["label"], "primary")
        flat = repr(resp["state_updates"])
        self.assertNotIn("test@example.com", flat)


class TestPollIdentityDeltaOnly(unittest.TestCase):
    @mock.patch("run.hibp_get", return_value=[ADOBE_BREACH, LINKEDIN_BREACH])
    def test_subsequent_poll_with_no_new_breaches_emits_nothing(self, _mock_get):
        h = id_hash("test@example.com")
        prior_state = {
            "identities": {
                h: {"label": "primary", "breaches_seen": ["Adobe", "LinkedIn"], "last_polled_at": "t0"}
            },
            "domains": {},
        }
        resp = handle_request({
            "command": "poll",
            "config": make_config(),
            "state": prior_state,
        })

        self.assertEqual(resp["status"], "ok")
        self.assertNotIn("events", resp)
        # State preserved
        self.assertEqual(
            sorted(resp["state_updates"]["identities"][h]["breaches_seen"]),
            ["Adobe", "LinkedIn"],
        )

    @mock.patch("run.hibp_get", return_value=[ADOBE_BREACH, LINKEDIN_BREACH])
    def test_subsequent_poll_with_one_new_breach_emits_only_that_one(self, _mock_get):
        h = id_hash("test@example.com")
        prior_state = {
            "identities": {
                h: {"label": "primary", "breaches_seen": ["Adobe"], "last_polled_at": "t0"}
            },
            "domains": {},
        }
        resp = handle_request({
            "command": "poll",
            "config": make_config(),
            "state": prior_state,
        })

        self.assertEqual(resp["status"], "ok")
        self.assertEqual(len(resp["events"]), 1)
        self.assertEqual(resp["events"][0]["payload"]["breach_name"], "LinkedIn")
        self.assertEqual(resp["events"][0]["payload"]["surface"], "identity")
        self.assertEqual(resp["events"][0]["payload"]["label"], "primary")
        self.assertIn("Rotate credential", resp["events"][0]["payload"]["recommended"])


class TestSnapshotDeterminism(unittest.TestCase):
    @mock.patch("run.hibp_get", return_value=[LINKEDIN_BREACH, ADOBE_BREACH])
    def test_breaches_seen_is_sorted_regardless_of_hibp_response_order(self, _mock_get):
        """HIBP doesn't guarantee order; snapshot must be byte-stable anyway."""
        resp = handle_request({
            "command": "poll",
            "config": make_config(),
            "state": {},
        })
        h = id_hash("test@example.com")
        seen = resp["state_updates"]["identities"][h]["breaches_seen"]
        self.assertEqual(seen, sorted(seen))
        self.assertEqual(seen, ["Adobe", "LinkedIn"])

    def test_snapshot_top_level_keys_present_when_no_key(self):
        """Presence-stable: identities + domains keys always returned."""
        resp = handle_request({
            "command": "poll",
            "config": {},
            "state": {},
        })
        self.assertIn("identities", resp["state_updates"])
        self.assertIn("domains", resp["state_updates"])


class TestPollIdentityAuthFailure(unittest.TestCase):
    @mock.patch("run.hibp_get")
    def test_auth_failure_logs_warn_does_not_crash(self, mock_get):
        from run import HIBPError
        mock_get.side_effect = HIBPError("auth/permission (401): Unauthorized", status=401, retry=False)

        resp = handle_request({
            "command": "poll",
            "config": make_config(),
            "state": {},
        })

        self.assertEqual(resp["status"], "ok")
        self.assertNotIn("events", resp)
        warns = [l for l in resp["logs"] if l.get("level") == "warn"]
        self.assertTrue(any("breachedaccount[primary] failed" in l["message"] for l in warns))


class TestHandlePushWebhook(unittest.TestCase):
    def test_push_for_new_breach_emits_event(self):
        config = make_config(with_domain=True)
        resp = handle_request({
            "command": "handle",
            "config": config,
            "event": {
                "payload": {
                    "domain": "example.com",
                    "breach": ADOBE_BREACH,
                }
            },
            "state": {"identities": {}, "domains": {}},
        })

        self.assertEqual(resp["status"], "ok")
        self.assertEqual(len(resp["events"]), 1)
        ev = resp["events"][0]
        self.assertEqual(ev["payload"]["surface"], "domain")
        self.assertEqual(ev["payload"]["label"], "example.com")
        self.assertEqual(ev["payload"]["breach_name"], "Adobe")

    def test_push_for_already_seen_breach_emits_nothing(self):
        config = make_config(with_domain=True)
        resp = handle_request({
            "command": "handle",
            "config": config,
            "event": {
                "payload": {
                    "domain": "example.com",
                    "breach": ADOBE_BREACH,
                }
            },
            "state": {
                "identities": {},
                "domains": {"example.com": {"breaches_seen": ["push|Adobe"], "last_polled_at": "t0"}},
            },
        })

        self.assertEqual(resp["status"], "ok")
        self.assertNotIn("events", resp)

    def test_push_rejects_unconfigured_domain(self):
        config = make_config(with_domain=True)
        resp = handle_request({
            "command": "handle",
            "config": config,
            "event": {
                "payload": {
                    "domain": "not-in-config.com",
                    "breach": ADOBE_BREACH,
                }
            },
            "state": {"identities": {}, "domains": {}},
        })

        self.assertEqual(resp["status"], "error")
        self.assertIn("not in configured domains", resp["error"])


if __name__ == "__main__":
    unittest.main()
