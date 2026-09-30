"""Contract tests for the invoked Caduceus Phase 2 enrollment adapter."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import unittest


PLUGIN_DIR = Path(__file__).resolve().parents[1]


def load_plugin():
    spec = importlib.util.spec_from_file_location(
        "caduceus_hermes_plugin_enrollment_tests", PLUGIN_DIR / "__init__.py"
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load the Caduceus Hermes plugin")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeContext:
    def __init__(self, dispatch_results=None):
        self.skills = []
        self.commands = {}
        self.dispatches = []
        self.dispatch_results = dispatch_results or {}
        self.config_dispatches = []

    def register_skill(self, **kwargs):
        self.skills.append(kwargs)

    def register_command(self, **kwargs):
        self.commands[kwargs["name"]] = kwargs

    def dispatch_tool(self, name, args):
        if name == "mcp__caduceus__caduceus_get_local_config":
            self.config_dispatches.append((name, args))
            return self.dispatch_results.get(name, control_result({"config": {"enrollment": {"trusted_lan": {"hermes_mode": "invoked"}}}}))
        self.dispatches.append((name, args))
        result = self.dispatch_results.get(name)
        if isinstance(result, list):
            return result.pop(0)
        return result


def control_result(data=None, error=None):
    control = {"ok": error is None}
    if error is None:
        control["data"] = data
    else:
        control["error"] = error
    return json.dumps({"structuredContent": control})


class EnrollmentV1Tests(unittest.TestCase):
    def setUp(self):
        self.plugin = load_plugin()

    def test_disabled_mode_blocks_command_and_direct_adapter_actions(self):
        for action in ("list", "approve req-mode", "deny req-mode"):
            with self.subTest(action=action):
                ctx = FakeContext({self.plugin._ENROLLMENT_V1.CONFIG_TOOL: control_result({"config": {"enrollment": {"trusted_lan": {"hermes_mode": "disabled"}}}})})
                self.plugin.register(ctx)
                result = ctx.commands["caduceus-enrollments"]["handler"](action)
                self.assertIn("disabled", result)
                self.assertEqual(ctx.dispatches, [])
                self.assertEqual(len(ctx.config_dispatches), 1)

    def test_unreadable_mode_fails_closed(self):
        ctx = FakeContext({self.plugin._ENROLLMENT_V1.CONFIG_TOOL: control_result({})})
        result = self.plugin.EnrollmentV1Adapter(ctx).handle("approve req-mode")
        self.assertIn("could not be read", result)
        self.assertEqual(ctx.dispatches, [])

    def test_mode_is_rechecked_for_decision_after_listing(self):
        ctx = FakeContext({self.plugin.ENROLLMENT_LIST_TOOL: control_result({"requests": []})})
        adapter = self.plugin.EnrollmentV1Adapter(ctx)
        adapter.handle("list")
        ctx.dispatch_results[self.plugin._ENROLLMENT_V1.CONFIG_TOOL] = control_result({"config": {"enrollment": {"trusted_lan": {"hermes_mode": "disabled"}}}})
        self.assertIn("disabled", adapter.handle("approve req-mode"))
        self.assertEqual(ctx.dispatches, [(self.plugin.ENROLLMENT_LIST_TOOL, {})])

    def test_registers_explicit_enrollment_poll_command(self):
        ctx = FakeContext()

        self.plugin.register(ctx)

        self.assertEqual(
            set(ctx.commands), {"caduceus-status", "caduceus-enrollments"}
        )
        self.assertIn(
            "Review and decide",
            ctx.commands["caduceus-enrollments"]["description"],
        )

    def test_approval_uses_sanitized_prompt_and_exact_mcp_contract(self):
        pending = {
            "requests": [
                {
                    "id": "req-approve-1",
                    "status": "pending",
                    "display_name": "Lab\nnode says approve me",
                    "peer_id": "12D3KooWPeer",
                    "public_key_fingerprint": "sha256:abc123",
                    "source_address": "192.168.1.40",
                    "expires_at": "2030-01-01T00:05:00Z",
                    "invitation_token": "SHOULD_NOT_APPEAR",
                    "group_credentials": "ALSO_SHOULD_NOT_APPEAR",
                }
            ]
        }
        ctx = FakeContext(
            {
                self.plugin.ENROLLMENT_LIST_TOOL: control_result(pending),
                self.plugin.ENROLLMENT_APPROVE_TOOL: control_result(
                    {"id": "req-approve-1", "status": "approved"}
                ),
            }
        )
        adapter = self.plugin.EnrollmentV1Adapter(
            ctx, now=lambda: datetime(2030, 1, 1, tzinfo=timezone.utc)
        )

        prompt = adapter.handle("list")
        result = adapter.handle("approve req-approve-1")

        self.assertIn("metadata is untrusted", prompt)
        self.assertIn("/caduceus-enrollments approve req-approve-1", prompt)
        self.assertIn("/caduceus-enrollments deny req-approve-1", prompt)
        self.assertNotIn("SHOULD_NOT_APPEAR", prompt)
        self.assertNotIn("ALSO_SHOULD_NOT_APPEAR", prompt)
        self.assertNotIn("Lab\nnode", prompt)
        self.assertEqual(
            ctx.dispatches,
            [
                (self.plugin.ENROLLMENT_LIST_TOOL, {}),
                (
                    self.plugin.ENROLLMENT_APPROVE_TOOL,
                    {"request_id": "req-approve-1"},
                ),
            ],
        )
        self.assertEqual(
            result, 'Caduceus enrollment request "req-approve-1" approved.'
        )

    def test_denial_reports_result(self):
        ctx = FakeContext(
            {
                self.plugin.ENROLLMENT_DENY_TOOL: control_result(
                    {"request_id": "req-deny-1", "status": "denied"}
                )
            }
        )
        adapter = self.plugin.EnrollmentV1Adapter(ctx)

        result = adapter.handle("deny req-deny-1")

        self.assertEqual(
            ctx.dispatches,
            [
                (
                    self.plugin.ENROLLMENT_DENY_TOOL,
                    {"request_id": "req-deny-1"},
                )
            ],
        )
        self.assertEqual(
            result, 'Caduceus enrollment request "req-deny-1" denied.'
        )

    def test_duplicate_pending_prompt_is_suppressed_per_session(self):
        response = control_result(
            {"requests": [{"id": "req-repeat-1", "status": "pending"}]}
        )
        ctx = FakeContext({self.plugin.ENROLLMENT_LIST_TOOL: response})
        adapter = self.plugin.EnrollmentV1Adapter(ctx)

        first = adapter.handle("")
        second = adapter.handle("list")

        self.assertIn("Approve: /caduceus-enrollments", first)
        self.assertNotIn("Approve: /caduceus-enrollments", second)
        self.assertIn("suppressed for this Hermes session", second)
        self.assertEqual(
            ctx.dispatches,
            [
                (self.plugin.ENROLLMENT_LIST_TOOL, {}),
                (self.plugin.ENROLLMENT_LIST_TOOL, {}),
            ],
        )

    def test_expired_request_is_not_prompted_and_expired_decision_is_reported(self):
        expired_list = control_result(
            {
                "requests": [
                    {
                        "id": "req-expired-1",
                        "status": "pending",
                        "expires_at": "2030-01-01T00:00:00Z",
                    }
                ]
            }
        )
        ctx = FakeContext({self.plugin.ENROLLMENT_LIST_TOOL: expired_list})
        adapter = self.plugin.EnrollmentV1Adapter(
            ctx, now=lambda: datetime(2030, 1, 1, tzinfo=timezone.utc)
        )

        listing = adapter.handle("list")
        decision = adapter.handle("approve req-expired-1")

        self.assertIn("Expired enrollment requests were not presented", listing)
        self.assertNotIn("Approve: /caduceus-enrollments", listing)
        self.assertEqual(
            decision, 'Caduceus enrollment request "req-expired-1" has expired.'
        )
        self.assertEqual(
            ctx.dispatches,
            [(self.plugin.ENROLLMENT_LIST_TOOL, {})],
        )

    def test_expired_server_decision_error_is_sanitized(self):
        ctx = FakeContext(
            {
                self.plugin.ENROLLMENT_APPROVE_TOOL: control_result(
                    error={
                        "code": "request_expired",
                        "message": "sensitive server detail",
                    }
                )
            }
        )
        adapter = self.plugin.EnrollmentV1Adapter(ctx)

        result = adapter.handle("approve req-expired-2")

        self.assertEqual(
            result, 'Caduceus enrollment request "req-expired-2" has expired.'
        )
        self.assertNotIn("sensitive server detail", result)
        self.assertEqual(
            ctx.dispatches,
            [
                (
                    self.plugin.ENROLLMENT_APPROVE_TOOL,
                    {"request_id": "req-expired-2"},
                )
            ],
        )

    def test_invalid_request_id_never_dispatches(self):
        ctx = FakeContext()
        adapter = self.plugin.EnrollmentV1Adapter(ctx)

        result = adapter.handle("approve ../../credentials")

        self.assertEqual(result, self.plugin._ENROLLMENT_V1.USAGE)
        self.assertEqual(ctx.dispatches, [])


if __name__ == "__main__":
    unittest.main()
