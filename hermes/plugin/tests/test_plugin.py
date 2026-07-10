"""Dependency-free contract tests for the Caduceus Hermes plugin."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import unittest


PLUGIN_DIR = Path(__file__).resolve().parents[1]


def load_plugin():
    spec = importlib.util.spec_from_file_location(
        "caduceus_hermes_plugin", PLUGIN_DIR / "__init__.py"
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load the Caduceus Hermes plugin")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeContext:
    def __init__(self, dispatch_result=None):
        self.skills = []
        self.commands = {}
        self.dispatches = []
        self.dispatch_result = dispatch_result

    def register_skill(self, **kwargs):
        self.skills.append(kwargs)

    def register_command(self, **kwargs):
        self.commands[kwargs["name"]] = kwargs

    def dispatch_tool(self, name, args):
        self.dispatches.append((name, args))
        return self.dispatch_result


class PluginTests(unittest.TestCase):
    def setUp(self):
        self.plugin = load_plugin()

    def test_registers_packaged_skill_and_status_command(self):
        ctx = FakeContext()

        self.plugin.register(ctx)

        self.assertEqual(len(ctx.skills), 1)
        self.assertEqual(ctx.skills[0]["name"], "remote-prompt-delegation")
        self.assertTrue(ctx.skills[0]["path"].is_file())
        self.assertIn("caduceus-status", ctx.commands)

    def test_status_dispatches_only_through_mcp(self):
        expected = {"ok": True, "data": {"peer_id": "worker-1"}}
        ctx = FakeContext(
            json.dumps({"structuredContent": expected, "result": "ignored"})
        )
        self.plugin.register(ctx)

        result = ctx.commands["caduceus-status"]["handler"]("")

        self.assertEqual(
            ctx.dispatches,
            [("mcp__caduceus__caduceus_get_local_node_status", {})],
        )
        self.assertEqual(json.loads(result), expected)

    def test_status_explains_missing_mcp_configuration(self):
        ctx = FakeContext(
            json.dumps(
                {
                    "error": (
                        "Unknown tool: "
                        "mcp__caduceus__caduceus_get_local_node_status"
                    )
                }
            )
        )
        self.plugin.register(ctx)

        result = ctx.commands["caduceus-status"]["handler"]("")

        self.assertIn("mcp_servers.caduceus.command", result)
        self.assertIn("/reload-mcp", result)

    def test_status_rejects_arguments_without_dispatching(self):
        ctx = FakeContext()
        self.plugin.register(ctx)

        result = ctx.commands["caduceus-status"]["handler"]("extra")

        self.assertEqual(result, "Usage: /caduceus-status")
        self.assertEqual(ctx.dispatches, [])

    def test_manifest_declares_supported_contract(self):
        manifest = (PLUGIN_DIR / "plugin.yaml").read_text(encoding="utf-8")

        self.assertIn("manifest_version: 1", manifest)
        self.assertIn("name: caduceus", manifest)
        self.assertIn('version: "0.2.0"', manifest)
        self.assertNotIn("requires_mcp:", manifest)

    def test_installed_subdirectory_retains_repository_license(self):
        packaged = (PLUGIN_DIR / "LICENSE").read_bytes()
        repository = (PLUGIN_DIR.parents[1] / "LICENSE").read_bytes()

        self.assertEqual(packaged, repository)


if __name__ == "__main__":
    unittest.main()
