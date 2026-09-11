"""Packaging metadata checks — no hermes-agent required."""
from __future__ import annotations

import pathlib
import re

import hermes_reachy

ROOT = pathlib.Path(__file__).resolve().parents[1]
PLUGIN_YAML = ROOT / "src" / "hermes_reachy" / "plugin.yaml"


def test_versions_agree():
    pyproject = re.search(r'^version = "([^"]+)"', (ROOT / "pyproject.toml").read_text(), re.MULTILINE)
    manifest = re.search(r"^version: (\S+)", PLUGIN_YAML.read_text(), re.MULTILINE)
    assert pyproject and manifest
    assert pyproject.group(1) == manifest.group(1) == hermes_reachy.__version__


def test_manifest_requires_an_api_key():
    yaml = __import__("pytest").importorskip("yaml")
    manifest = yaml.safe_load(PLUGIN_YAML.read_text())
    required = {entry["name"]: entry for entry in manifest["requires_env"]}
    optional = {entry["name"] for entry in manifest["optional_env"]}
    assert set(required) == {"REACHY_WS_PORT", "REACHY_WS_API_KEY"}
    # `secret` masks the install prompt; `password` marks it secret in the config UI.
    assert required["REACHY_WS_API_KEY"]["secret"] is True
    assert required["REACHY_WS_API_KEY"]["password"] is True
    assert "REACHY_WS_API_KEY_FILE" in optional
