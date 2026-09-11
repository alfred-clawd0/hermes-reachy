"""Packaging metadata checks; the installer check needs hermes-agent."""
from __future__ import annotations

import pathlib
import re
from types import SimpleNamespace

import pytest

import hermes_reachy

ROOT = pathlib.Path(__file__).resolve().parents[1]
PLUGIN_YAML = ROOT / "src" / "hermes_reachy" / "plugin.yaml"


def _manifest():
    yaml = pytest.importorskip("yaml")
    return yaml.safe_load(PLUGIN_YAML.read_text())


def test_versions_agree():
    pyproject = re.search(r'^version = "([^"]+)"', (ROOT / "pyproject.toml").read_text(), re.MULTILINE)
    manifest = re.search(r"^version: (\S+)", PLUGIN_YAML.read_text(), re.MULTILINE)
    assert pyproject and manifest
    assert pyproject.group(1) == manifest.group(1) == hermes_reachy.__version__


def test_key_alternatives_are_both_optional_in_the_manifest():
    manifest = _manifest()
    required = {entry["name"] for entry in manifest["requires_env"]}
    optional = {entry["name"]: entry for entry in manifest["optional_env"]}
    # Either key variable satisfies the requirement (enforced by validate_config), so Hermes'
    # literal per-name requires_env check must not demand one of them.
    assert required == {"REACHY_WS_PORT"}
    assert {"REACHY_WS_API_KEY", "REACHY_WS_API_KEY_FILE"} <= set(optional)
    # `secret` masks the install prompt; `password` marks it secret in the config UI.
    assert optional["REACHY_WS_API_KEY"]["secret"] is True
    assert optional["REACHY_WS_API_KEY"]["password"] is True


def test_hermes_installer_accepts_a_key_file_only_setup(monkeypatch):
    manifest = _manifest()
    plugins_cmd = pytest.importorskip("hermes_cli.plugins_cmd")
    monkeypatch.setenv("REACHY_WS_PORT", "8770")
    monkeypatch.setenv("REACHY_WS_API_KEY_FILE", "/run/secrets/reachy_ws_api_key")
    assert plugins_cmd._missing_requires_env_names(manifest) == []
    printed = []
    plugins_cmd._prompt_plugin_env_vars(manifest, SimpleNamespace(print=lambda *a, **k: printed.append(a)))
    assert printed == []  # nothing to prompt for
    # Regression guard: with the inline key under requires_env, Hermes reported this setup as
    # missing REACHY_WS_API_KEY.
    nagging = {**manifest, "requires_env": [*manifest["requires_env"], {"name": "REACHY_WS_API_KEY"}]}
    assert plugins_cmd._missing_requires_env_names(nagging) == ["REACHY_WS_API_KEY"]
