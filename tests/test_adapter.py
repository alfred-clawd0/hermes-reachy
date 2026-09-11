"""Tests for the Reachy platform adapter.

The adapter subclasses Hermes gateway base classes, so these tests are skipped when
hermes-agent is not importable (e.g. a bare CI without it). Run with:

    PYTHONPATH="$HOME/.hermes/hermes-agent" python -m pytest -q
"""
from __future__ import annotations

import logging
import os
import re

import pytest

adapter = pytest.importorskip(
    "hermes_reachy.adapter",
    reason="hermes-agent (gateway.*) not importable in this environment",
)
pytestmark = pytest.mark.usefixtures("reachy_platform")

SECRET = "sekrit-0123456789"


class _FakeCtx:
    def __init__(self):
        self.platforms = []

    def register_platform(self, **kwargs):
        self.platforms.append(kwargs)


def _cfg(**extra):
    return adapter.PlatformConfig(enabled=True, extra=extra)


def test_robot_id_from_path():
    f = adapter.ReachyAdapter._robot_id_from_path
    assert f("/robot/reachy") == "reachy"
    assert f("/robot/kitchen-bot?token=x") == "kitchen-bot"
    assert f("") == adapter.DEFAULT_ROBOT_ID
    assert f("/") == adapter.DEFAULT_ROBOT_ID


def test_tag_turn_origin():
    tagged = adapter.ReachyAdapter._tag_turn({"type": "say"}, "t1")
    assert tagged["turn_id"] == "t1" and tagged["origin"] == "turn"
    proactive = adapter.ReachyAdapter._tag_turn({"type": "say"}, None)
    assert proactive["turn_id"] is None and proactive["origin"] == "proactive"


# ── validate_config / is_configured ────────────────────────────────────────
def test_validate_config_accepts_inline_key(monkeypatch):
    monkeypatch.setenv("REACHY_WS_PORT", "8770")
    monkeypatch.setenv("REACHY_WS_API_KEY", SECRET)
    assert adapter.validate_config(_cfg()) is True
    reachy = adapter.ReachyAdapter(_cfg())
    assert reachy._port == 8770
    assert reachy._host == "127.0.0.1"  # loopback by default
    assert reachy._api_key == SECRET.encode()


def test_validate_config_accepts_key_file_only(monkeypatch, tmp_path):
    key_file = tmp_path / "reachy.key"
    key_file.write_text(f"  {SECRET}\n", encoding="utf-8")
    monkeypatch.setenv("REACHY_WS_PORT", "8770")
    monkeypatch.setenv("REACHY_WS_API_KEY_FILE", str(key_file))
    assert adapter.validate_config(_cfg()) is True
    assert adapter.ReachyAdapter(_cfg())._api_key == SECRET.encode()


def _invalid_setups(tmp_path):
    empty = tmp_path / "empty.key"
    empty.write_text(" \n", encoding="utf-8")
    locked = tmp_path / "locked.key"
    locked.write_text(SECRET, encoding="utf-8")
    locked.chmod(0)
    port = {"REACHY_WS_PORT": "8770"}
    key = {"REACHY_WS_API_KEY": SECRET}
    return {
        "no-key": (port, {}, "no API key configured"),
        "blank-inline-key": ({**port, "REACHY_WS_API_KEY": "   "}, {}, "no API key configured"),
        "non-string-key": (port, {"api_key": 12345}, "api_key must be a string"),
        "empty-key-file": ({**port, "REACHY_WS_API_KEY_FILE": str(empty)}, {}, "is empty"),
        "missing-key-file": ({**port, "REACHY_WS_API_KEY_FILE": str(tmp_path / "nope")}, {}, "is unreadable"),
        "unreadable-key-file": ({**port, "REACHY_WS_API_KEY_FILE": str(locked)}, {}, "is unreadable"),
        "key-file-is-a-directory": ({**port, "REACHY_WS_API_KEY_FILE": str(tmp_path)}, {}, "is unreadable"),
        "no-port": (key, {}, "REACHY_WS_PORT is not set"),
        "port-not-an-int": ({**key, "REACHY_WS_PORT": "eighty"}, {}, "is not an integer"),
        "port-zero": ({**key, "REACHY_WS_PORT": "0"}, {}, "outside 1-65535"),
        "port-too-large": ({**key, "REACHY_WS_PORT": "65536"}, {}, "outside 1-65535"),
        "port-negative": ({**key, "REACHY_WS_PORT": "-1"}, {}, "outside 1-65535"),
        "malformed-allowlist-entry": (
            {**port, **key, "REACHY_ALLOWED_ROBOTS": "reachy, robot one"}, {}, "invalid robot id(s) 'robot one'"
        ),
    }


@pytest.mark.parametrize(
    "case",
    [
        "no-key",
        "blank-inline-key",
        "non-string-key",
        "empty-key-file",
        "missing-key-file",
        "unreadable-key-file",
        "key-file-is-a-directory",
        "no-port",
        "port-not-an-int",
        "port-zero",
        "port-too-large",
        "port-negative",
        "malformed-allowlist-entry",
    ],
)
def test_validate_config_rejects(case, monkeypatch, tmp_path, caplog):
    if case == "unreadable-key-file" and os.geteuid() == 0:
        pytest.skip("root can read mode-000 files")
    env, extra, why = _invalid_setups(tmp_path)[case]
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    caplog.set_level(logging.ERROR, logger="hermes_reachy.adapter")
    assert adapter.validate_config(_cfg(**extra)) is False
    assert why in caplog.text
    assert SECRET not in caplog.text
    # The adapter shares the loader, so it can never start with a config validate_config rejects.
    with pytest.raises(adapter.ReachyConfigError, match=re.escape(why)):
        adapter.ReachyAdapter(_cfg(**extra))


def test_inline_key_takes_precedence_over_key_file(monkeypatch, tmp_path):
    monkeypatch.setenv("REACHY_WS_PORT", "8770")
    monkeypatch.setenv("REACHY_WS_API_KEY", SECRET)
    monkeypatch.setenv("REACHY_WS_API_KEY_FILE", str(tmp_path / "missing"))
    assert adapter.validate_config(_cfg()) is True
    assert adapter.ReachyAdapter(_cfg())._api_key == SECRET.encode()


def test_is_configured_gates_enablement_on_port(monkeypatch):
    # check_fn is a passive dependency probe; opting in is is_connected's job.
    assert adapter.check_requirements() is adapter.WEBSOCKETS_AVAILABLE
    assert adapter.is_configured(_cfg()) is False
    assert adapter.is_configured(_cfg(port=8770)) is True
    monkeypatch.setenv("REACHY_WS_PORT", "8770")
    assert adapter.is_configured(_cfg()) is True


# ── robot allowlist ────────────────────────────────────────────────────────
@pytest.mark.parametrize("raw", [None, "", "   ", " , ,"])
def test_unset_or_blank_allowlist_means_default_robot(raw, monkeypatch):
    if raw is not None:
        monkeypatch.setenv("REACHY_ALLOWED_ROBOTS", raw)
    reachy = adapter.ReachyAdapter(_cfg(port=8770, api_key=SECRET))
    assert reachy._allowed_robots == {adapter.DEFAULT_ROBOT_ID}
    assert reachy._is_dm_allowed("reachy") is True
    assert reachy._is_dm_allowed("kitchen") is False


def test_allowlist_parsing_and_allow_all(monkeypatch):
    monkeypatch.setenv("REACHY_ALLOWED_ROBOTS", " kitchen , lab ,")
    reachy = adapter.ReachyAdapter(_cfg(port=8770, api_key=SECRET))
    assert reachy._allowed_robots == {"kitchen", "lab"}
    assert reachy._dm_policy == "allowlist"
    assert not reachy._is_dm_allowed("reachy")

    monkeypatch.setenv("REACHY_ALLOW_ALL_ROBOTS", "TRUE")
    reachy = adapter.ReachyAdapter(_cfg(port=8770, api_key=SECRET))
    assert reachy._is_dm_allowed("any-robot-at-all") is True
    assert reachy._dm_policy == "open"


def test_config_allowlist_used_when_env_unset():
    reachy = adapter.ReachyAdapter(_cfg(port=8770, api_key=SECRET, allowed_robots=["kitchen"]))
    assert reachy._allowed_robots == {"kitchen"}


def test_gateway_authorization_is_not_bypassed():
    reachy = adapter.ReachyAdapter(_cfg(port=8770, api_key=SECRET))
    # Not the relay's upstream bypass: the gateway's allowlist / allow-all / pairing still apply.
    assert reachy.authorization_is_upstream is False
    assert reachy.enforces_own_access_policy is True
    assert reachy._dm_policy == "allowlist"


def test_is_loopback():
    assert adapter._is_loopback("127.0.0.1")
    assert adapter._is_loopback("::1")
    assert adapter._is_loopback("localhost")
    assert not adapter._is_loopback("0.0.0.0")
    assert not adapter._is_loopback("reachy-host.local")


# ── log-safe helpers ───────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "raw, problem",
    [
        ("", "empty frame"),
        (b"   ", "empty frame"),
        ('{"api_key":"' + SECRET, "invalid JSON"),
        ("[" * 100_000, "invalid JSON"),  # deep nesting -> RecursionError, still a category
        (f'["{SECRET}"]', "not a JSON object"),
        ('{"type":"hello"}', ""),
    ],
)
def test_decode_frame_reports_only_a_category(raw, problem):
    frame, found = adapter.ReachyAdapter._decode_frame(raw)
    assert found == problem
    assert SECRET not in found
    assert (frame == {"type": "hello"}) if not problem else (frame == {})


def test_path_for_log_drops_the_query_string():
    assert adapter._path_for_log(f"/robot/kitchen?api_key={SECRET}") == "/robot/kitchen"
    assert adapter._path_for_log("/robot/<script>") == "<redacted>"
    assert adapter._path_for_log("") == ""


# ── registration ───────────────────────────────────────────────────────────
def test_env_enablement_seed(monkeypatch):
    monkeypatch.delenv("REACHY_WS_PORT", raising=False)
    assert adapter._env_enablement() is None
    monkeypatch.setenv("REACHY_WS_PORT", "8770")
    monkeypatch.setenv("REACHY_HOME_CHANNEL", "reachy")
    monkeypatch.setenv("REACHY_WS_API_KEY", SECRET)
    monkeypatch.setenv("REACHY_WS_API_KEY_FILE", "/run/secrets/reachy")
    seed = adapter._env_enablement()
    assert seed["port"] == 8770
    assert seed["home_channel"]["chat_id"] == "reachy"
    assert seed["api_key_file"] == "/run/secrets/reachy"
    assert SECRET not in repr(seed)  # the key itself is never copied into config/status
    # A malformed port must not raise here; validate_config reports it.
    monkeypatch.setenv("REACHY_WS_PORT", "eighty")
    assert adapter._env_enablement()["port"] == "eighty"


def test_register_platform_registers_reachy():
    ctx = _FakeCtx()
    adapter.register_platform(ctx)
    assert len(ctx.platforms) == 1
    p = ctx.platforms[0]
    assert p["name"] == "reachy"
    # Only the port: either key variable satisfies the key requirement (validate_config).
    assert p["required_env"] == ["REACHY_WS_PORT"]
    assert callable(p["adapter_factory"])
    assert callable(p["check_fn"])
    assert p["validate_config"] is adapter.validate_config
    assert p["is_connected"] is adapter.is_configured
    assert p["allowed_users_env"] == "REACHY_ALLOWED_ROBOTS"
    assert p["allow_all_env"] == "REACHY_ALLOW_ALL_ROBOTS"


def test_no_private_markers_in_source():
    import pathlib

    src = pathlib.Path(adapter.__file__).read_text().lower()
    for marker in ("tars", "manfred", "192.168", "spiner", "gap-map"):
        assert marker not in src
