"""Smoke tests. Expand once you've customized the streams."""

from tap_template.tap import TapTemplate


def test_tap_instantiates():
    tap = TapTemplate(config={"api_key": "test"}, parse_env_config=False)
    assert tap.name == "tap-template"


def test_streams_discoverable():
    tap = TapTemplate(config={"api_key": "test"}, parse_env_config=False)
    streams = tap.discover_streams()
    assert len(streams) >= 1
    assert all(s.name for s in streams)
