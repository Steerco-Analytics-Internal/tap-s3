"""Smoke tests and the config contract with the Hotglue S3 connector form."""

import subprocess
import sys

from tests.conftest import make_tap

from tap_s3.tap import TapS3

HOTGLUE_FORM_KEYS = [
    "aws_access_key_id",
    "aws_secret_access_key",
    "bucket",
    "path_prefix",
    "incremental_mode",
]


def test_config_keys_match_the_hotglue_form():
    properties = TapS3.config_jsonschema["properties"]
    own_keys = [key for key in properties if key in HOTGLUE_FORM_KEYS]
    assert own_keys == HOTGLUE_FORM_KEYS
    assert list(properties)[:5] == HOTGLUE_FORM_KEYS
    assert {"region", "start_date"} <= set(properties)


def test_required_keys():
    required = set(TapS3.config_jsonschema["required"])
    assert required == {"aws_access_key_id", "aws_secret_access_key", "bucket"}


def test_secret_is_marked_secret():
    properties = TapS3.config_jsonschema["properties"]
    assert properties["aws_secret_access_key"]["secret"] is True
    assert properties["aws_secret_access_key"].get("writeOnly") is True
    for key in ("aws_access_key_id", "bucket", "path_prefix", "incremental_mode"):
        assert not properties[key].get("secret")


def test_optional_key_types():
    properties = TapS3.config_jsonschema["properties"]
    assert properties["incremental_mode"]["type"] == ["boolean", "string", "integer", "null"]
    assert properties["lookback_minutes"]["default"] == 60
    assert properties["exclude_pattern"]["type"] == ["string", "null"]
    assert properties["incremental_mode"]["default"] is True
    assert properties["path_prefix"]["type"] == ["string", "null"]
    assert properties["start_date"]["format"] == "date-time"


def test_tap_instantiates_against_an_empty_bucket(bucket):
    tap = make_tap()
    assert tap.name == "tap-s3"
    assert tap.discover_streams() == []


def test_about_runs_from_the_command_line():
    result = subprocess.run(
        [sys.executable, "-m", "tap_s3.tap", "--about", "--format", "json"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert '"name": "tap-s3"' in result.stdout
    assert "aws_secret_access_key" in result.stdout


def test_parse_flag_defaults():
    from tap_s3.tap import parse_flag

    assert parse_flag(None, default=True) is True
    assert parse_flag(None, default=False) is False
    assert parse_flag("  ", default=True) is True


def test_utc_now_is_aware(monkeypatch):
    import datetime

    import tap_s3.tap

    monkeypatch.undo()
    assert tap_s3.tap.utc_now().tzinfo == datetime.timezone.utc
