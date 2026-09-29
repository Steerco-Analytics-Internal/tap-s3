"""Hotglue's per-stream record limit, `_hg_max_records_limit`.

A field-sample job sends it in the config. The tap must stop each named
stream at its limit, and must not move any bookmark for it.
"""

import json

import pytest
from click.testing import CliRunner
from singer_sdk.exceptions import ConfigValidationError
from tests.conftest import (
    CONFIG,
    discover,
    last_state,
    make_tap,
    minutes,
    records,
    select_all,
    sync,
)

from tap_s3.tap import RECORD_LIMITS_SETTING, TapS3


def put_rows(bucket, folder, count, with_items=False):
    rows = []
    for index in range(count):
        row = {"id": index}
        if with_items:
            row["items"] = [{"v": index}, {"v": index + 1000}]
        rows.append(json.dumps(row))
    bucket.put(f"{folder}/a.jsonl", "\n".join(rows) + "\n", minutes(1))


def test_the_setting_stops_each_named_stream_at_its_limit(bucket):
    put_rows(bucket, "o", 40, with_items=True)
    put_rows(bucket, "p", 30)
    catalog = select_all(discover())
    limits = {"o": 10, "o__items": 3, "p": 10}
    messages = sync(catalog, **{RECORD_LIMITS_SETTING: limits})
    assert len(records(messages, "o")) == 10
    assert len(records(messages, "o__items")) == 3
    assert len(records(messages, "p")) == 10


def test_a_stream_the_setting_leaves_out_has_no_limit(bucket):
    put_rows(bucket, "o", 25)
    put_rows(bucket, "p", 25)
    messages = sync(select_all(discover()), **{RECORD_LIMITS_SETTING: {"o": 5}})
    assert len(records(messages, "o")) == 5
    assert len(records(messages, "p")) == 25


def test_a_limited_sync_moves_no_bookmark(bucket):
    put_rows(bucket, "o", 40)
    catalog = select_all(discover())
    state = {"bookmarks": {"o": {"replication_key": "_s3_last_modified"}}}
    messages = sync(catalog, state=state, **{RECORD_LIMITS_SETTING: {"o": 10}})
    assert len(records(messages, "o")) == 10
    assert "replication_key_value" not in last_state(messages)["bookmarks"].get("o", {})
    after = sync(catalog, state=last_state(messages))
    assert len(records(after, "o")) == 40


def test_the_limit_applies_to_streams_built_by_discovery(bucket):
    put_rows(bucket, "o", 5, with_items=True)
    tap = make_tap(**{RECORD_LIMITS_SETTING: {"o": 2, "o__items": 1}})
    assert tap.streams["o"].ABORT_AT_RECORD_COUNT == 2
    assert tap.streams["o__items"].ABORT_AT_RECORD_COUNT == 1


def test_no_setting_means_no_limit(bucket):
    put_rows(bucket, "o", 5)
    tap = make_tap(catalog=select_all(discover()))
    assert tap.streams["o"].ABORT_AT_RECORD_COUNT is None


@pytest.mark.parametrize(
    "value",
    [
        "10",
        [10],
        {"o": 0},
        {"o": -1},
        {"o": "10"},
        {"o": 2.5},
        {"o": True},
    ],
)
def test_a_bad_setting_is_a_config_error(bucket, value):
    put_rows(bucket, "o", 5)
    catalog = select_all(discover())
    with pytest.raises(ConfigValidationError):
        make_tap(catalog=catalog, **{RECORD_LIMITS_SETTING: value}).streams


def test_the_command_line_applies_the_setting_from_the_config_file(bucket, tmp_path):
    put_rows(bucket, "o", 40, with_items=True)
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(CONFIG))
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps(select_all(discover())))
    sample_config_path = tmp_path / "sample-config.json"
    sample_config_path.write_text(
        json.dumps(dict(CONFIG, **{RECORD_LIMITS_SETTING: {"o": 10, "o__items": 10}}))
    )

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        TapS3.cli,
        ["--config", str(sample_config_path), "--catalog", str(catalog_path)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.stderr
    messages = [json.loads(line) for line in result.stdout.splitlines()]
    assert len(records(messages, "o")) == 10
    assert len(records(messages, "o__items")) == 10


def test_a_limited_stream_stops_reading_once_it_has_its_rows(bucket):
    put_rows(bucket, "o", 20)
    bucket.put("o/b.jsonl", "{broken\n", minutes(2))
    messages = sync(select_all(discover()), **{RECORD_LIMITS_SETTING: {"o": 5}})
    assert len(records(messages, "o")) == 5


@pytest.mark.parametrize("value", [[10], {"o": 0}])
def test_discovery_refuses_a_bad_setting(bucket, tmp_path, value):
    put_rows(bucket, "o", 5)
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(dict(CONFIG, **{RECORD_LIMITS_SETTING: value})))
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(TapS3.cli, ["--config", str(config_path), "--discover"])
    assert result.exit_code != 0
    assert isinstance(result.exception, ConfigValidationError)
    assert RECORD_LIMITS_SETTING in str(result.exception)
