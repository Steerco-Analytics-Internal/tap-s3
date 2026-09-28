"""End to end: the tap's command line, discovery then sync, against moto."""

import json

from click.testing import CliRunner
from tests.conftest import CONFIG, iso, minutes, select_all

from tap_s3.tap import TapS3


def run_cli(args):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(TapS3.cli, args, catch_exceptions=False)
    assert result.exit_code == 0, result.stderr
    return result.stdout


def test_discover_then_sync_with_catalog_and_state(bucket, tmp_path):
    bucket.put("accounts_2026-09-01.csv", "id,name,arr\n1,Acme,1200.50\n", minutes(1))
    bucket.put("accounts_2026-09-02.csv.gz", b"", None)
    bucket.put("contacts/2026/09/part-1.jsonl", '{"id": 7, "email": "a@x.io"}\n', minutes(1))
    bucket.put("contacts/2026/09/part-2.jsonl", '{"id": 8, "email": "b@x.io"}\n', minutes(3))
    bucket.put("notes.xlsx", b"PK")

    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(dict(CONFIG, path_prefix="")))

    catalog = json.loads(run_cli(["--config", str(config_path), "--discover"]))
    entries = {entry["stream"]: entry for entry in catalog["streams"]}
    assert sorted(entries) == ["accounts", "contacts"]
    accounts = entries["accounts"]
    assert accounts["key_properties"] == ["_s3_key", "_row_number"]
    assert accounts["replication_key"] == "_s3_last_modified"
    assert accounts["replication_method"] == "INCREMENTAL"
    assert accounts["schema"]["properties"]["arr"]["type"] == ["number", "null"]
    root_metadata = next(m for m in accounts["metadata"] if m["breadcrumb"] == [])
    assert root_metadata["metadata"]["valid-replication-keys"] == ["_s3_last_modified"]

    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps(select_all(catalog)))
    state_path = tmp_path / "state.json"
    state = {
        "bookmarks": {
            "contacts": {"replication_key": "_s3_last_modified", "replication_key_value": iso(1)}
        }
    }
    state_path.write_text(json.dumps(state))

    output = run_cli(
        ["--config", str(config_path), "--catalog", str(catalog_path), "--state", str(state_path)]
    )
    messages = [json.loads(line) for line in output.splitlines()]
    types = [m["type"] for m in messages]

    for stream in ("accounts", "contacts"):
        stream_types = [
            m["type"] for m in messages if m.get("stream") == stream
        ]
        assert stream_types[0] == "SCHEMA"
        assert set(stream_types[1:]) == {"RECORD"}
    first_record = types.index("RECORD")
    assert all(t == "SCHEMA" or t == "STATE" for t in types[:first_record])
    assert types[-1] == "STATE"

    record_values = [(m["stream"], m["record"]["id"]) for m in messages if m["type"] == "RECORD"]
    assert record_values == [("accounts", 1), ("contacts", 8)]
    final = messages[-1]["value"]["bookmarks"]
    assert final["accounts"]["replication_key_value"] == iso(1)
    assert final["contacts"]["replication_key_value"] == iso(3)


def test_discover_without_credentials_fails_clearly(bucket, tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"bucket": "tap-s3-test"}))
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(TapS3.cli, ["--config", str(config_path), "--discover"])
    assert result.exit_code != 0
    assert "Missing required config settings" in str(result.exception)
