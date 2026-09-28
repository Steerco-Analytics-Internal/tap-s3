"""Sync behavior: ordering, bookmarks, filters, limits and failures."""

import contextlib
import io
import json

import pytest
from tests.conftest import (
    bookmark,
    discover,
    iso,
    last_state,
    make_tap,
    minutes,
    records,
    select_all,
    sync,
    window_end,
)

from tap_s3.streams import ObjectParseError


def keys(messages, stream="orders"):
    """The distinct object keys in record order."""
    seen = []
    for record in records(messages, stream):
        if record["_s3_key"] not in seen:
            seen.append(record["_s3_key"])
    return seen


def test_objects_are_read_oldest_first(bucket):
    bucket.put("orders/z.csv", "id\n1\n", minutes(1))
    bucket.put("orders/a.csv", "id\n2\n", minutes(3))
    bucket.put("orders/m.csv", "id\n3\n", minutes(2))
    messages = sync()
    assert keys(messages) == ["orders/z.csv", "orders/m.csv", "orders/a.csv"]
    stream_records = records(messages, "orders")
    assert [r["_s3_last_modified"] for r in stream_records] == [iso(1), iso(2), iso(3)]
    assert bookmark(messages, "orders") == window_end()


def test_bookmark_moves_after_each_object(bucket, monkeypatch):
    monkeypatch.setattr("tap_s3.streams.CHECKPOINT_OBJECTS", 1)
    bucket.put("orders/a.csv", "id\n1\n2\n", minutes(1))
    bucket.put("orders/b.csv", "id\n3\n", minutes(2))
    messages = sync()
    sequence = [
        (m["type"], m.get("record", {}).get("_s3_key"))
        if m["type"] == "RECORD"
        else (m["type"], m.get("value", {}).get("bookmarks", {}).get("orders", {}).get(
            "replication_key_value"
        ))
        for m in messages
        if m["type"] in ("RECORD", "STATE")
    ]
    # The SDK writes an empty STATE first and a final STATE last. The tap
    # writes STATE every CHECKPOINT_OBJECTS objects, so here it writes one
    # checkpoint per object. Once every object is read, the bookmark moves to
    # the start of the lookback window.
    assert sequence == [
        ("STATE", None),
        ("RECORD", "orders/a.csv"),
        ("RECORD", "orders/a.csv"),
        ("STATE", iso(1)),
        ("RECORD", "orders/b.csv"),
        ("STATE", iso(2)),
        ("STATE", window_end()),
        ("STATE", window_end()),
    ]


def test_bookmark_across_two_runs_with_new_and_modified_objects(bucket, clock):
    bucket.put("orders/a.csv", "id\n1\n", minutes(1))
    bucket.put("orders/b.csv", "id\n2\n", minutes(2))
    catalog = select_all(discover())
    clock(62)
    first = sync(catalog)
    assert keys(first) == ["orders/a.csv", "orders/b.csv"]
    state = last_state(first)
    assert state["bookmarks"]["orders"]["replication_key"] == "_s3_last_modified"
    assert state["bookmarks"]["orders"]["replication_key_value"] == iso(2)
    assert state["bookmarks"]["orders"]["window"] == {}

    bucket.put("orders/c.csv", "id\n3\n", minutes(3))
    bucket.put("orders/a.csv", "id\n1\n10\n", minutes(4))
    clock(70)
    second = sync(catalog, state=state)
    assert keys(second) == ["orders/c.csv", "orders/a.csv"]
    assert [r["id"] for r in records(second, "orders")] == ["3", "1", "10"]
    assert bookmark(second, "orders") == iso(10)

    third = sync(catalog, state=last_state(second))
    assert records(third) == []
    assert bookmark(third, "orders") == iso(10)


def test_object_at_the_bookmark_is_skipped(bucket):
    bucket.put("orders/a.csv", "id\n1\n", minutes(1))
    bucket.put("orders/b.csv", "id\n2\n", minutes(2))
    state = {"bookmarks": {"orders": {"replication_key": "_s3_last_modified",
                                      "replication_key_value": iso(1)}}}
    assert keys(sync(state=state)) == ["orders/b.csv"]


def test_bookmark_written_by_another_tool_with_a_z_suffix(bucket):
    bucket.put("orders/a.csv", "id\n1\n", minutes(1))
    bucket.put("orders/b.csv", "id\n2\n", minutes(2))
    state = {"bookmarks": {"orders": {"replication_key_value": "2026-09-01T12:01:00Z"}}}
    assert keys(sync(state=state)) == ["orders/b.csv"]


def test_bookmark_for_another_replication_key_is_ignored(bucket):
    bucket.put("orders/a.csv", "id\n1\n", minutes(1))
    state = {"bookmarks": {"orders": {"replication_key": "updated_at",
                                      "replication_key_value": iso(5)}}}
    assert keys(sync(state=state)) == ["orders/a.csv"]


def test_incremental_mode_false_reads_everything(bucket):
    bucket.put("orders/a.csv", "id\n1\n", minutes(1))
    bucket.put("orders/b.csv", "id\n2\n", minutes(2))
    state = {"bookmarks": {"orders": {"replication_key": "_s3_last_modified",
                                      "replication_key_value": iso(2)}}}
    messages = sync(state=state, incremental_mode=False)
    assert keys(messages) == ["orders/a.csv", "orders/b.csv"]
    assert bookmark(messages, "orders") == window_end()


def test_start_date_ignores_older_objects(bucket):
    bucket.put("orders/a.csv", "id\n1\n", minutes(1))
    bucket.put("orders/b.csv", "id\n2\n", minutes(2))
    bucket.put("orders/c.csv", "id\n3\n", minutes(3))
    messages = sync(start_date="2026-09-01T12:02:00Z")
    assert keys(messages) == ["orders/b.csv", "orders/c.csv"]
    messages = sync(start_date="2026-09-01T12:02:00", incremental_mode=False)
    assert keys(messages) == ["orders/b.csv", "orders/c.csv"]


def test_start_date_and_bookmark_together(bucket):
    bucket.put("orders/a.csv", "id\n1\n", minutes(1))
    bucket.put("orders/b.csv", "id\n2\n", minutes(2))
    bucket.put("orders/c.csv", "id\n3\n", minutes(3))
    state = {"bookmarks": {"orders": {"replication_key_value": iso(2)}}}
    assert keys(sync(state=state, start_date=iso(0))) == ["orders/c.csv"]


def test_header_only_object_still_moves_the_bookmark(bucket):
    bucket.put("orders/a.csv", "id\n1\n", minutes(1))
    bucket.put("orders/b.csv", "id\n", minutes(2))
    messages = sync()
    assert keys(messages) == ["orders/a.csv"]
    assert bookmark(messages, "orders") == window_end()


def test_bookmark_waits_for_every_object_with_the_same_timestamp(bucket):
    bucket.put("orders/a.jsonl", '{"id": 1}\n', minutes(1))
    bucket.put("orders/b.jsonl", '{"id": 2}\n', minutes(2))
    bucket.put("orders/c.jsonl", '{"id": 3}\n', minutes(2))
    catalog = select_all(discover())
    bucket.put("orders/c.jsonl", '{"id": "not a number"}\n', minutes(2))
    tap = make_tap(catalog=catalog)
    output = io.StringIO()
    with contextlib.redirect_stdout(output), pytest.raises(ObjectParseError):
        tap.sync_all()
    messages = [json.loads(line) for line in output.getvalue().splitlines()]
    assert keys(messages) == ["orders/a.jsonl", "orders/b.jsonl"]
    assert bookmark(messages, "orders") == iso(1)


def test_record_limit_stops_early(bucket):
    bucket.put("orders/a.csv", "id\n" + "\n".join(str(i) for i in range(1, 51)) + "\n", minutes(1))
    bucket.put("orders/b.csv", "id\n51\n", minutes(2))
    bucket.put("people.csv", "id\n1\n2\n3\n", minutes(1))
    tap = make_tap(catalog=select_all(discover()))
    bucket.put("orders/b.csv", "id\nnot a number\n", minutes(2))
    fetched = []
    tap.bucket.client.meta.events.register(
        "before-call.s3.GetObject", lambda params, **kwargs: fetched.append(params["url_path"])
    )
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        assert tap.run_sync_dry_run(dry_run_record_limit=5) is True
    messages = [json.loads(line) for line in output.getvalue().splitlines()]
    assert len(records(messages, "orders")) == 5
    assert len(records(messages, "people")) == 3
    assert not any("b.csv" in path for path in fetched)


def test_columns_not_in_the_catalog_are_dropped_and_logged_once(bucket, tap_logs):
    bucket.put("people/a.csv", "id,name\n1,Ada\n", minutes(1))
    catalog = select_all(discover())
    bucket.put("people/b.csv", "id,name,secret,notes\n2,Grace,x,y\n3,Alan,z,w\n", minutes(2))
    bucket.put("people/c.csv", "id,other\n4,q\n", minutes(3))
    messages = sync(catalog)
    stream_records = records(messages, "people")
    assert [set(r) for r in stream_records] == [
        {"id", "name", "_s3_key", "_s3_last_modified", "_row_number"}
    ] * 4
    warnings = [m for m in tap_logs if "not in the catalog schema" in m]
    assert len(warnings) == 1
    assert "secret, notes" in warnings[0]
    assert "people/b.csv" in warnings[0]


def test_deselected_columns_are_left_out(bucket):
    bucket.put("people.csv", "id,name,email\n1,Ada,a@example.com\n")
    catalog = select_all(discover())
    for item in catalog["streams"][0]["metadata"]:
        if item["breadcrumb"] == ["properties", "email"]:
            item["metadata"]["selected"] = False
    stream_records = records(sync(catalog), "people")
    assert "email" not in stream_records[0]
    assert stream_records[0]["name"] == "Ada"


def test_csv_na_in_a_numeric_looking_column_syncs_as_text(bucket):
    lines = ["id,amount"] + [f"{i},{i * 10}" for i in range(1, 1201)]
    lines[1101] = "1101,N/A"
    bucket.put("payments.csv", "\n".join(lines) + "\n")
    catalog = select_all(discover())
    properties = catalog["streams"][0]["schema"]["properties"]
    assert properties["amount"]["type"] == ["string", "null"]
    stream_records = records(sync(catalog), "payments")
    assert len(stream_records) == 1200
    assert stream_records[0]["amount"] == "10"
    assert stream_records[1100]["amount"] == "N/A"


def test_csv_text_in_an_unsampled_older_object_syncs(bucket):
    bucket.put("payments/old.csv", "id,amount\n1,unknown\n", minutes(0))
    for index in range(1, 6):
        bucket.put(f"payments/new{index}.csv", f"id,amount\n{index},{index}\n", minutes(index))
    stream_records = records(sync(), "payments")
    assert stream_records[0]["amount"] == "unknown"


def test_json_type_break_after_the_sample_fails_with_the_row(bucket):
    lines = [json.dumps({"id": i, "amount": i * 10}) for i in range(1, 1201)]
    lines[1100] = json.dumps({"id": 1101, "amount": "N/A"})
    bucket.put("payments.jsonl", "\n".join(lines) + "\n")
    catalog = select_all(discover())
    assert catalog["streams"][0]["schema"]["properties"]["amount"]["type"] == [
        "integer",
        "null",
    ]
    with pytest.raises(ObjectParseError) as caught:
        sync(catalog)
    message = str(caught.value)
    assert "s3://tap-s3-test/payments.jsonl row 1101" in message
    assert "column 'amount'" in message
    assert "'N/A'" in message


def test_json_type_break_in_an_unsampled_older_object_fails(bucket):
    bucket.put("payments/old.jsonl", '{"id": 1, "amount": "unknown"}\n', minutes(0))
    for index in range(1, 6):
        body = json.dumps({"id": index, "amount": index}) + "\n"
        bucket.put(f"payments/new{index}.jsonl", body, minutes(index))
    with pytest.raises(ObjectParseError, match="payments/old.jsonl row 1: column 'amount'"):
        sync()


def test_unparseable_object_fails_the_sync_with_the_key(bucket):
    bucket.put("events/a.jsonl", '{"id": 1}\n', minutes(1))
    catalog = select_all(discover())
    bucket.put("events/b.jsonl", '{"id": 2}\n{broken\n', minutes(2))
    with pytest.raises(ObjectParseError) as caught:
        sync(catalog)
    assert "s3://tap-s3-test/events/b.jsonl" in str(caught.value)
    assert "line 2" in str(caught.value)


def test_catalog_stream_with_no_objects_emits_nothing(bucket):
    bucket.put("people.csv", "id\n1\n")
    catalog = select_all(discover())
    bucket.client.delete_object(Bucket=bucket.name, Key="people.csv")
    assert records(sync(catalog)) == []


def test_sync_uses_the_catalog_schema_without_sampling(bucket):
    bucket.put("people.csv", "id\n1\n")
    catalog = select_all(discover())
    catalog["streams"][0]["schema"]["properties"]["id"] = {"type": ["string", "null"]}
    assert records(sync(catalog))[0]["id"] == "1"


def test_unselected_streams_are_not_read(bucket):
    bucket.put("people.csv", "id\n1\n")
    bucket.put("orders.csv", "id\n1\n")
    catalog = select_all(discover())
    for entry in catalog["streams"]:
        if entry["stream"] == "orders":
            for item in entry["metadata"]:
                item["metadata"]["selected"] = False
    messages = sync(catalog)
    assert {m["stream"] for m in messages if m["type"] == "RECORD"} == {"people"}
