"""Regression tests for defects found in review.

Each test names the finding it covers.
"""

import contextlib
import io
import json
import math
import tracemalloc

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from tests.conftest import (
    CONFIG,
    bookmark,
    discover,
    iso,
    last_state,
    make_tap,
    minutes,
    records,
    schemas,
    select_all,
    sync,
)

from tap_s3 import client as client_module
from tap_s3.formats import FileFormat, ObjectSource, iter_rows
from tap_s3.streams import ObjectParseError
from tap_s3.tap import TapS3


def keys(messages, stream="orders"):
    seen = []
    for record in records(messages, stream):
        if record["_s3_key"] not in seen:
            seen.append(record["_s3_key"])
    return seen


def parquet_bytes(table, **options):
    buffer = io.BytesIO()
    pq.write_table(table, buffer, **options)
    return buffer.getvalue()


def sync_capturing(catalog, **settings):
    """Sync, and return the messages plus any exception raised."""
    tap = make_tap(catalog=catalog, **settings)
    output = io.StringIO()
    error = None
    with contextlib.redirect_stdout(output):
        try:
            tap.sync_all()
        except Exception as err:  # noqa: BLE001
            error = err
    messages = [json.loads(line) for line in output.getvalue().splitlines()]
    return messages, error


# Finding 1: late objects are skipped.


def test_same_second_late_arrival_is_read(bucket, clock):
    bucket.put("orders/a.csv", "id\n1\n", minutes(10))
    catalog = select_all(discover())
    clock(30)
    first = sync(catalog)
    assert keys(first) == ["orders/a.csv"]

    bucket.put("orders/b.csv", "id\n2\n", minutes(10))
    clock(40)
    second = sync(catalog, state=last_state(first))
    assert keys(second) == ["orders/b.csv"]


def test_object_older_than_the_bookmark_inside_the_window_is_read(bucket, clock):
    bucket.put("orders/a.csv", "id\n1\n", minutes(50))
    bucket.put("orders/b.csv", "id\n2\n", minutes(90))
    catalog = select_all(discover())
    clock(100)
    first = sync(catalog)
    assert keys(first) == ["orders/a.csv", "orders/b.csv"]

    bucket.put("orders/c.csv", "id\n3\n", minutes(70))
    clock(110)
    second = sync(catalog, state=last_state(first))
    assert keys(second) == ["orders/c.csv"]


def test_window_state_is_pruned(bucket, clock):
    bucket.put("orders/a.csv", "id\n1\n", minutes(50))
    bucket.put("orders/b.csv", "id\n2\n", minutes(90))
    catalog = select_all(discover())
    clock(100)
    first = sync(catalog)
    stream_state = last_state(first)["bookmarks"]["orders"]
    assert stream_state["replication_key_value"] == iso(40)
    assert sorted(stream_state["window"]) == ["orders/a.csv", "orders/b.csv"]
    assert all(stream_state["window"].values())

    clock(140)
    second = sync(catalog, state=last_state(first))
    stream_state = last_state(second)["bookmarks"]["orders"]
    assert keys(second) == []
    assert stream_state["replication_key_value"] == iso(80)
    assert list(stream_state["window"]) == ["orders/b.csv"]

    clock(200)
    third = sync(catalog, state=last_state(second))
    assert last_state(third)["bookmarks"]["orders"]["window"] == {}


def test_a_changed_etag_inside_the_window_is_read_again(bucket, clock):
    bucket.put("orders/a.csv", "id\n1\n", minutes(50))
    catalog = select_all(discover())
    clock(60)
    first = sync(catalog)
    bucket.put("orders/a.csv", "id\n1\n2\n", minutes(50))
    clock(70)
    second = sync(catalog, state=last_state(first))
    assert [r["id"] for r in records(second, "orders")] == [1, 2]


def test_lookback_minutes_zero_uses_the_listing_time(bucket, clock):
    bucket.put("orders/a.csv", "id\n1\n", minutes(10))
    catalog = select_all(discover())
    clock(30)
    first = sync(catalog, lookback_minutes=0)
    assert bookmark(first, "orders") == iso(30)


def test_lookback_minutes_is_in_the_config():
    prop = TapS3.config_jsonschema["properties"]["lookback_minutes"]
    assert prop["type"] == ["integer", "null"]
    assert prop["default"] == 60


# Finding 2: an empty JSON string fails the sync.


@pytest.mark.parametrize(
    "key, body",
    [
        ("people.jsonl", '{"id": 1, "age": 30}\n{"id": 2, "age": ""}\n'),
        ("people.json", '[{"id": 1, "age": 30}, {"id": 2, "age": ""}]'),
    ],
)
def test_empty_json_string_in_a_typed_column_is_null(bucket, key, body):
    bucket.put(key, body)
    catalog = discover()
    assert schemas(catalog)["people"]["age"]["type"] == ["integer", "null"]
    stream_records = records(sync(select_all(catalog)), "people")
    assert [r["age"] for r in stream_records] == [30, None]


def test_empty_json_string_in_a_string_column_stays_empty(bucket):
    bucket.put("people.jsonl", '{"name": "Ada"}\n{"name": ""}\n')
    stream_records = records(sync(), "people")
    assert [r["name"] for r in stream_records] == ["Ada", ""]


# Finding 3: NaN and Infinity break JSON output.


def test_jsonl_nan_and_infinity_become_null(bucket):
    bucket.put(
        "scores.jsonl",
        '{"id": 1, "score": 1.5, "extra": {"v": NaN}}\n'
        '{"id": 2, "score": NaN, "extra": {"v": 1}}\n'
        '{"id": 3, "score": Infinity, "extra": {"v": -Infinity}}\n',
    )
    stream_records = records(sync(), "scores")
    assert [r["score"] for r in stream_records] == [1.5, None, None]
    assert [r["extra"] for r in stream_records] == [{"v": None}, {"v": 1}, {"v": None}]


def test_parquet_nan_becomes_null(bucket):
    table = pa.table(
        {
            "score": pa.array([1.5, math.nan, math.inf], pa.float64()),
            "nested": pa.array([[math.nan], [1.0], []], pa.list_(pa.float64())),
        }
    )
    bucket.put("scores.parquet", parquet_bytes(table))
    stream_records = records(sync(), "scores")
    assert [r["score"] for r in stream_records] == [1.5, None, None]
    assert [r["nested"] for r in stream_records] == [[None], [1.0], []]


# Finding 4: Hotglue can send incremental_mode as a string.


def test_incremental_mode_accepts_strings_in_the_schema():
    prop = TapS3.config_jsonschema["properties"]["incremental_mode"]
    assert set(prop["type"]) == {"boolean", "string", "null"}


@pytest.mark.parametrize(
    "value, incremental",
    [
        ("true", True),
        ("TRUE", True),
        (" yes ", True),
        ("1", True),
        ("false", False),
        ("False", False),
        ("0", False),
        ("no", False),
        ("", True),
        (None, True),
        (True, True),
        (False, False),
    ],
)
def test_incremental_mode_values(bucket, value, incremental):
    bucket.put("orders/a.csv", "id\n1\n", minutes(1))
    bucket.put("orders/b.csv", "id\n2\n", minutes(2))
    state = {"bookmarks": {"orders": {"replication_key_value": iso(1)}}}
    settings = {} if value is None else {"incremental_mode": value}
    catalog = select_all(discover())
    messages = sync(catalog, state=state, **settings)
    expected = ["orders/b.csv"] if incremental else ["orders/a.csv", "orders/b.csv"]
    assert keys(messages) == expected


def test_incremental_mode_missing_means_true(bucket):
    tap = make_tap()
    assert tap.incremental_mode is True


# Finding 5: the type-break message.


def test_type_break_message_does_not_promise_rediscovery(bucket):
    lines = ["id,amount"] + [f"{i},{i}" for i in range(1, 1101)] + ["1101,N/A"]
    bucket.put("payments.csv", "\n".join(lines) + "\n")
    with pytest.raises(ObjectParseError) as caught:
        sync()
    message = str(caught.value)
    assert "run discovery again" not in message
    assert "fix the file or change the column's type in the catalog" in message


# Finding 6: one unparseable object breaks discovery.


def test_discovery_skips_an_unparseable_object_and_logs_it(bucket, tap_logs):
    bucket.put("events/good.jsonl", '{"id": 1, "kind": "open"}\n', minutes(1))
    bucket.put("events/bad.jsonl", '{"id": 2}\n{broken\n', minutes(2))
    bucket.put("people.csv", "id\n1\n", minutes(1))
    catalog = discover()
    properties = schemas(catalog)
    assert {"events", "people"} == set(properties)
    assert {"id", "kind"} <= set(properties["events"])
    warnings = [m for m in tap_logs if "events/bad.jsonl" in m]
    assert len(warnings) == 1
    assert "line 2" in warnings[0]

    with pytest.raises(ObjectParseError, match="events/bad.jsonl"):
        sync(select_all(catalog))


def test_exclude_pattern_applies_to_discovery_and_sync(bucket):
    bucket.put("orders/a.csv", "id\n1\n", minutes(1))
    bucket.put("orders/manifest.json", '{"files": 3}', minutes(2))
    bucket.put("orders/b.csv", "id\n2\n", minutes(3))
    bucket.put("manifest.json", '{"files": 3}', minutes(1))
    catalog = discover(exclude_pattern=r"(^|/)manifest\.json$")
    assert list(schemas(catalog)) == ["orders"]
    messages = sync(select_all(catalog), exclude_pattern=r"(^|/)manifest\.json$")
    assert keys(messages) == ["orders/a.csv", "orders/b.csv"]


def test_exclude_pattern_is_relative_to_the_prefix(bucket):
    bucket.put("exports/skip/a.csv", "id\n1\n")
    bucket.put("exports/keep/a.csv", "id\n1\n")
    catalog = discover(path_prefix="exports", exclude_pattern="^skip/")
    assert list(schemas(catalog)) == ["keep"]


def test_invalid_exclude_pattern_fails_clearly(bucket):
    with pytest.raises(ValueError, match="exclude_pattern"):
        discover(exclude_pattern="(")


def test_latin1_is_the_last_resort_decoder(bucket):
    bucket.put("legacy.csv", b"id,name\n1,\x81\x8d\n")
    stream_records = records(sync(), "legacy")
    assert stream_records[0]["name"] == "\x81\x8d"


# Finding 7: the BOM survives the cp1252 fallback.


def test_bom_is_stripped_before_the_cp1252_fallback(bucket):
    body = b"\xef\xbb\xbf" + "id,name\n1,Café\n".encode("cp1252")
    bucket.put("legacy.csv", body)
    catalog = discover()
    assert "id" in schemas(catalog)["legacy"]
    stream_records = records(sync(select_all(catalog)), "legacy")
    assert stream_records[0]["id"] == 1
    assert stream_records[0]["name"] == "Café"


def test_bom_is_stripped_in_gzipped_csv(bucket):
    import gzip

    bucket.put("legacy.csv.gz", gzip.compress(b"\xef\xbb\xbfid\n1\n"))
    assert records(sync(), "legacy")[0]["id"] == 1


# Finding 8: naive datetimes.


def test_naive_text_timestamps_are_utc(bucket):
    bucket.put("events.csv", "id,at\n1,2026-09-01T10:00:00\n2,2026-09-02\n")
    stream_records = records(sync(), "events")
    assert [r["at"] for r in stream_records] == [
        "2026-09-01T10:00:00+00:00",
        "2026-09-02T00:00:00+00:00",
    ]


def test_naive_parquet_timestamps_are_utc(bucket):
    import datetime

    naive = datetime.datetime(2026, 9, 1, 10, 0)
    table = pa.table(
        {
            "at": pa.array([naive], pa.timestamp("us")),
            "nested": pa.array(
                [{"at": naive}], pa.struct([("at", pa.timestamp("us"))])
            ),
        }
    )
    bucket.put("events.parquet", parquet_bytes(table))
    record = records(sync(), "events")[0]
    assert record["at"] == "2026-09-01T10:00:00+00:00"
    assert record["nested"] == {"at": "2026-09-01T10:00:00+00:00"}


# Finding 9: two raw names sanitize to the same stream.


def test_names_that_sanitize_to_one_stream_warn(bucket, tap_logs):
    bucket.put("Sales Data/a.csv", "id\n1\n")
    bucket.put("Sales-Data/b.csv", "id\n2\n")
    bucket.put("accounts.csv", "id\n1\n")
    bucket.put("accounts/b.csv", "id\n1\n")
    bucket.put("accounts_2026-09-01.csv", "id\n1\n")
    tap = make_tap()
    assert sorted(tap.layout.streams) == ["Sales_Data", "accounts"]
    warnings = [m for m in tap_logs if "Sales_Data" in m and "Sales Data" in m]
    assert len(warnings) == 1
    assert "Sales-Data" in warnings[0]
    assert not any("'accounts'" in m and "merges" in m for m in tap_logs)


# Finding 10: the catalog's FULL_TABLE method is ignored.


def test_full_table_in_the_catalog_ignores_the_bookmark(bucket):
    bucket.put("orders/a.csv", "id\n1\n", minutes(1))
    bucket.put("orders/b.csv", "id\n2\n", minutes(2))
    catalog = select_all(discover())
    catalog["streams"][0]["replication_method"] = "FULL_TABLE"
    state = {"bookmarks": {"orders": {"replication_key_value": iso(2)}}}
    messages = sync(catalog, state=state)
    assert keys(messages) == ["orders/a.csv", "orders/b.csv"]


# Finding 11: source columns collide with metadata columns.


def test_source_columns_named_like_metadata_are_renamed(bucket, tap_logs):
    bucket.put("files.csv", "_s3_key,_row_number,name\nsource-key,99,Ada\n")
    catalog = discover()
    properties = schemas(catalog)["files"]
    assert "_s3_key_source" in properties
    assert "_row_number_source" in properties
    assert properties["_row_number"] == {"type": ["integer"]}
    record = records(sync(select_all(catalog)), "files")[0]
    assert record["_s3_key"] == "files.csv"
    assert record["_row_number"] == 1
    assert record["_s3_key_source"] == "source-key"
    assert record["_row_number_source"] == 99
    warnings = [m for m in tap_logs if "_s3_key_source" in m]
    assert warnings, tap_logs


def test_parquet_columns_named_like_metadata_are_renamed(bucket):
    table = pa.table({"_s3_last_modified": ["yesterday"], "id": [1]})
    bucket.put("files.parquet", parquet_bytes(table))
    record = records(sync(), "files")[0]
    assert record["_s3_last_modified_source"] == "yesterday"
    assert record["_s3_last_modified"].endswith("+00:00")


# Finding 12: .json loads the whole file.


def spy_reads(monkeypatch):
    reads = []
    original = client_module._StreamingBodyReader.readinto

    def readinto(self, buffer):
        count = original(self, buffer)
        reads.append(count)
        return count

    monkeypatch.setattr(client_module._StreamingBodyReader, "readinto", readinto)
    return reads


def test_large_json_is_streamed(monkeypatch, bucket):
    rows = 200_000
    items = ",".join(f'{{"id": {i}, "email": "user{i}@example.com"}}' for i in range(1, rows + 1))
    document = f"[{items}]"
    body = document.encode("utf-8")
    bucket.put("big.json", body)
    catalog = select_all(make_tap().catalog_dict)
    stream = make_tap(catalog=catalog).streams["big"]
    reads = spy_reads(monkeypatch)
    rows_iter = stream.get_records(None)
    first = next(rows_iter)
    assert first["id"] == 1
    assert sum(reads) < len(body) / 5
    assert 1 + sum(1 for _ in rows_iter) == rows


# Test gaps named in review.


def test_no_bookmark_lands_mid_object(bucket, monkeypatch):
    bucket.put("orders/a.csv", "id\n1\n2\n", minutes(1))
    bucket.put("orders/b.csv", "id\n3\n4\n5\n", minutes(2))
    catalog = select_all(discover())
    bucket.put("orders/b.csv", "id\n3\n4\nbad\n", minutes(2))
    monkeypatch.setattr("singer_sdk.Stream.STATE_MSG_FREQUENCY", 1)
    messages, error = sync_capturing(catalog)
    assert isinstance(error, ObjectParseError)
    assert len(records(messages, "orders")) == 4
    states = [m["value"] for m in messages if m["type"] == "STATE"]
    assert len(states) >= 2
    for state in states:
        stream_state = state.get("bookmarks", {}).get("orders", {})
        assert stream_state.get("replication_key_value") in (None, iso(1))
        assert "orders/b.csv" not in stream_state.get("window", {})


def test_header_only_object_emits_its_bookmark_before_a_failure(bucket):
    bucket.put("orders/a.csv", "id\n", minutes(1))
    bucket.put("orders/b.csv", "id\n2\n", minutes(2))
    catalog = select_all(discover())
    bucket.put("orders/b.csv", "id\nbad\n", minutes(2))
    messages, error = sync_capturing(catalog)
    assert isinstance(error, ObjectParseError)
    assert records(messages) == []
    assert bookmark(messages, "orders") == iso(1)


def test_parquet_is_read_with_ranged_requests(bucket):
    table = pa.table({"id": list(range(200_000)), "value": [f"v{i}" for i in range(200_000)]})
    body = parquet_bytes(table, row_group_size=20_000)
    bucket.put("big.parquet", body)
    tap = make_tap(catalog={"streams": []})
    requests = []
    tap.bucket.client.meta.events.register(
        "before-call.s3.GetObject",
        lambda params, **kwargs: requests.append(params["headers"].get("Range")),
    )
    from tap_s3.formats import parquet_schema

    source = tap.bucket.source("big.parquet", len(body), False)
    parquet_schema(source)
    assert requests and all(r and r.startswith("bytes=") for r in requests)
    transferred = 0
    for header in requests:
        start, end = header[len("bytes="):].split("-")
        transferred += int(end) - int(start) + 1
    assert transferred < len(body) / 10


# Delta finding A: STATE size grows with the square of the window.


def test_state_stays_small_with_many_objects_in_the_window(bucket, clock):
    for index in range(1500):
        bucket.put(f"events/part-{index:05d}.csv", f"id\n{index}\n", minutes(1000))
    clock(1010)
    catalog = select_all(discover())
    messages = sync(catalog)
    states = [m for m in messages if m["type"] == "STATE"]
    assert len(records(messages, "events")) == 1500
    assert len(states) <= 25
    assert sum(len(json.dumps(m)) for m in states) < 2_000_000
    window = last_state(messages)["bookmarks"]["events"]["window"]
    assert isinstance(window, dict) and len(window) == 1500


def test_state_is_written_every_30_seconds(bucket, clock, monkeypatch):
    ticks = iter(range(0, 10_000, 31))
    monkeypatch.setattr("tap_s3.streams.monotonic", lambda: next(ticks))
    for index in range(3):
        bucket.put(f"orders/{index}.csv", "id\n1\n", minutes(index))
    messages = sync()
    values = [
        m["value"].get("bookmarks", {}).get("orders", {}).get("replication_key_value")
        for m in messages
        if m["type"] == "STATE"
    ]
    assert iso(0) in values and iso(1) in values


def test_state_is_written_before_a_failure(bucket, clock):
    bucket.put("orders/a.csv", "id\n1\n", minutes(1))
    bucket.put("orders/b.csv", "id\n2\n", minutes(2))
    catalog = select_all(discover())
    bucket.put("orders/b.csv", "id\nbad\n", minutes(2))
    clock(30)
    messages, error = sync_capturing(catalog)
    assert isinstance(error, ObjectParseError)
    stream_state = last_state(messages)["bookmarks"]["orders"]
    assert stream_state["replication_key_value"] == iso(-30)
    assert list(stream_state["window"]) == ["orders/a.csv"]


# Delta finding B: text that overflows to infinity.


def test_number_text_that_overflows_becomes_null(bucket):
    bucket.put("m.csv", "id,v\n1,1.5\n2,1e400\n3,-1e400\n")
    stream_records = records(sync(), "m")
    assert [r["v"] for r in stream_records] == [1.5, None, None]


# Delta finding C: the JSON wrapper rule.


def test_json_wrapper_ignores_a_sibling_array_with_non_objects(bucket):
    bucket.put("w.json", json.dumps({"data": [{"id": 1}], "notes": [{"n": 1}, "x"]}))
    stream_records = records(sync(), "w")
    assert [r["id"] for r in stream_records] == [1]


def test_json_wrapper_ignores_a_first_array_with_non_objects(bucket):
    bucket.put("w.json", json.dumps({"notes": [{"n": 1}, "x"], "data": [{"id": 1}]}))
    stream_records = records(sync(), "w")
    assert [r["id"] for r in stream_records] == [1]


def test_second_array_of_objects_is_seen_at_discovery(bucket, tap_logs):
    data = [{"id": i} for i in range(1200)]
    bucket.put("w.json", json.dumps({"data": data, "other": [{"x": 1}]}))
    catalog = discover()
    assert any(
        "s3://tap-s3-test/w.json" in m and "more than one array" in m for m in tap_logs
    )
    with pytest.raises(ObjectParseError, match="more than one array"):
        sync(select_all(catalog))


# Delta finding D: state written before the window existed.


def test_old_state_without_a_window_is_lowered_once(bucket, clock):
    clock(100)
    bucket.put("orders/late.csv", "id\n1\n", minutes(80))
    state = {"bookmarks": {"orders": {"replication_key_value": iso(90)}}}
    messages = sync(state=state)
    assert keys(messages) == ["orders/late.csv"]


def test_state_with_a_window_is_not_lowered(bucket, clock):
    clock(100)
    bucket.put("orders/late.csv", "id\n1\n", minutes(80))
    state = {"bookmarks": {"orders": {"replication_key_value": iso(90), "window": {}}}}
    assert keys(sync(state=state)) == []


# Delta finding E: unknown incremental_mode strings.


@pytest.mark.parametrize("value", ["maybe", "ture", "2"])
def test_unknown_incremental_mode_fails_clearly(bucket, value):
    bucket.put("orders/a.csv", "id\n1\n")
    with pytest.raises(Exception, match="incremental_mode"):
        sync(incremental_mode=value)
    with pytest.raises(Exception, match="incremental_mode"):
        discover(incremental_mode=value)


@pytest.mark.parametrize(
    "value, expected", [(" No ", False), ("YES", True), (" 0 ", False), ("  ", True)]
)
def test_incremental_mode_words(value, expected):
    from tap_s3.tap import parse_flag

    assert parse_flag(value, default=True) is expected


class _FileSource(ObjectSource):
    """An object source over a local file, so moto's copies don't count."""

    def __init__(self, path):
        self.path = path

    @contextlib.contextmanager
    def open(self):
        with open(self.path, "rb") as handle:
            yield handle


@pytest.mark.parametrize("wrapped", [False, True], ids=["array", "wrapped"])
def test_large_json_uses_bounded_memory(tmp_path, wrapped):
    rows = 200_000
    path = tmp_path / "big.json"
    with open(path, "w") as handle:
        handle.write('{"count": 1, "data": [' if wrapped else "[")
        for i in range(1, rows + 1):
            handle.write(("," if i > 1 else "") + f'{{"id": {i}, "email": "u{i}@example.com"}}')
        handle.write("]}" if wrapped else "]")
    size = path.stat().st_size
    tracemalloc.start()
    try:
        reader = iter_rows(_FileSource(path), FileFormat(".json", False))
        count = sum(1 for _ in reader)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert count == rows
    assert peak < size / 5


def test_unknown_incremental_mode_fails_discovery_without_validation(bucket):
    from singer_sdk.exceptions import ConfigValidationError

    tap = TapS3(
        config={**CONFIG, "incremental_mode": "maybe"},
        parse_env_config=False,
        validate_config=False,
        setup_mapper=False,
    )
    with pytest.raises(ConfigValidationError, match="Use true, false, yes, no, 1 or 0"):
        tap.discover_streams()
