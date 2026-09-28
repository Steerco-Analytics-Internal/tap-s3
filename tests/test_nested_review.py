"""Regression tests for the review of nested data.

Each test names the finding it covers.
"""

import contextlib
import io
import itertools
import json
import random

from tests.conftest import (
    discover,
    last_state,
    make_tap,
    minutes,
    records,
    schemas,
    select_all,
    sync,
)


def data(record):
    return {key: value for key, value in record.items() if not key.startswith("_")}


def root_metadata(entry):
    return next(item for item in entry["metadata"] if item["breadcrumb"] == [])["metadata"]


def select(catalog, names):
    chosen = select_all(catalog)
    for entry in chosen["streams"]:
        root_metadata(entry)["selected"] = entry["stream"] in names
    return chosen


def run(catalog, limits=None, state=None):
    tap = make_tap(catalog=catalog, state=state)
    for name, limit in (limits or {}).items():
        tap.streams[name].ABORT_AT_RECORD_COUNT = limit
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        tap.sync_all()
    return [json.loads(line) for line in output.getvalue().splitlines()]


# Finding 1: column naming is one-to-one per path, not settled per row.


def test_non_ascii_keys_keep_their_columns_across_rows(bucket):
    bucket.put(
        "s.jsonl",
        '{"id":1,"客户":{"名前":"Ann","年齢":30}}\n{"id":2,"客户":{"年齢":40}}\n',
    )
    catalog = select_all(discover())
    assert [c for c in schemas(catalog)["s"] if not c.startswith("_")] == [
        "id",
        "客户__名前",
        "客户__年齢",
    ]
    assert [data(r) for r in records(sync(catalog), "s")] == [
        {"id": 1, "客户__名前": "Ann", "客户__年齢": 30},
        {"id": 2, "客户__名前": None, "客户__年齢": 40},
    ]


def test_a_key_with_double_underscore_next_to_a_nested_path(bucket):
    rows = [{"a__b": 1, "a": {"b": 2}}, {"a": {"b": 3}}, {"a__b": 4}]
    bucket.put("s.json", json.dumps(rows))
    stream_records = [data(r) for r in records(sync(), "s")]
    assert stream_records == [
        {"a_x5f__b": 1, "a__b": 2},
        {"a_x5f__b": None, "a__b": 3},
        {"a_x5f__b": 4, "a__b": None},
    ]


def test_swapped_key_order_gives_the_same_columns(bucket):
    rows = [{"m": {"x-y": 1, "x_y": 2}}, {"m": {"x_y": 2, "x-y": 1}}, {"m": {"x_y": 5}}]
    bucket.put("s.json", json.dumps(rows))
    stream_records = [data(r) for r in records(sync(), "s")]
    assert stream_records == [
        {"m__x_x2d_y": 1, "m__x_y": 2},
        {"m__x_x2d_y": 1, "m__x_y": 2},
        {"m__x_x2d_y": None, "m__x_y": 5},
    ]


def test_every_name_decodes_to_its_path():
    from tap_s3.nested import CHILD_RESERVED, ROOT_RESERVED, column_name, decode_name

    alphabet = ["_", "_", "-", " ", "x", "a", "5", "f", "é", "客", "#", "/", "0"]
    rng = random.Random(7)
    names = {}
    paths = set()
    for _ in range(20_000):
        depth = rng.randint(1, 4)
        paths.add(
            tuple(
                "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 7)))
                for _ in range(depth)
            )
        )
    for length in range(6):
        for combo in itertools.product(["_", "x", "5", "-"], repeat=length):
            key = "".join(combo)
            paths.update({(key,), (key, "b"), ("b", key), (key, key), ("x", key, "_")})
    for reserved in (frozenset(), ROOT_RESERVED, CHILD_RESERVED):
        names.clear()
        for path in paths:
            name = column_name(path, reserved)
            assert decode_name(name) == path, (path, name)
            assert names.setdefault(name, path) == path
            assert name not in reserved
            if reserved is CHILD_RESERVED:
                assert not name.startswith("_parent__")


# Finding 2: two different lists merged into one child stream.


def test_two_lists_with_similar_paths_stay_apart(bucket):
    bucket.put("s.json", json.dumps([{"id": 1, "a__b": [{"v": 1}], "a": {"b": [{"v": 2}]}}]))
    catalog = select_all(discover())
    assert sorted(schemas(catalog)) == ["s", "s__a__b", "s__a_x5f__b"]
    messages = sync(catalog)
    assert [(r["_row_key"], r["v"]) for r in records(messages, "s__a_x5f__b")] == [
        ("s.json#1/a_x5f__b#0", 1)
    ]
    assert [(r["_row_key"], r["v"]) for r in records(messages, "s__a__b")] == [
        ("s.json#1/a__b#0", 2)
    ]


# Finding 3a: a file stream and a child stream with the same name.


def test_file_stream_and_child_stream_names_stay_unique(bucket, tap_logs):
    bucket.put("orders.json", json.dumps([{"id": 1, "items": [{"v": 1}]}]))
    bucket.put("orders__items.json", json.dumps([{"sku": "x"}]))
    catalog = discover()
    names = [entry["stream"] for entry in catalog["streams"]]
    assert sorted(names) == ["orders", "orders__items", "orders__items_2"]
    assert len(set(names)) == len(names)
    child = next(e for e in catalog["streams"] if e["stream"] == "orders__items_2")
    assert root_metadata(child)["tap-s3.parent-stream"] == "orders"
    assert any("orders__items_2" in m and "is taken" in m for m in tap_logs)
    messages = sync(select_all(catalog))
    assert [data(r) for r in records(messages, "orders__items")] == [{"sku": "x"}]
    assert [data(r) for r in records(messages, "orders__items_2")] == [{"v": 1}]
    assert [data(r) for r in records(messages, "orders")] == [{"id": 1}]


# Finding 3b: catalog mode took the longest name prefix as the parent.


def test_child_uses_its_recorded_parent_not_a_name_prefix(bucket):
    bucket.put("orders.json", json.dumps([{"id": 1, "v2": {"lines": [{"q": 1}]}}]), minutes(1))
    bucket.put("orders__v2.json", json.dumps([{"z": 1}]), minutes(1))
    catalog = discover()
    assert sorted(e["stream"] for e in catalog["streams"]) == [
        "orders",
        "orders__v2",
        "orders__v2__lines",
    ]
    messages = sync(select_all(catalog))
    assert [r["q"] for r in records(messages, "orders__v2__lines")] == [1]
    assert last_state(messages)["bookmarks"]["orders__v2__lines"]["replication_key_value"]


def test_a_renamed_child_still_finds_its_parent(bucket):
    bucket.put("orders.json", json.dumps([{"id": 1, "lines": [{"q": 1}, {"q": 2}]}]))
    catalog = select_all(discover())
    for entry in catalog["streams"]:
        if entry["stream"] == "orders__lines":
            entry["stream"] = entry["tap_stream_id"] = "zz_lines"
    assert [r["q"] for r in records(sync(catalog), "zz_lines")] == [1, 2]


# Finding 5: a stream that hits its record limit isn't marked done.


def test_limited_child_streams_keep_their_state(bucket, clock):
    lines = [json.dumps({"id": i, "a": [{"v": i}], "b": [{"w": i}] if i in (0, 49) else []})
             for i in range(50)]
    bucket.put("o/x.jsonl", "\n".join(lines) + "\n", minutes(1))
    catalog = select(discover(), {"o__a", "o__b"})
    first = run(catalog, limits={"o__a": 5, "o__b": 5})
    assert len(records(first, "o__a")) == 5
    bookmarks = last_state(first)["bookmarks"]
    for name in ("o__a", "o__b"):
        assert "replication_key_value" not in bookmarks.get(name, {})
        assert not bookmarks.get(name, {}).get("window")
    second = sync(catalog, state=last_state(first))
    assert len(records(second, "o__a")) == 50
    assert len(records(second, "o__b")) == 2


def test_a_dry_run_moves_no_bookmark(bucket):
    for index in range(3):
        bucket.put(f"o/{index}.json", json.dumps([{"id": index}]), minutes(index))
    tap = make_tap(catalog=select_all(discover()))
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        tap.run_sync_dry_run(dry_run_record_limit=1)
    messages = [json.loads(line) for line in output.getvalue().splitlines()]
    assert "replication_key_value" not in last_state(messages)["bookmarks"].get("o", {})


# Finding 6: tests that kill surviving mutants.


def test_reading_goes_on_while_the_file_stream_is_selected(bucket):
    lines = [json.dumps({"id": i, "items": [{"v": i}]}) for i in range(50)]
    bucket.put("o/a.jsonl", "\n".join(lines) + "\n", minutes(1))
    bucket.put("o/b.jsonl", json.dumps({"id": 50, "items": [{"v": 50}]}) + "\n", minutes(2))
    messages = run(select_all(discover()), limits={"o__items": 2})
    assert len(records(messages, "o__items")) == 2
    assert len(records(messages, "o")) == 51


def test_a_child_that_has_an_object_doesnt_get_it_again(bucket, clock):
    bucket.put("o/a.json", json.dumps([{"id": 1, "items": [{"v": 1}]}]), minutes(1))
    catalog = discover()
    clock(30)
    first = sync(select(catalog, {"o__items"}))
    assert len(records(first, "o__items")) == 1
    second = sync(select_all(catalog), state=last_state(first))
    assert len(records(second, "o")) == 1
    assert records(second, "o__items") == []


def test_null_parent_values_make_no_parent_columns(bucket):
    rows = [
        {"id": 1, "info": {"a": 1}, "items": [{"v": 1}]},
        {"id": 2, "info": None, "note": None, "items": [{"v": 2}]},
    ]
    bucket.put("o.json", json.dumps(rows))
    catalog = discover()
    child = schemas(catalog)["o__items"]
    assert "_parent__info" not in child
    assert "_parent__note" not in child
    assert child["_parent__id"] == {"type": ["integer", "null"]}


def test_schema_comes_before_records_for_every_stream(bucket):
    bucket.put("o.json", json.dumps([{"id": 1, "a": [{"v": 1, "c": [{"d": 1}]}]}]))
    messages = sync(select_all(discover()))
    seen = set()
    for message in messages:
        if message["type"] == "SCHEMA":
            seen.add(message["stream"])
        if message["type"] == "RECORD":
            assert message["stream"] in seen
    assert {"o", "o__a", "o__a__c"} <= seen


def test_a_catalog_object_links_children_by_name(bucket, tap_logs):
    from singer_sdk._singerlib import Catalog

    bucket.put("o.json", json.dumps([{"id": 1, "items": [{"v": 1}]}]))
    catalog = Catalog.from_dict(select_all(discover()))
    tap = make_tap(catalog=catalog)
    assert sorted(tap.streams) == ["o", "o__items"]
    assert any("o__items" in m and "by its name" in m for m in tap_logs)


# Final review N3: a CSV header and a JSON key give the same column.


def test_csv_headers_and_json_keys_share_one_encoding(bucket, tap_logs):
    bucket.put("h/a.csv", "id,a__b,_s3_key,x-y\n1,2,3,4\n", minutes(1))
    bucket.put(
        "h/b.jsonl",
        json.dumps({"id": 5, "a__b": 6, "_s3_key": 7, "x-y": 8}) + "\n",
        minutes(2),
    )
    catalog = select_all(discover())
    assert [c for c in schemas(catalog)["h"]] == [
        "id",
        "a_x5f__b",
        "_x5f_s3_key",
        "x-y",
        "_s3_key",
        "_s3_last_modified",
        "_row_number",
    ]
    rows = [dict(data(r), key=r["_s3_key"]) for r in records(sync(catalog), "h")]
    assert rows == [
        {"id": "1", "a_x5f__b": "2", "x-y": "4", "key": "h/a.csv"},
        {"id": "5", "a_x5f__b": "6", "x-y": "8", "key": "h/b.jsonl"},
    ]
    assert any("a__b to a_x5f__b" in m for m in tap_logs)


# Each guard for limited streams holds on its own.


EARLY = "2026-09-01T11:00:00+00:00"


def limited_child_run(bucket):
    # Three objects with one row each stay under the limit, so each object
    # finishes, and only the guards keep the bookmark still.
    for index in range(3):
        bucket.put(f"o/{index}.jsonl", json.dumps({"id": index, "a": [{"v": index}]}) + "\n",
                   minutes(index + 1))
    catalog = select(discover(), {"o__a"})
    state = {"bookmarks": {"o__a": {"replication_key_value": EARLY, "window": {}}}}
    messages = run(catalog, limits={"o__a": 5}, state=state)
    assert len(records(messages, "o__a")) == 3
    return last_state(messages)["bookmarks"]["o__a"]


def test_the_state_copy_alone_protects_a_limited_stream(bucket, monkeypatch):
    from tap_s3.streams import S3Stream

    monkeypatch.setattr(S3Stream, "_keeps_progress", staticmethod(lambda stream: True))
    assert limited_child_run(bucket) == {"replication_key_value": EARLY, "window": {}}


def test_the_progress_guard_alone_protects_a_limited_stream(bucket, monkeypatch):
    from tap_s3.streams import S3Stream

    monkeypatch.setattr(
        S3Stream,
        "_progress_state",
        staticmethod(lambda stream: stream.get_context_state(None)),
    )
    assert limited_child_run(bucket) == {"replication_key_value": EARLY, "window": {}}
