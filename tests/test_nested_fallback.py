"""Child streams in a catalog that has lost the tap's metadata keys.

A catalog store, such as Hotglue's, may drop metadata keys it doesn't know.
The tap then links each child stream by an exact match of its name against
the streams it names in the bucket, the way discovery does.
"""

import json
import pathlib

from tests.conftest import discover, last_state, minutes, records, select_all, sync

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
MODEL_N = (FIXTURES / "modeln_customers_nested_2026-09.json").read_text()
ADJUSTMENTS = "customers_nested__valueRealization__adjustments"
OWN_KEYS = ("tap-s3.parent-stream", "tap-s3.list-path")


def strip(catalog):
    """Drop the tap's metadata keys, as a catalog store might."""
    for entry in catalog["streams"]:
        for item in entry["metadata"]:
            for key in OWN_KEYS:
                item["metadata"].pop(key, None)
    assert "tap-s3" not in json.dumps(catalog)
    return catalog


def links(tap_logs, stream):
    return [m for m in tap_logs if f"Stream '{stream}' is linked" in m]


def test_stripped_catalog_syncs_the_children(bucket, tap_logs):
    bucket.put("customers_nested/2026-09.json", MODEL_N, minutes(1))
    catalog = strip(select_all(discover()))
    messages = sync(catalog)
    assert len(records(messages, "customers_nested")) == 5
    adjustments = records(messages, ADJUSTMENTS)
    assert len(adjustments) == 20
    assert adjustments[0]["_parent__domain"] == "northwind-bio.example"
    found = links(tap_logs, ADJUSTMENTS)
    assert len(found) == 1
    assert "parent stream 'customers_nested' by its name" in found[0]


def test_metadata_is_used_when_present(bucket, tap_logs):
    bucket.put("customers_nested/2026-09.json", MODEL_N, minutes(1))
    sync(select_all(discover()))
    found = links(tap_logs, ADJUSTMENTS)
    assert len(found) == 1
    assert "by its catalog metadata" in found[0]


def test_stripped_catalog_links_grandchildren(bucket):
    document = [{"id": 1, "lines": [{"sku": "A", "taxes": [{"rate": 0.2}, {"rate": 0.1}]}]}]
    bucket.put("orders.json", json.dumps(document))
    catalog = strip(select_all(discover()))
    messages = sync(catalog)
    lines = records(messages, "orders__lines")
    taxes = records(messages, "orders__lines__taxes")
    assert [row["sku"] for row in lines] == ["A"]
    assert [row["rate"] for row in taxes] == [0.2, 0.1]
    assert {row["_parent_row"] for row in taxes} == {lines[0]["_row_key"]}


def test_stripped_catalog_with_a_renamed_child_from_discovery(bucket, tap_logs):
    bucket.put("orders.json", json.dumps([{"id": 1, "items": [{"v": 1}]}]))
    bucket.put("orders__items.json", json.dumps([{"sku": "x"}]))
    catalog = strip(select_all(discover()))
    messages = sync(catalog)
    assert [row["v"] for row in records(messages, "orders__items_2")] == [1]
    assert [row["sku"] for row in records(messages, "orders__items")] == ["x"]
    assert "by its name" in links(tap_logs, "orders__items_2")[0]


def test_stripped_catalog_skips_a_child_whose_name_a_new_file_stream_took(
    bucket, tap_logs, clock
):
    bucket.put("orders.json", json.dumps([{"id": 1, "items": [{"v": 1}]}]), minutes(1))
    catalog = strip(select_all(discover()))
    assert sorted(e["stream"] for e in catalog["streams"]) == ["orders", "orders__items"]
    bucket.put("orders__items.json", json.dumps([{"sku": "x"}]), minutes(2))
    state = {"bookmarks": {"orders__items": {"replication_key_value": "keep-me"}}}
    messages = sync(catalog, state=state)
    assert {m["stream"] for m in messages if m["type"] == "RECORD"} == {"orders"}
    assert any(
        "Stream 'orders__items' is skipped" in m and "no child stream with that name" in m
        for m in tap_logs
    )
    assert last_state(messages)["bookmarks"]["orders__items"] == {
        "replication_key_value": "keep-me"
    }


def test_the_same_change_resolves_when_the_metadata_is_kept(bucket):
    bucket.put("orders.json", json.dumps([{"id": 1, "items": [{"v": 1}]}]), minutes(1))
    catalog = select_all(discover())
    bucket.put("orders__items.json", json.dumps([{"sku": "x"}]), minutes(2))
    messages = sync(catalog)
    assert [row["v"] for row in records(messages, "orders__items")] == [1]


def test_stripped_catalog_skips_a_child_name_the_bucket_lacks(bucket, tap_logs):
    bucket.put("orders.json", json.dumps([{"id": 1, "items": [{"v": 1}]}]))
    catalog = strip(select_all(discover()))
    for entry in catalog["streams"]:
        if entry["stream"] == "orders__items":
            entry["stream"] = entry["tap_stream_id"] = "orders__things"
    messages = sync(catalog)
    assert {m["stream"] for m in messages if m["type"] == "RECORD"} == {"orders"}
    assert any("Stream 'orders__things' is skipped" in m for m in tap_logs)
