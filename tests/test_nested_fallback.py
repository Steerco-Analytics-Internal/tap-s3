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


def test_stripped_catalog_skips_a_child_renamed_at_discovery(bucket, tap_logs):
    bucket.put("orders.json", json.dumps([{"id": 1, "items": [{"v": 1}]}]))
    bucket.put("orders__items.json", json.dumps([{"sku": "x"}]))
    catalog = strip(select_all(discover()))
    messages = sync(catalog)
    assert records(messages, "orders__items_2") == []
    assert [row["sku"] for row in records(messages, "orders__items")] == ["x"]
    assert any(
        "Stream 'orders__items_2' is skipped" in m and "clash suffix" in m for m in tap_logs
    )


def test_stripped_catalog_skips_a_child_whose_name_a_new_file_stream_took(
    bucket, tap_logs, clock
):
    bucket.put("orders.json", json.dumps([{"id": 1, "items": [{"v": 1}]}]), minutes(1))
    catalog = strip(select_all(discover()))
    assert sorted(e["stream"] for e in catalog["streams"]) == ["orders", "orders__items"]
    bucket.put("orders__items.json", json.dumps([{"sku": "x"}]), minutes(2))
    state = {"bookmarks": {"orders__items": {"replication_key_value": "2026-09-01T11:00:00+00:00"}}}
    messages = sync(catalog, state=state)
    assert {m["stream"] for m in messages if m["type"] == "RECORD"} == {"orders"}
    assert any(
        "Stream 'orders__items' is skipped" in m and "clash suffix" in m
        for m in tap_logs
    )
    assert last_state(messages)["bookmarks"]["orders__items"] == {
        "replication_key_value": "2026-09-01T11:00:00+00:00"
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


# Final review N1: the stripped fallback linked the wrong list.


def test_stripped_fallback_never_links_a_list_that_moved_names(bucket, tap_logs, clock):
    document = [{"id": 1, "items": [{"v": "from_items"}], "items_2": [{"v": "from_items_2"}]}]
    bucket.put("orders.json", json.dumps(document), minutes(1))
    bucket.put("orders__items.json", json.dumps([{"sku": "x"}]), minutes(1))
    catalog = select_all(discover())
    assert sorted(e["stream"] for e in catalog["streams"]) == [
        "orders",
        "orders__items",
        "orders__items_2",
        "orders__items_2_2",
    ]
    stripped = strip(catalog)
    bucket.client.delete_object(Bucket=bucket.name, Key="orders__items.json")
    clock(2000)
    early = "2026-09-01T11:00:00+00:00"
    state = {"bookmarks": {"orders__items_2": {"replication_key_value": early}}}
    messages = sync(stripped, state=state)
    for name in ("orders__items_2", "orders__items_2_2"):
        assert records(messages, name) == []
        assert any(f"Stream '{name}' is skipped" in m and "clash suffix" in m for m in tap_logs)
    assert last_state(messages)["bookmarks"]["orders__items_2"] == {
        "replication_key_value": early
    }


def test_a_list_named_like_a_suffix_still_links(bucket):
    bucket.put("orders.json", json.dumps([{"id": 1, "items_2": [{"v": 1}]}]))
    catalog = strip(select_all(discover()))
    assert [row["v"] for row in records(sync(catalog), "orders__items_2")] == [1]


def test_stripped_child_with_columns_the_bucket_lacks_is_skipped(bucket, tap_logs):
    bucket.put("orders.json", json.dumps([{"id": 1, "items": [{"v": 1}]}]), minutes(1))
    catalog = strip(select_all(discover()))
    bucket.put("orders.json", json.dumps([{"id": 1, "items": [{"w": 1}]}]), minutes(2))
    messages = sync(catalog)
    assert records(messages, "orders__items") == []
    assert any(
        "Stream 'orders__items' is skipped" in m and "lacks its columns v" in m
        for m in tap_logs
    )


def test_several_file_streams_can_give_a_child_the_same_name(bucket, tap_logs):
    bucket.put("a.json", json.dumps([{"b": {"c": [{"v": 1}]}}]))
    bucket.put("a__b.json", json.dumps([{"c": [{"v": 2}]}]))
    catalog = strip(select_all(discover()))
    names = sorted(e["stream"] for e in catalog["streams"])
    assert names == ["a", "a__b", "a__b__c", "a__b__c_2"]
    messages = sync(catalog)
    assert records(messages, "a__b__c") == [] and records(messages, "a__b__c_2") == []
    assert any("Stream 'a__b__c' is skipped" in m for m in tap_logs)


# Final review N2: the fallback plans only the file streams it needs.


def test_fallback_reads_only_the_file_streams_it_needs(bucket, tap_logs):
    import contextlib
    import io

    from tests.conftest import make_tap

    for index in range(6):
        bucket.put(f"s{index}/a.json", json.dumps([{"id": index, "l": [{"v": index}]}]))
    catalog = strip(select_all(discover()))
    for entry in catalog["streams"]:
        for item in entry["metadata"]:
            if item["breadcrumb"] == []:
                item["metadata"]["selected"] = entry["stream"] == "s0__l"
    tap = make_tap(catalog=catalog)
    keys = []
    tap.bucket.client.meta.events.register(
        "before-call.s3.GetObject", lambda params, **kwargs: keys.append(params["url_path"])
    )
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        tap.sync_all()
    messages = [json.loads(line) for line in output.getvalue().splitlines()]
    assert [row["v"] for row in records(messages, "s0__l")] == [0]
    assert {key.rsplit("/", 2)[-2] for key in keys} == {"s0"}
    warnings = [m for m in tap_logs if "Run discovery again and save the catalog" in m]
    assert len(warnings) == 1


def test_a_suffixed_sibling_in_the_catalog_blocks_the_whole_file_stream(bucket, tap_logs):
    document = [{"id": 1, "items": [{"v": 1}], "other": [{"w": 1}]}]
    bucket.put("orders.json", json.dumps(document), minutes(1))
    bucket.put("orders__items.json", json.dumps([{"sku": "x"}]), minutes(1))
    catalog = strip(select_all(discover()))
    assert "orders__items_2" in [e["stream"] for e in catalog["streams"]]
    bucket.client.delete_object(Bucket=bucket.name, Key="orders__items.json")
    messages = sync(catalog)
    assert records(messages, "orders__other") == []
    assert any(
        "Stream 'orders__other' is skipped" in m
        and "in the catalog has a clash suffix" in m
        for m in tap_logs
    )
