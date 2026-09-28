"""Nested JSON and Parquet data taken apart into tables."""

import contextlib
import io
import json
import pathlib
import tracemalloc

import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner
from tests import golden
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
    window_end,
)

from tap_s3.formats import FileFormat, ObjectSource, iter_rows
from tap_s3.nested import MAX_DEPTH, explode
from tap_s3.streams import ObjectParseError
from tap_s3.tap import TapS3

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
MODEL_N = (FIXTURES / "modeln_customers_nested_2026-09.json").read_text()
ADJUSTMENTS = "customers_nested__valueRealization__adjustments"


def select(catalog, names):
    """Select only the named streams, with all their properties."""
    chosen = select_all(catalog)
    for entry in chosen["streams"]:
        for item in entry["metadata"]:
            if item["breadcrumb"] == []:
                item["metadata"]["selected"] = entry["stream"] in names
    return chosen


def data_columns(properties):
    return [name for name in properties if not name.startswith("_")]


def sync_capturing(catalog, **settings):
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


def run_cli(args):
    result = CliRunner(mix_stderr=False).invoke(TapS3.cli, args, catch_exceptions=False)
    assert result.exit_code == 0, result.stderr
    return result.stdout


def test_model_n_report_end_to_end(bucket, tmp_path):
    bucket.put("customers_nested/2026-09.json", MODEL_N, minutes(1))
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(CONFIG))

    catalog = json.loads(run_cli(["--config", str(config_path), "--discover"]))
    entries = {entry["stream"]: entry for entry in catalog["streams"]}
    assert sorted(entries) == ["customers_nested", ADJUSTMENTS]
    assert not any("licenseUtilization" in name for name in entries)

    parent = entries["customers_nested"]["schema"]["properties"]
    assert data_columns(parent) == [
        "id",
        "name",
        "domain",
        "month",
        "valueRealization__grossSales",
        "valueRealization__totalRequested",
        "valueRealization__avoidedLeakage",
        "valueRealization__netSalesAllowed",
        "licenseUtilization__storage__used",
        "licenseUtilization__storage__threshold",
        "licenseUtilization__storage__unit",
        "licenseUtilization__transactions__used",
        "licenseUtilization__transactions__threshold",
        "licenseUtilization__transactions__unit",
        "licenseUtilization__revenue_processed__used",
        "licenseUtilization__revenue_processed__threshold",
        "licenseUtilization__revenue_processed__unit",
        "licenseUtilization__api_calls__used",
        "licenseUtilization__api_calls__threshold",
        "licenseUtilization__api_calls__unit",
    ]
    assert parent["valueRealization__grossSales"] == {"type": ["number", "null"]}
    assert parent["licenseUtilization__storage__threshold"] == {"type": ["integer", "null"]}
    assert parent["licenseUtilization__transactions__threshold"] == {
        "type": ["number", "null"]
    }
    assert "valueRealization" not in parent
    assert "valueRealization__adjustments" not in parent
    assert entries["customers_nested"]["key_properties"] == ["_s3_key", "_row_number"]

    child_entry = entries[ADJUSTMENTS]
    child = child_entry["schema"]["properties"]
    assert child_entry["key_properties"] == ["_s3_key", "_row_key"]
    assert child_entry["replication_key"] == "_s3_last_modified"
    assert data_columns(child) == [
        "name",
        "moduleTag",
        "requested",
        "invalid",
        "leakagePct",
        "allowed",
    ]
    for column in ("_parent__id", "_parent__name", "_parent__domain", "_parent__month"):
        assert child[column] == {"type": ["string", "null"]}
    assert child["_parent_row"] == {"type": ["string"]}
    assert child["_index"] == {"type": ["integer"]}
    assert child["_row_key"] == {"type": ["string"]}

    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps(select_all(catalog)))
    output = run_cli(["--config", str(config_path), "--catalog", str(catalog_path)])
    messages = [json.loads(line) for line in output.splitlines()]
    parents = records(messages, "customers_nested")
    adjustments = records(messages, ADJUSTMENTS)
    assert len(parents) == 5
    assert len(adjustments) == 20
    assert {m["stream"] for m in messages if m["type"] == "RECORD"} == {
        "customers_nested",
        ADJUSTMENTS,
    }

    first = parents[0]
    assert first["id"] == "northwind-bio"
    assert first["valueRealization__grossSales"] == 52380.74
    assert first["licenseUtilization__storage__used"] == 8.3
    assert first["licenseUtilization__storage__unit"] == "TB"

    assert adjustments[0] == {
        "name": "Chargebacks",
        "moduleTag": "Provider Management",
        "requested": 7043.84,
        "invalid": 38.98,
        "leakagePct": 0.55,
        "allowed": 7004.86,
        "_parent__id": "northwind-bio",
        "_parent__name": "Northwind Biologics",
        "_parent__domain": "northwind-bio.example",
        "_parent__month": "2026-09",
        "_parent_row": "customers_nested/2026-09.json#1",
        "_index": 0,
        "_row_key": "customers_nested/2026-09.json#1/valueRealization__adjustments#0",
        "_s3_key": "customers_nested/2026-09.json",
        "_s3_last_modified": iso(1),
    }
    assert {row["_parent__domain"] for row in adjustments} == {
        row["domain"] for row in parents
    }
    assert [row["_index"] for row in adjustments[:4]] == [0, 1, 2, 3]
    assert len({row["_row_key"] for row in adjustments}) == 20
    final = messages[-1]["value"]["bookmarks"]
    assert final["customers_nested"]["replication_key_value"] == window_end()
    assert final[ADJUSTMENTS]["replication_key_value"] == window_end()


def test_each_object_is_read_once_for_all_streams(bucket):
    bucket.put("customers_nested/2026-09.json", MODEL_N, minutes(1))
    catalog = select_all(discover())
    tap = make_tap(catalog=catalog)
    gets = []
    tap.bucket.client.meta.events.register(
        "before-call.s3.GetObject", lambda params, **kwargs: gets.append(params)
    )
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        tap.sync_all()
    assert len(gets) == 1


def test_deep_nesting_with_grandchildren(bucket):
    document = [
        {
            "id": "o1",
            "region": {"code": "EU", "meta": {"tier": 2}},
            "lines": [
                {
                    "sku": "A",
                    "price": {"amount": 5, "currency": "EUR"},
                    "taxes": [{"kind": "vat", "rate": 0.2}, {"kind": "eco", "rate": 0.01}],
                },
                {"sku": "B", "price": {"amount": 7, "currency": "EUR"}, "taxes": []},
            ],
        }
    ]
    bucket.put("orders.json", json.dumps(document))
    catalog = discover()
    found = schemas(catalog)
    assert sorted(found) == ["orders", "orders__lines", "orders__lines__taxes"]
    assert data_columns(found["orders"]) == ["id", "region__code", "region__meta__tier"]
    assert data_columns(found["orders__lines"]) == ["sku", "price__amount", "price__currency"]
    assert "_parent__id" in found["orders__lines"]
    taxes = found["orders__lines__taxes"]
    assert data_columns(taxes) == ["kind", "rate"]
    assert "_parent__sku" in taxes and "_parent__id" not in taxes

    messages = sync(select_all(catalog))
    lines = records(messages, "orders__lines")
    grandchildren = records(messages, "orders__lines__taxes")
    assert [row["sku"] for row in lines] == ["A", "B"]
    assert lines[0]["price__amount"] == 5
    assert len(grandchildren) == 2
    assert grandchildren[1]["_parent__sku"] == "A"
    assert grandchildren[1]["_parent_row"] == lines[0]["_row_key"]
    assert grandchildren[1]["_row_key"] == "orders.json#1/lines#0/taxes#1"
    assert grandchildren[1]["_index"] == 1


def nest(depth, leaf):
    value = leaf
    for level in reversed(range(depth)):
        value = {f"l{level}": value}
    return value


def test_depth_cap_keeps_the_rest_as_json_text():
    record = nest(12, 1)
    piece = next(explode(record, "deep", "k#1", {}))
    capped = "__".join(f"l{level}" for level in range(MAX_DEPTH))
    assert list(piece.row) == [capped]
    assert json.loads(piece.row[capped]) == {"l10": {"l11": 1}}
    shallow = next(explode(nest(MAX_DEPTH, 1), "deep", "k#1", {}))
    assert shallow.row == {capped: 1}


def test_depth_cap_counts_list_levels():
    record = nest(MAX_DEPTH, [{"x": 1}])
    pieces = list(explode(record, "deep", "k#1", {}))
    assert len(pieces) == 1
    name = "__".join(f"l{level}" for level in range(MAX_DEPTH))
    assert json.loads(pieces[0].row[name]) == [{"x": 1}]
    # One level up, the list is a child stream, and its items sit at the cap.
    record = nest(MAX_DEPTH - 1, [{"x": {"y": 1}}])
    pieces = list(explode(record, "deep", "k#1", {}))
    assert len(pieces) == 2
    assert json.loads(pieces[1].row["x"]) == {"y": 1}


def test_name_collisions_get_a_suffix_and_a_warning(bucket, tap_logs):
    rows = [{"a__b": 1, "a": {"b": 2}, "x y": {"c": 3}, "x-y": {"c": 4}}]
    bucket.put("clash.json", json.dumps(rows))
    catalog = discover()
    assert data_columns(schemas(catalog)["clash"]) == ["a__b", "a__b_2", "x_y__c", "x_y__c_2"]
    record = records(sync(select_all(catalog)), "clash")[0]
    assert (record["a__b"], record["a__b_2"]) == (1, 2)
    assert (record["x_y__c"], record["x_y__c_2"]) == (3, 4)
    warnings = [m for m in tap_logs if "nested columns whose names clash" in m]
    assert warnings and "a.b to a__b_2" in warnings[0]


def test_child_columns_named_like_child_metadata_are_renamed(bucket):
    rows = [{"id": 1, "items": [{"_index": 9, "_row_key": "mine", "_parent__id": 5, "v": 1}]}]
    bucket.put("o.json", json.dumps(rows))
    catalog = discover()
    child = schemas(catalog)["o__items"]
    assert {"_index_source", "_row_key_source", "_parent__id_source"} <= set(child)
    row = records(sync(select_all(catalog)), "o__items")[0]
    assert (row["_index"], row["_index_source"]) == (0, 9)
    assert row["_row_key_source"] == "mine"
    assert (row["_parent__id"], row["_parent__id_source"]) == (1, 5)


def test_mixed_and_plain_lists_are_json_text(bucket):
    rows = [
        {"id": 1, "tags": ["a", "b"], "mixed": [{"x": 1}, "y"], "grid": [[1, 2]]},
        {"id": 2, "tags": [], "mixed": [None, {"x": 2}], "grid": None},
    ]
    bucket.put("lists.jsonl", "\n".join(json.dumps(row) for row in rows) + "\n")
    catalog = discover()
    found = schemas(catalog)
    assert sorted(found) == ["lists", "lists__mixed"]
    for column in ("tags", "mixed", "grid"):
        assert found["lists"][column] == {"type": ["string", "null"]}
    messages = sync(select_all(catalog))
    parent = records(messages, "lists")
    assert parent[0]["tags"] == '["a", "b"]'
    assert parent[0]["mixed"] == '[{"x": 1}, "y"]'
    assert parent[0]["grid"] == "[[1, 2]]"
    assert parent[1]["tags"] is None and parent[1]["mixed"] is None
    child = records(messages, "lists__mixed")
    assert [(row["x"], row["_index"]) for row in child] == [(2, 1)]


def test_empty_lists_and_null_objects(bucket, tap_logs):
    rows = [
        {"id": 1, "info": {"a": 1}, "items": [{"v": 1}]},
        {"id": 2, "info": None, "items": []},
        {"id": 3, "info": {}, "items": None},
    ]
    bucket.put("sparse.json", json.dumps(rows))
    catalog = discover()
    found = schemas(catalog)
    assert data_columns(found["sparse"]) == ["id", "info__a"]
    messages = sync(select_all(catalog))
    parent = records(messages, "sparse")
    assert [row["info__a"] for row in parent] == [1, None, None]
    assert all("info" not in row and "items" not in row for row in parent)
    assert len(records(messages, "sparse__items")) == 1
    assert not any("not in the catalog schema" in m for m in tap_logs)


def test_only_empty_lists_and_null_objects_make_no_columns(bucket):
    bucket.put("empty.json", json.dumps([{"id": 1, "info": None, "items": []}]))
    found = schemas(discover())
    assert list(found) == ["empty"]
    assert data_columns(found["empty"]) == ["id", "info"]


def test_child_schema_is_a_union_across_files(bucket):
    bucket.put("orders/a.json", json.dumps([{"id": 1, "lines": [{"sku": "A"}]}]), minutes(1))
    bucket.put(
        "orders/b.jsonl",
        json.dumps({"id": 2, "lines": [{"sku": "B", "qty": 2}, {"note": "x"}]}) + "\n",
        minutes(2),
    )
    found = schemas(discover())
    assert data_columns(found["orders__lines"]) == ["sku", "qty", "note"]
    assert found["orders__lines"]["qty"] == {"type": ["integer", "null"]}


def test_child_types_merge_to_string(bucket):
    bucket.put("o/a.json", json.dumps([{"lines": [{"v": 1}]}]), minutes(1))
    bucket.put("o/b.json", json.dumps([{"lines": [{"v": "one"}]}]), minutes(2))
    catalog = discover()
    assert schemas(catalog)["o__lines"]["v"] == {"type": ["string", "null"]}
    child = records(sync(select_all(catalog)), "o__lines")
    assert [row["v"] for row in child] == ["1", "one"]


def test_jsonl_nested_input(bucket):
    lines = [
        {"id": 1, "who": {"name": "Ada"}, "events": [{"at": "2026-09-01T00:00:00Z"}]},
        {"id": 2, "who": {"name": "Alan"}, "events": [{"at": "2026-09-02T00:00:00Z"}]},
    ]
    bucket.put("log.jsonl", "\n".join(json.dumps(line) for line in lines) + "\n")
    catalog = discover()
    found = schemas(catalog)
    assert found["log__events"]["at"] == {"type": ["string", "null"], "format": "date-time"}
    messages = sync(select_all(catalog))
    assert [row["who__name"] for row in records(messages, "log")] == ["Ada", "Alan"]
    events = records(messages, "log__events")
    assert [row["_parent_row"] for row in events] == ["log.jsonl#1", "log.jsonl#2"]
    assert events[1]["at"] == "2026-09-02T00:00:00+00:00"


def test_parquet_nested_structs_and_lists(bucket):
    line_type = pa.struct(
        [
            ("sku", pa.string()),
            ("price", pa.struct([("amount", pa.float64())])),
            ("taxes", pa.list_(pa.struct([("rate", pa.float64())]))),
        ]
    )
    table = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "meta": pa.array(
                [{"source": {"system": "erp"}}, None],
                pa.struct([("source", pa.struct([("system", pa.string())]))]),
            ),
            "lines": pa.array(
                [
                    [{"sku": "A", "price": {"amount": 5.0}, "taxes": [{"rate": 0.2}]}],
                    [],
                ],
                pa.list_(line_type),
            ),
            "labels": pa.array([["x"], None], pa.list_(pa.string())),
        }
    )
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    bucket.put("orders.parquet", buffer.getvalue())
    catalog = discover()
    found = schemas(catalog)
    assert sorted(found) == ["orders", "orders__lines", "orders__lines__taxes"]
    assert data_columns(found["orders"]) == ["id", "labels", "meta__source__system"]
    assert found["orders"]["labels"] == {"type": ["string", "null"]}
    assert data_columns(found["orders__lines"]) == ["sku", "price__amount"]
    assert found["orders__lines"]["_parent__id"] == {"type": ["integer", "null"]}
    assert found["orders__lines__taxes"]["rate"] == {"type": ["number", "null"]}
    assert found["orders__lines__taxes"]["_parent__sku"] == {"type": ["string", "null"]}

    messages = sync(select_all(catalog))
    parent = records(messages, "orders")
    assert [row["meta__source__system"] for row in parent] == ["erp", None]
    assert parent[0]["labels"] == '["x"]'
    lines = records(messages, "orders__lines")
    assert [(row["sku"], row["price__amount"], row["_parent__id"]) for row in lines] == [
        ("A", 5.0, 1)
    ]
    taxes = records(messages, "orders__lines__taxes")
    assert [(row["rate"], row["_parent__sku"]) for row in taxes] == [(0.2, "A")]


def test_select_only_a_child_stream(bucket):
    bucket.put("customers_nested/2026-09.json", MODEL_N, minutes(1))
    catalog = select(discover(), {ADJUSTMENTS})
    messages = sync(catalog)
    assert {m["stream"] for m in messages if m["type"] in ("RECORD", "SCHEMA")} == {ADJUSTMENTS}
    assert len(records(messages, ADJUSTMENTS)) == 20
    bookmarks = last_state(messages)["bookmarks"]
    assert bookmarks[ADJUSTMENTS]["replication_key_value"] == window_end()
    assert "replication_key_value" not in bookmarks.get("customers_nested", {})


def test_select_only_the_parent_stream(bucket):
    bucket.put("customers_nested/2026-09.json", MODEL_N, minutes(1))
    catalog = select(discover(), {"customers_nested"})
    messages = sync(catalog)
    assert {m["stream"] for m in messages if m["type"] in ("RECORD", "SCHEMA")} == {
        "customers_nested"
    }
    assert len(records(messages, "customers_nested")) == 5
    assert "replication_key_value" not in last_state(messages)["bookmarks"].get(
        ADJUSTMENTS, {}
    )


def test_child_with_its_parent_left_out_of_the_catalog(bucket):
    bucket.put("customers_nested/2026-09.json", MODEL_N, minutes(1))
    catalog = select_all(discover())
    catalog["streams"] = [e for e in catalog["streams"] if e["stream"] == ADJUSTMENTS]
    messages = sync(catalog)
    assert {m["stream"] for m in messages if m["type"] == "RECORD"} == {ADJUSTMENTS}
    assert len(records(messages)) == 20


def test_child_stream_without_a_parent_in_the_bucket_is_skipped(bucket, tap_logs):
    bucket.put("customers_nested/2026-09.json", MODEL_N, minutes(1))
    catalog = select_all(discover())
    for entry in catalog["streams"]:
        if entry["stream"] == ADJUSTMENTS:
            entry["stream"] = entry["tap_stream_id"] = "gone__things"
    messages = sync(catalog)
    assert {m["stream"] for m in messages if m["type"] == "RECORD"} == {"customers_nested"}
    assert any("gone__things" in m and "no parent stream" in m for m in tap_logs)


def test_child_found_after_discovery_is_ignored(bucket):
    bucket.put("o/a.json", json.dumps([{"id": 1}]), minutes(1))
    catalog = select_all(discover())
    bucket.put("o/b.json", json.dumps([{"id": 2, "new": [{"v": 1}]}]), minutes(2))
    messages = sync(catalog)
    assert {m["stream"] for m in messages if m["type"] == "RECORD"} == {"o"}
    assert len(records(messages, "o")) == 2


def test_newly_selected_child_reads_old_objects_once(bucket, clock):
    bucket.put("orders/a.json", json.dumps([{"id": 1, "lines": [{"v": 1}]}]), minutes(1))
    bucket.put("orders/b.json", json.dumps([{"id": 2, "lines": [{"v": 2}]}]), minutes(2))
    catalog = discover()
    clock(100)
    first = sync(select(catalog, {"orders"}))
    assert len(records(first, "orders")) == 2
    assert records(first, "orders__lines") == []

    bucket.put("orders/c.json", json.dumps([{"id": 3, "lines": [{"v": 3}]}]), minutes(90))
    clock(200)
    second = sync(select_all(catalog), state=last_state(first))
    assert [row["id"] for row in records(second, "orders")] == [3]
    assert [row["v"] for row in records(second, "orders__lines")] == [1, 2, 3]
    bookmarks = last_state(second)["bookmarks"]
    assert bookmarks["orders"]["replication_key_value"] == iso(140)
    assert bookmarks["orders__lines"]["replication_key_value"] == iso(140)

    third = sync(select_all(catalog), state=last_state(second))
    assert records(third) == []


def test_failure_mid_child_keeps_the_object_undone(bucket):
    bucket.put("orders/a.json", json.dumps([{"id": 1, "lines": [{"v": 1}]}]), minutes(1))
    bucket.put("orders/b.json", json.dumps([{"id": 2, "lines": [{"v": 2}]}]), minutes(2))
    catalog = select_all(discover())
    bad = [{"id": 2, "lines": [{"v": 3}, {"v": "not a number"}]}, {"id": 4}]
    bucket.put("orders/b.json", json.dumps(bad), minutes(2))
    messages, error = sync_capturing(catalog)
    assert isinstance(error, ObjectParseError)
    message = str(error)
    assert "s3://tap-s3-test/orders/b.json" in message
    assert "stream 'orders__lines' row orders/b.json#1/lines#1" in message
    assert [row["v"] for row in records(messages, "orders__lines")] == [1, 3]
    assert bookmark(messages, "orders") == iso(1)
    assert bookmark(messages, "orders__lines") == iso(1)


def test_record_limit_applies_to_child_streams(bucket):
    bucket.put("customers_nested/2026-09.json", MODEL_N, minutes(1))
    catalog = select(discover(), {ADJUSTMENTS})
    tap = make_tap(catalog=catalog)
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        assert tap.run_sync_dry_run(dry_run_record_limit=3) is True
    messages = [json.loads(line) for line in output.getvalue().splitlines()]
    assert len(records(messages, ADJUSTMENTS)) == 3
    assert len(records(messages, "customers_nested")) <= 4


def test_record_limit_stops_reading_when_only_children_are_selected(bucket):
    lines = [json.dumps({"id": i, "items": [{"v": i}]}) for i in range(50)]
    bucket.put("o/a.jsonl", "\n".join(lines) + "\n", minutes(1))
    bucket.put("o/b.jsonl", "{broken\n", minutes(2))
    catalog = select(discover(), {"o__items"})
    tap = make_tap(catalog=catalog)
    tap.streams["o__items"].ABORT_AT_RECORD_COUNT = 5
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        tap.sync_all()
    messages = [json.loads(line) for line in output.getvalue().splitlines()]
    assert len(records(messages, "o__items")) == 5


class _FileSource(ObjectSource):
    def __init__(self, path):
        self.path = path

    @contextlib.contextmanager
    def open(self):
        with open(self.path, "rb") as handle:
            yield handle


def nested_file(path, customers):
    with open(path, "w") as handle:
        handle.write("[")
        for index in range(customers):
            customer = {
                "id": f"c{index}",
                "stats": {"a": index, "b": {"c": index * 2}},
                "lines": [{"n": line, "d": {"e": line}} for line in range(5)],
            }
            handle.write(("," if index else "") + json.dumps(customer))
        handle.write("]")
    return path.stat().st_size


def peak_while_exploding(path):
    tracemalloc.start()
    try:
        counts = {}
        rows = iter_rows(_FileSource(path), FileFormat(".json", False))
        for number, row in enumerate(rows, 1):
            for piece in explode(row, "big", f"big.json#{number}", {}):
                counts[piece.stream] = counts.get(piece.stream, 0) + 1
        return counts, tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()


def test_large_nested_json_uses_bounded_memory(tmp_path):
    small_size = nested_file(tmp_path / "small.json", 20_000)
    large_size = nested_file(tmp_path / "large.json", 80_000)
    small_counts, small_peak = peak_while_exploding(tmp_path / "small.json")
    large_counts, large_peak = peak_while_exploding(tmp_path / "large.json")
    assert small_counts == {"big": 20_000, "big__lines": 100_000}
    assert large_counts == {"big": 80_000, "big__lines": 400_000}
    # The file is four times larger. Memory stays nearly flat.
    assert large_size > 3.9 * small_size
    assert large_peak < 1.6 * small_peak
    assert large_peak < large_size / 3


def test_flat_files_match_v1_0_0(bucket):
    expected = json.loads(golden.FIXTURE.read_text())
    actual = golden.flat_output(bucket, minutes, discover, select_all, sync)
    assert json.loads(json.dumps(actual)) == expected


def test_parquet_struct_below_the_depth_cap_is_json_text(bucket):
    data_type = pa.int64()
    value = 1
    for level in reversed(range(MAX_DEPTH + 1)):
        data_type = pa.struct([(f"l{level}", data_type)])
        value = {f"l{level}": value}
    table = pa.table({"deep": pa.array([value["l0"]], data_type[0].type)})
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    bucket.put("deep.parquet", buffer.getvalue())
    catalog = discover()
    name = "deep__" + "__".join(f"l{level}" for level in range(1, MAX_DEPTH))
    assert schemas(catalog)["deep"][name] == {"type": ["string", "null"]}
    record = records(sync(select_all(catalog)), "deep")[0]
    assert json.loads(record[name]) == {"l10": 1}


def test_parent_names_that_clash_get_a_suffix():
    record = {"a b": 1, "a-b": 2, "items": [{"v": 1}]}
    child = list(explode(record, "s", "k#1", {}))[1]
    assert (child.row["_parent__a_b"], child.row["_parent__a_b_2"]) == (1, 2)


def test_child_stream_reads_nothing_on_its_own(bucket):
    bucket.put("customers_nested/2026-09.json", MODEL_N, minutes(1))
    tap = make_tap()
    assert list(tap.streams[ADJUSTMENTS].get_records(None)) == []


def test_a_catalog_saved_on_v1_0_0_still_syncs(bucket, tap_logs):
    rows = [{"id": 1, "info": {"a": 1}, "tags": ["x"], "items": [{"v": 1}]}]
    bucket.put("old.json", json.dumps(rows))
    catalog = select_all(discover())
    catalog["streams"] = [e for e in catalog["streams"] if e["stream"] == "old"]
    catalog["streams"][0]["schema"]["properties"] = {
        "id": {"type": ["integer", "null"]},
        "info": {"type": ["object", "null"]},
        "tags": {"type": ["array", "null"]},
        "items": {"type": ["array", "null"]},
        "_s3_key": {"type": ["string"]},
        "_s3_last_modified": {"type": ["string"], "format": "date-time"},
        "_row_number": {"type": ["integer"]},
    }
    record = records(sync(catalog), "old")[0]
    assert (record["id"], record["info"], record["items"]) == (1, None, None)
    assert record["tags"] == ["x"]
    assert any("info__a" in m and "not in the catalog schema" in m for m in tap_logs)
