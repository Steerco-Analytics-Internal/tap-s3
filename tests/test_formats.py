"""Parsing every supported format, plain and gzipped, against moto."""

import datetime
import decimal
import io
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from tests.conftest import discover, gzipped, records, schemas, select_all, sync

from tap_s3.client import S3Bucket
from tap_s3.formats import column_names, sniff_delimiter, split_file_name
from tap_s3.streams import ObjectParseError

ROWS = [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]


def data_rows(stream_records):
    """Records without the metadata columns."""
    return [
        {key: value for key, value in record.items() if not key.startswith("_")}
        for record in stream_records
    ]


def as_csv(delimiter=","):
    lines = [delimiter.join(["id", "name"])]
    lines += [delimiter.join([str(row["id"]), row["name"]]) for row in ROWS]
    return "\n".join(lines) + "\n"


def as_json():
    return json.dumps(ROWS)


def as_jsonl():
    return "\n".join(json.dumps(row) for row in ROWS) + "\n"


def as_parquet(table=None):
    buffer = io.BytesIO()
    pq.write_table(table if table is not None else pa.Table.from_pylist(ROWS), buffer)
    return buffer.getvalue()


FORMATS = {
    "people.csv": as_csv(),
    "people.tsv": as_csv("\t"),
    "people.txt": as_csv("|"),
    "people.json": as_json(),
    "people.jsonl": as_jsonl(),
    "people.ndjson": as_jsonl(),
    "people.parquet": as_parquet(),
}


@pytest.mark.parametrize("compressed", [False, True], ids=["plain", "gzip"])
@pytest.mark.parametrize("file_name", sorted(FORMATS))
def test_every_format(bucket, file_name, compressed):
    body = FORMATS[file_name]
    key = file_name + (".gz" if compressed else "")
    bucket.put(key, gzipped(body) if compressed else body)
    catalog = discover()
    properties = schemas(catalog)["people"]
    assert properties["id"]["type"] == ["integer", "null"]
    assert properties["name"]["type"] == ["string", "null"]
    stream_records = records(sync(select_all(catalog)), "people")
    assert data_rows(stream_records) == ROWS
    assert [r["_row_number"] for r in stream_records] == [1, 2]
    assert {r["_s3_key"] for r in stream_records} == {key}


@pytest.mark.parametrize(
    "file_name, stem, extension, compressed",
    [
        ("a.csv", "a", ".csv", False),
        ("A.CSV", "A", ".csv", False),
        ("a.b.csv.gz", "a.b", ".csv", True),
        ("a.NDJSON.GZ", "a", ".ndjson", True),
        ("a.snappy.parquet", "a.snappy", ".parquet", False),
    ],
)
def test_split_file_name(file_name, stem, extension, compressed):
    found_stem, file_format = split_file_name(file_name)
    assert (found_stem, file_format.extension, file_format.compressed) == (
        stem,
        extension,
        compressed,
    )


@pytest.mark.parametrize("file_name", ["a.xlsx", "a.gz", ".csv", "csv", "a.csv.zip"])
def test_split_file_name_rejects(file_name):
    assert split_file_name(file_name) is None


def assert_fails_loudly(tap_logs, key, message):
    """Discovery logs and skips the object. A sync fails on it."""
    uri = f"s3://tap-s3-test/{key}"
    catalog = discover()
    assert any(uri in line and message in line for line in tap_logs), tap_logs
    with pytest.raises(ObjectParseError) as caught:
        sync(select_all(catalog))
    assert uri in str(caught.value)
    assert message in str(caught.value)


def sync_one(bucket, key, body):
    bucket.put(key, body)
    catalog = discover()
    name = catalog["streams"][0]["stream"]
    return schemas(catalog)[name], data_rows(records(sync(select_all(catalog)), name))


def test_csv_with_a_bom(bucket):
    properties, rows = sync_one(bucket, "bom.csv", b"\xef\xbb\xbfid,name\n1,Ada\n")
    assert "id" in properties and "﻿id" not in properties
    assert rows == [{"id": 1, "name": "Ada"}]


def test_csv_in_cp1252(bucket):
    body = "id,name\n1,Café\n2,naïve € ok\n".encode("cp1252")
    _, rows = sync_one(bucket, "legacy.csv", body)
    assert rows == [{"id": 1, "name": "Café"}, {"id": 2, "name": "naïve € ok"}]


def test_cp1252_after_many_utf8_rows(bucket):
    lines = ["id,name"] + [f"{i},row {i}" for i in range(1, 5001)] + ["5001,Café"]
    body = ("\n".join(lines) + "\n").encode("cp1252")
    bucket.put("late.csv", body)
    catalog = select_all(discover())
    rows = data_rows(records(sync(catalog), "late"))
    assert len(rows) == 5001
    assert rows[0] == {"id": 1, "name": "row 1"}
    assert rows[-1] == {"id": 5001, "name": "Café"}
    assert [r["id"] for r in rows] == list(range(1, 5002))


def test_csv_without_a_trailing_newline(bucket):
    _, rows = sync_one(bucket, "last.csv", "id,name\r\n1,Ada\r\n2,Grace")
    assert rows == [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]


def test_csv_with_only_a_bom_has_no_columns(bucket):
    properties, rows = sync_one(bucket, "bom_only.csv", b"\xef\xbb\xbf")
    assert set(properties) == {"_s3_key", "_s3_last_modified", "_row_number"}
    assert rows == []


def test_csv_with_quoted_newlines_and_quotes(bucket):
    body = 'id,note\n1,"line one\nline two"\n2,"say ""hi"", ok"\n3,"a\r\nb"\n'
    _, rows = sync_one(bucket, "notes.csv", body)
    assert rows == [
        {"id": 1, "note": "line one\nline two"},
        {"id": 2, "note": 'say "hi", ok'},
        {"id": 3, "note": "a\r\nb"},
    ]


def test_csv_with_a_line_that_crosses_the_sniff_window(bucket):
    long_value = "x" * 20000
    body = f"id,note\n1,{long_value}\n2,short\n"
    _, rows = sync_one(bucket, "long.csv", body)
    assert rows == [{"id": 1, "note": long_value}, {"id": 2, "note": "short"}]


@pytest.mark.parametrize(
    "key, delimiter",
    [("semi.csv", ";"), ("tabbed.csv", "\t"), ("piped.txt", "|"), ("semi.tsv", ";")],
)
def test_delimiters_are_sniffed(bucket, key, delimiter):
    body = f"id{delimiter}name{delimiter}city\n1{delimiter}Ada{delimiter}Paris, FR\n"
    _, rows = sync_one(bucket, key, body)
    assert rows == [{"id": 1, "name": "Ada", "city": "Paris, FR"}]


@pytest.mark.parametrize(
    "sample, extension, expected",
    [
        ("", ".csv", ","),
        ("", ".tsv", "\t"),
        ("id\n1\n2\n", ".csv", ","),
        ("id\n1\n2\n", ".tsv", "\t"),
        ("a;b\n1;2\n", ".txt", ";"),
        ("name\nSmith; John\nDoe; Jane\n", ".csv", ","),
    ],
)
def test_sniff_delimiter(sample, extension, expected):
    assert sniff_delimiter(sample, extension) == expected


def test_header_only_csv_has_columns_and_no_rows(bucket):
    properties, rows = sync_one(bucket, "empty_export.csv", "id,name,email\n")
    assert {"id", "name", "email"} <= set(properties)
    assert properties["email"]["type"] == ["string", "null"]
    assert rows == []


def test_ragged_rows(bucket):
    body = "id,name,city\n1,Ada\n2,Grace,NYC,extra\n\n3,Alan,London\n"
    properties, rows = sync_one(bucket, "ragged.csv", body)
    assert "column_4" in properties
    assert rows == [
        {"id": 1, "name": "Ada", "city": None, "column_4": None},
        {"id": 2, "name": "Grace", "city": "NYC", "column_4": "extra"},
        {"id": 3, "name": "Alan", "city": "London", "column_4": None},
    ]


def test_duplicate_and_blank_headers(bucket):
    body = "id,name,name,,name\n1,a,b,c,d\n"
    properties, rows = sync_one(bucket, "dupes.csv", body)
    assert [p for p in properties if not p.startswith("_")] == [
        "id",
        "name",
        "name_2",
        "column_4",
        "name_3",
    ]
    assert rows == [{"id": 1, "name": "a", "name_2": "b", "column_4": "c", "name_3": "d"}]


def test_column_names():
    assert column_names(["a", "a_2", "a", " "]) == ["a", "a_2", "a_3", "column_4"]


def test_json_array(bucket):
    _, rows = sync_one(bucket, "people.json", as_json())
    assert rows == ROWS


def test_json_object_wrapping_an_array(bucket):
    body = json.dumps({"count": 2, "next": None, "data": ROWS, "tags": []})
    _, rows = sync_one(bucket, "people.json", body)
    assert rows == ROWS


def test_json_wrapper_with_only_an_empty_array(bucket):
    properties, rows = sync_one(bucket, "people.json", json.dumps({"data": []}))
    assert rows == []
    assert set(properties) == {"_s3_key", "_s3_last_modified", "_row_number"}


@pytest.mark.parametrize(
    "document, message",
    [
        ("42", "not an array or an object"),
        ('{"a": [{"x": 1}], "b": [{"y": 2}]}', "more than one array"),
        ('{"a": 1}', "no single field"),
        ('[{"a": 1}, 2]', "item 2 of the array is not a JSON object"),
        ('[{"a": 1}', "IncompleteJSONError"),
        ('[{"a": 1}] trailing', "IncompleteJSONError"),
        ("   ", "IncompleteJSONError"),
        ('{"a": [{"x": 1}, 2]}', "item 2 of field 'a' is not a JSON object"),
    ],
)
def test_bad_json_fails_with_the_key(bucket, tap_logs, document, message):
    bucket.put("bad.json", document or " ")
    assert_fails_loudly(tap_logs, "bad.json", message)


def test_json_wrapper_skips_other_fields(bucket):
    document = {
        "meta": {"page": {"size": 2}},
        "ids": [1, 2, [3]],
        "names": ["x"],
        "data": ROWS,
        "empty": [],
    }
    _, rows = sync_one(bucket, "people.json", json.dumps(document))
    assert rows == ROWS


def test_jsonl_with_blank_lines(bucket):
    body = "\n" + as_jsonl() + "\n   \n"
    _, rows = sync_one(bucket, "people.jsonl", body)
    assert rows == ROWS


@pytest.mark.parametrize(
    "body, message",
    [
        ('{"id": 1}\n{"id": \n', "line 2"),
        ('{"id": 1}\n[1, 2]\n', "line 2 is not a JSON object"),
    ],
)
def test_bad_jsonl_fails_with_the_key_and_line(bucket, tap_logs, body, message):
    bucket.put("bad.jsonl", body)
    assert_fails_loudly(tap_logs, "bad.jsonl", message)


def typed_parquet_table():
    utc = datetime.timezone.utc
    return pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "small": pa.array([1, None], pa.int8()),
            "score": pa.array([1.5, None], pa.float64()),
            "price": pa.array([decimal.Decimal("9.99"), None], pa.decimal128(10, 2)),
            "active": pa.array([True, False], pa.bool_()),
            "name": pa.array(["Ada", None], pa.string()),
            "tier": pa.array(["gold", "gold"]).dictionary_encode(),
            "created": pa.array(
                [datetime.datetime(2026, 9, 1, 12, tzinfo=utc), None],
                pa.timestamp("us", tz="UTC"),
            ),
            "birthday": pa.array([datetime.date(1815, 12, 10), None], pa.date32()),
            "blob": pa.array([b"\x00\x01", None], pa.binary()),
            "address": pa.array(
                [{"city": "London", "zip": "N1"}, None],
                pa.struct([("city", pa.string()), ("zip", pa.string())]),
            ),
            "tags": pa.array([["a", "b"], []], pa.list_(pa.string())),
            "attrs": pa.array([[("k", 1)], None], pa.map_(pa.string(), pa.int32())),
            "at_time": pa.array([datetime.time(9, 30), None], pa.time64("us")),
            "events": pa.array(
                [[{"at": datetime.datetime(2026, 1, 1, tzinfo=utc)}], None],
                pa.list_(pa.struct([("at", pa.timestamp("us", tz="UTC"))])),
            ),
        }
    )


def test_parquet_with_typed_and_nested_columns(bucket):
    properties, rows = sync_one(bucket, "typed.parquet", as_parquet(typed_parquet_table()))
    types = {name: prop["type"][0] for name, prop in properties.items()}
    assert types == {
        "id": "integer",
        "small": "integer",
        "score": "number",
        "price": "number",
        "active": "boolean",
        "name": "string",
        "tier": "string",
        "created": "string",
        "birthday": "string",
        "blob": "string",
        "address": "object",
        "tags": "array",
        "attrs": "object",
        "at_time": "string",
        "events": "array",
        "_s3_key": "string",
        "_s3_last_modified": "string",
        "_row_number": "integer",
    }
    assert properties["created"]["format"] == "date-time"
    assert properties["birthday"]["format"] == "date"
    assert rows[0] == {
        "id": 1,
        "small": 1,
        "score": 1.5,
        "price": 9.99,
        "active": True,
        "name": "Ada",
        "tier": "gold",
        "created": "2026-09-01T12:00:00+00:00",
        "birthday": "1815-12-10",
        "blob": "AAE=",
        "address": {"city": "London", "zip": "N1"},
        "tags": ["a", "b"],
        "attrs": {"k": 1},
        "at_time": "09:30:00",
        "events": [{"at": "2026-01-01T00:00:00+00:00"}],
    }
    assert rows[1]["small"] is None and rows[1]["address"] is None
    assert rows[1]["tags"] == []


def test_parquet_reads_by_row_group(bucket, monkeypatch):
    monkeypatch.setattr("tap_s3.formats.PARQUET_BATCH_ROWS", 10)
    table = pa.table({"id": list(range(1, 101))})
    buffer = io.BytesIO()
    pq.write_table(table, buffer, row_group_size=25)
    _, rows = sync_one(bucket, "many.parquet", buffer.getvalue())
    assert [row["id"] for row in rows] == list(range(1, 101))


def test_truncated_gzip_fails_with_the_key(bucket, tap_logs):
    body = gzipped(as_csv() * 2000)
    bucket.put("accounts.csv.gz", body[: len(body) // 2])
    assert_fails_loudly(tap_logs, "accounts.csv.gz", "EOFError")


def test_not_gzip_at_all_fails_with_the_key(bucket, tap_logs):
    bucket.put("accounts.csv.gz", as_csv())
    assert_fails_loudly(tap_logs, "accounts.csv.gz", "BadGzipFile")


@pytest.mark.parametrize("key", ["bad.parquet", "bad.parquet.gz"])
def test_invalid_parquet_fails_with_the_key(bucket, tap_logs, key):
    body = b"PAR1 this is not parquet" * 10
    bucket.put(key, gzipped(body) if key.endswith(".gz") else body)
    assert_fails_loudly(tap_logs, key, "ArrowInvalid")


def test_range_reader_seeks_and_reads(bucket):
    bucket.put("blob.bin", b"0123456789")
    source = S3Bucket("testing", "testing", "tap-s3-test", "us-east-1").source(
        "blob.bin", 10, False
    )
    with source.open_seekable() as handle:
        raw = handle.raw
        assert raw.seekable() and raw.readable()
        raw.seek(2)
        raw.seek(3, io.SEEK_CUR)
        assert raw.tell() == 5
        assert raw.read(3) == b"567"
        raw.seek(-1, io.SEEK_END)
        assert raw.read(5) == b"9"
        assert raw.read(5) == b""
        assert raw.readinto(bytearray()) == 0
