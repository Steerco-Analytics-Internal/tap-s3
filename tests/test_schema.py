"""Type inference and value conversion."""

import datetime
import decimal
import json

import pytest
from tests.conftest import discover, minutes, schemas

from tap_s3.schema import (
    ColumnTypes,
    RecordConverter,
    ValueConversionError,
    json_value_type,
    merge_types,
    parse_iso_datetime,
    property_schema,
    schema_type,
    text_type,
    to_json_value,
)


@pytest.mark.parametrize(
    "text, expected",
    [
        ("0", "integer"),
        ("42", "integer"),
        ("-7", "integer"),
        ("+7", "integer"),
        (" 42 ", "integer"),
        ("007", "string"),
        ("3.14", "number"),
        ("-0.5", "number"),
        (".5", "number"),
        ("1e6", "number"),
        ("1.", "string"),
        ("-", "string"),
        ("NaN", "string"),
        ("inf", "string"),
        ("1,000", "string"),
        ("true", "boolean"),
        ("FALSE", "boolean"),
        ("yes", "string"),
        ("2026-09-01", "date-time"),
        ("2026-09-01T12:30", "date-time"),
        ("2026-09-01 12:30:45", "date-time"),
        ("2026-09-01T12:30:45.123456789Z", "date-time"),
        ("2026-09-01T12:30:45+0200", "date-time"),
        ("2026-09-01T12:30:45-05", "date-time"),
        ("2026-02-30", "string"),
        ("09/01/2026", "string"),
        ("Sep 1 2026", "string"),
        ("20260901", "integer"),
        ("hello", "string"),
    ],
)
def test_text_type(text, expected):
    assert text_type(text) == expected


def test_parse_iso_datetime_normalizes():
    parsed = parse_iso_datetime("2026-09-01T12:30:45.123456789Z")
    assert parsed.isoformat() == "2026-09-01T12:30:45.123456+00:00"
    assert parse_iso_datetime("2026-09-01").isoformat() == "2026-09-01T00:00:00"
    assert parse_iso_datetime("nope") is None


@pytest.mark.parametrize(
    "current, new, expected",
    [
        ("empty", "integer", "integer"),
        ("integer", "empty", "integer"),
        ("integer", "number", "number"),
        ("number", "integer", "number"),
        ("integer", "boolean", "string"),
        ("date-time", "integer", "string"),
        ("date", "date-time", "string"),
        ("object", "array", "string"),
        ("object", "object", "object"),
        ("empty", "empty", "empty"),
    ],
)
def test_merge_types(current, new, expected):
    assert merge_types(current, new) == expected


def test_json_value_of_an_unknown_python_type_is_a_string():
    assert json_value_type(object()) == "string"


def test_json_values():
    columns = ColumnTypes()
    columns.observe_json_row(
        {
            "flag": True,
            "count": 3,
            "ratio": 0.5,
            "at": "2026-09-01T00:00:00Z",
            "label": "42",
            "blank": "",
            "none": None,
            "nested": {"a": 1},
            "list": [1, 2],
        }
    )
    columns.observe_json_row({"count": 4.5, "mixed": 1})
    columns.observe_json_row({"mixed": "one"})
    assert columns.types == {
        "flag": "boolean",
        "count": "number",
        "ratio": "number",
        "at": "date-time",
        "label": "string",
        "blank": "empty",
        "none": "empty",
        "nested": "object",
        "list": "array",
        "mixed": "string",
    }
    properties = columns.properties()
    assert properties["blank"] == {"type": ["string", "null"]}
    assert properties["nested"] == {"type": ["object", "null"]}
    assert properties["list"] == {"type": ["array", "null"]}


def test_inference_from_csv(bucket):
    body = (
        "int,num,flag,when,iso_date,us_date,mixed,zip,blank,sparse\n"
        "1,1.5,true,2026-09-01T10:00:00Z,2026-09-01,09/01/2026,1,02134,,\n"
        "2,2,False,2026-09-02 11:00,2026-09-02,09/02/2026,two,10001,,7\n"
        ",,,,,,,,,\n"
    )
    bucket.put("types.csv", body)
    properties = schemas(discover())["types"]
    assert properties["int"] == {"type": ["integer", "null"]}
    assert properties["num"] == {"type": ["number", "null"]}
    assert properties["flag"] == {"type": ["boolean", "null"]}
    assert properties["when"] == {"type": ["string", "null"], "format": "date-time"}
    assert properties["iso_date"] == {"type": ["string", "null"], "format": "date-time"}
    assert properties["us_date"] == {"type": ["string", "null"]}
    assert properties["mixed"] == {"type": ["string", "null"]}
    assert properties["zip"] == {"type": ["string", "null"]}
    assert properties["blank"] == {"type": ["string", "null"]}
    assert properties["sparse"] == {"type": ["integer", "null"]}
    assert properties["_s3_key"] == {"type": ["string"]}
    assert properties["_s3_last_modified"] == {"type": ["string"], "format": "date-time"}
    assert properties["_row_number"] == {"type": ["integer"]}


def test_every_property_is_nullable(bucket):
    bucket.put("a.csv", "x,y\n1,2\n")
    bucket.put("b.json", json.dumps([{"x": {"n": 1}, "y": [1]}]))
    for name, properties in schemas(discover()).items():
        for column, prop in properties.items():
            if not column.startswith("_"):
                assert "null" in prop["type"], (name, column)


def test_column_union_across_files(bucket):
    bucket.put("people/a.csv", "id,name\n1,Ada\n", minutes(1))
    bucket.put("people/b.csv", "id,email\n2,g@example.com\n", minutes(2))
    bucket.put("people/c.jsonl", '{"id": 3, "phone": "555"}\n', minutes(3))
    properties = schemas(discover())["people"]
    assert [p for p in properties if not p.startswith("_")] == ["id", "phone", "email", "name"]


def test_types_merge_across_files(bucket):
    bucket.put("people/a.csv", "id,score\n1,5\n", minutes(1))
    bucket.put("people/b.csv", "id,score\nx1,5.5\n", minutes(2))
    properties = schemas(discover())["people"]
    assert properties["id"]["type"] == ["string", "null"]
    assert properties["score"]["type"] == ["number", "null"]


def test_only_the_5_newest_objects_are_sampled(bucket):
    bucket.put("people/old.csv", "id,legacy\n1,x\n", minutes(0))
    for index in range(1, 6):
        bucket.put(f"people/new{index}.csv", "id\n1\n", minutes(index))
    properties = schemas(discover())["people"]
    assert "legacy" not in properties


def test_only_1000_rows_per_object_are_sampled(bucket):
    lines = ["id,code"] + [f"{i},{i}" for i in range(1, 1001)] + ["1001,ABC"]
    bucket.put("codes.csv", "\n".join(lines) + "\n")
    assert schemas(discover())["codes"]["code"]["type"] == ["integer", "null"]


@pytest.mark.parametrize(
    "prop, expected",
    [
        ({"type": ["integer", "null"]}, "integer"),
        ({"type": "number"}, "number"),
        ({"type": ["string", "null"], "format": "date-time"}, "date-time"),
        ({"type": ["string"], "format": "email"}, "string"),
        ({"type": ["string", "integer"]}, None),
        ({"type": ["null"]}, None),
        ({"type": ["custom", "null"]}, None),
        ({}, None),
    ],
)
def test_schema_type(prop, expected):
    assert schema_type(prop) == expected


def test_property_schema_round_trips():
    for kind in ("integer", "number", "boolean", "object", "array", "date-time", "date", "string"):
        assert schema_type(property_schema(kind)) == kind


def convert(kind, value):
    converter = RecordConverter({"v": property_schema(kind)})
    return converter.convert({"v": value}).record["v"]


@pytest.mark.parametrize(
    "kind, value, expected",
    [
        ("integer", "42", 42),
        ("integer", 42, 42),
        ("integer", 42.0, 42),
        ("integer", decimal.Decimal("3"), 3),
        ("number", "1.5", 1.5),
        ("number", "2", 2),
        ("number", 2, 2),
        ("number", decimal.Decimal("1.1"), decimal.Decimal("1.1")),
        ("boolean", "TRUE", True),
        ("boolean", "false", False),
        ("boolean", True, True),
        ("date-time", "2026-09-01", "2026-09-01T00:00:00+00:00"),
        ("date-time", "2026-09-01T10:00:00Z", "2026-09-01T10:00:00+00:00"),
        ("date-time", datetime.date(2026, 9, 1), "2026-09-01T00:00:00+00:00"),
        ("date-time", "2026-09-01T10:00:00-05:00", "2026-09-01T10:00:00-05:00"),
        ("number", float("nan"), None),
        ("number", decimal.Decimal("Infinity"), None),
        ("integer", "", None),
        ("boolean", "", None),
        ("string", "", ""),
        ("date", datetime.datetime(2026, 9, 1, 5), "2026-09-01"),
        ("date", "2026-09-01", "2026-09-01"),
        ("string", "x", "x"),
        ("string", 5, "5"),
        ("string", 1.5, "1.5"),
        ("string", False, "false"),
        ("string", {"a": [1]}, '{"a": [1]}'),
        ("string", [1, "é"], '[1, "é"]'),
        ("string", datetime.date(2026, 9, 1), "2026-09-01"),
        ("string", decimal.Decimal("1.25"), "1.25"),
        ("object", {"a": datetime.date(2026, 9, 1)}, {"a": "2026-09-01"}),
        ("array", (1, b"\x00"), [1, "AA=="]),
        ("integer", None, None),
    ],
)
def test_conversion(kind, value, expected):
    assert convert(kind, value) == expected


@pytest.mark.parametrize(
    "kind, value",
    [
        ("integer", "N/A"),
        ("integer", "1.5"),
        ("integer", True),
        ("integer", 1.5),
        ("number", "abc"),
        ("number", False),
        ("boolean", "yes"),
        ("boolean", 1),
        ("date-time", "09/01/2026"),
        ("date-time", 5),
        ("date", 5),
        ("object", "{}"),
        ("array", "[]"),
    ],
)
def test_conversion_failures(kind, value):
    with pytest.raises(ValueConversionError, match="column 'v'"):
        convert(kind, value)


def test_untyped_property_passes_values_through():
    converter = RecordConverter({"v": {"type": ["string", "integer"]}})
    assert converter.convert({"v": 5}).record == {"v": 5}


def test_converter_drops_unknown_columns_and_fills_missing():
    converter = RecordConverter({"a": property_schema("integer"), "b": property_schema("string")})
    converted = converter.convert({"a": "1", "extra": "x"})
    assert converted.record == {"a": 1, "b": None}
    assert converted.dropped == ["extra"]


def test_to_json_value_handles_nested_types():
    value = {
        "when": datetime.datetime(2026, 9, 1, 1, 2, 3),
        "time": datetime.time(4, 5),
        "money": decimal.Decimal("1.50"),
        "pairs": [("k", 1)],
        "raw": b"\xff",
    }
    assert to_json_value(value) == {
        "when": "2026-09-01T01:02:03+00:00",
        "time": "04:05:00",
        "money": 1.5,
        "pairs": {"k": 1},
        "raw": "/w==",
    }
