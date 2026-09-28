"""Type inference for discovery, and value conversion for sync.

Columns from delimited files are always strings. For JSON and JSONL,
inference is conservative: a column gets a type other than string only when
every non-empty sampled value has that type. Parquet uses its own schema.
"""

import base64
import datetime
import decimal
import json
import math
import re
from typing import Any, Callable, Dict, Iterable, List, Optional

import pyarrow as pa

INTEGER = "integer"
NUMBER = "number"
BOOLEAN = "boolean"
DATE_TIME = "date-time"
DATE = "date"
STRING = "string"
OBJECT = "object"
ARRAY = "array"
EMPTY = "empty"

_INTEGER_TEXT = re.compile(r"^[+-]?(0|[1-9]\d*)$")
_NUMBER_TEXT = re.compile(r"^[+-]?(0|[1-9]\d*)?(\.\d+)?([eE][+-]?\d+)?$")
_ISO_DATE_TIME_TEXT = re.compile(
    r"^(\d{4}-\d{2}-\d{2})"
    r"(?:[T ](\d{2}:\d{2}(?::\d{2})?)(\.\d+)?(Z|[+-]\d{2}(?::?\d{2})?)?)?$",
    re.IGNORECASE,
)


class ValueConversionError(ValueError):
    """A value does not match the type the catalog gives its column."""


def parse_iso_datetime(text: str) -> Optional[datetime.datetime]:
    """Parse an ISO 8601 date or date-time. Return None for anything else.

    Only the ISO layout is accepted, so `09/01/2026` and `Sep 1 2026` stay
    strings. A date without a time is midnight.
    """
    match = _ISO_DATE_TIME_TEXT.match(text.strip())
    if not match:
        return None
    day, clock, fraction, zone = match.groups()
    normalized = day
    if clock:
        normalized += "T" + clock
        if fraction:
            normalized += fraction[:7]
        if zone:
            zone = zone.upper()
            if zone == "Z":
                zone = "+00:00"
            elif len(zone) == 3:
                zone += ":00"
            elif ":" not in zone:
                zone = zone[:3] + ":" + zone[3:]
            normalized += zone
    try:
        return datetime.datetime.fromisoformat(normalized)
    except ValueError:
        return None


def is_number_text(text: str) -> bool:
    """True when text is a plain decimal number, such as `-1.5` or `2e3`.

    Leading zeros, `NaN` and `inf` don't count.
    """
    stripped = text.strip()
    return bool(
        _NUMBER_TEXT.match(stripped) and any(char.isdigit() for char in stripped)
    )


def json_value_type(value: Any) -> str:
    """Infer the type of one decoded JSON value.

    A JSON string can only become a date-time. A string such as "42" stays a
    string, because the producer chose to quote it.
    """
    if value is None:
        return EMPTY
    if isinstance(value, bool):
        return BOOLEAN
    if isinstance(value, int):
        return INTEGER
    if isinstance(value, (float, decimal.Decimal)):
        return NUMBER
    if isinstance(value, dict):
        return OBJECT
    if isinstance(value, list):
        return ARRAY
    if isinstance(value, str):
        if value == "":
            return EMPTY
        return DATE_TIME if parse_iso_datetime(value) is not None else STRING
    return STRING


def arrow_type(data_type: "pa.DataType") -> str:
    """Map a scalar Parquet column's Arrow type to a column type.

    `tap_s3.nested` handles dictionary, struct, list and map types first.
    """
    types = pa.types
    if types.is_boolean(data_type):
        return BOOLEAN
    if types.is_integer(data_type):
        return INTEGER
    if types.is_floating(data_type) or types.is_decimal(data_type):
        return NUMBER
    if types.is_timestamp(data_type):
        return DATE_TIME
    if types.is_date(data_type):
        return DATE
    return STRING


def merge_types(current: str, new: str) -> str:
    """Combine two column types into the narrowest type that holds both."""
    if current == new or new == EMPTY:
        return current
    if current == EMPTY:
        return new
    if {current, new} == {INTEGER, NUMBER}:
        return NUMBER
    return STRING


class ColumnTypes:
    """Collects column names and types across sampled files.

    Columns keep the order in which they are first seen.
    """

    def __init__(self, rename: Callable[[str], str] = str) -> None:
        self.types: Dict[str, str] = {}
        self.rename = rename

    def observe(self, name: str, column_type: str) -> None:
        """Record one observed type for a column, under its renamed name."""
        name = self.rename(name)
        self.types[name] = merge_types(self.types.get(name, EMPTY), column_type)

    def merge(self, other: "ColumnTypes") -> None:
        """Add the columns that another collector found."""
        for name, column_type in other.types.items():
            self.types[name] = merge_types(self.types.get(name, EMPTY), column_type)

    def observe_columns(self, names: Iterable[str]) -> None:
        """Record columns that exist but have no values yet."""
        for name in names:
            self.observe(name, EMPTY)

    def observe_text_row(self, row: Dict[str, Any]) -> None:
        """Record a row from a delimited file.

        Delimited columns are always text, with no inference. Steerco's sync
        schema converts text to numbers, dates and booleans downstream, so a
        value outside the sample can't break the sync.
        """
        for name, value in row.items():
            self.observe(name, EMPTY if value is None else STRING)

    def observe_json_row(self, row: Dict[str, Any]) -> None:
        """Record a row from a JSON or JSONL file."""
        for name, value in row.items():
            self.observe(name, json_value_type(value))

    def properties(self) -> Dict[str, dict]:
        """Build nullable JSON schema properties for the collected columns."""
        return {name: property_schema(kind) for name, kind in self.types.items()}


def property_schema(column_type: str) -> dict:
    """The nullable JSON schema for one column type."""
    if column_type in (INTEGER, NUMBER, BOOLEAN, OBJECT, ARRAY):
        return {"type": [column_type, "null"]}
    if column_type in (DATE_TIME, DATE):
        return {"type": ["string", "null"], "format": column_type}
    return {"type": ["string", "null"]}


def schema_type(prop: dict) -> Optional[str]:
    """Read a column type back from a JSON schema property.

    Returns None when the property has several non-null types, or a type this
    tap does not produce. Values in such a column pass through as JSON.
    """
    declared = prop.get("type", [])
    if isinstance(declared, str):
        declared = [declared]
    kinds = [kind for kind in declared if kind != "null"]
    if len(kinds) != 1:
        return None
    kind = kinds[0]
    if kind == "string":
        return prop.get("format") if prop.get("format") in (DATE_TIME, DATE) else STRING
    if kind in (INTEGER, NUMBER, BOOLEAN, OBJECT, ARRAY):
        return kind
    return None


def is_not_finite(value: Any) -> bool:
    """True for NaN and infinite floats or decimals, which JSON can't hold."""
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, decimal.Decimal):
        return not value.is_finite()
    return False


def iso_utc(value: datetime.datetime) -> str:
    """Format a datetime with an offset. A naive value is taken as UTC."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    return value.isoformat()


def to_json_value(value: Any) -> Any:
    """Convert a nested value into plain JSON values.

    NaN and infinity become None. A naive datetime is taken as UTC.
    """
    if is_not_finite(value):
        return None
    if isinstance(value, dict):
        return {str(key): to_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        if value and all(isinstance(item, tuple) and len(item) == 2 for item in value):
            # PyArrow returns a Parquet map as a list of key and value pairs.
            return {str(key): to_json_value(item) for key, item in value}
        return [to_json_value(item) for item in value]
    if isinstance(value, datetime.datetime):
        return iso_utc(value)
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value


def _fail(value: Any, kind: str) -> ValueConversionError:
    return ValueConversionError(f"value {value!r} is not a valid {kind}")


def _to_integer(value: Any) -> Any:
    if isinstance(value, bool):
        raise _fail(value, INTEGER)
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, decimal.Decimal) and value == value.to_integral_value():
        return int(value)
    if isinstance(value, str) and _INTEGER_TEXT.match(value.strip()):
        return int(value.strip())
    raise _fail(value, INTEGER)


def _to_number(value: Any) -> Any:
    if isinstance(value, bool):
        raise _fail(value, NUMBER)
    if isinstance(value, (int, float, decimal.Decimal)):
        return value
    if isinstance(value, str) and is_number_text(value):
        stripped = value.strip()
        return int(stripped) if _INTEGER_TEXT.match(stripped) else float(stripped)
    raise _fail(value, NUMBER)


def _to_boolean(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    raise _fail(value, BOOLEAN)


def _to_date_time(value: Any) -> Any:
    if isinstance(value, datetime.datetime):
        return iso_utc(value)
    if isinstance(value, datetime.date):
        return iso_utc(datetime.datetime.combine(value, datetime.time()))
    if isinstance(value, str):
        parsed = parse_iso_datetime(value)
        if parsed is not None:
            return iso_utc(parsed)
    raise _fail(value, DATE_TIME)


def _to_date(value: Any) -> Any:
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, str):
        return value
    raise _fail(value, DATE)


def _to_string(value: Any) -> Any:
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(to_json_value(value), ensure_ascii=False)
    converted = to_json_value(value)
    return converted if isinstance(converted, str) else str(converted)


def _to_object(value: Any) -> Any:
    converted = to_json_value(value)
    if isinstance(converted, dict):
        return converted
    raise _fail(value, OBJECT)


def _to_array(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return [to_json_value(item) for item in value]
    raise _fail(value, ARRAY)


_CONVERTERS: Dict[Optional[str], Callable[[Any], Any]] = {
    INTEGER: _to_integer,
    NUMBER: _to_number,
    BOOLEAN: _to_boolean,
    DATE_TIME: _to_date_time,
    DATE: _to_date,
    STRING: _to_string,
    OBJECT: _to_object,
    ARRAY: _to_array,
    None: to_json_value,
}


class RecordConverter:
    """Converts raw rows to the types in a stream's catalog schema.

    Columns that are not in the schema are left out. `dropped` holds their
    names, so the stream can log them. A schema column that the row lacks is
    set to None, so every record has the same keys.

    An empty string is None in every column that isn't a string, the same as
    during inference. NaN and infinity are None in every column.
    """

    def __init__(self, properties: Dict[str, dict]) -> None:
        self.converters = {}
        self.blank_is_null = set()
        for name, prop in properties.items():
            kind = schema_type(prop)
            self.converters[name] = _CONVERTERS[kind]
            if kind not in (STRING, None):
                self.blank_is_null.add(name)

    def convert(self, row: Dict[str, Any]) -> "ConvertedRow":
        """Convert one row. Raise ValueConversionError on a type mismatch."""
        record: Dict[str, Any] = {}
        dropped: List[str] = []
        for name, value in row.items():
            converter = self.converters.get(name)
            if converter is None:
                # A null in an unknown column carries no data, such as a null
                # object whose fields became columns. Only report real values.
                if value is not None:
                    dropped.append(name)
                continue
            if (
                value is None
                or is_not_finite(value)
                or (value == "" and name in self.blank_is_null)
            ):
                record[name] = None
                continue
            try:
                record[name] = converter(value)
            except ValueConversionError as err:
                raise ValueConversionError(f"column {name!r}: {err}") from None
            if is_not_finite(record[name]):
                # Text such as 1e400 overflows to infinity when parsed.
                record[name] = None
        for name in self.converters:
            record.setdefault(name, None)
        return ConvertedRow(record, dropped)


class ConvertedRow:
    """A converted record and the names of the columns it dropped."""

    def __init__(self, record: Dict[str, Any], dropped: List[str]) -> None:
        self.record = record
        self.dropped = dropped
