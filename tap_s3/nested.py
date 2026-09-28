"""Take nested JSON and Parquet data apart into tables.

- A nested object becomes columns, with the path joined by `__`.
- A list of objects becomes a child stream, with one row per item.
- A list of plain values, or a list that mixes objects with other values,
  becomes one text column that holds the JSON.

Column names are one-to-one with paths. `encode_part` escapes each key so
that `decode_name` gives back the exact path from any name the tap makes.
So two different paths never share a column, whatever keys a row has, and
the name of a path depends on nothing but the path. The same rules run over
decoded values during a sync and during JSON discovery, and over Arrow types
during Parquet discovery.
"""

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, Iterator, List, Set, Tuple

import pyarrow as pa

from tap_s3.schema import STRING, arrow_type, to_json_value

SEPARATOR = "__"
MAX_DEPTH = 10
PARENT_PREFIX = "_parent__"
KEY_COLUMN = "_s3_key"
LAST_MODIFIED_COLUMN = "_s3_last_modified"
ROW_NUMBER_COLUMN = "_row_number"
PARENT_ROW_COLUMN = "_parent_row"
INDEX_COLUMN = "_index"
ROW_KEY_COLUMN = "_row_key"
ROOT_RESERVED: FrozenSet[str] = frozenset(
    {KEY_COLUMN, LAST_MODIFIED_COLUMN, ROW_NUMBER_COLUMN}
)
CHILD_RESERVED: FrozenSet[str] = frozenset(
    {KEY_COLUMN, LAST_MODIFIED_COLUMN, PARENT_ROW_COLUMN, INDEX_COLUMN, ROW_KEY_COLUMN}
)

_WORD = re.compile(r"\w")
_ESCAPE = re.compile(r"_x([0-9a-f]*)_")

Path = Tuple[str, ...]
Lineage = Tuple[Path, ...]


def _escape(char: str) -> str:
    return f"_x{ord(char):x}_"


def encode_part(key: Any) -> str:
    """Encode one key so that `decode_name` can give it back.

    Unicode letters and digits stay. Any other character becomes `_xHH_`,
    with the code point in hex. An underscore stays bare only between the
    first and last character, before a kept character, and where the text
    after it can't read as `_xHH_`. Every other underscore is escaped. So an
    encoded part never starts or ends with a bare underscore, and never
    holds `__` outside an escape. An empty key is `_x_`.

    The part is built from the end, so each underscore can see the exact
    text that follows it.
    """
    text = str(key)
    if not text:
        return "_x_"
    suffix = ""
    for index in range(len(text) - 1, -1, -1):
        char = text[index]
        if char == "_":
            bare = (
                index > 0
                and suffix != ""
                and suffix[0] != "_"
                # A separator or the end can follow the part, so read the part
                # as if an underscore came next.
                and _ESCAPE.match("_" + suffix + "_") is None
            )
            piece = "_" if bare else _escape(char)
        elif _WORD.match(char):
            piece = char
        else:
            piece = _escape(char)
        suffix = piece + suffix
    return suffix


def decode_name(name: str) -> Path:
    """Give back the path that a column or list name stands for."""
    parts: List[str] = []
    current: List[str] = []
    index = 0
    while index < len(name):
        escape = _ESCAPE.match(name, index)
        if escape:
            digits = escape.group(1)
            current.append(chr(int(digits, 16)) if digits else "")
            index = escape.end()
        elif name.startswith(SEPARATOR, index):
            parts.append("".join(current))
            current = []
            index += len(SEPARATOR)
        else:
            current.append(name[index])
            index += 1
    parts.append("".join(current))
    return tuple(parts)


def top_level_name(key: str, reserved: FrozenSet[str] = frozenset()) -> str:
    """The column for a key at the top level of a row.

    The key keeps its own name, so flat files keep their columns. Two kinds
    of keys are encoded instead, so that every name maps back to one path:

    - A key named like a metadata column, or starting with `_parent__` in a
      child row. The first underscore becomes `_x5f_`, as in `_x5f_s3_key`.
    - A key that reads as an encoded name, such as `a__b`. It is encoded
      with `encode_part`, as in `a_x5f__b`.
    """
    text = str(key)
    if text in reserved or (reserved is CHILD_RESERVED and text.startswith(PARENT_PREFIX)):
        return "_x5f_" + encode_part(text[1:])
    if decode_name(text) != (text,):
        return encode_part(text)
    return text


def column_name(path: Path, reserved: FrozenSet[str] = frozenset()) -> str:
    """The column for a path. Nested paths are encoded and joined by `__`."""
    if len(path) == 1:
        return top_level_name(path[0], reserved)
    return SEPARATOR.join(encode_part(part) for part in path)


def path_name(path: Path) -> str:
    """A list's path, as it appears in a child stream name and row key."""
    return SEPARATOR.join(encode_part(part) for part in path)


def child_stream_name(stream: str, path: Path) -> str:
    """The natural name of the child stream for a list inside `stream`."""
    return stream + SEPARATOR + path_name(path)


def parent_column(key: str) -> str:
    """The column that carries a parent's plain value into a child row."""
    return PARENT_PREFIX + encode_part(key)


def json_text(value: Any) -> str:
    """Serialize a value kept whole, such as a list of plain values."""
    return json.dumps(to_json_value(value), ensure_ascii=False)


@dataclass
class Split:
    """One row taken apart.

    `columns` holds the row's own columns, in order. `lists` holds each list
    of objects that becomes a child stream. `plain` holds the direct keys
    whose values are neither objects, lists nor null, for the child rows'
    `_parent__` columns. `containers` holds the column names of object and
    list paths, so discovery can drop their null placeholders. `notes` holds
    keys that were encoded because of their names, for a warning.
    """

    columns: Dict[str, Any] = field(default_factory=dict)
    lists: List[Tuple[Path, Any]] = field(default_factory=list)
    plain: Dict[str, Any] = field(default_factory=dict)
    containers: Set[str] = field(default_factory=set)
    notes: List[str] = field(default_factory=list)


def _add(split: Split, path: Path, payload: Any, reserved: FrozenSet[str]) -> None:
    name = column_name(path, reserved)
    if len(path) == 1 and name != str(path[0]):
        split.notes.append(f"{path[0]} to {name}")
    split.columns[name] = payload


def split_value(
    record: Dict[str, Any], level: int = 0, reserved: FrozenSet[str] = ROOT_RESERVED
) -> Split:
    """Take one decoded row apart.

    `level` is how deep the row sits below the file's top level. At
    MAX_DEPTH, an object or a list of objects is kept whole as JSON text.
    """
    split = Split()

    def walk(obj: Dict[str, Any], parts: Path) -> None:
        for key, value in obj.items():
            path = parts + (str(key),)
            too_deep = level + len(path) >= MAX_DEPTH
            if isinstance(value, dict):
                split.containers.add(column_name(path, reserved))
                if too_deep:
                    _add(split, path, json_text(value), reserved)
                else:
                    walk(value, path)
            elif isinstance(value, (list, tuple)):
                items = [item for item in value if item is not None]
                if items and all(isinstance(item, dict) for item in items) and not too_deep:
                    split.containers.add(column_name(path, reserved))
                    split.lists.append((path, value))
                elif items:
                    _add(split, path, json_text(value), reserved)
            else:
                _add(split, path, value, reserved)
                if len(path) == 1 and value is not None:
                    split.plain[str(key)] = value

    walk(record, ())
    return split


def split_arrow(
    fields: List["pa.Field"], level: int = 0, reserved: FrozenSet[str] = ROOT_RESERVED
) -> Split:
    """Take an Arrow schema apart by the same rules as `split_value`.

    Column payloads are column types. A list payload is the item's Arrow type.
    """
    split = Split()

    def walk(fields: List["pa.Field"], parts: Path) -> None:
        for arrow_field in fields:
            path = parts + (str(arrow_field.name),)
            data_type = arrow_field.type
            if pa.types.is_dictionary(data_type):
                data_type = data_type.value_type
            too_deep = level + len(path) >= MAX_DEPTH
            if pa.types.is_struct(data_type):
                split.containers.add(column_name(path, reserved))
                if too_deep:
                    _add(split, path, STRING, reserved)
                else:
                    walk(list(data_type), path)
            elif (
                pa.types.is_list(data_type)
                or pa.types.is_large_list(data_type)
                or pa.types.is_fixed_size_list(data_type)
            ):
                item_type = data_type.value_type
                if pa.types.is_struct(item_type) and not too_deep:
                    split.containers.add(column_name(path, reserved))
                    split.lists.append((path, item_type))
                else:
                    _add(split, path, STRING, reserved)
            elif pa.types.is_map(data_type):
                _add(split, path, STRING, reserved)
            else:
                column_type = arrow_type(data_type)
                _add(split, path, column_type, reserved)
                if len(path) == 1:
                    split.plain[str(arrow_field.name)] = column_type

    walk(fields, ())
    return split


def parent_columns(plain: Dict[str, Any]) -> Dict[str, Any]:
    """Name the parent's plain values for a child row."""
    return {parent_column(key): value for key, value in plain.items()}


@dataclass
class Piece:
    """One row for one stream, from a taken-apart record.

    `lineage` holds the path of each list from the file's top level down to
    this row's list. It is empty for the record's own row.
    """

    lineage: Lineage
    row: Dict[str, Any]
    split: Split


def explode(
    record: Dict[str, Any], row_key: str, base: Dict[str, Any]
) -> Iterator[Piece]:
    """Yield the record's own row, then every child row, depth first.

    `row_key` is the record's key, `<_s3_key>#<_row_number>`. `base` holds
    the file columns that every child row carries.
    """
    split = split_value(record)
    yield Piece((), split.columns, split)
    yield from _children(split, (), row_key, base, 0)


def _children(
    split: Split, lineage: Lineage, row_key: str, base: Dict[str, Any], level: int
) -> Iterator[Piece]:
    parents = parent_columns(split.plain)
    for path, items in split.lists:
        child_lineage = lineage + (path,)
        child_level = level + len(path)
        for index, item in enumerate(items):
            if item is None:
                continue
            key = f"{row_key}/{path_name(path)}#{index}"
            item_split = split_value(item, child_level, CHILD_RESERVED)
            row = dict(item_split.columns)
            row.update(parents)
            row[PARENT_ROW_COLUMN] = row_key
            row[INDEX_COLUMN] = index
            row[ROW_KEY_COLUMN] = key
            row.update(base)
            yield Piece(child_lineage, row, item_split)
            yield from _children(item_split, child_lineage, key, base, child_level)


@dataclass
class ArrowStream:
    """The columns found in a Parquet schema for one stream."""

    columns: List[Tuple[str, str]] = field(default_factory=list)
    containers: Set[str] = field(default_factory=set)
    notes: List[str] = field(default_factory=list)


def arrow_streams(schema: "pa.Schema") -> Dict[Lineage, ArrowStream]:
    """Find the streams and column types in a Parquet schema, by lineage."""
    found: Dict[Lineage, ArrowStream] = {}

    def visit(lineage: Lineage, split: Split, parents: Dict[str, Any], level: int) -> None:
        stream = found.setdefault(lineage, ArrowStream())
        stream.columns.extend(split.columns.items())
        stream.columns.extend(parents.items())
        stream.containers |= split.containers
        stream.notes.extend(split.notes)
        own_parents = parent_columns(split.plain)
        for path, item_type in split.lists:
            child_level = level + len(path)
            visit(
                lineage + (path,),
                split_arrow(list(item_type), child_level, CHILD_RESERVED),
                own_parents,
                child_level,
            )

    visit((), split_arrow(list(schema)), {}, 0)
    return found
