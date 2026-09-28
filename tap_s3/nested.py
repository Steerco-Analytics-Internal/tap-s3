"""Take nested JSON and Parquet data apart into tables.

- A nested object becomes columns, with the path joined by `__`.
- A list of objects becomes a child stream, with one row per item.
- A list of plain values, or a list that mixes objects with other values,
  becomes one text column that holds the JSON.

The same rules run over decoded values during a sync and during JSON
discovery, and over Arrow types during Parquet discovery, so both produce
the same column and stream names.
"""

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple

import pyarrow as pa

from tap_s3.layout import sanitize_name
from tap_s3.schema import STRING, arrow_type, to_json_value

SEPARATOR = "__"
MAX_DEPTH = 10
PARENT_PREFIX = "_parent__"
PARENT_ROW_COLUMN = "_parent_row"
INDEX_COLUMN = "_index"
ROW_KEY_COLUMN = "_row_key"
CHILD_ONLY_COLUMNS = (PARENT_ROW_COLUMN, INDEX_COLUMN, ROW_KEY_COLUMN)
FILE_COLUMNS = ("_s3_key", "_s3_last_modified")

Path = Tuple[str, ...]


def part_name(part: str) -> str:
    """Sanitize one path part like a stream name."""
    return sanitize_name(str(part)) or "field"


def column_name(path: Path) -> str:
    """The column for a path. A direct key keeps its own name."""
    if len(path) == 1:
        return str(path[0])
    return SEPARATOR.join(part_name(part) for part in path)


def path_name(path: Path) -> str:
    """A list's path, as it appears in a child stream name and row key."""
    return SEPARATOR.join(part_name(part) for part in path)


def child_stream_name(stream: str, path: Path) -> str:
    """The child stream for a list of objects at `path` inside `stream`."""
    return stream + SEPARATOR + path_name(path)


def json_text(value: Any) -> str:
    """Serialize a value kept whole, such as a list of plain values."""
    return json.dumps(to_json_value(value), ensure_ascii=False)


def is_child_reserved(name: str) -> bool:
    """True for names that child rows use for their own metadata."""
    return (
        name in CHILD_ONLY_COLUMNS
        or name in FILE_COLUMNS
        or name.startswith(PARENT_PREFIX)
    )


@dataclass
class Split:
    """One row taken apart.

    `columns` holds the row's own columns, in order. `lists` holds each list
    of objects that becomes a child stream. `plain` holds the direct keys
    whose values are neither objects nor lists, for the child rows'
    `_parent__` columns. `containers` holds the column names of object and
    list paths, so discovery can drop their null placeholders. `notes` holds
    renamed columns, for a warning.
    """

    columns: Dict[str, Any] = field(default_factory=dict)
    lists: List[Tuple[Path, Any]] = field(default_factory=list)
    plain: Dict[str, Any] = field(default_factory=dict)
    containers: Set[str] = field(default_factory=set)
    notes: List[str] = field(default_factory=list)


def _resolve(
    direct: List[Tuple[Path, Any]], nested: List[Tuple[Path, Any]], split: Split, child: bool
) -> Split:
    """Name the columns. Direct keys claim their names first.

    A flattened name that is already taken gets `_2`, then `_3`. In a child
    row, a column named like child metadata gets a `_source` suffix.
    """
    for path, payload in direct + nested:
        name = column_name(path)
        if child and is_child_reserved(name):
            split.notes.append(f"{name} to {name}_source")
            name = f"{name}_source"
        candidate = name
        counter = 2
        while candidate in split.columns:
            candidate = f"{name}_{counter}"
            counter += 1
        if candidate != name:
            split.notes.append(f"{'.'.join(map(str, path))} to {candidate}")
        split.columns[candidate] = payload
    return split


def split_value(record: Dict[str, Any], level: int = 0, child: bool = False) -> Split:
    """Take one decoded row apart.

    `level` is how deep the row sits below the file's top level. Past
    MAX_DEPTH, an object or a list of objects is kept whole as JSON text.
    """
    split = Split()
    direct: List[Tuple[Path, Any]] = []
    nested: List[Tuple[Path, Any]] = []

    def add(path: Path, value: Any) -> None:
        (direct if len(path) == 1 else nested).append((path, value))

    def walk(obj: Dict[str, Any], parts: Path) -> None:
        for key, value in obj.items():
            path = parts + (key,)
            too_deep = level + len(path) >= MAX_DEPTH
            if isinstance(value, dict):
                split.containers.add(column_name(path))
                if too_deep:
                    add(path, json_text(value))
                else:
                    walk(value, path)
            elif isinstance(value, (list, tuple)):
                items = [item for item in value if item is not None]
                if items and all(isinstance(item, dict) for item in items) and not too_deep:
                    split.containers.add(column_name(path))
                    split.lists.append((path, value))
                elif items:
                    add(path, json_text(value))
            else:
                add(path, value)
                if len(path) == 1 and value is not None:
                    split.plain[str(key)] = value

    walk(record, ())
    return _resolve(direct, nested, split, child)


def split_arrow(fields: List["pa.Field"], level: int = 0, child: bool = False) -> Split:
    """Take an Arrow schema apart by the same rules as `split_value`.

    Column payloads are column types. A list payload is the item's Arrow type.
    """
    split = Split()
    direct: List[Tuple[Path, Any]] = []
    nested: List[Tuple[Path, Any]] = []

    def add(path: Path, column_type: str) -> None:
        (direct if len(path) == 1 else nested).append((path, column_type))

    def walk(fields: List["pa.Field"], parts: Path) -> None:
        for arrow_field in fields:
            path = parts + (arrow_field.name,)
            data_type = arrow_field.type
            if pa.types.is_dictionary(data_type):
                data_type = data_type.value_type
            too_deep = level + len(path) >= MAX_DEPTH
            if pa.types.is_struct(data_type):
                split.containers.add(column_name(path))
                if too_deep:
                    add(path, STRING)
                else:
                    walk(list(data_type), path)
            elif (
                pa.types.is_list(data_type)
                or pa.types.is_large_list(data_type)
                or pa.types.is_fixed_size_list(data_type)
            ):
                item_type = data_type.value_type
                if pa.types.is_struct(item_type) and not too_deep:
                    split.containers.add(column_name(path))
                    split.lists.append((path, item_type))
                else:
                    add(path, STRING)
            elif pa.types.is_map(data_type):
                add(path, STRING)
            else:
                column_type = arrow_type(data_type)
                add(path, column_type)
                if len(path) == 1:
                    split.plain[str(arrow_field.name)] = column_type

    walk(fields, ())
    return _resolve(direct, nested, split, child)


def parent_columns(plain: Dict[str, Any]) -> Dict[str, Any]:
    """Name the parent's plain values for a child row."""
    columns: Dict[str, Any] = {}
    for key, value in plain.items():
        name = PARENT_PREFIX + part_name(key)
        candidate = name
        counter = 2
        while candidate in columns:
            candidate = f"{name}_{counter}"
            counter += 1
        columns[candidate] = value
    return columns


@dataclass
class Piece:
    """One row for one stream, from a taken-apart record."""

    stream: str
    row: Dict[str, Any]
    split: Split
    level: int


def explode(
    record: Dict[str, Any], stream: str, row_key: str, base: Dict[str, Any]
) -> Iterator[Piece]:
    """Yield the record's own row, then every child row, depth first.

    `row_key` is the record's key, `<_s3_key>#<_row_number>`. `base` holds
    the file columns that every child row carries.
    """
    split = split_value(record)
    yield Piece(stream, split.columns, split, 0)
    yield from _children(split, stream, row_key, base, 0)


def _children(
    split: Split, stream: str, row_key: str, base: Dict[str, Any], level: int
) -> Iterator[Piece]:
    parents = parent_columns(split.plain)
    for path, items in split.lists:
        child = child_stream_name(stream, path)
        child_level = level + len(path)
        for index, item in enumerate(items):
            if item is None:
                continue
            key = f"{row_key}/{path_name(path)}#{index}"
            item_split = split_value(item, child_level, child=True)
            row = dict(item_split.columns)
            row.update(parents)
            row[PARENT_ROW_COLUMN] = row_key
            row[INDEX_COLUMN] = index
            row[ROW_KEY_COLUMN] = key
            row.update(base)
            yield Piece(child, row, item_split, child_level)
            yield from _children(item_split, child, key, base, child_level)


def arrow_streams(
    schema: "pa.Schema", stream: str, rename: Callable[[str], str]
) -> Tuple[Dict[str, List[Tuple[str, str]]], Dict[str, Set[str]], List[str]]:
    """Find the streams and column types in a Parquet schema.

    Returns the columns of each stream in order, the container names of each
    stream, and any renamed columns. `rename` renames the root's columns
    that clash with its metadata columns.
    """
    columns: Dict[str, List[Tuple[str, str]]] = {}
    containers: Dict[str, Set[str]] = {}
    notes: List[str] = []

    def visit(
        name: str, split: Split, parents: Optional[Dict[str, Any]], level: int
    ) -> None:
        found = columns.setdefault(name, [])
        for column, column_type in split.columns.items():
            found.append((rename(column) if parents is None else column, column_type))
        if parents:
            found.extend(parents.items())
        containers.setdefault(name, set()).update(split.containers)
        notes.extend(split.notes)
        own_parents = parent_columns(split.plain)
        for path, item_type in split.lists:
            child_level = level + len(path)
            visit(
                child_stream_name(name, path),
                split_arrow(list(item_type), child_level, child=True),
                own_parents,
                child_level,
            )

    visit(stream, split_arrow(list(schema)), None, 0)
    return columns, containers, notes
