"""Readers for the file formats the tap supports.

Each reader yields one dict per row. Delimited readers yield string values, or
None for an empty cell. JSON readers yield decoded JSON values. The Parquet
reader yields the Python values that PyArrow produces.
"""

import collections
import csv
import io
import json
import logging
from dataclasses import dataclass
from typing import (
    Any,
    BinaryIO,
    Callable,
    ContextManager,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
)

import ijson
import pyarrow as pa
import pyarrow.parquet as pq

LOGGER = logging.getLogger("tap-s3")

# The csv module refuses fields over 128 KiB by default. Exports with long
# text fields exceed that, so raise the cap to the largest portable value.
csv.field_size_limit(2**31 - 1)

DELIMITED = "delimited"
JSON = "json"
JSONL = "jsonl"
PARQUET = "parquet"

FORMAT_BY_EXTENSION = {
    ".csv": DELIMITED,
    ".tsv": DELIMITED,
    ".txt": DELIMITED,
    ".json": JSON,
    ".jsonl": JSONL,
    ".ndjson": JSONL,
    ".parquet": PARQUET,
}

UTF8_BOM = b"\xef\xbb\xbf"
DELIMITED_ENCODINGS = ("utf-8", "cp1252")
LAST_RESORT_ENCODING = "latin-1"
SNIFF_CHARS = 16 * 1024
PARQUET_BATCH_ROWS = 10_000
CANDIDATE_DELIMITERS = ",\t;|"


@dataclass(frozen=True)
class FileFormat:
    """The format of one object, taken from its file name."""

    extension: str
    compressed: bool

    @property
    def kind(self) -> str:
        """The reader family: delimited, json, jsonl or parquet."""
        return FORMAT_BY_EXTENSION[self.extension]


def split_file_name(file_name: str) -> Optional[Tuple[str, FileFormat]]:
    """Split a file name into its stem and format.

    Returns None when the extension is not supported. The stem keeps its case.
    """
    name = file_name
    compressed = name.lower().endswith(".gz")
    if compressed:
        name = name[: -len(".gz")]
    lowered = name.lower()
    for extension in FORMAT_BY_EXTENSION:
        if lowered.endswith(extension) and len(name) > len(extension):
            return name[: -len(extension)], FileFormat(extension, compressed)
    return None


class ObjectSource:
    """What a reader needs from an object: a stream and a seekable file.

    `open` gives the decompressed bytes as a forward-only stream. `open_seekable`
    gives a seekable file, which Parquet needs. Both are context managers.
    `description` names the object in log messages.
    """

    description = "the object"

    def open(self) -> ContextManager[BinaryIO]:  # pragma: no cover
        raise NotImplementedError

    def open_seekable(self) -> ContextManager[BinaryIO]:  # pragma: no cover
        raise NotImplementedError


HeaderCallback = Optional[Callable[[List[str]], None]]


def iter_rows(
    source: ObjectSource, file_format: FileFormat, on_header: HeaderCallback = None
) -> Iterator[Dict[str, Any]]:
    """Yield the rows of one object.

    For delimited files, `on_header` receives the column names before the
    first row, so a header-only file still reports its columns.
    """
    kind = file_format.kind
    if kind == DELIMITED:
        return _read_delimited(source, file_format, on_header)
    if kind == JSON:
        return _read_json(source)
    if kind == JSONL:
        return _read_jsonl(source)
    return _read_parquet(source)


def _read_delimited(
    source: ObjectSource, file_format: FileFormat, on_header: HeaderCallback
) -> Iterator[Dict[str, Any]]:
    """Read a delimited file as UTF-8, then cp1252, then latin-1.

    The file is streamed, so a decode error can come after some rows. Each
    fallback reopens the object and skips the rows that were already yielded.
    Row boundaries are the same in every encoding, because all of them are
    ASCII compatible. Latin-1 maps every byte, so the last pass can't fail.
    """
    yielded = 0
    for encoding in DELIMITED_ENCODINGS:
        try:
            for row in _delimited_rows(source, file_format, encoding, yielded, on_header):
                yielded += 1
                yield row
            return
        except UnicodeDecodeError as err:
            LOGGER.warning(
                "%s is not valid %s (%s). Reading it with the next encoding.",
                source.description,
                encoding,
                err,
            )
    yield from _delimited_rows(
        source, file_format, LAST_RESORT_ENCODING, yielded, on_header
    )


def skip_bom(raw: BinaryIO) -> None:
    """Move past a UTF-8 byte order mark, if the stream starts with one.

    This runs on the raw bytes, so no decoder sees the mark.
    """
    if raw.peek(len(UTF8_BOM))[: len(UTF8_BOM)] == UTF8_BOM:  # type: ignore[attr-defined]
        raw.read(len(UTF8_BOM))


def _delimited_rows(
    source: ObjectSource,
    file_format: FileFormat,
    encoding: str,
    skip: int,
    on_header: HeaderCallback,
) -> Iterator[Dict[str, Any]]:
    with source.open() as raw:
        skip_bom(raw)
        text = io.TextIOWrapper(raw, encoding=encoding, newline="")
        head = text.read(SNIFF_CHARS)
        delimiter = sniff_delimiter(head, file_format.extension)
        reader = csv.reader(_lines(head, text), delimiter=delimiter)
        header = next(reader, None)
        if header is None:
            return
        columns = column_names(header)
        if on_header is not None:
            on_header(columns)
        seen = 0
        for cells in reader:
            if not cells:
                continue
            seen += 1
            if seen <= skip:
                continue
            row: Dict[str, Any] = {name: None for name in columns}
            for index, cell in enumerate(cells):
                name = columns[index] if index < len(columns) else f"column_{index + 1}"
                row[name] = cell if cell != "" else None
            yield row


def _lines(head: str, rest: io.TextIOBase) -> Iterator[str]:
    """Yield whole lines from text that was partly read into `head`.

    The csv module treats each item as a line, so a line split at the end of
    `head` must be joined to its remainder before the reader sees it.
    """
    cut = max(head.rfind("\n"), head.rfind("\r")) + 1
    yield from io.StringIO(head[:cut], newline="")
    tail = head[cut:]
    following = iter(rest)
    first = next(following, None)
    if first is None:
        if tail:
            yield tail
        return
    yield tail + first
    yield from following


def sniff_delimiter(sample: str, extension: str) -> str:
    """Guess the delimiter from the start of the file.

    The guess must appear in the header line. Otherwise the default for the
    extension is used: a tab for `.tsv` and a comma for the rest.
    """
    default = "\t" if extension == ".tsv" else ","
    lines = sample.splitlines()
    if not lines:
        return default
    header = lines[0]
    complete = lines[:-1] if len(lines) > 1 else lines
    try:
        delimiter = csv.Sniffer().sniff("\n".join(complete[:50]), CANDIDATE_DELIMITERS).delimiter
    except csv.Error:
        return default
    if delimiter in header:
        return delimiter
    return default


def column_names(header: List[str]) -> List[str]:
    """Name every column. A blank name becomes `column_N` and a repeat gets `_2`."""
    names: List[str] = []
    used = set()
    for index, cell in enumerate(header):
        base = cell if cell.strip() else f"column_{index + 1}"
        name = base
        counter = 2
        while name in used:
            name = f"{base}_{counter}"
            counter += 1
        used.add(name)
        names.append(name)
    return names


_CONTAINER_STARTS = ("start_map", "start_array")
_CONTAINER_ENDS = ("end_map", "end_array")


def _read_json(source: ObjectSource) -> Iterator[Dict[str, Any]]:
    """Stream the records of a JSON document with ijson.

    The document is an array of objects, or an object with one field that
    holds an array of objects. Other fields on the wrapper are ignored.
    """
    with source.open() as raw:
        skip_bom(raw)
        events = iter(ijson.parse(raw, use_float=True))
        _, event, _ = next(events)
        if event == "start_array":
            yield from _array_objects(events, "the array")
        elif event == "start_map":
            yield from _wrapped_objects(events)
        else:
            raise ValueError("the JSON document is not an array or an object")
        # Reading to the end makes ijson reject trailing content.
        collections.deque(events, maxlen=0)


def _build(event: str, value: Any, events: Iterator[Tuple[str, str, Any]]) -> Any:
    """Build one JSON value, starting from its first event."""
    builder = ijson.ObjectBuilder()
    builder.event(event, value)
    depth = 1 if event in _CONTAINER_STARTS else 0
    while depth:
        _, event, value = next(events)
        builder.event(event, value)
        if event in _CONTAINER_STARTS:
            depth += 1
        elif event in _CONTAINER_ENDS:
            depth -= 1
    return builder.value


def _skip(event: str, events: Iterator[Tuple[str, str, Any]]) -> None:
    """Consume one JSON value without building it."""
    depth = 1 if event in _CONTAINER_STARTS else 0
    while depth:
        _, event, _ = next(events)
        if event in _CONTAINER_STARTS:
            depth += 1
        elif event in _CONTAINER_ENDS:
            depth -= 1


def _array_objects(
    events: Iterator[Tuple[str, str, Any]], label: str, index: int = 0
) -> Iterator[Dict[str, Any]]:
    """Yield the objects of an array whose start event was already read."""
    for _, event, value in events:
        if event == "end_array":
            return
        index += 1
        if event != "start_map":
            raise ValueError(f"item {index} of {label} is not a JSON object")
        yield _build(event, value, events)


def _wrapped_objects(events: Iterator[Tuple[str, str, Any]]) -> Iterator[Dict[str, Any]]:
    """Yield the objects of the one field that holds an array of objects."""
    filled: List[str] = []
    arrays = 0
    for _, event, name in events:
        if event == "end_map":
            break
        _, event, value = next(events)
        if event != "start_array":
            _skip(event, events)
            continue
        _, event, value = next(events)
        if event == "end_array":
            arrays += 1
            continue
        if event != "start_map":
            _skip(event, events)
            for _, event, _ in events:
                if event == "end_array":
                    break
                _skip(event, events)
            continue
        arrays += 1
        filled.append(name)
        if len(filled) > 1:
            raise ValueError(f"the JSON object has more than one array of objects: {filled}")
        yield _build(event, value, events)
        yield from _array_objects(events, f"field {name!r}", index=1)
    if not filled and arrays != 1:
        raise ValueError("the JSON object has no single field that holds an array of objects")


def _read_jsonl(source: ObjectSource) -> Iterator[Dict[str, Any]]:
    with source.open() as raw:
        skip_bom(raw)
        text = io.TextIOWrapper(raw, encoding="utf-8")
        for line_number, line in enumerate(text, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as err:
                raise ValueError(f"line {line_number}: {err}") from err
            if not isinstance(value, dict):
                raise ValueError(f"line {line_number} is not a JSON object")
            yield value


def _read_parquet(source: ObjectSource) -> Iterator[Dict[str, Any]]:
    with source.open_seekable() as handle:
        parquet_file = pq.ParquetFile(handle)
        for batch in parquet_file.iter_batches(batch_size=PARQUET_BATCH_ROWS):
            yield from batch.to_pylist()


def parquet_schema(source: ObjectSource) -> "pa.Schema":
    """Read the Arrow schema from the Parquet footer, without reading rows."""
    with source.open_seekable() as handle:
        return pq.ParquetFile(handle).schema_arrow
