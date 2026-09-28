"""Readers for the file formats the tap supports.

Each reader yields one dict per row. Delimited readers yield string values, or
None for an empty cell. JSON readers yield decoded JSON values. The Parquet
reader yields the Python values that PyArrow produces.
"""

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
    """

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
    """Read a delimited file as UTF-8, and fall back to cp1252.

    The file is streamed, so a decode error can come after some rows. The
    fallback reopens the object and skips the rows that were already yielded.
    Row boundaries are the same in both encodings, because both are ASCII
    compatible.
    """
    yielded = 0
    try:
        for row in _delimited_rows(source, file_format, "utf-8-sig", 0, on_header):
            yielded += 1
            yield row
        return
    except UnicodeDecodeError as err:
        LOGGER.warning("The file is not valid UTF-8 (%s). Reading it as cp1252.", err)
    yield from _delimited_rows(source, file_format, "cp1252", yielded, on_header)


def _delimited_rows(
    source: ObjectSource,
    file_format: FileFormat,
    encoding: str,
    skip: int,
    on_header: HeaderCallback,
) -> Iterator[Dict[str, Any]]:
    with source.open() as raw:
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


def _read_json(source: ObjectSource) -> Iterator[Dict[str, Any]]:
    with source.open() as raw:
        document = json.load(io.TextIOWrapper(raw, encoding="utf-8-sig"))
    for index, item in enumerate(json_records(document), 1):
        if not isinstance(item, dict):
            raise ValueError(f"item {index} of the array is not a JSON object")
        yield item


def json_records(document: Any) -> List[Any]:
    """Find the array of records in a JSON document.

    The document is either an array, or an object with one field that holds
    an array of objects. Other scalar fields on the wrapper are ignored.
    """
    if isinstance(document, list):
        return document
    if not isinstance(document, dict):
        raise ValueError("the JSON document is not an array or an object")
    arrays = [
        name
        for name, value in document.items()
        if isinstance(value, list) and all(isinstance(item, dict) for item in value)
    ]
    filled = [name for name in arrays if document[name]]
    if len(filled) == 1:
        return document[filled[0]]
    if len(filled) > 1:
        raise ValueError(f"the JSON object has more than one array of objects: {filled}")
    if len(arrays) == 1:
        return []
    raise ValueError("the JSON object has no single field that holds an array of objects")


def _read_jsonl(source: ObjectSource) -> Iterator[Dict[str, Any]]:
    with source.open() as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig")
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
