"""The stream class, and schema discovery for one stream.

Every stream uses the same class. Its name and schema come from the bucket
layout during discovery, or from the catalog during a sync.
"""

import contextlib
import datetime
import itertools
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, List, Optional

from botocore.exceptions import BotoCoreError, ClientError
from singer_sdk import Stream

from tap_s3.formats import DELIMITED, PARQUET, iter_rows, parquet_schema
from tap_s3.layout import S3Object
from tap_s3.schema import ColumnTypes, RecordConverter, ValueConversionError

if TYPE_CHECKING:
    from tap_s3.tap import TapS3

LOGGER = logging.getLogger("tap-s3")

KEY_COLUMN = "_s3_key"
LAST_MODIFIED_COLUMN = "_s3_last_modified"
ROW_NUMBER_COLUMN = "_row_number"
METADATA_PROPERTIES = {
    KEY_COLUMN: {"type": ["string"]},
    LAST_MODIFIED_COLUMN: {"type": ["string"], "format": "date-time"},
    ROW_NUMBER_COLUMN: {"type": ["integer"]},
}

SAMPLE_OBJECTS = 5
SAMPLE_ROWS = 1000


class ObjectParseError(Exception):
    """An object could not be read as its format, or broke its catalog schema."""


@contextlib.contextmanager
def parse_errors(uri: str) -> Iterator[None]:
    """Re-raise any parse failure as ObjectParseError that names the object.

    S3 errors pass through unchanged, because they are not parse failures.
    """
    try:
        yield
    except (ObjectParseError, BotoCoreError, ClientError):
        raise
    except Exception as err:
        raise ObjectParseError(
            f"Could not parse {uri}: {type(err).__name__}: {err}"
        ) from err


def to_utc(value: datetime.datetime) -> datetime.datetime:
    """Give a naive datetime the UTC zone. Leave an aware one alone."""
    if value.tzinfo is None:
        return value.replace(tzinfo=datetime.timezone.utc)
    return value


def parse_timestamp(text: str) -> datetime.datetime:
    """Parse a bookmark or start date. A naive value is UTC."""
    cleaned = text.strip()
    if cleaned.endswith(("Z", "z")):
        cleaned = cleaned[:-1] + "+00:00"
    return to_utc(datetime.datetime.fromisoformat(cleaned))


def infer_schema(tap: "TapS3", objects: List[S3Object]) -> dict:
    """Build a stream's schema from its most recent objects.

    Samples the newest SAMPLE_OBJECTS objects and up to SAMPLE_ROWS rows from
    each. Parquet columns come from the file's own schema.
    """
    columns = ColumnTypes()
    newest_first = sorted(objects, key=lambda obj: obj.sort_key, reverse=True)
    for obj in newest_first[:SAMPLE_OBJECTS]:
        source = tap.bucket.source(obj.key, obj.size, obj.file_format.compressed)
        with parse_errors(tap.uri(obj.key)):
            if obj.file_format.kind == PARQUET:
                columns.observe_arrow_schema(parquet_schema(source))
                continue
            rows = iter_rows(source, obj.file_format, columns.observe_columns)
            observe = (
                columns.observe_json_row
                if obj.file_format.kind != DELIMITED
                else columns.observe_text_row
            )
            with contextlib.closing(rows):  # type: ignore[type-var]
                for row in itertools.islice(rows, SAMPLE_ROWS):
                    observe(row)
    properties = columns.properties()
    properties.update(METADATA_PROPERTIES)
    return {"type": "object", "properties": properties}


class S3Stream(Stream):
    """Rows from every object that belongs to one stream."""

    primary_keys = [KEY_COLUMN, ROW_NUMBER_COLUMN]
    replication_key = LAST_MODIFIED_COLUMN
    is_sorted = True

    def __init__(self, tap: "TapS3", name: str, schema: dict) -> None:
        super().__init__(tap=tap, name=name, schema=schema)
        self._reported_dropped = False

    @property
    def s3_tap(self) -> "TapS3":
        """The tap, typed as TapS3."""
        return self._tap  # type: ignore[return-value]

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Yield rows object by object, oldest first.

        The bookmark moves after each object's last row. When several objects
        share a LastModified value, it moves after the last of them, so a
        failure between them cannot skip one on the next run.
        """
        objects = self._objects_to_read(context)
        converter = RecordConverter(self.schema["properties"])
        for index, obj in enumerate(objects):
            yield from self._object_records(obj, converter)
            following = objects[index + 1] if index + 1 < len(objects) else None
            if following is None or following.last_modified > obj.last_modified:
                self._advance_bookmark(context, obj.last_modified)
        if not objects:
            self.logger.info("No new objects for stream '%s'.", self.name)

    def _objects_to_read(self, context: Optional[dict]) -> List[S3Object]:
        tap = self.s3_tap
        objects = sorted(
            tap.layout.streams.get(self.name, []), key=lambda obj: obj.sort_key
        )
        start_date = tap.start_date
        if start_date is not None:
            objects = [obj for obj in objects if obj.last_modified >= start_date]
        bookmark = self._bookmark(context) if tap.incremental_mode else None
        if bookmark is not None:
            objects = [obj for obj in objects if obj.last_modified > bookmark]
        return objects

    def _object_records(
        self, obj: S3Object, converter: RecordConverter
    ) -> Iterator[Dict[str, Any]]:
        tap = self.s3_tap
        uri = tap.uri(obj.key)
        self.logger.info("Reading %s", uri)
        source = tap.bucket.source(obj.key, obj.size, obj.file_format.compressed)
        last_modified = obj.last_modified.isoformat()
        with parse_errors(uri):
            rows = iter_rows(source, obj.file_format)
            with contextlib.closing(rows):  # type: ignore[type-var]
                for row_number, row in enumerate(rows, 1):
                    try:
                        converted = converter.convert(row)
                    except ValueConversionError as err:
                        raise ObjectParseError(
                            f"Could not parse {uri} row {row_number}: {err}. "
                            "The catalog schema expects another type. Fix the "
                            "file, or run discovery again and refresh the catalog."
                        ) from err
                    self._report_dropped(converted.dropped, uri)
                    record = converted.record
                    record[KEY_COLUMN] = obj.key
                    record[LAST_MODIFIED_COLUMN] = last_modified
                    record[ROW_NUMBER_COLUMN] = row_number
                    yield record

    def _report_dropped(self, dropped: List[str], uri: str) -> None:
        columns = [name for name in dropped if name not in METADATA_PROPERTIES]
        if columns and not self._reported_dropped:
            self._reported_dropped = True
            self.logger.warning(
                "Stream '%s' drops columns that are not in the catalog schema: %s. "
                "First seen in %s. This warning shows once per stream.",
                self.name,
                ", ".join(columns),
                uri,
            )

    def _bookmark(self, context: Optional[dict]) -> Optional[datetime.datetime]:
        state = self.get_context_state(context)
        if state.get("replication_key") not in (None, LAST_MODIFIED_COLUMN):
            return None
        value = state.get("replication_key_value")
        return parse_timestamp(value) if value else None

    def _advance_bookmark(
        self, context: Optional[dict], last_modified: datetime.datetime
    ) -> None:
        state = self.get_context_state(context)
        state["replication_key"] = LAST_MODIFIED_COLUMN
        state["replication_key_value"] = last_modified.isoformat()
        # The SDK only flushes state after it writes a record. Mark it dirty so
        # an object with no rows still moves the bookmark.
        self._is_state_flushed = False
        self._write_state_message()

    def _increment_stream_state(
        self, latest_record: Dict[str, Any], *, context: Optional[dict] = None
    ) -> None:
        """Do nothing. The bookmark moves per object in get_records.

        The SDK moves it per record by default, which would bookmark an object
        before all of its rows were emitted.
        """
