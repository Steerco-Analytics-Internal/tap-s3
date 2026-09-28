"""The stream class, and schema discovery for one stream.

Every stream uses the same class. Its name and schema come from the bucket
layout during discovery, or from the catalog during a sync.
"""

import contextlib
import datetime
import itertools
import logging
from time import monotonic
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
)

from botocore.exceptions import BotoCoreError, ClientError
from singer_sdk import Stream

from tap_s3.client import ObjectChangedError
from tap_s3.formats import DELIMITED, PARQUET, iter_rows, parquet_schema
from tap_s3.layout import S3Object
from tap_s3.nested import (
    INDEX_COLUMN,
    PARENT_ROW_COLUMN,
    ROW_KEY_COLUMN,
    arrow_streams,
    explode,
)
from tap_s3.schema import EMPTY, ColumnTypes, RecordConverter, ValueConversionError

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

CHILD_METADATA_PROPERTIES = {
    KEY_COLUMN: {"type": ["string"]},
    LAST_MODIFIED_COLUMN: {"type": ["string"], "format": "date-time"},
    PARENT_ROW_COLUMN: {"type": ["string"]},
    INDEX_COLUMN: {"type": ["integer"]},
    ROW_KEY_COLUMN: {"type": ["string"]},
}

SAMPLE_OBJECTS = 5
SAMPLE_ROWS = 1000

WINDOW_STATE_KEY = "window"
CHECKPOINT_OBJECTS = 100
CHECKPOINT_SECONDS = 30


class ObjectParseError(Exception):
    """An object could not be read as its format, or broke its catalog schema."""


@contextlib.contextmanager
def parse_errors(uri: str) -> Iterator[None]:
    """Re-raise any parse failure as ObjectParseError that names the object.

    S3 errors pass through unchanged, because they are not parse failures.
    """
    try:
        yield
    except (ObjectParseError, ObjectChangedError, BotoCoreError, ClientError):
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


def source_column_name(name: str) -> str:
    """Rename a source column that has a metadata column's name.

    The metadata columns form the primary key, so they keep their names.
    """
    return f"{name}_source" if name in METADATA_PROPERTIES else name


def rename_metadata_columns(row: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """Rename source columns that clash with metadata columns in one row."""
    clashes = [name for name in row if name in METADATA_PROPERTIES]
    if not clashes:
        return row, []
    return {source_column_name(name): value for name, value in row.items()}, clashes


def warn_renamed(logger: logging.Logger, stream: str, clashes: Iterable[str]) -> None:
    """Log the source columns that were renamed for one stream."""
    logger.warning(
        "Stream '%s' has source columns named like metadata columns. They are "
        "renamed: %s.",
        stream,
        ", ".join(f"{name} to {source_column_name(name)}" for name in sorted(clashes)),
    )


class _StreamSample:
    """Columns and container names found for one stream during discovery."""

    def __init__(self, rename: Any) -> None:
        self.columns = ColumnTypes(rename)
        self.containers: Set[str] = set()

    def merge(self, other: "_StreamSample") -> None:
        self.columns.merge(other.columns)
        self.containers |= other.containers


def warn_renamed_nested(logger: logging.Logger, stream: str, notes: Iterable[str]) -> None:
    """Log flattened columns that were renamed to avoid a clash."""
    logger.warning(
        "Stream '%s' has nested columns whose names clash. They are renamed: %s.",
        stream,
        ", ".join(sorted(notes)),
    )


def infer_schemas(tap: "TapS3", name: str, objects: List[S3Object]) -> Dict[str, dict]:
    """Build the schemas of a stream and its child streams.

    Samples the newest SAMPLE_OBJECTS objects that parse, and up to
    SAMPLE_ROWS rows from each. An object that can't be parsed is logged and
    skipped, so one bad file doesn't stop discovery. Parquet columns come from
    the file's own schema. Nested JSON and Parquet data is taken apart by the
    rules in `tap_s3.nested`, and child schemas are unions across rows and
    files. The stream itself comes first in the result.
    """
    clashes: Set[str] = set()
    notes: Set[str] = set()

    def rename(column: str) -> str:
        if column in METADATA_PROPERTIES:
            clashes.add(column)
        return source_column_name(column)

    samples: Dict[str, _StreamSample] = {name: _StreamSample(rename)}
    newest_first = sorted(objects, key=lambda obj: obj.sort_key, reverse=True)
    sampled = 0
    for obj in newest_first:
        if sampled == SAMPLE_OBJECTS:
            break
        attempt: Dict[str, _StreamSample] = {}
        try:
            _sample_object(tap, name, obj, attempt, rename, notes)
        except (ObjectParseError, ObjectChangedError) as err:
            LOGGER.warning("Discovery skipped %s: %s", tap.uri(obj.key), err)
            continue
        for stream, sample in attempt.items():
            samples.setdefault(stream, _StreamSample(str)).merge(sample)
        sampled += 1
    if clashes:
        warn_renamed(LOGGER, name, clashes)
    if notes:
        warn_renamed_nested(LOGGER, name, notes)
    schemas: Dict[str, dict] = {}
    for stream in [name] + sorted(set(samples) - {name}):
        sample = samples[stream]
        properties = {
            column: prop
            for column, prop in sample.columns.properties().items()
            if not (column in sample.containers and sample.columns.types[column] == EMPTY)
        }
        properties.update(METADATA_PROPERTIES if stream == name else CHILD_METADATA_PROPERTIES)
        schemas[stream] = {"type": "object", "properties": properties}
    return schemas


def _sample_object(
    tap: "TapS3",
    name: str,
    obj: S3Object,
    samples: Dict[str, _StreamSample],
    rename: Any,
    notes: Set[str],
) -> None:
    source = tap.bucket.source(
        obj.key, obj.size, obj.file_format.compressed, obj.etag
    )

    def sample_for(stream: str) -> _StreamSample:
        if stream not in samples:
            samples[stream] = _StreamSample(rename if stream == name else str)
        return samples[stream]

    with parse_errors(tap.uri(obj.key)):
        if obj.file_format.kind == PARQUET:
            streams, containers, found_notes = arrow_streams(
                parquet_schema(source), name, lambda column: column
            )
            notes.update(found_notes)
            for stream, columns in streams.items():
                sample = sample_for(stream)
                for column, column_type in columns:
                    sample.columns.observe(column, column_type)
                sample.containers |= containers.get(stream, set())
            return
        root = sample_for(name)
        rows = iter_rows(source, obj.file_format, root.columns.observe_columns)
        with contextlib.closing(rows):  # type: ignore[type-var]
            for row in itertools.islice(rows, SAMPLE_ROWS):
                if obj.file_format.kind == DELIMITED:
                    root.columns.observe_text_row(row)
                    continue
                for piece in explode(row, name, "", {}):
                    sample = sample_for(piece.stream)
                    notes.update(piece.split.notes)
                    sample.containers |= piece.split.containers
                    sample.columns.observe_json_row(
                        {
                            column: value
                            for column, value in piece.row.items()
                            if piece.stream == name
                            or column not in CHILD_METADATA_PROPERTIES
                        }
                    )


class S3Stream(Stream):
    """Rows from every object that belongs to one stream."""

    primary_keys = [KEY_COLUMN, ROW_NUMBER_COLUMN]
    replication_key = LAST_MODIFIED_COLUMN
    is_sorted = True

    def __init__(self, tap: "TapS3", name: str, schema: dict) -> None:
        super().__init__(tap=tap, name=name, schema=schema)
        self._reported_dropped = False
        self._reported_renamed = False
        self._reported_nested = False

    @property
    def s3_tap(self) -> "TapS3":
        """The tap, typed as TapS3."""
        return self._tap  # type: ignore[return-value]

    @property
    def is_incremental(self) -> bool:
        """True unless the config or the catalog asks for a full read."""
        return self.s3_tap.incremental_mode and self.replication_method != "FULL_TABLE"

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Read each object once, and route its rows to every selected stream.

        This stream yields its own rows to the SDK. Rows for child streams go
        straight to the child stream, so no object is read twice.

        S3 LastModified has one-second resolution, and a multipart upload
        keeps the time it started. So an object can appear after a later
        bookmark was written. To catch it, the bookmark never passes the
        listing time minus `lookback_minutes`. Objects read inside that
        window are kept in each stream's state as a map of key to ETag, and
        are skipped next time.

        An object counts as done for a stream only after all of its rows for
        every selected stream were emitted. The bookmark moves after each
        object. When several objects share a LastModified value, it moves
        after the last of them, so a failure between them can't skip one on
        the next run.
        """
        tap = self.s3_tap
        window_start = tap.window_start
        listed = tap.layout.streams.get(self.name, [])
        group: List[S3Stream] = [self, *self.child_streams]  # type: ignore[list-item]
        active = [stream for stream in group if stream.selected]
        progress = {
            stream.name: _Progress(stream.get_context_state(None), listed, window_start)
            for stream in active
        }
        children = {
            stream.name: stream for stream in active if isinstance(stream, S3ChildStream)
        }
        for child in children.values():
            child.begin()
        objects = self._objects_to_read(listed)
        converter = RecordConverter(self.schema["properties"])
        since_checkpoint = 0
        last_checkpoint = monotonic()
        streams = {stream.name: stream for stream in active}
        try:
            for index, obj in enumerate(objects):
                targets = {
                    name
                    for name, stream_progress in progress.items()
                    if not (
                        streams[name].is_incremental and stream_progress.already_read(obj)
                    )
                }
                if not targets:
                    continue
                yield from self._object_records(obj, converter, targets, children)
                if children and not self.selected and all(
                    child.limit_reached for child in children.values()
                ):
                    # Every selected stream has its rows. Stop reading.
                    return
                following = objects[index + 1] if index + 1 < len(objects) else None
                shares_time = (
                    following is not None
                    and following.last_modified == obj.last_modified
                )
                bookmark = None if shares_time else min(obj.last_modified, window_start)
                for name in targets:
                    progress[name].finish(obj, bookmark)
                since_checkpoint += 1
                # The interval grows with the window, so the bytes written stay
                # linear in the number of objects.
                window_size = max(len(item.window) for item in progress.values())
                interval = max(CHECKPOINT_OBJECTS, window_size // 10)
                if (
                    since_checkpoint >= interval
                    or monotonic() - last_checkpoint >= CHECKPOINT_SECONDS
                ):
                    self._checkpoint(progress.values())
                    since_checkpoint = 0
                    last_checkpoint = monotonic()
            if not objects:
                self.logger.info("No objects for stream '%s'.", self.name)
            # Every listed object up to the window start is now read or skipped.
            for item in progress.values():
                item.raise_bookmark(window_start)
        except Exception:
            # Keep the progress of the objects that finished before the error.
            self._checkpoint(progress.values())
            raise
        self._checkpoint(progress.values())

    def _objects_to_read(self, listed: List[S3Object]) -> List[S3Object]:
        objects = sorted(listed, key=lambda obj: obj.sort_key)
        start_date = self.s3_tap.start_date
        if start_date is not None:
            objects = [obj for obj in objects if obj.last_modified >= start_date]
        return objects

    def _checkpoint(self, progress: Iterable["_Progress"]) -> None:
        """Prune each window into state, then write a STATE message."""
        for item in progress:
            item.prune()
        # The SDK only flushes state after it writes a record. Mark it dirty so
        # an object with no rows still moves the bookmark.
        self._is_state_flushed = False
        self._write_state_message()

    def _object_records(
        self,
        obj: S3Object,
        converter: RecordConverter,
        targets: Set[str],
        children: Dict[str, "S3ChildStream"],
    ) -> Iterator[Dict[str, Any]]:
        tap = self.s3_tap
        uri = tap.uri(obj.key)
        self.logger.info("Reading %s", uri)
        source = tap.bucket.source(
            obj.key, obj.size, obj.file_format.compressed, obj.etag
        )
        last_modified = obj.last_modified.isoformat()
        base = {KEY_COLUMN: obj.key, LAST_MODIFIED_COLUMN: last_modified}
        emit_own = self.name in targets
        with parse_errors(uri):
            rows = iter_rows(source, obj.file_format)
            with contextlib.closing(rows):  # type: ignore[type-var]
                for row_number, raw in enumerate(rows, 1):
                    if obj.file_format.kind == DELIMITED:
                        own, pieces = raw, []
                    else:
                        found = explode(raw, self.name, f"{obj.key}#{row_number}", base)
                        first = next(found)
                        self._report_nested(first.split.notes)
                        own, pieces = first.row, found
                    if emit_own:
                        yield self._own_record(
                            own, converter, uri, row_number, obj.key, last_modified
                        )
                    for piece in pieces:
                        child = children.get(piece.stream)
                        if child is not None and child.name in targets:
                            child.emit(piece.row, uri, piece.split.notes)

    def _own_record(
        self,
        row: Dict[str, Any],
        converter: RecordConverter,
        uri: str,
        row_number: int,
        key: str,
        last_modified: str,
    ) -> Dict[str, Any]:
        row, clashes = rename_metadata_columns(row)
        if clashes and not self._reported_renamed:
            self._reported_renamed = True
            warn_renamed(self.logger, self.name, clashes)
        try:
            converted = converter.convert(row)
        except ValueConversionError as err:
            raise ObjectParseError(
                f"Could not parse {uri} row {row_number}: {err}. "
                "The value doesn't match the column's type in the "
                "catalog. To sync this object, fix the file or change "
                "the column's type in the catalog."
            ) from err
        self._report_dropped(converted.dropped, uri)
        record = converted.record
        record[KEY_COLUMN] = key
        record[LAST_MODIFIED_COLUMN] = last_modified
        record[ROW_NUMBER_COLUMN] = row_number
        return record

    def _report_nested(self, notes: List[str]) -> None:
        if notes and not self._reported_nested:
            self._reported_nested = True
            warn_renamed_nested(self.logger, self.name, notes)

    def _sync_children(self, child_context: Optional[dict]) -> None:
        """Do nothing. get_records routes child rows itself, once per object."""

    def get_child_context(self, record: dict, context: Optional[dict]) -> Optional[dict]:
        """Child streams take no context. Their rows come from get_records."""
        return None

    def _report_dropped(self, dropped: List[str], uri: str) -> None:
        if dropped and not self._reported_dropped:
            self._reported_dropped = True
            self.logger.warning(
                "Stream '%s' drops columns that are not in the catalog schema: %s. "
                "First seen in %s. This warning shows once per stream.",
                self.name,
                ", ".join(dropped),
                uri,
            )

    def _increment_stream_state(
        self, latest_record: Dict[str, Any], *, context: Optional[dict] = None
    ) -> None:
        """Do nothing. The bookmark moves per object in get_records.

        The SDK moves it per record by default, which would bookmark an object
        before all of its rows were emitted.
        """


class S3ChildStream(S3Stream):
    """Rows from the lists of objects inside another stream's records.

    A child stream never reads objects itself. Its root stream reads each
    object once and hands the child its rows through `emit`.
    """

    primary_keys = [KEY_COLUMN, ROW_KEY_COLUMN]

    def __init__(self, tap: "TapS3", name: str, schema: dict, root: S3Stream) -> None:
        super().__init__(tap=tap, name=name, schema=schema)
        # The SDK skips a stream with a parent type in sync_all and in dry
        # runs. It is set on the instance, not the class, because the SDK links
        # parents and children by class, and every stream here shares one.
        self.parent_stream_type = S3Stream
        self.root = root
        self._converter: Optional[RecordConverter] = None
        self._emitted = 0

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Yield nothing. The root stream routes this stream's rows."""
        return iter(())

    def begin(self) -> None:
        """Start a sync: write the SCHEMA message and reset the row count."""
        self._write_schema_message()
        self._converter = RecordConverter(self.schema["properties"])
        self._emitted = 0

    @property
    def limit_reached(self) -> bool:
        """True once the SDK's record limit for this stream is reached."""
        limit = self.ABORT_AT_RECORD_COUNT
        return limit is not None and self._emitted >= limit

    def emit(self, row: Dict[str, Any], uri: str, notes: List[str]) -> None:
        """Convert one row and write it as a RECORD message."""
        if self.limit_reached:
            return
        self._report_nested(notes)
        assert self._converter is not None, "begin() runs before emit()"
        try:
            converted = self._converter.convert(row)
        except ValueConversionError as err:
            raise ObjectParseError(
                f"Could not parse {uri}, stream '{self.name}' row "
                f"{row.get(ROW_KEY_COLUMN)}: {err}. The value doesn't match the "
                "column's type in the catalog. To sync this object, fix the file "
                "or change the column's type in the catalog."
            ) from err
        self._report_dropped(converted.dropped, uri)
        self._write_record_message(converted.record)
        self._emitted += 1


class _Progress:
    """A stream's bookmark and window, kept in its state dict.

    Every update is O(1) and changes the state dict in place, so a STATE
    message written by the SDK at any time holds only finished objects.
    Pruning walks the window, so it runs only at checkpoints.
    """

    def __init__(
        self,
        state: dict,
        listed: List[S3Object],
        window_start: datetime.datetime,
    ) -> None:
        self.state = state
        self.last_modified = {obj.key: obj.last_modified for obj in listed}
        self.bookmark = self._read_bookmark(state)
        window = state.get(WINDOW_STATE_KEY)
        if not isinstance(window, dict):
            # State written before the window existed: its bookmark may be
            # past objects that arrived late. Lower it once to the window
            # start, so this run reads them.
            if self.bookmark is not None and self.bookmark > window_start:
                self.bookmark = window_start
                self._store_bookmark(window_start)
            window = {}
        self.window: Dict[str, str] = window
        state[WINDOW_STATE_KEY] = window

    @staticmethod
    def _read_bookmark(state: dict) -> Optional[datetime.datetime]:
        if state.get("replication_key") not in (None, LAST_MODIFIED_COLUMN):
            return None
        value = state.get("replication_key_value")
        return parse_timestamp(value) if value else None

    def _store_bookmark(self, bookmark: datetime.datetime) -> None:
        self.state["replication_key"] = LAST_MODIFIED_COLUMN
        self.state["replication_key_value"] = bookmark.isoformat()

    def already_read(self, obj: S3Object) -> bool:
        """True when the object is at or before the bookmark, or in the window."""
        if self.bookmark is not None and obj.last_modified <= self.bookmark:
            return True
        return self.window.get(obj.key) == obj.etag

    def raise_bookmark(self, bookmark: datetime.datetime) -> None:
        """Move the bookmark forward. It never moves back."""
        if self.bookmark is None or bookmark > self.bookmark:
            self.bookmark = bookmark
            self._store_bookmark(bookmark)

    def finish(self, obj: S3Object, bookmark: Optional[datetime.datetime]) -> None:
        """Record a finished object, and move the bookmark if one is given."""
        self.window[obj.key] = obj.etag
        if bookmark is not None:
            self.raise_bookmark(bookmark)

    def prune(self) -> None:
        """Drop window entries the bookmark filter already covers.

        Pruning uses the LastModified values from this run's listing. An entry
        for a key that is no longer listed is dropped too.
        """
        if self.bookmark is None:
            return
        stale = [
            key
            for key in self.window
            if key not in self.last_modified or self.last_modified[key] <= self.bookmark
        ]
        for key in stale:
            del self.window[key]
