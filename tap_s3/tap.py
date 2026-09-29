"""The tap-s3 tap class."""

import datetime
import json
import re
from functools import cached_property
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Pattern,
    Set,
    Tuple,
)

from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from singer_sdk._singerlib import Catalog
from singer_sdk.exceptions import ConfigValidationError

from tap_s3.client import S3Bucket
from tap_s3.layout import BucketLayout, build_layout, normalize_prefix
from tap_s3.nested import (
    PARENT_ROW_COLUMN,
    ROW_KEY_COLUMN,
    Lineage,
    Path,
    child_stream_name,
)
from tap_s3.streams import (
    METADATA_PROPERTIES,
    S3ChildStream,
    S3Stream,
    infer_schemas,
    parse_timestamp,
)

PARENT_STREAM_METADATA = "tap-s3.parent-stream"
LIST_PATH_METADATA = "tap-s3.list-path"
REQUIRED_SETTINGS = ("aws_access_key_id", "aws_secret_access_key", "bucket")
DEFAULT_LOOKBACK_MINUTES = 60
# Hotglue's field-sample job sends a record limit per stream in this setting.
# Hotglue's own SDK reads it. The Meltano SDK doesn't, so the tap applies it.
RECORD_LIMITS_SETTING = "_hg_max_records_limit"


def utc_now() -> datetime.datetime:
    """The current time in UTC. Tests replace it to pin the clock."""
    return datetime.datetime.now(datetime.timezone.utc)


TRUE_WORDS = ("true", "1", "yes")
FALSE_WORDS = ("false", "0", "no")
FLAG_PATTERN = r"^\s*((?i:true|false|yes|no)|[01])?\s*$"


def parse_flag(value: Any, default: bool, name: str = "incremental_mode") -> bool:
    """Read a boolean setting that Hotglue might send as a string.

    `true`, `1` and `yes` mean true. `false`, `0` and `no` mean false. Case
    and surrounding spaces don't matter. None and an empty string mean the
    default. Any other value is a config error.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return default
    if text in TRUE_WORDS:
        return True
    if text in FALSE_WORDS:
        return False
    raise ConfigValidationError(
        f"The {name} setting must be true or false, not {value!r}. "
        "Use true, false, yes, no, 1 or 0."
    )


class TapS3(Tap):
    """Reads CSV, TSV, JSON, JSONL and Parquet files from an S3 bucket."""

    name = "tap-s3"

    listed_at: datetime.datetime

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._raw_catalog = _read_raw_catalog(kwargs.get("catalog"))
        super().__init__(*args, **kwargs)

    # The first five settings match the Hotglue S3 connector form, so the
    # connection form stays the same. Do not rename them.
    config_jsonschema = th.PropertiesList(
        th.Property(
            "aws_access_key_id",
            th.StringType,
            required=True,
            description="The access key ID of an IAM user that can read the bucket.",
        ),
        th.Property(
            "aws_secret_access_key",
            th.StringType,
            required=True,
            secret=True,
            description="The secret access key for the access key ID.",
        ),
        th.Property(
            "bucket",
            th.StringType,
            required=True,
            description="The name of the S3 bucket.",
        ),
        th.Property(
            "path_prefix",
            th.StringType,
            description=(
                "The folder to read, such as `exports/crm`. "
                "Leave it empty to read from the bucket root."
            ),
        ),
        th.Property(
            "incremental_mode",
            th.CustomType(
                {
                    "type": ["boolean", "string", "integer", "null"],
                    "pattern": FLAG_PATTERN,
                    "minimum": 0,
                    "maximum": 1,
                }
            ),
            default=True,
            description=(
                "When true, a sync reads only objects modified after the last "
                "sync. When false, every sync reads every object."
            ),
        ),
        th.Property(
            "region",
            th.StringType,
            description=(
                "The bucket's AWS region. Leave it empty to look it up with "
                "GetBucketLocation."
            ),
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Ignore objects last modified before this date and time.",
        ),
        th.Property(
            "lookback_minutes",
            th.IntegerType,
            default=DEFAULT_LOOKBACK_MINUTES,
            description=(
                "How far back each sync checks again for objects that arrived "
                "late. The bookmark stays at least this far behind the listing "
                "time."
            ),
        ),
        th.Property(
            RECORD_LIMITS_SETTING,
            th.CustomType(
                {
                    "type": ["object", "null"],
                    "additionalProperties": {"type": "integer", "minimum": 1},
                }
            ),
            description=(
                "Set by Hotglue, not by users. The most records to write for "
                "each named stream, as in a field-sample job."
            ),
        ),
        th.Property(
            "exclude_pattern",
            th.StringType,
            description=(
                "A regular expression. The tap ignores objects whose key, "
                "relative to `path_prefix`, matches it. Use it to leave out "
                "files such as manifests."
            ),
        ),
    ).to_dict()

    def _require_settings(self) -> None:
        # Discovery runs without config validation, so check the required
        # settings here to give a clear error.
        missing = [name for name in REQUIRED_SETTINGS if not self.config.get(name)]
        if missing:
            raise ValueError(f"Missing required config settings: {', '.join(missing)}")

    @cached_property
    def bucket(self) -> S3Bucket:
        """The configured bucket."""
        self._require_settings()
        return S3Bucket(
            aws_access_key_id=self.config["aws_access_key_id"],
            aws_secret_access_key=self.config["aws_secret_access_key"],
            bucket=self.config["bucket"],
            region=self.config.get("region") or None,
        )

    @cached_property
    def record_limits(self) -> Dict[str, int]:
        """The record limit for each stream named in RECORD_LIMITS_SETTING.

        Discovery skips config validation, so the tap checks the value here.
        A bad value is a config error, not a sync with no limit.
        """
        raw = self.config.get(RECORD_LIMITS_SETTING)
        if raw is None:
            return {}
        if not isinstance(raw, dict):
            raise ConfigValidationError(f"{RECORD_LIMITS_SETTING} must be an object.")
        limits: Dict[str, int] = {}
        for name, limit in raw.items():
            if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
                raise ConfigValidationError(
                    f"{RECORD_LIMITS_SETTING} for stream '{name}' must be a "
                    "whole number of at least 1."
                )
            limits[str(name)] = limit
        return limits

    @cached_property
    def prefix(self) -> str:
        """The path prefix, as a folder with a trailing slash."""
        return normalize_prefix(self.config.get("path_prefix"))

    @cached_property
    def exclude_pattern(self) -> Optional[Pattern[str]]:
        """The compiled `exclude_pattern`, or None."""
        pattern = self.config.get("exclude_pattern")
        if not pattern:
            return None
        try:
            return re.compile(pattern)
        except re.error as err:
            raise ValueError(
                f"The exclude_pattern setting is not a valid regex: {err}"
            ) from err

    @cached_property
    def layout(self) -> BucketLayout:
        """Every stream under the prefix and its objects. Listed once per run.

        `listed_at` records when the listing started. The lookback window is
        measured back from it.
        """
        exclude = self.exclude_pattern
        self.listed_at = utc_now()
        layout = build_layout(
            self.bucket.list_objects(self.prefix), self.prefix, exclude
        )
        self.logger.info(
            "Found %d streams in %s.", len(layout.streams), self.uri(self.prefix)
        )
        return layout

    @property
    def incremental_mode(self) -> bool:
        """Whether a sync skips objects it has already read."""
        return parse_flag(self.config.get("incremental_mode"), default=True)

    @property
    def lookback(self) -> datetime.timedelta:
        """The window in which late objects are still picked up."""
        value = self.config.get("lookback_minutes")
        minutes = DEFAULT_LOOKBACK_MINUTES if value is None else max(int(value), 0)
        return datetime.timedelta(minutes=minutes)

    @property
    def window_start(self) -> datetime.datetime:
        """The highest bookmark this run can write."""
        _ = self.layout  # Listing the bucket sets listed_at.
        return self.listed_at - self.lookback

    @cached_property
    def start_date(self) -> Optional[datetime.datetime]:
        """The configured start date, in UTC."""
        value = self.config.get("start_date")
        return parse_timestamp(value) if value else None

    def uri(self, key: str) -> str:
        """The s3:// address of a key in the configured bucket."""
        return f"s3://{self.config['bucket']}/{key}"

    def discover_streams(self) -> List[Stream]:
        """Build streams from the catalog during a sync, or from the bucket.

        With a catalog, the catalog's schemas are used as they are, and the
        bucket is not sampled again.
        """
        # Check the settings that config validation doesn't cover, before any
        # stream reads. Discovery skips config validation altogether.
        _ = self.incremental_mode
        _ = self.exclude_pattern
        _ = self.record_limits
        if self.input_catalog:
            return self._streams_from_catalog()
        plan = self._plan(self.layout.streams)
        roots = {
            name: S3Stream(tap=self, name=name, schema=schema)
            for name, schema in plan.roots.items()
        }
        streams: List[Stream] = list(roots.values())
        built: Dict[str, S3Stream] = dict(roots)
        for planned in plan.children:
            parent = built[planned.parent]
            root = roots[planned.root]
            child = S3ChildStream(
                self, planned.name, planned.schema, root, parent, planned.lineage
            )
            root.child_streams.append(child)
            built[planned.name] = child
            streams.append(child)
        return streams

    def _plan(self, roots: Iterable[str]) -> "_StreamPlan":
        """Name the child streams of the given file streams.

        Discovery plans every file stream. A sync plans only the file streams
        it needs to link catalog children that lost their parent metadata.
        Every file stream name in the bucket counts as taken, so the naming
        matches discovery for the planned file streams. The naming is
        deterministic, so the same bucket gives the same names and lineages.
        """
        plan = _StreamPlan()
        pending: List[Tuple[str, Lineage, dict]] = []
        for name in sorted(set(roots)):
            schemas = infer_schemas(self, name, self.layout.streams[name])
            plan.roots[name] = schemas.pop(())
            pending.extend((name, lineage, schema) for lineage, schema in schemas.items())
        taken = set(self.layout.streams)
        names: Dict[Tuple[str, Lineage], str] = {(name, ()): name for name in plan.roots}
        # Parents come before their children, so a child's name can build on
        # its parent's final name.
        for root, lineage, schema in sorted(
            pending, key=lambda item: (len(item[1]), item[0], item[1])
        ):
            parent = names[(root, lineage[:-1])]
            natural = child_stream_name(parent, lineage[-1])
            name = natural
            counter = 2
            while name in taken:
                name = f"{natural}_{counter}"
                counter += 1
            if name != natural:
                self.logger.warning(
                    "Stream name '%s' is taken by another stream. The child stream "
                    "for list %s in stream '%s' is named '%s'.",
                    natural,
                    ".".join(lineage[-1]),
                    parent,
                    name,
                )
            taken.add(name)
            names[(root, lineage)] = name
            plan.children.append(
                _PlannedChild(name, natural, root, parent, lineage, schema)
            )
        return plan

    def _streams_from_catalog(self) -> List[Stream]:
        """Build streams from the catalog, and link each child to its parent.

        A child stream's catalog entry names its parent stream and its list
        path in custom metadata at breadcrumb `[]`: PARENT_STREAM_METADATA and
        LIST_PATH_METADATA. The tap links a child by that metadata.

        - When a catalog store drops the metadata, the tap falls back to
          names. See `_link_by_name`. It never guesses from a name prefix.
        - A child that can't be linked, or whose parent is missing, is skipped
          with a warning, and its state is left alone.
        - The tap logs once per child how it linked it.
        - When the catalog leaves out a file stream that the bucket still has,
          and a child needs it, the tap adds it, deselected, to read objects.
        """
        extra = _stream_metadata(self._raw_catalog)
        roots: Dict[str, S3Stream] = {}
        specs: Dict[str, Tuple[str, Path, dict]] = {}
        how: Dict[str, str] = {}
        stripped: List[Tuple[str, dict, bool]] = []
        for entry in self.input_catalog.streams:  # type: ignore[union-attr]
            name = entry.stream or entry.tap_stream_id
            schema = entry.schema.to_dict()
            metadata = extra.get(entry.tap_stream_id, {})
            parent = metadata.get(PARENT_STREAM_METADATA)
            path = metadata.get(LIST_PATH_METADATA)
            if parent is not None:
                if (
                    isinstance(parent, str)
                    and isinstance(path, list)
                    and path
                    and all(isinstance(part, str) for part in path)
                ):
                    specs[name] = (parent, tuple(path), schema)
                    how[name] = "by its catalog metadata"
                else:
                    self.logger.warning(
                        "Stream '%s' is skipped: its %s or %s metadata is not valid.",
                        name,
                        PARENT_STREAM_METADATA,
                        LIST_PATH_METADATA,
                    )
                continue
            properties = schema.get("properties", {})
            if ROW_KEY_COLUMN in properties and PARENT_ROW_COLUMN in properties:
                selected = entry.metadata.resolve_selection().get((), True)
                stripped.append((name, schema, selected))
                continue
            roots[name] = S3Stream(tap=self, name=name, schema=schema)
        child_names = set(specs) | {name for name, _, _ in stripped}
        for name, schema, parent_name, path in self._link_by_name(stripped, child_names):
            specs[name] = (parent_name, path, schema)
            how[name] = (
                f"by its name, because its catalog entry has no "
                f"{PARENT_STREAM_METADATA} metadata"
            )
        streams: List[Stream] = list(roots.values())
        built: Dict[str, S3Stream] = dict(roots)

        def resolve(name: str, seen: Tuple[str, ...]) -> Optional[S3Stream]:
            if name in built:
                return built[name]
            if name in specs and name not in seen:
                parent_name, path, schema = specs[name]
                parent = resolve(parent_name, seen + (name,))
                if parent is None:
                    return None
                root = parent.root if isinstance(parent, S3ChildStream) else parent
                child = S3ChildStream(
                    self, name, schema, root, parent, parent.lineage + (path,)
                )
                root.child_streams.append(child)
                streams.append(child)
                built[name] = child
                self.logger.info(
                    "Stream '%s' is linked to its parent stream '%s' %s.",
                    name,
                    parent_name,
                    how[name],
                )
                return child
            if name not in specs and name in self.layout.streams:
                hidden = S3Stream(
                    tap=self,
                    name=name,
                    schema={"type": "object", "properties": dict(METADATA_PROPERTIES)},
                )
                hidden.selected = False
                streams.append(hidden)
                built[name] = hidden
                return hidden
            return None

        for name in sorted(specs):
            if resolve(name, ()) is None:
                self.logger.warning(
                    "Stream '%s' is skipped: its parent stream '%s' is missing.",
                    name,
                    specs[name][0],
                )
        return streams

    def _link_by_name(
        self, stripped: List[Tuple[str, dict, bool]], child_names: Set[str]
    ) -> Iterator[Tuple[str, dict, str, Path]]:
        """Link catalog children that lost their metadata, by exact name.

        The tap plans only the file streams whose names start the names of
        selected children, and links a child only when all of these hold:

        - Exactly one planned child has the catalog name as its natural name,
          and the plan gave that child its natural name, with no clash suffix.
        - No child name under that file stream, in the plan or in the
          catalog, carries a clash suffix. A suffix means names moved between
          lists, so a name alone can't be trusted.
        - Every column in the catalog entry is a column of the planned child.

        Every other child is skipped with a warning, and its state is left
        alone. Yields the name, schema, parent name and list path of each
        linked child.
        """
        if not stripped:
            return
        self.logger.warning(
            "%d child streams have no %s metadata in the catalog. The tap links "
            "them by name. Run discovery again and save the catalog to restore "
            "the metadata.",
            len(stripped),
            PARENT_STREAM_METADATA,
        )

        def candidate_roots(name: str) -> Set[str]:
            return {root for root in self.layout.streams if name.startswith(root + "__")}

        wanted: Set[str] = set()
        for name, _, selected in stripped:
            if selected:
                wanted |= candidate_roots(name)
        plan = self._plan(wanted)
        for name, schema, _ in stripped:
            if not candidate_roots(name) & wanted:
                self.logger.info("Stream '%s' isn't selected, so it isn't linked.", name)
                continue
            reason = _name_link_problem(name, schema, plan, child_names)
            if isinstance(reason, str):
                self.logger.warning(
                    "Stream '%s' is skipped: its catalog entry has no %s metadata, "
                    "and %s. Run discovery again.",
                    name,
                    PARENT_STREAM_METADATA,
                    reason,
                )
                continue
            yield name, schema, reason.parent, reason.lineage[-1]

    @property
    def catalog_dict(self) -> dict:
        """The catalog, with each child stream's parent and list path.

        The SDK drops metadata keys it doesn't know, so the tap adds its own
        keys at breadcrumb `[]` here.
        """
        catalog = super().catalog_dict
        extra = {
            stream.tap_stream_id: {
                PARENT_STREAM_METADATA: stream.parent.name,
                LIST_PATH_METADATA: list(stream.lineage[-1]),
            }
            for stream in self.streams.values()
            if isinstance(stream, S3ChildStream)
        }
        for entry in catalog.get("streams", []):
            added = extra.get(entry.get("tap_stream_id"))
            if not added:
                continue
            for item in entry.get("metadata", []):
                if item.get("breadcrumb") == []:
                    item.setdefault("metadata", {}).update(added)
        return catalog


class _PlannedChild(NamedTuple):
    """A child stream the bucket holds: its name, file stream and parent."""

    name: str
    natural: str
    root: str
    parent: str
    lineage: Lineage
    schema: dict


class _StreamPlan:
    """The file streams and child streams the bucket holds, with names."""

    def __init__(self) -> None:
        self.roots: Dict[str, dict] = {}
        self.children: List[_PlannedChild] = []


_CLASH_SUFFIX = re.compile(r"(.+)_(\d+)")


def _name_link_problem(
    name: str, schema: dict, plan: "_StreamPlan", child_names: Set[str]
) -> Any:
    """The planned child to link a catalog child to, or why it can't be linked."""
    known = {child.natural for child in plan.children} | child_names

    def has_suffix(candidate: str) -> bool:
        match = _CLASH_SUFFIX.fullmatch(candidate)
        return bool(match) and match.group(1) in known

    if has_suffix(name):
        return "its name carries a clash suffix"
    matches = [planned for planned in plan.children if planned.natural == name]
    if not matches:
        return "the bucket has no child stream with that name"
    if len(matches) > 1:
        return "several child streams in the bucket have that name"
    planned = matches[0]
    siblings = [child for child in plan.children if child.root == planned.root]
    if any(child.name != child.natural for child in siblings):
        return f"a child stream of '{planned.root}' in the bucket has a clash suffix"
    under_root = [child for child in child_names if child.startswith(planned.root + "__")]
    if any(has_suffix(child) for child in under_root):
        return f"a child stream of '{planned.root}' in the catalog has a clash suffix"
    columns = set(planned.schema.get("properties", {}))
    extra = set(schema.get("properties", {})) - columns
    if extra:
        return (
            "the bucket's child stream with that name lacks its columns "
            + ", ".join(sorted(extra))
        )
    return planned


def _read_raw_catalog(catalog: Any) -> Optional[dict]:
    """Keep the catalog as JSON, with metadata keys the SDK would drop."""
    if catalog is None:
        return None
    if isinstance(catalog, Catalog):
        # A Catalog object has already lost the tap's own metadata keys.
        return catalog.to_dict()
    if isinstance(catalog, dict):
        return catalog
    with open(catalog, encoding="utf-8") as handle:
        return json.load(handle)


def _stream_metadata(raw: Optional[dict]) -> Dict[str, dict]:
    """Each stream's metadata at breadcrumb `[]`, from the raw catalog."""
    found: Dict[str, dict] = {}
    for entry in (raw or {}).get("streams", []):
        stream_id = entry.get("tap_stream_id") or entry.get("stream")
        for item in entry.get("metadata", []):
            if item.get("breadcrumb") == []:
                found[stream_id] = item.get("metadata", {})
    return found


if __name__ == "__main__":
    TapS3.cli()
