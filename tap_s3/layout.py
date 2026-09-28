"""How S3 objects map to streams.

Streams come from the bucket's layout, not from fixed classes. Each key is
read relative to `path_prefix`:

- An object inside a folder belongs to the stream named after its top-level
  folder, at any depth.
- An object at the prefix root belongs to the stream named after its file
  stem, with trailing date, timestamp and numeric tokens removed.

A root file and a top-level folder that give the same name form one stream.
"""

import datetime
import logging
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Pattern, Set, Tuple

from tap_s3.formats import FileFormat, split_file_name

LOGGER = logging.getLogger("tap-s3")

MAX_LOGGED_KEYS = 100

_SEPARATOR = r"[-_. ]+"
_DATE = r"\d{4}-?\d{2}-?\d{2}"
_TIME = r"(?:[T_ -]?\d{2}(?:[:.-]?\d{2}){1,2}(?:\.\d+)?)?"
_ZONE = r"(?:Z|[+-]\d{2}:?\d{2})?"
_TRAILING_TOKEN = re.compile(
    rf"(?:{_SEPARATOR}(?:{_DATE}{_TIME}{_ZONE}|\d+)|\s*\(\d+\))$", re.IGNORECASE
)
_LETTER = re.compile(r"[^\W\d_]")
_INVALID_NAME_CHARS = re.compile(r"[^A-Za-z0-9_]+")


@dataclass(frozen=True)
class S3Object:
    """One object that belongs to a stream."""

    key: str
    last_modified: datetime.datetime
    size: int
    etag: str
    file_format: FileFormat
    stream_name: str

    @property
    def sort_key(self) -> Tuple[datetime.datetime, str]:
        """Oldest first, then by key, so equal timestamps sort the same way."""
        return self.last_modified, self.key


@dataclass
class BucketLayout:
    """The streams found under the prefix, and the keys that were skipped."""

    streams: Dict[str, List[S3Object]]
    unsupported_keys: List[str]


def normalize_prefix(path_prefix: Optional[str]) -> str:
    """Treat the prefix as a folder: no leading slash, one trailing slash."""
    prefix = (path_prefix or "").strip().lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return prefix


def sanitize_name(name: str) -> str:
    """Replace each run of characters outside `[A-Za-z0-9_]` with `_`."""
    return _INVALID_NAME_CHARS.sub("_", name).strip("_")


def strip_trailing_tokens(stem: str) -> str:
    """Remove trailing dates, timestamps and numeric suffixes from a stem.

    A token only counts when a separator comes before it, so `q3` stays. A
    stem with no letters, such as `2026-09-01`, is returned unchanged.
    """
    if not _LETTER.search(stem):
        return stem
    current = stem
    while True:
        stripped = _TRAILING_TOKEN.sub("", current, count=1)
        if stripped == current or not _LETTER.search(stripped):
            return current
        current = stripped


def is_hidden(relative_key: str) -> bool:
    """True when any path segment starts with `.` or `_`."""
    return any(segment.startswith((".", "_")) for segment in relative_key.split("/"))


def raw_stream_name(relative_key: str, stem: str) -> str:
    """The stream name before sanitizing: the top-level folder, or the stem."""
    segments = relative_key.split("/")
    if len(segments) > 1:
        return segments[0]
    return strip_trailing_tokens(stem)


def stream_name_for(relative_key: str, stem: str) -> Optional[str]:
    """Name the stream for a key relative to the prefix.

    Returns None when nothing in the name survives sanitizing.
    """
    return sanitize_name(raw_stream_name(relative_key, stem)) or None


def build_layout(
    listing: Iterable[dict],
    prefix: str,
    exclude: Optional[Pattern[str]] = None,
) -> BucketLayout:
    """Group listed objects into streams.

    `listing` holds ListObjectsV2 `Contents` entries. Folder markers,
    zero-byte objects, hidden files and keys that match `exclude` are left
    out. Objects with an unsupported extension, or a name with no usable
    characters, are collected in `unsupported_keys` and logged in one warning.
    """
    streams: Dict[str, List[S3Object]] = {}
    raw_names: Dict[str, Set[str]] = {}
    unsupported: List[str] = []
    excluded = 0
    for entry in listing:
        key = entry["Key"]
        relative = key[len(prefix) :]
        if not relative or relative.endswith("/") or entry.get("Size", 0) == 0:
            continue
        if is_hidden(relative):
            continue
        if exclude is not None and exclude.search(relative):
            excluded += 1
            continue
        split = split_file_name(relative.rsplit("/", 1)[-1])
        raw_name = raw_stream_name(relative, split[0]) if split else ""
        name = sanitize_name(raw_name)
        if split is None or not name:
            unsupported.append(key)
            continue
        raw_names.setdefault(name, set()).add(raw_name)
        streams.setdefault(name, []).append(
            S3Object(
                key=key,
                last_modified=entry["LastModified"],
                size=entry["Size"],
                etag=entry.get("ETag", ""),
                file_format=split[1],
                stream_name=name,
            )
        )
    for objects in streams.values():
        objects.sort(key=lambda obj: obj.sort_key)
    if excluded:
        LOGGER.info("Excluded %d objects that match exclude_pattern.", excluded)
    for name, raws in sorted(raw_names.items()):
        if len(raws) > 1:
            LOGGER.warning(
                "Stream '%s' merges objects from differently named sources: %s. "
                "Rename the folders or files if they hold different data.",
                name,
                ", ".join(repr(raw) for raw in sorted(raws)),
            )
    if unsupported:
        shown = unsupported[:MAX_LOGGED_KEYS]
        more = len(unsupported) - len(shown)
        LOGGER.warning(
            "Skipped %d objects with an unsupported file type or name: %s%s",
            len(unsupported),
            ", ".join(shown),
            f", and {more} more" if more else "",
        )
    return BucketLayout(streams=streams, unsupported_keys=unsupported)
