"""The tap-s3 tap class."""

import datetime
import re
from functools import cached_property
from typing import Any, List, Optional, Pattern

from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from singer_sdk.exceptions import ConfigValidationError

from tap_s3.client import S3Bucket
from tap_s3.layout import BucketLayout, build_layout, normalize_prefix
from tap_s3.streams import S3Stream, infer_schema, parse_timestamp

REQUIRED_SETTINGS = ("aws_access_key_id", "aws_secret_access_key", "bucket")
DEFAULT_LOOKBACK_MINUTES = 60


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
                {"type": ["boolean", "string", "null"], "pattern": FLAG_PATTERN}
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
        if self.input_catalog:
            return [
                S3Stream(
                    tap=self,
                    name=entry.stream or entry.tap_stream_id,
                    schema=entry.schema.to_dict(),
                )
                for entry in self.input_catalog.streams
            ]
        return [
            S3Stream(tap=self, name=name, schema=infer_schema(self, name, objects))
            for name, objects in sorted(self.layout.streams.items())
        ]


if __name__ == "__main__":
    TapS3.cli()
