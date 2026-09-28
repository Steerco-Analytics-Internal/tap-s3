"""The tap-s3 tap class."""

import datetime
from functools import cached_property
from typing import List, Optional

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_s3.client import S3Bucket
from tap_s3.layout import BucketLayout, build_layout, normalize_prefix
from tap_s3.streams import S3Stream, infer_schema, parse_timestamp

REQUIRED_SETTINGS = ("aws_access_key_id", "aws_secret_access_key", "bucket")


class TapS3(Tap):
    """Reads CSV, TSV, JSON, JSONL and Parquet files from an S3 bucket."""

    name = "tap-s3"

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
            th.BooleanType,
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
    def layout(self) -> BucketLayout:
        """Every stream under the prefix and its objects. Listed once per run."""
        layout = build_layout(self.bucket.list_objects(self.prefix), self.prefix)
        self.logger.info(
            "Found %d streams in %s.", len(layout.streams), self.uri(self.prefix)
        )
        return layout

    @property
    def incremental_mode(self) -> bool:
        """Whether a sync skips objects at or before the bookmark."""
        return self.config.get("incremental_mode", True) is not False

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
            S3Stream(tap=self, name=name, schema=infer_schema(self, objects))
            for name, objects in sorted(self.layout.streams.items())
        ]


if __name__ == "__main__":
    TapS3.cli()
