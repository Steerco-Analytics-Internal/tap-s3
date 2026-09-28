"""S3 access: credentials, region, listing and reading objects."""

import contextlib
import gzip
import io
import logging
import shutil
import tempfile
from typing import Any, BinaryIO, Iterator, Optional

import boto3
from botocore.exceptions import ClientError

from tap_s3.formats import ObjectSource

LOGGER = logging.getLogger("tap-s3")

STREAM_BUFFER_BYTES = 1024 * 1024
RANGE_BUFFER_BYTES = 8 * 1024 * 1024
LIST_PAGE_SIZE = 1000


class RegionLookupError(Exception):
    """The bucket's region could not be found."""


class ObjectChangedError(Exception):
    """An object changed after it was listed, so its reads no longer agree."""


def get_pinned(client: Any, bucket: str, key: str, etag: str, **options: Any) -> dict:
    """GetObject, pinned to the ETag from the listing with IfMatch.

    Every open and ranged read of an object goes through here, so all of them
    see the same version. A changed object fails with ObjectChangedError.
    """
    if etag:
        options["IfMatch"] = etag
    try:
        return client.get_object(Bucket=bucket, Key=key, **options)
    except ClientError as err:
        status = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        code = err.response.get("Error", {}).get("Code")
        if status == 412 or code == "PreconditionFailed":
            raise ObjectChangedError(
                f"s3://{bucket}/{key} changed during the sync. The tap stopped "
                "reading it, and it will be read next run."
            ) from err
        raise


def normalize_location(location: Optional[str]) -> str:
    """Turn a GetBucketLocation answer into a region name.

    S3 returns no value for us-east-1 and the legacy value `EU` for eu-west-1.
    """
    if not location:
        return "us-east-1"
    if location == "EU":
        return "eu-west-1"
    return location


def resolve_region(session: "boto3.Session", bucket: str) -> str:
    """Find the bucket's region with GetBucketLocation.

    When that call is denied, S3 often still names the region in the
    `x-amz-bucket-region` header of the error, so that header is used next.
    """
    client = session.client("s3", region_name="us-east-1")
    try:
        response = client.get_bucket_location(Bucket=bucket)
    except ClientError as err:
        headers = err.response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
        region = headers.get("x-amz-bucket-region")
        if region:
            return str(region)
        raise RegionLookupError(
            f"Could not find the region of bucket {bucket!r}: {err}. "
            "Set `region` in the config."
        ) from err
    return normalize_location(response.get("LocationConstraint"))


class S3Bucket:
    """A bucket, with the client for its region."""

    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        bucket: str,
        region: Optional[str] = None,
    ) -> None:
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        self.bucket = bucket
        self.region = region or resolve_region(session, bucket)
        self.client = session.client("s3", region_name=self.region)

    def list_objects(self, prefix: str) -> Iterator[dict]:
        """Yield every object under the prefix, across all result pages."""
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=self.bucket,
            Prefix=prefix,
            PaginationConfig={"PageSize": LIST_PAGE_SIZE},
        )
        for page in pages:
            yield from page.get("Contents", [])

    def source(
        self, key: str, size: int, compressed: bool, etag: str = ""
    ) -> "S3ObjectSource":
        """A reader source for one object, pinned to its listed ETag."""
        return S3ObjectSource(self.client, self.bucket, key, size, compressed, etag)


class _StreamingBodyReader(io.RawIOBase):
    """Adapts a botocore StreamingBody to the raw IO interface."""

    def __init__(self, body: Any) -> None:
        self._body = body

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        data = self._body.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)


class _RangeReader(io.RawIOBase):
    """A seekable file over an object, read with ranged GET requests."""

    def __init__(self, source: "S3ObjectSource") -> None:
        self._source = source
        self._size = source.size
        self._position = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._position

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self._position = offset
        elif whence == io.SEEK_CUR:
            self._position += offset
        else:
            self._position = self._size + offset
        return self._position

    def readinto(self, buffer: Any) -> int:
        if self._position >= self._size or len(buffer) == 0:
            return 0
        end = min(self._position + len(buffer), self._size) - 1
        source = self._source
        response = get_pinned(
            source.client,
            source.bucket,
            source.key,
            source.etag,
            Range=f"bytes={self._position}-{end}",
        )
        data = response["Body"].read()
        buffer[: len(data)] = data
        self._position += len(data)
        return len(data)


class S3ObjectSource(ObjectSource):
    """Opens one S3 object for the format readers."""

    def __init__(
        self,
        client: Any,
        bucket: str,
        key: str,
        size: int,
        compressed: bool,
        etag: str = "",
    ):
        self.client = client
        self.bucket = bucket
        self.key = key
        self.size = size
        self.compressed = compressed
        self.etag = etag
        self.description = f"s3://{bucket}/{key}"

    @contextlib.contextmanager
    def open(self) -> Iterator[BinaryIO]:
        """Stream the object's bytes, decompressed when it ends in `.gz`."""
        body = get_pinned(self.client, self.bucket, self.key, self.etag)["Body"]
        try:
            raw = io.BufferedReader(
                _StreamingBodyReader(body), buffer_size=STREAM_BUFFER_BYTES
            )
            if self.compressed:
                yield gzip.GzipFile(fileobj=raw)  # type: ignore[misc]
            else:
                yield raw  # type: ignore[misc]
        finally:
            body.close()

    @contextlib.contextmanager
    def open_seekable(self) -> Iterator[BinaryIO]:
        """A seekable file for Parquet.

        A plain object is read with ranged requests. A gzipped object cannot
        seek, so it is decompressed into a temporary file first.
        """
        if not self.compressed:
            yield io.BufferedReader(  # type: ignore[misc]
                _RangeReader(self),
                buffer_size=RANGE_BUFFER_BYTES,
            )
            return
        with self.open() as stream, tempfile.TemporaryFile() as spool:
            shutil.copyfileobj(stream, spool)
            spool.seek(0)
            yield spool  # type: ignore[misc]
