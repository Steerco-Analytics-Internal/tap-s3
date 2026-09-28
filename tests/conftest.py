"""Shared fixtures: a moto S3 bucket and helpers to run the tap against it."""

import contextlib
import copy
import datetime
import gzip
import io
import json
import logging
from typing import Any, Dict, List, Optional, Union

import boto3
import pytest
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID
from moto.s3.models import s3_backends

from tap_s3.tap import TapS3

BUCKET = "tap-s3-test"
BASE_TIME = datetime.datetime(2026, 9, 1, 12, 0, 0)

CONFIG = {
    "aws_access_key_id": "testing",
    "aws_secret_access_key": "testing",
    "bucket": BUCKET,
    "region": "us-east-1",
}


NOW_OFFSET = 24 * 60


def minutes(offset: int) -> datetime.datetime:
    """A naive UTC timestamp `offset` minutes after BASE_TIME."""
    return BASE_TIME + datetime.timedelta(minutes=offset)


def iso(offset: int) -> str:
    """The ISO text the tap writes for `minutes(offset)`."""
    return minutes(offset).replace(tzinfo=datetime.timezone.utc).isoformat()


def gzipped(data: Union[str, bytes]) -> bytes:
    """Gzip text or bytes."""
    raw = data.encode("utf-8") if isinstance(data, str) else data
    return gzip.compress(raw)


class Bucket:
    """Writes objects into the mocked bucket."""

    def __init__(self, client: Any, name: str) -> None:
        self.client = client
        self.name = name

    def put(
        self,
        key: str,
        data: Union[str, bytes],
        modified: Optional[datetime.datetime] = None,
    ) -> None:
        """Upload an object, and optionally set its LastModified value."""
        body = data.encode("utf-8") if isinstance(data, str) else data
        self.client.put_object(Bucket=self.name, Key=key, Body=body)
        if modified is not None:
            self.set_modified(key, modified)

    def set_modified(self, key: str, modified: datetime.datetime) -> None:
        """Change an object's LastModified value in the moto backend."""
        backend = s3_backends[DEFAULT_ACCOUNT_ID]["global"]
        backend.buckets[self.name].keys[key].last_modified = modified


@pytest.fixture
def aws(monkeypatch):
    """Mock AWS with fake credentials."""
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(name, "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    with mock_aws():
        yield


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    """Pin the tap's clock. Call the fixture with an offset to move it.

    By default, the clock reads one day after BASE_TIME, so the lookback
    window ends well after every test object.
    """

    def set_now(offset: int) -> None:
        now = minutes(offset).replace(tzinfo=datetime.timezone.utc)
        monkeypatch.setattr("tap_s3.tap.utc_now", lambda: now, raising=False)

    set_now(NOW_OFFSET)
    return set_now


def window_end(lookback: int = 60) -> str:
    """The bookmark a finished sync writes with the default clock."""
    return iso(NOW_OFFSET - lookback)


@pytest.fixture
def bucket(aws) -> Bucket:
    """An empty bucket in us-east-1."""
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=BUCKET)
    return Bucket(client, BUCKET)


def make_tap(
    catalog: Optional[dict] = None, state: Optional[dict] = None, **settings: Any
) -> TapS3:
    """Build a tap with the test config plus any overrides."""
    config = dict(CONFIG)
    config.update(settings)
    return TapS3(config=config, catalog=catalog, state=state, parse_env_config=False)


def discover(**settings: Any) -> dict:
    """Run discovery and return the catalog as a dict."""
    return make_tap(**settings).catalog_dict


def schemas(catalog: dict) -> Dict[str, dict]:
    """Map each stream name to its schema properties."""
    return {entry["stream"]: entry["schema"]["properties"] for entry in catalog["streams"]}


def select_all(catalog: dict) -> dict:
    """Mark every stream and property in a catalog as selected."""
    selected = copy.deepcopy(catalog)
    for entry in selected["streams"]:
        for item in entry["metadata"]:
            item["metadata"]["selected"] = True
    return selected


def sync(
    catalog: Optional[dict] = None, state: Optional[dict] = None, **settings: Any
) -> List[dict]:
    """Run a sync and return the Singer messages it wrote."""
    if catalog is None:
        catalog = select_all(discover(**settings))
    tap = make_tap(catalog=catalog, state=state, **settings)
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        tap.sync_all()
    return [json.loads(line) for line in output.getvalue().splitlines() if line]


def records(messages: List[dict], stream: Optional[str] = None) -> List[dict]:
    """The records in a message list, optionally for one stream."""
    return [
        message["record"]
        for message in messages
        if message["type"] == "RECORD" and (stream is None or message["stream"] == stream)
    ]


def last_state(messages: List[dict]) -> dict:
    """The value of the last STATE message."""
    states = [message["value"] for message in messages if message["type"] == "STATE"]
    return states[-1]


def bookmark(messages: List[dict], stream: str) -> Optional[str]:
    """The stream's bookmark in the last STATE message."""
    stream_state = last_state(messages).get("bookmarks", {}).get(stream, {})
    return stream_state.get("replication_key_value")


class _ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.messages: List[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


@pytest.fixture
def tap_logs():
    """Messages logged to the `tap-s3` logger.

    The SDK replaces the root logging handlers when a tap starts, which
    removes pytest's caplog handler. A handler on the tap's own logger stays.
    """
    handler = _ListHandler()
    logger = logging.getLogger("tap-s3")
    logger.addHandler(handler)
    try:
        yield handler.messages
    finally:
        logger.removeHandler(handler)
