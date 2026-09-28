"""The flat-file output of v1.0.0, kept as a fixture.

Files with no nested objects or lists must produce the same catalog and
records as v1.0.0. `flat_output` builds a fixed bucket and runs discovery and
a sync. To record the fixture again from a v1.0.0 checkout, run:

    python -m tests.golden
"""

import datetime
import decimal
import io
import json
import os
import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "v1_0_0_flat.json"

FLAT_ROWS = [
    {"id": 1, "name": "Ada", "score": 1.5, "active": True, "joined": "2026-09-01",
     "note": None, "code": "007"},
    {"id": 2, "name": "Grace", "score": 2, "active": False,
     "joined": "2026-09-02T10:00:00Z", "note": "", "code": "42"},
    {"id": 3, "name": "Alan", "score": None, "active": None, "joined": None,
     "note": "x", "code": None},
]


def put_flat_files(bucket, minutes) -> None:
    """Upload the flat files with fixed LastModified values."""
    bucket.put("flat/array.json", json.dumps(FLAT_ROWS), minutes(1))
    bucket.put(
        "flat/wrapped.json", json.dumps({"count": 3, "data": FLAT_ROWS}), minutes(2)
    )
    bucket.put(
        "lines.jsonl", "\n".join(json.dumps(row) for row in FLAT_ROWS) + "\n", minutes(3)
    )
    bucket.put("people.csv", "id,name,score\n1,Ada,1.5\n2,,x\n", minutes(4))
    table = pa.table(
        {
            "id": pa.array([1, 2], pa.int64()),
            "score": pa.array([1.5, None], pa.float64()),
            "price": pa.array([decimal.Decimal("9.99"), None], pa.decimal128(10, 2)),
            "active": pa.array([True, None], pa.bool_()),
            "name": pa.array(["Ada", None], pa.string()),
            "at": pa.array(
                [datetime.datetime(2026, 9, 1, 12, tzinfo=datetime.timezone.utc), None],
                pa.timestamp("us", tz="UTC"),
            ),
            "day": pa.array([datetime.date(2026, 9, 1), None], pa.date32()),
        }
    )
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    bucket.put("typed.parquet", buffer.getvalue(), minutes(5))


def flat_output(bucket, minutes, discover, select_all, sync) -> dict:
    """Discover and sync the flat files. Return the catalog and records."""
    put_flat_files(bucket, minutes)
    catalog = discover()
    messages = sync(select_all(catalog))
    records = [
        {"stream": m["stream"], "record": m["record"]}
        for m in messages
        if m["type"] == "RECORD"
    ]
    schemas = [
        {"stream": m["stream"], "schema": m["schema"], "key_properties": m["key_properties"]}
        for m in messages
        if m["type"] == "SCHEMA"
    ]
    return {"catalog": catalog, "schemas": schemas, "records": records}


def _record() -> None:  # pragma: no cover
    """Write the fixture from the code in this checkout."""
    from unittest import mock

    import boto3
    from moto import mock_aws
    from tests import conftest

    os.environ.update(AWS_ACCESS_KEY_ID="testing", AWS_SECRET_ACCESS_KEY="testing")
    now = conftest.minutes(conftest.NOW_OFFSET).replace(tzinfo=datetime.timezone.utc)
    with mock_aws(), mock.patch("tap_s3.tap.utc_now", lambda: now, create=True):
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=conftest.BUCKET)
        bucket = conftest.Bucket(client, conftest.BUCKET)
        output = flat_output(
            bucket, conftest.minutes, conftest.discover, conftest.select_all, conftest.sync
        )
    FIXTURE.write_text(json.dumps(output, indent=1, sort_keys=True) + "\n")


if __name__ == "__main__":  # pragma: no cover
    _record()
