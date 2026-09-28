"""Stream naming, grouping and listing, against moto."""

import boto3
import pytest
from botocore.exceptions import ClientError
from tests.conftest import BUCKET, discover, make_tap, minutes, schemas

from tap_s3 import client as client_module
from tap_s3.client import S3Bucket
from tap_s3.layout import (
    build_layout,
    is_hidden,
    normalize_prefix,
    sanitize_name,
    stream_name_for,
    strip_trailing_tokens,
)
from tap_s3.tap import TapS3

CSV = "id,name\n1,Ada\n"


@pytest.mark.parametrize(
    "stem, expected",
    [
        ("accounts", "accounts"),
        ("accounts_2026-09-01", "accounts"),
        ("accounts-20260901T1200", "accounts"),
        ("accounts-20260901", "accounts"),
        ("accounts_2026-09-01T12:30:00Z", "accounts"),
        ("accounts_2026-09-01_12-30-00", "accounts"),
        ("accounts 2026-09-01 123000", "accounts"),
        ("accounts_2026-09-01T12:30:00.123+02:00", "accounts"),
        ("accounts_2026_09_01", "accounts"),
        ("accounts.2026-09-01", "accounts"),
        ("accounts_1727500000", "accounts"),
        ("accounts_001", "accounts"),
        ("accounts_2026-09-01_2", "accounts"),
        ("accounts (1)", "accounts"),
        ("accounts_part_3", "accounts_part"),
        ("sales_q3", "sales_q3"),
        ("report2026", "report2026"),
        ("2026-09-01", "2026-09-01"),
        ("20260901", "20260901"),
        ("v2_accounts", "v2_accounts"),
    ],
)
def test_strip_trailing_tokens(stem, expected):
    assert strip_trailing_tokens(stem) == expected


@pytest.mark.parametrize(
    "name, expected",
    [
        ("accounts", "accounts"),
        ("Accounts", "Accounts"),
        ("my accounts", "my_accounts"),
        ("my-accounts.v2", "my_accounts_v2"),
        ("Café Clients", "Caf_Clients"),
        ("--x--", "x"),
        ("a__b", "a__b"),
        ("é", ""),
    ],
)
def test_sanitize_name(name, expected):
    assert sanitize_name(name) == expected


@pytest.mark.parametrize(
    "relative_key, stem, expected",
    [
        ("accounts.csv", "accounts", "accounts"),
        ("accounts_2026-09-01.csv", "accounts_2026-09-01", "accounts"),
        ("contacts/2026/09/01/part-0001.csv", "part-0001", "contacts"),
        ("Deal Notes/export.json", "export", "Deal_Notes"),
        ("2026-09-01.csv", "2026-09-01", "2026_09_01"),
        ("é.csv", "é", None),
        ("é/x.csv", "x", None),
    ],
)
def test_stream_name_for(relative_key, stem, expected):
    assert stream_name_for(relative_key, stem) == expected


@pytest.mark.parametrize(
    "prefix, expected",
    [
        (None, ""),
        ("", ""),
        ("  ", ""),
        ("exports", "exports/"),
        ("exports/", "exports/"),
        ("/exports/crm", "exports/crm/"),
    ],
)
def test_normalize_prefix(prefix, expected):
    assert normalize_prefix(prefix) == expected


@pytest.mark.parametrize(
    "relative_key, hidden",
    [
        ("accounts.csv", False),
        (".accounts.csv", True),
        ("_SUCCESS", True),
        ("accounts/_temporary/0/part.csv", True),
        ("accounts/.DS_Store", True),
        ("_staging/accounts.csv", True),
        ("my_accounts/data_file.csv", False),
    ],
)
def test_is_hidden(relative_key, hidden):
    assert is_hidden(relative_key) is hidden


def test_dated_root_files_merge_into_one_stream(bucket):
    bucket.put("accounts_2026-09-01.csv", CSV, minutes(1))
    bucket.put("accounts-20260901T1200.csv", CSV, minutes(2))
    bucket.put("accounts.csv", CSV, minutes(3))
    bucket.put("accounts (1).csv", CSV, minutes(4))
    tap = make_tap()
    assert list(tap.layout.streams) == ["accounts"]
    assert [obj.key for obj in tap.layout.streams["accounts"]] == [
        "accounts_2026-09-01.csv",
        "accounts-20260901T1200.csv",
        "accounts.csv",
        "accounts (1).csv",
    ]


def test_folders_name_streams_at_any_depth(bucket):
    bucket.put("contacts/a.csv", CSV)
    bucket.put("contacts/2026/09/01/b.csv", CSV)
    bucket.put("contacts/2026/09/02/deeper/c.json", '[{"id": 3}]')
    tap = make_tap()
    assert list(tap.layout.streams) == ["contacts"]
    assert len(tap.layout.streams["contacts"]) == 3


def test_folder_files_keep_their_dates(bucket):
    bucket.put("orders_2026/a.csv", CSV)
    assert list(make_tap().layout.streams) == ["orders_2026"]


def test_root_file_and_folder_with_the_same_name_merge(bucket):
    bucket.put("accounts.csv", "id,name\n1,Ada\n", minutes(1))
    bucket.put("accounts/2026-09-02.csv", "id,region\n2,EU\n", minutes(2))
    tap = make_tap()
    assert list(tap.layout.streams) == ["accounts"]
    assert len(tap.layout.streams["accounts"]) == 2
    properties = schemas(tap.catalog_dict)["accounts"]
    assert {"id", "name", "region"} <= set(properties)


def test_case_is_kept_and_names_differ_by_case(bucket):
    bucket.put("Accounts.csv", CSV)
    bucket.put("accounts.csv", CSV)
    assert sorted(make_tap().layout.streams) == ["Accounts", "accounts"]


def test_unicode_spaces_and_odd_characters(bucket):
    bucket.put("Café Clients/ü data (1).csv", CSV)
    bucket.put("Données clients 2026-09-01.csv", CSV)
    bucket.put("deals+pipeline=Q3!.csv", CSV)
    bucket.put("日本/x.csv", CSV)
    tap = make_tap()
    assert sorted(tap.layout.streams) == ["Caf_Clients", "Donn_es_clients", "deals_pipeline_Q3"]
    assert tap.layout.unsupported_keys == ["日本/x.csv"]


def test_skips_markers_empty_and_hidden_objects(bucket):
    bucket.put("accounts/", b"")
    bucket.put("empty.csv", b"")
    bucket.put(".hidden.csv", CSV)
    bucket.put("_SUCCESS", CSV)
    bucket.put("accounts/_temporary/part.csv", CSV)
    bucket.put("accounts/.DS_Store", b"\x00\x01")
    bucket.put("accounts/real.csv", CSV)
    tap = make_tap()
    assert {name: [o.key for o in objs] for name, objs in tap.layout.streams.items()} == {
        "accounts": ["accounts/real.csv"]
    }
    assert tap.layout.unsupported_keys == []


def test_unknown_extensions_warn_and_are_skipped(bucket, tap_logs):
    bucket.put("accounts.csv", CSV)
    bucket.put("notes.xlsx", b"PK\x03\x04")
    bucket.put("contacts/readme.md", "# hi")
    bucket.put("archive.csv.zip", b"PK")
    tap = make_tap()
    assert list(tap.layout.streams) == ["accounts"]
    assert sorted(tap.layout.unsupported_keys) == [
        "archive.csv.zip",
        "contacts/readme.md",
        "notes.xlsx",
    ]
    warnings = [message for message in tap_logs if "unsupported" in message]
    assert len(warnings) == 1
    for key in ("notes.xlsx", "contacts/readme.md", "archive.csv.zip"):
        assert key in warnings[0]


def test_unknown_extension_warning_is_capped(bucket, tap_logs, monkeypatch):
    monkeypatch.setattr("tap_s3.layout.MAX_LOGGED_KEYS", 2)
    for index in range(5):
        bucket.put(f"file{index}.xlsx", b"x")
    make_tap()
    assert any("5 objects" in message and "and 3 more" in message for message in tap_logs)


@pytest.mark.parametrize("prefix", ["exports", "exports/", "/exports/"])
def test_prefix_with_and_without_trailing_slash(bucket, prefix):
    bucket.put("exports/accounts.csv", CSV)
    bucket.put("exports/contacts/a.csv", CSV)
    bucket.put("exports2/other.csv", CSV)
    bucket.put("root.csv", CSV)
    tap = make_tap(path_prefix=prefix)
    assert sorted(tap.layout.streams) == ["accounts", "contacts"]


def test_empty_prefix_reads_the_bucket_root(bucket):
    bucket.put("exports/accounts.csv", CSV)
    bucket.put("root.csv", CSV)
    assert sorted(make_tap(path_prefix="").layout.streams) == ["exports", "root"]


def test_prefix_that_matches_nothing(bucket):
    bucket.put("exports/accounts.csv", CSV)
    catalog = discover(path_prefix="missing/")
    assert catalog["streams"] == []


def test_empty_bucket(bucket):
    assert discover()["streams"] == []


def test_listing_paginates_past_1000_objects(bucket):
    for index in range(1005):
        bucket.put(f"events/part-{index:04d}.csv", CSV)
    s3_bucket = S3Bucket("testing", "testing", BUCKET, region="us-east-1")
    pages = []
    s3_bucket.client.meta.events.register(
        "after-call.s3.ListObjectsV2", lambda **kwargs: pages.append(1)
    )
    layout = build_layout(s3_bucket.list_objects(""), "")
    assert len(layout.streams["events"]) == 1005
    assert len(pages) == 2
    assert len(make_tap().layout.streams["events"]) == 1005


def test_region_is_resolved_from_the_bucket(aws):
    boto3.client("s3", region_name="eu-west-2").create_bucket(
        Bucket="eu-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    tap = make_tap(bucket="eu-bucket", region=None)
    assert tap.bucket.region == "eu-west-2"
    assert tap.bucket.client.meta.region_name == "eu-west-2"


def test_region_defaults_to_us_east_1(bucket):
    tap = make_tap(region=None)
    assert tap.bucket.region == "us-east-1"


def test_configured_region_skips_the_lookup(bucket, monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError("GetBucketLocation should not run")

    monkeypatch.setattr(client_module, "resolve_region", fail)
    assert make_tap(region="us-east-1").bucket.region == "us-east-1"


@pytest.mark.parametrize(
    "location, region",
    [(None, "us-east-1"), ("", "us-east-1"), ("EU", "eu-west-1"), ("ap-south-1", "ap-south-1")],
)
def test_normalize_location(location, region):
    assert client_module.normalize_location(location) == region


class _DeniedClient:
    def __init__(self, headers):
        self.headers = headers

    def get_bucket_location(self, Bucket):
        raise ClientError(
            {
                "Error": {"Code": "AccessDenied", "Message": "Access Denied"},
                "ResponseMetadata": {"HTTPHeaders": self.headers},
            },
            "GetBucketLocation",
        )


class _Session:
    def __init__(self, client):
        self._client = client

    def client(self, *args, **kwargs):
        return self._client


def test_denied_lookup_uses_the_region_header():
    session = _Session(_DeniedClient({"x-amz-bucket-region": "ca-central-1"}))
    assert client_module.resolve_region(session, "b") == "ca-central-1"


def test_denied_lookup_without_a_header_fails_clearly():
    session = _Session(_DeniedClient({}))
    with pytest.raises(client_module.RegionLookupError, match="Set `region`"):
        client_module.resolve_region(session, "b")


def test_missing_required_settings_fail_clearly(aws):
    tap = TapS3(config={"bucket": "b"}, parse_env_config=False, validate_config=False,
                setup_mapper=False)
    with pytest.raises(ValueError, match="aws_access_key_id, aws_secret_access_key"):
        tap.discover_streams()
