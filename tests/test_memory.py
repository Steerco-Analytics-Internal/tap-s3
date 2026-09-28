"""Large objects are streamed, not loaded whole."""

from tests.conftest import gzipped, make_tap, select_all

from tap_s3 import client as client_module

ROWS = 200_000


def big_csv() -> bytes:
    lines = ["id,email,amount,note"]
    lines += [f"{i},user{i}@example.com,{i * 1.25},row number {i}" for i in range(1, ROWS + 1)]
    return ("\n".join(lines) + "\n").encode("utf-8")


def spy_reads(monkeypatch):
    """Record the size of every read from an S3 response body."""
    reads = []
    original = client_module._StreamingBodyReader.readinto

    def readinto(self, buffer):
        count = original(self, buffer)
        reads.append(count)
        return count

    monkeypatch.setattr(client_module._StreamingBodyReader, "readinto", readinto)
    return reads


def stream_rows(key):
    tap = make_tap()
    catalog = select_all(tap.catalog_dict)
    tap = make_tap(catalog=catalog)
    stream = tap.streams["big"]
    return stream.get_records(None)


def check_streaming(monkeypatch, bucket, key, body):
    bucket.put(key, body)
    reads = spy_reads(monkeypatch)
    rows = stream_rows(key)
    reads.clear()
    first = next(rows)
    assert str(first["id"]) == "1"
    read_before_first_row = sum(reads)
    assert read_before_first_row <= 2 * client_module.STREAM_BUFFER_BYTES
    count = 1 + sum(1 for _ in rows)
    assert count == ROWS
    assert len(reads) > 5
    return read_before_first_row


def test_large_csv_is_streamed_in_chunks(monkeypatch, bucket):
    body = big_csv()
    assert len(body) > 5 * client_module.STREAM_BUFFER_BYTES
    read_before_first_row = check_streaming(monkeypatch, bucket, "big.csv", body)
    assert read_before_first_row < len(body) / 5


def test_large_gzipped_csv_is_streamed_in_chunks(monkeypatch, bucket):
    monkeypatch.setattr(client_module, "STREAM_BUFFER_BYTES", 64 * 1024)
    check_streaming(monkeypatch, bucket, "big.csv.gz", gzipped(big_csv()))


def test_large_jsonl_is_streamed_in_chunks(monkeypatch, bucket):
    lines = [f'{{"id": {i}, "email": "user{i}@example.com"}}' for i in range(1, ROWS + 1)]
    body = ("\n".join(lines) + "\n").encode("utf-8")
    check_streaming(monkeypatch, bucket, "big.jsonl", body)
