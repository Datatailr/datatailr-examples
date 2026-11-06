"""Exercise the S3 gateway with a real S3 client (boto3) against the in-memory backend.

The tests cover exactly the S3 subset that Neon's ``remote_storage`` crate uses:
PutObject, GetObject (with Range and If-None-Match), HeadObject, ListObjectsV2
(with prefix, delimiter and pagination), DeleteObject, DeleteObjects and
CopyObject.
"""

from __future__ import annotations

import socket
import threading
import time

import boto3
import pytest
import uvicorn
from botocore.config import Config
from botocore.exceptions import ClientError

from neon_postgres.s3_gateway.app import create_app
from neon_postgres.s3_gateway.backends import MemoryBackend

BUCKET = "neon"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def gateway():
    backend = MemoryBackend()
    port = _free_port()
    config = uvicorn.Config(create_app(backend), host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not server.started:
        if time.time() > deadline:
            raise RuntimeError("gateway did not start")
        time.sleep(0.05)
    yield backend, f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture
def s3(gateway):
    backend, url = gateway
    backend.clear()
    return boto3.client(
        "s3",
        endpoint_url=url,
        aws_access_key_id="neon",
        aws_secret_access_key="neon",
        region_name="local",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}),
    )


def _error_code(exc: ClientError) -> str:
    return exc.response["Error"]["Code"]


def test_put_get_head_roundtrip(s3):
    s3.put_object(Bucket=BUCKET, Key="pageserver/tenants/t1/index_part.json", Body=b'{"a": 1}')

    got = s3.get_object(Bucket=BUCKET, Key="pageserver/tenants/t1/index_part.json")
    assert got["Body"].read() == b'{"a": 1}'
    assert got["ContentLength"] == 8
    assert got["ETag"].startswith('"')

    head = s3.head_object(Bucket=BUCKET, Key="pageserver/tenants/t1/index_part.json")
    assert head["ContentLength"] == 8
    assert head["ETag"] == got["ETag"]
    assert head["LastModified"] is not None


def test_large_body_roundtrip(s3):
    body = bytes(range(256)) * 4096 * 5  # 5 MiB, exercises aws-chunked uploads
    s3.put_object(Bucket=BUCKET, Key="layers/big", Body=body)
    assert s3.get_object(Bucket=BUCKET, Key="layers/big")["Body"].read() == body


def test_missing_object(s3):
    with pytest.raises(ClientError) as exc:
        s3.get_object(Bucket=BUCKET, Key="nope")
    assert _error_code(exc.value) == "NoSuchKey"

    with pytest.raises(ClientError) as exc:
        s3.head_object(Bucket=BUCKET, Key="nope")
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_range_get(s3):
    s3.put_object(Bucket=BUCKET, Key="wal/seg", Body=b"0123456789")

    partial = s3.get_object(Bucket=BUCKET, Key="wal/seg", Range="bytes=2-5")
    assert partial["Body"].read() == b"2345"
    assert partial["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert partial["ContentRange"] == "bytes 2-5/10"

    tail = s3.get_object(Bucket=BUCKET, Key="wal/seg", Range="bytes=7-")
    assert tail["Body"].read() == b"789"

    suffix = s3.get_object(Bucket=BUCKET, Key="wal/seg", Range="bytes=-3")
    assert suffix["Body"].read() == b"789"


def test_if_none_match_returns_not_modified(s3):
    s3.put_object(Bucket=BUCKET, Key="k", Body=b"v")
    etag = s3.head_object(Bucket=BUCKET, Key="k")["ETag"]

    with pytest.raises(ClientError) as exc:
        s3.get_object(Bucket=BUCKET, Key="k", IfNoneMatch=etag)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 304

    assert s3.get_object(Bucket=BUCKET, Key="k", IfNoneMatch='"other"')["Body"].read() == b"v"


def test_list_with_prefix_and_delimiter(s3):
    for key in [
        "pageserver/tenants/t1/timelines/a/000-100",
        "pageserver/tenants/t1/timelines/a/index_part.json-00000001",
        "pageserver/tenants/t1/timelines/b/index_part.json-00000001",
        "safekeeper/t1/a/seg1",
    ]:
        s3.put_object(Bucket=BUCKET, Key=key, Body=b"x")

    flat = s3.list_objects_v2(Bucket=BUCKET, Prefix="pageserver/tenants/t1/timelines/")
    assert [o["Key"] for o in flat["Contents"]] == [
        "pageserver/tenants/t1/timelines/a/000-100",
        "pageserver/tenants/t1/timelines/a/index_part.json-00000001",
        "pageserver/tenants/t1/timelines/b/index_part.json-00000001",
    ]
    assert "CommonPrefixes" not in flat

    grouped = s3.list_objects_v2(Bucket=BUCKET, Prefix="pageserver/tenants/t1/timelines/", Delimiter="/")
    assert "Contents" not in grouped
    assert [p["Prefix"] for p in grouped["CommonPrefixes"]] == [
        "pageserver/tenants/t1/timelines/a/",
        "pageserver/tenants/t1/timelines/b/",
    ]

    # A prefix that is not a directory boundary (how the pageserver finds the latest index_part).
    partial = s3.list_objects_v2(Bucket=BUCKET, Prefix="pageserver/tenants/t1/timelines/a/index_part.json")
    assert [o["Key"] for o in partial["Contents"]] == [
        "pageserver/tenants/t1/timelines/a/index_part.json-00000001"
    ]

    empty = s3.list_objects_v2(Bucket=BUCKET, Prefix="does/not/exist/")
    assert empty["KeyCount"] == 0
    assert "Contents" not in empty


def test_list_pagination(s3):
    keys = [f"p/{i:03d}" for i in range(7)]
    for key in keys:
        s3.put_object(Bucket=BUCKET, Key=key, Body=b"x")

    seen = []
    token = None
    pages = 0
    while True:
        kwargs = {"Bucket": BUCKET, "Prefix": "p/", "MaxKeys": 3}
        if token:
            kwargs["ContinuationToken"] = token
        page = s3.list_objects_v2(**kwargs)
        pages += 1
        seen.extend(o["Key"] for o in page.get("Contents", []))
        if not page["IsTruncated"]:
            break
        token = page["NextContinuationToken"]
    assert seen == keys
    assert pages == 3


def test_delete_single_and_batch(s3):
    for key in ["d/1", "d/2", "d/3"]:
        s3.put_object(Bucket=BUCKET, Key=key, Body=b"x")

    s3.delete_object(Bucket=BUCKET, Key="d/1")
    s3.delete_object(Bucket=BUCKET, Key="d/does-not-exist")  # S3 semantics: still 204

    result = s3.delete_objects(
        Bucket=BUCKET,
        Delete={"Objects": [{"Key": "d/2"}, {"Key": "d/3"}, {"Key": "d/missing"}]},
    )
    assert sorted(d["Key"] for d in result["Deleted"]) == ["d/2", "d/3", "d/missing"]
    assert "Errors" not in result

    assert s3.list_objects_v2(Bucket=BUCKET, Prefix="d/")["KeyCount"] == 0


def test_copy_object(s3):
    s3.put_object(Bucket=BUCKET, Key="src/partial", Body=b"partial segment")

    s3.copy_object(Bucket=BUCKET, Key="dst/partial", CopySource={"Bucket": BUCKET, "Key": "src/partial"})

    assert s3.get_object(Bucket=BUCKET, Key="dst/partial")["Body"].read() == b"partial segment"
    assert s3.get_object(Bucket=BUCKET, Key="src/partial")["Body"].read() == b"partial segment"

    with pytest.raises(ClientError) as exc:
        s3.copy_object(Bucket=BUCKET, Key="dst/x", CopySource={"Bucket": BUCKET, "Key": "src/missing"})
    assert _error_code(exc.value) == "NoSuchKey"


def test_head_bucket(s3):
    assert s3.head_bucket(Bucket=BUCKET)["ResponseMetadata"]["HTTPStatusCode"] == 200
