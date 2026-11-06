"""A minimal S3-compatible HTTP endpoint on top of a ``Backend``.

It implements the subset of the S3 REST API that Neon's ``remote_storage``
crate calls: PutObject, GetObject (Range, If-None-Match), HeadObject,
ListObjectsV2, DeleteObject, DeleteObjects, CopyObject and HeadBucket.

Request signatures are not checked: the gateway listens on localhost inside
the service container and is reached only by the Neon processes next to it.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import os
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import formatdate
from urllib.parse import unquote
from xml.sax.saxutils import escape

from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse

from neon_postgres.s3_gateway.backends import Backend, NotFound, ObjectInfo

log = logging.getLogger("s3_gateway")

S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"
CHUNK = 1024 * 1024


# --------------------------------------------------------------------------- helpers


def _etag(info: ObjectInfo) -> str:
    """A stable opaque ETag. Neon only compares ETags for equality."""
    digest = hashlib.md5(f"{info.size}:{info.mtime:.6f}".encode()).hexdigest()
    return f'"{digest}"'


def _http_date(mtime: float) -> str:
    return formatdate(mtime, usegmt=True)


def _iso_date(mtime: float) -> str:
    return datetime.fromtimestamp(mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _xml(body: str, status: int = 200) -> Response:
    return Response(content='<?xml version="1.0" encoding="UTF-8"?>\n' + body, status_code=status, media_type="application/xml")


def _error(code: str, message: str, status: int, key: str = "") -> Response:
    extra = f"<Key>{escape(key)}</Key>" if key else ""
    return _xml(f"<Error><Code>{code}</Code><Message>{escape(message)}</Message>{extra}</Error>", status)


def _parse_range(header: str, size: int) -> tuple[int, int] | None:
    """Return the inclusive (start, end) for a ``bytes=`` range, or None for an unsatisfiable one."""
    if not header.startswith("bytes="):
        return None
    spec = header[len("bytes="):].split(",")[0].strip()
    start_s, _, end_s = spec.partition("-")
    if start_s == "":
        count = int(end_s)
        if count <= 0:
            return None
        return max(size - count, 0), size - 1
    start = int(start_s)
    end = int(end_s) if end_s else size - 1
    if start >= size:
        return None
    return start, min(end, size - 1)


class AwsChunkedDecoder:
    """Incrementally decode an ``aws-chunked`` request body.

    Format: ``<hex size>[;chunk-signature=...]\\r\\n<data>\\r\\n`` repeated, then a
    zero-size chunk followed by optional trailer headers. The trailer checksums
    are ignored.
    """

    def __init__(self) -> None:
        self._buffer = b""
        self._remaining = 0  # payload bytes still expected in the current chunk
        self._skip = 0  # bytes of the CRLF after a chunk payload still to drop
        self.done = False

    def feed(self, data: bytes) -> bytes:
        if self.done:
            return b""
        self._buffer += data
        out = bytearray()
        while True:
            if self._skip > 0:
                drop = min(self._skip, len(self._buffer))
                self._buffer = self._buffer[drop:]
                self._skip -= drop
                if self._skip > 0:
                    break
            if self._remaining > 0:
                take = min(self._remaining, len(self._buffer))
                out += self._buffer[:take]
                self._buffer = self._buffer[take:]
                self._remaining -= take
                if self._remaining > 0:
                    break
                self._skip = 2  # the CRLF after the chunk payload
                continue
            line_end = self._buffer.find(b"\r\n")
            if line_end < 0:
                break
            header = self._buffer[:line_end].decode("ascii", "replace")
            self._buffer = self._buffer[line_end + 2:]
            size = int(header.split(";", 1)[0].strip() or "0", 16)
            if size == 0:
                self.done = True
                break
            self._remaining = size
        return bytes(out)


def _is_aws_chunked(request: Request) -> bool:
    encoding = request.headers.get("content-encoding", "")
    sha = request.headers.get("x-amz-content-sha256", "")
    return "aws-chunked" in encoding or sha.startswith("STREAMING-")


async def _spool_body(request: Request, path: str) -> tuple[int, str]:
    """Write the (decoded) request body to ``path``; return (size, md5 hex)."""
    decoder = AwsChunkedDecoder() if _is_aws_chunked(request) else None
    digest = hashlib.md5()
    size = 0
    with open(path, "wb") as handle:
        async for chunk in request.stream():
            if decoder is not None:
                chunk = decoder.feed(chunk)
            if chunk:
                handle.write(chunk)
                digest.update(chunk)
                size += len(chunk)
    return size, digest.hexdigest()


def _file_iter(path: str, start: int, end: int):
    """Yield bytes ``start..end`` (inclusive) of the file, then delete it."""
    try:
        with open(path, "rb") as handle:
            handle.seek(start)
            remaining = end - start + 1
            while remaining > 0:
                data = handle.read(min(CHUNK, remaining))
                if not data:
                    break
                remaining -= len(data)
                yield data
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


def _encode_token(key: str) -> str:
    return base64.urlsafe_b64encode(key.encode()).decode()


def _decode_token(token: str) -> str:
    return base64.urlsafe_b64decode(token.encode()).decode()


# --------------------------------------------------------------------------- app


def create_app(backend: Backend, spool_dir: str | None = None) -> FastAPI:
    app = FastAPI(title="S3 gateway over Datatailr blob storage", docs_url=None, redoc_url=None, openapi_url=None)
    spool_dir = spool_dir or tempfile.gettempdir()

    def spool_path() -> str:
        fd, path = tempfile.mkstemp(prefix="s3gw-", dir=spool_dir)
        os.close(fd)
        return path

    # ---- pageserver control-plane upcall (registered first so it wins over /{bucket})

    @app.post("/validate")
    async def validate_generations(request: Request):
        """Answer the pageserver's generation validation call.

        A real storage controller confirms that each tenant's attach generation is
        still current before the pageserver deletes layer files. There is exactly one
        pageserver here, so every generation it asks about is the current one.
        """
        body = await request.json()
        return {"tenants": [{"id": tenant["id"], "valid": True} for tenant in body.get("tenants", [])]}

    # ---- bucket-level operations

    @app.api_route("/{bucket}", methods=["GET", "HEAD", "POST"])
    @app.api_route("/{bucket}/", methods=["GET", "HEAD", "POST"])
    async def bucket_ops(bucket: str, request: Request):
        if request.method == "HEAD":
            return Response(status_code=200)
        if request.method == "POST":
            if "delete" in request.query_params:
                return await delete_objects(bucket, request)
            return _error("NotImplemented", "unsupported bucket POST", 501)
        if "versions" in request.query_params:
            return _error("NotImplemented", "object versions are not supported", 501)
        if "location" in request.query_params:
            return _xml(f'<LocationConstraint xmlns="{S3_NS}">local</LocationConstraint>')
        return list_objects(bucket, request)

    def list_objects(bucket: str, request: Request) -> Response:
        params = request.query_params
        prefix = params.get("prefix", "")
        delimiter = params.get("delimiter", "")
        max_keys = max(1, min(int(params.get("max-keys", "1000")), 1000))
        start_after = params.get("start-after", "")
        token = params.get("continuation-token")
        if token:
            start_after = _decode_token(token)

        objects = backend.list(bucket, prefix)

        # Merge keys and common prefixes in key order, each entry tagged by kind.
        entries: list[tuple[str, ObjectInfo | None]] = []
        seen_prefixes: set[str] = set()
        for info in objects:
            rest = info.key[len(prefix):]
            if delimiter and delimiter in rest:
                common = prefix + rest.split(delimiter, 1)[0] + delimiter
                if common not in seen_prefixes:
                    seen_prefixes.add(common)
                    entries.append((common, None))
            else:
                entries.append((info.key, info))
        entries = [entry for entry in entries if entry[0] > start_after]

        page = entries[:max_keys]
        truncated = len(entries) > max_keys

        parts = [
            f'<ListBucketResult xmlns="{S3_NS}">',
            f"<Name>{escape(bucket)}</Name>",
            f"<Prefix>{escape(prefix)}</Prefix>",
            f"<MaxKeys>{max_keys}</MaxKeys>",
            f"<KeyCount>{len(page)}</KeyCount>",
            f"<IsTruncated>{'true' if truncated else 'false'}</IsTruncated>",
        ]
        if delimiter:
            parts.append(f"<Delimiter>{escape(delimiter)}</Delimiter>")
        if token:
            parts.append(f"<ContinuationToken>{escape(token)}</ContinuationToken>")
        elif params.get("start-after"):
            parts.append(f"<StartAfter>{escape(params['start-after'])}</StartAfter>")
        if truncated:
            parts.append(f"<NextContinuationToken>{_encode_token(page[-1][0])}</NextContinuationToken>")
        for name, info in page:
            if info is None:
                parts.append(f"<CommonPrefixes><Prefix>{escape(name)}</Prefix></CommonPrefixes>")
            else:
                parts.append(
                    "<Contents>"
                    f"<Key>{escape(info.key)}</Key>"
                    f"<LastModified>{_iso_date(info.mtime)}</LastModified>"
                    f"<ETag>{escape(_etag(info))}</ETag>"
                    f"<Size>{info.size}</Size>"
                    "<StorageClass>STANDARD</StorageClass>"
                    "</Contents>"
                )
        parts.append("</ListBucketResult>")
        return _xml("".join(parts))

    async def delete_objects(bucket: str, request: Request) -> Response:
        path = spool_path()
        try:
            await _spool_body(request, path)
            with open(path, "rb") as handle:
                root = ET.fromstring(handle.read())
        finally:
            os.unlink(path)
        keys = [
            element.text or ""
            for element in root.iter()
            if element.tag in ("Key", f"{{{S3_NS}}}Key")
        ]
        quiet = any(
            (element.text or "").lower() == "true"
            for element in root.iter()
            if element.tag in ("Quiet", f"{{{S3_NS}}}Quiet")
        )
        deleted, errors = [], []
        for key in keys:
            try:
                backend.delete(bucket, key)
                deleted.append(key)
            except Exception as exc:  # report, do not abort the batch
                log.warning("delete %s/%s failed: %s", bucket, key, exc)
                errors.append((key, str(exc)))
        parts = [f'<DeleteResult xmlns="{S3_NS}">']
        if not quiet:
            parts += [f"<Deleted><Key>{escape(key)}</Key></Deleted>" for key in deleted]
        parts += [
            f"<Error><Key>{escape(key)}</Key><Code>InternalError</Code><Message>{escape(msg)}</Message></Error>"
            for key, msg in errors
        ]
        parts.append("</DeleteResult>")
        return _xml("".join(parts))

    # ---- object-level operations

    @app.api_route("/{bucket}/{key:path}", methods=["GET", "HEAD", "PUT", "DELETE"])
    async def object_ops(bucket: str, key: str, request: Request):
        if key == "":
            return await bucket_ops(bucket, request)
        if request.method == "PUT":
            source = request.headers.get("x-amz-copy-source")
            if source:
                return copy_object(bucket, key, source)
            return await put_object(bucket, key, request)
        if request.method == "DELETE":
            backend.delete(bucket, key)
            return Response(status_code=204)
        info = backend.stat(bucket, key)
        if info is None:
            if request.method == "HEAD":
                return Response(status_code=404)
            return _error("NoSuchKey", "The specified key does not exist.", 404, key)
        headers = {
            "ETag": _etag(info),
            "Last-Modified": _http_date(info.mtime),
            "Accept-Ranges": "bytes",
            "Content-Type": "application/octet-stream",
        }
        if request.headers.get("if-none-match") == headers["ETag"]:
            return Response(status_code=304, headers={"ETag": headers["ETag"]})
        if request.method == "HEAD":
            headers["Content-Length"] = str(info.size)
            return Response(status_code=200, headers=headers)
        return get_object(bucket, key, info, request, headers)

    def get_object(bucket: str, key: str, info: ObjectInfo, request: Request, headers: dict) -> Response:
        path = spool_path()
        try:
            backend.download(bucket, key, path)
        except NotFound:
            os.unlink(path)
            return _error("NoSuchKey", "The specified key does not exist.", 404, key)
        except Exception:
            os.unlink(path)
            raise
        size = os.path.getsize(path)
        start, end, status = 0, size - 1, 200
        range_header = request.headers.get("range")
        if range_header:
            span = _parse_range(range_header, size)
            if span is None:
                os.unlink(path)
                return _error("InvalidRange", "The requested range is not satisfiable", 416, key)
            start, end = span
            status = 206
            headers["Content-Range"] = f"bytes {start}-{end}/{size}"
        length = max(end - start + 1, 0) if size else 0
        headers["Content-Length"] = str(length)
        if length == 0:
            os.unlink(path)
            return Response(status_code=status, headers=headers, content=b"")
        return StreamingResponse(_file_iter(path, start, end), status_code=status, headers=headers)

    async def put_object(bucket: str, key: str, request: Request) -> Response:
        path = spool_path()
        try:
            _, md5 = await _spool_body(request, path)
            backend.upload(bucket, key, path)
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass
        return Response(status_code=200, headers={"ETag": f'"{md5}"'})

    def copy_object(bucket: str, key: str, source: str) -> Response:
        source = unquote(source).lstrip("/")
        src_bucket, _, src_key = source.partition("/")
        src_key = src_key.split("?", 1)[0]
        path = spool_path()
        try:
            try:
                backend.download(src_bucket, src_key, path)
            except NotFound:
                return _error("NoSuchKey", "The specified key does not exist.", 404, src_key)
            backend.upload(bucket, key, path)
        finally:
            os.unlink(path)
        info = backend.stat(bucket, key) or ObjectInfo(key, 0, 0.0)
        return _xml(
            f'<CopyObjectResult xmlns="{S3_NS}">'
            f"<LastModified>{_iso_date(info.mtime)}</LastModified>"
            f"<ETag>{escape(_etag(info))}</ETag>"
            "</CopyObjectResult>"
        )

    return app
