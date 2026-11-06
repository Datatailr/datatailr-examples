# Neon Postgres on Datatailr

This example runs a Postgres database as a Datatailr service. A service has no
persistent disk, so the database keeps its durable state in Datatailr blob
storage. It does this with [Neon](https://github.com/neondatabase/neon), an
open source Postgres whose storage layer writes to object storage.

Neon speaks the S3 API. Datatailr blob storage speaks the `dt blob` API. A
small S3 gateway inside the service container translates between the two.

```
                 container of the "Neon Postgres" service
 +-------------------------------------------------------------------+
 |  Postgres 17 (compute_ctl)  <-->  pageserver   <-->  S3 gateway   |---> dt blob
 |        5432, all interfaces        safekeeper  <-->  127.0.0.1    |     (Datatailr
 |                                    storage_broker        :9000    |      blob storage)
 |  status API on the platform port: /health, /                      |
 +-------------------------------------------------------------------+
```

## What is inside

```
neon_postgres/
├── s3_gateway/
│   ├── app.py          # the S3 subset Neon uses, as a FastAPI app
│   └── backends.py     # Datatailr blob, local directory and in-memory backends
├── service/
│   ├── service.py      # main(port): gateway + Neon stack + status API
│   ├── neon_stack.py   # starts broker, safekeeper, pageserver and compute
│   └── build_pre.sh    # copies the Neon binaries into the job image
├── tests/              # gateway tests with boto3, config and state tests
├── deploy.py
├── requirements.txt
└── requirements-dev.txt
```

### The S3 gateway (`s3_gateway/`)

Neon's storage layer uses a small part of the S3 API: `PutObject`,
`GetObject` with `Range` and `If-None-Match`, `HeadObject`, `ListObjectsV2`,
`DeleteObject`, `DeleteObjects` and `CopyObject`. The gateway serves these on
`127.0.0.1:9000` and maps each one to the `datatailr.Blob` SDK, so every object
lands at `<bucket>/<key>` in the user's blob storage.

Three things do not exist in the blob API and are emulated:

- A range read downloads the whole object and returns the slice. Neon only
  range-reads partial WAL segments, which are 16 MB at most.
- An ETag is derived from the object size and modification time.
- A copy is a download followed by an upload.

Uploads arrive in the `aws-chunked` encoding that the AWS SDK uses; the
gateway decodes it as it spools the body to a temporary file. Request
signatures are not checked, because the gateway only listens on localhost.

### The Neon stack (`service/neon_stack.py`)

Two platform details needed care. Blob downloads use `Blob.get` rather than
`Blob.get_file`, because inside a job the file variant came back empty. And
`compute_ctl` raises the core dump limit to unlimited and aborts when the
container's hard limit is finite, so `build_pre.sh` compiles a tiny
`setrlimit` shim that is preloaded into `compute_ctl` only when that limit is
finite.

One container runs the whole stack: `storage_broker`, one `safekeeper`, one
`pageserver` and `compute_ctl`, which runs Postgres. All of them bind to
localhost except Postgres, which listens on port 5432 on all interfaces.

The tenant id, timeline id and the attach generation are stored in
`<bucket>/neon-postgres/state.json`. On every start the service increments the
generation, saves it, and attaches the tenant. A fresh bucket gets a new tenant
and timeline. An existing bucket is re-attached: the pageserver loads the timeline from
the bucket, and the service creates a new branch at the timeline's last
durable LSN and starts Postgres on that branch. A branch is needed because
two things do not survive a restart: the safekeeper's copy of the timeline,
which lives on local disk only, and the pageserver's in-memory position of the
previous WAL record, which Postgres needs before it writes anything. A branch
point is the one place where Postgres may start without it. The old timeline
stays as the ancestor of the new one, so every restart adds one branch.

### Durability

Local disk is a cache. When the container dies, the database comes back at the
last layer the pageserver uploaded. Writes after that upload are lost. One
setting bounds that window and defaults to one minute:

| Variable | Default | Meaning |
|---|---|---|
| `NEON_CHECKPOINT_TIMEOUT` | `1m` | How long the pageserver waits before it flushes and uploads a layer |
| `NEON_CHECKPOINT_DISTANCE` | `67108864` | Bytes of WAL after which it flushes and uploads anyway |
| `NEON_COMPACTION_TARGET_SIZE` | `67108864` | Largest layer file compaction may produce |
| `NEON_PARTIAL_BACKUP_TIMEOUT` | `1m` | How long the safekeeper waits before it uploads a partial WAL segment |

Lower values mean more calls to blob storage. Inside a job, blob transfers go
through the platform's remote client, which caps one transfer at 100 MB, so
the two size settings must stay below that. The safekeeper also uploads its
WAL to the bucket, but this example does not replay that WAL on restart. Neon's
own recovery test (`test_s3_wal_replay`) shows how to do that, and it would
shrink the window to the safekeeper upload interval.

This setup is good for data that a workflow can regenerate, or for development
databases. It is not a system of record: production Neon runs three
safekeepers on separate machines, which a Datatailr service cannot provide.

### Settings

| Variable | Default | Meaning |
|---|---|---|
| `NEON_PG_PASSWORD` | `cloud_admin` | Password of the `cloud_admin` superuser |
| `NEON_BUCKET` | `neon` | Directory in blob storage that holds this database |
| `NEON_PG_PORT` | `5432` | Postgres port |
| `NEON_SHARED_BUFFERS` | `256MB` | Postgres `shared_buffers` |
| `NEON_MAX_CONNECTIONS` | `100` | Postgres `max_connections` |
| `S3_GATEWAY_CONCURRENCY` | `8` | How many `dt blob` calls run at the same time |
| `NEON_STORAGE_BACKEND` | `datatailr` | `local` stores objects in a directory instead |

Two databases must use different `NEON_BUCKET` values.

## Deployment

The image build downloads about 2 GB of Neon images with
[crane](https://github.com/google/go-containerregistry) and copies the
binaries into `/opt/neon`. Expect the first build to take several minutes.

From this directory, run:

```bash
NEON_PG_PASSWORD=change-me python deploy.py
```

The service appears on the platform as **Neon Postgres**. Its status API is
served on the platform port: `/health` returns `OK` once Postgres accepts
connections, and `/` returns the tenant, timeline and generation.

## Connecting from another job

Other jobs in the same environment reach Postgres by the service hostname on
port 5432. The hostname is the service name in lowercase with hyphens:

```python
import psycopg

with psycopg.connect(
    host="neon-postgres",
    port=5432,
    user="cloud_admin",
    password="change-me",
    dbname="postgres",
) as conn:
    conn.execute("CREATE TABLE IF NOT EXISTS t(id int primary key, v text)")
    conn.execute("INSERT INTO t VALUES (1, 'hello') ON CONFLICT DO NOTHING")
    print(conn.execute("SELECT * FROM t").fetchall())
```

Clients outside the platform cannot connect: the platform ingress carries HTTP
only.

To see what the database writes, list the bucket:

```bash
dt blob ls neon/ -r
```

## Running locally with Docker

The local backend stores objects in a directory, so you can run the service
without a Datatailr cluster and check that data survives a restart:

```bash
cat > /tmp/Dockerfile.neon <<'EOF'
FROM python:3.13-slim-trixie
COPY neon_postgres/service/build_pre.sh /tmp/build_pre.sh
RUN bash /tmp/build_pre.sh && pip install --no-cache-dir fastapi uvicorn \
    && useradd -m dt && mkdir /store && chown dt /store
COPY neon_postgres /app/neon_postgres
WORKDIR /app
USER dt
ENV NEON_STORAGE_BACKEND=local NEON_LOCAL_STORE=/store PYTHONUNBUFFERED=1
CMD ["python", "-m", "neon_postgres.service.service", "--port", "8080"]
EOF
docker build -f /tmp/Dockerfile.neon -t neon-postgres .
docker volume create neon-store
docker run --rm -d --name neon -v neon-store:/store -p 5432:5432 -p 8080:8080 neon-postgres
```

Wait until `curl localhost:8080/health` prints `OK`, then connect with
`psql postgresql://cloud_admin:cloud_admin@localhost:5432/postgres`. Write
some rows, wait a minute for the upload, kill the container, start it again
with the same volume, and the rows are still there.

## Tests

```bash
pip install -r neon_postgres/requirements.txt -r neon_postgres/requirements-dev.txt
python -m pytest neon_postgres/tests
```

The gateway tests drive it with boto3, the same way a real S3 client would.
