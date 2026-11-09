# Neon Postgres on Datatailr

This example runs a Postgres database as a Datatailr service. A service has no
persistent disk, so the database keeps its durable state in Datatailr blob
storage. It does this with [Neon](https://github.com/neondatabase/neon), an
open source Postgres whose storage layer writes to object storage. A second
job, the Neon Postgres Explorer, is a web app to run queries against such a
database and manage its schemas, tables and indexes.

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
├── explorer/
│   ├── app.py          # FastAPI app: query, metadata and management endpoints
│   ├── db.py           # catalog queries with psycopg
│   ├── actions.py      # management actions rendered as SQL
│   ├── audit.py        # the query audit trail, read from the service log
│   ├── services.py     # finds Neon Postgres services on the platform
│   └── static/         # the single-page UI
├── tests/              # gateway tests with boto3, config, state and explorer tests
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

### The explorer (`explorer/`)

A FastAPI app with a single-page UI, deployed as the **Neon Postgres
Explorer** app. The connection panel lists the Neon Postgres services found
on the platform (every service whose status API reports a tenant), or takes a
host, port, database, user and password by hand. The browser keeps the
connection details and sends them with every request, so the app holds no
state and several people can use it against different databases.

- Query tab: run SQL with Ctrl+Enter, see the result set as a table, the
  status line, timing and errors. Every statement runs with autocommit.
- Table tab: columns, indexes, constraints, the view definition and sample
  rows of the relation picked in the sidebar.
- Management: create schema, create table, add, rename or drop a column,
  create or drop an index, rename, comment, truncate or drop a relation, drop a
  schema. Each action renders its SQL into the query editor with properly
  quoted identifiers, and nothing runs until you press Run.
- Server tab: version, size, and the databases on the server. Click one to
  reconnect to it.
- Audit tab: the statements in the service log, with who ran them. See
  [Audit logging](#audit-logging).

### Durability

Local disk is a cache. When the container dies, the database comes back at the
last layer the pageserver uploaded. Writes after that upload are lost. One
setting bounds that window and defaults to one minute:

| Variable | Default | Meaning |
|---|---|---|
| `NEON_CHECKPOINT_TIMEOUT` | `1m` | How long the pageserver waits before it flushes and uploads a layer |
| `NEON_CHECKPOINT_DISTANCE` | `67108864` | Bytes of WAL after which it flushes and uploads anyway |
| `NEON_PARTIAL_BACKUP_TIMEOUT` | `1m` | How long the safekeeper waits before it uploads a partial WAL segment |

Lower values mean more calls to blob storage. The safekeeper also uploads its
WAL to the bucket, but this example does not replay that WAL on restart. Neon's
own recovery test (`test_s3_wal_replay`) shows how to do that, and it would
shrink the window to the safekeeper upload interval.

This setup is good for data that a workflow can regenerate, or for development
databases. It is not a system of record: production Neon runs three
safekeepers on separate machines, which a Datatailr service cannot provide.

### Settings

| Variable | Default | Meaning |
|---|---|---|
| `NEON_PG_PASSWORD` | `cloud_admin` | Password of the `cloud_admin` superuser. Set your own, see Deployment. |
| `NEON_BUCKET` | none, required | Directory in blob storage that holds this database |
| `NEON_PG_PORT` | `5432` | Postgres port |
| `NEON_SHARED_BUFFERS` | `256MB` | Postgres `shared_buffers` |
| `NEON_MAX_CONNECTIONS` | `100` | Postgres `max_connections` |
| `NEON_LOG_STATEMENT` | `mod` | Which statements Postgres logs: `none`, `ddl`, `mod` or `all`. See [Audit logging](#audit-logging). |
| `S3_GATEWAY_CONCURRENCY` | `8` | How many `dt blob` calls run at the same time |
| `NEON_STORAGE_BACKEND` | `datatailr` | `local` stores objects in a directory instead |

Two databases must use different `NEON_BUCKET` values. The deploy script
refuses to run without one.

### Audit logging

The service log is the query audit trail. Postgres writes every logged
statement to its log with the database user, the database, the client's
application name and the client host. `compute_ctl` passes those lines to the
service's stderr, and the platform keeps them with the other job logs. Read
them with the CLI:

```bash
dt log read -r neon-postgres
```

A line looks like this:

```
PG:2026-11-09 11:42:46.715 GMT [665] user=cloud_admin db=postgres app=neon-postgres-explorer:dgeidman host=172.18.0.4 LOG:  statement: select count(*) from orders
```

Which statements are logged depends on `NEON_LOG_STATEMENT`. The default,
`mod`, logs every statement that changes the schema or the data, from every
client. `all` also logs reads. `ddl` logs schema changes only and `none` turns
the log off. Connections are always logged, with the user, the database and
the application name.

The Postgres user is `cloud_admin` for everyone, so the Postgres log cannot
tell people apart by itself. The explorer fills that gap. Every request to the
explorer carries the platform user, in the `X-Datatailr-User` header that the
platform adds. The explorer puts that name into the application name of its
database connection, as `neon-postgres-explorer:<user>`, and the name lands in
the `app=` field of every log line of that session. The explorer also raises
`log_statement` to `all` for its own sessions, so every statement that a person
types is logged, whatever the server-wide setting is. That needs a superuser
connection. If the connection is not a superuser, the explorer keeps the
server-wide level.

The Audit tab of the explorer shows the statements from the log of the
connected service, newest first. It reads the last 2,000 lines of the stderr
log through the platform CLI, so the service name in the connection panel must
be the name of a platform job. A filter box narrows the rows by user, SQL or
host. The explorer needs permission to read the service's log. If it has no
such permission, the tab shows the error from the platform.

A client that connects to port 5432 directly chooses its own application
name. Its statements show the database user and the host, but no platform
user. If a job sets its application name to the name of the platform user it
works for, the Audit tab shows that name too.

The log lines the trail is made from stay as long as the platform keeps job
logs. `all` is expensive here: `compute_ctl` runs four monitoring queries every
half second on the same superuser, and `all` logs every one of them. `mod` logs
none of those, because they only read.

These do not exist in this build of Neon Postgres: the `pgaudit` extension,
`pg_stat_statements`, and any Neon table that records queries. The Postgres
build comes from the Neon storage image, which ships eleven extensions, all of
them Neon's own or part of Postgres. The compute image that Neon Cloud uses
has `pgaudit`, but it also needs a control plane this example does not run.

## Deployment

The image build downloads about 2 GB of Neon images with
[crane](https://github.com/google/go-containerregistry) and copies the
binaries into `/opt/neon`. Expect the first build to take several minutes.

Choose the blob storage directory for the database with `NEON_BUCKET`. All of
the database's objects land under that directory, and no other job should
write there. The name is yours to pick, for example the name of the project
that owns the data.

Set a password before you deploy. `NEON_PG_PASSWORD` becomes the password of
the `cloud_admin` superuser, and every job in the environment can reach the
database on port 5432. If you do not set it, the password is `cloud_admin`,
which anyone who reads this README knows. The deploy script prints a warning
in that case. The value is stored in the job definition on the platform, where
other users with access to the job can read it.

From this directory, run:

```bash
NEON_BUCKET=my-database NEON_PG_PASSWORD='a-long-random-password' python deploy.py
```

This deploys both jobs. `python deploy.py service` deploys the database only
and `python deploy.py explorer` the app only; the app needs no variables.

The service appears on the platform as **Neon Postgres**. Its status API is
served on the platform port: `/health` returns `OK` once Postgres accepts
connections, and `/` returns the tenant, timeline and generation. The app
appears as **Neon Postgres Explorer** in the apps section; open it, pick the
service, enter the password and connect.

## Connecting from another job

Other jobs in the same environment reach Postgres by the service hostname on
port 5432. The hostname is the service name in lowercase with hyphens. The
user is `cloud_admin` and the password is the `NEON_PG_PASSWORD` value from
the deploy. To read it back later:

```bash
dt job get neon-postgres --json | grep NEON_PG_PASSWORD
```

```python
import psycopg

with psycopg.connect(
    host="neon-postgres",
    port=5432,
    user="cloud_admin",
    password="a-long-random-password",  # the NEON_PG_PASSWORD you deployed with
    dbname="postgres",
) as conn:
    conn.execute("CREATE TABLE IF NOT EXISTS t(id int primary key, v text)")
    conn.execute("INSERT INTO t VALUES (1, 'hello') ON CONFLICT DO NOTHING")
    print(conn.execute("SELECT * FROM t").fetchall())
```

Clients outside the platform cannot connect: the platform ingress carries HTTP
only.

To see what the database writes, list its directory in blob storage:

```bash
dt blob ls my-database/ -r
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

## Running the explorer locally

With the Docker container above running, start the app and point it at
`127.0.0.1:5432`:

```bash
pip install -r neon_postgres/explorer/requirements.txt
uvicorn neon_postgres.explorer.app:app --reload
```

## Tests

```bash
pip install -r neon_postgres/requirements.txt -r neon_postgres/explorer/requirements.txt -r neon_postgres/requirements-dev.txt
python -m pytest neon_postgres/tests
NEON_TEST_DSN=postgresql://cloud_admin:cloud_admin@localhost:5432/postgres python -m pytest neon_postgres/tests
```

The gateway tests drive it with boto3, the same way a real S3 client would.
The explorer API tests need a running Postgres and are skipped unless
`NEON_TEST_DSN` is set. The audit tests parse a sample of the service log and
check that the explorer tags its connections with the platform user.
