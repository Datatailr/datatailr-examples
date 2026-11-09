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
│   ├── pg_tunnel.py    # Postgres over a WebSocket, for clients outside the platform
│   └── build_pre.sh    # copies the Neon binaries into the job image
├── explorer/
│   ├── app.py          # FastAPI app: query, metadata and management endpoints
│   ├── db.py           # catalog queries with psycopg
│   ├── actions.py      # management actions rendered as SQL
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
| `NEON_PG_TUNNEL` | `1` | `0` turns off the `/pg` WebSocket endpoint, see External connections |
| `NEON_SHARED_BUFFERS` | `256MB` | Postgres `shared_buffers` |
| `NEON_MAX_CONNECTIONS` | `100` | Postgres `max_connections` |
| `S3_GATEWAY_CONCURRENCY` | `8` | How many `dt blob` calls run at the same time |
| `NEON_STORAGE_BACKEND` | `datatailr` | `local` stores objects in a directory instead |

Two databases must use different `NEON_BUCKET` values. The deploy script
refuses to run without one.

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

A client outside the platform, such as `psql` on a laptop, needs a tunnel.
See [External connections](#external-connections).

To see what the database writes, list its directory in blob storage:

```bash
dt blob ls my-database/ -r
```

## External connections

The platform ingress carries HTTP only. The Postgres wire protocol is raw TCP.
A client on your laptop therefore cannot open port 5432 on the platform. Two
paths carry a byte stream through the HTTP ingress. Both work with `psql` and
with every other Postgres client. Both need a Datatailr login.

Security warning. On both paths the platform checks your login cookie and the
`access` permission of a job before a byte reaches Postgres. Postgres then
checks the `cloud_admin` password. A person who holds a valid login cookie and
the password is the database superuser from anywhere on the internet. Set a
strong `NEON_PG_PASSWORD`, keep the `access` list of the service and of the
workstation short, and treat the login cookie (the `jwt_cookie` value in
`~/.dt/remote_client/remote_client.cfg`) as a secret.

### Through a workstation

A platform workstation runs an SSH server. The SSH connection from your
laptop travels inside a WebSocket to the ingress. The SSH proxy on the
platform checks your login, starts the workstation if it is stopped, and
connects to port 22 of the workstation container. The workstation is on the
same service network as the database, and its SSH server allows port
forwarding. No code change is needed.

1. If you have not logged in on this laptop, run `datatailr login`.
2. If you have not set up SSH for the workstation, run
   `python -m datatailr setup-ssh default-workstation`. Use the name of the
   workstation template in your environment. The command installs
   `websocat`, creates an SSH key and writes a `dt-default-workstation` host
   to `~/.ssh/config`.
3. Open the tunnel. The hostname `neon-postgres` resolves inside the
   workstation. Pick a free local port, for example 25432:

   ```bash
   ssh -N -L 127.0.0.1:25432:neon-postgres:5432 dt-default-workstation
   ```

4. Connect from a second terminal:

   ```bash
   psql "postgresql://cloud_admin:<NEON_PG_PASSWORD>@127.0.0.1:25432/postgres"
   ```

The workstation must be in the same environment as the service. The
`setup-ssh` command uses `dev`. If the workstation is stopped, the first
`ssh` waits while the platform starts it. This path was tested from a laptop
against the `v2-trial` installation: `psql` created a table, inserted a row,
read it back and dropped the table.

### Through the service

The status API of the service has a WebSocket endpoint at `/pg`. Every
binary frame that arrives there goes to the Postgres socket of the container,
and every byte from Postgres comes back as a binary frame. On your laptop,
`websocat` turns a local TCP port into that WebSocket.

1. If `websocat` is not installed, install it. On macOS: `brew install
   websocat`. The `setup-ssh` command above installs it as well.
2. Read the login cookie that `datatailr login` saved:

   ```bash
   JWT=$(python3 -c "import json,os;print(json.load(open(os.path.expanduser('~/.dt/remote_client/remote_client.cfg')))['jwt_cookie'])")
   ```

3. Start the local listener. Replace `<domain>` with the platform domain,
   and `dev` with the environment of the service:

   ```bash
   websocat -E -b tcp-l:127.0.0.1:25432 "wss://<domain>/job/dev/neon-postgres/pg" -H "Cookie: X-Datatailr-Oidc-Data=$JWT"
   ```

4. Connect from a second terminal:

   ```bash
   psql "postgresql://cloud_admin:<NEON_PG_PASSWORD>@127.0.0.1:25432/postgres"
   ```

TLS ends at the ingress, so the relay carries plain Postgres frames. The
`psql` default `sslmode=prefer` works. `sslmode=require` fails with
"server does not support SSL". The service writes one log line when a tunnel
opens and one when it closes, with the platform user name and the byte
counts. Set `NEON_PG_TUNNEL=0` at deploy time to turn the endpoint off.

This path was tested on a laptop: `psql`, `websocat`, the relay and a local
Postgres container. The deployed service on `v2-trial` does not have the
endpoint yet, because it predates this change. Redeploy the service to get
it. Note that a redeploy restarts the database.

### What does not work

- No platform setting publishes a TCP port of a job. A job definition has no
  port field, and the cloud load balancer in front of the platform forwards
  HTTPS to the ingress only.
- A load balancer or firewall rule for port 5432 in the cloud account would
  bypass the platform login and expose the Postgres password prompt to the
  internet. It also needs a fixed node for the service. Do not do this.
- The SSH proxy connects to workstations only, never to a service container.
  A tunnel to the database therefore always runs through a workstation or
  through the `/pg` endpoint.
- Postgres `sslmode=require` does not work on either path, because the
  tunnel ends inside the container. The connection is encrypted between the
  laptop and the ingress by HTTPS on both paths.

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
`NEON_TEST_DSN` is set.
