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
│   ├── service.py      # main(port): gateway + Neon stack + role sync + status API
│   ├── neon_stack.py   # starts broker, safekeeper, pageserver and compute
│   ├── roles.py        # mirrors platform users and groups into Postgres roles
│   └── build_pre.sh    # copies the Neon binaries into the job image
├── explorer/
│   ├── app.py          # FastAPI app: query, metadata, data grid and management endpoints
│   ├── db.py           # catalog queries and generated DDL with psycopg
│   ├── rows.py         # the data grid: parameterised select, insert, update, delete
│   ├── actions.py      # management actions rendered as SQL
│   ├── audit.py        # the query audit trail, read from the service log
│   ├── services.py     # finds Neon Postgres services on the platform
│   └── static/         # the single-page UI
├── access.py           # the role secret and per-user role passwords, shared by both jobs
├── tests/              # gateway tests with boto3, config, state, role sync and explorer tests
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
Explorer** app. It is a small database tool in the spirit of DBeaver: a
schema browser, a data grid, a SQL editor and the usual table operations.
The connection panel lists the Neon Postgres services found on the platform.
A service counts when its status API reports a tenant. The panel also takes a
host, port, database, user and password by hand. On the platform it also
offers "Connect as me": the app then connects as the Postgres role of the
signed-in platform user and asks for no password (see Access control). The
browser keeps the connection details and sends them with every request. The
app holds no state, so several people can use it against different databases.

Schema browser. The sidebar lists the schemas of the current database. Expand
a schema to see its tables and views with their estimated row count and size.
Each schema has buttons to create a table in it and to drop it. The Server tab
lists the databases on the server. Click one to reconnect to it.

Table tab. Pick a table or view in the sidebar. The Structure sub-tab shows
the columns with their types, nullability, defaults and comments, the indexes,
the constraints and the definition of a view. The DDL button shows the CREATE
statements that rebuild the table or view, assembled from the catalog, with a
Copy button.

Data grid. The Data sub-tab shows the rows of the table one page at a time.
Click a column header to sort by it, and click it again for descending order.
Add a filter from the column, operator and value boxes. Each filter shows as
a chip above the grid. Click the chip to remove the filter. Click a cell to edit
it. Enter saves the cell, Escape cancels, and Ctrl+Shift+N sets it to NULL.
Insert row opens a form with one field per column. A blank field takes the
column default. Tick rows and press Delete selected to remove them. Export CSV
downloads the rows of the current page. A view is read only.

Every grid operation runs as parameterised SQL. Column and table names go
through `psycopg.sql.Identifier` and values travel as query parameters, so a
value is never pasted into the statement text. The grid addresses a row by its
`ctid`, the position Postgres gives it. If someone else changed the row after
the page was loaded, the update matches no row and the grid asks you to
reload.

SQL editor. The Query tab runs the text in the editor with Ctrl+Enter and
shows the result set, the status line, the timing and the error text. Every
statement runs with autocommit. The history list keeps the last 30 statements
that ran in this browser. Export CSV downloads the result set, up to the row
limit.

Table operations. The app can create or drop a schema and create a table
with its columns, types, primary key and defaults. It can add, rename,
comment or drop a column, and create or drop an index. It can rename, comment,
truncate or drop a table or view. Each operation renders its SQL with quoted identifiers and shows it in a
confirmation dialog. Run executes the statement, Put in editor copies it into
the Query tab, and Cancel closes the dialog. A dialog for a destructive
operation (drop, truncate, delete rows) says that the change cannot be undone.

Audit tab. The statements in the service log, with who ran them. See
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
| `NEON_ROLE_SECRET` | derived from `NEON_PG_PASSWORD` by `deploy.py` | Shared by both jobs. The service derives every per-user role password from it. Unset means no role sync. See Access control. |
| `NEON_ROLE_SYNC_INTERVAL` | `60` | Seconds between two syncs of platform users and groups into roles |

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
and `python deploy.py explorer` the app only. Both jobs receive
`NEON_ROLE_SECRET`, so deploy them with the same variables, or the explorer
derives the wrong role passwords. See Access control.

The service appears on the platform as **Neon Postgres**. Its status API is
served on the platform port: `/health` returns `OK` once Postgres accepts
connections, and `/` returns the tenant, timeline and generation. The app
appears as **Neon Postgres Explorer** in the apps section; open it, pick the
service and connect as yourself, or untick "Connect as me" and enter the
`cloud_admin` password.

## Connecting from another job

Other jobs in the same environment reach Postgres by the service hostname on
port 5432. The hostname is the service name in lowercase with hyphens. The
superuser is `cloud_admin` and its password is the `NEON_PG_PASSWORD` value
from the deploy. A job can also connect as a platform user, with the
password that Access control describes. To read the superuser password back
later:

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

## Access control

Datatailr ACLs decide who can open the explorer app and who can read, change
or operate the service. They do not reach inside the database: an ACL is
attached to a platform object (a job, a blob, a key in the KV store) and
cannot be attached to a table. Table access is the job of Postgres. This
example connects the two. The service turns platform users and groups into
Postgres roles, and the explorer connects as the platform user who is using
it. You then grant tables to groups with plain SQL, and Postgres enforces the
grants for every client.

### How it works

The service holds a secret, `NEON_ROLE_SECRET`. Every minute it reads the
users and groups of the platform and makes the roles match:

- Every platform group becomes a Postgres role of the same name that cannot
  log in. Grant tables to these roles.
- Every platform user becomes a Postgres role of the same name that can log
  in. Its password is derived from the secret and the user name, and it is a
  member of the roles of the user's groups.
- A user who leaves a group loses that membership. A user who leaves the
  platform can no longer log in. Roles are never dropped, because a role may
  own tables.
- Roles the sync created carry the comment `datatailr:user` or
  `datatailr:group`. The sync never changes a role without that comment, so a
  role you created by hand keeps its settings. System users, `root` and names
  of built-in roles such as `cloud_admin` are skipped.

The explorer holds the same secret. The platform sends the signed user to
every app in the `X-Datatailr-User` header, and the auth-server in the app
container verifies the signature before the request reaches the app. When you
tick "Connect as me", the explorer reads your user name from that header,
derives the password of your role, and connects as you. It never asks you for
a password. Every query then runs with the privileges of your role, and the
error "permission denied" tells you that a table is not granted to you or to
one of your groups.

The status API of the service reports the sync under `role_sync`: the time of
the last sync, the last error, and how many users and groups it mirrored.

### Grant tables to groups

Connect as `cloud_admin` and grant tables to group roles. For example, to let
the `analysts` group read the `sales` schema and the `developers` group do
anything in it:

```sql
grant usage on schema sales to analysts, developers;
grant select on all tables in schema sales to analysts;
alter default privileges in schema sales grant select on tables to analysts;
grant all on all tables in schema sales to developers;
alter default privileges in schema sales grant all on tables to developers;
```

A user who belongs to both groups gets both sets of privileges. To give one
user more than their groups allow, grant to the user role directly. Group
membership changes made on the platform, with `dt group add-users` or in the
users page, reach Postgres within a minute.

The `dtusers` group contains every user of the platform. A grant to `dtusers`
means everyone. In Postgres 15 and later, every role can use the `public`
schema but cannot create tables in it. Grant select on its tables like on any
other schema.

### Deploy with a secret

`deploy.py` gives both jobs the same `NEON_ROLE_SECRET`. When you do not set
it, the deploy derives it from `NEON_PG_PASSWORD`. Knowing the secret gives
every role password but not the `cloud_admin` password. If the password is
the default `cloud_admin`, the derived secret is public knowledge too, and the
deploy prints a warning. Set your own:

```bash
NEON_BUCKET=my-database NEON_PG_PASSWORD='a-long-random-password' \
NEON_ROLE_SECRET='another-long-random-secret' python deploy.py
```

Deploy the service and the explorer together, or with the same secret, or the
explorer derives the wrong passwords. `NEON_ROLE_SECRET=off` deploys without
the sync. Then the explorer offers no "Connect as me", and everyone connects
as `cloud_admin` as before.

The Settings table above lists `NEON_ROLE_SECRET` and
`NEON_ROLE_SYNC_INTERVAL`.

### Other clients

Any job can connect as a platform user when it knows the secret, with the
password from `neon_postgres.access.role_password(secret, user)`. A job that
connects as `cloud_admin` still bypasses all grants. Keep that password to
administrators.

### Limits

- The sync runs every minute. A new user or group appears in Postgres after
  the next sync, not at once.
- The catalog stays visible. A user sees that a table exists, with its
  columns, but cannot read its rows without a grant. Row-level security on top
  of this works as in any Postgres, with `current_user` as the key.
- A user whose name matches a group name, a built-in role, or a role you
  created by hand gets no role. The service log says so.
- The sync lists users and groups as the user the service runs as. That user
  needs the right to run `dt user ls` and `dt group ls`.

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
The explorer API tests and the role sync tests need a running Postgres and are
skipped unless `NEON_TEST_DSN` is set. The role sync tests create and drop
roles named `acl_*` and a schema `acl_test`, so point them at a database you
can spare, for example the Docker container above. The user in the DSN must be
a superuser: the sync reads `pg_authid`. The audit tests parse a sample of the
service log and check that the explorer tags its connections with the platform
user.
