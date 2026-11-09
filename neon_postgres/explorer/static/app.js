/* Neon Postgres Explorer front end.
 * The connection details live in this page (and sessionStorage) and are sent
 * with every API call. Management actions ask the server for the SQL, put it in
 * the editor, and the user runs it. */

const $ = (id) => document.getElementById(id);
const state = { connection: null, current: null, schemas: [] };

/* ---------- helpers */

async function api(path, body) {
  // Paths are relative: the platform serves the app under /job/<env>/<name>/ and
  // the page carries a <base href> for it.
  const response = await fetch(path.replace(/^\//, ""), {
    method: body === undefined ? "GET" : "POST",
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const data = await response.json().catch(() => ({ detail: response.statusText }));
  if (!response.ok) throw new Error(data.detail || `HTTP ${response.status}`);
  return data;
}

function withConnection(body) {
  if (!state.connection) throw new Error("connect first");
  return { connection: state.connection, ...body };
}

function setMsg(el, text, kind) {
  el.textContent = text || "";
  el.className = "msg" + (kind ? " " + kind : "");
}

function cell(value) {
  const td = document.createElement("td");
  if (value === null || value === undefined) {
    td.textContent = "null";
    td.className = "null";
  } else if (typeof value === "number") {
    td.textContent = String(value);
    td.className = "num";
  } else if (typeof value === "boolean") {
    td.textContent = value ? "true" : "false";
  } else if (typeof value === "object") {
    td.textContent = JSON.stringify(value);
  } else {
    td.textContent = value.length > 200 ? value.slice(0, 200) + "…" : value;
    td.title = value.length > 200 ? value : "";
  }
  return td;
}

function renderTable(container, columns, rows, options = {}) {
  container.innerHTML = "";
  if (!columns.length) {
    container.textContent = "";
    return;
  }
  const table = document.createElement("table");
  const thead = table.createTHead().insertRow();
  columns.forEach((c) => {
    const th = document.createElement("th");
    th.textContent = c;
    thead.appendChild(th);
  });
  const tbody = table.createTBody();
  rows.forEach((row) => {
    const tr = tbody.insertRow();
    row.forEach((v) => tr.appendChild(cell(v)));
    if (options.onClick) {
      tr.style.cursor = "pointer";
      tr.addEventListener("click", () => options.onClick(row));
    }
  });
  container.appendChild(table);
}

function renderRecords(container, records, columns) {
  renderTable(container, columns, records.map((r) => columns.map((c) => r[c])));
}

function showTab(name) {
  document.querySelectorAll("#tabs button").forEach((b) => b.classList.toggle("active", b.dataset.tab === name));
  document.querySelectorAll(".tab").forEach((s) => (s.hidden = s.id !== "tab-" + name));
}

function putSql(sql, run = false) {
  $("sql").value = sql;
  showTab("query");
  $("sql").focus();
  if (run) runQuery();
}

/* ---------- connection */

function readConnectionForm() {
  return {
    host: $("host").value.trim(),
    port: Number($("port").value) || 5432,
    dbname: $("dbname").value.trim() || "postgres",
    user: $("user").value.trim() || "cloud_admin",
    password: $("password").value,
  };
}

async function connect() {
  const connection = readConnectionForm();
  setMsg($("connect-msg"), "connecting…");
  $("connect-btn").disabled = true;
  try {
    const info = await api("/api/connect", { connection });
    state.connection = connection;
    sessionStorage.setItem("neon-explorer-connection", JSON.stringify(connection));
    $("conn-status").textContent = `${connection.user}@${connection.host}:${connection.port}/${info.database}`;
    $("conn-status").className = "ok";
    setMsg($("connect-msg"), info.version.split(" on ")[0] + ", " + info.size, "ok");
    renderRecords($("server-info"), [info], Object.keys(info));
    $("browser").hidden = false;
    await Promise.all([loadSchemas(), loadDatabases()]);
  } catch (err) {
    state.connection = null;
    $("conn-status").textContent = "not connected";
    $("conn-status").className = "muted";
    setMsg($("connect-msg"), err.message, "error");
  } finally {
    $("connect-btn").disabled = false;
  }
}

async function findServices() {
  const select = $("service-select");
  setMsg($("connect-msg"), "looking for database services…");
  try {
    const { services } = await api("/api/services");
    select.innerHTML = '<option value="">type the host below</option>';
    services.forEach((s) => {
      const option = document.createElement("option");
      option.value = JSON.stringify(s);
      option.textContent = `${s.display_name} (${s.host}:${s.port})`;
      select.appendChild(option);
    });
    setMsg($("connect-msg"), services.length ? `${services.length} database service(s) found` : "no database services found; type the host", services.length ? "ok" : "");
    if (services.length === 1) {
      select.selectedIndex = 1;
      select.dispatchEvent(new Event("change"));
    }
  } catch (err) {
    setMsg($("connect-msg"), err.message, "error");
  }
}

$("service-select").addEventListener("change", (e) => {
  if (!e.target.value) return;
  const s = JSON.parse(e.target.value);
  $("host").value = s.host;
  $("port").value = s.port;
  $("dbname").value = s.database || "postgres";
  $("user").value = s.user || "cloud_admin";
  $("password").focus();
});
$("connect-btn").addEventListener("click", connect);
$("refresh-services").addEventListener("click", findServices);
$("password").addEventListener("keydown", (e) => e.key === "Enter" && connect());

/* ---------- schema browser */

async function loadSchemas() {
  const { schemas } = await api("/api/schemas", withConnection({}));
  state.schemas = schemas;
  const list = $("schema-list");
  list.innerHTML = "";
  schemas.forEach((schema) => {
    const li = document.createElement("li");
    li.className = "schema";
    const label = document.createElement("span");
    label.textContent = `${schema.name} `;
    const count = document.createElement("small");
    count.className = "muted";
    count.textContent = `(${schema.relations})`;
    label.appendChild(count);
    const children = document.createElement("ul");
    children.hidden = true;
    label.addEventListener("click", async () => {
      const open = li.classList.toggle("open");
      children.hidden = !open;
      if (open && !children.dataset.loaded) await loadRelations(schema.name, children);
    });
    label.addEventListener("contextmenu", (e) => {
      e.preventDefault();
      if (confirm(`Generate SQL to drop schema "${schema.name}"?`)) action("drop_schema", { name: schema.name, cascade: true });
    });
    li.append(label, children);
    list.appendChild(li);
  });
}

async function loadRelations(schema, container) {
  container.innerHTML = "<li class='muted'>loading…</li>";
  try {
    const { relations } = await api("/api/relations", withConnection({ schema }));
    container.innerHTML = "";
    container.dataset.loaded = "1";
    if (!relations.length) container.innerHTML = "<li class='muted'>empty</li>";
    relations.forEach((rel) => {
      const li = document.createElement("li");
      li.className = "rel";
      li.textContent = rel.name;
      const small = document.createElement("small");
      small.textContent = rel.kind === "table" ? `~${rel.estimated_rows < 0 ? "?" : rel.estimated_rows} · ${rel.size}` : rel.kind;
      li.appendChild(small);
      li.addEventListener("click", () => openRelation(schema, rel.name, li));
      container.appendChild(li);
    });
  } catch (err) {
    container.innerHTML = `<li class='msg error'>${err.message}</li>`;
  }
}

async function refreshSchemas() {
  const openNames = [...document.querySelectorAll("#schema-list .schema.open > span")].map((s) => s.firstChild.textContent.trim());
  await loadSchemas();
  for (const li of document.querySelectorAll("#schema-list .schema")) {
    const name = li.querySelector("span").firstChild.textContent.trim();
    if (openNames.includes(name)) li.querySelector("span").click();
  }
}

/* ---------- relation details */

async function openRelation(schema, name, li) {
  document.querySelectorAll(".tree .rel.active").forEach((el) => el.classList.remove("active"));
  if (li) li.classList.add("active");
  showTab("relation");
  try {
    const rel = await api("/api/relation", withConnection({ schema, name, sample: 50 }));
    state.current = rel;
    $("relation-empty").hidden = true;
    $("relation-view").hidden = false;
    $("relation-title").textContent = `${rel.schema}.${rel.name}`;
    $("relation-meta").textContent = [rel.kind, `owner ${rel.owner}`, rel.size, rel.estimated_rows >= 0 ? `~${rel.estimated_rows} rows` : null, rel.comment]
      .filter(Boolean)
      .join(" · ");
    renderRecords($("relation-columns"), rel.columns, ["name", "type", "nullable", "default", "primary_key", "comment"]);
    renderRecords($("relation-indexes"), rel.indexes, ["name", "unique", "primary_key", "size", "definition"]);
    renderRecords($("relation-constraints"), rel.constraints, ["name", "type", "definition"]);
    $("relation-definition-wrap").hidden = !rel.definition;
    $("relation-definition").textContent = rel.definition || "";
    renderTable($("relation-sample"), rel.sample.columns, rel.sample.rows);
    const isTable = rel.kind.endsWith("table");
    document.querySelectorAll("#relation-actions button").forEach((b) => {
      b.hidden = !isTable && ["add-column", "create-index", "truncate"].includes(b.dataset.action);
    });
  } catch (err) {
    $("relation-empty").hidden = false;
    $("relation-empty").textContent = err.message;
    $("relation-view").hidden = true;
  }
}

/* ---------- queries */

async function runQuery() {
  const sql = $("sql").value.trim();
  if (!sql) return;
  const limit = Number($("limit").value) || 1000;
  setMsg($("query-msg"), "running…");
  $("run-btn").disabled = true;
  try {
    const result = await api("/api/query", withConnection({ sql, limit }));
    renderTable($("results"), result.columns, result.rows);
    const parts = [result.status, `${result.duration_ms} ms`];
    if (result.columns.length) parts.push(`${result.rows.length} row(s)${result.truncated ? ", truncated" : ""}`);
    setMsg($("query-msg"), parts.filter(Boolean).join(" · "), "ok");
    if (!result.columns.length) refreshSchemas(); // DDL or DML without a result set may have changed metadata
  } catch (err) {
    $("results").innerHTML = "";
    setMsg($("query-msg"), err.message, "error");
  } finally {
    $("run-btn").disabled = false;
  }
}

$("run-btn").addEventListener("click", runQuery);
$("sql").addEventListener("keydown", (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
    e.preventDefault();
    runQuery();
  }
});

/* ---------- management actions: a dialog collects parameters, the server renders the SQL */

function field(label, name, attrs = {}) {
  const wrap = document.createElement("label");
  wrap.textContent = label + " ";
  let input;
  if (attrs.type === "checkbox") {
    input = document.createElement("input");
    input.type = "checkbox";
    input.checked = Boolean(attrs.value);
  } else if (attrs.options) {
    input = document.createElement("select");
    attrs.options.forEach((o) => {
      const opt = document.createElement("option");
      opt.value = opt.textContent = o;
      input.appendChild(opt);
    });
    if (attrs.value) input.value = attrs.value;
  } else {
    input = document.createElement("input");
    input.value = attrs.value || "";
    if (attrs.placeholder) input.placeholder = attrs.placeholder;
  }
  input.name = name;
  wrap.appendChild(input);
  return wrap;
}

function openDialog(title, fields, onOk) {
  const body = $("dialog-body");
  body.innerHTML = "";
  fields.forEach((f) => body.appendChild(f));
  $("dialog-title").textContent = title;
  const dialog = $("dialog");
  const form = $("dialog-form");
  form.onsubmit = (e) => {
    e.preventDefault();
    const values = {};
    form.querySelectorAll("input, select").forEach((i) => {
      if (!i.name) return;
      values[i.name] = i.type === "checkbox" ? i.checked : i.value;
    });
    dialog.close();
    onOk(values);
  };
  $("dialog-cancel").onclick = () => dialog.close();
  dialog.showModal();
  const first = body.querySelector("input, select");
  if (first) first.focus();
}

async function action(name, params) {
  try {
    const { sql } = await api("/api/action", { action: name, params });
    putSql(sql);
  } catch (err) {
    alert(err.message);
  }
}

let typeOptions = ["text", "integer", "bigint", "numeric", "boolean", "timestamptz", "jsonb"];
api("/api/types").then((d) => (typeOptions = d.types)).catch(() => {});

$("new-schema").addEventListener("click", () => {
  openDialog("Create schema", [field("Name", "name", { placeholder: "analytics" })], (v) => action("create_schema", { name: v.name }));
});

function newTableDialog(schema) {
  const rows = [];
  const body = document.createElement("div");
  const nameField = field("Table name", "name", { placeholder: "events" });
  const grid = document.createElement("div");
  grid.className = "cols";
  ["column", "type", "pk", "not null", "default"].forEach((h) => {
    const s = document.createElement("strong");
    s.textContent = h;
    grid.appendChild(s);
  });
  const addRow = (values = {}) => {
    const name = Object.assign(document.createElement("input"), { placeholder: "name", value: values.name || "" });
    const type = document.createElement("select");
    typeOptions.forEach((t) => type.appendChild(Object.assign(document.createElement("option"), { value: t, textContent: t })));
    if (values.type) type.value = values.type;
    const pk = Object.assign(document.createElement("input"), { type: "checkbox", checked: Boolean(values.pk) });
    const nn = Object.assign(document.createElement("input"), { type: "checkbox", checked: Boolean(values.nn) });
    const def = Object.assign(document.createElement("input"), { placeholder: "", value: values.def || "" });
    [name, type, pk, nn, def].forEach((el) => grid.appendChild(el));
    rows.push({ name, type, pk, nn, def });
  };
  addRow({ name: "id", type: "bigserial", pk: true });
  addRow();
  const more = Object.assign(document.createElement("button"), { type: "button", className: "secondary small", textContent: "+ column" });
  more.addEventListener("click", () => addRow());
  body.append(nameField, grid, more);
  openDialog(`Create table in ${schema}`, [body], () => {
    const columns = rows
      .filter((r) => r.name.value.trim())
      .map((r) => ({ name: r.name.value.trim(), type: r.type.value, primary_key: r.pk.checked, nullable: !r.nn.checked, default: r.def.value }));
    action("create_table", { schema, name: nameField.querySelector("input").value.trim(), columns });
  });
}

document.querySelectorAll("#relation-actions button").forEach((button) =>
  button.addEventListener("click", () => {
    const rel = state.current;
    if (!rel) return;
    const q = (s) => '"' + s.replace(/"/g, '""') + '"';
    switch (button.dataset.action) {
      case "select":
        return putSql(`select * from ${q(rel.schema)}.${q(rel.name)} limit 100;`, true);
      case "drop":
        return openDialog(`Drop ${rel.kind} ${rel.schema}.${rel.name}`, [field("Cascade", "cascade", { type: "checkbox" })], (v) =>
          action("drop_relation", { schema: rel.schema, name: rel.name, kind: rel.kind, cascade: v.cascade })
        );
      case "truncate":
        return action("truncate_table", { schema: rel.schema, name: rel.name });
      case "rename":
        return openDialog("Rename", [field("New name", "new_name", { value: rel.name })], (v) =>
          action("rename_relation", { schema: rel.schema, name: rel.name, new_name: v.new_name, kind: rel.kind })
        );
      case "comment":
        return openDialog("Comment", [field("Comment", "comment", { value: rel.comment || "" })], (v) =>
          action("set_comment", { schema: rel.schema, name: rel.name, comment: v.comment })
        );
      case "add-column":
        return openDialog(
          "Add column",
          [
            field("Name", "column"),
            field("Type", "col_type", { options: typeOptions }),
            field("Allow null", "nullable", { type: "checkbox", value: true }),
            field("Default", "default"),
          ],
          (v) => action("add_column", { schema: rel.schema, name: rel.name, column: v.column, col_type: v.col_type, nullable: v.nullable, default: v.default })
        );
      case "create-index":
        return openDialog(
          "Create index",
          [
            field("Columns (comma separated)", "columns", { value: rel.columns[0] ? rel.columns[0].name : "" }),
            field("Index name (optional)", "index_name"),
            field("Unique", "unique", { type: "checkbox" }),
          ],
          (v) =>
            action("create_index", {
              schema: rel.schema,
              name: rel.name,
              columns: v.columns.split(",").map((c) => c.trim()).filter(Boolean),
              index_name: v.index_name,
              unique: v.unique,
            })
        );
    }
  })
);

// Column-level actions: click a column row to drop, rename or comment it.
$("relation-columns").addEventListener("click", (e) => {
  const tr = e.target.closest("tbody tr");
  const rel = state.current;
  if (!tr || !rel) return;
  const column = tr.cells[0].textContent;
  openDialog(
    `Column ${column}`,
    [field("Action", "what", { options: ["rename", "comment", "drop"] }), field("New name or comment", "value", { value: column })],
    (v) => {
      if (v.what === "drop") action("drop_column", { schema: rel.schema, name: rel.name, column });
      else if (v.what === "rename") action("rename_column", { schema: rel.schema, name: rel.name, column, new_name: v.value });
      else action("set_comment", { schema: rel.schema, name: rel.name, column, comment: v.value });
    }
  );
});

// Index rows: click to drop.
$("relation-indexes").addEventListener("click", (e) => {
  const tr = e.target.closest("tbody tr");
  const rel = state.current;
  if (!tr || !rel) return;
  const index = tr.cells[0].textContent;
  if (confirm(`Generate SQL to drop index "${index}"?`)) action("drop_index", { schema: rel.schema, index_name: index });
});

// Schema label double-click: create a table in it.
$("schema-list").addEventListener("dblclick", (e) => {
  const label = e.target.closest(".schema > span");
  if (label) newTableDialog(label.firstChild.textContent.trim());
});

/* ---------- server tab */

async function loadDatabases() {
  const { databases } = await api("/api/databases", withConnection({}));
  renderTable($("databases"), ["name", "owner", "size"], databases.map((d) => [d.name, d.owner, d.size]), {
    onClick: (row) => {
      $("dbname").value = row[0];
      connect();
    },
  });
}

/* ---------- audit tab */

const AUDIT_COLUMNS = ["time", "platform_user", "user", "database", "host", "kind", "sql"];
let auditRows = [];

function renderAudit() {
  const needle = $("audit-filter").value.trim().toLowerCase();
  const rows = needle
    ? auditRows.filter((r) => AUDIT_COLUMNS.some((c) => String(r[c] ?? "").toLowerCase().includes(needle)))
    : auditRows;
  renderTable($("audit-rows"), AUDIT_COLUMNS, rows.map((r) => AUDIT_COLUMNS.map((c) => r[c])));
  setMsg($("audit-msg"), `${rows.length} of ${auditRows.length} statement(s)`, "ok");
}

async function loadAudit() {
  if (!state.connection) {
    setMsg($("audit-msg"), "connect first", "error");
    return;
  }
  // The service log is read by job name, which is the hostname of a platform service.
  const service = state.connection.host;
  setMsg($("audit-msg"), `reading the log of ${service}…`);
  try {
    const { statements } = await api("/api/audit", { service, lines: Number($("audit-lines").value) || 2000 });
    auditRows = statements;
    renderAudit();
  } catch (err) {
    auditRows = [];
    renderTable($("audit-rows"), [], []);
    setMsg($("audit-msg"), err.message, "error");
  }
}

$("audit-refresh").addEventListener("click", loadAudit);
$("audit-filter").addEventListener("input", renderAudit);

/* ---------- tabs and startup */

document.querySelectorAll("#tabs button").forEach((b) =>
  b.addEventListener("click", () => {
    showTab(b.dataset.tab);
    if (b.dataset.tab === "audit" && !auditRows.length) loadAudit();
  })
);

(function start() {
  const saved = sessionStorage.getItem("neon-explorer-connection");
  if (saved) {
    const c = JSON.parse(saved);
    $("host").value = c.host;
    $("port").value = c.port;
    $("dbname").value = c.dbname;
    $("user").value = c.user;
    $("password").value = c.password;
    connect();
  } else {
    findServices();
  }
})();
