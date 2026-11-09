/* Neon Postgres Explorer front end.
 * The connection details live in this page (and sessionStorage) and are sent
 * with every API call. Management actions ask the server for the SQL and show
 * it in a confirmation dialog before anything runs. The data grid edits rows
 * through parameterised endpoints. */

const $ = (id) => document.getElementById(id);
const state = { connection: null, current: null, schemas: [], lastResult: null, grid: null, platformUser: null, roleSecret: false };
const HISTORY_KEY = "neon-explorer-history";
const HISTORY_SIZE = 30;
const DESTRUCTIVE = ["drop_schema", "drop_relation", "truncate_table", "drop_column", "drop_index"];

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

function display(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function fillCell(td, value) {
  td.className = "";
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

function cell(value) {
  return fillCell(document.createElement("td"), value);
}

function renderTable(container, columns, rows, options = {}) {
  container.innerHTML = "";
  if (!columns.length) return;
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

function showSubtab(name) {
  document.querySelectorAll(".subtabs button").forEach((b) => b.classList.toggle("active", b.dataset.subtab === name));
  $("subtab-data").hidden = name !== "data";
  $("subtab-structure").hidden = name !== "structure";
}

function putSql(sql, run = false) {
  $("sql").value = sql;
  showTab("query");
  $("sql").focus();
  if (run) runQuery();
}

function quoteIdent(s) {
  return '"' + s.replace(/"/g, '""') + '"';
}

/* ---------- CSV export (client side, from the rows already loaded) */

function toCsv(columns, rows) {
  const escape = (v) => {
    const text = display(v);
    return /[",\n\r]/.test(text) ? '"' + text.replace(/"/g, '""') + '"' : text;
  };
  return [columns, ...rows].map((r) => r.map(escape).join(",")).join("\r\n") + "\r\n";
}

function downloadCsv(name, columns, rows) {
  const blob = new Blob([toCsv(columns, rows)], { type: "text/csv;charset=utf-8" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = name.replace(/[^\w.-]+/g, "_") + ".csv";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(a.href);
}

/* ---------- connection */

function readConnectionForm() {
  const asUser = $("as-user").checked && !$("as-user-row").hidden;
  return {
    host: $("host").value.trim(),
    port: Number($("port").value) || 5432,
    dbname: $("dbname").value.trim() || "postgres",
    user: asUser ? "" : $("user").value.trim() || "cloud_admin",
    password: asUser ? "" : $("password").value,
    as_user: asUser,
  };
}

function updateCredentialFields() {
  $("credentials").hidden = $("as-user").checked && !$("as-user-row").hidden;
}

/* Ask the app who is using it. On the platform the answer is the signed-in
 * user, and "connect as me" is offered when the app also holds the role secret
 * the service derives role passwords from. */
async function loadWhoami() {
  try {
    const me = await api("/api/whoami");
    state.platformUser = me.user;
    state.roleSecret = me.role_secret;
  } catch (err) {
    state.platformUser = null;
  }
  const offer = Boolean(state.platformUser && state.roleSecret);
  $("as-user-row").hidden = !offer;
  if (offer) {
    $("as-user-label").textContent = `Connect as me (${state.platformUser})`;
    $("as-user-label").title = "Use the Postgres role the service created for your platform user";
  }
  updateCredentialFields();
}

async function connect() {
  const connection = readConnectionForm();
  setMsg($("connect-msg"), "connecting…");
  $("connect-btn").disabled = true;
  try {
    const info = await api("/api/connect", { connection });
    state.connection = connection;
    sessionStorage.setItem("neon-explorer-connection", JSON.stringify(connection));
    $("conn-status").textContent = `${info.user}@${connection.host}:${connection.port}/${info.database}`;
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
  // A service that syncs platform users into roles is best used as yourself.
  if (!$("as-user-row").hidden) $("as-user").checked = Boolean(s.role_sync);
  updateCredentialFields();
  if (!$("credentials").hidden) $("password").focus();
});
$("as-user").addEventListener("change", updateCredentialFields);
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
    li.dataset.name = schema.name;
    const label = document.createElement("span");
    label.textContent = `${schema.name} `;
    const count = document.createElement("small");
    count.className = "muted";
    count.textContent = `(${schema.relations})`;
    label.appendChild(count);
    const tools = document.createElement("span");
    tools.className = "tools";
    const add = Object.assign(document.createElement("button"), { className: "small secondary", textContent: "+ table", title: "Create a table in this schema" });
    add.addEventListener("click", (e) => {
      e.stopPropagation();
      newTableDialog(schema.name);
    });
    const drop = Object.assign(document.createElement("button"), { className: "small danger", textContent: "drop", title: "Drop this schema" });
    drop.addEventListener("click", (e) => {
      e.stopPropagation();
      openDialog(`Drop schema ${schema.name}`, [field("Cascade (drop everything inside)", "cascade", { type: "checkbox" })], (v) =>
        action("drop_schema", { name: schema.name, cascade: v.cascade })
      );
    });
    tools.append(add, drop);
    const children = document.createElement("ul");
    children.hidden = true;
    label.addEventListener("click", async () => {
      const open = li.classList.toggle("open");
      children.hidden = !open;
      if (open && !children.dataset.loaded) await loadRelations(schema.name, children);
    });
    li.append(label, tools, children);
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
  const openNames = [...document.querySelectorAll("#schema-list .schema.open")].map((li) => li.dataset.name);
  await loadSchemas();
  for (const li of document.querySelectorAll("#schema-list .schema")) {
    if (openNames.includes(li.dataset.name)) li.querySelector("span").click();
  }
}

/* ---------- relation details */

async function openRelation(schema, name, li) {
  document.querySelectorAll(".tree .rel.active").forEach((el) => el.classList.remove("active"));
  if (li) li.classList.add("active");
  showTab("relation");
  try {
    const rel = await api("/api/relation", withConnection({ schema, name, sample: 0 }));
    state.current = rel;
    $("relation-empty").hidden = true;
    $("relation-view").hidden = false;
    $("relation-ddl-wrap").hidden = true;
    $("relation-title").textContent = `${rel.schema}.${rel.name}`;
    $("relation-meta").textContent = [rel.kind, `owner ${rel.owner}`, rel.size, rel.estimated_rows >= 0 ? `~${rel.estimated_rows} rows` : null, rel.comment]
      .filter(Boolean)
      .join(" · ");
    renderRecords($("relation-columns"), rel.columns, ["name", "type", "nullable", "default", "primary_key", "comment"]);
    renderRecords($("relation-indexes"), rel.indexes, ["name", "unique", "primary_key", "size", "definition"]);
    renderRecords($("relation-constraints"), rel.constraints, ["name", "type", "definition"]);
    $("relation-definition-wrap").hidden = !rel.definition;
    $("relation-definition").textContent = rel.definition || "";
    const isTable = rel.kind.endsWith("table");
    document.querySelectorAll("#relation-actions button").forEach((b) => {
      b.hidden = !isTable && ["add-column", "create-index", "truncate"].includes(b.dataset.action);
    });
    showSubtab("data");
    resetGrid(rel);
    await loadRows();
  } catch (err) {
    $("relation-empty").hidden = false;
    $("relation-empty").textContent = err.message;
    $("relation-view").hidden = true;
  }
}

async function reopenCurrent() {
  const rel = state.current;
  if (rel) await openRelation(rel.schema, rel.name, document.querySelector(".tree .rel.active"));
}

/* ---------- data grid */

function resetGrid(rel) {
  state.grid = { schema: rel.schema, name: rel.name, filters: [], sort: [], offset: 0, selected: new Set(), page: null };
  const columnSelect = $("filter-column");
  columnSelect.innerHTML = "";
  rel.columns.forEach((c) => columnSelect.appendChild(Object.assign(document.createElement("option"), { value: c.name, textContent: c.name })));
  $("filter-value").value = "";
  renderChips();
}

function renderChips() {
  const wrap = $("filter-chips");
  wrap.innerHTML = "";
  state.grid.filters.forEach((f, i) => {
    const chip = document.createElement("span");
    chip.className = "chip";
    chip.textContent = f.op.endsWith("null") ? `${f.column} ${f.op}` : `${f.column} ${f.op} ${f.value}`;
    chip.title = "remove this filter";
    chip.addEventListener("click", () => {
      state.grid.filters.splice(i, 1);
      state.grid.offset = 0;
      renderChips();
      loadRows();
    });
    wrap.appendChild(chip);
  });
}

function pageSize() {
  return Math.max(1, Math.min(Number($("page-size").value) || 100, 1000));
}

async function loadRows() {
  const g = state.grid;
  if (!g) return;
  setMsg($("grid-msg"), "loading…");
  try {
    const page = await api("/api/rows", withConnection({ schema: g.schema, name: g.name, filters: g.filters, sort: g.sort, limit: pageSize(), offset: g.offset }));
    g.page = page;
    g.selected.clear();
    renderGrid(page);
    const from = page.total ? page.offset + 1 : 0;
    const to = page.offset + page.rows.length;
    $("page-info").textContent = `${from}–${to} of ${page.total}`;
    $("prev-page").disabled = page.offset === 0;
    $("next-page").disabled = to >= page.total;
    $("insert-row").disabled = !page.editable;
    setMsg($("grid-msg"), page.editable ? "" : "read only: not a table", page.editable ? "" : "warn");
  } catch (err) {
    $("grid").innerHTML = "";
    setMsg($("grid-msg"), err.message, "error");
  }
  updateDeleteButton();
}

function updateDeleteButton() {
  const g = state.grid;
  const n = g && g.page && g.page.editable ? g.selected.size : 0;
  $("delete-rows").disabled = n === 0;
  $("delete-rows").textContent = n ? `Delete ${n} selected` : "Delete selected";
}

function renderGrid(page) {
  const g = state.grid;
  const container = $("grid");
  container.innerHTML = "";
  const table = document.createElement("table");
  const head = table.createTHead().insertRow();
  if (page.editable) {
    const th = document.createElement("th");
    th.className = "sel";
    const all = Object.assign(document.createElement("input"), { type: "checkbox", title: "select all rows on this page" });
    all.addEventListener("change", () => {
      table.querySelectorAll("tbody input[type=checkbox]").forEach((cb) => {
        cb.checked = all.checked;
        cb.dispatchEvent(new Event("change"));
      });
    });
    th.appendChild(all);
    head.appendChild(th);
  }
  page.columns.forEach((c) => {
    const th = document.createElement("th");
    th.textContent = c;
    const sorted = g.sort.find((s) => s.column === c);
    if (sorted) th.className = "sorted" + (sorted.desc ? " desc" : "");
    th.title = "click to sort";
    th.addEventListener("click", () => {
      const current = g.sort[0];
      g.sort = current && current.column === c && !current.desc ? [{ column: c, desc: true }] : current && current.column === c ? [] : [{ column: c, desc: false }];
      g.offset = 0;
      loadRows();
    });
    head.appendChild(th);
  });
  const tbody = table.createTBody();
  page.rows.forEach((row, r) => {
    const tr = tbody.insertRow();
    tr.dataset.index = r;
    if (page.editable) {
      const td = document.createElement("td");
      td.className = "sel";
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.addEventListener("change", () => {
        const id = page.ids[Number(tr.dataset.index)];
        if (cb.checked) g.selected.add(id);
        else g.selected.delete(id);
        tr.classList.toggle("selected", cb.checked);
        updateDeleteButton();
      });
      td.appendChild(cb);
      tr.appendChild(td);
    }
    row.forEach((v, c) => {
      const td = cell(v);
      if (page.editable) {
        td.classList.add("editable");
        td.title = (td.title ? td.title + "\n" : "") + "click to edit";
        td.addEventListener("click", () => editCell(td, tr, c));
      }
      tr.appendChild(td);
    });
  });
  container.appendChild(table);
}

function editCell(td, tr, columnIndex) {
  if (td.classList.contains("editing")) return;
  const g = state.grid;
  const page = g.page;
  const rowIndex = Number(tr.dataset.index);
  const original = page.rows[rowIndex][columnIndex];
  const column = page.columns[columnIndex];
  td.classList.add("editing");
  td.textContent = "";
  const input = document.createElement("input");
  input.value = display(original);
  input.title = "Enter saves, Escape cancels, Ctrl+Shift+N sets null";
  let done = false;
  const finish = async (value) => {
    if (done) return;
    done = true;
    const unchanged = (value === null && original === null) || (value !== null && original !== null && value === display(original));
    if (unchanged) {
      fillCell(td, original);
      td.classList.add("editable");
      return;
    }
    try {
      const result = await api("/api/rows/update", withConnection({ schema: g.schema, name: g.name, id: page.ids[rowIndex], values: { [column]: value } }));
      page.ids[rowIndex] = result.id;
      page.rows[rowIndex] = result.row;
      [...tr.cells].slice(page.editable ? 1 : 0).forEach((c, i) => {
        fillCell(c, result.row[i]);
        c.classList.add("editable");
      });
      td.classList.add("changed");
      setMsg($("grid-msg"), `updated ${column}`, "ok");
    } catch (err) {
      fillCell(td, original);
      td.classList.add("editable");
      setMsg($("grid-msg"), err.message, "error");
    }
  };
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter") finish(input.value);
    else if (e.key === "Escape") finish(original === null ? null : display(original));
    else if (e.key.toLowerCase() === "n" && e.ctrlKey && e.shiftKey) {
      e.preventDefault();
      finish(null);
    }
  });
  input.addEventListener("blur", () => finish(input.value));
  td.appendChild(input);
  input.focus();
  input.select();
}

function insertRowDialog() {
  const rel = state.current;
  const g = state.grid;
  if (!rel || !g) return;
  const grid = document.createElement("div");
  grid.className = "fields";
  const inputs = rel.columns.map((col) => {
    const label = document.createElement("label");
    label.textContent = col.name;
    const input = document.createElement("input");
    input.placeholder = col.default ? `default: ${col.default}` : col.nullable ? "null" : col.type;
    input.title = col.type;
    const nullBox = document.createElement("label");
    nullBox.className = "hint";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    nullBox.append(cb, " null");
    grid.append(label, input, nullBox);
    return { col, input, cb };
  });
  $("dialog").classList.add("wide");
  openDialog(`Insert into ${rel.schema}.${rel.name}`, [grid], async () => {
    const values = {};
    inputs.forEach(({ col, input, cb }) => {
      if (cb.checked) values[col.name] = null;
      else if (input.value !== "") values[col.name] = input.value;
    });
    try {
      await api("/api/rows/insert", withConnection({ schema: g.schema, name: g.name, values }));
      await loadRows();
      setMsg($("grid-msg"), "row inserted", "ok");
    } catch (err) {
      setMsg($("grid-msg"), err.message, "error");
    }
  });
}

async function deleteSelected() {
  const g = state.grid;
  const ids = [...g.selected];
  if (!ids.length) return;
  confirmSql(
    `Delete ${ids.length} row(s) from ${g.schema}.${g.name}`,
    `delete from ${quoteIdent(g.schema)}.${quoteIdent(g.name)} where ctid = any($1::tid[]);\n-- $1 = ${JSON.stringify(ids)}`,
    "This cannot be undone.",
    async () => {
      try {
        const { deleted } = await api("/api/rows/delete", withConnection({ schema: g.schema, name: g.name, ids }));
        await loadRows();
        setMsg($("grid-msg"), `${deleted} row(s) deleted`, "ok");
      } catch (err) {
        setMsg($("grid-msg"), err.message, "error");
      }
    },
    false
  );
}

$("add-filter").addEventListener("click", () => {
  const g = state.grid;
  if (!g) return;
  const op = $("filter-op").value;
  g.filters.push({ column: $("filter-column").value, op, value: op.endsWith("null") ? null : $("filter-value").value });
  g.offset = 0;
  $("filter-value").value = "";
  renderChips();
  loadRows();
});
$("filter-value").addEventListener("keydown", (e) => e.key === "Enter" && $("add-filter").click());
$("filter-op").addEventListener("change", () => ($("filter-value").disabled = $("filter-op").value.endsWith("null")));
$("insert-row").addEventListener("click", insertRowDialog);
$("delete-rows").addEventListener("click", deleteSelected);
$("reload-rows").addEventListener("click", loadRows);
$("export-rows").addEventListener("click", () => {
  const g = state.grid;
  if (g && g.page) downloadCsv(`${g.schema}.${g.name}`, g.page.columns, g.page.rows);
});
$("prev-page").addEventListener("click", () => {
  state.grid.offset = Math.max(0, state.grid.offset - pageSize());
  loadRows();
});
$("next-page").addEventListener("click", () => {
  state.grid.offset += pageSize();
  loadRows();
});
$("page-size").addEventListener("change", () => {
  state.grid.offset = 0;
  loadRows();
});
["=", "<>", "<", "<=", ">", ">=", "like", "ilike", "is null", "is not null"].forEach((op) =>
  $("filter-op").appendChild(Object.assign(document.createElement("option"), { value: op, textContent: op }))
);
document.querySelectorAll(".subtabs button").forEach((b) => b.addEventListener("click", () => showSubtab(b.dataset.subtab)));

/* ---------- queries and history */

function loadHistory() {
  try {
    return JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]");
  } catch {
    return [];
  }
}

function renderHistory() {
  const select = $("history");
  select.innerHTML = '<option value="">history</option>';
  loadHistory().forEach((sql, i) => {
    const option = document.createElement("option");
    option.value = String(i);
    option.textContent = sql.replace(/\s+/g, " ").slice(0, 80);
    option.title = sql;
    select.appendChild(option);
  });
}

function remember(sql) {
  const history = loadHistory().filter((s) => s !== sql);
  history.unshift(sql);
  localStorage.setItem(HISTORY_KEY, JSON.stringify(history.slice(0, HISTORY_SIZE)));
  renderHistory();
}

$("history").addEventListener("change", (e) => {
  const sql = loadHistory()[Number(e.target.value)];
  if (sql !== undefined) $("sql").value = sql;
  e.target.value = "";
});

async function runQuery(sqlText) {
  const sql = (sqlText || $("sql").value).trim();
  if (!sql) return;
  const limit = Number($("limit").value) || 1000;
  setMsg($("query-msg"), "running…");
  $("run-btn").disabled = true;
  try {
    const result = await api("/api/query", withConnection({ sql, limit }));
    remember(sql);
    state.lastResult = result;
    renderTable($("results"), result.columns, result.rows);
    $("export-query").hidden = !result.columns.length;
    const parts = [result.status, `${result.duration_ms} ms`];
    if (result.columns.length) parts.push(`${result.rows.length} row(s)${result.truncated ? ", truncated" : ""}`);
    setMsg($("query-msg"), parts.filter(Boolean).join(" · "), "ok");
    if (!result.columns.length) refreshSchemas(); // DDL or DML without a result set may have changed metadata
    return result;
  } catch (err) {
    $("results").innerHTML = "";
    $("export-query").hidden = true;
    setMsg($("query-msg"), err.message, "error");
    throw err;
  } finally {
    $("run-btn").disabled = false;
  }
}

$("run-btn").addEventListener("click", () => runQuery().catch(() => {}));
$("sql").addEventListener("keydown", (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
    e.preventDefault();
    runQuery().catch(() => {});
  }
});
$("export-query").addEventListener("click", () => {
  const r = state.lastResult;
  if (r && r.columns.length) downloadCsv("query-result", r.columns, r.rows);
});

/* ---------- dialogs: one collects parameters, the other confirms generated SQL */

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

function openDialog(title, fields, onOk, okLabel = "OK") {
  const body = $("dialog-body");
  body.innerHTML = "";
  fields.forEach((f) => body.appendChild(f));
  $("dialog-title").textContent = title;
  $("dialog-ok").textContent = okLabel;
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
    dialog.classList.remove("wide");
    onOk(values);
  };
  $("dialog-cancel").onclick = () => {
    dialog.close();
    dialog.classList.remove("wide");
  };
  dialog.showModal();
  const first = body.querySelector("input, select");
  if (first) first.focus();
}

function confirmSql(title, sql, note, onRun, allowEdit = true) {
  const dialog = $("confirm");
  $("confirm-title").textContent = title;
  $("confirm-sql").textContent = sql;
  setMsg($("confirm-note"), note, note ? "warn" : "");
  $("confirm-edit").hidden = !allowEdit;
  $("confirm-run").onclick = () => {
    dialog.close();
    onRun();
  };
  $("confirm-edit").onclick = () => {
    dialog.close();
    putSql(sql);
  };
  $("confirm-cancel").onclick = () => dialog.close();
  dialog.showModal();
  $("confirm-cancel").focus();
}

async function action(name, params) {
  let sql;
  try {
    sql = (await api("/api/action", { action: name, params })).sql;
  } catch (err) {
    alert(err.message);
    return;
  }
  const destructive = DESTRUCTIVE.includes(name);
  confirmSql(
    destructive ? "This statement destroys data" : "Run this statement?",
    sql,
    destructive ? "This cannot be undone. Read the statement before you run it." : "",
    async () => {
      try {
        await runQuery(sql);
        showTab("relation");
        if (name === "drop_relation" || name === "drop_schema") {
          state.current = null;
          $("relation-view").hidden = true;
          $("relation-empty").hidden = false;
          $("relation-empty").textContent = "Pick a table or view in the sidebar.";
        } else if (state.current) {
          await reopenCurrent();
        }
      } catch {
        showTab("query"); // the editor shows the error and keeps the statement
      }
    }
  );
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
  $("dialog").classList.add("wide");
  openDialog(`Create table in ${schema}`, [body], () => {
    const columns = rows
      .filter((r) => r.name.value.trim())
      .map((r) => ({ name: r.name.value.trim(), type: r.type.value, primary_key: r.pk.checked, nullable: !r.nn.checked, default: r.def.value }));
    action("create_table", { schema, name: nameField.querySelector("input").value.trim(), columns });
  });
}

async function showDdl() {
  const rel = state.current;
  if (!rel) return;
  try {
    const { sql } = await api("/api/ddl", withConnection({ schema: rel.schema, name: rel.name }));
    $("relation-ddl").textContent = sql;
    $("relation-ddl-wrap").hidden = false;
  } catch (err) {
    alert(err.message);
  }
}
$("copy-ddl").addEventListener("click", () => navigator.clipboard.writeText($("relation-ddl").textContent));
$("hide-ddl").addEventListener("click", () => ($("relation-ddl-wrap").hidden = true));

document.querySelectorAll("#relation-actions button").forEach((button) =>
  button.addEventListener("click", () => {
    const rel = state.current;
    if (!rel) return;
    switch (button.dataset.action) {
      case "select":
        return putSql(`select * from ${quoteIdent(rel.schema)}.${quoteIdent(rel.name)} limit 100;`, true);
      case "ddl":
        return showDdl();
      case "drop":
        return openDialog(`Drop ${rel.kind} ${rel.schema}.${rel.name}`, [field("Cascade (drop dependent objects too)", "cascade", { type: "checkbox" })], (v) =>
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

// Column rows in the structure view: rename, comment or drop the column.
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

// Index rows: drop.
$("relation-indexes").addEventListener("click", (e) => {
  const tr = e.target.closest("tbody tr");
  const rel = state.current;
  if (!tr || !rel) return;
  action("drop_index", { schema: rel.schema, index_name: tr.cells[0].textContent });
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

/* ---------- tabs and startup */

document.querySelectorAll("#tabs button").forEach((b) => b.addEventListener("click", () => showTab(b.dataset.tab)));

(async function start() {
  renderHistory();
  await loadWhoami();
  const saved = sessionStorage.getItem("neon-explorer-connection");
  if (saved) {
    const c = JSON.parse(saved);
    $("host").value = c.host;
    $("port").value = c.port;
    $("dbname").value = c.dbname;
    $("as-user").checked = Boolean(c.as_user);
    if (!c.as_user) {
      $("user").value = c.user;
      $("password").value = c.password;
    }
    updateCredentialFields();
    connect();
  } else {
    findServices();
  }
})();
