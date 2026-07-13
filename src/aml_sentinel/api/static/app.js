/* AML Sentinel demo UI: live alert feed (websocket), case detail with
   evidence subgraph (plain SVG, circular layout), dispositions, stats. */

const TYPOLOGY_COLORS = {
  cycle: "var(--cycle)",
  structuring: "var(--structuring)",
  high_value_degree_outlier: "var(--degree)",
};
const colorFor = (t) => TYPOLOGY_COLORS[t] || "var(--text-muted)";
const $ = (id) => document.getElementById(id);

let selectedCaseId = null;
const knownCases = new Map(); // case_id -> summary

/* ---------- stats ---------- */

async function refreshStats() {
  const s = await (await fetch("/stats")).json();
  $("stat-total").textContent = s.total;
  $("stat-open").textContent = s.open;
  $("stat-tp").textContent = s.true_positive;
  $("stat-fp").textContent = s.false_positive;
}

/* ---------- alert queue ---------- */

function alertRow(c) {
  const li = document.createElement("li");
  li.dataset.caseId = c.case_id;
  li.classList.toggle("disposed", c.status === "disposed");
  if (c.case_id === selectedCaseId) li.classList.add("selected");

  const dot = document.createElement("span");
  dot.className = "dot";
  dot.style.background = colorFor(c.typology);

  const badge = document.createElement("span");
  badge.className = "badge";
  badge.textContent = `${c.typology} · ${c.accounts.length} accts`;

  const meta = document.createElement("span");
  meta.className = "meta";
  meta.textContent = `${c.score.toFixed(2)} · ${c.fired_at.slice(0, 16).replace("T", " ")}`;

  li.append(dot, badge, meta);
  if (c.disposition) {
    const v = document.createElement("span");
    v.className = `verdict ${c.disposition === "true_positive" ? "tp" : "fp"}`;
    v.textContent = c.disposition === "true_positive" ? "TP" : "FP";
    li.append(v);
  }
  li.onclick = () => selectCase(c.case_id);
  return li;
}

function upsertAlert(c, { prepend = false, flash = false } = {}) {
  knownCases.set(c.case_id, c);
  const list = $("alert-list");
  const existing = list.querySelector(`li[data-case-id="${c.case_id}"]`);
  const row = alertRow(c);
  if (flash) row.classList.add("flash");
  if (existing) existing.replaceWith(row);
  else if (prepend) list.prepend(row);
  else list.append(row);
  $("queue-empty").hidden = knownCases.size > 0;
}

async function loadAlerts() {
  const alerts = await (await fetch("/alerts?limit=200")).json();
  $("alert-list").replaceChildren();
  knownCases.clear();
  for (const c of alerts) upsertAlert(c);
}

/* ---------- case detail ---------- */

async function selectCase(caseId) {
  selectedCaseId = caseId;
  document
    .querySelectorAll("#alert-list li")
    .forEach((li) => li.classList.toggle("selected", Number(li.dataset.caseId) === caseId));
  const detail = await (await fetch(`/cases/${caseId}`)).json();
  renderCase(detail);
}

function renderCase(d) {
  $("case-empty").hidden = true;
  $("case-body").hidden = false;
  $("case-title").textContent = `Case #${d.case_id} — ${d.alert_id}`;

  const meta = $("case-meta");
  meta.replaceChildren();
  const items = [
    ["typology", d.typology],
    ["score", d.score.toFixed(2)],
    ["fired", d.fired_at.replace("T", " ")],
    ["status", d.status],
  ];
  if (d.disposition) items.push(["disposition", d.disposition.replace("_", " ")]);
  for (const [k, v] of items) {
    const span = document.createElement("span");
    const b = document.createElement("b");
    b.textContent = v;
    span.append(`${k}: `, b);
    meta.append(span);
  }

  renderSubgraph(d);
  renderEvidence(d.evidence.edges);

  const open = d.status === "open";
  $("btn-tp").disabled = !open;
  $("btn-fp").disabled = !open;
  $("dispose-state").textContent = open
    ? ""
    : `disposed as ${d.disposition.replace("_", " ")}${d.disposition_notes ? ` — ${d.disposition_notes}` : ""}`;
  $("btn-tp").onclick = () => dispose(d.case_id, "true_positive");
  $("btn-fp").onclick = () => dispose(d.case_id, "false_positive");
}

const SVG_NS = "http://www.w3.org/2000/svg";
const svgEl = (tag, attrs) => {
  const el = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
  return el;
};

function renderSubgraph(d) {
  const svg = $("subgraph");
  svg.replaceChildren();
  const W = 460, H = 320, cx = W / 2, cy = H / 2;
  const r = Math.min(W, H) / 2 - 42;
  const nodes = d.evidence.nodes;
  const pos = new Map(
    nodes.map((n, i) => {
      const a = (2 * Math.PI * i) / nodes.length - Math.PI / 2;
      return [n, [cx + r * Math.cos(a), cy + r * Math.sin(a)]];
    }),
  );

  const defs = svgEl("defs", {});
  const marker = svgEl("marker", {
    id: "arrow", viewBox: "0 0 10 10", refX: 9, refY: 5,
    markerWidth: 7, markerHeight: 7, orient: "auto-start-reverse",
  });
  const tip = svgEl("path", { d: "M 0 0 L 10 5 L 0 10 z" });
  tip.style.fill = "var(--text-secondary)";
  marker.append(tip);
  defs.append(marker);
  svg.append(defs);

  const NODE_R = 14;
  for (const e of d.evidence.edges) {
    const [x1, y1] = pos.get(e.src);
    const [x2, y2] = pos.get(e.dst);
    const dx = x2 - x1, dy = y2 - y1;
    const len = Math.hypot(dx, dy) || 1;
    // trim the line so the arrowhead lands on the node's rim
    const sx = x1 + (dx / len) * NODE_R, sy = y1 + (dy / len) * NODE_R;
    const ex = x2 - (dx / len) * (NODE_R + 3), ey = y2 - (dy / len) * (NODE_R + 3);
    const line = svgEl("line", {
      x1: sx, y1: sy, x2: ex, y2: ey, class: "sg-edge hot", "marker-end": "url(#arrow)",
    });
    const title = svgEl("title", {});
    title.textContent = `${e.tx_id}: ${e.src} → ${e.dst}  $${e.amount.toLocaleString()}`;
    line.append(title);
    svg.append(line);

    const label = svgEl("text", {
      x: (sx + ex) / 2 + (dy / len) * 10,
      y: (sy + ey) / 2 - (dx / len) * 10,
      class: "sg-amount",
    });
    label.textContent = `$${Math.round(e.amount).toLocaleString()}`;
    svg.append(label);
  }

  for (const n of nodes) {
    const [x, y] = pos.get(n);
    const circle = svgEl("circle", { cx: x, cy: y, r: NODE_R, class: "sg-node" });
    circle.style.stroke = colorFor(d.typology);
    const label = svgEl("text", { x, y: y + NODE_R + 13, class: "sg-label" });
    label.textContent = n;
    svg.append(circle, label);
  }
}

function renderEvidence(edges) {
  const tbody = $("evidence").querySelector("tbody");
  tbody.replaceChildren();
  for (const e of edges) {
    const tr = document.createElement("tr");
    for (const v of [
      e.tx_id, e.src, e.dst,
      `$${e.amount.toLocaleString(undefined, { minimumFractionDigits: 2 })}`,
      e.event_time.replace("T", " "),
    ]) {
      const td = document.createElement("td");
      td.textContent = v;
      tr.append(td);
    }
    tr.children[3].classList.add("num");
    tbody.append(tr);
  }
}

async function dispose(caseId, disposition) {
  const r = await fetch(`/cases/${caseId}/disposition`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ disposition }),
  });
  if (!r.ok) return;
  const detail = await r.json();
  renderCase(detail);
  upsertAlert({ ...knownCases.get(caseId), status: detail.status, disposition: detail.disposition });
  refreshStats();
}

/* ---------- live feed ---------- */

function connectWs() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws/alerts`);
  ws.onopen = () => {
    $("ws-status").textContent = "● live";
    $("ws-status").classList.add("live");
  };
  ws.onmessage = (ev) => {
    upsertAlert(JSON.parse(ev.data), { prepend: true, flash: true });
    refreshStats();
  };
  ws.onclose = () => {
    $("ws-status").textContent = "reconnecting…";
    $("ws-status").classList.remove("live");
    setTimeout(connectWs, 2000);
  };
}

loadAlerts().then(refreshStats);
connectWs();
setInterval(refreshStats, 10_000);
