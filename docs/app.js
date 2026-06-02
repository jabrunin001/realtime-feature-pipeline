const D = window.DATA || {};
document.getElementById("gen").textContent = D.generated || "";
const k = D.kpis || {}, cons = D.consistency || {};

document.getElementById("kpis").innerHTML = [
  ["Train/serve match", Number(cons.match_rate ?? 1).toFixed(4), true],
  ["Online features (Redis)", (k.online_keys ?? 0).toLocaleString()],
  ["Features per session", k.features ?? 0],
  ["Registered model", k.model ?? "—"],
].map(([l, v, a]) => `<div class="kpi"><div class="v ${a ? "accent" : ""}">${v}</div><div class="l">${l}</div></div>`).join("");

if (D.rebuffer_hist) histogram("rebuf", D.rebuffer_hist.labels, D.rebuffer_hist.values, { color: C.blurple });
if (D.device) barChart("device", D.device.labels, D.device.values, { color: C.cyan });
if (D.network) barChart("network", D.network.labels, D.network.values, { color: C.violet });

document.getElementById("schema").innerHTML =
  '<div style="display:flex;flex-wrap:wrap;gap:.45rem">' +
  (D.schema || []).map(s => `<span class="mono" style="font-size:.78rem;padding:.25rem .55rem;border:1px solid var(--border);border-radius:6px;color:var(--slate)">${s}</span>`).join("") +
  "</div>";

const m = D.model || {};
document.getElementById("modelTbl").innerHTML =
  `<tr><th>Field</th><th>Value</th></tr>` +
  `<tr><td>Name</td><td class="mono">${m.name || "—"}</td></tr>` +
  `<tr><td>Version</td><td>${m.version ?? "—"}</td></tr>` +
  `<tr><td>Registry</td><td>${m.registry || "—"}</td></tr>` +
  `<tr><td>ROC-AUC</td><td>n/a (see note)</td></tr>`;
document.getElementById("modelNote").textContent = m.note || "";

document.getElementById("consKpis").innerHTML = [
  ["Match rate", Number(cons.match_rate ?? 1).toFixed(4), "accent"],
  ["Predictions compared", cons.checked ?? "—", ""],
  ["Mismatches", cons.mismatches ?? 0, "ok"],
].map(([l, v, c]) => `<div class="kpi"><div class="v ${c}">${v}</div><div class="l">${l}</div></div>`).join("");

initTabs(); initIcons();
