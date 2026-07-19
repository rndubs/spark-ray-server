"use strict";
// Spark Training Dashboard — vanilla JS, polls the JSON API every 3s.
// No build step, no CDN (the Spark may be offline behind a bare SSH tunnel).

const $ = (s, r = document) => r.querySelector(s);
const el = (t, cls, txt) => { const e = document.createElement(t);
  if (cls) e.className = cls; if (txt != null) e.textContent = txt; return e; };

async function getJSON(u) { const r = await fetch(u); if (!r.ok) throw new Error(u + " " + r.status); return r.json(); }
async function getText(u) { const r = await fetch(u); return r.text(); }

// ---------------------------------------------------------------- formatters
function fmtDur(s) {
  if (s == null) return "—";
  s = Math.round(s); const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), ss = s % 60;
  return h ? `${h}h${String(m).padStart(2,"0")}m` : m ? `${m}m${String(ss).padStart(2,"0")}s` : `${ss}s`;
}
function fmtGB(mb) { return mb == null ? "—" : mb >= 1024 ? (mb/1024).toFixed(1)+" GB" : Math.round(mb)+" MB"; }
function fmtNum(x, d = 3) { return (x == null) ? "—" : Number(x).toFixed(d); }
function ago(ts) { if (!ts) return ""; const d = (Date.now() - Date.parse(ts))/1000;
  return d < 60 ? "just now" : d < 3600 ? Math.round(d/60)+"m ago" : Math.round(d/3600)+"h ago"; }
function statusClass(s) { return "b-" + String(s||"unknown").toLowerCase(); }

function badgeSpan(text, cls) { const b = el("span", "badge " + cls, text); return b; }

function shaLink(row) {
  if (!row.sha) return el("span", "muted", "—");
  const a = el("a", "mono", row.sha.slice(0,9));
  a.href = "#"; a.title = "git show";
  a.onclick = (e) => { e.preventDefault(); e.stopPropagation(); openDrawer(row.run_id, "git"); };
  return a;
}

function readsBadges(reads) {
  const frag = document.createDocumentFragment();
  if (!reads) return frag;
  if (reads.contaminated) frag.append(el("span", "tag red", "eval/frozen READ"));
  else if (reads.contaminated_ever) frag.append(el("span", "tag amber", "frozen declared"));
  if (reads.undeclared && reads.undeclared.length)
    frag.append(el("span", "tag amber", `${reads.undeclared.length} undeclared`));
  return frag;
}

// ---------------------------------------------------------------- host strip
function renderHost(h) {
  const host = $("#host"); host.innerHTML = "";
  const brand = el("div", "brand");
  brand.append(el("span", null, "🔥"));
  const h1 = el("h1", null, "Spark Training"); brand.append(h1);
  host.append(brand);

  const g = h.gpu || {};
  const stat = (k, v, sub, pct) => {
    const s = el("div", "stat"); s.append(el("div", "k", k));
    const vv = el("div", "v"); vv.append(document.createTextNode(v));
    if (sub) { const sm = el("small", null, " " + sub); vv.append(sm); }
    s.append(vv);
    if (pct != null) { const bar = el("div", "bar"); const i = el("i");
      i.style.width = Math.min(100, pct) + "%";
      if (pct > 90) i.style.background = "var(--red)"; else if (pct > 75) i.style.background = "var(--amber)";
      bar.append(i); s.append(bar); }
    return s;
  };
  host.append(stat("GPU util", g.util_pct != null ? g.util_pct + "%" : "—", "", g.util_pct));
  const memPct = g.mem_total_mb ? 100 * g.mem_used_mb / g.mem_total_mb : null;
  host.append(stat("GPU mem", g.mem_used_mb != null ? fmtGB(g.mem_used_mb) : "—",
    g.mem_total_mb ? "/ " + fmtGB(g.mem_total_mb) : "", memPct));
  host.append(stat("Temp", g.temp_c != null ? g.temp_c + "°C" : "—"));
  host.append(stat("Power", g.power_w != null ? Math.round(g.power_w) + "W" : "—",
    g.power_limit_w ? "/ " + Math.round(g.power_limit_w) + "W" : ""));
  host.append(stat("Host mem", h.mem_used_gb != null ? h.mem_used_gb + " GB" : "—",
    h.mem_total_gb ? "/ " + h.mem_total_gb : "", h.mem_pct));
  host.append(stat("Load", h.loadavg ? h.loadavg[0].toFixed(2) : "—",
    h.ncpu ? "/ " + h.ncpu : ""));
  host.append(stat(h.disk_watch ? h.disk_watch + " free" : "disk free",
    h.disk_free_gb != null ? h.disk_free_gb + " GB" : "—",
    h.disk_total_gb ? "/ " + h.disk_total_gb : ""));
  host.append(el("div", "grow"));

  const ray = h.ray || {};
  const rs = el("div", "stat");
  const line = el("div", null); const dot = el("span", null); dot.id = "dot";
  dot.style.background = ray.up === true ? "var(--green)" : ray.up === false ? "var(--red)" : "var(--grey)";
  line.append(dot);
  const rl = el("span", null, ray.up === true ? " Ray up" : ray.up === false ? " Ray down" : " Ray ?");
  line.append(rl);
  rs.append(el("div", "k", "cluster")); rs.append(line);
  const link = el("a", null, "dashboard :8265");
  link.href = "http://127.0.0.1:" + (ray.port || 8265); link.target = "_blank";
  link.style.fontSize = "11px";
  rs.append(link);
  host.append(rs);
}

// ---------------------------------------------------------------- jobs table
const JOB_COLS = ["Status","Description","Provenance","Progress","Resources","GPU-hours","Data reads"];

function jobCells(row) {
  const tr = el("tr", "job"); tr.dataset.id = row.run_id;
  tr.onclick = () => openDrawer(row.run_id);

  // status
  let td = el("td");
  td.append(badgeSpan(row.effective_status || row.status, statusClass(row.effective_status || row.status)));
  (row.badges||[]).forEach(b => td.append(el("span", "tag " + (b.level||"grey"), b.text)));
  if (!row.registered) td.append(el("span", "tag grey", "unregistered"));
  tr.append(td);

  // description
  td = el("td"); const d = el("div", "desc", row.desc || row.name || row.run_id);
  td.append(d);
  const sub = el("div", "sub"); sub.append(document.createTextNode(row.run_id));
  td.append(sub); tr.append(td);

  // provenance
  td = el("td"); td.append(shaLink(row));
  if (row.dirty) td.append(el("span", "tag amber", "dirty"));
  const pr = el("div", "sub");
  pr.textContent = [row.branch, row.variant, row.seeds ? "seed " + row.seeds.join(",") : null]
    .filter(Boolean).join(" · ");
  td.append(pr); tr.append(td);

  // progress
  td = el("td", "prog"); const p = row.progress || {};
  if (p.step != null) {
    td.append(document.createTextNode(p.total ? `${p.step} / ${p.total}` : `step ${p.step}`));
    if (p.total) { const bar = el("div","bar"); const i = el("i");
      i.style.width = Math.min(100, 100*p.step/p.total)+"%"; bar.append(i); td.append(bar); }
    const meta = el("div","sub", (p.steps_per_s ? p.steps_per_s+"/s" : "") +
      (p.eta_s ? " · ETA " + fmtDur(p.eta_s) : ""));
    td.append(meta);
  } else td.append(el("span","muted","—"));
  tr.append(td);

  // resources
  td = el("td"); const r = row.resources || {};
  td.append(document.createTextNode("RSS " + fmtGB(r.rss_mb)));
  const gm = el("div","sub", r.gpu_mem_mb != null ? "GPU " + fmtGB(r.gpu_mem_mb) : "");
  td.append(gm); tr.append(td);

  // gpu-hours
  td = el("td"); const gh = row.gpu_hours || {};
  const val = el("span", "mono", fmtNum(gh.hours, 3));
  td.append(val);
  if (gh.frozen) td.append(el("span","tag grey","final"));
  td.append(el("div","sub", gh.mean_util_pct != null ? gh.mean_util_pct+"% mean util" : ""));
  tr.append(td);

  // data reads
  td = el("td"); const reads = row.reads || {};
  const decl = (reads.declared||[]).map(x => x.tier).filter((v,i,a)=>a.indexOf(v)===i);
  if (decl.length) td.append(el("span","sub", decl.join(", ")));
  else td.append(el("span","muted","none declared"));
  td.append(readsBadges(reads));
  tr.append(td);

  return tr;
}

function renderJobs(data) {
  const head = $("#jobs thead"); if (!head.children.length) {
    const tr = el("tr"); JOB_COLS.forEach(c => tr.append(el("th", null, c))); head.append(tr);
  }
  const tb = $("#jobs tbody"); tb.innerHTML = "";
  (data.jobs||[]).forEach(row => tb.append(jobCells(row)));
  $("#jobs-meta").textContent = (data.jobs||[]).length + " shown";
  if (!(data.jobs||[]).length) tb.append(rowMsg(JOB_COLS.length, "no jobs"));
}

function rowMsg(span, msg) { const tr = el("tr"); const td = el("td","muted",msg);
  td.colSpan = span; tr.append(td); return tr; }

// LEDGER line: one pre-formatted, copyable row for L3 ledger drafting.
function ledgerLine(row) {
  const gh = row.gpu_hours || {};
  return [row.ended_ts || "", row.run_id, (row.sha||"").slice(0,9),
    row.variant||"", row.status,
    "loss=" + fmtNum(row.last_loss, 4),
    "gpu_h=" + fmtNum(gh.hours, 3),
    "wall=" + fmtDur(row.elapsed_s),
    (row.desc? '"'+row.desc+'"':"")].join("  ");
}

const HIST_COLS = ["Status","Description","SHA","Final loss","Wall","GPU-hours","LEDGER"];
function renderHist(data) {
  const head = $("#hist thead"); if (!head.children.length) {
    const tr = el("tr"); HIST_COLS.forEach(c => tr.append(el("th", null, c))); head.append(tr);
  }
  const tb = $("#hist tbody"); tb.innerHTML = "";
  const jobs = data.jobs || [];
  if (!jobs.length) { tb.append(rowMsg(HIST_COLS.length, "no completed jobs yet")); return; }
  jobs.forEach(row => {
    const tr = el("tr","job"); tr.dataset.id = row.run_id;
    tr.onclick = () => openDrawer(row.run_id);
    let td = el("td"); td.append(badgeSpan(row.status, statusClass(row.status))); tr.append(td);
    td = el("td"); td.append(el("div","desc",row.desc||row.name||row.run_id));
    td.append(el("div","sub",row.run_id)); tr.append(td);
    td = el("td"); td.append(shaLink(row)); tr.append(td);
    tr.append(el("td",null,fmtNum(row.last_loss,4)));
    tr.append(el("td",null,fmtDur(row.elapsed_s)));
    td = el("td","mono",fmtNum((row.gpu_hours||{}).hours,3)); tr.append(td);
    td = el("td"); const btn = el("button","copy","copy");
    btn.onclick = (e) => { e.stopPropagation(); navigator.clipboard.writeText(ledgerLine(row));
      btn.textContent = "copied"; setTimeout(()=>btn.textContent="copy", 1200); };
    td.append(btn); tr.append(td);
    tb.append(tr);
  });
}

// ---------------------------------------------------------------- detail drawer
let curChart = null, curDetailId = null, followLog = true;

function closeDrawer() { $("#drawer").classList.remove("open");
  setTimeout(()=>{ $("#drawer").style.display="none"; $("#ov").style.display="none"; }, 180);
  curDetailId = null; if (curChart) { curChart.destroy(); curChart = null; } }

async function openDrawer(id, focus) {
  curDetailId = id; followLog = true;
  $("#ov").style.display = "block"; $("#drawer").style.display = "block";
  requestAnimationFrame(()=>$("#drawer").classList.add("open"));
  $("#ov").onclick = closeDrawer;
  await refreshDetail(id);
  if (focus === "git") { const g = $("#git-pre"); if (g) g.scrollIntoView(); }
}

async function refreshDetail(id) {
  let row; try { row = await getJSON("/api/jobs/" + encodeURIComponent(id)); }
  catch(e){ return; }
  if (curDetailId !== id) return;

  const dh = $("#drawer .dh"); dh.innerHTML = "";
  const left = el("div");
  const t = el("h1", null, row.desc || row.name || id); left.append(t);
  const s = el("div","sub"); s.textContent = id; left.append(s);
  const bl = el("div"); bl.style.marginTop = "6px";
  bl.append(badgeSpan(row.effective_status||row.status, statusClass(row.effective_status||row.status)));
  (row.badges||[]).forEach(b=>bl.append(el("span","tag "+(b.level||"grey"), b.text)));
  if (row.reads && row.reads.contaminated) bl.append(el("span","tag red","CONTAMINATION"));
  left.append(bl);
  dh.append(left);
  const x = el("button","close","×"); x.onclick = closeDrawer; dh.append(x);

  const body = $("#drawer .body"); body.innerHTML = "";

  // provenance / stats
  body.append(el("h3", null, "Provenance & cost"));
  const kv = el("div","kv");
  const gh = row.gpu_hours || {};
  const rows = [
    ["git", (row.sha||"—").slice(0,12) + (row.dirty? "  (dirty at submit)":"") + "  ·  " + (row.branch||"")],
    ["variant / seeds", [row.variant, (row.seeds||[]).join(",")].filter(Boolean).join("  ·  ") || "—"],
    ["progress", row.progress && row.progress.step != null ?
      `${row.progress.step}${row.progress.total? " / "+row.progress.total:""}` +
      (row.progress.eta_s? "  ·  ETA "+fmtDur(row.progress.eta_s):"") : "—"],
    ["last loss", fmtNum(row.last_loss,5)],
    ["GPU-hours", fmtNum(gh.hours,4) + (gh.frozen? "  (final)":"  (live)") +
      (gh.mean_util_pct!=null? `  ·  ${gh.mean_util_pct}% mean util`:"")],
    ["wall clock", fmtDur(row.elapsed_s)],
    ["resources", row.resources ? `RSS ${fmtGB(row.resources.rss_mb)}` +
      (row.resources.gpu_mem_mb!=null? `  ·  GPU ${fmtGB(row.resources.gpu_mem_mb)} (unified: overlaps RSS)`:"") : "—"],
    ["mem budget", row.mem_gb!=null? row.mem_gb+" GB":"—"],
    ["artifacts", row.artifacts_dir||"—"],
    ["run dir", row.run_dir||"—"],
  ];
  rows.forEach(([k,v])=>{ kv.append(el("div","k",k)); kv.append(el("div","mono",String(v))); });
  body.append(kv);

  // ledger copy
  const lc = el("div"); lc.style.margin="10px 0";
  const lb = el("button","copy","copy LEDGER line");
  lb.onclick = ()=>{ navigator.clipboard.writeText(ledgerLine(row));
    lb.textContent="copied"; setTimeout(()=>lb.textContent="copy LEDGER line",1200); };
  lc.append(lb); body.append(lc);

  // data reads
  body.append(el("h3", null, "Data reads"));
  const reads = row.reads || {};
  if (!(reads.declared||[]).length && !(reads.observed||[]).length)
    body.append(el("div","muted","none declared, none observed"));
  (reads.declared||[]).forEach(x=>{
    const line = el("div","mono"); line.style.fontSize="12px";
    line.append(el("span","pill-"+(x.frozen?"frozen":x.tier.split("/")[0]), "["+x.tier+"] "));
    line.append(document.createTextNode(x.path + "  (declared)"));
    body.append(line);
  });
  (reads.observed||[]).forEach(x=>{
    const line = el("div","mono"); line.style.fontSize="12px";
    line.append(el("span","pill-"+(x.frozen?"frozen":x.tier.split("/")[0]), "["+x.tier+"] "));
    line.append(document.createTextNode(x.path + (x.declared?"  (observed)":"  (observed, UNDECLARED)")));
    body.append(line);
  });
  if (reads.observed && !reads.observed.length && row.status==="RUNNING")
    body.append(el("div","muted","fd sampling is best-effort (~30s) and misses short-lived opens"));

  // curves
  body.append(el("h3", null, "Training curves"));
  const tb = el("div","toolbar");
  const logck = el("label","ck"); const cb = el("input"); cb.type="checkbox";
  logck.append(cb); logck.append(document.createTextNode(" log-y"));
  tb.append(logck); body.append(tb);
  const chart = el("div","chart"); chart.id="chart"; body.append(chart);
  logCb = cb;
  await drawCurves(id, chart, cb);
  cb.onchange = ()=>drawCurves(id, chart, cb);

  // checkpoint + config
  if (row.checkpoint) {
    body.append(el("h3", null, "Checkpoint"));
    const c = row.checkpoint;
    body.append(el("div","mono", `${c.path}  ·  ${c.size_mb} MB  ·  ${new Date(c.mtime*1000).toLocaleString()}`));
  }
  if (row.config) {
    body.append(el("h3", null, "Variant config"));
    const pre = el("pre", null, JSON.stringify(row.config, null, 2)); body.append(pre);
  }

  // log tail
  body.append(el("h3", null, "Log (last 100 lines)"));
  const flw = el("label","ck"); const fcb = el("input"); fcb.type="checkbox"; fcb.checked=true;
  flw.append(fcb); flw.append(document.createTextNode(" auto-follow"));
  fcb.onchange = ()=>followLog = fcb.checked; body.append(flw);
  const logpre = el("pre"); logpre.id="logpre"; body.append(logpre);
  await refreshLog(id, logpre);

  // git show (anchor)
  body.append(el("h3", null, "git show"));
  const gpre = el("pre"); gpre.id="git-pre"; gpre.textContent="loading…"; body.append(gpre);
  getText("/api/jobs/"+encodeURIComponent(id)+"/git").then(t=>{ if(curDetailId===id) gpre.textContent=t; });
}

async function drawCurves(id, container, logcb) {
  let s; try { s = await getJSON("/api/jobs/"+encodeURIComponent(id)+"/series"); } catch(e){ return; }
  if (curDetailId !== id) return;
  container.innerHTML = "";
  if (!s.n) { container.append(el("div","muted","no metrics rows yet")); return; }
  const keys = (s.default && s.default.length) ? s.default
    : Object.keys(s.columns).filter(k=>k.endsWith("_total")).slice(0,4);
  const palette = ["#58a6ff","#3fb950","#d29922","#f85149","#bc8cff","#39c5cf"];
  const xs = s.steps.map((v,i)=> v==null? i : v);
  const series = [{}]; const data = [xs];
  keys.forEach((k,i)=>{ series.push({label:k, stroke:palette[i%palette.length], width:2});
    data.push(s.columns[k].map(v=>v==null?null:v)); });
  if (curChart) { curChart.destroy(); curChart = null; }
  const opts = { width: container.clientWidth||760, height:260,
    scales:{ y:{ distr: logcb && logcb.checked ? 3 : 1 } },
    axes:[{stroke:"#8b949e"},{stroke:"#8b949e"}],
    legend:{show:true}, series };
  curChart = new uPlot(opts, data, container);
}

async function refreshLog(id, pre) {
  const t = await getText("/api/jobs/"+encodeURIComponent(id)+"/log?tail=100");
  if (curDetailId !== id) return;
  pre.textContent = t;
  if (followLog) pre.scrollTop = pre.scrollHeight;
}

// Lightweight live refresh while the drawer is open: only the log tail and
// curves (which is where new data lands). Avoids rebuilding the DOM every
// tick, so scroll position and toggles survive.
let logCb = null;
async function liveDetail(id) {
  const pre = $("#logpre"); const chart = $("#chart");
  if (pre) refreshLog(id, pre);
  if (chart) drawCurves(id, chart, logCb);
}

// ---------------------------------------------------------------- poll loop
async function tick() {
  try {
    const [host, jobs, hist] = await Promise.all([
      getJSON("/api/host"), getJSON("/api/jobs"), getJSON("/api/history")]);
    renderHost(host); renderJobs(jobs); renderHist(hist);
    $("#err").textContent = "";
    if (curDetailId) liveDetail(curDetailId);  // log + curves only; no rebuild
  } catch(e) {
    $("#err").textContent = "poll error: " + e.message + " (retrying)";
  }
}
document.addEventListener("keydown", e=>{ if (e.key==="Escape") closeDrawer(); });
tick(); setInterval(tick, 3000);
