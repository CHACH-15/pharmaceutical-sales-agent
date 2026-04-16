import React, { useState, useEffect, useRef, useCallback } from "react";

const API="http://localhost:8000";
const WS_URL="ws://localhost:8000/ws";

const api = {
  token:   () => localStorage.getItem("token"),
  headers: () => ({ "Content-Type": "application/json", ...(api.token() ? { Authorization: `Bearer ${api.token()}` } : {}) }),
  post:    (path, body) => fetch(`${API}${path}`, { method: "POST",   headers: api.headers(), body: JSON.stringify(body) }).then(r => r.json()),
  get:     (path)       => fetch(`${API}${path}`, { headers: api.headers() }).then(r => r.json()),
  patch:   (path, body) => fetch(`${API}${path}`, { method: "PATCH",  headers: api.headers(), body: JSON.stringify(body) }).then(r => r.json()),
  del:     (path)       => fetch(`${API}${path}`, { method: "DELETE", headers: api.headers() }).then(r => r.json()),
  upload:  (path, formData) => fetch(`${API}${path}`, { method: "POST", headers: { Authorization: `Bearer ${api.token()}` }, body: formData }).then(r => r.json()),
};

const S = {
  coral:"#ff6b6b", coralDark:"#e85555", coralLight:"#fff0f0", coralMid:"#ffdede",
  bg:"#f7f6f4", white:"#ffffff", text:"#1a1a1a", textMid:"#555", textLight:"#999", border:"#e5e3e0",
  green:"#16a34a", greenLight:"#f0fdf4",
};

const css = {
  app:       { fontFamily:"'DM Sans', sans-serif", background:S.bg, minHeight:"100vh", color:S.text },
  loginWrap: { minHeight:"100vh", display:"flex", alignItems:"center", justifyContent:"center", background:S.bg },
  loginCard: { background:S.white, borderRadius:20, padding:"3rem 2.5rem", width:440, boxShadow:"0 8px 40px rgba(0,0,0,0.08)", border:`1px solid ${S.border}` },
  logo:      { fontSize:"2.8rem", fontWeight:700, color:S.coral, textAlign:"center", marginBottom:"0.3rem", letterSpacing:"-1px" },
  logoSub:   { textAlign:"center", color:S.textLight, fontSize:"0.85rem", marginBottom:"2.5rem" },
  input:     { width:"100%", padding:"0.75rem 1rem", border:`1.5px solid ${S.border}`, borderRadius:10, fontSize:"0.95rem", outline:"none", background:S.white, boxSizing:"border-box" },
  label:     { fontSize:"0.8rem", fontWeight:600, color:S.textMid, marginBottom:"0.35rem", display:"block" },
  btn:       { width:"100%", padding:"0.85rem", background:S.coral, color:"#fff", border:"none", borderRadius:10, fontWeight:700, fontSize:"1rem", cursor:"pointer", marginTop:"1.5rem" },
  btnSm:     { padding:"0.5rem 1.2rem", background:S.coral, color:"#fff", border:"none", borderRadius:8, fontWeight:600, fontSize:"0.85rem", cursor:"pointer" },
  btnGhost:  { padding:"0.5rem 1.2rem", background:"transparent", color:S.coral, border:`1.5px solid ${S.coral}`, borderRadius:8, fontWeight:600, fontSize:"0.85rem", cursor:"pointer" },
  btnDanger: { padding:"0.5rem 1.2rem", background:"#fee2e2", color:"#dc2626", border:"none", borderRadius:8, fontWeight:600, fontSize:"0.85rem", cursor:"pointer" },
  btnGreen:  { padding:"0.5rem 1.2rem", background:S.green, color:"#fff", border:"none", borderRadius:8, fontWeight:600, fontSize:"0.85rem", cursor:"pointer" },
  error:     { background:"#fee2e2", color:"#dc2626", borderRadius:8, padding:"0.75rem 1rem", fontSize:"0.88rem", marginBottom:"1rem" },
  layout:    { display:"flex", height:"100vh", overflow:"hidden" },
  sidebar:   { width:270, background:S.white, borderRight:`1px solid ${S.border}`, display:"flex", flexDirection:"column", flexShrink:0 },
  sidebarHeader:  { padding:"1.8rem 1.4rem 1.2rem", borderBottom:`1px solid ${S.border}` },
  sidebarLogo:    { fontSize:"1.8rem", fontWeight:700, color:S.coral, letterSpacing:"-0.5px" },
  sidebarTagline: { fontSize:"0.72rem", color:S.textLight, textTransform:"uppercase", letterSpacing:"0.08em", marginTop:2 },
  newChatBtn:     { margin:"1rem 1rem 0", padding:"0.65rem 1rem", background:S.coralLight, color:S.coralDark, border:`1.5px solid ${S.coralMid}`, borderRadius:10, fontWeight:600, fontSize:"0.88rem", cursor:"pointer", width:"calc(100% - 2rem)" },
  sidebarSection: { fontSize:"0.7rem", fontWeight:700, color:S.textLight, textTransform:"uppercase", letterSpacing:"0.1em", padding:"1.2rem 1.4rem 0.6rem" },
  sidebarScroll:  { flex:1, overflowY:"auto", padding:"0 0.6rem 1rem" },
  chatItem: (a) => ({ padding:"0.75rem 0.9rem", borderRadius:10, cursor:"pointer", marginBottom:3, background:a?S.coralLight:"transparent", border:`1px solid ${a?S.coralMid:"transparent"}`, transition:"all 0.15s" }),
  chatItemTitle:  { fontSize:"0.88rem", fontWeight:500, color:S.text, whiteSpace:"nowrap", overflow:"hidden", textOverflow:"ellipsis" },
  chatItemDate:   { fontSize:"0.73rem", color:S.textLight, marginTop:2 },
  sidebarFooter:  { padding:"1rem 1.4rem", borderTop:`1px solid ${S.border}` },
  userChip:       { display:"flex", alignItems:"center", gap:"0.7rem" },
  userAvatar:     { width:34, height:34, borderRadius:"50%", background:S.coralLight, color:S.coral, display:"flex", alignItems:"center", justifyContent:"center", fontWeight:700, fontSize:"0.9rem", flexShrink:0 },
  userName:       { fontSize:"0.88rem", fontWeight:600, color:S.text },
  userRole:       { fontSize:"0.72rem", color:S.textLight },
  logoutBtn:      { marginLeft:"auto", background:"none", border:"none", color:S.textLight, cursor:"pointer", fontSize:"1.1rem" },
  adminLink:      { display:"block", textAlign:"center", fontSize:"0.8rem", color:S.coral, fontWeight:600, padding:"0.5rem", cursor:"pointer", borderTop:`1px solid ${S.border}`, marginTop:"0.5rem" },
  main:           { flex:1, display:"flex", flexDirection:"column", overflow:"hidden" },
  chatHeader:     { textAlign:"center", padding:"3rem 2rem 2rem" },
  headerLogo:     { fontSize:"3rem", fontWeight:700, color:S.coral, letterSpacing:"-1px" },
  headerSub:      { color:S.textLight, fontSize:"0.92rem", marginTop:"0.4rem" },
  headerLine:     { width:48, height:3, background:S.coral, borderRadius:2, margin:"1rem auto 0", opacity:0.4 },
  messagesArea:   { flex:1, overflowY:"auto", padding:"0 2rem 2rem", maxWidth:820, width:"100%", margin:"0 auto", boxSizing:"border-box" },
  msgBlock:       { marginBottom:"1.8rem" },
  msgLabel: (r)  => ({ fontSize:"0.72rem", fontWeight:700, textTransform:"uppercase", letterSpacing:"0.1em", color:r==="user"?S.textMid:S.coral, marginBottom:"0.5rem", display:"flex", alignItems:"center", gap:"0.5rem" }),
  msgDot:   (r)  => ({ width:6, height:6, borderRadius:"50%", background:r==="user"?S.textMid:S.coral, display:"inline-block" }),
  msgBubble:(r)  => ({ background:S.white, border:`1px solid ${S.border}`, borderLeft:r==="assistant"?`3px solid ${S.coral}`:`1px solid ${S.border}`, borderRadius:"0 14px 14px 14px", padding:"1.1rem 1.4rem", fontSize:"0.96rem", lineHeight:1.75, color:S.text, boxShadow:"0 2px 10px rgba(0,0,0,0.04)" }),
  sources:        { marginTop:"0.8rem", paddingTop:"0.8rem", borderTop:`1px solid ${S.border}`, fontSize:"0.78rem", color:S.textLight, display:"flex", alignItems:"center", gap:"0.4rem" },
  inputBar:       { padding:"1rem 2rem 1.5rem", borderTop:`1px solid ${S.border}`, background:S.bg },
  inputInner:     { maxWidth:820, margin:"0 auto", display:"flex", gap:"0.75rem", alignItems:"center" },
  inputField:     { flex:1, padding:"0.9rem 1.2rem", border:`1.5px solid ${S.border}`, borderRadius:12, fontSize:"0.97rem", outline:"none", background:S.white },
  sendBtn:        { padding:"0.9rem 1.5rem", background:S.coral, color:"#fff", border:"none", borderRadius:12, fontWeight:700, cursor:"pointer", fontSize:"0.9rem" },
  adminWrap:      { flex:1, overflow:"auto", padding:"2rem 2.5rem" },
  adminTitle:     { fontSize:"1.6rem", fontWeight:700, color:S.text, marginBottom:"0.3rem" },
  adminSub:       { color:S.textLight, fontSize:"0.9rem", marginBottom:"2rem" },
  statCard:       { background:S.white, border:`1px solid ${S.border}`, borderRadius:14, padding:"1.4rem 1.6rem" },
  statNum:        { fontSize:"2rem", fontWeight:700, color:S.coral },
  statLabel:      { fontSize:"0.8rem", color:S.textLight, textTransform:"uppercase", letterSpacing:"0.06em", marginTop:4 },
  table:          { width:"100%", borderCollapse:"collapse", background:S.white, borderRadius:14, overflow:"hidden", boxShadow:"0 1px 4px rgba(0,0,0,0.05)" },
  th:             { background:S.coralLight, color:S.coralDark, fontWeight:700, padding:"0.75rem 1rem", textAlign:"left", fontSize:"0.82rem", textTransform:"uppercase", letterSpacing:"0.05em" },
  td:             { padding:"0.75rem 1rem", borderBottom:`1px solid ${S.border}`, fontSize:"0.9rem" },
  badge:(r)=>    ({ display:"inline-block", padding:"0.2rem 0.6rem", borderRadius:20, background:r==="admin"?"#fef3c7":r==="superviseur"?"#ede9fe":S.coralLight, color:r==="admin"?"#d97706":r==="superviseur"?"#7c3aed":S.coral, fontSize:"0.75rem", fontWeight:600 }),
  modal:          { position:"fixed", inset:0, background:"rgba(0,0,0,0.35)", display:"flex", alignItems:"center", justifyContent:"center", zIndex:1000 },
  modalCard:      { background:S.white, borderRadius:16, padding:"2rem", width:460, boxShadow:"0 20px 60px rgba(0,0,0,0.15)" },
  chatTable:      { width:"100%", borderCollapse:"collapse", marginTop:"1rem", fontSize:"0.88rem", borderRadius:10, overflow:"hidden" },
  chatTh:         { background:S.coralLight, color:S.coralDark, fontWeight:700, padding:"0.55rem 0.9rem", textAlign:"left", borderBottom:`2px solid ${S.coralMid}` },
  chatTd:         { padding:"0.5rem 0.9rem", borderBottom:`1px solid ${S.border}`, color:S.text },
  tableActions:   { display:"flex", gap:"0.5rem", marginTop:"0.75rem", flexWrap:"wrap" },
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

function Input({ label, ...props }) {
  const [focused, setFocused] = useState(false);
  return (
    <div style={{ marginBottom:"1rem" }}>
      {label && <label style={css.label}>{label}</label>}
      <input {...props} style={{ ...css.input, borderColor:focused?S.coral:S.border }}
        onFocus={()=>setFocused(true)} onBlur={()=>setFocused(false)} />
    </div>
  );
}

function Spinner() {
  return (
    <div style={{ display:"flex", alignItems:"center", gap:"0.5rem", padding:"0.8rem 1.4rem" }}>
      {[0,0.2,0.4].map((d,i)=>(
        <div key={i} style={{ width:8, height:8, borderRadius:"50%", background:S.coral, animation:`pulse 1s ${d}s infinite` }}/>
      ))}
      <style>{`@keyframes pulse{0%,100%{opacity:.3;transform:scale(.8)}50%{opacity:1;transform:scale(1)}}`}</style>
    </div>
  );
}

function EmailInput({ value, onChange, label }) {
  return (
    <div style={{ marginBottom:"1rem" }}>
      {label && <label style={css.label}>{label}</label>}
      <div style={{ display:"flex", border:`1.5px solid ${S.border}`, borderRadius:10, overflow:"hidden", background:S.white }}>
        <input value={value} onChange={onChange} placeholder="prenom.nom" required
          style={{ ...css.input, border:"none", borderRadius:0, flex:1 }}/>
        <span style={{ padding:"0 0.9rem", color:S.textLight, fontSize:"0.88rem", background:"#f5f5f5", display:"flex", alignItems:"center", borderLeft:`1px solid ${S.border}`, whiteSpace:"nowrap" }}>
          @hikma.com
        </span>
      </div>
    </div>
  );
}

const roleLabel = (r) => r==="delegue_medical"?"Délégué Médical":r==="superviseur"?"Superviseur":r==="admin"?"Admin":r;

// ─── CSV Export ───────────────────────────────────────────────────────────────

function exportCSV(table) {
  const headers = table.headers.join(";");
  const rows    = table.rows.map(r => r.join(";")).join("\n");
  const csv     = `${headers}\n${rows}`;
  const blob    = new Blob(["\uFEFF" + csv], { type:"text/csv;charset=utf-8;" });
  const url     = URL.createObjectURL(blob);
  const a       = document.createElement("a");
  a.href        = url;
  a.download    = `hikma_export_${new Date().toISOString().slice(0,10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

// ─── Bar Chart ────────────────────────────────────────────────────────────────

function BarChart({ table }) {
  if (!table || !table.headers || !table.rows || table.rows.length === 0) return null;

  const labelCol = 0;
  let valueCol   = -1;
  for (let c = 1; c < table.headers.length; c++) {
    const vals = table.rows.map(r => parseFloat(String(r[c]).replace(/[,\s]/g, "")));
    if (vals.some(v => !isNaN(v))) { valueCol = c; break; }
  }
  if (valueCol === -1) return null;

  const labels = table.rows.map(r => String(r[labelCol]).slice(0, 16));
  const values = table.rows.map(r => parseFloat(String(r[valueCol]).replace(/[,\s]/g, "")) || 0);
  const maxVal = Math.max(...values, 1);

  const pad  = { top:20, right:20, bottom:60, left:60 };
  const W    = Math.max(400, labels.length * 70 + pad.left + pad.right);
  const H    = 240;
  const iW   = W - pad.left - pad.right;
  const iH   = H - pad.top  - pad.bottom;
  const barW = Math.max(20, Math.min(50, iW / labels.length * 0.7));
  const gap  = iW / labels.length;

  const bx = (i) => pad.left + i * gap + (gap - barW) / 2;
  const by = (v) => pad.top + iH - (v / maxVal) * iH;
  const bh = (v) => (v / maxVal) * iH;

  const yTicks = [0, 0.25, 0.5, 0.75, 1].map(t => maxVal * t);

  return (
    <div style={{ overflowX:"auto", marginTop:"1rem" }}>
      <svg width={W} height={H} style={{ display:"block", minWidth:"100%" }}>
        {yTicks.map((v,i) => (
          <g key={i}>
            <line x1={pad.left} y1={by(v)} x2={W-pad.right} y2={by(v)} stroke={S.border} strokeWidth="1" strokeDasharray="4,4"/>
            <text x={pad.left-6} y={by(v)+4} textAnchor="end" fontSize="10" fill={S.textLight}>
              {v >= 1000 ? `${(v/1000).toFixed(0)}k` : v.toFixed(0)}
            </text>
          </g>
        ))}
        {values.map((v, i) => (
          <g key={i}>
            <rect x={bx(i)} y={by(v)} width={barW} height={bh(v)} fill={S.coral} rx="4" opacity="0.9"/>
            <text x={bx(i)+barW/2} y={by(v)-5} textAnchor="middle" fontSize="10" fontWeight="600" fill={S.coralDark}>
              {v >= 1000 ? `${(v/1000).toFixed(1)}k` : v}
            </text>
            <text x={bx(i)+barW/2} y={H-pad.bottom+16} textAnchor="middle" fontSize="10" fill={S.textMid}
              transform={`rotate(-35, ${bx(i)+barW/2}, ${H-pad.bottom+16})`}>
              {labels[i]}
            </text>
          </g>
        ))}
        <line x1={pad.left} y1={pad.top} x2={pad.left} y2={pad.top+iH} stroke={S.border} strokeWidth="1.5"/>
        <line x1={pad.left} y1={pad.top+iH} x2={W-pad.right} y2={pad.top+iH} stroke={S.border} strokeWidth="1.5"/>
        <text x={14} y={H/2} textAnchor="middle" fontSize="10" fill={S.textLight}
          transform={`rotate(-90,14,${H/2})`}>{table.headers[valueCol]}</text>
      </svg>
    </div>
  );
}

// ─── Chat Table ───────────────────────────────────────────────────────────────

function ChatTable({ table }) {
  const [showChart, setShowChart] = useState(false);
  if (!table || !table.headers || !table.rows) return null;

  return (
    <div style={{ marginTop:"1rem" }}>
      <div style={{ overflowX:"auto" }}>
        <table style={css.chatTable}>
          <thead>
            <tr>{table.headers.map((h,i) => <th key={i} style={css.chatTh}>{h}</th>)}</tr>
          </thead>
          <tbody>
            {table.rows.map((row, ri) => (
              <tr key={ri} style={{ background:ri%2===0?S.white:S.coralLight }}>
                {row.map((cell, ci) => <td key={ci} style={css.chatTd}>{cell}</td>)}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div style={css.tableActions}>
        <button style={css.btnGhost} onClick={() => exportCSV(table)}>
          📥 Exporter CSV
        </button>
        <button style={{ ...css.btnGhost, color:showChart?S.coralDark:"#7c3aed", borderColor:showChart?S.coral:"#7c3aed" }}
          onClick={() => setShowChart(v => !v)}>
          {showChart ? "📊 Masquer le graphique" : "📊 Afficher le graphique"}
        </button>
      </div>
      {showChart && (
        <div style={{ background:S.white, border:`1px solid ${S.border}`, borderRadius:10, padding:"1rem", marginTop:"0.75rem" }}>
          <BarChart table={table} />
        </div>
      )}
    </div>
  );
}

// ─── Line Chart (admin stats) ─────────────────────────────────────────────────

function LineChart({ data }) {
  if (!data || data.length === 0) return (
    <div style={{ textAlign:"center", color:S.textLight, padding:"3rem" }}>
      Aucune donnée — apparaîtra au fur et à mesure des inscriptions
    </div>
  );
  const pad={top:24,right:40,bottom:52,left:52};
  const pointSpacing=Math.max(60,Math.min(120,700/Math.max(data.length-1,1)));
  const W=Math.max(600,pointSpacing*Math.max(data.length-1,1));
  const H2=220,H=H2-pad.top-pad.bottom,W2=W+pad.left+pad.right;
  const maxV=Math.max(...data.map(d=>d.total),1);
  const x=(i)=>pad.left+i*pointSpacing;
  const y=(v)=>pad.top+H-(v/maxV)*H;
  const line=data.map((d,i)=>`${i===0?"M":"L"}${x(i)},${y(d.total)}`).join(" ");
  const area=`${line} L${x(data.length-1)},${pad.top+H} L${x(0)},${pad.top+H} Z`;
  const yTicks=[0,0.25,0.5,0.75,1].map(t=>Math.round(maxV*t));
  const maxLabels=Math.floor(W/55);
  const step=Math.max(1,Math.ceil(data.length/maxLabels));
  const xLabels=data.map((d,i)=>({d,i})).filter(({i})=>i%step===0||i===data.length-1);
  return (
    <div style={{ overflowX:"auto", overflowY:"hidden" }}>
      <svg width={W2} height={H2} style={{ display:"block", minWidth:"100%" }}>
        <defs><linearGradient id="cg" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={S.coral} stopOpacity="0.2"/>
          <stop offset="100%" stopColor={S.coral} stopOpacity="0"/>
        </linearGradient></defs>
        {yTicks.map((v,i)=>(
          <g key={i}>
            <line x1={pad.left} y1={y(v)} x2={pad.left+W} y2={y(v)} stroke={S.border} strokeWidth="1" strokeDasharray="4,4"/>
            <text x={pad.left-8} y={y(v)+4} textAnchor="end" fontSize="11" fill={S.textLight}>{v}</text>
          </g>
        ))}
        {data.map((_,i)=>(
          <line key={i} x1={x(i)} y1={pad.top} x2={x(i)} y2={pad.top+H} stroke={S.border} strokeWidth="1" strokeDasharray="2,4" opacity="0.5"/>
        ))}
        <path d={area} fill="url(#cg)"/>
        <path d={line} fill="none" stroke={S.coral} strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"/>
        {data.map((d,i)=>(
          <g key={i}>
            <circle cx={x(i)} cy={y(d.total)} r="5" fill={S.white} stroke={S.coral} strokeWidth="2.5"/>
            <text x={x(i)} y={y(d.total)-10} textAnchor="middle" fontSize="10" fontWeight="600" fill={S.coral}>{d.total}</text>
          </g>
        ))}
        {xLabels.map(({d,i})=>(
          <text key={i} x={x(i)} y={pad.top+H+20} textAnchor="middle" fontSize="11" fill={S.textMid}>{d.date?.slice(5)}</text>
        ))}
        <line x1={pad.left} y1={pad.top} x2={pad.left} y2={pad.top+H} stroke={S.border} strokeWidth="1.5"/>
        <line x1={pad.left} y1={pad.top+H} x2={pad.left+W} y2={pad.top+H} stroke={S.border} strokeWidth="1.5"/>
      </svg>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────
// LOGIN
// ─────────────────────────────────────────────────────────────
function LoginPage({ onLogin }) {
  const [mode, setMode]       = useState("login");
  const [form, setForm]       = useState({ emailPrefix:"", password:"", first_name:"", last_name:"", gsu:"", role:"delegue_medical" });
  const [error, setError]     = useState("");
  const [loading, setLoading] = useState(false);
  const set = (k) => (e) => setForm(f=>({...f,[k]:e.target.value}));

  const submit = async (e) => {
    e.preventDefault(); setError(""); setLoading(true);
    try {
      const email = form.emailPrefix + "@hikma.com";
      const body  = mode==="login"
        ? { email, password:form.password }
        : { email, password:form.password, first_name:form.first_name, last_name:form.last_name, gsu:form.gsu, role:form.role };
      const res = await api.post(mode==="login"?"/auth/login":"/auth/register", body);
      if (res.detail) { setError(res.detail); return; }
      localStorage.setItem("token", res.token);
      onLogin(res.user);
    } catch { setError("Erreur de connexion au serveur"); }
    finally { setLoading(false); }
  };

  return (
    <div style={css.loginWrap}>
      <div style={css.loginCard}>
        <div style={css.logo}>wisdom.</div>
        <div style={css.logoSub}>Intelligence Analytique Hikma</div>
        {error && <div style={css.error}>{error}</div>}
        <form onSubmit={submit}>
          {mode==="register" && (
            <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:"0 1rem" }}>
              <Input label="Prénom" value={form.first_name} onChange={set("first_name")} placeholder="Prénom" required/>
              <Input label="Nom"    value={form.last_name}  onChange={set("last_name")}  placeholder="Nom"    required/>
            </div>
          )}
          <EmailInput label="Email" value={form.emailPrefix} onChange={set("emailPrefix")}/>
          {mode==="register" && (
            <div style={{ background:"#f0f9ff", border:"1px solid #bae6fd", borderRadius:8, padding:"0.6rem 0.85rem", fontSize:"0.82rem", color:"#0369a1", marginBottom:"0.5rem" }}>
              ⚠️ Seuls les emails pré-autorisés par l'administrateur peuvent s'inscrire.
            </div>
          )}
          <Input label="Mot de passe" type="password" value={form.password} onChange={set("password")} placeholder="••••••••" required/>
          {mode==="register" && (
            <>
              <Input label="GSU (Zone)" value={form.gsu} onChange={set("gsu")} placeholder="ex: Sfax 1A2" required/>
              <div style={{ marginBottom:"1rem" }}>
                <label style={css.label}>Rôle</label>
                <select value={form.role} onChange={set("role")} style={css.input}>
                  <option value="delegue_medical">Délégué Médical</option>
                  <option value="superviseur">Superviseur</option>
                </select>
              </div>
            </>
          )}
          <button style={{ ...css.btn, opacity:loading?0.7:1 }} disabled={loading}>
            {loading?"…":mode==="login"?"Se connecter":"Créer un compte"}
          </button>
        </form>
        <p style={{ textAlign:"center", marginTop:"1.2rem", fontSize:"0.88rem", color:S.textLight }}>
          {mode==="login"?"Pas encore de compte ? ":"Déjà un compte ? "}
          <span style={{ color:S.coral, cursor:"pointer", fontWeight:600 }}
            onClick={()=>{ setMode(mode==="login"?"register":"login"); setError(""); }}>
            {mode==="login"?"S'inscrire":"Se connecter"}
          </span>
        </p>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────
// CHAT
// ─────────────────────────────────────────────────────────────
function ChatPage({ user, onLogout, onAdmin }) {
  const [sessions, setSessions] = useState([]);
  const [activeId, setActiveId] = useState(null);
  const [messages, setMessages] = useState([]);
  const [input, setInput]       = useState("");
  const [loading, setLoading]   = useState(false);
  const bottomRef               = useRef(null);
  const wsRef                   = useRef(null);

  const loadSessions = useCallback(async () => {
    const data = await api.get("/chat/sessions");
    if (Array.isArray(data)) setSessions(data);
  }, []);

  useEffect(()=>{ loadSessions(); }, [loadSessions]);

  useEffect(()=>{
    if (activeId) {
      api.get(`/chat/sessions/${activeId}/messages`).then(data => {
        if (Array.isArray(data)) setMessages(data);
      });
    } else { setMessages([]); }
  }, [activeId]);

  useEffect(()=>{ bottomRef.current?.scrollIntoView({ behavior:"smooth" }); }, [messages, loading]);

  const newChat = async () => {
    const s = await api.post("/chat/sessions", {});
    if (s.id) { await loadSessions(); setActiveId(s.id); setMessages([]); }
  };

  const send = async () => {
    if (!input.trim() || loading) return;
    const q = input.trim(); setInput("");

    let sid = activeId;
    if (!sid) {
      const s = await api.post("/chat/sessions", {});
      sid = s.id;
      setActiveId(sid);
    }

    // Add user message immediately
    setMessages(m => [...m, { id: Date.now(), role: "user", content: q }]);
    setLoading(true);

    // Open WebSocket connection
    const ws = new WebSocket(`${WS_URL}?token=${api.token()}&session_id=${sid}`);
    wsRef.current = ws;

    const assistantId = Date.now() + 1;

    ws.onopen = () => {
      ws.send(JSON.stringify({ message: q }));
    };

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);

      if (data.type === "stream_start") {
        // Insert empty assistant bubble — tokens will fill it progressively
        setMessages(m => [...m, { id: assistantId, role: "assistant", content: "" }]);
        setLoading(false);
      }

      if (data.type === "token") {
        // Append each token to the assistant bubble as it arrives
        setMessages(m => m.map(msg =>
          msg.id === assistantId
            ? { ...msg, content: msg.content + data.content }
            : msg
        ));
      }

      if (data.type === "stream_end") {
        ws.close();
        loadSessions();
      }

      if (data.type === "error") {
        setMessages(m => m.map(msg =>
          msg.id === assistantId
            ? { ...msg, content: "Erreur : " + data.content }
            : msg
        ));
        setLoading(false);
        ws.close();
      }
    };

    ws.onerror = () => {
      setMessages(m => [...m, {
        id: assistantId, role: "assistant", content: "Erreur de connexion au serveur."
      }]);
      setLoading(false);
    };
  };

  const fmt = (iso) => iso ? new Date(iso).toLocaleDateString("fr-FR",{day:"2-digit",month:"short"}) : "";

  return (
    <div style={css.layout}>
      <aside style={css.sidebar}>
        <div style={css.sidebarHeader}>
          <div style={css.sidebarLogo}>wisdom.</div>
          <div style={css.sidebarTagline}>Intelligence Analytique</div>
        </div>
        <button style={css.newChatBtn} onClick={newChat}>＋ Nouvelle conversation</button>
        <div style={css.sidebarSection}>Conversations</div>
        <div style={css.sidebarScroll}>
          {sessions.length===0 && (
            <div style={{ color:S.textLight, textAlign:"center", padding:"2rem 0", fontSize:"0.85rem" }}>
              Aucune conversation
            </div>
          )}
          {sessions.map(s=>(
            <div key={s.id} style={css.chatItem(s.id===activeId)} onClick={()=>setActiveId(s.id)}>
              <div style={css.chatItemTitle}>{s.title}</div>
              <div style={css.chatItemDate}>{fmt(s.updated_at)}</div>
            </div>
          ))}
        </div>
        <div style={css.sidebarFooter}>
          {user.role==="admin" && <div style={css.adminLink} onClick={onAdmin}>⚙ Administration</div>}
          <div style={css.userChip}>
            <div style={css.userAvatar}>{user.first_name?.[0]}{user.last_name?.[0]}</div>
            <div>
              <div style={css.userName}>{user.first_name} {user.last_name}</div>
              <div style={css.userRole}>{roleLabel(user.role)}</div>
            </div>
            <button style={css.logoutBtn} onClick={onLogout} title="Déconnexion">⎋</button>
          </div>
        </div>
      </aside>

      <main style={css.main}>
        {!activeId && messages.length===0 && (
          <div style={css.chatHeader}>
            <div style={css.headerLogo}>wisdom.</div>
            <div style={css.headerSub}>Analyse des ventes & performances Hikma</div>
            <div style={css.headerLine}/>
          </div>
        )}
        <div style={{ ...css.messagesArea, paddingTop:"2rem" }}>
          {messages.map(m=>(
            <div key={m.id} style={css.msgBlock}>
              <div style={css.msgLabel(m.role)}>
                <span style={css.msgDot(m.role)}/>
                {m.role==="user"?"Vous":"Hikma"}
              </div>
              <div style={css.msgBubble(m.role)}>
                <div dangerouslySetInnerHTML={{ __html:(m.content||"").replace(/\n/g,"<br/>") }}/>
                {m.table && <ChatTable table={m.table}/>}
                {m.sources?.length>0 && (
                  <div style={css.sources}>
                    <span style={{ color:S.coral }}>●</span>
                    {m.sources.join(" · ")}
                  </div>
                )}
              </div>
            </div>
          ))}
          {loading && (
            <div style={css.msgBlock}>
              <div style={css.msgLabel("assistant")}><span style={css.msgDot("assistant")}/>Hikma</div>
              <div style={css.msgBubble("assistant")}><Spinner/></div>
            </div>
          )}
          <div ref={bottomRef}/>
        </div>
        <div style={css.inputBar}>
          <div style={css.inputInner}>
            <input
              style={css.inputField}
              value={input}
              onChange={e=>setInput(e.target.value)}
              onKeyDown={e=>e.key==="Enter"&&!e.shiftKey&&send()}
              placeholder="Posez votre question..."
            />
            <button style={css.sendBtn} onClick={send} disabled={loading}>Envoyer</button>
          </div>
        </div>
      </main>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────
// DOCUMENTS TAB (Admin)
// ─────────────────────────────────────────────────────────────
function DocumentsTab() {
  const [status, setStatus]       = useState({});
  const [uploading, setUploading] = useState({ ims:false, target:false });
  const [results, setResults]     = useState([]);
  const imsRef                    = useRef();
  const targetRef                 = useRef();

  const loadStatus = async () => {
    const d = await api.get("/admin/data-status");
    setStatus(d || {});
  };

  useEffect(()=>{ loadStatus(); }, []);

  const upload = async (docType, inputRef) => {
    const files = inputRef.current?.files;
    if (!files || files.length === 0) return;
    setUploading(u=>({...u,[docType]:true}));
    setResults([]);
    const fd = new FormData();
    for (const f of files) fd.append("files", f);
    const res = await api.upload(`/admin/ingest/${docType}`, fd);
    setResults(res.results || []);
    setUploading(u=>({...u,[docType]:false}));
    inputRef.current.value = "";
    await loadStatus();
  };

  const delData = async (docType) => {
    if (!confirm(`Supprimer toutes les données ${docType.toUpperCase()} ? Elles devront être rechargées.`)) return;
    await api.del(`/admin/data/${docType}`);
    await loadStatus();
  };

  const DataZone = ({ docType, label, color, inputRef }) => {
    const info   = status[docType] || {};
    const loaded = info.loaded && info.rows > 0;
    return (
      <div style={{ background:S.white, border:`1px solid ${S.border}`, borderRadius:14, padding:"1.6rem", marginBottom:"1.5rem" }}>
        <div style={{ display:"flex", alignItems:"center", gap:"0.6rem", marginBottom:"1rem" }}>
          <span style={{ display:"inline-block", padding:"0.2rem 0.7rem", borderRadius:20, background:color.bg, color:color.fg, fontWeight:700, fontSize:"0.8rem" }}>{docType.toUpperCase()}</span>
          <span style={{ fontWeight:700 }}>{label}</span>
          {loaded
            ? <span style={{ marginLeft:"auto", padding:"0.2rem 0.7rem", borderRadius:20, background:S.greenLight, color:S.green, fontWeight:600, fontSize:"0.78rem" }}>✓ Chargé</span>
            : <span style={{ marginLeft:"auto", padding:"0.2rem 0.7rem", borderRadius:20, background:"#fee2e2", color:"#dc2626", fontWeight:600, fontSize:"0.78rem" }}>Non chargé</span>
          }
        </div>
        {loaded && (
          <div style={{ background:"#f8f8f8", borderRadius:8, padding:"0.75rem 1rem", marginBottom:"1rem", fontSize:"0.85rem" }}>
            <div><b>{(info.rows||0).toLocaleString()}</b> lignes dans la base</div>
            {info.date_range?.min && <div style={{ color:S.textLight, marginTop:"0.2rem" }}>Période : {info.date_range.min} → {info.date_range.max}</div>}
            {info.columns && <div style={{ color:S.textLight, marginTop:"0.2rem", fontSize:"0.78rem" }}>Colonnes : {(info.columns||[]).join(", ")}</div>}
          </div>
        )}
        <div style={{ display:"flex", gap:"0.75rem", alignItems:"center", marginBottom:"0.5rem" }}>
          <input ref={inputRef} type="file" accept=".xlsx,.xls,.csv" style={{ flex:1, fontSize:"0.85rem", color:S.textMid }}/>
          <button style={{ ...css.btnSm, background:color.fg, whiteSpace:"nowrap" }}
            disabled={uploading[docType]} onClick={() => upload(docType, inputRef)}>
            {uploading[docType] ? "Chargement…" : loaded ? "⬆ Remplacer" : "⬆ Charger"}
          </button>
          {loaded && (
            <button style={{ ...css.btnDanger, padding:"0.5rem 0.8rem" }} onClick={() => delData(docType)}>
              🗑 Supprimer
            </button>
          )}
        </div>
        <div style={{ fontSize:"0.78rem", color:S.textLight }}>
          Formats acceptés : .xlsx, .xls, .csv — les données remplacent les précédentes et persistent après redémarrage.
        </div>
      </div>
    );
  };

  return (
    <>
      <div style={{ fontSize:"1.3rem", fontWeight:700, marginBottom:"0.3rem" }}>📂 Données Analytics</div>
      <div style={{ color:S.textLight, fontSize:"0.9rem", marginBottom:"0.5rem" }}>
        Importez les fichiers IMS et TARGET. Les données sont stockées en base SQLite et persistent après redémarrage du serveur.
      </div>
      <div style={{ background:"#e8f4fd", border:"1px solid #bae6fd", borderRadius:8, padding:"0.75rem 1rem", marginBottom:"1.5rem", fontSize:"0.85rem", color:"#0369a1" }}>
        ℹ️ Architecture Text-to-SQL : l'IA interroge directement vos données Excel via SQL — les réponses sont toujours exactes.
      </div>
      {results.length > 0 && (
        <div style={{ marginBottom:"1rem" }}>
          {results.map((r,i) => (
            <div key={i} style={{ padding:"0.5rem 0.9rem", borderRadius:8, marginBottom:"0.4rem", fontSize:"0.85rem",
              background:r.success?S.greenLight:"#fee2e2", color:r.success?S.green:"#dc2626" }}>
              {r.success ? "✓" : "✗"} {r.filename} — {r.message}
            </div>
          ))}
        </div>
      )}
      <DataZone docType="ims"    label="Données IMS (marché)"          color={{ bg:"#e8f4fd", fg:"#2980b9" }} inputRef={imsRef}/>
      <DataZone docType="target" label="Objectifs commerciaux (TARGET)" color={{ bg:"#fef9e7", fg:"#d68910" }} inputRef={targetRef}/>
    </>
  );
}

// ─────────────────────────────────────────────────────────────
// ADMIN PAGE
// ─────────────────────────────────────────────────────────────
function AdminPage({ user, onBack }) {
  const [tab, setTab]             = useState("users");
  const [users, setUsers]         = useState([]);
  const [stats, setStats]         = useState(null);
  const [whitelist, setWhitelist] = useState([]);
  const [modal, setModal]         = useState(null);
  const [form, setForm]           = useState({});
  const [error, setError]         = useState("");
  const [wForm, setWForm]         = useState({ emailPrefix:"", role:"delegue_medical", gsu:"" });

  const loadUsers     = async () => { const d=await api.get("/admin/users");          if(Array.isArray(d)) setUsers(d); };
  const loadStats     = async () => { const d=await api.get("/admin/stats");           if(d.total_users!==undefined) setStats(d); };
  const loadWhitelist = async () => { const d=await api.get("/admin/allowed-emails"); if(Array.isArray(d)) setWhitelist(d); };

  useEffect(()=>{ loadUsers(); loadStats(); loadWhitelist(); }, []);

  const set = (k) => (e) => setForm(f=>({...f,[k]:e.target.value}));

  const saveUser = async (e) => {
    e.preventDefault(); setError("");
    try {
      if (modal==="add") {
        const res = await api.post("/admin/users", form);
        if (res.detail) { setError(res.detail); return; }
      } else {
        const payload = {...form}; if (!payload.password) delete payload.password;
        const res = await api.patch(`/admin/users/${modal.id}`, payload);
        if (res.detail) { setError(res.detail); return; }
      }
      setModal(null); setForm({}); loadUsers(); loadStats();
    } catch { setError("Erreur serveur"); }
  };

  const deleteUser = async (uid) => {
    if (!confirm("Supprimer cet utilisateur ?")) return;
    await api.del(`/admin/users/${uid}`); loadUsers(); loadStats();
  };

  const openEdit = (u) => {
    setForm({ email:u.email, first_name:u.first_name, last_name:u.last_name, gsu:u.gsu, role:u.role });
    setModal(u); setError("");
  };

  const addWhitelist = async () => {
    if (!wForm.emailPrefix) return;
    const email = wForm.emailPrefix + "@hikma.com";
    const res   = await api.post("/admin/allowed-emails", { email, role:wForm.role, gsu:wForm.gsu });
    if (res.detail) { alert(res.detail); return; }
    setWForm({ emailPrefix:"", role:"delegue_medical", gsu:"" }); loadWhitelist();
  };

  const navItems = [
    { key:"documents", label:"📂 Documents IA" },
    { key:"users",     label:"👥 Utilisateurs" },
    { key:"whitelist", label:"✉️ Emails autorisés" },
    { key:"stats",     label:"📊 Statistiques" },
  ];

  return (
    <div style={css.layout}>
      <aside style={css.sidebar}>
        <div style={css.sidebarHeader}>
          <div style={css.sidebarLogo}>wisdom.</div>
          <div style={css.sidebarTagline}>Administration</div>
        </div>
        <div style={{ padding:"1.2rem 1rem", flex:1 }}>
          {navItems.map(n=>(
            <div key={n.key} style={{ ...css.chatItem(tab===n.key), marginBottom:4 }} onClick={()=>setTab(n.key)}>
              <div style={{ fontWeight:600, fontSize:"0.9rem", color:tab===n.key?S.coral:S.text }}>{n.label}</div>
            </div>
          ))}
        </div>
        <div style={css.sidebarFooter}>
          <div style={{ ...css.adminLink, marginTop:0 }} onClick={onBack}>← Retour au chat</div>
          <div style={{ ...css.userChip, marginTop:"0.8rem" }}>
            <div style={css.userAvatar}>{user.first_name?.[0]}{user.last_name?.[0]}</div>
            <div>
              <div style={css.userName}>{user.first_name} {user.last_name}</div>
              <div style={css.userRole}>Admin</div>
            </div>
          </div>
        </div>
      </aside>

      <div style={css.adminWrap}>

        {tab==="documents" && <DocumentsTab/>}

        {tab==="users" && (
          <>
            <div style={{ display:"flex", justifyContent:"space-between", alignItems:"flex-start", marginBottom:"2rem" }}>
              <div>
                <div style={css.adminTitle}>Gestion des utilisateurs</div>
                <div style={css.adminSub}>{users.length} utilisateur{users.length>1?"s":""}</div>
              </div>
              <button style={css.btnSm} onClick={()=>{ setModal("add"); setForm({role:"delegue_medical"}); setError(""); }}>＋ Ajouter</button>
            </div>
            <table style={css.table}>
              <thead>
                <tr>{["Nom","Email","GSU","Rôle","Statut","Requêtes","Actions"].map(h=><th key={h} style={css.th}>{h}</th>)}</tr>
              </thead>
              <tbody>
                {users.map(u=>(
                  <tr key={u.id} style={{ background:u.is_active?S.white:"#fafafa" }}>
                    <td style={css.td}>{u.first_name} {u.last_name}</td>
                    <td style={{ ...css.td, color:S.textMid }}>{u.email}</td>
                    <td style={css.td}>{u.gsu||"—"}</td>
                    <td style={css.td}><span style={css.badge(u.role)}>{roleLabel(u.role)}</span></td>
                    <td style={css.td}><span style={{ color:u.is_active?"#16a34a":"#dc2626", fontWeight:600, fontSize:"0.82rem" }}>{u.is_active?"Actif":"Désactivé"}</span></td>
                    <td style={css.td}>{u.query_count}</td>
                    <td style={css.td}>
                      <div style={{ display:"flex", gap:"0.5rem" }}>
                        <button style={css.btnGhost} onClick={()=>openEdit(u)}>Modifier</button>
                        {u.role!=="admin" && <button style={css.btnDanger} onClick={()=>deleteUser(u.id)}>Supprimer</button>}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        )}

        {tab==="whitelist" && (
          <>
            <div style={css.adminTitle}>Emails autorisés</div>
            <div style={css.adminSub}>Seuls ces emails @hikma.com peuvent s'inscrire</div>
            <div style={{ background:S.white, border:`1px solid ${S.border}`, borderRadius:14, padding:"1.4rem 1.6rem", marginBottom:"1.5rem" }}>
              <div style={{ fontWeight:600, marginBottom:"1rem" }}>Ajouter un email</div>
              <div style={{ display:"grid", gridTemplateColumns:"2fr 1fr 1fr auto", gap:"0.75rem", alignItems:"end" }}>
                <div>
                  <label style={css.label}>Email</label>
                  <div style={{ display:"flex", border:`1.5px solid ${S.border}`, borderRadius:10, overflow:"hidden" }}>
                    <input value={wForm.emailPrefix} onChange={e=>setWForm(f=>({...f,emailPrefix:e.target.value}))} placeholder="prenom.nom"
                      style={{ ...css.input, border:"none", borderRadius:0, flex:1 }}/>
                    <span style={{ padding:"0 0.7rem", color:S.textLight, fontSize:"0.82rem", background:"#f5f5f5", display:"flex", alignItems:"center", borderLeft:`1px solid ${S.border}`, whiteSpace:"nowrap" }}>@hikma.com</span>
                  </div>
                </div>
                <div><label style={css.label}>GSU</label><input value={wForm.gsu} onChange={e=>setWForm(f=>({...f,gsu:e.target.value}))} placeholder="Sfax 1A2" style={css.input}/></div>
                <div>
                  <label style={css.label}>Rôle</label>
                  <select value={wForm.role} onChange={e=>setWForm(f=>({...f,role:e.target.value}))} style={css.input}>
                    <option value="delegue_medical">Délégué Médical</option>
                    <option value="superviseur">Superviseur</option>
                  </select>
                </div>
                <button style={{ ...css.btnSm, height:42 }} onClick={addWhitelist}>Ajouter</button>
              </div>
            </div>
            <table style={css.table}>
              <thead><tr>{["Email","GSU","Rôle","Ajouté le","Action"].map(h=><th key={h} style={css.th}>{h}</th>)}</tr></thead>
              <tbody>
                {whitelist.length===0 && <tr><td style={{ ...css.td, color:S.textLight }} colSpan={5}>Aucun email autorisé</td></tr>}
                {whitelist.map(e=>(
                  <tr key={e.id}>
                    <td style={css.td}>{e.email}</td>
                    <td style={css.td}>{e.gsu||"—"}</td>
                    <td style={css.td}><span style={css.badge(e.role)}>{roleLabel(e.role)}</span></td>
                    <td style={css.td}>{new Date(e.created_at).toLocaleDateString("fr-FR")}</td>
                    <td style={css.td}>
                      <button style={css.btnDanger} onClick={async()=>{
                        if (!confirm("Supprimer ?")) return;
                        await api.del(`/admin/allowed-emails/${e.id}`); loadWhitelist();
                      }}>Supprimer</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        )}

        {tab==="stats" && (
          <>
            <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center", marginBottom:"0.3rem" }}>
              <div style={css.adminTitle}>Statistiques</div>
              <button style={{ ...css.btnSm, background:S.green }}
                onClick={async () => {
                  const res  = await fetch(`${API}/admin/export-excel`, { headers:{ Authorization:`Bearer ${api.token()}` }});
                  const blob = await res.blob();
                  const url  = URL.createObjectURL(blob);
                  const a    = document.createElement("a");
                  a.href     = url;
                  a.download = `hikma_stats_${new Date().toISOString().slice(0,10)}.xlsx`;
                  a.click(); URL.revokeObjectURL(url);
                }}>📥 Exporter Excel</button>
            </div>
            <div style={css.adminSub}>Vue d'ensemble de l'utilisation</div>
            {!stats ? <div style={{ color:S.textLight }}>Chargement...</div> : (
              <>
                <div style={{ display:"grid", gridTemplateColumns:"repeat(4,1fr)", gap:"1rem", marginBottom:"1rem" }}>
                  <div style={css.statCard}><div style={css.statNum}>{stats.total_users}</div><div style={css.statLabel}>Utilisateurs autorisés</div></div>
                  <div style={css.statCard}><div style={css.statNum}>{stats.active_users}</div><div style={css.statLabel}>Utilisateurs actifs</div></div>
                  <div style={css.statCard}><div style={{ ...css.statNum, color:"#0369a1" }}>{stats.avg_response_s ?? "—"}<span style={{ fontSize:"1rem", fontWeight:400 }}>s</span></div><div style={css.statLabel}>Temps moyen réponse</div></div>
                  <div style={css.statCard}><div style={css.statNum}>{stats.total_queries}</div><div style={css.statLabel}>Questions posées</div></div>
                </div>
                <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:"1rem", marginBottom:"2rem" }}>
                  <div style={{ ...css.statCard, borderLeft:`4px solid ${S.coral}` }}><div style={css.statNum}>{stats.active_delegues}</div><div style={css.statLabel}>Délégués médicaux actifs</div></div>
                  <div style={{ ...css.statCard, borderLeft:"4px solid #7c3aed" }}><div style={{ ...css.statNum, color:"#7c3aed" }}>{stats.active_superviseurs}</div><div style={css.statLabel}>Superviseurs actifs</div></div>
                </div>
                <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:"1.5rem" }}>
                  <div>
                    <div style={{ fontWeight:700, marginBottom:"0.8rem" }}>🏆 Top utilisateurs</div>
                    <table style={css.table}><thead><tr><th style={css.th}>Nom</th><th style={css.th}>Rôle</th><th style={css.th}>Questions</th></tr></thead>
                    <tbody>{!stats.top_users.length
                      ? <tr><td style={{ ...css.td,color:S.textLight }} colSpan={3}>Aucune donnée</td></tr>
                      : stats.top_users.map((u,i)=><tr key={i}><td style={css.td}>{u.name}</td><td style={css.td}><span style={css.badge(u.role)}>{roleLabel(u.role)}</span></td><td style={css.td}><strong>{u.count}</strong></td></tr>)}
                    </tbody></table>
                  </div>
                  <div>
                    <div style={{ fontWeight:700, marginBottom:"0.8rem" }}>📉 Moins actifs</div>
                    <table style={css.table}><thead><tr><th style={css.th}>Nom</th><th style={css.th}>Rôle</th><th style={css.th}>Questions</th></tr></thead>
                    <tbody>{!stats.least_users.length
                      ? <tr><td style={{ ...css.td,color:S.textLight }} colSpan={3}>Aucune donnée</td></tr>
                      : stats.least_users.map((u,i)=><tr key={i}><td style={css.td}>{u.name}</td><td style={css.td}><span style={css.badge(u.role)}>{roleLabel(u.role)}</span></td><td style={css.td}><strong>{u.count}</strong></td></tr>)}
                    </tbody></table>
                  </div>
                  <div style={{ gridColumn:"1 / -1" }}>
                    <div style={{ fontWeight:700, marginBottom:"0.8rem" }}>🔁 Questions répétées</div>
                    <table style={css.table}><thead><tr><th style={css.th}>Question</th><th style={css.th}>Répétitions</th></tr></thead>
                    <tbody>{!(stats.repeated_questions||[]).length
                      ? <tr><td style={{ ...css.td,color:S.textLight }} colSpan={2}>Aucune question répétée</td></tr>
                      : (stats.repeated_questions||[]).map((q,i)=><tr key={i}><td style={css.td}>{q.question}</td><td style={css.td}><strong style={{ color:S.coral }}>{q.count}×</strong></td></tr>)}
                    </tbody></table>
                  </div>
                  <div>
                    <div style={{ fontWeight:700, marginBottom:"0.8rem" }}>💊 Produits questionnés</div>
                    <table style={css.table}><thead><tr><th style={css.th}>Terme</th><th style={css.th}>Mentions</th></tr></thead>
                    <tbody>{!(stats.top_products||[]).length
                      ? <tr><td style={{ ...css.td,color:S.textLight }} colSpan={2}>Aucune donnée</td></tr>
                      : (stats.top_products||[]).map((p,i)=><tr key={i}><td style={css.td}>{p.product}</td><td style={css.td}><strong style={{ color:S.coral }}>{p.count}</strong></td></tr>)}
                    </tbody></table>
                  </div>
                  <div>
                    <div style={{ fontWeight:700, marginBottom:"0.8rem" }}>❓ Top questions</div>
                    <table style={css.table}><thead><tr><th style={css.th}>Question</th><th style={css.th}>Nb</th></tr></thead>
                    <tbody>{!(stats.top_questions||[]).length
                      ? <tr><td style={{ ...css.td,color:S.textLight }} colSpan={2}>Aucune donnée</td></tr>
                      : (stats.top_questions||[]).map((q,i)=><tr key={i}><td style={css.td}>{q.question}</td><td style={css.td}><strong>{q.count}</strong></td></tr>)}
                    </tbody></table>
                  </div>
                  <div style={{ gridColumn:"1 / -1" }}>
                    <div style={{ fontWeight:700, marginBottom:"0.8rem" }}>📈 Croissance des utilisateurs</div>
                    <div style={{ background:S.white, border:`1px solid ${S.border}`, borderRadius:14, padding:"1.5rem 1rem 1rem", boxShadow:"0 1px 4px rgba(0,0,0,0.05)" }}>
                      <LineChart data={stats.users_over_time||[]}/>
                    </div>
                  </div>
                </div>
              </>
            )}
          </>
        )}
      </div>

      {modal && (
        <div style={css.modal} onClick={e=>e.target===e.currentTarget&&setModal(null)}>
          <div style={css.modalCard}>
            <div style={{ fontWeight:700, fontSize:"1.2rem", marginBottom:"1.5rem" }}>
              {modal==="add"?"Ajouter un utilisateur":"Modifier l'utilisateur"}
            </div>
            {error && <div style={css.error}>{error}</div>}
            <form onSubmit={saveUser}>
              <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:"0 1rem" }}>
                <Input label="Prénom" value={form.first_name||""} onChange={set("first_name")} required/>
                <Input label="Nom"    value={form.last_name||""}  onChange={set("last_name")}  required/>
              </div>
              <Input label="Email" type="email" value={form.email||""} onChange={set("email")} required/>
              {modal==="add" && <Input label="Mot de passe" type="password" value={form.password||""} onChange={set("password")} required/>}
              <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:"0 1rem" }}>
                <Input label="GSU (Zone)" value={form.gsu||""} onChange={set("gsu")} placeholder="ex: Sfax 1A2" required/>
                <div style={{ marginBottom:"1rem" }}>
                  <label style={css.label}>Rôle</label>
                  <select value={form.role||"delegue_medical"} onChange={set("role")} style={css.input}>
                    <option value="delegue_medical">Délégué Médical</option>
                    <option value="superviseur">Superviseur</option>
                    <option value="admin">Admin</option>
                  </select>
                </div>
              </div>
              <div style={{ display:"flex", gap:"0.75rem", justifyContent:"flex-end", marginTop:"1rem" }}>
                <button type="button" style={css.btnGhost} onClick={()=>setModal(null)}>Annuler</button>
                <button type="submit" style={css.btnSm}>Enregistrer</button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────
// APP ROOT
// ─────────────────────────────────────────────────────────────
export default function App() {
  const [user, setUser]       = useState(null);
  const [page, setPage]       = useState("login");
  const [loading, setLoading] = useState(true);

  useEffect(()=>{
    const token = localStorage.getItem("token");
    if (token) {
      api.get("/auth/me")
        .then(data=>{ if(data.id){ setUser(data); setPage("chat"); } else { localStorage.removeItem("token"); } })
        .catch(()=>{ localStorage.removeItem("token"); })
        .finally(()=>setLoading(false));
    } else { setLoading(false); }
  }, []);

  const logout = () => { localStorage.removeItem("token"); setUser(null); setPage("login"); };

  if (loading) return (
    <div style={{ ...css.loginWrap, ...css.app }}>
      <div style={{ color:S.coral, fontSize:"2rem", fontWeight:700 }}>wisdom.</div>
    </div>
  );

  return (
    <div style={css.app}>
      <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet"/>
      {page==="login" && <LoginPage onLogin={u=>{ setUser(u); setPage("chat"); }}/>}
      {page==="chat"  && <ChatPage  user={user} onLogout={logout} onAdmin={()=>setPage("admin")}/>}
      {page==="admin" && user?.role==="admin" && <AdminPage user={user} onBack={()=>setPage("chat")}/>}
    </div>
  );
}