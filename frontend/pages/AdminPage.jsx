import { useState, useEffect, useRef } from "react";
import { api, API_BASE, roleLabel } from "../api.js";
import { S, css } from "../styles.js";
import { Input, EmailInput, LineChart } from "../components.jsx";

// ── Documents tab ─────────────────────────────────────────────────────────────
function DocumentsTab() {
  const [status, setStatus]       = useState({});
  const [uploading, setUploading] = useState({ ims: false, target: false });
  const [results, setResults]     = useState([]);
  const imsRef    = useRef();
  const targetRef = useRef();

  const loadStatus = async () => {
    const d = await api.get("/admin/data-status");
    if (d) setStatus(d);
  };
  useEffect(() => { loadStatus(); }, []);

  const upload = async (docType, inputRef) => {
    const files = inputRef.current?.files;
    if (!files?.length) return;
    setUploading(u => ({ ...u, [docType]: true }));
    setResults([]);
    const fd = new FormData();
    for (const f of files) fd.append("files", f);
    const res = await api.upload(`/admin/ingest/${docType}`, fd);
    setResults(res.results || []);
    setUploading(u => ({ ...u, [docType]: false }));
    inputRef.current.value = "";
    await loadStatus();
  };

  const delData = async docType => {
    if (!confirm(`Supprimer toutes les données ${docType.toUpperCase()} ?`)) return;
    await api.del(`/admin/data/${docType}`);
    await loadStatus();
  };

  const DataZone = ({ docType, label, color, inputRef }) => {
    const info   = status[docType] || {};
    const loaded = info.loaded && info.rows > 0;
    return (
      <div style={{ background: S.white, border: `1px solid ${S.border}`, borderRadius: 14, padding: "1.6rem", marginBottom: "1.5rem" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.6rem", marginBottom: "1rem" }}>
          <span style={{ padding: "0.2rem 0.7rem", borderRadius: 20, background: color.bg, color: color.fg, fontWeight: 700, fontSize: "0.8rem" }}>{docType.toUpperCase()}</span>
          <span style={{ fontWeight: 700 }}>{label}</span>
          {loaded
            ? <span style={{ marginLeft: "auto", padding: "0.2rem 0.7rem", borderRadius: 20, background: S.greenLight, color: S.green, fontWeight: 600, fontSize: "0.78rem" }}>✓ Chargé</span>
            : <span style={{ marginLeft: "auto", padding: "0.2rem 0.7rem", borderRadius: 20, background: "#fee2e2", color: "#dc2626", fontWeight: 600, fontSize: "0.78rem" }}>Non chargé</span>
          }
        </div>
        {loaded && (
          <div style={{ background: "#f8f8f8", borderRadius: 8, padding: "0.75rem 1rem", marginBottom: "1rem", fontSize: "0.85rem" }}>
            <div><b>{(info.rows || 0).toLocaleString()}</b> lignes</div>
            {info.date_range?.min && <div style={{ color: S.textLight, marginTop: "0.2rem" }}>Période : {info.date_range.min} → {info.date_range.max}</div>}
          </div>
        )}
        <div style={{ display: "flex", gap: "0.75rem", alignItems: "center", marginBottom: "0.5rem" }}>
          <input ref={inputRef} type="file" accept=".xlsx,.xls,.csv" style={{ flex: 1, fontSize: "0.85rem" }} />
          <button style={{ ...css.btnSm, background: color.fg, whiteSpace: "nowrap" }} disabled={uploading[docType]} onClick={() => upload(docType, inputRef)}>
            {uploading[docType] ? "Chargement…" : loaded ? "⬆ Remplacer" : "⬆ Charger"}
          </button>
          {loaded && <button style={{ ...css.btnDanger, padding: "0.5rem 0.8rem" }} onClick={() => delData(docType)}>🗑</button>}
        </div>
        <div style={{ fontSize: "0.78rem", color: S.textLight }}>Formats : .xlsx, .xls, .csv</div>
      </div>
    );
  };

  return (
    <>
      <div style={{ fontSize: "1.3rem", fontWeight: 700, marginBottom: "0.5rem" }}>📂 Données Analytics</div>
      <div style={{ background: "#e8f4fd", border: "1px solid #bae6fd", borderRadius: 8, padding: "0.75rem 1rem", marginBottom: "1.5rem", fontSize: "0.85rem", color: "#0369a1" }}>
        ℹ️ L'IA interroge directement vos données via SQL — les réponses sont précises.
      </div>
      {results.length > 0 && (
        <div style={{ marginBottom: "1rem" }}>
          {results.map((r, i) => (
            <div key={i} style={{ padding: "0.5rem 0.9rem", borderRadius: 8, marginBottom: "0.4rem", fontSize: "0.85rem", background: r.success ? S.greenLight : "#fee2e2", color: r.success ? S.green : "#dc2626" }}>
              {r.success ? "✓" : "✗"} {r.filename} — {r.message}
            </div>
          ))}
        </div>
      )}
      <DataZone docType="ims"    label="Données IMS (marché)"           color={{ bg: "#e8f4fd", fg: "#2980b9" }} inputRef={imsRef} />
      <DataZone docType="target" label="Objectifs commerciaux (TARGET)" color={{ bg: "#fef9e7", fg: "#d68910" }} inputRef={targetRef} />
    </>
  );
}

// ── Users tab ─────────────────────────────────────────────────────────────────
function UsersTab({ onStatsRefresh }) {
  const [users, setUsers]   = useState([]);
  const [modal, setModal]   = useState(null);   // null | "add" | user-object
  const [form, setForm]     = useState({});
  const [error, setError]   = useState("");

  const load = async () => { const d = await api.get("/admin/users"); if (Array.isArray(d)) setUsers(d); };
  useEffect(() => { load(); }, []);

  const set = k => e => setForm(f => ({ ...f, [k]: e.target.value }));

  const save = async e => {
    e.preventDefault(); setError("");
    try {
      let res;
      if (modal === "add") {
        res = await api.post("/admin/users", form);
      } else {
        const payload = { ...form }; if (!payload.password) delete payload.password;
        res = await api.patch(`/admin/users/${modal.id}`, payload);
      }
      if (res.detail) { setError(res.detail); return; }
      setModal(null); setForm({});
      load(); onStatsRefresh?.();
    } catch { setError("Erreur serveur"); }
  };

  const del = async id => {
    if (!confirm("Supprimer cet utilisateur ?")) return;
    await api.del(`/admin/users/${id}`);
    load(); onStatsRefresh?.();
  };

  const openEdit = u => { setForm({ email: u.email, first_name: u.first_name, last_name: u.last_name, gsu: u.gsu, role: u.role }); setModal(u); setError(""); };

  return (
    <>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: "1.5rem" }}>
        <div>
          <div style={css.adminTitle}>Gestion des utilisateurs</div>
          <div style={css.adminSub}>{users.length} utilisateur{users.length > 1 ? "s" : ""}</div>
        </div>
        <button style={css.btnSm} onClick={() => { setModal("add"); setForm({ role: "delegue_medical" }); setError(""); }}>＋ Ajouter</button>
      </div>

      <table style={css.table}>
        <thead>
          <tr>{["Nom", "Email", "GSU", "Rôle", "Statut", "Requêtes", "Actions"].map(h => <th key={h} style={css.th}>{h}</th>)}</tr>
        </thead>
        <tbody>
          {users.map(u => (
            <tr key={u.id} style={{ background: u.is_active ? S.white : "#fafafa" }}>
              <td style={css.td}>{u.first_name} {u.last_name}</td>
              <td style={{ ...css.td, color: S.textMid }}>{u.email}</td>
              <td style={css.td}>{u.gsu || "—"}</td>
              <td style={css.td}><span style={css.badge(u.role)}>{roleLabel(u.role)}</span></td>
              <td style={css.td}><span style={{ color: u.is_active ? "#16a34a" : "#dc2626", fontWeight: 600, fontSize: "0.82rem" }}>{u.is_active ? "Actif" : "Désactivé"}</span></td>
              <td style={css.td}>{u.query_count}</td>
              <td style={css.td}>
                <div style={{ display: "flex", gap: "0.5rem" }}>
                  <button style={css.btnGhost} onClick={() => openEdit(u)}>Modifier</button>
                  {u.role !== "admin" && <button style={css.btnDanger} onClick={() => del(u.id)}>Supprimer</button>}
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {modal && (
        <div style={css.modal} onClick={e => e.target === e.currentTarget && setModal(null)}>
          <div style={css.modalCard}>
            <div style={{ fontWeight: 700, fontSize: "1.2rem", marginBottom: "1.5rem" }}>
              {modal === "add" ? "Ajouter un utilisateur" : "Modifier l'utilisateur"}
            </div>
            {error && <div style={css.error}>{error}</div>}
            <form onSubmit={save}>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0 1rem" }}>
                <Input label="Prénom" value={form.first_name || ""} onChange={set("first_name")} required />
                <Input label="Nom"    value={form.last_name  || ""} onChange={set("last_name")}  required />
              </div>
              <Input label="Email" type="email" value={form.email || ""} onChange={set("email")} required />
              {modal === "add" && <Input label="Mot de passe" type="password" value={form.password || ""} onChange={set("password")} required />}
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0 1rem" }}>
                <Input label="GSU" value={form.gsu || ""} onChange={set("gsu")} placeholder="ex: Sfax 1A2" />
                <div style={{ marginBottom: "1rem" }}>
                  <label style={css.label}>Rôle</label>
                  <select value={form.role || "delegue_medical"} onChange={set("role")} style={css.input}>
                    <option value="delegue_medical">Délégué Médical</option>
                    <option value="superviseur">Superviseur</option>
                    <option value="admin">Admin</option>
                  </select>
                </div>
              </div>
              <div style={{ display: "flex", gap: "0.75rem", justifyContent: "flex-end", marginTop: "1rem" }}>
                <button type="button" style={css.btnGhost} onClick={() => setModal(null)}>Annuler</button>
                <button type="submit" style={css.btnSm}>Enregistrer</button>
              </div>
            </form>
          </div>
        </div>
      )}
    </>
  );
}

// ── Whitelist tab ─────────────────────────────────────────────────────────────
function WhitelistTab() {
  const [whitelist, setWhitelist] = useState([]);
  const [wForm, setWForm]         = useState({ emailPrefix: "", role: "delegue_medical", gsu: "" });

  const load = async () => { const d = await api.get("/admin/allowed-emails"); if (Array.isArray(d)) setWhitelist(d); };
  useEffect(() => { load(); }, []);

  const add = async () => {
    if (!wForm.emailPrefix) return;
    const res = await api.post("/admin/allowed-emails", { email: wForm.emailPrefix + "@hikma.com", role: wForm.role, gsu: wForm.gsu });
    if (res.detail) { alert(res.detail); return; }
    setWForm({ emailPrefix: "", role: "delegue_medical", gsu: "" });
    load();
  };

  return (
    <>
      <div style={css.adminTitle}>Emails autorisés</div>
      <div style={css.adminSub}>Seuls ces emails @hikma.com peuvent s'inscrire</div>

      <div style={{ background: S.white, border: `1px solid ${S.border}`, borderRadius: 14, padding: "1.4rem 1.6rem", marginBottom: "1.5rem" }}>
        <div style={{ fontWeight: 600, marginBottom: "1rem" }}>Ajouter un email</div>
        <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr 1fr auto", gap: "0.75rem", alignItems: "end" }}>
          <div>
            <label style={css.label}>Email</label>
            <div style={{ display: "flex", border: `1.5px solid ${S.border}`, borderRadius: 10, overflow: "hidden" }}>
              <input value={wForm.emailPrefix} onChange={e => setWForm(f => ({ ...f, emailPrefix: e.target.value }))} placeholder="prenom.nom" style={{ ...css.input, border: "none", borderRadius: 0, flex: 1 }} />
              <span style={{ padding: "0 0.7rem", color: S.textLight, fontSize: "0.82rem", background: "#f5f5f5", display: "flex", alignItems: "center", borderLeft: `1px solid ${S.border}`, whiteSpace: "nowrap" }}>@hikma.com</span>
            </div>
          </div>
          <div><label style={css.label}>GSU</label><input value={wForm.gsu} onChange={e => setWForm(f => ({ ...f, gsu: e.target.value }))} placeholder="Sfax 1A2" style={css.input} /></div>
          <div>
            <label style={css.label}>Rôle</label>
            <select value={wForm.role} onChange={e => setWForm(f => ({ ...f, role: e.target.value }))} style={css.input}>
              <option value="delegue_medical">Délégué Médical</option>
              <option value="superviseur">Superviseur</option>
            </select>
          </div>
          <button style={{ ...css.btnSm, height: 42 }} onClick={add}>Ajouter</button>
        </div>
      </div>

      <table style={css.table}>
        <thead><tr>{["Email", "GSU", "Rôle", "Ajouté le", "Action"].map(h => <th key={h} style={css.th}>{h}</th>)}</tr></thead>
        <tbody>
          {whitelist.length === 0 && <tr><td style={{ ...css.td, color: S.textLight }} colSpan={5}>Aucun email autorisé</td></tr>}
          {whitelist.map(e => (
            <tr key={e.id}>
              <td style={css.td}>{e.email}</td>
              <td style={css.td}>{e.gsu || "—"}</td>
              <td style={css.td}><span style={css.badge(e.role)}>{roleLabel(e.role)}</span></td>
              <td style={css.td}>{new Date(e.created_at).toLocaleDateString("fr-FR")}</td>
              <td style={css.td}>
                <button style={css.btnDanger} onClick={async () => { if (!confirm("Supprimer ?")) return; await api.del(`/admin/allowed-emails/${e.id}`); load(); }}>Supprimer</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}

// ── Stats tab ─────────────────────────────────────────────────────────────────
function StatsTab() {
  const [stats, setStats] = useState(null);

  const load = async () => { const d = await api.get("/admin/stats"); if (d?.total_users !== undefined) setStats(d); };
  useEffect(() => { load(); }, []);

  const exportExcel = async () => {
    const res  = await fetch(`${API_BASE}/admin/export-excel`, { headers: { Authorization: `Bearer ${api.token()}` } });
    const blob = await res.blob();
    const url  = URL.createObjectURL(blob);
    Object.assign(document.createElement("a"), { href: url, download: `hikma_stats_${new Date().toISOString().slice(0, 10)}.xlsx` }).click();
    URL.revokeObjectURL(url);
  };

  if (!stats) return <div style={{ color: S.textLight }}>Chargement…</div>;

  return (
    <>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "0.3rem" }}>
        <div style={css.adminTitle}>Statistiques</div>
        <button style={{ ...css.btnSm, background: S.green }} onClick={exportExcel}>📥 Exporter Excel</button>
      </div>
      <div style={css.adminSub}>Vue d'ensemble de l'utilisation</div>

      {/* KPI cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: "1rem", marginBottom: "1rem" }}>
        {[
          { num: stats.total_users,   label: "Utilisateurs autorisés" },
          { num: stats.active_users,  label: "Utilisateurs actifs" },
          { num: (stats.avg_response_s ?? "—") + "s", label: "Temps moyen réponse", color: "#0369a1" },
          { num: stats.total_queries, label: "Questions posées" },
        ].map(({ num, label, color }) => (
          <div key={label} style={css.statCard}>
            <div style={{ ...css.statNum, ...(color ? { color } : {}) }}>{num}</div>
            <div style={css.statLabel}>{label}</div>
          </div>
        ))}
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1rem", marginBottom: "2rem" }}>
        <div style={{ ...css.statCard, borderLeft: `4px solid ${S.coral}` }}><div style={css.statNum}>{stats.active_delegues}</div><div style={css.statLabel}>Délégués médicaux actifs</div></div>
        <div style={{ ...css.statCard, borderLeft: "4px solid #7c3aed" }}><div style={{ ...css.statNum, color: "#7c3aed" }}>{stats.active_superviseurs}</div><div style={css.statLabel}>Superviseurs actifs</div></div>
      </div>

      {/* Tables grid */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "1.5rem" }}>
        {[
          { title: "🏆 Top utilisateurs", rows: stats.top_users,    cols: ["Nom", "Rôle", "Questions"] },
          { title: "📉 Moins actifs",     rows: stats.least_users,  cols: ["Nom", "Rôle", "Questions"] },
        ].map(({ title, rows, cols }) => (
          <div key={title}>
            <div style={{ fontWeight: 700, marginBottom: "0.8rem" }}>{title}</div>
            <table style={css.table}>
              <thead><tr>{cols.map(c => <th key={c} style={css.th}>{c}</th>)}</tr></thead>
              <tbody>
                {!rows?.length
                  ? <tr><td style={{ ...css.td, color: S.textLight }} colSpan={cols.length}>Aucune donnée</td></tr>
                  : rows.map((u, i) => (
                    <tr key={i}>
                      <td style={css.td}>{u.name}</td>
                      <td style={css.td}><span style={css.badge(u.role)}>{roleLabel(u.role)}</span></td>
                      <td style={css.td}><strong>{u.count}</strong></td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        ))}

        <div style={{ gridColumn: "1 / -1" }}>
          <div style={{ fontWeight: 700, marginBottom: "0.8rem" }}>🔁 Questions répétées</div>
          <table style={css.table}>
            <thead><tr>{["Question", "Répétitions"].map(c => <th key={c} style={css.th}>{c}</th>)}</tr></thead>
            <tbody>
              {!stats.repeated_questions?.length
                ? <tr><td style={{ ...css.td, color: S.textLight }} colSpan={2}>Aucune question répétée</td></tr>
                : stats.repeated_questions.map((q, i) => (
                  <tr key={i}><td style={css.td}>{q.question}</td><td style={css.td}><strong style={{ color: S.coral }}>{q.count}×</strong></td></tr>
                ))}
            </tbody>
          </table>
        </div>

        <div>
          <div style={{ fontWeight: 700, marginBottom: "0.8rem" }}>💊 Produits questionnés</div>
          <table style={css.table}>
            <thead><tr>{["Terme", "Mentions"].map(c => <th key={c} style={css.th}>{c}</th>)}</tr></thead>
            <tbody>
              {!stats.top_products?.length
                ? <tr><td style={{ ...css.td, color: S.textLight }} colSpan={2}>Aucune donnée</td></tr>
                : stats.top_products.map((p, i) => (
                  <tr key={i}><td style={css.td}>{p.product}</td><td style={css.td}><strong style={{ color: S.coral }}>{p.count}</strong></td></tr>
                ))}
            </tbody>
          </table>
        </div>

        <div>
          <div style={{ fontWeight: 700, marginBottom: "0.8rem" }}>❓ Top questions</div>
          <table style={css.table}>
            <thead><tr>{["Question", "Nb"].map(c => <th key={c} style={css.th}>{c}</th>)}</tr></thead>
            <tbody>
              {!stats.top_questions?.length
                ? <tr><td style={{ ...css.td, color: S.textLight }} colSpan={2}>Aucune donnée</td></tr>
                : stats.top_questions.map((q, i) => (
                  <tr key={i}><td style={css.td}>{q.question}</td><td style={css.td}><strong>{q.count}</strong></td></tr>
                ))}
            </tbody>
          </table>
        </div>

        <div style={{ gridColumn: "1 / -1" }}>
          <div style={{ fontWeight: 700, marginBottom: "0.8rem" }}>📈 Croissance des utilisateurs</div>
          <div style={{ background: S.white, border: `1px solid ${S.border}`, borderRadius: 14, padding: "1.5rem 1rem 1rem", boxShadow: "0 1px 4px rgba(0,0,0,0.05)" }}>
            <LineChart data={stats.users_over_time || []} />
          </div>
        </div>
      </div>
    </>
  );
}

// ── AdminPage root ────────────────────────────────────────────────────────────
const NAV = [
  { key: "documents", label: "📂 Documents IA" },
  { key: "users",     label: "👥 Utilisateurs" },
  { key: "whitelist", label: "✉️ Emails autorisés" },
  { key: "stats",     label: "📊 Statistiques" },
];

export default function AdminPage({ user, onBack }) {
  const [tab, setTab] = useState("users");

  return (
    <div style={css.layout}>
      <aside style={css.sidebar}>
        <div style={css.sidebarHeader}>
          <div style={css.sidebarLogo}>wisdom.</div>
          <div style={css.sidebarTagline}>Administration</div>
        </div>

        <div style={{ padding: "1.2rem 1rem", flex: 1 }}>
          {NAV.map(n => (
            <div
              key={n.key}
              style={{ ...css.chatItem(tab === n.key), marginBottom: 4 }}
              onClick={() => setTab(n.key)}
            >
              <div style={{ fontWeight: 600, fontSize: "0.9rem", color: tab === n.key ? S.coral : S.text }}>{n.label}</div>
            </div>
          ))}
        </div>

        <div style={css.sidebarFooter}>
          <div style={{ ...css.adminLink, marginTop: 0 }} onClick={onBack}>← Retour au chat</div>
          <div style={{ ...css.userChip, marginTop: "0.8rem" }}>
            <div style={css.userAvatar}>{user.first_name?.[0]}{user.last_name?.[0]}</div>
            <div>
              <div style={css.userName}>{user.first_name} {user.last_name}</div>
              <div style={css.userRole}>Admin</div>
            </div>
          </div>
        </div>
      </aside>

      <div style={css.adminWrap}>
        {tab === "documents" && <DocumentsTab />}
        {tab === "users"     && <UsersTab />}
        {tab === "whitelist" && <WhitelistTab />}
        {tab === "stats"     && <StatsTab />}
      </div>
    </div>
  );
}