import { useState } from "react";
import { S, css } from "./styles";

// ── Input ─────────────────────────────────────────────────────────────────────
export function Input({ label, ...props }) {
  const [focused, setFocused] = useState(false);
  return (
    <div style={{ marginBottom: "1rem" }}>
      {label && <label style={css.label}>{label}</label>}
      <input
        {...props}
        style={{ ...css.input, borderColor: focused ? S.coral : S.border }}
        onFocus={() => setFocused(true)}
        onBlur={() => setFocused(false)}
      />
    </div>
  );
}

// ── EmailInput (@hikma.com suffix) ────────────────────────────────────────────
export function EmailInput({ value, onChange, label }) {
  return (
    <div style={{ marginBottom: "1rem" }}>
      {label && <label style={css.label}>{label}</label>}
      <div style={{ display: "flex", border: `1.5px solid ${S.border}`, borderRadius: 10, overflow: "hidden", background: S.white }}>
        <input
          value={value}
          onChange={onChange}
          placeholder="prenom.nom"
          required
          style={{ ...css.input, border: "none", borderRadius: 0, flex: 1 }}
        />
        <span style={{ padding: "0 0.9rem", color: S.textLight, fontSize: "0.88rem", background: "#f5f5f5", display: "flex", alignItems: "center", borderLeft: `1px solid ${S.border}`, whiteSpace: "nowrap" }}>
          @hikma.com
        </span>
      </div>
    </div>
  );
}

// ── Spinner ───────────────────────────────────────────────────────────────────
export function Spinner() {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", padding: "0.8rem 1.4rem" }}>
      {[0, 0.2, 0.4].map((d, i) => (
        <div key={i} style={{ width: 8, height: 8, borderRadius: "50%", background: S.coral, animation: `pulse 1s ${d}s infinite` }} />
      ))}
      <style>{`@keyframes pulse{0%,100%{opacity:.3;transform:scale(.8)}50%{opacity:1;transform:scale(1)}}`}</style>
    </div>
  );
}

// ── BarChart (embedded inside ChatTable) ──────────────────────────────────────
function BarChart({ table }) {
  if (!table?.headers || !table?.rows?.length) return null;

  let valueCol = -1;
  for (let c = 1; c < table.headers.length; c++) {
    const vals = table.rows.map(r => parseFloat(String(r[c]).replace(/[,\s]/g, "")));
    if (vals.some(v => !isNaN(v))) { valueCol = c; break; }
  }
  if (valueCol === -1) return null;

  const labels = table.rows.map(r => String(r[0]).slice(0, 16));
  const values = table.rows.map(r => parseFloat(String(r[valueCol]).replace(/[,\s]/g, "")) || 0);
  const maxVal = Math.max(...values, 1);

  const pad  = { top: 20, right: 20, bottom: 60, left: 60 };
  const W    = Math.max(400, labels.length * 70 + pad.left + pad.right);
  const H    = 240;
  const iW   = W - pad.left - pad.right;
  const iH   = H - pad.top - pad.bottom;
  const barW = Math.max(20, Math.min(50, (iW / labels.length) * 0.7));
  const gap  = iW / labels.length;

  const bx = i => pad.left + i * gap + (gap - barW) / 2;
  const by = v => pad.top + iH - (v / maxVal) * iH;
  const bh = v => (v / maxVal) * iH;
  const yTicks = [0, 0.25, 0.5, 0.75, 1].map(t => maxVal * t);

  return (
    <div style={{ overflowX: "auto", marginTop: "1rem" }}>
      <svg width={W} height={H} style={{ display: "block", minWidth: "100%" }}>
        {yTicks.map((v, i) => (
          <g key={i}>
            <line x1={pad.left} y1={by(v)} x2={W - pad.right} y2={by(v)} stroke={S.border} strokeWidth="1" strokeDasharray="4,4" />
            <text x={pad.left - 6} y={by(v) + 4} textAnchor="end" fontSize="10" fill={S.textLight}>
              {v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v.toFixed(0)}
            </text>
          </g>
        ))}
        {values.map((v, i) => (
          <g key={i}>
            <rect x={bx(i)} y={by(v)} width={barW} height={bh(v)} fill={S.coral} rx="4" opacity="0.9" />
            <text x={bx(i) + barW / 2} y={by(v) - 5} textAnchor="middle" fontSize="10" fontWeight="600" fill={S.coralDark}>
              {v >= 1000 ? `${(v / 1000).toFixed(1)}k` : v}
            </text>
            <text
              x={bx(i) + barW / 2} y={H - pad.bottom + 16}
              textAnchor="middle" fontSize="10" fill={S.textMid}
              transform={`rotate(-35,${bx(i) + barW / 2},${H - pad.bottom + 16})`}
            >
              {labels[i]}
            </text>
          </g>
        ))}
        <line x1={pad.left} y1={pad.top} x2={pad.left} y2={pad.top + iH} stroke={S.border} strokeWidth="1.5" />
        <line x1={pad.left} y1={pad.top + iH} x2={W - pad.right} y2={pad.top + iH} stroke={S.border} strokeWidth="1.5" />
        <text x={14} y={H / 2} textAnchor="middle" fontSize="10" fill={S.textLight} transform={`rotate(-90,14,${H / 2})`}>
          {table.headers[valueCol]}
        </text>
      </svg>
    </div>
  );
}

// ── CSV export helper ─────────────────────────────────────────────────────────
function exportCSV(table) {
  const csv  = [table.headers.join(";"), ...table.rows.map(r => r.join(";"))].join("\n");
  const blob = new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8;" });
  const url  = URL.createObjectURL(blob);
  const a    = Object.assign(document.createElement("a"), { href: url, download: `hikma_${Date.now()}.csv` });
  a.click();
  URL.revokeObjectURL(url);
}

// ── ChatTable ─────────────────────────────────────────────────────────────────
export function ChatTable({ table }) {
  const [showChart, setShowChart] = useState(false);
  if (!table?.headers || !table?.rows) return null;

  return (
    <div style={{ marginTop: "1rem" }}>
      <div style={{ overflowX: "auto" }}>
        <table style={css.chatTable}>
          <thead>
            <tr>{table.headers.map((h, i) => <th key={i} style={css.chatTh}>{h}</th>)}</tr>
          </thead>
          <tbody>
            {table.rows.map((row, ri) => (
              <tr key={ri} style={{ background: ri % 2 === 0 ? S.white : S.coralLight }}>
                {row.map((cell, ci) => <td key={ci} style={css.chatTd}>{cell}</td>)}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div style={css.tableActions}>
        <button style={css.btnGhost} onClick={() => exportCSV(table)}>📥 Exporter CSV</button>
        <button
          style={{ ...css.btnGhost, color: showChart ? S.coralDark : "#7c3aed", borderColor: showChart ? S.coral : "#7c3aed" }}
          onClick={() => setShowChart(v => !v)}
        >
          {showChart ? "📊 Masquer graphique" : "📊 Afficher graphique"}
        </button>
      </div>
      {showChart && (
        <div style={{ background: S.white, border: `1px solid ${S.border}`, borderRadius: 10, padding: "1rem", marginTop: "0.75rem" }}>
          <BarChart table={table} />
        </div>
      )}
    </div>
  );
}

// ── LineChart (admin stats) ───────────────────────────────────────────────────
export function LineChart({ data }) {
  if (!data?.length) return (
    <div style={{ textAlign: "center", color: S.textLight, padding: "3rem" }}>
      Aucune donnée — apparaîtra au fur et à mesure des inscriptions
    </div>
  );

  const pad = { top: 24, right: 40, bottom: 52, left: 52 };
  const pointSpacing = Math.max(60, Math.min(120, 700 / Math.max(data.length - 1, 1)));
  const W  = Math.max(600, pointSpacing * Math.max(data.length - 1, 1));
  const H2 = 220;
  const H  = H2 - pad.top - pad.bottom;
  const W2 = W + pad.left + pad.right;
  const maxV = Math.max(...data.map(d => d.total), 1);

  const x = i => pad.left + i * pointSpacing;
  const y = v => pad.top + H - (v / maxV) * H;
  const line = data.map((d, i) => `${i === 0 ? "M" : "L"}${x(i)},${y(d.total)}`).join(" ");
  const area = `${line} L${x(data.length - 1)},${pad.top + H} L${x(0)},${pad.top + H} Z`;
  const yTicks = [0, 0.25, 0.5, 0.75, 1].map(t => Math.round(maxV * t));
  const maxLabels = Math.floor(W / 55);
  const step = Math.max(1, Math.ceil(data.length / maxLabels));
  const xLabels = data.map((d, i) => ({ d, i })).filter(({ i }) => i % step === 0 || i === data.length - 1);

  return (
    <div style={{ overflowX: "auto", overflowY: "hidden" }}>
      <svg width={W2} height={H2} style={{ display: "block", minWidth: "100%" }}>
        <defs>
          <linearGradient id="cg" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={S.coral} stopOpacity="0.2" />
            <stop offset="100%" stopColor={S.coral} stopOpacity="0" />
          </linearGradient>
        </defs>
        {yTicks.map((v, i) => (
          <g key={i}>
            <line x1={pad.left} y1={y(v)} x2={pad.left + W} y2={y(v)} stroke={S.border} strokeWidth="1" strokeDasharray="4,4" />
            <text x={pad.left - 8} y={y(v) + 4} textAnchor="end" fontSize="11" fill={S.textLight}>{v}</text>
          </g>
        ))}
        {data.map((_, i) => (
          <line key={i} x1={x(i)} y1={pad.top} x2={x(i)} y2={pad.top + H} stroke={S.border} strokeWidth="1" strokeDasharray="2,4" opacity="0.5" />
        ))}
        <path d={area} fill="url(#cg)" />
        <path d={line} fill="none" stroke={S.coral} strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
        {data.map((d, i) => (
          <g key={i}>
            <circle cx={x(i)} cy={y(d.total)} r="5" fill={S.white} stroke={S.coral} strokeWidth="2.5" />
            <text x={x(i)} y={y(d.total) - 10} textAnchor="middle" fontSize="10" fontWeight="600" fill={S.coral}>{d.total}</text>
          </g>
        ))}
        {xLabels.map(({ d, i }) => (
          <text key={i} x={x(i)} y={pad.top + H + 20} textAnchor="middle" fontSize="11" fill={S.textMid}>{d.date?.slice(5)}</text>
        ))}
        <line x1={pad.left} y1={pad.top} x2={pad.left} y2={pad.top + H} stroke={S.border} strokeWidth="1.5" />
        <line x1={pad.left} y1={pad.top + H} x2={pad.left + W} y2={pad.top + H} stroke={S.border} strokeWidth="1.5" />
      </svg>
    </div>
  );
}