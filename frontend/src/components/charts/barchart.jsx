import { S, CHART_COLORS } from "../../constants/styles";

export default function BarChart({ table }) {
  if (!table?.rows?.length) return null;
  const labelCol = 0;
  let valueCol = -1;
  for (let c = 1; c < table.headers.length; c++) {
    if (table.rows.some(r => !isNaN(parseFloat(String(r[c]).replace(/[\s,]/g,""))))) { valueCol = c; break; }
  }
  if (valueCol === -1) return null;
  const labels = table.rows.map(r => String(r[labelCol]).slice(0,18));
  const values = table.rows.map(r => parseFloat(String(r[valueCol]).replace(/[\s,]/g,"")) || 0);
  const maxVal = Math.max(...values, 1);
  const pad = { top:24, right:20, bottom:64, left:64 };
  const W   = Math.max(420, labels.length * 72 + pad.left + pad.right);
  const H   = 240;
  const iW  = W - pad.left - pad.right;
  const iH  = H - pad.top - pad.bottom;
  const barW = Math.max(20, Math.min(50, iW / labels.length * 0.7));
  const gap  = iW / labels.length;
  const bx = i => pad.left + i * gap + (gap - barW) / 2;
  const by = v => pad.top + iH - (v / maxVal) * iH;
  return (
    <div style={{ overflowX:"auto" }}>
      <svg width={W} height={H} style={{ display:"block" }}>
        {[0,0.25,0.5,0.75,1].map((t,i)=>(
          <g key={i}>
            <line x1={pad.left} y1={by(maxVal*t)} x2={W-pad.right} y2={by(maxVal*t)} stroke={S.border} strokeWidth={1} strokeDasharray="4,4"/>
            <text x={pad.left-8} y={by(maxVal*t)+4} textAnchor="end" fontSize={10} fill={S.textLight}>
              {maxVal*t>=1000?`${((maxVal*t)/1000).toFixed(1)}k`:(maxVal*t).toFixed(0)}
            </text>
          </g>
        ))}
        {values.map((v,i)=>(
          <g key={i}>
            <rect x={bx(i)} y={by(v)} width={barW} height={iH-(iH-(v/maxVal)*iH)} fill={CHART_COLORS[i%CHART_COLORS.length]} rx={4} opacity={0.9}/>
            <text x={bx(i)+barW/2} y={by(v)-5} textAnchor="middle" fontSize={10} fontWeight={600} fill={CHART_COLORS[i%CHART_COLORS.length]}>
              {v>=1000?`${(v/1000).toFixed(1)}k`:v}
            </text>
            <text x={bx(i)+barW/2} y={H-pad.bottom+16} textAnchor="middle" fontSize={10} fill={S.textMid}
              transform={`rotate(-35,${bx(i)+barW/2},${H-pad.bottom+16})`}>{labels[i]}</text>
          </g>
        ))}
        <line x1={pad.left} y1={pad.top} x2={pad.left} y2={pad.top+iH} stroke={S.border} strokeWidth={1.5}/>
        <line x1={pad.left} y1={pad.top+iH} x2={W-pad.right} y2={pad.top+iH} stroke={S.border} strokeWidth={1.5}/>
        <text x={pad.left-40} y={H/2} textAnchor="middle" fontSize={10} fill={S.textLight}
          transform={`rotate(-90,${pad.left-40},${H/2})`}>{table.headers[valueCol]}</text>
      </svg>
    </div>
  );
}