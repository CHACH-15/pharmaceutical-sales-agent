import { S, css } from "../../constants/styles";

export default function EmailInput({ label, value, onChange }) {
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