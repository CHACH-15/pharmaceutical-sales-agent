import { S } from "../../constants/styles";

export default function Spinner() {
  return (
    <div style={{ display:"flex", alignItems:"center", gap:"0.5rem", padding:"0.8rem 1.4rem" }}>
      {[0, 0.2, 0.4].map((d, i) => (
        <div key={i} style={{ width:8, height:8, borderRadius:"50%", background:S.coral, animation:`pulse 1s ${d}s infinite` }}/>
      ))}
      <style>{`@keyframes pulse{0%,100%{opacity:.3;transform:scale(.8)}50%{opacity:1;transform:scale(1)}}`}</style>
    </div>
  );
}