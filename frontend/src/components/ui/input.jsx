import { useState } from "react";
import { S, css } from "../../constants/styles";

export default function Input({ label, ...props }) {
  const [focused, setFocused] = useState(false);
  return (
    <div style={{ marginBottom:"1rem" }}>
      {label && <label style={css.label}>{label}</label>}
      <input {...props}
        style={{ ...css.input, borderColor: focused ? S.coral : S.border }}
        onFocus={() => setFocused(true)}
        onBlur={() => setFocused(false)}
      />
    </div>
  );
}