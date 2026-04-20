import { useState } from "react";
import { api } from "../api.js";
import { S, css } from "../styles.js";
import { EmailInput, Input } from "../components.jsx";

export default function RegisterPage({ onLogin, onGoLogin }) {
  const [form, setForm]       = useState({ emailPrefix: "", password: "", first_name: "", last_name: "", gsu: "", role: "delegue_medical" });
  const [error, setError]     = useState("");
  const [loading, setLoading] = useState(false);

  const set = k => e => setForm(f => ({ ...f, [k]: e.target.value }));

  const submit = async e => {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      const res = await api.post("/auth/register", {
        email:      form.emailPrefix + "@hikma.com",
        password:   form.password,
        first_name: form.first_name,
        last_name:  form.last_name,
        gsu:        form.gsu,
        role:       form.role,
      });
      if (res.detail) { setError(res.detail); return; }
      localStorage.setItem("token", res.token);
      onLogin(res.user);
    } catch {
      setError("Erreur de connexion au serveur.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={css.loginWrap}>
      <div style={{ ...css.loginCard, width: 480 }}>
        <div style={css.logo}>wisdom.</div>
        <div style={css.logoSub}>Créer un compte Hikma</div>

        {error && <div style={css.error}>{error}</div>}

        <div style={{ background: "#e8f4fd", border: "1px solid #bae6fd", borderRadius: 8, padding: "0.6rem 0.85rem", fontSize: "0.82rem", color: "#0369a1", marginBottom: "1rem" }}>
          ⚠️ Seuls les emails pré-autorisés par l'administrateur peuvent s'inscrire.
        </div>

        <form onSubmit={submit}>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0 1rem" }}>
            <Input label="Prénom" value={form.first_name} onChange={set("first_name")} placeholder="Prénom" required />
            <Input label="Nom"    value={form.last_name}  onChange={set("last_name")}  placeholder="Nom"    required />
          </div>

          <EmailInput label="Email" value={form.emailPrefix} onChange={set("emailPrefix")} />

          <Input label="Mot de passe" type="password" value={form.password} onChange={set("password")} placeholder="••••••••" required />

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0 1rem" }}>
            <Input label="GSU (Zone)" value={form.gsu} onChange={set("gsu")} placeholder="ex: Sfax 1A2" required />
            <div style={{ marginBottom: "1rem" }}>
              <label style={css.label}>Rôle</label>
              <select value={form.role} onChange={set("role")} style={css.input}>
                <option value="delegue_medical">Délégué Médical</option>
                <option value="superviseur">Superviseur</option>
              </select>
            </div>
          </div>

          <button style={{ ...css.btn, opacity: loading ? 0.7 : 1 }} disabled={loading}>
            {loading ? "Création…" : "Créer un compte"}
          </button>
        </form>

        <p style={{ textAlign: "center", marginTop: "1.2rem", fontSize: "0.88rem", color: S.textLight }}>
          Déjà un compte ?{" "}
          <span style={{ color: S.coral, cursor: "pointer", fontWeight: 600 }} onClick={onGoLogin}>
            Se connecter
          </span>
        </p>
      </div>
    </div>
  );
}