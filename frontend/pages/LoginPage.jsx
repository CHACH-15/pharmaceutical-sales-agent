import { useState } from "react";
import { api } from "../api.js";
import { S, css } from "../styles.js";
import { EmailInput, Input } from "../components.jsx";

export default function LoginPage({ onLogin, onGoRegister }) {
  const [form, setForm]       = useState({ emailPrefix: "", password: "" });
  const [error, setError]     = useState("");
  const [loading, setLoading] = useState(false);

  const set = k => e => setForm(f => ({ ...f, [k]: e.target.value }));

  const submit = async e => {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      const res = await api.post("/auth/login", {
        email:    form.emailPrefix + "@hikma.com",
        password: form.password,
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
      <div style={css.loginCard}>
        <div style={css.logo}>wisdom.</div>
        <div style={css.logoSub}>Intelligence Analytique Hikma</div>

        {error && <div style={css.error}>{error}</div>}

        <form onSubmit={submit}>
          <EmailInput label="Email" value={form.emailPrefix} onChange={set("emailPrefix")} />
          <Input label="Mot de passe" type="password" value={form.password} onChange={set("password")} placeholder="••••••••" required />
          <button style={{ ...css.btn, opacity: loading ? 0.7 : 1 }} disabled={loading}>
            {loading ? "Connexion…" : "Se connecter"}
          </button>
        </form>

        <p style={{ textAlign: "center", marginTop: "1.2rem", fontSize: "0.88rem", color: S.textLight }}>
          Pas encore de compte ?{" "}
          <span style={{ color: S.coral, cursor: "pointer", fontWeight: 600 }} onClick={onGoRegister}>
            S'inscrire
          </span>
        </p>
      </div>
    </div>
  );
}