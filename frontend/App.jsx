import { useState, useEffect } from "react";
import { api } from "./api.js";
import { S, css } from "./styles.js";
import LoginPage    from "./pages/LoginPage.jsx";
import RegisterPage from "./pages/RegisterPage.jsx";
import ChatPage     from "./pages/ChatPage.jsx";
import AdminPage    from "./pages/AdminPage.jsx";

export default function App() {
  const [user, setUser]     = useState(null);
  const [page, setPage]     = useState("login");   // "login" | "register" | "chat" | "admin"
  const [ready, setReady]   = useState(false);

  // ── Auto-login from stored token ───────────────────────────────────────────
  useEffect(() => {
    const token = localStorage.getItem("token");
    if (!token) { setReady(true); return; }

    api.get("/auth/me")
      .then(data => {
        if (data?.id) { setUser(data); setPage("chat"); }
        else { localStorage.removeItem("token"); }
      })
      .catch(() => { localStorage.removeItem("token"); })
      .finally(() => setReady(false));

    setReady(true);
  }, []);

  const handleLogin = u => { setUser(u); setPage("chat"); };
  const handleLogout = () => { localStorage.removeItem("token"); setUser(null); setPage("login"); };

  // ── Splash while checking token ────────────────────────────────────────────
  if (!ready) return (
    <div style={{ ...css.loginWrap, ...css.app }}>
      <div style={{ color: S.coral, fontSize: "2.5rem", fontWeight: 700, letterSpacing: "-1px" }}>wisdom.</div>
    </div>
  );

  return (
    <div style={css.app}>
      <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet" />

      {page === "login"    && <LoginPage    onLogin={handleLogin} onGoRegister={() => setPage("register")} />}
      {page === "register" && <RegisterPage onLogin={handleLogin} onGoLogin={() => setPage("login")} />}
      {page === "chat"     && <ChatPage     user={user} onLogout={handleLogout} onAdmin={() => setPage("admin")} />}
      {page === "admin"    && user?.role === "admin" && <AdminPage user={user} onBack={() => setPage("chat")} />}
    </div>
  );
}