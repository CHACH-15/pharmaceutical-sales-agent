import { useState, useEffect, useRef, useCallback } from "react";
import { api, WS_BASE, roleLabel } from "../api.js";
import { S, css } from "../styles.js";
import { Spinner, ChatTable } from "../components.jsx";

export default function ChatPage({ user, onLogout, onAdmin }) {
  const [sessions, setSessions] = useState([]);
  const [activeId, setActiveId] = useState(null);
  const [messages, setMessages] = useState([]);
  const [input, setInput]       = useState("");
  const [loading, setLoading]   = useState(false);
  const bottomRef               = useRef(null);
  const wsRef                   = useRef(null);

  // ── Data fetching ──────────────────────────────────────────────────────────
  const loadSessions = useCallback(async () => {
    const data = await api.get("/chat/sessions");
    if (Array.isArray(data)) setSessions(data);
  }, []);

  useEffect(() => { loadSessions(); }, [loadSessions]);

  useEffect(() => {
    if (activeId) {
      api.get(`/chat/sessions/${activeId}/messages`).then(data => {
        if (Array.isArray(data)) setMessages(data);
      });
    } else {
      setMessages([]);
    }
  }, [activeId]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

  // ── Session management ─────────────────────────────────────────────────────
  const newChat = async () => {
    const s = await api.post("/chat/sessions", {});
    if (s.id) { await loadSessions(); setActiveId(s.id); setMessages([]); }
  };

  const deleteSession = async (e, id) => {
    e.stopPropagation();
    await api.del(`/chat/sessions/${id}`);
    if (activeId === id) { setActiveId(null); setMessages([]); }
    await loadSessions();
  };

  // ── Send message ───────────────────────────────────────────────────────────
  const send = async () => {
    if (!input.trim() || loading) return;
    const q = input.trim();
    setInput("");

    let sid = activeId;
    if (!sid) {
      const s = await api.post("/chat/sessions", {});
      sid = s.id;
      setActiveId(sid);
    }

    setMessages(m => [...m, { id: Date.now(), role: "user", content: q }]);
    setLoading(true);

    const assistantId = Date.now() + 1;
    const ws = new WebSocket(`${WS_BASE}?token=${api.token()}&session_id=${sid}`);
    wsRef.current = ws;

    ws.onopen = () => ws.send(JSON.stringify({ message: q }));

    ws.onmessage = ({ data }) => {
      const msg = JSON.parse(data);

      if (msg.type === "stream_start") {
        setMessages(m => [...m, { id: assistantId, role: "assistant", content: "" }]);
        setLoading(false);
      }
      if (msg.type === "token") {
        setMessages(m => m.map(x => x.id === assistantId ? { ...x, content: x.content + msg.content } : x));
      }
      if (msg.type === "stream_end") { ws.close(); loadSessions(); }
      if (msg.type === "error") {
        setMessages(m => m.map(x => x.id === assistantId ? { ...x, content: "Erreur : " + msg.content } : x));
        setLoading(false);
        ws.close();
      }
    };

    ws.onerror = () => {
      setMessages(m => [...m, { id: assistantId, role: "assistant", content: "Erreur de connexion au serveur." }]);
      setLoading(false);
    };
  };

  const fmt = iso => iso ? new Date(iso).toLocaleDateString("fr-FR", { day: "2-digit", month: "short" }) : "";

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div style={css.layout}>
      {/* Sidebar */}
      <aside style={css.sidebar}>
        <div style={css.sidebarHeader}>
          <div style={css.sidebarLogo}>wisdom.</div>
          <div style={css.sidebarTagline}>Intelligence Analytique</div>
        </div>

        <button style={css.newChatBtn} onClick={newChat}>＋ Nouvelle conversation</button>

        <div style={css.sidebarSection}>Conversations</div>

        <div style={css.sidebarScroll}>
          {sessions.length === 0 && (
            <div style={{ color: S.textLight, textAlign: "center", padding: "2rem 0", fontSize: "0.85rem" }}>
              Aucune conversation
            </div>
          )}
          {sessions.map(s => (
            <div key={s.id} style={css.chatItem(s.id === activeId)} onClick={() => setActiveId(s.id)}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                <div style={css.chatItemTitle}>{s.title}</div>
                <button
                  style={{ background: "none", border: "none", color: S.textLight, cursor: "pointer", fontSize: "0.8rem", padding: "0 0 0 4px", flexShrink: 0 }}
                  onClick={e => deleteSession(e, s.id)}
                  title="Supprimer"
                >✕</button>
              </div>
              <div style={css.chatItemDate}>{fmt(s.updated_at)}</div>
            </div>
          ))}
        </div>

        <div style={css.sidebarFooter}>
          {user.role === "admin" && (
            <div style={css.adminLink} onClick={onAdmin}>⚙ Administration</div>
          )}
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

      {/* Main chat area */}
      <main style={css.main}>
        {!activeId && messages.length === 0 && (
          <div style={css.chatHeader}>
            <div style={css.headerLogo}>wisdom.</div>
            <div style={css.headerSub}>Analyse des ventes & performances Hikma</div>
            <div style={css.headerLine} />
            <div style={{ marginTop: "1.5rem", color: S.textLight, fontSize: "0.85rem" }}>
              Posez une question sur les ventes, objectifs, taux de réalisation ou parts de marché.
            </div>
          </div>
        )}

        <div style={{ ...css.messagesArea, paddingTop: "2rem" }}>
          {messages.map(m => (
            <div key={m.id} style={css.msgBlock}>
              <div style={css.msgLabel(m.role)}>
                <span style={css.msgDot(m.role)} />
                {m.role === "user" ? "Vous" : "Hikma"}
              </div>
              <div style={css.msgBubble(m.role)}>
                <div dangerouslySetInnerHTML={{ __html: (m.content || "").replace(/\n/g, "<br/>") }} />
                {m.table && <ChatTable table={m.table} />}
              </div>
            </div>
          ))}

          {loading && (
            <div style={css.msgBlock}>
              <div style={css.msgLabel("assistant")}><span style={css.msgDot("assistant")} />Hikma</div>
              <div style={css.msgBubble("assistant")}><Spinner /></div>
            </div>
          )}

          <div ref={bottomRef} />
        </div>

        {/* Input bar */}
        <div style={css.inputBar}>
          <div style={css.inputInner}>
            <input
              style={css.inputField}
              value={input}
              onChange={e => setInput(e.target.value)}
              onKeyDown={e => e.key === "Enter" && !e.shiftKey && send()}
              placeholder="Posez votre question sur les ventes, objectifs, performances…"
              disabled={loading}
            />
            <button style={{ ...css.sendBtn, opacity: loading ? 0.6 : 1 }} onClick={send} disabled={loading}>
              Envoyer
            </button>
          </div>
        </div>
      </main>
    </div>
  );
}