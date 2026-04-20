export const API_BASE  = "http://localhost:8000";
export const WS_BASE   = "ws://localhost:8000/ws";

const token   = () => localStorage.getItem("token");
const headers = () => ({
  "Content-Type": "application/json",
  ...(token() ? { Authorization: `Bearer ${token()}` } : {}),
});

const request = async (method, path, body) => {
  const res = await fetch(`${API_BASE}${path}`, {
    method,
    headers: headers(),
    ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  });
  return res.json();
};

export const api = {
  token,
  get:    (path)       => request("GET",    path),
  post:   (path, body) => request("POST",   path, body),
  patch:  (path, body) => request("PATCH",  path, body),
  del:    (path)       => request("DELETE", path),
  upload: (path, formData) =>
    fetch(`${API_BASE}${path}`, {
      method:  "POST",
      headers: { Authorization: `Bearer ${token()}` },
      body:    formData,
    }).then(r => r.json()),
};

export const roleLabel = r =>
  r === "delegue_medical" ? "Délégué Médical"
  : r === "superviseur"   ? "Superviseur"
  : r === "admin"         ? "Admin"
  : r;