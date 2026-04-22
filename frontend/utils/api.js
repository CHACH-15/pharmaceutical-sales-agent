import { API_URL } from "../config";

const token   = () => localStorage.getItem("token");
const headers = () => ({
  "Content-Type": "application/json",
  ...(token() ? { Authorization: `Bearer ${token()}` } : {}),
});

export const api = {
  token,
  headers,
  get:    (path)       => fetch(`${API_URL}${path}`, { headers: headers() }).then(r => r.json()),
  post:   (path, body) => fetch(`${API_URL}${path}`, { method:"POST",   headers: headers(), body: JSON.stringify(body) }).then(r => r.json()),
  patch:  (path, body) => fetch(`${API_URL}${path}`, { method:"PATCH",  headers: headers(), body: JSON.stringify(body) }).then(r => r.json()),
  del:    (path)       => fetch(`${API_URL}${path}`, { method:"DELETE", headers: headers() }).then(r => r.json()),
  upload: (path, fd)   => fetch(`${API_URL}${path}`, { method:"POST", headers:{ Authorization:`Bearer ${token()}` }, body: fd }).then(r => r.json()),
};