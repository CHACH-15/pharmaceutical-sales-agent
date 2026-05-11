"""
query_enhancer.py
~~~~~~~~~~~~~~~~~
Pre-processing layer that resolves query ambiguities before the SQL agent.

Client: openai.AsyncOpenAI → Groq's OpenAI-compatible endpoint
  Base URL : https://api.groq.com/openai/v1
  Model    : llama-3.3-70b-versatile
    - Best free model on Groq (2025), GPT-4 class quality
    - Excellent French/English bilingual performance
    - response_format={"type":"json_object"} → guaranteed valid JSON, no fences

Why OpenAI SDK instead of the Groq SDK?
  - response_format={"type":"json_object"} is an OpenAI API feature
    that forces valid JSON output — eliminates markdown-fence post-processing
  - Same Groq API key, no extra cost or rate-limit difference
  - Full feature parity + easier migration if providers change

What the enhancer resolves:
  1. Date ambiguities     ("cette année", "last month", "Q2")
  2. KPI routing          ("taux de réalisation" → achievement + JOIN flag)
  3. Entity extraction    (product, GSU, MR, market, gouvernorat)
  4. Scope restriction    (gamme/sous_gamme WHERE clause for permissioned users)
  5. GSU default scope    (delegue_medical always scoped to their territory)
  6. Personal MR queries  ("my performance" → filter by mr = user's name)
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from openai import AsyncOpenAI

from app.config import settings

logger = logging.getLogger(__name__)

# ── OpenAI client → Groq endpoint ─────────────────────────────────────────────
# Same Groq API key. response_format=json_object guarantees parseable output.
_enhancer_client = AsyncOpenAI(
    api_key=settings.groq_api_key,
    base_url="https://api.groq.com/openai/v1",
)

_ENHANCER_MODEL      = "llama-3.3-70b-versatile"   # best free model on Groq
_ENHANCER_MAX_TOKENS = 450                           # full JSON + flags


# ── Typed result ──────────────────────────────────────────────────────────────
@dataclass
class EnhancementResult:
    enhanced_query:  str           = ""
    detected_kpi:    str           = "other"
    year_p1:         Optional[str] = None
    year_p0:         Optional[str] = None
    month:           Optional[str] = None
    date_explicit:   bool          = False
    products:        list[str]     = field(default_factory=list)
    gsu:             Optional[str] = None
    mr:              Optional[str] = None
    sv:              Optional[str] = None
    market:          Optional[str] = None
    gouvernorat:     Optional[str] = None
    gamme:           Optional[str] = None
    scope:           str           = "national"
    needs_join:      bool          = False
    ambiguity_flags: list[str]     = field(default_factory=list)
    raw_json:        dict          = field(default_factory=dict)

    @classmethod
    def from_json(cls, data: dict, original_query: str) -> "EnhancementResult":
        dc   = data.get("date_context", {})
        ents = data.get("entities", {})
        return cls(
            enhanced_query  = data.get("enhanced_query", original_query),
            detected_kpi    = data.get("detected_kpi", "other"),
            year_p1         = dc.get("year_p1"),
            year_p0         = dc.get("year_p0"),
            month           = dc.get("month"),
            date_explicit   = bool(dc.get("explicit", False)),
            products        = ents.get("products") or [],
            gsu             = ents.get("gsu"),
            mr              = ents.get("mr"),
            sv              = ents.get("sv"),
            market          = ents.get("market"),
            gouvernorat     = ents.get("gouvernorat"),
            gamme           = ents.get("gamme"),
            scope           = data.get("scope", "national"),
            needs_join      = bool(data.get("needs_join", False)),
            ambiguity_flags = data.get("ambiguity_flags") or [],
            raw_json        = data,
        )

    @classmethod
    def passthrough(cls, original_query: str) -> "EnhancementResult":
        return cls(enhanced_query=original_query)


# ── Enhancer system prompt ────────────────────────────────────────────────────
_ENHANCER_SYSTEM = """
You are a query pre-processor for a pharmaceutical sales analytics system (Hikma Tunisia).
Your ONLY job is to resolve ambiguities before a SQL agent receives the query.

Return ONLY a valid JSON object — no markdown, no explanation, no extra text.

{
  "enhanced_query": "<rewritten query, same language as user>",
  "detected_kpi": "<evolution|market_growth|growth_index|pdm|msi|penetration|achievement|dashboard|index_evolution|index_penetration|other>",
  "date_context": {
    "period_type": "<monthly|quarterly|annual|custom|unknown>",
    "year_p1": "<4-digit year or null>",
    "year_p0": "<4-digit reference year or null>",
    "month":   "<MM or null>",
    "explicit": true
  },
  "entities": {
    "products":    [],
    "gsu":         null,
    "mr":          null,
    "sv":          null,
    "market":      null,
    "gouvernorat": null,
    "gamme":       null
  },
  "scope": "<national|regional|gsu|mr|product|market>",
  "needs_join": false,
  "ambiguity_flags": []
}

## RESOLUTION RULES

Time references:
- "cette année"/"this year"     → latest year from available data range
- "année dernière"/"last year"  → year_p0 = year_p1 - 1
- "le mois dernier"/"last month"→ resolve from current date
- Q1 → months 01-03 | Q2 → 04-06 | Q3 → 07-09 | Q4 → 10-12

KPI detection:
- "évolution"/"growth Hikma"             → evolution
- "croissance marché"/"market growth"    → market_growth
- "growth index"/"indice de croissance"  → growth_index
- "part de marché"/"PDM"                 → pdm
- "indice PDM"/"MSI"                     → msi
- "pénétration"/"coverage"              → penetration
- "taux de réalisation"/"achievement"    → achievement, needs_join=true
- "tableau de bord"/"dashboard"          → dashboard
- "index évolution"/"evolution index"    → index_evolution
- "index pénétration"/"penetration index"→ index_penetration
- "mon objectif"/"my target"             → needs_join=true

Personal queries:
- Set mr = user full name ONLY when the query explicitly contains:
  "my"/"mon"/"mes"/"je"/"ma performance"/"mes ventes" + Medical Representative role.
- Do NOT set mr for general questions even if the user is a Medical Representative.

Constraints:
- Do NOT invent names or GSU values — leave null if unsure
- Preserve the user's language in enhanced_query
- needs_join = true ONLY when objectives/targets are explicitly involved
"""


# ── Helpers ───────────────────────────────────────────────────────────────────
def _extract_date_context(catalog_text: str) -> str:
    m = re.search(r"ims_data\s*:\s*(\d{4}-\d{2})\s*→\s*(\d{4}-\d{2})", catalog_text)
    if m:
        return f"Available IMS data: {m.group(1)} to {m.group(2)}"
    now = datetime.now()
    return f"Current date: {now.strftime('%Y-%m')} (year={now.year})"

def _build_user_context_str(uc: dict) -> str:
    if not uc: return ""
    role = {"delegue_medical":"Medical Representative","superviseur":"Supervisor","admin":"Admin"}.get(uc.get("role",""), uc.get("role",""))
    name = " ".join(filter(None, [uc.get("first_name",""), uc.get("last_name","")]))
    parts = [f"Role: {role}"]
    if name:           parts.append(f"name={name}")
    if uc.get("gsu"):  parts.append(f"GSU={uc['gsu']}")
    return ", ".join(parts)

_SKIP_PREFIXES   = ("📊 Analyse du fichier",)
_MIN_QUERY_LEN   = 8
_TOTAL_HIKMA_RE  = re.compile(
    r"(ventes?\s+hikma\s+total|total\s+hikma|hikma\s+global|"
    r"all\s+hikma|toutes?\s+les?\s+ventes?\s+hikma|hikma\s+national|national\s+hikma)",
    re.IGNORECASE,
)

def _should_skip(msg: str) -> bool:
    s = msg.strip()
    return len(s) < _MIN_QUERY_LEN or any(s.startswith(p) for p in _SKIP_PREFIXES)

def _is_total_hikma(msg: str) -> bool:
    return bool(_TOTAL_HIKMA_RE.search(msg))


# ── Context block builder ─────────────────────────────────────────────────────
def _build_context_block(result: EnhancementResult, original_query: str, uc: dict) -> str:
    lines: list[str] = []

    if result.enhanced_query and result.enhanced_query.strip() != original_query.strip():
        lines.append(f"[Clarified intent: {result.enhanced_query}]")

    kpi_map = {
        "evolution":         "KPI-1 Évolution Hikma (valeur TND)",
        "market_growth":     "KPI-2 Croissance Marché (all brands)",
        "growth_index":      "KPI-3 Growth Index (Hikma/Marché ×100)",
        "pdm":               "KPI-4 Part de Marché valeur (%)",
        "msi":               "KPI-5 Market Share Index (PDM_P1/PDM_P0 ×100)",
        "penetration":       "KPI-6 Pénétration Géographique + Volume (présence GSU)",
        "achievement":       "Taux de Réalisation (ims_data ⋈ target_data)",
        "dashboard":         "Full KPI Dashboard (KPI 1–6)",
        "index_evolution":   "KPI-7 Index Évolution — (ratio_S×100+100)/(ratio_M×100+100)",
        "index_penetration": (
            "KPI-8 Index Pénétration = "
            "(pdm_hikma_zone×100+100)/(pdm_marche_zone×100+100) — "
            "pdm_hikma_zone=hikma_zone/marche_zone, "
            "pdm_marche_zone=marche_zone/marche_national. "
            "Use canonical SQL from KPI-8 section. Scope by gouvernorat/gsu/mr."
        ),
    }
    if result.detected_kpi and result.detected_kpi != "other":
        lines.append(f"[KPI: {kpi_map.get(result.detected_kpi, result.detected_kpi)}]")

    if (result.detected_kpi == "index_evolution"
            and uc.get("role") == "delegue_medical" and result.mr):
        fn, ln = uc.get("first_name",""), uc.get("last_name","")
        if fn or ln:
            lines.append(f"[Personal MR query: filter ims_data by mr='{fn} {ln}' — use KPI-7b template]")

    dp = []
    if result.year_p1: dp.append(f"year={result.year_p1}")
    if result.year_p0: dp.append(f"reference={result.year_p0}")
    if result.month:   dp.append(f"month={result.month}")
    if dp: lines.append(f"[Date context: {', '.join(dp)}]")

    if result.needs_join:
        lines.append("[Requires JOIN: ims_data ⋈ target_data on date+product+forme+gsu+gamme+sous_gamme using COALESCE on nullable keys]")

    if result.ambiguity_flags:
        lines.append(f"[Resolved: {'; '.join(result.ambiguity_flags)}]")

    if uc.get("role") == "delegue_medical" and uc.get("gsu") and not result.gsu:
        lines.append(f"[Default territory scope: GSU={uc['gsu']}]")

    # ── Gamme scope restriction (gamme ONLY — never sous_gamme) ──────────────────
    role  = uc.get("role", "")
    perms = uc.get("gamme_permissions", [])
    if perms and role in ("delegue_medical", "superviseur") and not _is_total_hikma(original_query):
        gammes = sorted({p["gamme"] for p in perms if p.get("gamme")})
        if gammes:
            gamme_clause = f"gamme IN ({', '.join(repr(g) for g in gammes)})"
            lines.append(
                f"[MANDATORY SCOPE RESTRICTION — add to EVERY SQL WHERE clause: "
                f"{gamme_clause}. Apply to ims_data, kpi_cache, and target_data. "
                f"Do NOT filter by sous_gamme. "
                f"Do NOT add an MR name filter unless the user explicitly used personal "
                f"pronouns (mon/mes/je/my). "
                f"Exception: 'ventes Hikma totales/national' bypasses this gamme scope.]"
            )

    return "\n".join(lines)


# ── Public API ────────────────────────────────────────────────────────────────
async def enhance_query(
    user_message: str,
    catalog_text: str,
    user_context: Optional[dict] = None,
) -> tuple[str, EnhancementResult]:
    """
    Enhance a user query before it reaches the SQL analytics agent.

    Uses openai.AsyncOpenAI → Groq (llama-3.3-70b-versatile) with
    response_format={"type":"json_object"} for guaranteed valid JSON.

    Returns (enriched_message, EnhancementResult).
    Falls back to raw query transparently on any failure.
    """
    uc = user_context or {}

    if _should_skip(user_message):
        logger.debug("Enhancer skipped (short/file message)")
        return user_message, EnhancementResult.passthrough(user_message)

    enhancer_input = "\n".join(filter(None, [
        _extract_date_context(catalog_text),
        _build_user_context_str(uc),
        f"\nUser query: {user_message}",
    ]))

    try:
        response = await _enhancer_client.chat.completions.create(
            model=_ENHANCER_MODEL,
            messages=[
                {"role": "system", "content": _ENHANCER_SYSTEM},
                {"role": "user",   "content": enhancer_input},
            ],
            temperature=0.0,
            max_tokens=_ENHANCER_MAX_TOKENS,
            response_format={"type": "json_object"},   # ← guaranteed valid JSON
            stream=False,
        )
        raw    = (response.choices[0].message.content or "{}").strip()
        result = EnhancementResult.from_json(json.loads(raw), user_message)

    except Exception as exc:
        logger.warning("⚠️ Query enhancer failed — using raw query: %s", exc)
        return user_message, EnhancementResult.passthrough(user_message)

    context_block = _build_context_block(result, user_message, uc)
    enriched      = f"{user_message}\n\n{context_block}" if context_block else user_message

    logger.info(
        "✨ Enhanced | kpi=%-20s | year=%s/%-4s | join=%s | scoped=%s | flags=%d",
        result.detected_kpi, result.year_p1, result.year_p0,
        result.needs_join, bool(uc.get("gamme_permissions")),
        len(result.ambiguity_flags),
    )
    return enriched, result