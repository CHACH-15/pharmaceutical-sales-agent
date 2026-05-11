from __future__ import annotations

import json
import logging
import re
from collections.abc import AsyncGenerator
from typing import Optional

from groq import AsyncGroq

from app.config import settings
from app.core.prompt_builder import build_messages, build_system_prompt
from app.core.query_enhancer import enhance_query
from app.database import get_db
from app.services.session_manager import session_manager

logger = logging.getLogger(__name__)

_client = AsyncGroq(
    api_key=settings.groq_api_key,
    base_url=settings.llm_base_url,
)

MAX_TOOL_CALLS       = 3
TOKENS_STANDARD      = min(settings.llm_max_tokens, 1200)  # raised: complex KPIs need space
TOKENS_FILE_ANALYSIS = 4096


def _is_file_analysis(message: str) -> bool:
    return message.startswith("📊 Analyse du fichier")


_NULLABLE_COLS = r"(?:gamme|sous_gamme|forme|gsu)"


def sanitize_sqlite_sql(sql: str) -> str:
    sql = re.sub(r"EXTRACT\s*\(\s*YEAR\s+FROM\s*([\w\.`\"]+)\s*\)",    r"strftime('%Y', \1)",    sql, flags=re.IGNORECASE)
    sql = re.sub(r"EXTRACT\s*\(\s*MONTH\s+FROM\s*([\w\.`\"]+)\s*\)",   r"strftime('%m', \1)",    sql, flags=re.IGNORECASE)
    sql = re.sub(r"EXTRACT\s*\(\s*DAY\s+FROM\s*([\w\.`\"]+)\s*\)",     r"strftime('%d', \1)",    sql, flags=re.IGNORECASE)
    sql = re.sub(r"DATE_TRUNC\s*\(\s*'month'\s*,\s*([\w\.`\"]+)\s*\)", r"strftime('%Y-%m', \1)", sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bTrue\b',  'TRUE',  sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bFalse\b', 'FALSE', sql, flags=re.IGNORECASE)
    sql = re.sub(
        r"strftime\('%Y',\s*([\w\.`\"]+)\)\s*=\s*(\d{4})(?!')",
        r"strftime('%Y', \1) = '\2'", sql, flags=re.IGNORECASE,
    )
    sql = re.sub(r'\bNOW\s*\(\s*\)',  "datetime('now')", sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bILIKE\b', 'LIKE', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bims8k\b', 'ims_data', sql, flags=re.IGNORECASE)

    def _coalesce_join(m: re.Match) -> str:
        la, lc, ra, rc = m.group(1), m.group(2), m.group(3), m.group(4)
        if lc.lower() == rc.lower():
            return f"COALESCE({la}.{lc}, '') = COALESCE({ra}.{rc}, '')"
        return m.group(0)

    sql = re.sub(
        rf"([\w]+)\.({_NULLABLE_COLS})\s*=\s*([\w]+)\.({_NULLABLE_COLS})",
        _coalesce_join, sql, flags=re.IGNORECASE,
    )
    return sql


def _run_sql(sql: str, _executed: list | None = None) -> str:
    """Execute SQL and return formatted result string.
    If _executed list is provided, appends the sanitized SQL to it so the
    caller can surface it to the user for transparency.
    """
    sql = sanitize_sqlite_sql(sql)
    logger.info("🔍 SQL:\n%s", sql)
    if _executed is not None:
        _executed.append(sql.strip())
    try:
        db   = get_db()
        rows = db.execute(sql).fetchall()
        db.close()
    except Exception as exc:
        logger.error("❌ SQL error: %s", exc)
        return f"❌ Erreur SQL : {exc}"
    if not rows:
        return "⚠️ La requête n'a retourné aucun résultat."
    cols   = list(rows[0].keys())
    header = " | ".join(cols)
    lines  = [header, "─" * len(header)]
    for row in rows[:200]:
        lines.append(" | ".join("NULL" if v is None else str(v) for v in row))
    logger.info("✅ SQL → %d row(s)", len(rows))
    return "\n".join(lines)


def _parse_function_call(text: str) -> dict | None:
    """Parse a JSON tool-call block from LLM output."""
    cleaned = text.replace("\\", "")
    cleaned = re.sub(r"(//.*?$|/\*.*?\*/)", "", cleaned, flags=re.MULTILINE | re.DOTALL)
    start   = cleaned.find("{")
    end     = cleaned.rfind("}")
    if start == -1 or end == -1 or start >= end:
        return None
    json_str  = cleaned[start:end + 1]
    in_quotes = False
    chars     = []
    for ch in json_str:
        if ch == '"':
            in_quotes = not in_quotes
        chars.append('"' if ch == "'" and not in_quotes else ch)
    json_str = (
        "".join(chars)
        .replace("True", "true")
        .replace("False", "false")
        .replace("None", "null")
    )
    try:
        obj = json.loads(json_str)
        if isinstance(obj, dict) and "function_name" in obj:
            return obj
    except json.JSONDecodeError:
        pass
    return None


def _extract_markdown_sql(text: str) -> str | None:
    """
    Detect when the LLM wrote a ```sql ... ``` block instead of using the
    JSON tool-call format.  Extract and return the SQL so we can execute it.
    Returns None if no executable markdown SQL block found.
    """
    m = re.search(r"```(?:sql)?\s*\n(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if m:
        sql = m.group(1).strip()
        if re.match(r"\s*(SELECT|WITH)\b", sql, re.IGNORECASE):
            return sql
    return None


def _looks_like_hallucination(text: str) -> bool:
    """
    Return True if the LLM response appears to contain fabricated data.
    Heuristics:
      - Has a markdown table AND contains suspiciously round numbers
        (5+ digits ending in 000)
    """
    round_numbers = re.findall(r'\b\d{3,}\s*000\b', text)
    has_table     = bool(re.search(r'\|.+\|.+\|', text))
    return has_table and len(round_numbers) >= 2


# ── Response cleaning ──────────────────────────────────────────────────────────

_CLEAN_PATTERNS: list[tuple[str, int, str]] = [
    # 1. JSON tool-call blocks
    (r'\{\s*"function_name"\s*:.*?\}',                   re.DOTALL, ""),
    # 2. Markdown SQL/code fenced blocks (the #1 visual pollution issue)
    (r'```(?:sql|SQL|python|py)?\s*\n.*?```',            re.DOTALL, ""),
    # 3. Inline backtick sql
    (r'`sql\s*\n.*?`',                                   re.DOTALL, ""),
    # 4. "Pour obtenir ces résultats, j'ai utilisé la requête SQL suivante :"
    #    and all variants of this "show my work" pattern
    (r'(?mi)^.*?(?:'
     r"pour obtenir ces résultats|j['\u2019]ai utilisé la requête|"
     r"j['\u2019]ai exécuté|j['\u2019]ai utilisé|"
     r"voici la requête|la requête sql|the sql query|"
     r"i used the following|using the query|"
     r"i['']ve run|i['']ve used|j['']ai lancé"
     r").*?$\n?",                                        re.IGNORECASE, ""),
    # 5. "Je vais maintenant récupérer / interroger / lancer …"
    (r'(?mi)^.*?(?:'
     r"je vais (?:maintenant |)(?:récupérer|lancer|exécuter|"
     r"interroger|utiliser|chercher)|"
     r"i(?:'ll| will) (?:now |)(?:query|fetch|run|use|search)"
     r").*?$\n?",                                        re.IGNORECASE, ""),
]


def _clean_response(text: str) -> str:
    """
    Strip all technical artefacts from the final user-facing response:
      - JSON tool-call blocks
      - ```sql / ```python code blocks
      - "j'ai utilisé la requête SQL suivante" paragraphs
      - "Je vais maintenant récupérer" sentences
    """
    for pattern, flags, replacement in _CLEAN_PATTERNS:
        text = re.sub(pattern, replacement, text, flags=flags)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


async def _call_llm(
    messages: list,
    temperature: float = 0.0,
    max_tokens: int = TOKENS_STANDARD,
) -> str:
    """Call LLM with automatic 429 retry (exponential backoff, 2 attempts)."""
    import asyncio
    for attempt in range(2):
        try:
            response = await _client.chat.completions.create(
                model=settings.llm_model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                stream=False,
            )
            return response.choices[0].message.content or ""
        except Exception as exc:
            err_str = str(exc)
            if "429" in err_str and attempt == 0:
                # Extract wait time from error message if available
                import re as _re
                m = _re.search(r"try again in (\d+)m(\d+)", err_str)
                if m:
                    wait = int(m.group(1)) * 60 + int(m.group(2))
                    wait = min(wait, 60)  # cap at 1 min so server doesn't hang
                else:
                    wait = 30
                logger.warning("⏳ Rate-limited (429) — retrying in %ds", wait)
                await asyncio.sleep(wait)
                continue
            raise  # re-raise on 2nd attempt or non-429 errors


def _execute_tool(func_call: dict, _executed: list | None = None) -> str:
    name   = func_call.get("function_name", "")
    params = func_call.get("params", {})
    if name == "execute_sql":
        sql = params.get("query", "").strip()
        return _run_sql(sql, _executed) if sql else "❌ Aucune requête SQL fournie."
    return f"❌ Outil inconnu : '{name}'"


# ── Prompt constants ───────────────────────────────────────────────────────────

# Sentinels used to pass executed SQL to the frontend through the token stream.
# The frontend parses __SQL_START__[...]__SQL_END__ and renders a SQL viewer.
_SQL_SENTINEL_START = "__SQL_START__"
_SQL_SENTINEL_END   = "__SQL_END__"

_TOOL_RESULT_PREFIX = (
    "DB results:\n\n```\n{result}\n```\n\n"
    "Write the final structured answer using ONLY these numbers. "
    "No code blocks, no JSON, no SQL explanations. "
    "If another query is needed, output only the JSON tool call."
)

_FORCE_SQL_MSG = (
    "⚠️ Call execute_sql NOW with a real SELECT query. "
    "Output ONLY: {\"function_name\":\"execute_sql\",\"params\":{\"query\":\"SELECT ...\"}}"
)

_HALLUCINATION_MSG = (
    "⚠️ Your answer contains invented numbers not from any SQL result. "
    "Call execute_sql to get real data. "
    "Output ONLY: {\"function_name\":\"execute_sql\",\"params\":{\"query\":\"SELECT ...\"}}"
)

# Compact final-answer reminder (appended only on last allowed tool call)
_FINAL_ANSWER_SUFFIX = (
    "\nFINAL ANSWER RULES: no ```sql``` blocks, no JSON tool calls, "
    "no 'j\\'ai utilisé la requête', all numbers from SQL results above only."
)

_STATISTICIAN_SYSTEM = """
## ROLE
Tu es un expert statisticien et analyste de données. L'utilisateur t'a fourni un fichier
de données avec ses statistiques descriptives et un aperçu de son contenu.

## INSTRUCTIONS
- Analyse les données fournies de façon approfondie et structurée.
- Utilise **uniquement** les données présentes dans le message.
- Présente ton analyse avec des **tableaux markdown** pour les comparaisons.
- Structure ta réponse avec des **titres markdown** (##, ###).
- Termine par des **recommandations actionnables** basées sur les données.
- Réponds dans la même langue que l'utilisateur (français ou anglais).
- N'invente pas de données ni de valeurs non présentes dans l'aperçu fourni.
- **Toutes les analyses de performance portent exclusivement sur les valeurs (TND / sales_value).**

## FORMAT DE RÉPONSE
1. **Vue d'ensemble** — dataset, structure, qualité
2. **Statistiques descriptives** — interprétation colonne par colonne
3. **Tendances & patterns** — insights clés
4. **Anomalies & outliers** — points d'attention
5. **Corrélations** — relations entre variables (si pertinent)
6. **Conclusion & recommandations** — insights actionnables
"""


# ── Main entry point ───────────────────────────────────────────────────────────

async def stream_response(
    session_id:   str,
    user_message: str,
    user_context: Optional[dict] = None,
) -> AsyncGenerator[str, None]:
    file_mode  = _is_file_analysis(user_message)
    max_tokens = TOKENS_FILE_ANALYSIS if file_mode else TOKENS_STANDARD

    logger.info(
        "🎯 Mode: %s | session=%s | tokens=%d",
        "FILE" if file_mode else "SQL", session_id, max_tokens,
    )

    history        = session_manager.get_trimmed_history(session_id, min(settings.llm_max_history_turns, 4))
    final_response = ""

    # ── FILE MODE ─────────────────────────────────────────────────────────────
    if file_mode:
        messages    = build_messages(history, user_message, user_context=user_context)
        messages[0] = {"role": "system", "content": _STATISTICIAN_SYSTEM}
        stream = await _client.chat.completions.create(
            model=settings.llm_model,
            messages=messages,
            temperature=0.3,
            max_tokens=max_tokens,
            stream=True,
        )
        chunks: list[str] = []
        async for chunk in stream:
            token = chunk.choices[0].delta.content
            if token:
                chunks.append(token)
                yield token
        final_response = "".join(chunks)

    # ── SQL MODE ──────────────────────────────────────────────────────────────
    else:
        gamme_perms  = (user_context or {}).get("gamme_permissions") or None
        system_text  = build_system_prompt(gamme_perms)

        enriched_message, enhancement = await enhance_query(
            user_message=user_message,
            catalog_text=system_text,
            user_context=user_context,
        )

        logger.info(
            "✨ Enhancement | kpi=%-20s | y=%s/%-4s | join=%s | scope=%s | flags=%d",
            enhancement.detected_kpi,
            enhancement.year_p1, enhancement.year_p0,
            enhancement.needs_join,
            bool(gamme_perms),
            len(enhancement.ambiguity_flags),
        )

        messages         = build_messages(history, enriched_message, user_context=user_context, enhancement=enhancement)
        tool_call_count  = 0
        sql_was_executed = False
        executed_sqls: list[str] = []   # all SQL queries run this turn — shown to user

        # ── Data completeness check ───────────────────────────────────────────
        # Injected once per turn into the system message. Compact format to
        # save tokens. Never counts as a tool call.
        try:
            coverage_sql = (
                "SELECT strftime('%Y',date) AS yr, "
                "COUNT(DISTINCT strftime('%Y-%m',date)) AS nb_mois, "
                "MIN(date) AS debut, MAX(date) AS fin "
                "FROM ims_data GROUP BY yr ORDER BY yr"
            )
            cov_db = get_db()
            cov_result = cov_db.execute(coverage_sql).fetchall()
            cov_db.close()

            max_months    = max((r[1] for r in cov_result), default=12)
            partial_years = [(r[0], r[1]) for r in cov_result if r[1] < max_months]

            # Build compact one-liner per year
            cov_lines = [f"{r[0]}:{r[1]}mo({r[2][:7]}→{r[3][:7]})"
                         + (" PARTIAL" if r[1] < max_months else "")
                         for r in cov_result]

            partial_warn = ""
            if partial_years:
                py_str = ", ".join(f"{yr}({nb}mo)" for yr, nb in partial_years)
                partial_warn = (
                    f" ⚠️ PARTIAL: {py_str} — restrict year comparisons to common months."
                )

            coverage_note = (
                f"\n[COVERAGE]{partial_warn} "
                + " | ".join(cov_lines)
                + "\n[/COVERAGE]\n"
            )
            messages[0]["content"] += coverage_note
        except Exception as _cov_err:
            logger.debug("Coverage check skipped: %s", _cov_err)

        while tool_call_count <= MAX_TOOL_CALLS:
            llm_response = await _call_llm(messages, temperature=0.0, max_tokens=max_tokens)
            logger.debug("🤖 LLM iter %d:\n%s", tool_call_count, llm_response[:400])

            # ── Path A: proper JSON tool call ─────────────────────────────────
            func_call = _parse_function_call(llm_response)
            if func_call:
                tool_call_count  += 1
                sql_was_executed  = True
                logger.info("🔧 Tool call #%d: %s", tool_call_count, func_call.get("function_name"))
                tool_result = _execute_tool(func_call, executed_sqls)
                messages.append({"role": "assistant", "content": llm_response})
                messages.append({
                    "role":    "user",
                    "content": _TOOL_RESULT_PREFIX.format(result=tool_result),
                })
                if tool_call_count >= MAX_TOOL_CALLS:
                    messages.append({"role": "user", "content": _FINAL_ANSWER_SUFFIX})
                continue

            # ── Path B: LLM wrote ```sql block instead of JSON ────────────────
            # The model "explained" its query in markdown rather than calling the
            # tool — intercept, execute the SQL ourselves, and loop for real answer.
            markdown_sql = _extract_markdown_sql(llm_response)
            if markdown_sql and not sql_was_executed:
                tool_call_count  += 1
                sql_was_executed  = True
                logger.warning("⚠️  Markdown SQL intercepted — executing directly")
                tool_result = _run_sql(markdown_sql, executed_sqls)
                # Rewrite history: pretend model issued a proper JSON call
                fake_call = json.dumps({
                    "function_name": "execute_sql",
                    "params": {"query": markdown_sql},
                })
                messages.append({"role": "assistant", "content": fake_call})
                messages.append({
                    "role":    "user",
                    "content": _TOOL_RESULT_PREFIX.format(result=tool_result),
                })
                messages.append({"role": "user", "content": _FINAL_ANSWER_SUFFIX})
                continue

            # ── Path C: hallucinated answer (round numbers, no SQL) ───────────
            if _looks_like_hallucination(llm_response) and not sql_was_executed:
                tool_call_count += 1
                logger.warning("🚨 Hallucination detected — demanding SQL execution")
                messages.append({"role": "assistant", "content": llm_response})
                messages.append({"role": "user", "content": _HALLUCINATION_MSG})
                continue

            # ── Path D: no SQL executed at all yet on a data question ─────────
            # Guard: if zero SQL calls happened and user asked a numeric question,
            # force one SQL call before accepting any answer.
            numeric_keywords = re.compile(
                r'\b(vente|sale|taux|réalisation|objectif|total|montant|'
                r'chiffre|valeur|TND|performance|kpi)\b',
                re.IGNORECASE,
            )
            if (not sql_was_executed
                    and tool_call_count == 0
                    and numeric_keywords.search(user_message)):
                tool_call_count += 1
                logger.warning("🚨 No SQL for numeric query — forcing tool call")
                messages.append({"role": "user", "content": _FORCE_SQL_MSG})
                continue

            # ── Path E: legitimate final answer ──────────────────────────────
            cleaned = _clean_response(llm_response)
            if not cleaned:
                cleaned = llm_response.strip()

            # Prepend executed SQL sentinel so the frontend can display it.
            # Format: __SQL_START__["SELECT ...","SELECT ..."]__SQL_END__

            # The frontend strips this before rendering the answer text.
            if executed_sqls:
                sentinel = (
                    _SQL_SENTINEL_START
                    + json.dumps(executed_sqls, ensure_ascii=False)
                    + _SQL_SENTINEL_END
                    + "\n"
                )
                yield sentinel

            CHUNK = 48
            chunks: list[str] = []
            for i in range(0, len(cleaned), CHUNK):
                piece = cleaned[i : i + CHUNK]
                chunks.append(piece)
                yield piece
            final_response = "".join(chunks)
            break

    session_manager.append(session_id, "user",      user_message)
    session_manager.append(session_id, "assistant", final_response)