from __future__ import annotations

import logging
import sqlite3
from typing import Any, Optional

from app.config import settings
from app.core.system_prompt import SYSTEM_PROMPT

logger = logging.getLogger(__name__)

_catalog_cache: str | None = None
_catalog_data:  dict | None = None


def invalidate_catalog_cache() -> None:
    global _catalog_cache, _catalog_data
    _catalog_cache = None
    _catalog_data  = None
    logger.info("📂 Data catalog cache invalidated.")


def _fmt(values: list, max_items: int = 20) -> str:
    if not values:
        return "(none)"
    shown = values[:max_items]
    tail  = f" +{len(values) - max_items} more" if len(values) > max_items else ""
    return ", ".join(f"'{v}'" for v in shown) + tail


def _load_catalog_data() -> dict:
    global _catalog_data
    if _catalog_data is not None:
        return _catalog_data

    conn = sqlite3.connect(settings.db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    def fetch(sql: str) -> list:
        return [r[0] for r in conn.execute(sql).fetchall() if r[0] is not None]

    products    = fetch("SELECT DISTINCT product     FROM ims_data WHERE ourproduct=1 ORDER BY product")
    formes      = fetch("SELECT DISTINCT forme       FROM ims_data WHERE ourproduct=1 ORDER BY forme")
    gammes      = fetch("SELECT DISTINCT gamme       FROM ims_data WHERE ourproduct=1 AND gamme IS NOT NULL ORDER BY gamme")
    sous_gammes = fetch("SELECT DISTINCT sous_gamme  FROM ims_data WHERE ourproduct=1 AND sous_gamme IS NOT NULL ORDER BY sous_gamme")
    gsu_list    = fetch("SELECT DISTINCT gsu         FROM ims_data ORDER BY gsu")
    regions     = fetch("SELECT DISTINCT region      FROM ims_data WHERE region IS NOT NULL ORDER BY region")
    govs        = fetch("SELECT DISTINCT gouvernorat FROM ims_data WHERE gouvernorat IS NOT NULL ORDER BY gouvernorat")
    mrs         = fetch("SELECT DISTINCT mr          FROM ims_data WHERE mr IS NOT NULL ORDER BY mr")
    svs         = fetch("SELECT DISTINCT sv          FROM ims_data WHERE sv IS NOT NULL ORDER BY sv")
    markets     = fetch("SELECT DISTINCT market      FROM ims_data ORDER BY market")

    prod_map_rows = conn.execute("""
        SELECT product,
               GROUP_CONCAT(DISTINCT forme)      AS formes,
               MAX(gamme)                         AS gamme,
               GROUP_CONCAT(DISTINCT sous_gamme) AS sous_gammes
        FROM   ims_data
        WHERE  ourproduct = 1 AND gamme IS NOT NULL
        GROUP  BY product ORDER BY gamme, product LIMIT 50
    """).fetchall()

    date_row = conn.execute(
        "SELECT MIN(strftime('%Y-%m', date)), MAX(strftime('%Y-%m', date)) FROM ims_data"
    ).fetchone()
    target_date_row = conn.execute(
        "SELECT MIN(strftime('%Y-%m', date)), MAX(strftime('%Y-%m', date)) FROM target_data"
    ).fetchone()

    year_rows = conn.execute(
        "SELECT strftime('%Y', date) AS yr, COUNT(DISTINCT strftime('%Y-%m', date)) AS nb "
        "FROM ims_data GROUP BY yr ORDER BY yr"
    ).fetchall()

    conn.close()

    _catalog_data = dict(
        products=products, formes=formes, gammes=gammes,
        sous_gammes=sous_gammes, gsu_list=gsu_list, regions=regions,
        govs=govs, mrs=mrs, svs=svs, markets=markets,
        prod_map_rows=[dict(r) for r in prod_map_rows],
        date_ims=(date_row[0], date_row[1]),
        date_tgt=(target_date_row[0], target_date_row[1]),
        year_coverage=[(r["yr"], r["nb"]) for r in year_rows],
    )
    return _catalog_data


def _build_data_catalog() -> str:
    global _catalog_cache
    if _catalog_cache is not None:
        return _catalog_cache

    try:
        d = _load_catalog_data()

        prod_lines = []
        for r in d["prod_map_rows"]:
            fl = [f.strip() for f in (r["formes"] or "").split(",") if f.strip()]
            disp = ", ".join(fl[:4]) + (f" +{len(fl)-4}" if len(fl) > 4 else "")
            prod_lines.append(f"  {r['product']} [{r['gamme']}/{r['sous_gammes'] or '?'}]: {disp}")

        year_lines = [
            f"  {yr}: {nb} month(s)" + (" ⚠️ PARTIAL" if nb < 12 else "")
            for yr, nb in d["year_coverage"]
        ]

        _catalog_cache = (
            "\n---\n"
            "## LIVE DATA CATALOG (EXACT values only — SQLite case-sensitive)\n\n"
            f"IMS : {d['date_ims'][0]} → {d['date_ims'][1]}  |  "
            f"TARGET: {d['date_tgt'][0]} → {d['date_tgt'][1]}\n\n"
            "Year coverage:\n" + "\n".join(year_lines) + "\n\n"
            f"Products    : {_fmt(d['products'], 30)}\n"
            f"Gammes      : {_fmt(d['gammes'], 10)}\n"
            f"Sous-gammes : {_fmt(d['sous_gammes'], 15)}\n"
            f"Gouvernorats: {_fmt(d['govs'], 30)}\n"
            f"Regions     : {_fmt(d['regions'], 10)}\n"
            f"GSUs        : {_fmt(d['gsu_list'], 20)}\n"
            f"MRs         : {_fmt(d['mrs'], 20)}\n"
            f"SVs         : {_fmt(d['svs'], 10)}\n"
            f"Markets     : {_fmt(d['markets'], 20)}\n\n"
            "Product → [gamme/sous_gamme]: formes\n"
            + "\n".join(prod_lines) + "\n"
        )
        logger.info("📂 Data catalog built (compact).")
        return _catalog_cache

    except Exception as exc:
        logger.warning("⚠️ Could not build data catalog: %s", exc)
        return f"\n---\n## LIVE DATA CATALOG\n⚠️ Could not load: {exc}\n"


def _build_scoped_addon(enhancement) -> str:
    if enhancement is None:
        return ""
    try:
        d = _load_catalog_data()
    except Exception:
        return ""

    lines: list[str] = []

    products = getattr(enhancement, "products", []) or []
    if products:
        try:
            conn = sqlite3.connect(settings.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            for prod in products[:3]:
                rows = conn.execute(
                    "SELECT DISTINCT forme FROM ims_data WHERE ourproduct=1 "
                    "AND LOWER(product)=LOWER(?) ORDER BY forme", (prod,)
                ).fetchall()
                if rows:
                    lines.append(f"  Formes for '{prod}': {', '.join(r[0] for r in rows)}")
            conn.close()
        except Exception:
            pass

    mr = getattr(enhancement, "mr", None)
    if mr:
        exact = next((m for m in d["mrs"] if m.lower() == mr.lower()), None)
        if exact:
            lines.append(f"  MR exact name: '{exact}'")
        else:
            close = [m for m in d["mrs"] if mr.lower() in m.lower()][:3]
            if close:
                lines.append(f"  MR '{mr}' not found — closest: {', '.join(close)}")

    gov = getattr(enhancement, "gouvernorat", None)
    if gov:
        exact = next((g for g in d["govs"] if g.lower() == gov.lower()), None)
        if exact:
            lines.append(f"  Gouvernorat exact: '{exact}'")

    if not lines:
        return ""
    return "\n### Entity lookup:\n" + "\n".join(lines) + "\n"


def _build_gamme_scope_block(gamme_permissions: list) -> str:
    quoted = ", ".join(
        f"'{g['gamme']}'" for g in gamme_permissions if g.get("gamme")
    )
    if not quoted:
        return ""
    return (
        "\n---\n## ⚠️ GAMME SCOPE — MANDATORY\n"
        f"Allowed: [{quoted}]\n"
        f"Add `AND gamme IN ({quoted})` to EVERY query on ims_data, kpi_cache, target_data.\n"
        "Outside this scope → reply: 'Vous n'avez pas accès à cette gamme.'\n"
    )


def build_system_prompt(
    gamme_permissions: Optional[list] = None,
    enhancement=None,
) -> str:
    base = SYSTEM_PROMPT + _build_data_catalog()
    if enhancement is not None:
        base += _build_scoped_addon(enhancement)
    if gamme_permissions:
        scope = _build_gamme_scope_block(gamme_permissions)
        if scope:
            base += scope
    return base


def build_messages(
    history:      list[dict[str, Any]],
    user_message: str,
    user_context: Optional[dict] = None,
    enhancement=None,
) -> list[dict[str, Any]]:
    gamme_perms: list = []
    if user_context:
        gamme_perms = user_context.get("gamme_permissions") or []

    system_content = build_system_prompt(
        gamme_perms if gamme_perms else None,
        enhancement=enhancement,
    )

    return [
        {"role": "system", "content": system_content},
        *history,
        {"role": "user",   "content": user_message},
    ]