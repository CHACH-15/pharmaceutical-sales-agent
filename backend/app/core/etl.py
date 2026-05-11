from __future__ import annotations

import io
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

GOUVERNORAT_COORDS: dict[str, tuple[float, float]] = {
    "Tunis":       (36.8190, 10.1658),
    "Ariana":      (36.8625, 10.1956),
    "Ben Arous":   (36.7535, 10.2282),
    "Manouba":     (36.8103, 10.1008),
    "Nabeul":      (36.4513, 10.7352),
    "Zaghouan":    (36.4041, 10.1429),
    "Bizerte":     (37.2745,  9.8739),
    "Béja":        (36.7328,  9.1817),
    "Jendouba":    (36.5012,  8.7802),
    "Le Kef":      (36.1820,  8.7042),
    "Siliana":     (36.0849,  9.3711),
    "Sousse":      (35.8284, 10.6360),
    "Monastir":    (35.7643, 10.8113),
    "Mahdia":      (35.5047, 11.0622),
    "Sfax":        (34.7400, 10.7600),
    "Kairouan":    (35.6781, 10.0963),
    "Kasserine":   (35.1676,  8.8365),
    "Sidi Bouzid": (35.0382,  9.4849),
    "Gabès":       (33.8827,  9.1193),
    "Médenine":    (33.3549, 10.5055),
    "Tataouine":   (32.9299, 10.4510),
    "Gafsa":       (34.4250,  8.7842),
    "Tozeur":      (33.9197,  8.1335),
    "Kébili":      (33.7050,  8.9710),
}

PERF_RED    = 80.0
PERF_YELLOW = 100.0


# ── Helpers ────────────────────────────────────────────────────────────────────

def _excel_serial(v) -> Optional[str]:
    try:
        dt = datetime(1899, 12, 30) + timedelta(days=int(float(v)))
        return dt.strftime("%Y-%m-01")
    except Exception:
        return None


def _parse_date(val) -> Optional[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, (int, float)):
        return _excel_serial(val)
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-01")
    s = str(val).strip()
    if re.match(r"\d{4}-\d{2}", s):
        return s[:7] + "-01"
    try:
        return _excel_serial(float(s))
    except Exception:
        return s


def _norm(s) -> str:
    """Lowercase + collapse whitespace for join keys."""
    if not s or (isinstance(s, float) and pd.isna(s)):
        return ""
    return re.sub(r"\s+", " ", str(s).strip().lower())


def _gamme_normalize(val) -> Optional[str]:
    """
    Normalize gamme value to lowercase 'aigu' or 'chronique'.
    Source: Marche.Gamme column which contains 'Aigu' or 'Chronique'.
    """
    if not val or (isinstance(val, float) and pd.isna(val)):
        return None
    v = str(val).strip().lower()
    if "aigu" in v or v.startswith("ai"):
        return "aigu"
    if "chron" in v:
        return "chronique"
    # Return as-is lowercased if neither matches
    return v or None


def _read_file(content: bytes, filename: str) -> pd.DataFrame:
    name = (filename or "").lower()
    if name.endswith(".csv"):
        for sep in (";", ",", "\t"):
            try:
                df = pd.read_csv(io.BytesIO(content), sep=sep)
                if df.shape[1] > 1:
                    return df
            except Exception:
                continue
        raise ValueError(f"Cannot parse CSV: {filename}")
    return pd.read_excel(io.BytesIO(content))


# ── Lookup builders ────────────────────────────────────────────────────────────

def _build_sect_lookup(sectori_df: pd.DataFrame) -> dict:
    """
    Build TWO lookups from Sectorisation:

    1. lk[brick_norm] → territory metadata (gouvernorat, region, sv).
       Uses first occurrence per brick for stable territoire fields.

    2. lk_by_gamme[(brick_norm, gamme_code)] → {mr, sv, gouvernorat, region}
       Allows picking the correct MR for a specific (brick, sous_gamme) pair.
       This is critical for bricks like Sfax 1A2 where different MRs handle
       different gamme codes (Ahmed Gharbi→AI2/META, Karim Belaid→AI1).

    Sectorisation columns used:
      Bricks      → key (matches IMS.GSU and TARGET.GSU)
      Gouvernorat → gouvernorat
      REGION      → region
      MR          → mr (Medical Representative)
      SV          → sv (Supervisor)
      GAMME       → sous_gamme code (AI1, AI2, META …)
    """
    df = sectori_df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    lk: dict        = {}  # brick → territory (first match)
    lk_by_gamme: dict = {}  # (brick, gamme_code) → {mr, sv, ...}

    for _, row in df.iterrows():
        brick = str(row.get("Bricks") or "").strip()
        if not brick or brick.lower() in ("nan", "none", ""):
            continue

        key           = _norm(brick)
        sous_gamme_raw = str(row.get("GAMME") or "").strip() or None
        gouvernorat    = str(row.get("Gouvernorat") or "").strip() or None
        region         = str(row.get("REGION")      or "").strip() or None
        mr             = str(row.get("MR")           or "").strip() or None
        sv             = str(row.get("SV")           or "").strip() or None

        # Brick-level lookup (first occurrence wins for stable fields)
        if key not in lk:
            lk[key] = {
                "gouvernorat": gouvernorat,
                "region":      region,
                "mr":          mr,
                "sv":          sv,
                "sous_gamme":  sous_gamme_raw,
            }
        else:
            # Fill sous_gamme if still None
            if sous_gamme_raw and lk[key]["sous_gamme"] is None:
                lk[key]["sous_gamme"] = sous_gamme_raw

        # Gamme-specific lookup (first MR per (brick, gamme_code) wins)
        if sous_gamme_raw:
            gamme_key = (key, sous_gamme_raw)
            if gamme_key not in lk_by_gamme:
                lk_by_gamme[gamme_key] = {
                    "gouvernorat": gouvernorat,
                    "region":      region,
                    "mr":          mr,
                    "sv":          sv,
                }

    # Attach gamme lookup to the result so callers can use it
    lk["__by_gamme__"] = lk_by_gamme
    return lk


# Mapping from product gamme (aigu/chronique) to the primary sous_gamme code
# used in Sectorisation.  This drives correct MR assignment per IMS row.
_GAMME_TO_SOUS_GAMME: dict[str, str] = {
    "aigu":      "AI2",   # primary aigu code in Sectorisation
    "chronique": "META",  # primary chronique code
}


def _build_marche_lookup(marche_df: pd.DataFrame) -> dict:
    """
    Build lookups from Marche file:
      by_forme  : HIKMA PHARMA_01 (forme) → { ourproduct, gamme }
      by_market : HIKMA PHARMA  (market)  → gamme

    Marche columns used:
      HIKMA PHARMA_01 → forme key (matches IMS.FORME and TARGET.FORME)
      HIKMA PHARMA    → market key (matches IMS.MARKET)
      Gamme           → 'Aigu' or 'Chronique' → gamme
      Produit Hikma   → 'oui' → ourproduct = 1
    """
    df = marche_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    by_forme:  dict = {}
    by_market: dict = {}

    for _, row in df.iterrows():
        hikma_col  = str(row.get("Produit Hikma") or "").strip().lower()
        gamme_val  = _gamme_normalize(row.get("Gamme"))
        forme_key  = _norm(row.get("HIKMA PHARMA_01"))
        market_key = _norm(row.get("HIKMA PHARMA"))

        if forme_key and forme_key not in by_forme:
            by_forme[forme_key] = {
                "ourproduct": hikma_col == "oui",
                "gamme":      gamme_val,
                # Store the product's OWN market key so we can flag is_own_market
                # in IMS rows. A Hikma product's own market = HIKMA PHARMA column.
                "own_market": market_key,
            }

        if market_key and market_key not in by_market and gamme_val:
            by_market[market_key] = gamme_val

    return {"by_forme": by_forme, "by_market": by_market}


# ── IMS transform ──────────────────────────────────────────────────────────────

def transform_ims(
    ims_df:     pd.DataFrame,
    marche_df:  pd.DataFrame,
    sectori_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Transform raw IMS data into the ims_data schema.

    IMS source columns:
      Month   → date
      GSU     → gsu  (brick code, key for Sectorisation join)
      MARKET  → market
      PRODUIT → product
      FORME   → forme (key for Marche join)
      VALEURS → sales_value
      UNITES  → sales_quantity
      SU      → dropped (numeric row identifier, not used)

    Enrichment from Sectorisation (join on GSU = Bricks):
      Gouvernorat → gouvernorat
      REGION      → region
      MR          → mr
      SV          → sv
      GAMME       → sous_gamme  (e.g. AI1, AI2, META)

    Enrichment from Marche (join on FORME = HIKMA PHARMA_01):
      Gamme         → gamme        (aigu / chronique)
      Produit Hikma → ourproduct   (1 if 'oui', else 0)
    """
    df = ims_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    sect   = _build_sect_lookup(sectori_df)
    marche = _build_marche_lookup(marche_df)

    # Extract the (brick, gamme_code) → MR sub-lookup built inside _build_sect_lookup
    sect_by_gamme: dict = sect.pop("__by_gamme__", {})

    # ── Date ──────────────────────────────────────────────────────────────────
    month_col = next(
        (c for c in df.columns if c.upper() in ("MONTH", "MOIS")), None
    )
    df["date"] = df[month_col].apply(_parse_date) if month_col else None

    # ── Rename IMS source columns ─────────────────────────────────────────────
    # NOTE: SU is a numeric row-identifier — it is NOT sous_gamme. Drop it.
    rename_map = {
        "GSU":     "gsu",
        "MARKET":  "market",
        "PRODUIT": "product",
        "FORME":   "forme",
        "VALEURS": "sales_value",
        "UNITES":  "sales_quantity",
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

    # Normalize join-key columns (strip whitespace, keep original case for DB)
    for col in ("gsu", "product", "forme"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace("nan", None)

    # ── Marche enrichment FIRST (gamme needed for correct MR lookup) ──────────
    def _m(forme, field):
        return marche["by_forme"].get(_norm(forme), {}).get(field)

    df["ourproduct"] = df["forme"].apply(lambda f: 1 if _m(f, "ourproduct") else 0)

    # gamme: Marche-by-forme → Marche-by-market fallback
    def _get_gamme(row):
        g = _m(row.get("forme", ""), "gamme")
        if g:
            return g
        return marche["by_market"].get(_norm(row.get("market", "")))

    df["gamme"] = df.apply(_get_gamme, axis=1)

    # ── sous_gamme: derived from product gamme, not first sectorisation row ──
    # Root-cause fix: a brick covers multiple gamme codes (AI1, AI2, META).
    # Using the first sectorisation row's code assigns the SAME sous_gamme to
    # all products regardless of their gamme (e.g. chronique products in
    # Ariana get AI2 instead of META).  The correct approach: map the product's
    # gamme to its primary sous_gamme code, then use (gsu, code) to find MR.
    df["sous_gamme"] = df["gamme"].apply(
        lambda g: _GAMME_TO_SOUS_GAMME.get(g) if g else None
    )

    # ── Sectorisation enrichment (via GSU = Bricks) ───────────────────────────
    # Territory fields (gouvernorat, region, sv) come from brick-level lookup.
    # MR comes from the (brick, sous_gamme_code) lookup for correctness.
    def _s(gsu, field):
        return sect.get(_norm(gsu), {}).get(field)

    def _mr_for_row(row):
        """Return MR using (gsu, sous_gamme_code) for precision, fallback to brick-level."""
        gsu        = row.get("gsu", "")
        sg_code    = row.get("sous_gamme")
        brick_key  = _norm(gsu)
        if sg_code:
            gamme_rec = sect_by_gamme.get((brick_key, sg_code))
            if gamme_rec:
                return gamme_rec["mr"]
        # Fallback: first MR in the brick
        return sect.get(brick_key, {}).get("mr")

    df["gouvernorat"] = df["gsu"].apply(lambda g: _s(g, "gouvernorat"))
    df["region"]      = df["gsu"].apply(lambda g: _s(g, "region"))
    df["sv"]          = df["gsu"].apply(lambda g: _s(g, "sv"))
    df["mr"]          = df.apply(_mr_for_row, axis=1)

    # ── is_own_market flag ────────────────────────────────────────────────────
    # A Hikma product appears in multiple IMS therapeutic markets, but its TRUE
    # sales belong to its OWN market (HIKMA PHARMA in Marche.xlsx).
    # Rows where IMS.MARKET != product's own market are competitive-context rows
    # that should NOT be summed when computing Hikma's absolute sales or taux.
    # is_own_market = 1 flags the one valid row per (date, product, forme, gsu).
    def _own_market(row):
        if not row.get("ourproduct"):
            return 0  # competitor rows: always keep all (needed for PDM)
        own_mkt = marche["by_forme"].get(_norm(row.get("forme", "")), {}).get("own_market")
        if not own_mkt:
            return 1  # unknown product: include by default
        return 1 if _norm(row.get("market", "")) == own_mkt else 0

    df["is_own_market"] = df.apply(_own_market, axis=1)

    # ── Select final columns ──────────────────────────────────────────────────
    wanted = [
        "date", "gouvernorat", "gsu", "region", "market", "product",
        "forme", "ourproduct", "is_own_market", "mr", "sv",
        "sales_value", "sales_quantity", "gamme", "sous_gamme",
    ]
    for col in wanted:
        if col not in df.columns:
            df[col] = None

    return df[wanted]


# ── TARGET transform ───────────────────────────────────────────────────────────

def transform_target(
    target_df:  pd.DataFrame,
    sectori_df: pd.DataFrame,
    marche_df:  pd.DataFrame,
) -> pd.DataFrame:
    """
    Transform raw TARGET data into the target_data schema.
    TARGET source columns are kept as-is in naming intent:
      MOIS            → date
      PRODUIT         → product
      FORME           → forme
      GSU             → gsu
      OBJECTIF UNITE  → unit_objectif
      OBJECTIF VALEUR → value_objectif

    Enrichment from Sectorisation (join on GSU = Bricks):
      GAMME → sous_gamme  (codes like AI1, AI2, META)

    Enrichment from Marche (join on FORME = HIKMA PHARMA_01):
      Gamme → gamme  (aigu / chronique)
    """
    df = target_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # ── Date ──────────────────────────────────────────────────────────────────
    month_col = next(
        (c for c in df.columns if c.upper() in ("MOIS", "MONTH")), None
    )
    df["date"] = df[month_col].apply(_parse_date) if month_col else None

    # ── Rename TARGET source columns ──────────────────────────────────────────
    rename_map = {
        "PRODUIT":         "product",
        "FORME":           "forme",
        "GSU":             "gsu",
        "OBJECTIF UNITE":  "unit_objectif",
        "OBJECTIF VALEUR": "value_objectif",
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

    for col in ("gsu", "product", "forme"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace("nan", None)

    # ── Sectorisation enrichment (via GSU = Bricks) ───────────────────────────
    sect   = _build_sect_lookup(sectori_df)
    marche = _build_marche_lookup(marche_df)

    # Pop the gamme sub-lookup (not needed for target — just avoid __by_gamme__ key)
    sect.pop("__by_gamme__", None)

    def _s(gsu, field):
        return sect.get(_norm(gsu), {}).get(field)

    def _m_forme(forme):
        return marche["by_forme"].get(_norm(forme), {}).get("gamme")

    # gamme: prefer Marche-by-forme; fall back to nothing (TARGET has no market col)
    df["gamme"] = df["forme"].apply(_m_forme)

    # sous_gamme: derive from product gamme using the same mapping as ims_data ETL
    # This ensures kpi_cache joins correctly on sous_gamme between both tables.
    df["sous_gamme"] = df["gamme"].apply(
        lambda g: _GAMME_TO_SOUS_GAMME.get(g) if g else None
    )

    # ── Select final columns ──────────────────────────────────────────────────
    wanted = [
        "date", "product", "forme", "gsu",
        "value_objectif", "unit_objectif", "gamme", "sous_gamme",
    ]
    for col in wanted:
        if col not in df.columns:
            df[col] = None

    # ── Deduplicate target rows ───────────────────────────────────────────────
    # TARGET.xlsx sometimes has duplicate rows per (date, product, forme, gsu):
    # either from data entry, or because the file contains both monthly and
    # cumulative targets for the same key.  Summing duplicates inflates the
    # denominator in taux de réalisation.
    # Strategy: keep only ONE row per (date, product, forme, gsu).
    # We take the MAXIMUM value_objectif (the most ambitious objective) so we
    # are never artificially flattering the taux.
    key_cols = ["date", "product", "forme", "gsu"]
    # Only deduplicate where all key cols are non-null
    mask_complete = df[key_cols].notna().all(axis=1)
    df_complete   = df[mask_complete].copy()
    df_incomplete = df[~mask_complete].copy()

    if not df_complete.empty:
        df_complete = (
            df_complete
            .sort_values("value_objectif", ascending=False, na_position="last")
            .drop_duplicates(subset=key_cols, keep="first")
        )

    df = pd.concat([df_complete, df_incomplete], ignore_index=True)

    before = len(df)  # will log in caller
    df.attrs["dedup_note"] = f"target_data deduped: {before} unique rows after MAX dedup"

    return df[wanted]


# ── KPI cache ──────────────────────────────────────────────════════════════════

def compute_kpi_cache(db) -> int:
    db.execute("DELETE FROM kpi_cache")

    # WHY THE CTE IS MANDATORY:
    # IMS has multiple rows per (date, product, forme, gsu) - one per therapeutic
    # market (e.g. MARCHE LIPOVEX + MARCHE STATINES for the same forme/gsu/date).
    # Without pre-aggregation the JOIN with target_data (which has exactly ONE row
    # per date+product+forme+gsu) multiplies t.value_objectif by the number of
    # market rows, inflating the target N-fold and collapsing taux to near-zero.
    # Fix: aggregate IMS to one row per join key BEFORE joining target.
    rows = db.execute("""
        WITH ims_agg AS (
            SELECT
                date,
                LOWER(TRIM(COALESCE(product,    ''))) AS product,
                LOWER(TRIM(COALESCE(forme,      ''))) AS forme,
                LOWER(TRIM(COALESCE(gsu,        ''))) AS gsu,
                gouvernorat,
                region,
                mr,
                sv,
                COALESCE(gamme,      '') AS gamme,
                COALESCE(sous_gamme, '') AS sous_gamme,
                SUM(sales_value)          AS sales_value,
                SUM(sales_quantity)       AS sales_quantity
            FROM ims_data
            WHERE ourproduct = 1
              AND is_own_market = 1   -- only the product's own-market row
            GROUP BY
                date, product, forme, gsu,
                gouvernorat, region, mr, sv,
                gamme, sous_gamme
        ),
        tgt AS (
            SELECT
                date,
                LOWER(TRIM(COALESCE(product, ''))) AS product,
                LOWER(TRIM(COALESCE(forme,   ''))) AS forme,
                LOWER(TRIM(COALESCE(gsu,     ''))) AS gsu,
                SUM(value_objectif)    AS value_objectif,
                SUM(unit_objectif)     AS unit_objectif
            FROM target_data
            GROUP BY date, product, forme, gsu
        )
        SELECT
            i.date,
            i.product,
            i.forme,
            i.gsu,
            i.gouvernorat,
            i.region,
            i.mr,
            i.sv,
            i.gamme,
            i.sous_gamme,
            ROUND(i.sales_value,                                           2) AS actual_value,
            ROUND(i.sales_quantity,                                        0) AS actual_quantity,
            ROUND(t.value_objectif,                                        2) AS target_value,
            ROUND(t.unit_objectif,                                         0) AS target_quantity,
            ROUND(i.sales_value    * 100.0 / NULLIF(t.value_objectif, 0),  1) AS taux_realisation_value_pct,
            ROUND(i.sales_quantity * 100.0 / NULLIF(t.unit_objectif,  0),  1) AS taux_realisation_unit_pct
        FROM ims_agg i
        JOIN tgt t
          ON  i.date                  = t.date
          AND LOWER(TRIM(i.product))  = LOWER(TRIM(t.product))
          AND LOWER(TRIM(i.forme))    = LOWER(TRIM(t.forme))
          AND LOWER(TRIM(i.gsu))      = LOWER(TRIM(t.gsu))
        ORDER BY i.date ASC
    """).fetchall()

    if rows:
        now = datetime.now().isoformat(timespec="seconds")
        db.executemany(
            """INSERT INTO kpi_cache
               (date, product, forme, gsu, gouvernorat, region, mr, sv,
                gamme, sous_gamme,
                actual_value, actual_quantity, target_value, target_quantity,
                taux_realisation_value_pct, taux_realisation_unit_pct, computed_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [
                (r["date"], r["product"], r["forme"], r["gsu"],
                 r["gouvernorat"], r["region"], r["mr"], r["sv"],
                 r["gamme"], r["sous_gamme"],
                 r["actual_value"], r["actual_quantity"],
                 r["target_value"], r["target_quantity"],
                 r["taux_realisation_value_pct"], r["taux_realisation_unit_pct"],
                 now)
                for r in rows
            ],
        )
        db.commit()

    logger.info("KPI cache rebuilt: %d rows", len(rows))
    return len(rows)


# ── IMS ETL entry point ────────────────────────────────────────────────────────

def run_ims_etl(
    ims_content:      bytes,
    ims_filename:     str,
    marche_content:   bytes,
    marche_filename:  str,
    sectori_content:  bytes,
    sectori_filename: str,
    admin_id:         int,
    db,
    incremental:      bool = False,
) -> dict:
    try:
        ims_raw     = _read_file(ims_content,     ims_filename)
        marche_raw  = _read_file(marche_content,  marche_filename)
        sectori_raw = _read_file(sectori_content, sectori_filename)

        ims_out = transform_ims(ims_raw, marche_raw, sectori_raw)

        if incremental:
            new_dates = ims_out["date"].dropna().unique().tolist()
            if new_dates:
                placeholders = ",".join("?" * len(new_dates))
                db.execute(
                    f"DELETE FROM ims_data WHERE date IN ({placeholders})", new_dates
                )
                db.commit()
            logger.info("Incremental: replacing %d month(s) in ims_data", len(new_dates))
        else:
            db.execute("DELETE FROM ims_data")
            db.commit()
            logger.info("Full load: truncated ims_data")

        ims_out.to_sql("ims_data", db, if_exists="append", index=False)
        kpi_rows = compute_kpi_cache(db)

        batch_row = db.execute(
            "SELECT COALESCE(MAX(upload_batch), 0) FROM uploaded_files"
        ).fetchone()
        batch = (batch_row[0] or 0) + 1
        now   = datetime.now().isoformat(timespec="seconds")
        dr    = db.execute("SELECT MIN(date), MAX(date) FROM ims_data").fetchone()

        for fname, ftype, content_bytes, raw_df in [
            (ims_filename,           "ims_raw",        ims_content,    ims_raw),
            (marche_filename,        "marche",          marche_content, marche_raw),
            (sectori_filename,       "sectorisation",   sectori_content, sectori_raw),
            ("ims_data [processed]", "ims_processed",  b"",            ims_out),
        ]:
            db.execute(
                "INSERT INTO uploaded_files "
                "(filename,file_type,file_size,row_count,columns,"
                " date_range_min,date_range_max,uploaded_by,uploaded_at,"
                " upload_batch,incremental) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (fname, ftype, len(content_bytes), len(raw_df),
                 json.dumps(list(raw_df.columns)),
                 dr[0], dr[1], admin_id, now, batch, int(incremental)),
            )
        db.commit()

        return {
            "success":     True,
            "incremental": incremental,
            "ims_rows":    len(ims_out),
            "kpi_rows":    kpi_rows,
            "batch":       batch,
            "date_range":  {"min": dr[0], "max": dr[1]},
        }

    except Exception as exc:
        logger.exception("IMS ETL error: %s", exc)
        return {"success": False, "error": str(exc)}


# ── TARGET ETL entry point ─────────────────────────────────────────────────────

def run_target_etl(
    target_content:   bytes,
    target_filename:  str,
    sectori_content:  bytes,
    sectori_filename: str,
    marche_content:   bytes,
    marche_filename:  str,
    admin_id:         int,
    db,
) -> dict:
    try:
        target_raw  = _read_file(target_content,  target_filename)
        sectori_raw = _read_file(sectori_content, sectori_filename)
        marche_raw  = _read_file(marche_content,  marche_filename)

        target_out = transform_target(target_raw, sectori_raw, marche_raw)

        db.execute("DELETE FROM target_data")
        db.commit()
        target_out.to_sql("target_data", db, if_exists="append", index=False)
        kpi_rows = compute_kpi_cache(db)

        batch_row = db.execute(
            "SELECT COALESCE(MAX(upload_batch), 0) FROM uploaded_files"
        ).fetchone()
        batch = (batch_row[0] or 0) + 1
        now   = datetime.now().isoformat(timespec="seconds")
        dr    = db.execute("SELECT MIN(date), MAX(date) FROM target_data").fetchone()

        for fname, ftype, content_bytes, raw_df in [
            (target_filename,              "target_raw",       target_content, target_raw),
            ("target_data [processed]",    "target_processed", b"",            target_out),
        ]:
            db.execute(
                "INSERT INTO uploaded_files "
                "(filename,file_type,file_size,row_count,columns,"
                " date_range_min,date_range_max,uploaded_by,uploaded_at,upload_batch) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (fname, ftype, len(content_bytes), len(raw_df),
                 json.dumps(list(raw_df.columns)),
                 dr[0], dr[1], admin_id, now, batch),
            )
        db.commit()

        return {
            "success":     True,
            "target_rows": len(target_out),
            "kpi_rows":    kpi_rows,
            "batch":       batch,
            "date_range":  {"min": dr[0], "max": dr[1]},
        }

    except Exception as exc:
        logger.exception("TARGET ETL error: %s", exc)
        return {"success": False, "error": str(exc)}


# ── Map data helper ────────────────────────────────────────────────────────────

def get_map_data(
    db,
    mr_name:    Optional[str] = None,
    sv_name:    Optional[str] = None,
    gsu_filter: Optional[str] = None,
) -> list:
    """
    Return per-gouvernorat KPIs for the map dashboard.

    Strategy (avoids both known pitfalls):
    ─ actual_value / actual_quantity  → from ims_data directly.
      Reason: kpi_cache is an INNER JOIN and silently drops IMS rows that have
      no matching target, causing up to 57% undercounting for some gouvernorats.

    ─ target_value / target_quantity  → from kpi_cache (already correctly matched).

    ─ taux_realisation_value_pct      → recomputed as
        SUM(kpi_cache.actual_value) / SUM(kpi_cache.target_value)
      so taux is always based on the same (date,product,forme,gsu) pairs where
      both actual AND target exist (i.e., a fair comparison).

    This gives: a correct full-period actual, a correct target for the tracked
    products, and a meaningful taux that is never inflated by unmatched rows.
    """
    # ── Filters ───────────────────────────────────────────────────────────────
    ims_conds: list[str] = [
        "i.ourproduct = 1",
        "i.is_own_market = 1",
        "i.gouvernorat IS NOT NULL",
    ]
    kpi_conds: list[str] = ["k.gouvernorat IS NOT NULL"]
    ims_params: list = []
    kpi_params: list = []

    if mr_name:
        ims_conds.append("i.mr = ?");   ims_params.append(mr_name)
        kpi_conds.append("k.mr = ?");   kpi_params.append(mr_name)
    if sv_name:
        ims_conds.append("i.sv = ?");   ims_params.append(sv_name)
        kpi_conds.append("k.sv = ?");   kpi_params.append(sv_name)
    if gsu_filter:
        ims_conds.append("i.gsu = ?");  ims_params.append(gsu_filter)
        kpi_conds.append("k.gsu = ?");  kpi_params.append(gsu_filter)

    where_ims = "WHERE " + " AND ".join(ims_conds)
    where_kpi = "WHERE " + " AND ".join(kpi_conds)

    # ── Actual sales (full, from ims_data) ───────────────────────────────────
    actual_rows = db.execute(f"""
        SELECT
            i.gouvernorat,
            i.region,
            GROUP_CONCAT(DISTINCT i.mr) AS mrs,
            GROUP_CONCAT(DISTINCT i.sv) AS svs,
            ROUND(SUM(i.sales_value),    2) AS total_actual_value,
            ROUND(SUM(i.sales_quantity), 0) AS total_actual_qty,
            COUNT(DISTINCT i.product)       AS nb_products
        FROM ims_data i
        {where_ims}
        GROUP BY i.gouvernorat, i.region
        ORDER BY i.gouvernorat
    """, ims_params).fetchall()

    # ── Targets + taux (from kpi_cache, already correct INNER JOIN) ──────────
    kpi_rows = db.execute(f"""
        SELECT
            k.gouvernorat,
            ROUND(SUM(k.actual_value),    2) AS kpi_actual_value,
            ROUND(SUM(k.target_value),    2) AS total_target_value,
            ROUND(SUM(k.actual_quantity), 0) AS kpi_actual_qty,
            ROUND(SUM(k.target_quantity), 0) AS total_target_qty,
            ROUND(SUM(k.actual_value) * 100.0 / NULLIF(SUM(k.target_value), 0), 1)
                AS taux_realisation_value_pct
        FROM kpi_cache k
        {where_kpi}
        GROUP BY k.gouvernorat
    """, kpi_params).fetchall()

    kpi_by_gov = {r["gouvernorat"]: dict(r) for r in kpi_rows}

    result = []
    for row in actual_rows:
        d   = dict(row)
        gov = d["gouvernorat"] or ""
        kpi = kpi_by_gov.get(gov, {})

        coords = GOUVERNORAT_COORDS.get(gov, (None, None))
        taux   = kpi.get("taux_realisation_value_pct")

        d["total_target_value"] = kpi.get("total_target_value")
        d["total_target_qty"]   = kpi.get("total_target_qty")
        d["taux_realisation_value_pct"] = taux
        d["lat"]    = coords[0]
        d["lng"]    = coords[1]
        d["status"] = (
            "over"  if taux is not None and taux >= PERF_YELLOW else
            "under" if taux is not None and taux <  PERF_RED    else
            "watch"
        )
        d["mrs"] = [x for x in (d.get("mrs") or "").split(",") if x]
        d["svs"] = [x for x in (d.get("svs") or "").split(",") if x]
        result.append(d)

    return result