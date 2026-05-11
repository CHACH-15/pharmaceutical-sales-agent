"""
report_generator.py
~~~~~~~~~~~~~~~~~~~
Generates branded PDF performance reports for Medical Representatives
and Supervisors using ReportLab.

Install: pip install reportlab --break-system-packages

MR  Report: GSU breakdown, attention zones, top products, 6-month trend
SV  Report: Team ranking, underperforming MRs, gouvernorat breakdown, trend
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    HRFlowable,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

logger = logging.getLogger(__name__)

# ── Output directory ───────────────────────────────────────────────────────────
REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

# ── Brand colours ──────────────────────────────────────────────────────────────
C_RED       = colors.HexColor("#DC2626")
C_RED_LIGHT = colors.HexColor("#FEF2F2")
C_AMBER     = colors.HexColor("#B45309")
C_AMB_LIGHT = colors.HexColor("#FFFBEB")
C_GREEN     = colors.HexColor("#16A34A")
C_GRN_LIGHT = colors.HexColor("#F0FDF4")
C_TEAL      = colors.HexColor("#0D9488")
C_BLUE      = colors.HexColor("#1D4ED8")
C_VIOLET    = colors.HexColor("#7C3AED")
C_INK       = colors.HexColor("#1C1917")
C_INK_MID   = colors.HexColor("#57534E")
C_INK_LIGHT = colors.HexColor("#A8A29E")
C_SURFACE   = colors.HexColor("#FAF9F7")
C_BORDER    = colors.HexColor("#E7E5E4")

MONTHS_FR = {
    "01": "Janvier",  "02": "Février",  "03": "Mars",
    "04": "Avril",    "05": "Mai",      "06": "Juin",
    "07": "Juillet",  "08": "Août",     "09": "Septembre",
    "10": "Octobre",  "11": "Novembre", "12": "Décembre",
}
MONTHS_SHORT = {
    "01": "Jan", "02": "Fév", "03": "Mar", "04": "Avr",
    "05": "Mai", "06": "Juin","07": "Juil","08": "Août",
    "09": "Sep", "10": "Oct", "11": "Nov", "12": "Déc",
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _period_long(p: str) -> str:
    yr, mo = p[:4], p[5:7]
    return f"{MONTHS_FR.get(mo, mo)} {yr}"

def _period_short(p: str) -> str:
    yr, mo = p[:4], p[5:7]
    return f"{MONTHS_SHORT.get(mo, mo)} {yr}"

def _fmt_tnd(v) -> str:
    if v is None: return "—"
    return f"{v:,.0f} TND".replace(",", "\u202f")

def _fmt_pct(v) -> str:
    if v is None: return "—"
    return f"{v:.1f}%"

def _perf_color(taux: Optional[float]) -> colors.Color:
    if taux is None:  return C_INK_LIGHT
    if taux >= 100:   return C_GREEN
    if taux >= 80:    return C_AMBER
    return C_RED

def _perf_bg(taux: Optional[float]) -> colors.Color:
    if taux is None:  return C_SURFACE
    if taux >= 100:   return C_GRN_LIGHT
    if taux >= 80:    return C_AMB_LIGHT
    return C_RED_LIGHT

def _status_icon(taux: Optional[float]) -> str:
    if taux is None:  return "—"
    if taux >= 100:   return "✅ Sur objectif"
    if taux >= 80:    return "⚠ En cours"
    return "❌ Attention"


# ── Base Builder ───────────────────────────────────────────────────────────────

class _BaseReportBuilder:
    PAGE_W = 170 * mm      # usable width on A4 with 15 mm margins each side

    def __init__(self, period: str, db_conn):
        self.period       = period
        self.period_label = _period_long(period)
        self.db           = db_conn
        self._build_styles()

    def _build_styles(self):
        self.s_title = ParagraphStyle(
            "title", fontSize=22, fontName="Helvetica-Bold",
            textColor=colors.white, alignment=TA_CENTER, spaceAfter=3,
        )
        self.s_sub = ParagraphStyle(
            "sub", fontSize=10, fontName="Helvetica",
            textColor=colors.HexColor("#FECACA"), alignment=TA_CENTER,
        )
        self.s_section = ParagraphStyle(
            "sect", fontSize=12, fontName="Helvetica-Bold",
            textColor=C_INK, spaceBefore=14, spaceAfter=6,
        )
        self.s_body = ParagraphStyle(
            "body", fontSize=9, fontName="Helvetica",
            textColor=C_INK_MID, spaceAfter=4,
        )
        self.s_footer = ParagraphStyle(
            "footer", fontSize=7, fontName="Helvetica",
            textColor=C_INK_LIGHT, alignment=TA_CENTER,
        )

    # ── Reusable blocks ────────────────────────────────────────────────────────

    def _header(self, name: str, role_label: str, extra: str = "") -> Table:
        rows = [
            [Paragraph("hikma.", ParagraphStyle(
                "logo", fontSize=28, fontName="Helvetica-Bold",
                textColor=colors.white, alignment=TA_LEFT))],
            [Paragraph("Rapport de Performance Mensuel", self.s_title)],
            [Paragraph(
                f"<b>{name}</b>  ·  {role_label}  ·  {self.period_label}",
                self.s_sub,
            )],
        ]
        if extra:
            rows.append([Paragraph(extra, self.s_sub)])

        t = Table(rows, colWidths=[self.PAGE_W])
        t.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), C_RED),
            ("TOPPADDING",    (0, 0), (-1, -1), 10),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
            ("LEFTPADDING",   (0, 0), (-1, -1), 18),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 18),
        ]))
        return t

    def _kpi_cards(self, cards: list[dict]) -> Table:
        """cards = [{"label":str, "value":str, "color":Color}, ...]"""
        n   = len(cards)
        cw  = self.PAGE_W / n
        top = [Paragraph(c["label"], ParagraphStyle(
                   "kl", fontSize=7, fontName="Helvetica",
                   textColor=C_INK_LIGHT, alignment=TA_CENTER,
                   textTransform="uppercase", letterSpacing=0.5,
               )) for c in cards]
        bot = [Paragraph(c["value"], ParagraphStyle(
                   "kv", fontSize=18, fontName="Helvetica-Bold",
                   textColor=c.get("color", C_INK), alignment=TA_CENTER,
               )) for c in cards]
        t = Table([top, bot], colWidths=[cw] * n)
        t.setStyle(TableStyle([
            ("BOX",           (0, 0), (-1, -1), 0.5, C_BORDER),
            ("INNERGRID",     (0, 0), (-1, -1), 0.25, C_BORDER),
            ("BACKGROUND",    (0, 0), (-1, 0), C_SURFACE),
            ("BACKGROUND",    (0, 1), (-1, 1), colors.white),
            ("TOPPADDING",    (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ]))
        return t

    def _section(self, text: str) -> Paragraph:
        return Paragraph(text, self.s_section)

    def _footer(self, name: str) -> list:
        now = date.today().strftime("%d/%m/%Y")
        return [
            Spacer(1, 18),
            HRFlowable(width="100%", thickness=0.5, color=C_BORDER),
            Spacer(1, 5),
            Paragraph(
                f"Généré le {now}  ·  Hikma Pharmaceuticals Tunisia  ·  "
                f"Document confidentiel réservé à {name}",
                self.s_footer,
            ),
        ]

    def _std_table(
        self,
        data: list[list],
        col_widths: list[float],
        header_color: colors.Color = C_RED,
        taux_col: int | None = None,     # column index of taux value (for colouring)
        taux_data: list | None = None,   # raw float taux values aligned to data rows
    ) -> Table:
        t = Table(data, colWidths=col_widths)
        style = [
            ("BACKGROUND",    (0, 0), (-1, 0), header_color),
            ("TEXTCOLOR",     (0, 0), (-1, 0), colors.white),
            ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, -1), 8),
            ("ALIGN",         (1, 0), (-1, -1), "CENTER"),
            ("ALIGN",         (0, 0), (0, -1),  "LEFT"),
            ("GRID",          (0, 0), (-1, -1), 0.25, C_BORDER),
            ("TOPPADDING",    (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING",   (0, 0), (-1, -1), 6),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, C_SURFACE]),
        ]
        if taux_col is not None and taux_data:
            for i, taux in enumerate(taux_data, 1):
                style.append(("BACKGROUND", (taux_col, i), (taux_col, i), _perf_bg(taux)))
                style.append(("TEXTCOLOR",  (taux_col, i), (taux_col, i), _perf_color(taux)))
                style.append(("FONTNAME",   (taux_col, i), (taux_col, i), "Helvetica-Bold"))
        t.setStyle(TableStyle(style))
        return t


# ── MR Report ──────────────────────────────────────────────────────────────────

class MRReportBuilder(_BaseReportBuilder):

    def generate(self, user_id: int, mr_name: str, gsu: str) -> str:
        filename = f"report_mr_{user_id}_{self.period}.pdf"
        filepath = REPORTS_DIR / filename

        doc = SimpleDocTemplate(
            str(filepath), pagesize=A4,
            leftMargin=15*mm, rightMargin=15*mm,
            topMargin=10*mm,  bottomMargin=15*mm,
        )
        story: list = []

        # ── 1. Header ─────────────────────────────────────────────────────────
        gsu_txt = f"Zone principale : {gsu}" if gsu else ""
        story.append(self._header(mr_name, "Délégué Médical", gsu_txt))
        story.append(Spacer(1, 12))

        # ── 2. Global KPIs ────────────────────────────────────────────────────
        g = self.db.execute("""
            SELECT
                ROUND(SUM(actual_value),0)   AS av,
                ROUND(SUM(target_value),0)   AS tv,
                ROUND(SUM(actual_value)*100.0/NULLIF(SUM(target_value),0),1) AS tr,
                ROUND(SUM(actual_quantity),0) AS aq,
                ROUND(SUM(target_quantity),0) AS tq,
                COUNT(DISTINCT gsu)           AS nb_gsu
            FROM kpi_cache
            WHERE strftime('%Y-%m', date) = ? AND mr = ?
        """, (self.period, mr_name)).fetchone()

        av, tv, tr, aq, tq, nb_gsu = (
            g[0], g[1], g[2], g[3], g[4], g[5]
        ) if g else (None,)*6

        story.append(self._section("📊  Résumé de Performance"))
        story.append(self._kpi_cards([
            {"label": "CA Réalisé",      "value": _fmt_tnd(av),          "color": C_INK},
            {"label": "Objectif CA",     "value": _fmt_tnd(tv),          "color": C_INK_MID},
            {"label": "Taux Réal.",      "value": _fmt_pct(tr),          "color": _perf_color(tr)},
            {"label": "Unités Réelles",  "value": f"{int(aq):,}" if aq else "—", "color": C_INK},
            {"label": "GSUs Actives",    "value": str(nb_gsu or 0),      "color": C_VIOLET},
        ]))
        story.append(Spacer(1, 14))

        # ── 3. GSU breakdown ─────────────────────────────────────────────────
        gsu_rows = self.db.execute("""
            SELECT
                gsu,
                ROUND(SUM(actual_value),0)   AS av,
                ROUND(SUM(target_value),0)   AS tv,
                ROUND(SUM(actual_value)*100.0/NULLIF(SUM(target_value),0),1) AS tr,
                ROUND(SUM(actual_quantity),0) AS aq
            FROM kpi_cache
            WHERE strftime('%Y-%m', date) = ? AND mr = ?
            GROUP BY gsu
            ORDER BY tr ASC
        """, (self.period, mr_name)).fetchall()

        story.append(self._section("🗺️  Performance par GSU"))
        if gsu_rows:
            headers = ["GSU", "CA Réel (TND)", "Objectif (TND)", "Taux Val.", "Unités", "Statut"]
            data = [headers]
            taux_vals = []
            for r in gsu_rows:
                taux_vals.append(r[3])
                data.append([
                    r[0] or "—",
                    _fmt_tnd(r[1]),
                    _fmt_tnd(r[2]),
                    _fmt_pct(r[3]),
                    f"{int(r[4]):,}" if r[4] else "—",
                    _status_icon(r[3]),
                ])
            cw = [35*mm, 33*mm, 33*mm, 22*mm, 22*mm, 25*mm]
            story.append(self._std_table(data, cw, taux_col=3, taux_data=taux_vals))
        else:
            story.append(Paragraph("Aucune donnée disponible.", self.s_body))

        story.append(Spacer(1, 14))

        # ── 4. Attention zones ────────────────────────────────────────────────
        attn = [r for r in gsu_rows if r[3] is not None and r[3] < 80]
        if attn:
            story.append(self._section("⚠️  GSUs Nécessitant une Attention Particulière"))
            data2 = [["GSU", "Taux Réalisation", "Écart vs Objectif (TND)"]]
            for r in attn:
                gap = (r[1] or 0) - (r[2] or 0)
                data2.append([r[0] or "—", _fmt_pct(r[3]),
                               f"{gap:,.0f}".replace(",", "\u202f")])
            t2 = Table(data2, colWidths=[70*mm, 50*mm, 50*mm])
            t2.setStyle(TableStyle([
                ("BACKGROUND",    (0, 0), (-1, 0), C_AMBER),
                ("TEXTCOLOR",     (0, 0), (-1, 0), colors.white),
                ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE",      (0, 0), (-1, -1), 9),
                ("GRID",          (0, 0), (-1, -1), 0.25, C_BORDER),
                ("BACKGROUND",    (0, 1), (-1, -1), C_AMB_LIGHT),
                ("TEXTCOLOR",     (0, 1), (-1, -1), C_AMBER),
                ("FONTNAME",      (0, 1), (-1, -1), "Helvetica-Bold"),
                ("ALIGN",         (1, 0), (-1, -1), "CENTER"),
                ("TOPPADDING",    (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ("LEFTPADDING",   (0, 0), (-1, -1), 8),
            ]))
            story.append(t2)
            story.append(Spacer(1, 14))

        # ── 5. Product breakdown ──────────────────────────────────────────────
        prod_rows = self.db.execute("""
            SELECT
                product,
                ROUND(SUM(actual_value),0) AS av,
                ROUND(SUM(target_value),0) AS tv,
                ROUND(SUM(actual_value)*100.0/NULLIF(SUM(target_value),0),1) AS tr
            FROM kpi_cache
            WHERE strftime('%Y-%m', date) = ? AND mr = ?
            GROUP BY product
            ORDER BY av DESC
            LIMIT 12
        """, (self.period, mr_name)).fetchall()

        if prod_rows:
            story.append(self._section("💊  Performance par Produit"))
            headers3 = ["Produit", "CA Réel (TND)", "Objectif (TND)", "Taux"]
            data3 = [headers3] + [
                [r[0] or "—", _fmt_tnd(r[1]), _fmt_tnd(r[2]), _fmt_pct(r[3])]
                for r in prod_rows
            ]
            cw3 = [70*mm, 40*mm, 40*mm, 20*mm]
            story.append(self._std_table(
                data3, cw3, header_color=C_TEAL,
                taux_col=3, taux_data=[r[3] for r in prod_rows]
            ))
            story.append(Spacer(1, 14))

        # ── 6. 6-month trend ─────────────────────────────────────────────────
        trend = self.db.execute("""
            SELECT
                strftime('%Y-%m', date) AS mo,
                ROUND(SUM(actual_value),0) AS av,
                ROUND(SUM(target_value),0) AS tv,
                ROUND(SUM(actual_value)*100.0/NULLIF(SUM(target_value),0),1) AS tr
            FROM kpi_cache
            WHERE mr = ?
              AND date >= date(? || '-01', '-5 months')
            GROUP BY mo
            ORDER BY mo
        """, (mr_name, self.period)).fetchall()

        if trend:
            story.append(self._section("📈  Évolution sur 6 Mois"))
            headers4 = ["Période", "CA Réel (TND)", "Objectif (TND)", "Taux"]
            data4 = [headers4] + [
                [_period_short(r[0]), _fmt_tnd(r[1]), _fmt_tnd(r[2]), _fmt_pct(r[3])]
                for r in trend
            ]
            t4 = Table(data4, colWidths=[40*mm, 44*mm, 44*mm, 42*mm])
            style4 = [
                ("BACKGROUND",    (0, 0), (-1, 0), C_BLUE),
                ("TEXTCOLOR",     (0, 0), (-1, 0), colors.white),
                ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE",      (0, 0), (-1, -1), 8),
                ("GRID",          (0, 0), (-1, -1), 0.25, C_BORDER),
                ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, C_SURFACE]),
                ("ALIGN",         (1, 0), (-1, -1), "CENTER"),
                ("TOPPADDING",    (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING",   (0, 0), (-1, -1), 6),
            ]
            for i, r in enumerate(trend, 1):
                if r[0] == self.period:           # highlight current month
                    style4 += [
                        ("BACKGROUND", (0, i), (-1, i), colors.HexColor("#EFF6FF")),
                        ("FONTNAME",   (0, i), (-1, i), "Helvetica-Bold"),
                    ]
                style4 += [
                    ("BACKGROUND", (3, i), (3, i), _perf_bg(r[3])),
                    ("TEXTCOLOR",  (3, i), (3, i), _perf_color(r[3])),
                    ("FONTNAME",   (3, i), (3, i), "Helvetica-Bold"),
                ]
            t4.setStyle(TableStyle(style4))
            story.append(t4)

        story += self._footer(mr_name)
        doc.build(story)
        logger.info("✅ MR report generated: %s", filepath)
        return str(filepath)


# ── SV Report ──────────────────────────────────────────────────────────────────

class SVReportBuilder(_BaseReportBuilder):

    def generate(self, user_id: int, sv_name: str) -> str:
        filename = f"report_sv_{user_id}_{self.period}.pdf"
        filepath = REPORTS_DIR / filename

        doc = SimpleDocTemplate(
            str(filepath), pagesize=A4,
            leftMargin=15*mm, rightMargin=15*mm,
            topMargin=10*mm,  bottomMargin=15*mm,
        )
        story: list = []

        # ── 1. Header ─────────────────────────────────────────────────────────
        story.append(self._header(sv_name, "Superviseur"))
        story.append(Spacer(1, 12))

        # ── 2. Team KPIs ──────────────────────────────────────────────────────
        g = self.db.execute("""
            SELECT
                ROUND(SUM(actual_value),0)   AS av,
                ROUND(SUM(target_value),0)   AS tv,
                ROUND(SUM(actual_value)*100.0/NULLIF(SUM(target_value),0),1) AS tr,
                COUNT(DISTINCT mr)            AS nb_mr,
                COUNT(DISTINCT gsu)           AS nb_gsu
            FROM kpi_cache
            WHERE strftime('%Y-%m', date) = ? AND sv = ?
        """, (self.period, sv_name)).fetchone()

        av, tv, tr, nb_mr, nb_gsu = (g[0],g[1],g[2],g[3],g[4]) if g else (None,)*5

        story.append(self._section("📊  Résumé de l'Équipe"))
        story.append(self._kpi_cards([
            {"label": "CA Réalisé Équipe", "value": _fmt_tnd(av),        "color": C_INK},
            {"label": "Objectif Équipe",   "value": _fmt_tnd(tv),        "color": C_INK_MID},
            {"label": "Taux Global",       "value": _fmt_pct(tr),        "color": _perf_color(tr)},
            {"label": "Délégués Actifs",   "value": str(nb_mr or 0),     "color": C_VIOLET},
            {"label": "GSUs Couvertes",    "value": str(nb_gsu or 0),    "color": C_TEAL},
        ]))
        story.append(Spacer(1, 14))

        # ── 3. MR ranking ─────────────────────────────────────────────────────
        mr_rows = self.db.execute("""
            SELECT
                mr,
                ROUND(SUM(actual_value),0)   AS av,
                ROUND(SUM(target_value),0)   AS tv,
                ROUND(SUM(actual_value)*100.0/NULLIF(SUM(target_value),0),1) AS tr,
                ROUND(SUM(actual_quantity),0) AS aq
            FROM kpi_cache
            WHERE strftime('%Y-%m', date) = ? AND sv = ?
            GROUP BY mr
            ORDER BY tr DESC
        """, (self.period, sv_name)).fetchall()

        story.append(self._section("👥  Classement des Délégués Médicaux"))
        if mr_rows:
            MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}
            headers = ["#", "Délégué", "CA Réel (TND)", "Objectif (TND)",
                       "Taux", "Unités", "Statut"]
            data = [headers]
            taux_vals = []
            for i, r in enumerate(mr_rows, 1):
                taux_vals.append(r[3])
                data.append([
                    MEDALS.get(i, str(i)),
                    r[0] or "—",
                    _fmt_tnd(r[1]),
                    _fmt_tnd(r[2]),
                    _fmt_pct(r[3]),
                    f"{int(r[4]):,}" if r[4] else "—",
                    _status_icon(r[3]),
                ])
            cw = [12*mm, 38*mm, 32*mm, 32*mm, 20*mm, 18*mm, 18*mm]
            t = Table(data, colWidths=cw)
            style = [
                ("BACKGROUND",    (0, 0), (-1, 0), C_RED),
                ("TEXTCOLOR",     (0, 0), (-1, 0), colors.white),
                ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE",      (0, 0), (-1, -1), 8),
                ("GRID",          (0, 0), (-1, -1), 0.25, C_BORDER),
                ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, C_SURFACE]),
                ("ALIGN",         (0, 0), (0, -1),  "CENTER"),
                ("ALIGN",         (2, 0), (-1, -1), "CENTER"),
                ("TOPPADDING",    (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING",   (0, 0), (-1, -1), 5),
            ]
            # Gold highlight for #1
            if mr_rows:
                style += [
                    ("BACKGROUND", (0, 1), (-1, 1), colors.HexColor("#FFFBEB")),
                    ("FONTNAME",   (0, 1), (-1, 1), "Helvetica-Bold"),
                ]
            # Taux column colouring
            for i, taux in enumerate(taux_vals, 1):
                style += [
                    ("BACKGROUND", (4, i), (4, i), _perf_bg(taux)),
                    ("TEXTCOLOR",  (4, i), (4, i), _perf_color(taux)),
                    ("FONTNAME",   (4, i), (4, i), "Helvetica-Bold"),
                ]
            t.setStyle(TableStyle(style))
            story.append(t)
        else:
            story.append(Paragraph("Aucune donnée disponible.", self.s_body))

        story.append(Spacer(1, 14))

        # ── 4. Underperforming MRs ────────────────────────────────────────────
        underperf = [r for r in mr_rows if r[3] is not None and r[3] < 80]
        if underperf:
            story.append(self._section("⚠️  Délégués Nécessitant un Suivi Renforcé"))
            data2 = [["Délégué", "Taux Réalisation", "Écart vs Objectif (TND)"]]
            for r in underperf:
                gap = (r[1] or 0) - (r[2] or 0)
                data2.append([r[0] or "—", _fmt_pct(r[3]),
                               f"{gap:,.0f}".replace(",", "\u202f")])
            t2 = Table(data2, colWidths=[70*mm, 50*mm, 50*mm])
            t2.setStyle(TableStyle([
                ("BACKGROUND",    (0, 0), (-1, 0), C_AMBER),
                ("TEXTCOLOR",     (0, 0), (-1, 0), colors.white),
                ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE",      (0, 0), (-1, -1), 9),
                ("GRID",          (0, 0), (-1, -1), 0.25, C_BORDER),
                ("BACKGROUND",    (0, 1), (-1, -1), C_AMB_LIGHT),
                ("TEXTCOLOR",     (0, 1), (-1, -1), C_AMBER),
                ("FONTNAME",      (0, 1), (-1, -1), "Helvetica-Bold"),
                ("ALIGN",         (1, 0), (-1, -1), "CENTER"),
                ("TOPPADDING",    (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ("LEFTPADDING",   (0, 0), (-1, -1), 8),
            ]))
            story.append(t2)
            story.append(Spacer(1, 14))

        # ── 5. Performance by gouvernorat ─────────────────────────────────────
        gov_rows = self.db.execute("""
            SELECT
                gouvernorat,
                ROUND(SUM(actual_value),0)  AS av,
                ROUND(SUM(target_value),0)  AS tv,
                ROUND(SUM(actual_value)*100.0/NULLIF(SUM(target_value),0),1) AS tr
            FROM kpi_cache
            WHERE strftime('%Y-%m', date) = ? AND sv = ?
              AND gouvernorat IS NOT NULL
            GROUP BY gouvernorat
            ORDER BY tr DESC
        """, (self.period, sv_name)).fetchall()

        if gov_rows:
            story.append(self._section("🗺️  Performance par Gouvernorat"))
            headers5 = ["Gouvernorat", "CA Réel (TND)", "Objectif (TND)", "Taux"]
            data5 = [headers5] + [
                [r[0] or "—", _fmt_tnd(r[1]), _fmt_tnd(r[2]), _fmt_pct(r[3])]
                for r in gov_rows
            ]
            story.append(self._std_table(
                data5,
                [50*mm, 42*mm, 42*mm, 36*mm],
                header_color=C_TEAL,
                taux_col=3,
                taux_data=[r[3] for r in gov_rows],
            ))
            story.append(Spacer(1, 14))

        # ── 6. Team trend (6 months) ──────────────────────────────────────────
        trend = self.db.execute("""
            SELECT
                strftime('%Y-%m', date) AS mo,
                ROUND(SUM(actual_value),0) AS av,
                ROUND(SUM(target_value),0) AS tv,
                ROUND(SUM(actual_value)*100.0/NULLIF(SUM(target_value),0),1) AS tr,
                COUNT(DISTINCT mr) AS nb_mr
            FROM kpi_cache
            WHERE sv = ?
              AND date >= date(? || '-01', '-5 months')
            GROUP BY mo
            ORDER BY mo
        """, (sv_name, self.period)).fetchall()

        if trend:
            story.append(self._section("📈  Évolution de l'Équipe sur 6 Mois"))
            headers6 = ["Période", "CA Réel (TND)", "Objectif (TND)", "Taux", "Délégués"]
            data6 = [headers6] + [
                [_period_short(r[0]), _fmt_tnd(r[1]), _fmt_tnd(r[2]),
                 _fmt_pct(r[3]), str(r[4] or 0)]
                for r in trend
            ]
            t6 = Table(data6, colWidths=[36*mm, 40*mm, 40*mm, 34*mm, 20*mm])
            style6 = [
                ("BACKGROUND",    (0, 0), (-1, 0), C_BLUE),
                ("TEXTCOLOR",     (0, 0), (-1, 0), colors.white),
                ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE",      (0, 0), (-1, -1), 8),
                ("GRID",          (0, 0), (-1, -1), 0.25, C_BORDER),
                ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, C_SURFACE]),
                ("ALIGN",         (1, 0), (-1, -1), "CENTER"),
                ("TOPPADDING",    (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING",   (0, 0), (-1, -1), 6),
            ]
            for i, r in enumerate(trend, 1):
                if r[0] == self.period:
                    style6 += [
                        ("BACKGROUND", (0, i), (-1, i), colors.HexColor("#EFF6FF")),
                        ("FONTNAME",   (0, i), (-1, i), "Helvetica-Bold"),
                    ]
                style6 += [
                    ("BACKGROUND", (3, i), (3, i), _perf_bg(r[3])),
                    ("TEXTCOLOR",  (3, i), (3, i), _perf_color(r[3])),
                    ("FONTNAME",   (3, i), (3, i), "Helvetica-Bold"),
                ]
            t6.setStyle(TableStyle(style6))
            story.append(t6)

        story += self._footer(sv_name)
        doc.build(story)
        logger.info("✅ SV report generated: %s", filepath)
        return str(filepath)


# ── Public API ─────────────────────────────────────────────────────────────────

def generate_report_for_user(
    user_id: int,
    period: str | None = None,
    db_conn=None,
) -> Optional[str]:
    """
    Generate the appropriate PDF report for a user.

    Args:
        user_id: DB user id
        period:  'YYYY-MM' string; defaults to previous calendar month
        db_conn: optional existing DB connection (caller must close it)

    Returns:
        Absolute path to generated PDF, or None if no data / error.
    """
    if period is None:
        today  = date.today()
        first  = today.replace(day=1)
        prev   = first - timedelta(days=1)
        period = prev.strftime("%Y-%m")

    # Use provided connection or open one
    close_after = db_conn is None
    if close_after:
        from app.database import get_db
        db_conn = get_db()

    try:
        user = db_conn.execute(
            "SELECT id, first_name, last_name, role, gsu FROM users "
            "WHERE id = ? AND is_active = 1",
            (user_id,),
        ).fetchone()

        if not user:
            logger.warning("generate_report: user %d not found or inactive", user_id)
            return None

        full_name = f"{user['first_name']} {user['last_name']}"
        role      = user["role"]
        gsu       = user["gsu"] or ""

        if role == "delegue_medical":
            builder = MRReportBuilder(period, db_conn)
            return builder.generate(user_id, full_name, gsu)

        if role == "superviseur":
            builder = SVReportBuilder(period, db_conn)
            return builder.generate(user_id, full_name)

        logger.info("generate_report: role '%s' not supported for reports", role)
        return None

    except Exception as exc:
        logger.error("Report generation failed user=%d period=%s: %s", user_id, period, exc)
        return None

    finally:
        if close_after:
            db_conn.close()