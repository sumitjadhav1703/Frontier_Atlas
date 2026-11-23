"""Render docs/architecture-source.md to architecture.pdf (≤3 pages) with reportlab."""

from __future__ import annotations

import hashlib
import io
import re
import textwrap
from pathlib import Path
from typing import Final
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    ListFlowable,
    ListItem,
    PageBreak,
    Paragraph,
    Preformatted,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

ARCHITECTURE_PDF_PATH: Final = Path("architecture.pdf")
ARCHITECTURE_SOURCE_PATH: Final = Path("docs/architecture-source.md")
MAX_PAGES: Final = 3

# Built-in Type1 fonts only cover WinAnsi; map the symbols the source uses.
_SYMBOLS: Final = str.maketrans(
    {"→": "->", "←": "<-", "⇄": "<->", "≤": "<=", "≥": ">=", "−": "-", "≈": "~",
     "⊆": "within", "θ": "theta", "≠": "!="}
)

_BASE = getSampleStyleSheet()
_BODY = ParagraphStyle("body", parent=_BASE["BodyText"], fontName="Helvetica",
                       fontSize=9, leading=11.2, spaceAfter=3, alignment=TA_LEFT)
_CELL = ParagraphStyle("cell", parent=_BODY, fontSize=8, leading=9.6, spaceAfter=0)
_STYLES: Final = {
    1: ParagraphStyle("h1", parent=_BODY, fontName="Helvetica-Bold", fontSize=14,
                      leading=17, spaceAfter=4),
    2: ParagraphStyle("h2", parent=_BODY, fontName="Helvetica-Bold", fontSize=12,
                      leading=15, spaceBefore=2, spaceAfter=4,
                      textColor=colors.HexColor("#1f3a5f")),
    3: ParagraphStyle("h3", parent=_BODY, fontName="Helvetica-Bold", fontSize=10,
                      leading=12.5, spaceBefore=5, spaceAfter=2),
}
_CODE = ParagraphStyle("code", parent=_BODY, fontName="Courier", fontSize=8, leading=9.6,
                       backColor=colors.HexColor("#f3f3f3"), borderPadding=3,
                       spaceBefore=2, spaceAfter=5)


def _inline(text: str) -> str:
    """Markdown inline (`code`, **bold**) to reportlab mini-markup; escapes the rest."""
    out = []
    for i, part in enumerate(re.split(r"(`[^`]*`)", text.translate(_SYMBOLS))):
        if i % 2:
            out.append(f'<font face="Courier">{escape(part[1:-1])}</font>')
        else:
            out.append(re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", escape(part)))
    return "".join(out)


def _table(lines: list[str], width: float) -> Table:
    rows = [[c.strip() for c in ln.strip().strip("|").split("|")] for ln in lines]
    rows = [r for r in rows if not all(re.fullmatch(r":?-{3,}:?", c) for c in r)]
    ncols = max(len(r) for r in rows)
    data = [
        [Paragraph(("<b>%s</b>" if ri == 0 else "%s") % _inline(c), _CELL) for c in r]
        + [""] * (ncols - len(r))
        for ri, r in enumerate(rows)
    ]
    t = Table(data, colWidths=[width / ncols] * ncols, repeatRows=1, hAlign="LEFT")
    t.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e6ecf3")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    return t


def markdown_flowables(source: str, width: float) -> list:
    """Parse the small Markdown subset used by architecture-source.md."""
    lines = source.splitlines()
    flow: list = []
    seen_page = False
    i = 0

    def take(pred) -> list[str]:
        nonlocal i
        block = []
        while i < len(lines) and pred(lines[i]):
            block.append(lines[i])
            i += 1
        return block

    while i < len(lines):
        line = lines[i]
        s = line.strip()
        if not s:
            i += 1
        elif s.startswith("```"):
            i += 1
            code = take(lambda ln: not ln.strip().startswith("```"))
            i += 1
            flow.append(Preformatted("\n".join(code).translate(_SYMBOLS), _CODE))
        elif s.startswith("\\["):
            i += 1
            math = take(lambda ln: not ln.strip().startswith("\\]"))
            i += 1
            flow.append(Preformatted(textwrap.dedent("\n".join(math)).strip("\n").translate(_SYMBOLS),
                                     _CODE))
        elif m := re.match(r"(#{1,3})\s+(.*)", s):
            level = len(m.group(1))
            if level == 2 and m.group(2).startswith("Page"):
                if seen_page:
                    flow.append(PageBreak())
                seen_page = True
            flow.append(Paragraph(_inline(m.group(2)), _STYLES[level]))
            i += 1
        elif s.startswith("|"):
            flow.append(_table(take(lambda ln: ln.strip().startswith("|")), width))
            flow.append(Spacer(1, 4))
        elif re.match(r"[-*]\s+", s):
            items = [re.sub(r"^[-*]\s+", "", b.strip())
                     for b in take(lambda ln: re.match(r"\s*[-*]\s+", ln) is not None)]
            flow.append(ListFlowable(
                [ListItem(Paragraph(_inline(t), _BODY), leftIndent=10) for t in items],
                bulletType="bullet", start="•", leftIndent=10, bulletFontSize=7,
            ))
        else:
            para = take(lambda ln: ln.strip() != "" and not re.match(
                r"\s*(#{1,3}\s|```|\||[-*]\s|\\\[)", ln))
            flow.append(Paragraph(_inline(" ".join(p.strip() for p in para)), _BODY))
    return flow


def count_pdf_pages(path_or_bytes: Path | str | bytes) -> int:
    """Count page objects (/Type /Page, not /Pages)."""
    data = path_or_bytes if isinstance(path_or_bytes, bytes) else Path(path_or_bytes).read_bytes()
    return len(re.findall(rb"/Type\s*/Page(?!\s*s)", data))


def render_architecture_pdf(
    path: Path | str = ARCHITECTURE_PDF_PATH,
    source: Path | str = ARCHITECTURE_SOURCE_PATH,
) -> Path:
    """Render the Markdown source; refuse to write a PDF over MAX_PAGES pages."""
    out = Path(path)
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4, leftMargin=15 * mm, rightMargin=15 * mm,
        topMargin=13 * mm, bottomMargin=13 * mm,
        title="AI Signal ingestion architecture", author="signal-ingest", invariant=True,
    )
    doc.build(markdown_flowables(Path(source).read_text(encoding="utf-8"), doc.width))
    data = buf.getvalue()
    pages = count_pdf_pages(data)
    if pages > MAX_PAGES:
        raise ValueError(f"architecture.pdf would be {pages} pages (> {MAX_PAGES}); trim the source")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(data)
    return out


def sha256_file(path: Path | str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


__all__ = [
    "ARCHITECTURE_PDF_PATH",
    "ARCHITECTURE_SOURCE_PATH",
    "MAX_PAGES",
    "count_pdf_pages",
    "markdown_flowables",
    "render_architecture_pdf",
    "sha256_file",
]
