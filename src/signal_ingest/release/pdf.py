"""Render ≤3-page architecture.pdf from page text (no external PDF dependency)."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Final, Sequence

ARCHITECTURE_PDF_PATH: Final = Path("architecture.pdf")
ARCHITECTURE_SOURCE_PATH: Final = Path("docs/architecture-source.md")

# Helvetica ASCII-safe lines; keep short for legibility on Letter page.
PAGE_TEXTS: Final[tuple[tuple[str, ...], ...]] = (
    (
        "AI Signal architecture.pdf (<=3 pages)",
        "Page 1 · Pipeline topology and 500k projection",
        "",
        "Sources -> discover -> PG queue/leases -> HTTPX pool",
        "-> shared raw store (content_key) -> parse/enrich",
        "-> grounded LLM extract -> resolve -> six-tab export.",
        "Bulk: YC, products, papers, arXiv, GitHub. Monitors: 5+5.",
        "Dual workers: same Postgres + mounted raw root.",
        "",
        "500k projection (formula; coefficients PENDING live):",
        "T >= max(stage_work/theta_eff, rate limits, token time).",
        "Storage ~= compressed_bytes/item x candidates + indexes.",
        "",
        "Synthetic 1k/10k: artifacts/gates/b7_benchmark.json",
        "(labeled synthetic — NOT live scrape throughput).",
        "LIVE bytes/item, yield, RPS: PENDING (B8/B9).",
    ),
    (
        "Page 2 · Byte/token budgets, 413/429, circuits",
        "",
        "Allowable input = context - reserved output - framing",
        "- safety margin; also provider input + transport caps.",
        "Split section->paragraph; recount on each fallback.",
        "Fallback: Gemini -> Groq -> optional Ollama.",
        "429: honor Retry-After; never shorten server cooldown.",
        "413 / context 400: re-budget + smaller split (bounded).",
        "Quota exhausted: deny dispatch; abstain or queue.",
        "Paid spend cap = 0 offline; fake != live qualification.",
        "",
        "Offline fake traces: B4/B5 gates.",
        "LIVE redacted provider trace: PENDING B8 unlock.",
        "Measured live token/latency numbers: PENDING.",
    ),
    (
        "Page 3 · Identity, freshness, storage trade-offs",
        "",
        "Semantic task keys, fencing tokens, idempotent upserts.",
        "Freshness: publication interval subset of [T-24h, T].",
        "Postgres for identity, metrics history, leases, edges.",
        "Optional vector/graph stores deferred (sync cost).",
        "Fixture exports segregated + not_for_live_export.",
        "",
        "Trial results: offline B0-B7 PASS; B8 BLOCKED",
        "(no free LLM keys/Ollama; no live >=1000 export).",
        "KNOWN GAPS / PENDING: live yield, canary lineage,",
        "public Sheet read-back, signed-out viewer checks.",
        "Do not invent measurements. Source: architecture-source.md",
    ),
)


def _escape_pdf_string(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _page_content_stream(lines: Sequence[str]) -> bytes:
    # Text object with leading; each line advances via T* after first Td.
    parts = ["BT", "/F1 11 Tf", "14 TL", "50 750 Td"]
    for i, line in enumerate(lines):
        esc = _escape_pdf_string(line)
        if i == 0:
            parts.append(f"({esc}) Tj")
        else:
            parts.append("T*")
            parts.append(f"({esc}) Tj")
    parts.append("ET")
    return "\n".join(parts).encode("latin-1", errors="replace")


def render_architecture_pdf(
    path: Path | str = ARCHITECTURE_PDF_PATH,
    pages: Sequence[Sequence[str]] = PAGE_TEXTS,
) -> Path:
    """Write a 3-page PDF stub with PENDING live measurement markers."""
    out = Path(path)
    if not out.is_absolute():
        out = Path.cwd() / out
    if len(pages) > 3:
        raise ValueError("architecture.pdf must be ≤3 pages")
    if len(pages) < 1:
        raise ValueError("at least one page required")

    streams = [_page_content_stream(p) for p in pages]
    # Object layout: 1 Catalog, 2 Pages, 3..N Page, then contents, then font.
    n_pages = len(pages)
    page_obj_ids = list(range(3, 3 + n_pages))
    content_obj_ids = list(range(3 + n_pages, 3 + 2 * n_pages))
    font_obj_id = 3 + 2 * n_pages

    objects: dict[int, bytes] = {}
    objects[1] = b"<< /Type /Catalog /Pages 2 0 R >>"
    kids = " ".join(f"{oid} 0 R" for oid in page_obj_ids)
    objects[2] = (
        f"<< /Type /Pages /Kids [{kids}] /Count {n_pages} >>".encode("ascii")
    )
    for page_id, content_id in zip(page_obj_ids, content_obj_ids, strict=True):
        objects[page_id] = (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            f"/Contents {content_id} 0 R "
            f"/Resources << /Font << /F1 {font_obj_id} 0 R >> >> >>"
        ).encode("ascii")
    for content_id, stream in zip(content_obj_ids, streams, strict=True):
        objects[content_id] = (
            f"<< /Length {len(stream)} >>\nstream\n".encode("ascii")
            + stream
            + b"\nendstream"
        )
    objects[font_obj_id] = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"

    # Assemble with xref.
    max_id = font_obj_id
    pieces: list[bytes] = [b"%PDF-1.4\n"]
    offsets: dict[int, int] = {}
    for oid in range(1, max_id + 1):
        offsets[oid] = sum(len(p) for p in pieces)
        pieces.append(f"{oid} 0 obj\n".encode("ascii"))
        pieces.append(objects[oid])
        pieces.append(b"\nendobj\n")
    xref_pos = sum(len(p) for p in pieces)
    pieces.append(f"xref\n0 {max_id + 1}\n".encode("ascii"))
    pieces.append(b"0000000000 65535 f \n")
    for oid in range(1, max_id + 1):
        pieces.append(f"{offsets[oid]:010d} 00000 n \n".encode("ascii"))
    pieces.append(
        f"trailer\n<< /Size {max_id + 1} /Root 1 0 R >>\n"
        f"startxref\n{xref_pos}\n%%EOF\n".encode("ascii")
    )
    out.write_bytes(b"".join(pieces))
    return out


def sha256_file(path: Path | str) -> str:
    data = Path(path).read_bytes()
    return hashlib.sha256(data).hexdigest()


def count_pdf_pages(path: Path | str) -> int:
    """Count /Type /Page entries (not /Pages) in a simple PDF."""
    text = Path(path).read_bytes()
    # Match dictionary entries that declare a Page (not Pages tree).
    count = 0
    needle = b"/Type /Page"
    start = 0
    while True:
        idx = text.find(needle, start)
        if idx < 0:
            break
        # Exclude /Type /Pages
        after = text[idx + len(needle) : idx + len(needle) + 1]
        if after != b"s":
            count += 1
        start = idx + len(needle)
    return count
