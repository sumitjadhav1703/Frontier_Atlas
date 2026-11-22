"""Semantic chunk planner with overlap, priority, and 413 re-budget."""

from __future__ import annotations

from dataclasses import dataclass, field

from signal_ingest.llm.budget import ComputedBudget, content_token_budget, shrink_content_budget
from signal_ingest.llm.tokens import estimate_tokens
from signal_ingest.llm.types import Segment, SegmentKind

# Prefer these kinds when packing under a tight budget.
_KIND_RANK = {
    SegmentKind.TITLE: 0,
    SegmentKind.DATE: 1,
    SegmentKind.PRICING: 2,
    SegmentKind.OWNER: 3,
    SegmentKind.CODE: 4,
    SegmentKind.JSON_LD: 5,
    SegmentKind.META: 6,
    SegmentKind.BODY: 7,
    SegmentKind.OTHER: 8,
}

DEFAULT_MAX_DEPTH = 4
DEFAULT_MIN_CHUNK_CHARS = 48
DEFAULT_OVERLAP_CHARS = 80


@dataclass
class Chunk:
    chunk_id: str
    segments: list[Segment]
    text: str
    token_estimate: int
    discarded_ids: list[str] = field(default_factory=list)
    depth: int = 0


@dataclass
class ChunkPlan:
    chunks: list[Chunk]
    budget: ComputedBudget
    overlap_chars: int
    min_chunk_chars: int
    max_depth: int
    discarded_coverage: list[str] = field(default_factory=list)
    depth: int = 0


def plan_chunks(
    segments: list[Segment],
    budget: ComputedBudget,
    *,
    overlap_chars: int = DEFAULT_OVERLAP_CHARS,
    min_chunk_chars: int = DEFAULT_MIN_CHUNK_CHARS,
    max_depth: int = DEFAULT_MAX_DEPTH,
    depth: int = 0,
) -> ChunkPlan:
    """Split by semantic priority then paragraphs; preserve offsets via segment ids."""
    if budget.route_disabled or content_token_budget(budget) <= 0:
        return ChunkPlan(
            chunks=[],
            budget=budget,
            overlap_chars=overlap_chars,
            min_chunk_chars=min_chunk_chars,
            max_depth=max_depth,
            discarded_coverage=[s.segment_id for s in segments],
            depth=depth,
        )

    ordered = sorted(
        segments,
        key=lambda s: (_KIND_RANK.get(s.kind, 99), -s.priority, s.start_offset),
    )
    # Expand oversized individual blocks on safe boundaries before packing.
    expanded: list[Segment] = []
    for seg in ordered:
        expanded.extend(_split_oversized_segment(seg, budget, min_chunk_chars=min_chunk_chars))

    max_content = content_token_budget(budget)
    chunks: list[Chunk] = []
    discarded: list[str] = []
    current: list[Segment] = []
    current_text_parts: list[str] = []

    def _flush() -> None:
        nonlocal current, current_text_parts
        if not current:
            return
        text = "\n".join(current_text_parts)
        chunks.append(
            Chunk(
                chunk_id=f"chunk:{len(chunks)+1:03d}:d{depth}",
                segments=list(current),
                text=text,
                token_estimate=estimate_tokens(text),
                depth=depth,
            )
        )
        # Overlap: keep tail of previous chunk as prefix of next.
        if overlap_chars > 0 and current:
            overlap_segs = _overlap_tail(current, overlap_chars)
            current = overlap_segs
            current_text_parts = [s.text for s in overlap_segs]
        else:
            current = []
            current_text_parts = []

    for seg in expanded:
        candidate_parts = current_text_parts + [seg.text]
        candidate_text = "\n".join(candidate_parts)
        if estimate_tokens(candidate_text) <= max_content:
            current.append(seg)
            current_text_parts.append(seg.text)
            continue
        # Does not fit with current — flush and try alone.
        _flush()
        alone = estimate_tokens(seg.text)
        if alone <= max_content:
            current = [seg]
            current_text_parts = [seg.text]
        else:
            # Still too big after split — discard with record (should be rare).
            discarded.append(seg.segment_id)

    _flush()
    # Remove pure-overlap empty duplicates: ensure every chunk has unique primary content.
    return ChunkPlan(
        chunks=chunks,
        budget=budget,
        overlap_chars=overlap_chars,
        min_chunk_chars=min_chunk_chars,
        max_depth=max_depth,
        discarded_coverage=discarded,
        depth=depth,
    )


def rebudget_on_overflow(
    plan: ChunkPlan,
    *,
    factor: float = 0.5,
) -> ChunkPlan:
    """After 413/context-400: shrink budget and re-split (strict max depth)."""
    if plan.depth + 1 >= plan.max_depth:
        raise ValueError(f"max re-budget depth {plan.max_depth} exceeded")
    new_budget = shrink_content_budget(plan.budget, factor=factor)
    # Flatten segments from prior plan (unique by id, preserve priority order).
    seen: set[str] = set()
    segs: list[Segment] = []
    for ch in plan.chunks:
        for s in ch.segments:
            if s.segment_id not in seen:
                seen.add(s.segment_id)
                segs.append(s)
    return plan_chunks(
        segs,
        new_budget,
        overlap_chars=plan.overlap_chars,
        min_chunk_chars=plan.min_chunk_chars,
        max_depth=plan.max_depth,
        depth=plan.depth + 1,
    )


def _split_oversized_segment(
    seg: Segment,
    budget: ComputedBudget,
    *,
    min_chunk_chars: int,
) -> list[Segment]:
    max_tok = content_token_budget(budget)
    if estimate_tokens(seg.text) <= max_tok:
        return [seg]
    # Split on paragraph then sentence then hard character windows.
    pieces = _safe_split(seg.text, max_tok, min_chunk_chars=min_chunk_chars)
    out: list[Segment] = []
    for i, piece in enumerate(pieces, start=1):
        out.append(
            Segment(
                segment_id=f"{seg.segment_id}#p{i}",
                kind=seg.kind,
                text=piece,
                start_offset=seg.start_offset,
                end_offset=seg.end_offset,
                priority=seg.priority,
                entity_hints=seg.entity_hints,
            )
        )
    return out or [seg]


def _safe_split(text: str, max_tokens: int, *, min_chunk_chars: int) -> list[str]:
    if estimate_tokens(text) <= max_tokens:
        return [text]
    # Paragraphs first.
    paras = [p for p in text.split("\n") if p.strip()]
    if len(paras) > 1:
        return _pack_pieces(paras, max_tokens, min_chunk_chars=min_chunk_chars)
    # Sentences.
    sentences = _split_sentences(text)
    if len(sentences) > 1:
        return _pack_pieces(sentences, max_tokens, min_chunk_chars=min_chunk_chars)
    # Hard windows on whitespace when possible.
    return _hard_windows(text, max_tokens, min_chunk_chars=min_chunk_chars)


def _pack_pieces(pieces: list[str], max_tokens: int, *, min_chunk_chars: int) -> list[str]:
    out: list[str] = []
    buf: list[str] = []
    for part in pieces:
        if estimate_tokens(part) > max_tokens:
            if buf:
                out.append(" ".join(buf))
                buf = []
            out.extend(_hard_windows(part, max_tokens, min_chunk_chars=min_chunk_chars))
            continue
        trial = (" ".join(buf + [part])).strip()
        if buf and estimate_tokens(trial) > max_tokens:
            out.append(" ".join(buf))
            buf = [part]
        else:
            buf.append(part)
    if buf:
        out.append(" ".join(buf))
    return [x for x in out if len(x) >= min_chunk_chars or estimate_tokens(x) <= max_tokens]


def _hard_windows(text: str, max_tokens: int, *, min_chunk_chars: int) -> list[str]:
    # Approximate chars per token window from estimator (4 bytes ≈ 1 token); use chars.
    # Keep at least min_chunk_chars unless text itself is shorter.
    max_chars = max(min_chunk_chars, max_tokens * 3)
    if len(text) <= max_chars:
        return [text]
    out: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        j = min(n, i + max_chars)
        if j < n:
            # Prefer whitespace boundary.
            k = text.rfind(" ", i + min_chunk_chars, j)
            if k > i:
                j = k
        piece = text[i:j].strip()
        if piece:
            # If still over tokens, force smaller.
            while estimate_tokens(piece) > max_tokens and len(piece) > min_chunk_chars:
                piece = piece[: max(min_chunk_chars, len(piece) // 2)].rstrip()
            out.append(piece)
        if j <= i:
            break
        i = j
    return out or [text[:max_chars]]


def _split_sentences(text: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    for ch in text:
        buf.append(ch)
        if ch in ".!?" and len(buf) > 1:
            parts.append("".join(buf).strip())
            buf = []
    if buf:
        parts.append("".join(buf).strip())
    return [p for p in parts if p]


def _overlap_tail(segments: list[Segment], overlap_chars: int) -> list[Segment]:
    if not segments or overlap_chars <= 0:
        return []
    last = segments[-1]
    if len(last.text) <= overlap_chars:
        return [
            Segment(
                segment_id=f"{last.segment_id}#overlap",
                kind=last.kind,
                text=last.text,
                start_offset=last.start_offset,
                end_offset=last.end_offset,
                priority=last.priority,
                entity_hints=last.entity_hints,
            )
        ]
    tail = last.text[-overlap_chars:]
    return [
        Segment(
            segment_id=f"{last.segment_id}#overlap",
            kind=last.kind,
            text=tail,
            start_offset=last.start_offset,
            end_offset=last.end_offset,
            priority=last.priority,
            entity_hints=last.entity_hints,
        )
    ]
