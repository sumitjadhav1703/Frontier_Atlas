"""Discovery: stream URLs/IDs and persist tasks + checkpoint atomically."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable, Sequence
from typing import Any
from uuid import UUID

import psycopg

from signal_ingest.crawl.policy import UrlPolicy
from signal_ingest.crawl.queue import enqueue_task, get_checkpoint, upsert_checkpoint
from signal_ingest.crawl.types import DiscoveredItem


async def discover(
    source: str,
    checkpoint: str | None = None,
    *,
    pages: Iterable[Sequence[DiscoveredItem]] | None = None,
    page_items: Sequence[DiscoveredItem] | None = None,
) -> AsyncIterator[DiscoveredItem]:
    """
    Asynchronous stream of discovered items.

    Callers typically pass either:
    - `pages`: iterable of page batches (each batch ends with an opaque cursor on
      the last item), or
    - `page_items`: a single in-memory page (tests/fixtures).

    Persistence is handled by `persist_discovery_page` so adapters stay pure.
    """
    if page_items is not None:
        for item in page_items:
            yield item
        return
    if pages is None:
        return
    cursor = checkpoint
    for page in pages:
        for item in page:
            if item.cursor is None and cursor is not None:
                item = DiscoveredItem(
                    url=item.url,
                    external_id=item.external_id,
                    cursor=cursor,
                    metadata=item.metadata,
                )
            if item.cursor is not None:
                cursor = item.cursor
            yield item


def persist_discovery_page(
    conn: psycopg.Connection,
    *,
    source_id: str,
    items: Sequence[DiscoveredItem],
    run_id: UUID | None = None,
    stage: str = "fetch",
    checkpoint_stage: str = "discover",
    partition_key: str = "",
    url_policy: UrlPolicy | None = None,
    normalize: bool = True,
) -> dict[str, Any]:
    """
    Persist discovered tasks and the page cursor atomically.

    Duplicate semantic keys are skipped. Returns counts and final cursor.
    """
    policy = url_policy or UrlPolicy()
    created = 0
    skipped = 0
    cursor: str | None = None
    with conn.transaction():
        for item in items:
            url = item.url
            if normalize:
                try:
                    url = policy.normalize(url)
                except Exception:
                    skipped += 1
                    continue
            tid = enqueue_task(
                conn,
                source_id=source_id,
                url=url,
                stage=stage,
                external_id=item.external_id,
                run_id=run_id,
                payload=dict(item.metadata),
            )
            if tid is None:
                skipped += 1
            else:
                created += 1
            if item.cursor is not None:
                cursor = item.cursor
        if cursor is not None:
            upsert_checkpoint(
                conn,
                source_id=source_id,
                stage=checkpoint_stage,
                partition_key=partition_key,
                cursor_opaque=cursor,
            )
    return {
        "created": created,
        "skipped": skipped,
        "cursor": cursor,
        "checkpoint": get_checkpoint(
            conn,
            source_id=source_id,
            stage=checkpoint_stage,
            partition_key=partition_key,
        ),
    }
