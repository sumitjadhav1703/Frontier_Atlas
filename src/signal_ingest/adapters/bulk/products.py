"""AgiPicks-shaped product bulk adapter."""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from typing import Any, Sequence

from signal_ingest.adapters.bulk.eligibility import product_eligible
from signal_ingest.adapters.bulk.mapping import (
    checkpoint_partition_key,
    owner_from_html,
    registrable_domain,
    classify_pricing,
    collapse_product_plans,
    content_revision,
    normalize_url,
)
from signal_ingest.adapters.bulk.types import BulkParseResult
from signal_ingest.config.clock import get_clock
from signal_ingest.crawl.types import DiscoveredItem, RawObservation

SOURCE_ID = "bulk.products"
SOURCE_NAME = "AgiPicks"


def discover_from_payload(
    payload: Any,
    *,
    partition: str = "default",
    revision: str | None = None,
) -> tuple[list[DiscoveredItem], str, str]:
    rows = _products(payload)
    collapsed = collapse_product_plans(rows)
    rev = revision or content_revision(collapsed)
    partition_key = checkpoint_partition_key(revision=rev, partition=partition)
    items: list[DiscoveredItem] = []
    for idx, row in enumerate(collapsed):
        external_id = _product_id(row)
        # Listing rows: fetch the vendor homepage for owner evidence, not the directory page.
        first = ("website", "url", "product_url") if row.get("_listing") else ("url", "product_url", "website")
        url = str(
            next((row[k] for k in first if row.get(k)), None)
            or f"https://agipicks.example/products/{external_id}"
        )
        items.append(
            DiscoveredItem(
                url=url,
                external_id=external_id,
                cursor=f"{rev}:{partition}:{idx}",
                metadata={
                    "adapter": SOURCE_ID,
                    "partition": partition,
                    "revision": rev,
                    "row": row,
                    "synthetic": bool(row.get("_synthetic", True)),
                },
            )
        )
    return items, rev, partition_key


def parse_observation(
    raw: RawObservation,
    *,
    row: dict[str, Any] | None = None,
    llm_owner: dict[str, Any] | None = None,
) -> BulkParseResult:
    data = row or _row_from_body(raw)
    if data is None:
        return BulkParseResult(
            ok=False,
            quarantine_reason="parse_drift",
            quarantine_detail={"error": "missing_product_json"},
        )

    external_id = _product_id(data)
    product_name = (
        data.get("productName")
        or data.get("name")
        or data.get("product_name")
        or data.get("title")
    )
    owner = (
        data.get("owner")
        or data.get("startupName")
        or data.get("company")
        or data.get("vendor")
    )
    owner_url = data.get("owner_url") or data.get("company_url") or data.get("about_url")
    pricing_url = data.get("pricing_url") or data.get("pricingUrl") or data.get("checkout_url")
    agipicks = _is_agipicks_row(data)
    if agipicks:
        # Attributed directory pricing: evidence is the AgiPicks tool page.
        pricing_url = pricing_url or data.get("url")
    owner_method = "row"
    owner_extra: dict[str, Any] = {}
    homepage_url = raw.final_url or raw.request_url
    website = data.get("website")
    # Parked / redirected-away vendor domains cannot evidence the owner.
    domain_mismatch = bool(
        not owner
        and _is_html(raw)
        and website
        and registrable_domain(homepage_url) != registrable_domain(str(website))
    )
    if not owner and not domain_mismatch:
        html_owner = (
            owner_from_html(raw.body, page_url=homepage_url, product_name=product_name)
            if _is_html(raw)
            else None
        )
        chosen = html_owner or (llm_owner if llm_owner and _owner_value(llm_owner) else None)
        if chosen:
            owner = _owner_value(chosen)
            owner_url = chosen.get("url") or raw.final_url or raw.request_url
            owner_method = str(chosen.get("method") or "json_ld")
            owner_extra = {k: v for k, v in chosen.items() if k not in {"value", "owner"}}
    product_url = (
        data.get("product_url")
        or data.get("url")
        or data.get("website")
        or raw.final_url
    )

    owner_evidenced = bool(owner) and bool(
        owner_url or data.get("owner_source") or data.get("attributed_owner")
    )
    if owner and owner_method != "row":
        owner_evidenced = bool(owner_url)
    # Directory-attributed owner field counts when explicitly marked.
    if owner and data.get("owner_evidenced") is True:
        owner_evidenced = True
    if owner and data.get("source_attribution") in {"official", "vendor", "directory"}:
        owner_evidenced = True
    # AgiPicks attributed company field counts only when the payload marks it evidenced.
    if owner and product_url and data.get("company"):
        owner_evidenced = owner_evidenced or data.get("company_evidenced") is True

    plans = list(data.get("plans") or [])
    pricing_model = classify_pricing(
        pricing=data.get("pricing") or data.get("pricingModel"),
        # AgiPicks: the attributed pricingModel field alone decides (no plan inference).
        plans=[] if agipicks else plans,
        has_free_tier=None if agipicks else data.get("has_free_tier"),
        has_paid_tier=None if agipicks else data.get("has_paid_tier"),
        trial_only=None
        if agipicks
        else data.get("trial_only")
        if data.get("trial_only") is not None
        else _infer_trial_only(data, plans),
        enterprise_only=None if agipicks else data.get("enterprise_only"),
    )
    pricing_evidenced = pricing_model is not None and bool(
        pricing_url
        or data.get("pricing_evidenced")
        or data.get("pricing")
        or data.get("pricingModel")
        or plans
    )

    if not product_name:
        return BulkParseResult(
            ok=False,
            record_type="PRODUCT",
            external_id=external_id,
            quarantine_reason="missing_product_name",
            quarantine_detail={},
        )
    if not pricing_evidenced or pricing_model is None:
        return BulkParseResult(
            ok=False,
            record_type="PRODUCT",
            external_id=external_id,
            quarantine_reason="missing_pricing_evidence",
            quarantine_detail={"pricing": data.get("pricing") or data.get("pricingModel"), "plans": plans},
        )
    if not owner_evidenced:
        return BulkParseResult(
            ok=False,
            record_type="PRODUCT",
            external_id=external_id,
            quarantine_reason="owner_domain_mismatch" if domain_mismatch else "missing_ownership_evidence",
            quarantine_detail={
                "owner": owner, "owner_url": owner_url, "homepage_url": raw.final_url,
                "website": website, "html": _is_html(raw),
            },
        )

    collected = get_clock().now()
    payload = {
        "schemaVersion": "1.0",
        "source": {"name": SOURCE_NAME, "url": str(product_url)},
        "content": {
            "startupName": str(owner),
            "pricingModel": pricing_model.value,
            "productName": str(product_name),
            "productUrl": str(data.get("website") if agipicks and data.get("website") else product_url),
            "ownerEvidenceUrl": owner_url or product_url,
            "pricingEvidenceUrl": pricing_url or product_url,
        },
        "collectedAt": collected.isoformat().replace("+00:00", "Z"),
        "meta": {
            "owner_evidenced": True,
            "owner_method": owner_method,
            "pricing_evidenced": True,
            **({"license": "CC BY 4.0", "attribution": "AgiPicks"} if agipicks else {}),
            "plans_collapsed": len(plans),
            "product_id": external_id,
        },
    }
    evidence = {
        "request_url": raw.request_url,
        "final_url": raw.final_url,
        "content_sha256": raw.content_sha256,
        "owner": {"value": owner, "url": owner_url or product_url, "method": owner_method, **owner_extra},
        "pricing": {
            "model": pricing_model.value,
            "url": pricing_url or product_url,
            "plans": plans,
            **({"field": "$.pricingModel", "pricesCheckedAt": data.get("pricesCheckedAt")} if agipicks else {}),
        },
    }
    if data.get("_listing"):
        evidence["listing"] = data["_listing"]
    eligible = product_eligible(
        payload, owner_evidenced=True, pricing_evidenced=True
    )
    return BulkParseResult(
        ok=True,
        record_type="PRODUCT",
        external_id=external_id,
        payload=payload,
        evidence=evidence,
        relations=[
            {
                "relation_type": "OWNS",
                "from_kind": "organization",
                "from_key": str(owner).strip().lower(),
                "to_kind": "product",
                "to_key": external_id,
                "is_primary": True,
            }
        ],
        eligible=eligible,
    )


def _is_agipicks_row(data: dict[str, Any]) -> bool:
    return "pricesCheckedAt" in data or "agipicks.com/tools/" in str(data.get("url") or "")


def _is_html(raw: RawObservation) -> bool:
    ctype = (raw.content_type or "").lower()
    if "html" in ctype:
        return True
    head = (raw.body or b"")[:512].lstrip().lower()
    return head.startswith(b"<!doctype html") or head.startswith(b"<html")


def _owner_value(blob: dict[str, Any]) -> str | None:
    val = blob.get("value") or blob.get("owner")
    return str(val).strip() if val and str(val).strip() else None


async def llm_owner_fallback(
    raw: RawObservation,
    *,
    provider: Any,
    row: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Grounded LLM owner from homepage HTML via extract(); None unless ACCEPTED."""
    from signal_ingest.llm import extract
    from signal_ingest.llm.types import ExtractCandidate, FieldDecision

    if not raw.body:
        return None
    data = row or {}
    name = data.get("productName") or data.get("name") or data.get("title")
    html = raw.body.decode("utf-8", errors="replace")
    candidate = ExtractCandidate(
        candidate_id=f"owner:{_product_id(data) if data else raw.final_url}",
        record_type="PRODUCT",
        html=html,
        target_entity=str(name) if name else None,
        fields_requested=("owner",),
        raw_hash=raw.content_sha256,
        source_url=raw.final_url,
    )
    result = await extract(candidate, provider)
    field = result.fields.get("owner")
    if field is None or field.decision is not FieldDecision.ACCEPTED or not field.value:
        return None
    tier, model = result.provider, result.model
    attempts = getattr(provider, "last_attempts", None) or []
    ok = [a for a in attempts if getattr(a, "status", None) == "ok"]
    if ok:
        tier = ok[-1].tier
        model = next(
            (t.litellm_model for t in getattr(provider, "tiers", ()) if t.name == tier), model
        )
    return {
        "value": str(field.value),
        "owner": str(field.value),
        "method": "llm",
        "provider": tier,
        "model": model,
        "url": raw.final_url or raw.request_url,
        "quote": field.quote,
        "segment_ids": list(field.segment_ids),
        "content_sha256": raw.content_sha256,
        # Redacted dispatch records (tier/status/tokens only; no keys or headers).
        "attempts": [asdict(a) if is_dataclass(a) else dict(a) for a in attempts],
    }


def _infer_trial_only(data: dict[str, Any], plans: list[dict[str, Any]]) -> bool | None:
    label = str(data.get("pricing") or data.get("pricingModel") or "").lower()
    if "trial" in label and "freemium" not in label:
        return True
    trial = False
    permanent_free = False
    for plan in plans:
        blob = " ".join(
            str(plan.get(k) or "") for k in ("name", "tier", "type", "pricing")
        ).lower()
        if "trial" in blob:
            trial = True
        if blob.strip() in {"free", "hobby"} or (
            "free" in blob and "trial" not in blob
        ):
            permanent_free = True
    if trial and not permanent_free:
        return True
    return None


def _products(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for key in ("products", "tools", "results", "items", "data"):
            val = payload.get(key)
            if isinstance(val, list):
                return [r for r in val if isinstance(r, dict)]
        return [payload]
    return []


def _product_id(row: dict[str, Any]) -> str:
    for key in ("product_id", "id", "slug"):
        if row.get(key) is not None:
            return str(row[key])
    url = row.get("url") or row.get("product_url")
    if url:
        return normalize_url(str(url))
    owner = str(row.get("owner") or row.get("company") or "").strip().lower()
    name = str(row.get("productName") or row.get("name") or "").strip().lower()
    return f"{owner}::{name}"


def _row_from_body(raw: RawObservation) -> dict[str, Any] | None:
    if raw.body is None:
        return None
    try:
        data = json.loads(raw.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if isinstance(data, dict) and isinstance(data.get("row"), dict):
        return data["row"]
    rows = _products(data)
    if len(rows) == 1:
        return rows[0]
    # Already-collapsed product object
    return data if isinstance(data, dict) else None


def discover_pages(
    pages: Sequence[Any],
    *,
    partition: str = "default",
) -> tuple[list[DiscoveredItem], str, str]:
    all_rows: list[dict[str, Any]] = []
    for page in pages:
        all_rows.extend(_products(page))
    return discover_from_payload(all_rows, partition=partition)
