"""OpenRouter public model listing as a PRODUCT source.

Product = base model id (":free"/":batch"/... variants collapse; tiers do not count
separately). Owner = the attributed display prefix of `name` before ": ".
Pricing from variant prices: all paid -> PAID, only free -> FREE, both -> FREEMIUM.
"""

from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any

from signal_ingest.adapters.bulk.eligibility import product_eligible
from signal_ingest.adapters.bulk.mapping import checkpoint_partition_key, content_revision
from signal_ingest.adapters.bulk.types import BulkParseResult
from signal_ingest.config.clock import get_clock
from signal_ingest.crawl.types import DiscoveredItem, RawObservation
from signal_ingest.models.records import PricingModel

SOURCE_ID = "bulk.openrouter"
SOURCE_NAME = "OpenRouter"
LISTING_URL = "https://openrouter.ai/api/v1/models"
MODEL_PAGE = "https://openrouter.ai/{}"


def _models(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        payload = payload.get("data") or payload.get("rows") or payload.get("models") or []
    return [m for m in payload if isinstance(m, dict) and m.get("id")] if isinstance(payload, list) else []


_SNAPSHOT_RE = re.compile(
    r"-(?:(?P<ymd>(?:19|20)\d{2}-\d{2}-\d{2})|(?P<ymd8>(?:19|20)\d{6})|(?P<md>\d{4})|preview)$"
)


def _valid_md(mmdd: str) -> bool:
    return 1 <= int(mmdd[:2]) <= 12 and 1 <= int(mmdd[2:]) <= 31


def base_id(model_id: str) -> str:
    """Variant id -> product id: strip ":free"-style variants, then trailing date snapshots
    (-YYYY-MM-DD, -YYYYMMDD, a valid -MMDD) and -preview, repeatedly."""
    out = str(model_id).split(":", 1)[0]
    while True:
        m = _SNAPSHOT_RE.search(out)
        if not m or "/" not in out[: m.start()]:
            return out
        if m.group("md") and not _valid_md(m.group("md")):
            return out
        out = out[: m.start()]


def discover_from_payload(
    payload: Any,
    *,
    partition: str = "default",
    revision: str | None = None,
) -> tuple[list[DiscoveredItem], str, str]:
    models = _models(payload)
    rev = revision or content_revision(models)
    partition_key = checkpoint_partition_key(revision=rev, partition=partition)
    groups: dict[str, list[dict[str, Any]]] = {}
    for m in models:
        groups.setdefault(base_id(m["id"]), []).append(m)
    items: list[DiscoveredItem] = []
    for idx, (bid, variants) in enumerate(groups.items()):
        primary = next(
            (v for v in variants if str(v["id"]) == bid),
            next((v for v in variants if ":" not in str(v["id"])), variants[0]),
        )
        row = {
            "id": bid,
            "name": primary.get("name"),
            "variants": [
                {"id": v["id"], "name": v.get("name"), "pricing": v.get("pricing")} for v in variants
            ],
        }
        listing = next((v["_listing"] for v in variants if v.get("_listing")), None)
        if listing:
            row["_listing"] = listing
        items.append(
            DiscoveredItem(
                url=MODEL_PAGE.format(str(primary["id"]).split(":", 1)[0]),
                external_id=bid,
                cursor=f"{rev}:{partition}:{idx}",
                metadata={
                    "adapter": SOURCE_ID,
                    "partition": partition,
                    "revision": rev,
                    "row": row,
                    "synthetic": listing is None,
                },
            )
        )
    return items, rev, partition_key


def _variant_price(pricing: Any) -> str | None:
    """'free' when every listed price is 0, 'paid' when any is > 0, None if unknown."""
    if not isinstance(pricing, dict) or not pricing:
        return None
    values: list[Decimal] = []
    for key in ("prompt", "completion", "request", "image"):
        if key not in pricing:
            continue
        try:
            values.append(Decimal(str(pricing[key])))
        except (InvalidOperation, ValueError):
            return None
    if not values or any(v < 0 for v in values):
        return None  # e.g. router pseudo-models priced -1
    return "paid" if any(v > 0 for v in values) else "free"


def classify_variants(variants: list[dict[str, Any]]) -> PricingModel | None:
    kinds = [_variant_price(v.get("pricing")) for v in variants]
    if not kinds or None in kinds:
        return None
    if all(k == "paid" for k in kinds):
        return PricingModel.PAID
    if all(k == "free" for k in kinds):
        return PricingModel.FREE
    return PricingModel.FREEMIUM


def split_owner(name: str | None, model_id: str = "") -> tuple[str | None, str | None]:
    """'Thinking Machines: Inkling 2 Mini' -> ('Thinking Machines', 'Inkling 2 Mini')."""
    text = " ".join(str(name or "").split())
    if ": " not in text:
        return None, text or None
    owner, product = text.split(": ", 1)
    suffix = model_id.split(":", 1)[1] if ":" in model_id else ""
    if suffix:
        product = re.sub(rf"\s*\({re.escape(suffix)}\)\s*$", "", product, flags=re.IGNORECASE)
    return owner.strip() or None, product.strip() or None


def parse_observation(raw: RawObservation, *, row: dict[str, Any] | None = None) -> BulkParseResult:
    data = row
    if data is None and raw.body:
        try:
            body = json.loads(raw.body.decode("utf-8"))
            data = body.get("row") if isinstance(body, dict) else None
        except (UnicodeDecodeError, json.JSONDecodeError):
            data = None
    if not isinstance(data, dict) or not data.get("id"):
        return BulkParseResult(
            ok=False, quarantine_reason="parse_drift", quarantine_detail={"error": "missing_model_json"}
        )
    bid = base_id(str(data["id"]))
    variants = list(data.get("variants") or [{"id": data["id"], "name": data.get("name"), "pricing": data.get("pricing")}])
    primary = next(
        (v for v in variants if str(v.get("id")) == bid),
        next((v for v in variants if ":" not in str(v.get("id"))), variants[0]),
    )
    owner, product_name = split_owner(primary.get("name") or data.get("name"), str(primary.get("id") or ""))
    # Evidence page = a real listed model page (the collapsed id may have none of its own).
    page = MODEL_PAGE.format(str(primary.get("id") or bid).split(":", 1)[0])
    pricing_model = classify_variants(variants)
    if not product_name:
        return BulkParseResult(
            ok=False, record_type="PRODUCT", external_id=bid,
            quarantine_reason="missing_product_name", quarantine_detail={},
        )
    if pricing_model is None:
        return BulkParseResult(
            ok=False, record_type="PRODUCT", external_id=bid,
            quarantine_reason="missing_pricing_evidence",
            quarantine_detail={"variants": variants},
        )
    if not owner:
        return BulkParseResult(
            ok=False, record_type="PRODUCT", external_id=bid,
            quarantine_reason="missing_ownership_evidence",
            quarantine_detail={"name": primary.get("name")},
        )
    listing = data.get("_listing") or {}
    listing_url = listing.get("url") or LISTING_URL
    payload = {
        "schemaVersion": "1.0",
        "source": {"name": SOURCE_NAME, "url": page},
        "content": {
            "startupName": owner,
            "pricingModel": pricing_model.value,
            "productName": product_name,
            "productUrl": page,
            "ownerEvidenceUrl": page,
            "pricingEvidenceUrl": page,
        },
        "collectedAt": get_clock().now().isoformat().replace("+00:00", "Z"),
        "meta": {
            "owner_evidenced": True,
            "owner_method": "directory_name_prefix",
            "pricing_evidenced": True,
            "product_id": bid,
            "variants": [v.get("id") for v in variants],
        },
    }
    evidence = {
        "request_url": raw.request_url,
        "final_url": raw.final_url,
        "content_sha256": raw.content_sha256,
        "owner": {"value": owner, "url": page, "method": "directory_name_prefix", "pointer": "$.name", "raw": primary.get("name")},
        "pricing": {"model": pricing_model.value, "url": page, "listing_url": listing_url, "pointer": "$.pricing", "variants": variants},
    }
    if listing:
        evidence["listing"] = listing
    return BulkParseResult(
        ok=True,
        record_type="PRODUCT",
        external_id=bid,
        payload=payload,
        evidence=evidence,
        relations=[
            {
                "relation_type": "OWNS",
                "from_kind": "organization",
                "from_key": owner.strip().lower(),
                "to_kind": "product",
                "to_key": bid,
                "is_primary": True,
            }
        ],
        eligible=product_eligible(payload, owner_evidenced=True, pricing_evidenced=True),
    )
