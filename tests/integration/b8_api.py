"""Read-only helpers for B8 live gate artifacts.

Oracle: docs/build-plan.md (B8) + actual builder outputs under var/live/manifests/.

Builder surfaces (verifier only reads):

  var/live/manifests/monitors_qualification.json   # primary monitors manifest
  var/live/manifests/monitors.json                 # legacy fallback
  var/live/manifests/bulk_pilots.json
  var/live/manifests/llm_canary.json
  var/live/manifests/qualification_summary.json
  var/live/exports/<run_id>/<as_of>/manifest.json + tab CSVs

  artifacts/gates/b8_llm_trace.json                # real success lineage (when present)
  artifacts/gates/b8_blocker.json

Recompute ≥1,000 minima from export rows (durableIdentity), not pilot/hand totals.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from signal_ingest.llm.adapters.base import is_live_provider

REPO_ROOT = Path(__file__).resolve().parents[2]
VAR_LIVE = REPO_ROOT / "var" / "live"
MANIFESTS_DIR = VAR_LIVE / "manifests"
EXPORT_ROOT = VAR_LIVE / "exports"
ARTIFACTS_GATES = REPO_ROOT / "artifacts" / "gates"
SOURCES_DIR = REPO_ROOT / "config" / "sources"

ACQUISITION_MANIFEST = MANIFESTS_DIR / "acquisition.json"
BULK_PILOTS_MANIFEST = MANIFESTS_DIR / "bulk_pilots.json"
QUALIFICATION_SUMMARY = MANIFESTS_DIR / "qualification_summary.json"
MONITORS_QUALIFICATION = MANIFESTS_DIR / "monitors_qualification.json"
MONITORS_MANIFEST_LEGACY = MANIFESTS_DIR / "monitors.json"
# Prefer builder name; legacy monitors.json remains a fallback.
MONITORS_MANIFEST = MONITORS_QUALIFICATION
LLM_CANARY_MANIFEST = MANIFESTS_DIR / "llm_canary.json"
LLM_LINEAGE_MANIFEST = MANIFESTS_DIR / "llm_lineage.json"
B8_LLM_TRACE = ARTIFACTS_GATES / "b8_llm_trace.json"
B8_BLOCKER = ARTIFACTS_GATES / "b8_blocker.json"
B8_AUDIT = ARTIFACTS_GATES / "b8_audit.json"

BULK_CLASSES = ("STARTUP", "PRODUCT", "RESEARCH_PAPER")
BULK_TAB_BY_CLASS = {
    "STARTUP": "Startups",
    "PRODUCT": "Products",
    "RESEARCH_PAPER": "Research Papers",
}
BULK_CLASS_ALIASES = {
    "STARTUP": ("STARTUP", "startup", "startups", "Startups"),
    "PRODUCT": ("PRODUCT", "product", "products", "Products"),
    "RESEARCH_PAPER": (
        "RESEARCH_PAPER",
        "research_paper",
        "papers",
        "Research Papers",
    ),
}
BULK_MINIMA = 1000
REQUIRED_MONITOR_COUNT = 10
LIVE_SEEDS_TARGET = 50

FAKE_PROVIDER_PREFIXES = ("fake", "replay", "mock", "scripted")


class MissingLiveArtifactError(AssertionError):
    """Required live B8 artifact is absent — gate stays BLOCKED."""


def load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise MissingLiveArtifactError(
            f"B8 BLOCKED: missing required live artifact {path.relative_to(REPO_ROOT)}"
        )
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise AssertionError(f"{path} must be a JSON object")
    return data


def try_load_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else None


def require_path(path: Path, *, what: str) -> Path:
    if not path.exists():
        raise MissingLiveArtifactError(
            f"B8 BLOCKED: missing {what} at {path.relative_to(REPO_ROOT)}"
        )
    return path


def latest_live_export_dir(export_root: Path | None = None) -> Path:
    """Newest export snapshot under var/live/exports (by mtime of manifest.json)."""
    root = export_root or EXPORT_ROOT
    require_path(root, what="live export root")
    manifests = sorted(
        root.glob("*/*/manifest.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not manifests:
        raise MissingLiveArtifactError(
            f"B8 BLOCKED: no live export snapshot under {root.relative_to(REPO_ROOT)}"
        )
    return manifests[0].parent


def load_export_manifest(export_dir: Path | None = None) -> dict[str, Any]:
    dest = export_dir or latest_live_export_dir()
    return load_json(dest / "manifest.json")


def _csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        raise MissingLiveArtifactError(
            f"B8 BLOCKED: missing export tab CSV {path.relative_to(REPO_ROOT)}"
        )
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def tab_csv_path(export_dir: Path, tab: str) -> Path:
    return export_dir / f"{tab.replace(' ', '_')}.csv"


def recompute_unique_bulk_counts(export_dir: Path | None = None) -> dict[str, int]:
    """Count unique durableIdentity values per bulk class from live export CSVs."""
    dest = export_dir or latest_live_export_dir()
    counts: dict[str, int] = {}
    for record_type, tab in BULK_TAB_BY_CLASS.items():
        rows = _csv_rows(tab_csv_path(dest, tab))
        ids: set[str] = set()
        for row in rows:
            lineage = (row.get("lineageMode") or row.get("lineage_mode") or "").strip().lower()
            if lineage and lineage != "live":
                continue
            key = (
                row.get("durableIdentity")
                or row.get("durable_identity")
                or row.get("source.url")
                or ""
            ).strip()
            if key:
                ids.add(key)
        counts[record_type] = len(ids)
    return counts


def load_acquisition_manifest() -> dict[str, Any]:
    """Prefer acquisition.json; else bulk pilots / qualification summary (pilots ≠ 1k)."""
    for path in (ACQUISITION_MANIFEST, BULK_PILOTS_MANIFEST, QUALIFICATION_SUMMARY):
        data = try_load_json(path)
        if data is not None:
            return data
    raise MissingLiveArtifactError(
        "B8 BLOCKED: missing acquisition/bulk_pilots/qualification_summary under "
        f"{MANIFESTS_DIR.relative_to(REPO_ROOT)}"
    )


def resolve_monitors_manifest_path() -> Path:
    if MONITORS_QUALIFICATION.is_file():
        return MONITORS_QUALIFICATION
    if MONITORS_MANIFEST_LEGACY.is_file():
        return MONITORS_MANIFEST_LEGACY
    raise MissingLiveArtifactError(
        "B8 BLOCKED: missing monitors manifest "
        f"({MONITORS_QUALIFICATION.relative_to(REPO_ROOT)} or "
        f"{MONITORS_MANIFEST_LEGACY.relative_to(REPO_ROOT)})"
    )


def load_monitors_manifest() -> dict[str, Any]:
    return load_json(resolve_monitors_manifest_path())


def configured_monitor_ids() -> list[str]:
    from signal_ingest.adapters.monitors.config import list_monitor_source_ids

    return list(list_monitor_source_ids(SOURCES_DIR))


def source_yaml_status(source_id: str) -> dict[str, Any]:
    path = SOURCES_DIR / f"{source_id}.yaml"
    require_path(path, what=f"source inventory for {source_id}")
    data: dict[str, Any] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, _, value = line.partition(":")
        value = value.strip().strip('"')
        if value.lower() == "null":
            value = None
        data[key.strip()] = value
    return data


def _discovery_ok(entry: dict[str, Any]) -> bool:
    if entry.get("discovery_ok") or entry.get("discovery_success") or entry.get("probe_discovery_ok"):
        return True
    discovery = entry.get("discovery")
    if isinstance(discovery, dict) and str(discovery.get("outcome") or "").lower() == "ok":
        return True
    probe = entry.get("probe")
    if isinstance(probe, dict) and str(probe.get("fetch_status") or "").lower() == "ok":
        return True
    return False


def _detail_attempted(entry: dict[str, Any]) -> bool:
    """Detail probe was run (outcome may be ok/invalid/denied; zero-fresh still OK)."""
    if entry.get("detail_ok") is True or entry.get("detail_success") is True:
        return True
    if entry.get("probe_detail_ok") is True:
        return True
    if isinstance(entry.get("detail"), dict):
        return True
    probe = entry.get("probe")
    if isinstance(probe, dict) and probe.get("detail_url"):
        return True
    return False


def qualified_monitor_entries(manifest: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Return monitor entries marked qualified with a successful discovery probe.

    Builder shape (`monitors_qualification.json`): status=qualified, nested
    discovery.outcome / probe.fetch_status. Zero fresh signals is allowed.
    Detail may be ok or terminal (challenge/denied) after the pair was attempted.
    """
    data = manifest if manifest is not None else load_monitors_manifest()
    sources = data.get("sources") or data.get("monitors") or []
    if not isinstance(sources, list):
        raise AssertionError("monitors manifest sources must be a list")
    out: list[dict[str, Any]] = []
    for entry in sources:
        if not isinstance(entry, dict):
            continue
        status = str(entry.get("status") or "").lower()
        if status != "qualified":
            continue
        if _discovery_ok(entry) and _detail_attempted(entry):
            out.append(entry)
            continue
        # Legacy boolean flags only.
        if _discovery_ok(entry) and (
            entry.get("detail_ok") or entry.get("detail_success") or entry.get("probe_detail_ok")
        ):
            out.append(entry)
    return out


def monitors_qualified_count(manifest: dict[str, Any] | None = None) -> int:
    data = manifest if manifest is not None else load_monitors_manifest()
    if "qualified_count" in data:
        try:
            return int(data["qualified_count"])
        except (TypeError, ValueError):
            pass
    return len(qualified_monitor_entries(data))


def load_llm_lineage() -> dict[str, Any]:
    """Load a successful live LLM lineage artifact (not a blocked canary stub)."""
    candidates = (B8_LLM_TRACE, LLM_LINEAGE_MANIFEST, LLM_CANARY_MANIFEST)
    blocked_canary: dict[str, Any] | None = None
    for path in candidates:
        data = try_load_json(path)
        if data is None:
            continue
        if data.get("live_llm_success") is False or str(data.get("status") or "").upper() == "BLOCKED":
            if path == LLM_CANARY_MANIFEST or data.get("manifest") == "llm_canary":
                blocked_canary = data
            continue
        if claims_live_llm_success(data):
            return data
        # Present but incomplete success claim — keep looking; remember last.
        blocked_canary = blocked_canary or data
    if blocked_canary is not None:
        raise MissingLiveArtifactError(
            "B8 BLOCKED: LLM canary/lineage present but live_llm_success is false "
            f"(see {LLM_CANARY_MANIFEST.relative_to(REPO_ROOT)} / "
            f"{B8_BLOCKER.relative_to(REPO_ROOT)}); fake/replay cannot close this gate"
        )
    raise MissingLiveArtifactError(
        "B8 BLOCKED: missing real LLM lineage artifact "
        f"({B8_LLM_TRACE.relative_to(REPO_ROOT)} or "
        f"{LLM_LINEAGE_MANIFEST.relative_to(REPO_ROOT)} or successful "
        f"{LLM_CANARY_MANIFEST.relative_to(REPO_ROOT)})"
    )


def provider_name_from_lineage(lineage: dict[str, Any]) -> str:
    for key in ("provider", "provider_name", "final_provider"):
        val = lineage.get(key)
        if val:
            return str(val)
    attempt = lineage.get("attempt") or lineage.get("final_attempt") or {}
    if isinstance(attempt, dict) and attempt.get("provider"):
        return str(attempt["provider"])
    transcript = lineage.get("transcript") or {}
    if isinstance(transcript, dict):
        for key in ("provider", "final_provider"):
            if transcript.get(key):
                return str(transcript[key])
        attempts = transcript.get("attempts") or []
        if attempts and isinstance(attempts[-1], dict) and attempts[-1].get("provider"):
            return str(attempts[-1]["provider"])
    inventory = lineage.get("provider_inventory") or {}
    routes = inventory.get("routes") if isinstance(inventory, dict) else None
    if isinstance(routes, list):
        for route in routes:
            if isinstance(route, dict) and route.get("ready") and route.get("provider"):
                return str(route["provider"])
    return ""


def claims_live_llm_success(lineage: dict[str, Any]) -> bool:
    if lineage.get("live_llm_success") is False:
        return False
    if lineage.get("live_success") is False:
        return False
    if str(lineage.get("status") or "").upper() == "BLOCKED":
        return False
    if lineage.get("live_llm_success") is True or lineage.get("live_success") is True:
        return True
    if lineage.get("qualified") is True:
        return True
    if str(lineage.get("status") or "").lower() in {"success", "accepted", "live_ok"}:
        return True
    if lineage.get("accepted_field_evidence") or lineage.get("field_evidence"):
        return True
    return False


def assert_provider_is_live(provider: str) -> None:
    name = (provider or "").strip().lower()
    assert name, "LLM lineage missing provider name"
    assert not any(name == p or name.startswith(p) for p in FAKE_PROVIDER_PREFIXES), (
        f"B8 BLOCKED: provider {provider!r} is fake/replay/mock and cannot qualify live"
    )
    assert is_live_provider(provider), (
        f"B8 BLOCKED: provider {provider!r} is not a live-qualified adapter "
        "(expected gemini|groq|ollama)"
    )


def find_export_row_by_identity(
    export_dir: Path,
    *,
    durable_id: str,
    tab: str | None = None,
) -> dict[str, str] | None:
    tabs = [tab] if tab else list(BULK_TAB_BY_CLASS.values()) + ["Jobs", "News"]
    needle = durable_id.strip()
    for name in tabs:
        path = tab_csv_path(export_dir, name)
        if not path.is_file():
            continue
        for row in _csv_rows(path):
            key = (row.get("durableIdentity") or row.get("durable_identity") or "").strip()
            if key == needle:
                return row
    return None


def claimed_bulk_counts(blob: dict[str, Any]) -> dict[str, int]:
    """Extract per-class claimed counts from acquisition / bulk_pilots / summary."""
    out: dict[str, int] = {}
    pools: list[dict[str, Any]] = []
    for key in ("counts", "unique_counts", "classes", "bulk_pilots"):
        block = blob.get(key)
        if isinstance(block, dict):
            pools.append(block)
    proven = blob.get("proven")
    if isinstance(proven, dict) and isinstance(proven.get("bulk_pilots"), dict):
        pools.append(proven["bulk_pilots"])
    tracks = blob.get("tracks")
    if isinstance(tracks, dict):
        bulk = tracks.get("bulk")
        if isinstance(bulk, dict) and isinstance(bulk.get("classes"), dict):
            pools.append(bulk["classes"])
    for cls, aliases in BULK_CLASS_ALIASES.items():
        for pool in pools:
            for alias in aliases:
                if alias in pool:
                    try:
                        out[cls] = int(pool[alias])
                        break
                    except (TypeError, ValueError):
                        continue
            if cls in out:
                break
    return out


def live_seed_progress() -> tuple[int | None, int]:
    """Return (current_live_seeds, target) from blocker / seeds file / summary."""
    target = LIVE_SEEDS_TARGET
    current: int | None = None
    blocker = try_load_json(B8_BLOCKER)
    if blocker and isinstance(blocker.get("proven"), dict):
        proven = blocker["proven"]
        if proven.get("live_seeds") is not None:
            try:
                current = int(proven["live_seeds"])
            except (TypeError, ValueError):
                current = None
        if proven.get("live_seeds_target") is not None:
            try:
                target = int(proven["live_seeds_target"])
            except (TypeError, ValueError):
                pass
    seeds_path = REPO_ROOT / "config" / "seeds.live.json"
    seeds_file = try_load_json(seeds_path)
    if seeds_file is not None:
        if seeds_file.get("target_count") is not None:
            try:
                target = int(seeds_file["target_count"])
            except (TypeError, ValueError):
                pass
        if seeds_file.get("identity_count") is not None:
            try:
                current = int(seeds_file["identity_count"])
            except (TypeError, ValueError):
                pass
        elif isinstance(seeds_file.get("identities"), list):
            current = len(seeds_file["identities"])
    summary = try_load_json(QUALIFICATION_SUMMARY)
    if current is None and summary and isinstance(summary.get("tracks"), dict):
        seeds = summary["tracks"].get("seeds")
        if isinstance(seeds, dict):
            for key in ("count", "live_seeds", "n", "size"):
                if seeds.get(key) is not None:
                    try:
                        current = int(seeds[key])
                        break
                    except (TypeError, ValueError):
                        pass
    return current, target


def unmet_gate_shortfalls(
    *,
    counts: dict[str, int] | None = None,
    qualified_monitors: list[dict[str, Any]] | None = None,
    llm_ok: bool | None = None,
    fixture_leak: bool | None = None,
) -> list[str]:
    """Compute exact shortfall strings for blocker documentation."""
    shortfalls: list[str] = []
    if counts is None:
        try:
            counts = recompute_unique_bulk_counts()
        except MissingLiveArtifactError as exc:
            shortfalls.append(f"missing_live_export:{exc}")
            counts = {}
    for cls in BULK_CLASSES:
        n = int(counts.get(cls, 0))
        if n < BULK_MINIMA:
            shortfalls.append(f"bulk_{cls.lower()}_unique={n}<{BULK_MINIMA}")
    if qualified_monitors is None:
        try:
            qualified_monitors = qualified_monitor_entries()
        except MissingLiveArtifactError as exc:
            shortfalls.append(f"missing_monitors_manifest:{exc}")
            qualified_monitors = []
    qcount = len(qualified_monitors)
    try:
        qcount = max(qcount, monitors_qualified_count())
    except MissingLiveArtifactError:
        pass
    if qcount < REQUIRED_MONITOR_COUNT:
        shortfalls.append(
            f"qualified_monitors={qcount}<{REQUIRED_MONITOR_COUNT}"
        )
    if llm_ok is None:
        try:
            lineage = load_llm_lineage()
            provider = provider_name_from_lineage(lineage)
            llm_ok = claims_live_llm_success(lineage) and is_live_provider(provider)
            if not (lineage.get("export_row_id") or lineage.get("durable_identity")):
                llm_ok = False
                shortfalls.append("llm_lineage_missing_export_row_link")
        except MissingLiveArtifactError:
            llm_ok = False
            shortfalls.append("missing_real_llm_lineage")
        except AssertionError:
            llm_ok = False
    if not llm_ok and "missing_real_llm_lineage" not in shortfalls:
        if not any(s.startswith("llm_") for s in shortfalls):
            shortfalls.append("missing_real_llm_lineage_into_live_export")
    seeds_n, seeds_target = live_seed_progress()
    if seeds_n is not None and seeds_n < seeds_target:
        shortfalls.append(f"live_seeds={seeds_n}<{seeds_target}")
    if fixture_leak:
        shortfalls.append("fixture_lineage_in_live_export")
    return shortfalls


def load_blocker() -> dict[str, Any]:
    return load_json(B8_BLOCKER)


def paid_spend_cap_from_live_artifacts() -> int | None:
    """Find paid_spend_cap=0 capture on live manifests, blocker, or providers.yaml."""
    paths: list[Path] = [
        ACQUISITION_MANIFEST,
        BULK_PILOTS_MANIFEST,
        QUALIFICATION_SUMMARY,
        LLM_CANARY_MANIFEST,
        B8_BLOCKER,
    ]
    if MONITORS_QUALIFICATION.is_file():
        paths.insert(0, MONITORS_QUALIFICATION)
    elif MONITORS_MANIFEST_LEGACY.is_file():
        paths.insert(0, MONITORS_MANIFEST_LEGACY)
    for path in paths:
        data = try_load_json(path)
        if data is None:
            continue
        if "paid_spend_cap" in data:
            try:
                return int(data["paid_spend_cap"])
            except (TypeError, ValueError):
                pass
        for nest_key in ("quotas", "quota", "provider_budgets", "budget", "unlock"):
            nest = data.get(nest_key)
            if isinstance(nest, dict) and "paid_spend_cap" in nest:
                try:
                    return int(nest["paid_spend_cap"])
                except (TypeError, ValueError):
                    pass
    # Configured hard zero-spend default (offline contract).
    providers = REPO_ROOT / "config" / "providers.yaml"
    if providers.is_file():
        text = providers.read_text(encoding="utf-8")
        for line in text.splitlines():
            if "paid_spend_cap" in line and ":" in line:
                _, _, raw = line.partition(":")
                try:
                    return int(raw.strip().split()[0])
                except (TypeError, ValueError, IndexError):
                    pass
    return None


__all__ = [
    "ACQUISITION_MANIFEST",
    "ARTIFACTS_GATES",
    "B8_AUDIT",
    "B8_BLOCKER",
    "B8_LLM_TRACE",
    "BULK_CLASSES",
    "BULK_MINIMA",
    "BULK_PILOTS_MANIFEST",
    "BULK_TAB_BY_CLASS",
    "EXPORT_ROOT",
    "FAKE_PROVIDER_PREFIXES",
    "LLM_CANARY_MANIFEST",
    "LLM_LINEAGE_MANIFEST",
    "LIVE_SEEDS_TARGET",
    "MANIFESTS_DIR",
    "MONITORS_MANIFEST",
    "MONITORS_MANIFEST_LEGACY",
    "MONITORS_QUALIFICATION",
    "MissingLiveArtifactError",
    "QUALIFICATION_SUMMARY",
    "REPO_ROOT",
    "REQUIRED_MONITOR_COUNT",
    "SOURCES_DIR",
    "VAR_LIVE",
    "assert_provider_is_live",
    "claimed_bulk_counts",
    "claims_live_llm_success",
    "configured_monitor_ids",
    "find_export_row_by_identity",
    "latest_live_export_dir",
    "live_seed_progress",
    "load_acquisition_manifest",
    "load_blocker",
    "load_export_manifest",
    "load_json",
    "load_llm_lineage",
    "load_monitors_manifest",
    "monitors_qualified_count",
    "paid_spend_cap_from_live_artifacts",
    "provider_name_from_lineage",
    "qualified_monitor_entries",
    "recompute_unique_bulk_counts",
    "require_path",
    "resolve_monitors_manifest_path",
    "source_yaml_status",
    "tab_csv_path",
    "try_load_json",
    "unmet_gate_shortfalls",
]
