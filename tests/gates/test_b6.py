"""B6 gate: deterministic startup/product entity resolution.

Oracle: docs/build-plan.md (B6) and docs/contracts.md Entity Mapping Log.
Offline only — fixture seeds, frozen clock, no live provider/network spend.
Cases must FAIL when behavior is missing/wrong and PASS when integrated correctly.
"""

from __future__ import annotations

import json
import random
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from tests.integration import b6_api
from tests.support import FIXTURES

ARTIFACTS = Path(__file__).resolve().parents[2] / "artifacts" / "gates"
RES_FIX = FIXTURES / "resolution"


def _load_json(name: str) -> dict[str, Any]:
    return json.loads((RES_FIX / name).read_text(encoding="utf-8"))


def _register():
    return b6_api.load_seed_register(b6_api.SEEDS_PATH)


def _resolve(
    raw_name: str,
    *,
    namespace: str = "startup",
    mode: str = "fixture",
    register: Any = None,
    **extra: Any,
):
    cand = b6_api.make_candidate(
        raw_name=raw_name,
        namespace=namespace,
        mode=mode,
        **extra,
    )
    return b6_api.resolve(cand, register=register or _register())


def _dec(result: Any) -> str:
    return b6_api.decision_value(result)


def _cid(result: Any) -> str | None:
    return b6_api.canonical_id_of(result)


def _cname(result: Any) -> str | None:
    return b6_api.canonical_name_of(result)


# ---------------------------------------------------------------------------
# Fixture labeling / seed register shape
# ---------------------------------------------------------------------------


def test_resolution_fixtures_labeled_synthetic():
    """Gate fixtures stay labeled synthetic / not for live export."""
    meta = _load_json("meta.json")
    assert meta["label"] == "synthetic"
    assert meta["not_for_live_export"] is True
    seeds = _load_json("seeds.json")
    assert seeds["lineage_mode"] == "fixture"
    for name in ("holdout_negatives.json", "positives.json", "cases.json"):
        data = _load_json(name)
        assert data["_meta"]["label"] == "synthetic"
        assert data["_meta"]["not_for_live_export"] is True


def test_seed_register_has_fifty_labeled_identities():
    """Fixture seed register exposes ≥50 stable IDs with aliases and namespaces.

    Detects: missing seed file, unlabeled identities, mixed namespaces collapsed.
    """
    seeds = _load_json("seeds.json")
    identities = seeds["identities"]
    assert len(identities) >= 50
    ids = [i["id"] for i in identities]
    assert len(ids) == len(set(ids))
    assert {i["namespace"] for i in identities} == {"startup", "product"}
    products = [i for i in identities if i["namespace"] == "product"]
    assert len(products) >= 10
    for p in products:
        assert p.get("owner_id"), f"product {p['id']} missing owner_id"
    reg = _register()
    assert len(reg.by_id) >= 50


# ---------------------------------------------------------------------------
# comparison_key
# ---------------------------------------------------------------------------


def test_comparison_key_strips_legal_suffix_case_punct():
    """OpenAI / OpenAI, Inc. / Open AI share a comparison key after normalize.

    Detects: missing NFKC/casefold/suffix strip or treating punctuation as identity.
    """
    k1 = b6_api.comparison_key("OpenAI")
    k2 = b6_api.comparison_key("OpenAI, Inc.")
    k3 = b6_api.comparison_key("Open AI")
    k4 = b6_api.comparison_key("openai")
    k5 = b6_api.comparison_key("  OpenAI  ")
    assert k1 == k2 == k3 == k4 == k5
    assert k1 == b6_api.comparison_key("OPENAI INC")
    jp = b6_api.comparison_key("楽天AI")
    assert jp
    assert b6_api.comparison_key("楽天AI") == jp


def test_comparison_key_ai_vs_ai_period_collide():
    """AI and A.I. collapse under punct folding — collision surface for abstain.

    Detects: treating punct variants as safely distinct without collision checks.
    """
    assert b6_api.comparison_key("AI") == b6_api.comparison_key("A.I.")


# ---------------------------------------------------------------------------
# OpenAI aliases → same canonical
# ---------------------------------------------------------------------------


def test_openai_aliases_resolve_to_same_canonical():
    """OpenAI / OpenAI, Inc. / Open AI → same canonical OpenAI.

    Detects: alias table gaps or over-strict exact-string matching.
    """
    reg = _register()
    results = [
        _resolve("OpenAI", register=reg),
        _resolve("OpenAI, Inc.", register=reg),
        _resolve("Open AI", register=reg),
    ]
    assert all(_dec(r) == "match" for r in results)
    assert {_cid(r) for r in results} == {"fixture.startup.openai"}
    assert all(_cname(r) == "OpenAI" for r in results)


def test_punctuation_and_case_variants_match_openai():
    """Punctuation/case folding reaches OpenAI without new IDs.

    Detects: case-sensitive register or leftover commas/Inc blocking match.
    """
    reg = _register()
    for raw in ("openai,  inc.", "OPENAI", "OpenAI Inc", "  OpenAI, Inc.  "):
        r = _resolve(raw, register=reg)
        assert _dec(r) == "match", raw
        assert _cid(r) == "fixture.startup.openai", raw


# ---------------------------------------------------------------------------
# Non-Latin
# ---------------------------------------------------------------------------


def test_non_latin_name_resolves():
    """Non-Latin canonical 楽天AI and Latin alias Rakuten AI map together.

    Detects: ASCII-only keys dropping Unicode identities.
    """
    reg = _register()
    a = _resolve("楽天AI", register=reg)
    b = _resolve("Rakuten AI", register=reg)
    assert _dec(a) == "match" and _cid(a) == "fixture.startup.rakuten-ai"
    assert _dec(b) == "match" and _cid(b) == "fixture.startup.rakuten-ai"


# ---------------------------------------------------------------------------
# Shared hosts / homonyms / AI vs A.I. / subsidiary / products / ambiguous
# ---------------------------------------------------------------------------


def test_shared_host_without_ownership_abstains():
    """Shared host alone cannot merge unknown tenants.

    Detects: domain-only auto-merge (planner weakest assumption).
    """
    r = _resolve("Mystery Tenant", domain="shared-host.example")
    assert _dec(r) == "abstain"
    assert _cid(r) is None


def test_homonym_nexus_entities_stay_distinct():
    """Nexus Alpha vs Nexus Analytics do not share a canonical id.

    Detects: common-name collapse across distinct seeds.
    """
    reg = _register()
    a = _resolve(
        "Nexus",
        domain="nexus-alpha.example",
        ownership_evidenced=True,
        register=reg,
    )
    b = _resolve(
        "Nexus Analytics",
        domain="nexus-analytics.example",
        ownership_evidenced=True,
        register=reg,
    )
    assert not (
        _dec(a) == "match"
        and _dec(b) == "match"
        and _cid(a) is not None
        and _cid(a) == _cid(b)
    )
    if _dec(a) == "match":
        assert _cid(a) == "fixture.startup.nexus-alpha"
    if _dec(b) == "match":
        assert _cid(b) == "fixture.startup.nexus-analytics"


def test_ai_vs_ai_period_ambiguous_abstains():
    """Bare AI / A.I. collide after normalize → abstain (no false merge).

    Detects: acronym punct-fold auto-accept without corroboration.
    """
    reg = _register()
    a = _resolve("AI", register=reg)
    b = _resolve("A.I.", register=reg)
    assert _dec(a) == "abstain"
    assert _dec(b) == "abstain"
    assert _cid(a) is None
    assert _cid(b) is None


def test_subsidiary_does_not_merge_to_parent():
    """DeepMind resolves to itself, never Google parent.

    Detects: subsidiary/parent false merge via shared corporate language.
    """
    reg = _register()
    r = _resolve("DeepMind", register=reg)
    assert _dec(r) == "match"
    assert _cid(r) == "fixture.startup.google-deepmind"
    assert _cid(r) != "fixture.startup.google"
    g = _resolve("Google", register=reg)
    assert _cid(g) != _cid(r)
    if _dec(g) == "match":
        assert _cid(g) == "fixture.startup.google"


def test_same_product_name_different_owners_distinct():
    """Studio under OpenAI ≠ Studio under Hugging Face.

    Detects: product aliases leaking across owners / missing owner scope.
    """
    reg = _register()
    oai = _resolve(
        "Studio",
        namespace="product",
        owner_id="fixture.startup.openai",
        register=reg,
    )
    hf = _resolve(
        "Studio",
        namespace="product",
        owner_id="fixture.startup.huggingface",
        register=reg,
    )
    assert _dec(oai) == "match" and _dec(hf) == "match"
    assert _cid(oai) == "fixture.product.studio-openai"
    assert _cid(hf) == "fixture.product.studio-huggingface"
    assert _cid(oai) != _cid(hf)


def test_ambiguous_alias_abstains():
    """Ambiguous short alias AI without corroboration abstains.

    Detects: unique-alias short-circuit when multiple seeds collide on key.
    """
    assert _dec(_resolve("AI")) == "abstain"


# ---------------------------------------------------------------------------
# Unknown orgs / replays / seed-order
# ---------------------------------------------------------------------------


def test_unknown_organization_creates_new_stable_id():
    """Trusted previously unseen org → decision=new with stable source-backed id.

    Detects: forcing unknown into nearest seed or unstable random ids on replay.
    """
    reg = _register()
    kwargs = {
        "domain": "brandnewunseencoxyz9182.example",
        "evidence_url": "https://brandnewunseencoxyz9182.example/",
        "source_id": "dir:brandnew-9182",
        "allow_create": True,
        "ownership_evidenced": True,
    }
    a = _resolve("BrandNewUnseenCoXYZ-9182", register=reg, **kwargs)
    b = _resolve("BrandNewUnseenCoXYZ-9182", register=reg, **kwargs)
    assert _dec(a) == "new"
    assert _cid(a) is not None
    assert not str(_cid(a)).startswith("fixture.startup.openai")
    assert _cid(a) == _cid(b)


def test_resolve_replay_is_idempotent():
    """Replaying the same raw→canonical observation yields identical decision/id.

    Detects: non-idempotent mapping log or mutating seed state on resolve.
    """
    reg = _register()
    first = [_resolve(x, register=reg) for x in ("OpenAI", "OpenAI, Inc.", "Open AI")]
    second = [_resolve(x, register=reg) for x in ("OpenAI", "OpenAI, Inc.", "Open AI")]
    for a, b in zip(first, second, strict=True):
        assert _dec(a) == _dec(b)
        assert _cid(a) == _cid(b)
        assert _cname(a) == _cname(b)


def test_seed_order_does_not_change_canonical_ids(tmp_path: Path):
    """Shuffling seed register order does not change resolved canonical IDs.

    Detects: first-wins race on colliding keys / order-dependent indexing.
    """
    seeds = _load_json("seeds.json")
    shuffled = deepcopy(seeds)
    rng = random.Random(42)
    rng.shuffle(shuffled["identities"])
    path_a = tmp_path / "seeds_a.json"
    path_b = tmp_path / "seeds_b.json"
    path_a.write_text(json.dumps(seeds), encoding="utf-8")
    path_b.write_text(json.dumps(shuffled), encoding="utf-8")
    reg_a = b6_api.load_seed_register(path_a)
    reg_b = b6_api.load_seed_register(path_b)
    samples = [
        ("OpenAI", "startup", None),
        ("Chat GPT", "product", "fixture.startup.openai"),
        ("DeepMind", "startup", None),
        ("楽天AI", "startup", None),
        ("Studio", "product", "fixture.startup.huggingface"),
    ]
    for raw, ns, owner in samples:
        extra = {"owner_id": owner} if owner else {}
        ra = _resolve(raw, namespace=ns, register=reg_a, **extra)
        rb = _resolve(raw, namespace=ns, register=reg_b, **extra)
        assert _dec(ra) == _dec(rb), raw
        assert _cid(ra) == _cid(rb), raw


# ---------------------------------------------------------------------------
# Holdout negatives — zero false merges
# ---------------------------------------------------------------------------


def test_holdout_negatives_zero_false_merges():
    """Curated hard negatives independent of seed aliases → zero false merges.

    Detects: over-eager comparison-key auto-accept (planner weakest assumption).
    """
    holdout = _load_json("holdout_negatives.json")
    reg = _register()
    false_merges: list[str] = []
    for pair in holdout["negatives"]:
        left = b6_api.make_candidate(**pair["left"])
        right = b6_api.make_candidate(**pair["right"])
        r1 = b6_api.resolve(left, register=reg)
        r2 = b6_api.resolve(right, register=reg)
        id1, id2 = _cid(r1), _cid(r2)
        if (
            pair.get("must_not_share_canonical", True)
            and id1 is not None
            and id1 == id2
            and _dec(r1) == "match"
            and _dec(r2) == "match"
        ):
            false_merges.append(pair["id"])
    assert false_merges == [], f"false merges on holdout: {false_merges}"


# ---------------------------------------------------------------------------
# TP / FP / FN report
# ---------------------------------------------------------------------------


def test_tp_fp_fn_report_precision_recall_abstention():
    """Labeled positives + holdout negatives yield TP/FP/FN with zero FP merges.

    Detects: silent metric omission or recall claimed without denominator.
    """
    positives = _load_json("positives.json")["positives"]
    holdout = _load_json("holdout_negatives.json")["negatives"]
    pairs: list[dict[str, Any]] = []
    for p in positives:
        pairs.append({"kind": "positive", **p})
    for n in holdout:
        pairs.append(
            {
                "kind": "negative",
                "left": n["left"],
                "right": n["right"],
                "id": n["id"],
            }
        )

    reg = _register()
    report = b6_api.evaluate_metrics(pairs, register=reg)
    assert report["sample_size"] == len(pairs)
    assert report["fp"] == 0, f"false merges/FP must be 0, got {report}"
    assert report["tp"] >= 1
    assert report["recall_denominator"] == report["tp"] + report["fn"]
    assert report["recall_denominator"] > 0
    assert 0.0 <= report["recall"] <= 1.0
    assert 0.0 <= report["precision"] <= 1.0
    assert "abstention" in report

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    out = ARTIFACTS / "b6_metrics.json"
    out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    assert out.is_file()

    misses = []
    for p in positives:
        extra = {
            k: v
            for k, v in p.items()
            if k in {"owner_id", "domain", "source_id", "evidence_url", "ownership_evidenced"}
        }
        r = _resolve(p["raw_name"], namespace=p["namespace"], register=reg, **extra)
        if not (_dec(r) == "match" and _cid(r) == p["expected_id"]):
            misses.append(
                {
                    "raw": p["raw_name"],
                    "expected": p["expected_id"],
                    "got": _cid(r),
                    "decision": _dec(r),
                }
            )
    assert report["fn"] == len(misses)


# ---------------------------------------------------------------------------
# Mapping log idempotency
# ---------------------------------------------------------------------------


def test_mapping_log_idempotent_event_key():
    """Idempotent observation key = source item + field + raw + ruleVersion.

    Detects: duplicate mapping rows on replay or missing rule version in key.
    """
    key1 = b6_api.mapping_event_key(
        source_item_id="obs-1",
        field="entityName",
        raw_name="OpenAI",
        rule_version="resolution-v1",
    )
    key2 = b6_api.mapping_event_key(
        source_item_id="obs-1",
        field="entityName",
        raw_name="OpenAI",
        rule_version="resolution-v1",
    )
    key3 = b6_api.mapping_event_key(
        source_item_id="obs-1",
        field="entityName",
        raw_name="OpenAI",
        rule_version="resolution-v2",
    )
    assert key1 == key2
    assert key1 != key3


def test_mapping_events_persist_idempotently(tmp_fixture_env, frozen_clock):
    """Persisting the same resolution event twice does not duplicate rows.

    Detects: missing unique constraint / non-idempotent insert on resolution_events.
    """
    from signal_ingest.config import Mode, load_settings
    from signal_ingest.storage import connect, create_run, init_db

    settings = load_settings(mode=Mode.FIXTURE)
    init_db(settings)
    with connect(settings) as conn:
        tables = conn.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name IN ('entities', 'source_identities', 'resolution_events')
            """
        ).fetchall()
        names = {r[0] for r in tables}
        assert "resolution_events" in names, "B6 migration missing resolution_events"
        assert "entities" in names, "B6 migration missing entities"
        conn.execute(
            "TRUNCATE resolution_events, source_identities, entities RESTART IDENTITY CASCADE"
        )
        conn.commit()

    reg = _register()
    result = _resolve(
        "OpenAI",
        register=reg,
        source_item_id="obs-openai-1",
        field="entityName",
        evidence_url="https://openai.com/",
    )
    assert _dec(result) == "match"

    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings)
        b6_api.record_mapping_event(conn, result, run_id=run_id)
        b6_api.record_mapping_event(conn, result, run_id=run_id)
        conn.commit()
        row = conn.execute("SELECT COUNT(*) FROM resolution_events").fetchone()
        assert row is not None
        assert int(row[0]) == 1


# ---------------------------------------------------------------------------
# Live mode rejects fixture seeds
# ---------------------------------------------------------------------------


def test_live_mode_rejects_fixture_seed_ids_and_aliases():
    """mode=live must reject fixture seed IDs and fixture aliases.

    Detects: fixture register leaking into live resolution path.
    """
    reg = _register()
    result = _resolve("OpenAI", mode="live", register=reg)
    dec = _dec(result)
    assert dec == "abstain"
    assert _cid(result) is None
    assert "fixture" in (result.method or "").lower() or "fixture" in (
        result.reason or ""
    ).lower()


def test_live_mode_rejects_when_candidate_carries_fixture_seed_id():
    """Candidate source_id with fixture.* prefix is rejected in live mode."""
    reg = _register()
    result = _resolve(
        "OpenAI",
        mode="live",
        register=reg,
        source_id="fixture.startup.openai",
    )
    assert _dec(result) == "abstain"
    assert _cid(result) is None


# ---------------------------------------------------------------------------
# Scenario table from cases.json
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "case_id",
    [
        "openai_aliases_same",
        "punct_case",
        "non_latin",
        "shared_hosts_abstain_without_ownership",
        "ai_vs_ai_period_collision",
        "subsidiary_not_parent",
        "same_product_different_owners",
        "ambiguous_alias_abstain",
        "unknown_org_new",
    ],
)
def test_named_cases_from_fixture(case_id: str):
    """Table-driven named failure/success cases from fixtures/resolution/cases.json."""
    cases = {c["id"]: c for c in _load_json("cases.json")["cases"]}
    case = cases[case_id]
    reg = _register()
    results = []
    for inp in case["inputs"]:
        payload = dict(inp)
        ns = payload.pop("namespace", "startup")
        raw = payload.pop("raw_name")
        results.append(_resolve(raw, namespace=ns, register=reg, **payload))

    expect = case["expect"]
    if expect.get("same_canonical"):
        ids = {_cid(r) for r in results}
        assert len(ids) == 1
        assert expect["canonical_id"] in ids
    if "decision" in expect:
        for r in results:
            assert _dec(r) == expect["decision"], (_dec(r), case_id, getattr(r, "reason", None))
    if "canonical_id" in expect and not expect.get("distinct_canonical_ids"):
        for r in results:
            if _dec(r) == "match":
                assert _cid(r) == expect["canonical_id"]
    if "not_id" in expect:
        for r in results:
            assert _cid(r) != expect["not_id"]
    if expect.get("distinct_canonical_ids"):
        ids = [_cid(r) for r in results]
        assert all(i is not None for i in ids)
        assert len(set(ids)) == len(ids)


# ---------------------------------------------------------------------------
# R3-B — resolution precision regressions (review findings R3-B)
# ---------------------------------------------------------------------------


def _r3_register(tmp_path: Path, mutate) -> Any:
    seeds = _load_json("seeds.json")
    mutate({i["id"]: i for i in seeds["identities"]})
    path = tmp_path / "seeds_r3.json"
    path.write_text(json.dumps(seeds), encoding="utf-8")
    return b6_api.load_seed_register(path)


def test_r3_descriptive_suffix_stripped_name_needs_corroboration():
    """Bare 'Inflection' (e.g. a genealogy firm) must not merge into 'Inflection AI'.

    Detects: canonical/alias index short-circuiting step 4 corroboration.
    """
    reg = _register()
    bare = _resolve("Inflection", register=reg)
    assert _dec(bare) == "abstain", bare
    assert _cid(bare) is None
    assert _cid(_resolve("Mistral", register=reg)) is None
    full = _resolve("Inflection AI", register=reg)
    assert _dec(full) == "match" and _cid(full) == "fixture.startup.inflection"
    corroborated = _resolve("Inflection", register=reg, domain="https://inflection.ai/")
    assert _dec(corroborated) == "match"
    assert _cid(corroborated) == "fixture.startup.inflection"


def test_r3_unrelated_source_id_is_not_corroboration():
    """A source id that points nowhere near the seed cannot corroborate a name."""
    reg = _register()
    r = _resolve("Inflection", register=reg, source_id="yc:inflection-genealogy")
    assert _cid(r) != "fixture.startup.inflection", r
    assert _dec(r) == "abstain"
    related = _resolve("Inflection", register=reg, source_id="directory:inflection")
    assert _dec(related) == "match" and _cid(related) == "fixture.startup.inflection"


def test_r3_new_ids_keep_source_namespace_and_separators():
    """Distinct raw source ids never collapse into one derived entity id."""
    reg = _register()

    def new_id(raw_name: str, sid: str) -> str:
        r = _resolve(raw_name, register=reg, source_id=sid, allow_create=True)
        assert _dec(r) == "new", r
        return str(_cid(r))

    assert new_id("Zqx Alpha", "github:ab/cd") != new_id("Zqx Beta", "github:abc/d")
    assert new_id("Zqx Gamma", "yc:acme-co") != new_id("Zqx Delta", "yc:acme")
    assert new_id("Zqx Eps", "yc:acme") != new_id("Zqx Eps", "dir:acme")
    assert new_id("Zqx Eps", "yc:acme") == new_id("Zqx Eps", "yc:acme")


def test_r3_seed_without_evidence_gets_no_invented_url(tmp_path: Path):
    """Missing seed evidence stays missing; never an example.invalid placeholder."""

    # OpenAI: distinctive name, so the bare-name match isolates the evidence check.
    def drop(by_id):
        by_id["fixture.startup.openai"].pop("evidence_url", None)
        by_id["fixture.startup.openai"].pop("url", None)

    reg = _r3_register(tmp_path, drop)
    assert "example.invalid" not in (reg.get("fixture.startup.openai").evidence_url or "")
    assert not reg.get("fixture.startup.openai").evidence_url
    r = _resolve("OpenAI", register=reg)
    assert _dec(r) == "match"
    assert not r.evidence_url


def test_r3_comparison_key_folds_accents_and_descriptive_suffixes():
    """AI/Labs strip for comparison only; accents fold; display name unchanged."""
    from signal_ingest.resolution.normalize import descriptive_key as dk

    ck = b6_api.comparison_key
    # Strict key keeps AI so "Open AI" still equals "OpenAI"; loose key strips it.
    assert dk("Mistral AI") == dk("Mistral") == dk("Mistral Labs") == ck("Mistral")
    assert ck("Mistral AI") != ck("Mistral")
    assert dk("AI Corp") == "ai"  # never stripped to empty
    assert ck("Café") == ck("Cafe") == ck("CAFÉ, Inc.")
    assert ck("AI") == ck("A.I.") and ck("AI")  # never empty
    assert ck("ＯｐｅｎＡＩ") == ck("OpenAI")
    reg = _register()
    for raw in ("OpenAI", "OpenAI, Inc.", "Open AI", "ＯｐｅｎＡＩ"):
        r = _resolve(raw, register=reg)
        assert _dec(r) == "match" and _cid(r) == "fixture.startup.openai", raw
        assert _cname(r) == "OpenAI"
    m = _resolve("Mistral AI", register=reg)
    assert _cname(m) == "Mistral AI"


def test_r3_domain_key_port_www_and_platform_hosts(tmp_path: Path):
    """Port/www stripped; platform hosts never let one tenant claim another."""
    from signal_ingest.resolution.normalize import domain_key

    assert domain_key("https://www.OpenAI.com:8443/path?q=1") == "openai.com"
    assert domain_key("openai.com.") == "openai.com"

    def platform(by_id):
        by_id["fixture.startup.groq"]["domains"].append("github.io")
        by_id["fixture.startup.replicate"]["domains"].append("acme.vercel.app")

    reg = _r3_register(tmp_path, platform)
    port = _resolve("Zqx Portal", register=reg, domain="openai.com:8443", ownership_evidenced=True)
    assert _cid(port) == "fixture.startup.openai"
    for host in ("github.io", "https://someone.github.io/", "other.vercel.app"):
        r = _resolve("Zqx Tenant", register=reg, domain=host, ownership_evidenced=True)
        assert _dec(r) != "match", (host, r)
    same = _resolve("Zqx Tenant", register=reg, domain="acme.vercel.app", ownership_evidenced=True)
    assert _cid(same) == "fixture.startup.replicate"


def test_r3_bare_source_ids_do_not_match_across_systems():
    """'mistral' from another system is not the directory id 'mistral'."""
    reg = _register()
    for kwargs in (
        {"source_id": "mistral"},
        {"source_id": "mistral", "source_system": "github"},
        {"source_id": "github:mistral"},
    ):
        r = _resolve("Zqx Wind", register=reg, **kwargs)
        assert _cid(r) != "fixture.startup.mistral", (kwargs, r)
    ok = _resolve("Zqx Wind", register=reg, source_id="mistral", source_system="directory")
    assert _cid(ok) == "fixture.startup.mistral"



def test_r3_plain_word_seed_names_need_corroboration():
    """Seeds named with one ordinary word ("Groq", "Writer") don't match bare text."""
    reg = _register()
    assert _dec(_resolve("Writer", register=reg)) != "match"
    assert _dec(_resolve("Groq", register=reg)) != "match"
    assert _dec(_resolve("OPENAI", register=reg)) == "match"  # distinctive seed spelling
