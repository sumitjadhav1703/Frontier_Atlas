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


# ---------------------------------------------------------------------------
# R4 — pgvector candidate suggestions + typed relationships (notes/work-orders/r4.md)
# Offline: FakeEmbedder (hashed bag-of-words, 384 dims). No model download.
# ---------------------------------------------------------------------------

from tests.integration import r4_api  # noqa: E402

R4_MIGRATION = Path(__file__).resolve().parents[2] / "migrations" / "009_r4_vector.sql"
R4_NEW_REL_TYPES = ("FOUNDED_BY", "AUTHORED", "IMPLEMENTED_BY", "OWNS", "MENTIONS", "HIRING")


@pytest.fixture
def r4_db(tmp_fixture_env, frozen_clock):
    """Fixture-mode DB with migrations applied and an empty entity crosswalk."""
    from signal_ingest.config import Mode, clear_settings_cache, load_settings
    from signal_ingest.storage import connect, init_db

    clear_settings_cache()
    settings = load_settings(mode=Mode.FIXTURE)
    init_db(settings)
    with connect(settings) as conn:
        conn.execute(
            "TRUNCATE resolution_events, source_identities, entities RESTART IDENTITY CASCADE"
        )
        conn.commit()
    return settings


def _r4_insert_entity(conn, eid: str, name: str) -> None:
    from signal_ingest.resolution import comparison_key

    conn.execute(
        """
        INSERT INTO entities (id, namespace, canonical_name, lineage_mode, comparison_key)
        VALUES (%s, 'startup', %s, 'fixture', %s)
        """,
        (eid, name, comparison_key(name)),
    )


R4_ENTITIES = (
    ("fixture.r4.zephyr", "Zephyr Quantum Robotics"),
    ("fixture.r4.zephyr-labs", "Zephyr Quantum Robotics Labs"),
    ("fixture.r4.bluefin", "Bluefin Bakery"),
)


def _r4_seed_entities(settings, embedder=None):
    from signal_ingest.storage import connect

    with connect(settings) as conn:
        for eid, name in R4_ENTITIES:
            _r4_insert_entity(conn, eid, name)
        if embedder is not None:
            r4_api.embed_entities(conn, embedder)
        conn.commit()


def test_r4_migration_009_applies_twice_with_vector_column_and_hnsw(r4_db):
    """009 is idempotent through init_db and as a raw re-run; vector + HNSW exist.

    Detects: missing migration, non-idempotent DDL, wrong dimension, missing index.
    """
    from signal_ingest.storage import connect, init_db
    from signal_ingest.storage.db import _run_sql_script

    assert R4_MIGRATION.is_file(), "migrations/009_r4_vector.sql missing"
    assert "009_r4_vector.sql" not in init_db(r4_db)  # second apply via runner: no-op
    with connect(r4_db) as conn:
        _run_sql_script(conn, R4_MIGRATION.read_text(encoding="utf-8"))  # raw re-run
        _run_sql_script(conn, R4_MIGRATION.read_text(encoding="utf-8"))
        conn.commit()
        assert conn.execute(
            "SELECT 1 FROM schema_migrations WHERE version = '009_r4_vector.sql'"
        ).fetchone()
        assert conn.execute("SELECT 1 FROM pg_extension WHERE extname = 'vector'").fetchone()
        cols = dict(
            conn.execute(
                """
                SELECT a.attname, format_type(a.atttypid, a.atttypmod)
                FROM pg_attribute a
                WHERE a.attrelid = 'entities'::regclass AND a.attnum > 0 AND NOT a.attisdropped
                """
            ).fetchall()
        )
        assert cols.get("embedding") == "vector(384)", cols
        assert cols.get("embedding_model") == "text", cols
        idx = [
            r[0]
            for r in conn.execute(
                "SELECT indexdef FROM pg_indexes WHERE tablename = 'entities'"
            ).fetchall()
        ]
        assert any(
            "hnsw" in d.lower() and "vector_cosine_ops" in d and "embedding" in d for d in idx
        ), idx
        rel = dict(
            conn.execute(
                """
                SELECT a.attname, format_type(a.atttypid, a.atttypmod)
                FROM pg_attribute a
                WHERE a.attrelid = 'relationships'::regclass AND a.attnum > 0
                  AND NOT a.attisdropped
                """
            ).fetchall()
        )
        assert rel.get("valid_from") == "timestamp with time zone", rel
        assert rel.get("valid_to") == "timestamp with time zone", rel
        assert "evidence_observation_id" in rel, rel
        nullable = conn.execute(
            """
            SELECT is_nullable FROM information_schema.columns
            WHERE table_name = 'relationships' AND column_name = 'evidence_observation_id'
            """
        ).fetchone()
        assert nullable and nullable[0] == "YES"


def test_r4_relationship_type_check_accepts_typed_edges_rejects_unknown(r4_db):
    """CHECK admits the existing + R4 relation types and rejects an unknown one.

    Detects: missing new types (FOUNDED_BY/AUTHORED/MENTIONS/HIRING) or no CHECK at all.
    """
    import psycopg
    from uuid import uuid4

    from signal_ingest.storage import connect

    sql = """
        INSERT INTO relationships (id, relation_type, from_kind, from_key, to_kind, to_key,
                                   valid_from, valid_to)
        VALUES (%s, %s, 'organization', %s, 'person', %s, NOW(), NULL)
    """
    with connect(r4_db) as conn:
        try:
            for rtype in R4_NEW_REL_TYPES + ("ALIASED_AS", "SAME_AS"):
                conn.execute(sql, (uuid4(), rtype, f"r4-check-{rtype}", f"r4-check-{rtype}-to"))
            with pytest.raises(psycopg.errors.CheckViolation):
                with conn.transaction():
                    conn.execute(sql, (uuid4(), "FRIENDS_WITH", "r4-check-x", "r4-check-y"))
        finally:
            conn.rollback()  # leave no edges behind in the shared fixture DB


def test_r4_embed_entities_fills_null_and_reembeds_on_model_change(r4_db):
    """embed_entities covers NULL embeddings, skips current ones, redoes stale models.

    Detects: re-embedding everything every run, or ignoring embedding_model drift.
    """
    from signal_ingest.storage import connect

    first = r4_api.FakeEmbedder()
    _r4_seed_entities(r4_db, first)
    assert len(first.calls) == len(R4_ENTITIES)
    assert any("Zephyr Quantum Robotics" in t for t in first.calls)

    with connect(r4_db) as conn:
        rows = conn.execute(
            "SELECT embedding IS NOT NULL, embedding_model FROM entities WHERE id LIKE 'fixture.r4.%%'"
        ).fetchall()
        assert rows and all(r[0] and r[1] == first.model_id for r in rows), rows

        again = r4_api.FakeEmbedder()
        r4_api.embed_entities(conn, again)
        assert again.calls == [], "current embeddings must not be recomputed"

        changed = r4_api.FakeEmbedder(model_id="fake-hash-bow-384-v2")
        r4_api.embed_entities(conn, changed)
        conn.commit()
        assert len(changed.calls) == len(R4_ENTITIES)
        models = {
            r[0]
            for r in conn.execute(
                "SELECT embedding_model FROM entities WHERE id LIKE 'fixture.r4.%%'"
            ).fetchall()
        }
        assert models == {"fake-hash-bow-384-v2"}


def test_r4_suggest_candidates_nearest_entity_and_excludes_self(r4_db):
    """HNSW cosine query returns the nearest entity first and honors exclude/threshold.

    Detects: wrong distance operator (L2/inner), missing self-exclusion, no threshold.
    """
    from signal_ingest.storage import connect

    fake = r4_api.FakeEmbedder()
    _r4_seed_entities(r4_db, fake)
    with connect(r4_db) as conn:
        loose = r4_api.suggest_candidates(
            conn, "Zephyr Quantum Robotics", fake, k=3, min_similarity=0.0
        )
        assert loose, "no suggestions returned"
        assert set(loose[0]) >= {"entity_id", "canonical_name", "similarity"}
        assert loose[0]["entity_id"] == "fixture.r4.zephyr"
        assert loose[0]["canonical_name"] == "Zephyr Quantum Robotics"
        assert float(loose[0]["similarity"]) == pytest.approx(1.0, abs=1e-4)
        sims = [float(s["similarity"]) for s in loose]
        assert sims == sorted(sims, reverse=True)

        # Default threshold 0.85 keeps the 0.866 neighbour and drops the bakery.
        default = r4_api.suggest_candidates(conn, "Zephyr Quantum Robotics", fake)
        ids = [s["entity_id"] for s in default]
        assert "fixture.r4.bluefin" not in ids
        assert ids[:2] == ["fixture.r4.zephyr", "fixture.r4.zephyr-labs"]

        no_self = r4_api.suggest_candidates(
            conn, "Zephyr Quantum Robotics", fake, exclude_entity_id="fixture.r4.zephyr"
        )
        assert [s["entity_id"] for s in no_self] == ["fixture.r4.zephyr-labs"]
        assert float(no_self[0]["similarity"]) == pytest.approx(
            r4_api.cosine("Zephyr Quantum Robotics", "Zephyr Quantum Robotics Labs"), abs=1e-3
        )

        assert len(r4_api.suggest_candidates(conn, "Zephyr Quantum Robotics", fake, k=1)) == 1
        assert r4_api.suggest_candidates(conn, "Completely Unrelated Words", fake) == []


def test_r4_suggest_candidates_empty_without_embeddings(r4_db):
    """No embeddings computed → no suggestions (fixture exports stay byte-stable)."""
    from signal_ingest.storage import connect

    _r4_seed_entities(r4_db, embedder=None)
    with connect(r4_db) as conn:
        out = r4_api.suggest_candidates(
            conn, "Zephyr Quantum Robotics", r4_api.FakeEmbedder(), min_similarity=0.0
        )
        assert out == []


def _r4_decision_table(reg) -> list[tuple]:
    rows: list[tuple] = []
    for p in _load_json("positives.json")["positives"]:
        cand = b6_api.make_candidate(**{k: v for k, v in p.items() if k not in {"expected_id", "expected_name"}})
        r = b6_api.resolve(cand, register=reg)
        rows.append(("pos", p["raw_name"], _dec(r), _cid(r), r.method))
    for n in _load_json("holdout_negatives.json")["negatives"]:
        for side in ("left", "right"):
            r = b6_api.resolve(b6_api.make_candidate(**n[side]), register=reg)
            rows.append(("neg", n["id"], side, _dec(r), _cid(r), r.method))
    for case in _load_json("cases.json")["cases"]:
        for inp in case["inputs"]:
            payload = dict(inp)
            ns = payload.pop("namespace", "startup")
            raw = payload.pop("raw_name")
            r = _resolve(raw, namespace=ns, register=reg, **payload)
            rows.append(("case", case["id"], raw, _dec(r), _cid(r), r.method))
    return rows


def _r4_metrics(reg) -> dict[str, Any]:
    positives = _load_json("positives.json")["positives"]
    holdout = _load_json("holdout_negatives.json")["negatives"]
    pairs = [{"kind": "positive", **p} for p in positives] + [
        {"kind": "negative", "left": n["left"], "right": n["right"], "id": n["id"]}
        for n in holdout
    ]
    return b6_api.evaluate_metrics(pairs, register=reg)


def test_r4_embeddings_never_change_resolution_decisions(r4_db):
    """B6 decisions and TP/FP/FN are identical with and without embeddings (FP=0).

    Detects: embedding similarity leaking into merge decisions (auto-merge).
    """
    from signal_ingest.resolution import persist_seed_register
    from signal_ingest.storage import connect

    reg = _register()
    before_table = _r4_decision_table(reg)
    before = _r4_metrics(reg)
    assert before["fp"] == 0

    fake = r4_api.FakeEmbedder()
    with connect(r4_db) as conn:
        persist_seed_register(conn, reg)
        r4_api.embed_entities(conn, fake)
        conn.commit()
        n = conn.execute("SELECT COUNT(*) FROM entities WHERE embedding IS NOT NULL").fetchone()
        assert int(n[0]) >= 50, "with-embeddings arm must actually embed the seed register"
        # Suggestions exist for a near-miss name, yet resolution still abstains on it.
        sugg = r4_api.suggest_candidates(conn, "Inflection", fake, min_similarity=0.0)
        assert sugg, "embedding arm produced no suggestions"

    reg_after = _register()
    after_table = _r4_decision_table(reg_after)
    after = _r4_metrics(reg_after)
    assert after_table == before_table
    for key in ("tp", "fp", "fn", "precision", "recall", "abstention", "sample_size"):
        assert after[key] == before[key], key
    assert after["fp"] == 0
    assert _dec(_resolve("Inflection", register=reg_after)) == "abstain"


def test_r4_mapping_log_suggested_candidates_only_with_embeddings(r4_db):
    """Mapping rows carry suggested_candidates once embeddings exist; empty otherwise.

    Abstained mentions list above-threshold neighbours; resolved ones list only
    *other* entities. Decisions and canonical IDs are never rewritten.
    Detects: suggestion auto-merging, self-suggestion, or non-empty column without embeddings.
    """
    from datetime import datetime, timezone

    from signal_ingest.export.project import _mapping_rows
    from signal_ingest.resolution import record_resolution_event
    from signal_ingest.resolution.mapping import annotate_mapping_suggestions
    from signal_ingest.storage import connect, create_run

    reg = _register()
    abstain = _resolve("Zephyr Quantum Robotics Labs", register=reg)
    matched = _resolve("OpenAI", register=reg)
    lonely = _resolve("DeepMind", register=reg)
    assert _dec(abstain) == "abstain" and _dec(matched) == "match" and _dec(lonely) == "match"

    as_of = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
    fake = r4_api.FakeEmbedder()

    def rows(conn, run_id):
        out = _mapping_rows(conn, run_id=run_id, as_of=as_of, lineage_mode="fixture")
        return {r["rawName"]: r for r in out}

    def decisions(conn):
        return conn.execute(
            "SELECT raw_name, decision, canonical_id, canonical_name FROM resolution_events "
            "ORDER BY raw_name"
        ).fetchall()

    with connect(r4_db) as conn:
        try:
            for eid, name in R4_ENTITIES + (
                ("fixture.startup.openai", "OpenAI"),
                ("fixture.r4.openai-twin", "OpenAI"),
                ("fixture.startup.google-deepmind", "Google DeepMind"),
            ):
                _r4_insert_entity(conn, eid, name)
            run_id = create_run(conn, settings=r4_db)
            for res in (abstain, matched, lonely):
                record_resolution_event(conn, res, run_id=run_id)
            conn.commit()

            # No embeddings yet: annotate writes nothing, rows stay empty.
            assert annotate_mapping_suggestions(conn, fake, run_id=run_id) == 0
            before = decisions(conn)
            plain = rows(conn, run_id)
            assert set(plain) == {"Zephyr Quantum Robotics Labs", "OpenAI", "DeepMind"}
            assert all(not r.get("suggested_candidates") for r in plain.values()), plain

            r4_api.embed_entities(conn, fake)
            assert annotate_mapping_suggestions(conn, fake, run_id=run_id) == 2
            conn.commit()
            after = decisions(conn)
            got = rows(conn, run_id)
        finally:
            conn.rollback()
            conn.execute("TRUNCATE resolution_events, source_identities, entities CASCADE")
            conn.commit()

    assert after == before, "suggestions must never change a mapping decision"

    z = got["Zephyr Quantum Robotics Labs"]
    assert z["decision"] == "abstain" and z["canonicalID"] is None
    sugg = z["suggested_candidates"]
    assert "Zephyr Quantum Robotics Labs (1.00)" in sugg, sugg
    assert "Zephyr Quantum Robotics (0.87)" in sugg, sugg
    assert "Bluefin" not in sugg

    o = got["OpenAI"]
    assert o["decision"] == "match" and o["canonicalID"] == "fixture.startup.openai"
    # Self (canonical id) excluded; the same-named twin is the one other candidate.
    assert o["suggested_candidates"].count("OpenAI (") == 1, o["suggested_candidates"]

    d = got["DeepMind"]
    assert d["decision"] == "match" and d["canonicalID"] == "fixture.startup.google-deepmind"
    assert not d.get("suggested_candidates"), "no other candidate above threshold → empty"


def test_r4_recursive_cte_papers_by_yc_startups_multi_hop(r4_db):
    """paper -[IMPLEMENTED_BY]-> repo <-[OWNS]- ... <-[OWNS]- YC startup, incl. 2-hop chain.

    Detects: single-hop-only CTE, non-YC owners leaking in, expired edges counted,
    or an OWNS cycle hanging the recursion.
    """
    from uuid import uuid4

    from signal_ingest.resolution import comparison_key
    from signal_ingest.resolution.graph_queries import papers_by_yc_startups
    from signal_ingest.storage import connect

    yc, other = "fixture.r4.ycstart", "fixture.r4.nonyc"
    edges = [
        # 1-hop: P1 -> R1 <- YC
        ("IMPLEMENTED_BY", "paper", "r4g-p1", "repository", "r4g-r1", None),
        ("OWNS", "organization", yc, "repository", "r4g-r1", None),
        # 2-hop: P2 -> R2 <- Sub <- YC (plus a Sub -> YC cycle)
        ("IMPLEMENTED_BY", "paper", "r4g-p2", "repository", "r4g-r2", None),
        ("OWNS", "organization", "r4g-sub", "repository", "r4g-r2", None),
        ("OWNS", "organization", yc, "organization", "r4g-sub", None),
        ("OWNS", "organization", "r4g-sub", "organization", yc, None),
        # non-YC owner, direct and 2-hop
        ("IMPLEMENTED_BY", "paper", "r4g-p3", "repository", "r4g-r3", None),
        ("OWNS", "organization", other, "repository", "r4g-r3", None),
        ("IMPLEMENTED_BY", "paper", "r4g-p4", "repository", "r4g-r4", None),
        ("OWNS", "organization", "r4g-sub2", "repository", "r4g-r4", None),
        ("OWNS", "organization", other, "organization", "r4g-sub2", None),
        # YC ownership that already ended
        ("IMPLEMENTED_BY", "paper", "r4g-p5", "repository", "r4g-r5", None),
        ("OWNS", "organization", yc, "repository", "r4g-r5", "2020-01-01T00:00:00Z"),
    ]
    with connect(r4_db) as conn:
        try:
            for eid, name, sid in ((yc, "Zqx YC Robotics", "yc:r4-zqx"),
                                   (other, "Zqx Other Robotics", "directory:r4-zqx-other")):
                _r4_insert_entity(conn, eid, name)
                conn.execute(
                    """
                    INSERT INTO source_identities (id, entity_id, namespace, identity_kind,
                        raw_value, comparison_key, lineage_mode)
                    VALUES (%s, %s, 'startup', 'source_id', %s, %s, 'fixture')
                    """,
                    (uuid4(), eid, sid, sid),
                )
            for rtype, fk, fkey, tk, tkey, valid_to in edges:
                conn.execute(
                    """
                    INSERT INTO relationships (id, relation_type, from_kind, from_key,
                        to_kind, to_key, valid_from, valid_to)
                    VALUES (%s, %s, %s, %s, %s, %s, '2024-01-01T00:00:00Z', %s)
                    """,
                    (uuid4(), rtype, fk, fkey, tk, tkey, valid_to),
                )
            assert comparison_key("Zqx YC Robotics")
            rows = [r for r in papers_by_yc_startups(conn) if str(r["paper_key"]).startswith("r4g-")]
        finally:
            conn.rollback()  # nothing committed: shared fixture DB left untouched

    got = {r["paper_key"]: r for r in rows}
    assert set(got) == {"r4g-p1", "r4g-p2"}, rows
    assert got["r4g-p1"]["startup_entity_id"] == yc and got["r4g-p1"]["hops"] == 1
    assert got["r4g-p1"]["repository_key"] == "r4g-r1"
    assert got["r4g-p2"]["startup_entity_id"] == yc and got["r4g-p2"]["hops"] == 2
    assert got["r4g-p2"]["repository_key"] == "r4g-r2"
    assert got["r4g-p2"]["startup_name"] == "Zqx YC Robotics"


def test_r4_resolver_cannot_read_embeddings():
    """Structural guard: resolution decisions never import the embedding module."""
    from pathlib import Path

    src = Path(__file__).resolve().parents[2] / "src" / "signal_ingest" / "resolution"
    for name in ("resolve.py", "register.py", "normalize.py"):
        text = (src / name).read_text(encoding="utf-8")
        assert "embed" not in text.replace("embedded", ""), name


# ---------------------------------------------------------------------------
# B9: live lab seeds from a run + run-level resolution
# ---------------------------------------------------------------------------


class _B9Conn:
    """connect() stand-in returning canned grouped bulk.openrouter rows (no DB)."""

    def __init__(self, rows):
        self.rows = rows
        self.sql: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.sql.append(sql)
        rows = self.rows

        class _Cur:
            def fetchall(self_inner):
                return list(rows)

        return _Cur()


def test_b9_known_lab_seeds_from_run_idempotent_no_aliases(tmp_fixture_env, tmp_path, monkeypatch):
    import shutil

    import signal_ingest.storage.db as storage_db
    from signal_ingest.config import Mode, load_settings
    from signal_ingest.live.seeds import (
        DEFAULT_LIVE_SEEDS_PATH,
        build_known_lab_seeds_from_run,
    )

    monkeypatch.setenv("SIGNAL_INGEST_EXPORT_ROOT_LIVE", str(tmp_path / "exports_live"))
    settings = load_settings(mode=Mode.LIVE)
    register = tmp_path / "seeds.live.json"
    shutil.copy(DEFAULT_LIVE_SEEDS_PATH, register)
    base = len(json.loads(register.read_text(encoding="utf-8"))["identities"])
    # (owner_key, owner_name, n), pre-sorted like the SQL: key, n desc, name.
    canned = [
        ("qzx-synthlab", "Qzx Synthlab", 3),
        ("qzx-synthlab", "Qzx Synthlab Inc", 1),
        ("vrb-fixturelab", "Vrb Fixturelab", 2),
    ]
    monkeypatch.setattr(storage_db, "connect", lambda _settings: _B9Conn(canned))

    counts = []
    for _ in range(2):
        out = build_known_lab_seeds_from_run(settings, "b9-run", path=register)
        assert out == register
        doc = json.loads(register.read_text(encoding="utf-8"))
        counts.append(len(doc["identities"]))
    assert counts[0] == counts[1] == base + 2, (base, counts)
    labs = {i["id"]: i for i in doc["identities"] if i["id"].startswith("live.lab.openrouter.")}
    new = {k: v for k, v in labs.items() if k.endswith(("qzx-synthlab", "vrb-fixturelab"))}
    assert set(new) == {"live.lab.openrouter.qzx-synthlab", "live.lab.openrouter.vrb-fixturelab"}
    for key, seed in new.items():
        owner_key = key.rsplit(".", 1)[1]
        assert seed["aliases"] == []
        assert seed["source_ids"] == {"openrouter": owner_key}
    assert new["live.lab.openrouter.qzx-synthlab"]["canonical_name"] == "Qzx Synthlab"
    # Live artifacts land under the temporary live root, not the repo.
    assert (tmp_path / "seeds" / "seeds.live.json").is_file()


def test_b9_known_lab_seeds_refuse_fixture_mode(tmp_fixture_env, tmp_path):
    from signal_ingest.config import Mode, load_settings
    from signal_ingest.live.seeds import build_known_lab_seeds_from_run
    from signal_ingest.resolution.types import SeedRegisterError

    with pytest.raises(SeedRegisterError):
        build_known_lab_seeds_from_run(
            load_settings(mode=Mode.FIXTURE), "b9-run", path=tmp_path / "seeds.live.json"
        )


@pytest.fixture
def b9_resolution_env(tmp_fixture_env, frozen_clock):
    from signal_ingest.config import Mode, load_settings
    from signal_ingest.storage import connect, init_db

    settings = load_settings(mode=Mode.FIXTURE)
    init_db(settings)
    with connect(settings) as conn:
        conn.execute(
            "TRUNCATE resolution_events, source_identities, entities RESTART IDENTITY CASCADE"
        )
        conn.execute("TRUNCATE quarantines, relationships, repo_metrics, candidates RESTART IDENTITY CASCADE")
        conn.commit()
    return settings


def test_b9_resolve_run_match_match_abstain_idempotent(b9_resolution_env):
    from signal_ingest.adapters.bulk.persist import upsert_candidate
    from signal_ingest.resolution.run_resolution import resolve_run
    from signal_ingest.storage import connect, create_run

    settings = b9_resolution_env
    rows = [
        ("bulk.yc", "STARTUP", "b9-yc-openai",
         {"content": {"entityName": "OpenAI, Inc."}, "meta": {"yc_id": "openai"},
          "source": {"url": "https://www.ycombinator.com/companies/openai"}}),
        ("bulk.products", "PRODUCT", "b9-prod-openai",
         {"content": {"startupName": "Open AI", "productName": "Synth Chat",
                      "ownerEvidenceUrl": "https://openai.example/"}, "meta": {"product_id": "b9-prod-openai"}}),
        ("bulk.products", "PRODUCT", "b9-prod-nobody",
         {"content": {"startupName": "Nobody Corp", "productName": "Synth Void",
                      "ownerEvidenceUrl": "https://nobody.example/"}, "meta": {"product_id": "b9-prod-nobody"}}),
    ]
    with connect(settings) as conn:
        run_id = create_run(conn, settings=settings)
        for source_id, record_type, ext, payload in rows:
            upsert_candidate(conn, source_id=source_id, record_type=record_type, external_id=ext,
                             payload=payload, evidence={"synthetic": True}, status="accepted", run_id=run_id)
        conn.commit()

        first = resolve_run(conn, run_id, mode="fixture")
        conn.commit()
        events = conn.execute(
            "SELECT raw_name, decision, canonical_id, payload FROM resolution_events "
            "WHERE run_id = %s ORDER BY raw_name", (run_id,),
        ).fetchall()
        second = resolve_run(conn, run_id, mode="fixture")
        conn.commit()
        n_after = conn.execute(
            "SELECT COUNT(*) FROM resolution_events WHERE run_id = %s", (run_id,)
        ).fetchone()[0]

    assert first["mentions"] == 3
    assert first["decisions"] == {"abstain": 1, "match": 2}, first
    assert second["decisions"] == first["decisions"]
    by_name = {r[0]: r for r in events}
    assert by_name["OpenAI, Inc."][1] == "match" and by_name["Open AI"][1] == "match"
    assert by_name["OpenAI, Inc."][2] == by_name["Open AI"][2] == "fixture.startup.openai"
    assert by_name["Nobody Corp"][1] == "abstain"
    assert n_after == len(events) == 3, "second run must not add mapping events"
    assert all("suggested_candidates" not in (r[3] or {}) for r in events)
    assert "embedded" not in first and "annotated" not in first


def test_b9_resolve_run_live_mode_rejects_fixture_seeds(b9_resolution_env):
    from signal_ingest.resolution.run_resolution import resolve_run
    from signal_ingest.resolution.seeds import DEFAULT_FIXTURE_SEEDS_PATH
    from signal_ingest.resolution.types import FixtureSeedRejectedError
    from signal_ingest.storage import connect, create_run

    with connect(b9_resolution_env) as conn:
        run_id = create_run(conn, settings=b9_resolution_env)
        conn.commit()
        with pytest.raises(FixtureSeedRejectedError):
            resolve_run(conn, run_id, mode="live", seeds_path=DEFAULT_FIXTURE_SEEDS_PATH)
