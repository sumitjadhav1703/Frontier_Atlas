"""Demo: one grounded LLM extraction through the live fallback chain.

Usage (live keys are read from .env at runtime; nothing secret is printed):
    .venv/bin/python scripts/demo_llm_extract.py                 # Gemini -> Groq -> Meta
    .venv/bin/python scripts/demo_llm_extract.py --skip gemini   # force a fallback to Groq
    .venv/bin/python scripts/demo_llm_extract.py --skip gemini --skip groq   # Meta tier

The page below is a fixed demo snippet. A value is ACCEPTED only when its quote sits
inside a retained segment of this page, so a fabricated answer is rejected.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os

os.environ.setdefault("LITELLM_MODE", "PRODUCTION")
os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")

from dotenv import load_dotenv  # noqa: E402

KEY_ENV = {"gemini": "GEMINI_API_KEY", "groq": "GROQ_API_KEY", "meta": "MODEL_API_KEY"}

HTML = """<html><head><title>Acme Notes - AI meeting notes</title></head><body>
<nav>Home Pricing Blog</nav><main><h1>Acme Notes</h1>
<p>Acme Notes is an AI assistant that writes meeting summaries.</p>
<section><h2>Pricing</h2><p>Acme Notes Pro costs $12 per month. A free plan is available.</p></section>
<p>Acme Notes is built by Acme Labs Inc.</p></main><footer>(c) 2026</footer></body></html>"""


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip", action="append", default=[], choices=sorted(KEY_ENV))
    args = parser.parse_args()
    load_dotenv(".env", override=False)
    from signal_ingest.llm import ExtractCandidate, extract
    from signal_ingest.llm.providers_config import build_chain

    env = {k: v for k, v in os.environ.items() if k not in {KEY_ENV[s] for s in args.skip}}
    chain = build_chain(env=env)
    print("tiers in order:", [t.name for t in chain.tiers])
    result = await extract(
        ExtractCandidate(
            candidate_id="demo-acme-notes",
            record_type="PRODUCT",
            html=HTML,
            target_entity="Acme Notes",
            fields_requested=("owner", "pricingModel"),
        ),
        chain,
    )
    print("status:", result.status.value, "| usage:", result.usage)
    for f in result.fields.values():
        print(f"  {f.field:<14} {f.decision.value:<9} value={f.value!r:<18} quote={f.quote!r} reason={f.reason}")
    attempts = [a.__dict__ for a in getattr(chain, "last_attempts", [])]
    print("dispatches:", json.dumps(attempts, default=str))


if __name__ == "__main__":
    asyncio.run(main())
