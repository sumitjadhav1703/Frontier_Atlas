"""Thin monitor adapter for jobs_inferencejobs."""

from __future__ import annotations

from signal_ingest.adapters.monitors.base import make_discover, make_parse

SOURCE_ID = "jobs_inferencejobs"

discover_list = make_discover(SOURCE_ID)
parse_detail = make_parse(SOURCE_ID)
