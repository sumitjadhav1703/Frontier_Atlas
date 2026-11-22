"""Thin monitor adapter for jobs_aiworkportal."""

from __future__ import annotations

from signal_ingest.adapters.monitors.base import make_discover, make_parse

SOURCE_ID = "jobs_aiworkportal"

discover_list = make_discover(SOURCE_ID)
parse_detail = make_parse(SOURCE_ID)
