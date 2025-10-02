#!/usr/bin/env python3
"""Utility to create JobSpy configuration JSON files from the command line."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Iterable, List, Sequence

DEFAULT_SCHEMA = "./schema.json"
DEFAULT_INHERITS = "./base.json"


def _flatten(values: Sequence[str] | None) -> List[str]:
    result: List[str] = []
    if not values:
        return result
    for value in values:
        segments = [segment.strip() for segment in value.split(",")]
        for segment in segments:
            if segment and segment not in result:
                result.append(segment)
    return result


def _relativize(target: str, base_dir: Path) -> str:
    path = Path(target).expanduser()
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    try:
        rel_path = path.relative_to(base_dir)
        return f"./{rel_path.as_posix()}" if rel_path.parts[0] != "." else rel_path.as_posix()
    except ValueError:
        rel = os.path.relpath(path, base_dir)
        return rel.replace(os.sep, "/")


def build_config(args: argparse.Namespace) -> dict:
    output_path = Path(args.output).with_suffix(".json")
    base_dir = output_path.parent.resolve()

    config: dict = {
        "$schema": _relativize(args.schema, base_dir),
        "inherits": _relativize(args.inherits, base_dir),
    }

    job_search: dict = {}
    if args.sites:
        job_search["sites"] = _flatten(args.sites)
    if args.search_terms:
        job_search["search_terms"] = _flatten(args.search_terms)
    if args.locations:
        job_search["locations"] = _flatten(args.locations)
    if args.results_wanted is not None:
        job_search["results_wanted"] = args.results_wanted
    if args.hours_old is not None:
        job_search["hours_old"] = args.hours_old
    if args.job_types:
        job_search["job_types"] = _flatten(args.job_types)
    if args.experience_levels:
        job_search["experience_levels"] = _flatten(args.experience_levels)

    if job_search:
        config["job_search"] = job_search

    filters: dict = {}
    if args.required_keywords:
        filters["required_keywords"] = _flatten(args.required_keywords)
    if args.exclude_keywords:
        filters["exclude_keywords"] = _flatten(args.exclude_keywords)
    if args.salary_min is not None:
        filters["salary_min"] = args.salary_min
    if args.salary_max is not None:
        filters["salary_max"] = args.salary_max
    if args.remote_only is not None:
        filters["remote_only"] = args.remote_only

    if filters:
        config["filters"] = filters

    if args.hnhiring_enabled or args.hnhiring_categories or args.hnhiring_days is not None:
        hnhiring: dict = {"enabled": bool(args.hnhiring_enabled or args.hnhiring_categories)}
        if args.hnhiring_categories:
            hnhiring["categories"] = _flatten(args.hnhiring_categories)
        if args.hnhiring_days is not None:
            hnhiring["days"] = args.hnhiring_days
        config["hnhiring"] = hnhiring

    if args.builtin_enabled or args.builtin_keywords or args.builtin_max_age_hours is not None:
        builtin: dict = {"enabled": bool(args.builtin_enabled or args.builtin_keywords)}
        if args.builtin_keywords:
            builtin["keywords"] = _flatten(args.builtin_keywords)
        if args.builtin_max_age_hours is not None:
            builtin["max_age_hours"] = args.builtin_max_age_hours
        config["builtin"] = builtin

    return config


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate JobSpy JSON configuration files")
    parser.add_argument("output", help="Destination path for the generated config (without extension)")

    parser.add_argument("--inherits", default=DEFAULT_INHERITS, help="Config file to inherit from")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA, help="Schema path to embed")

    parser.add_argument("--site", dest="sites", action="append", help="Job site to include (may repeat)")
    parser.add_argument("--search-term", dest="search_terms", action="append", help="Search term (may repeat)")
    parser.add_argument("--location", dest="locations", action="append", help="Location (may repeat)")
    parser.add_argument("--results-wanted", type=int, help="Desired number of results per query")
    parser.add_argument("--hours-old", type=int, help="Max age of postings in hours")
    parser.add_argument("--job-type", dest="job_types", action="append", help="Job type filter (repeat)")
    parser.add_argument("--experience-level", dest="experience_levels", action="append", help="Experience level filter (repeat)")

    parser.add_argument("--required-keyword", dest="required_keywords", action="append", help="Required keyword (repeat)")
    parser.add_argument("--exclude-keyword", dest="exclude_keywords", action="append", help="Excluded keyword (repeat)")
    parser.add_argument("--salary-min", type=int, help="Minimum salary filter")
    parser.add_argument("--salary-max", type=int, help="Maximum salary filter")
    remote_group = parser.add_mutually_exclusive_group()
    remote_group.add_argument("--remote-only", dest="remote_only", action="store_true")
    remote_group.add_argument("--allow-onsite", dest="remote_only", action="store_false")
    parser.set_defaults(remote_only=None)

    parser.add_argument("--hnhiring-enabled", action="store_true", help="Enable HN Hiring collector")
    parser.add_argument("--hnhiring-category", dest="hnhiring_categories", action="append", help="HN Hiring category (repeat)")
    parser.add_argument("--hnhiring-days", type=int, help="HN Hiring look-back window in days")

    parser.add_argument("--builtin-enabled", action="store_true", help="Enable BuiltIn collector")
    parser.add_argument("--builtin-keyword", dest="builtin_keywords", action="append", help="BuiltIn keyword (repeat)")
    parser.add_argument("--builtin-max-age-hours", type=int, help="BuiltIn maximum posting age in hours")

    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing config if present")

    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(list(argv) if argv is not None else None)
    output_path = Path(args.output).with_suffix(".json")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists() and not args.overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file: {output_path}")

    config = build_config(args)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(config, handle, indent=2, ensure_ascii=False)
        handle.write("\n")

    print(f"✅ Config written to {output_path}")


if __name__ == "__main__":
    main()
