#!/usr/bin/env python3
"""Run a multi-model check asking for NAICS/SIC codes for sample companies."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import random
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEFAULT_API_KEY = None
DEFAULT_TARGET_FILE = Path("job_results/jobs_20250930_130131.csv")
CSV_PATH_ENV_VAR = "NAICS_COMPANY_CSV"
DEFAULT_TARGET_COLUMN = "company"
DEFAULT_MODELS = [
    "gpt-4.1-mini"
]
MAX_OUTPUT_TOKENS = 240
NOTES_MAX_CHARS = 180
OPENAI_CHAT_COMPLETIONS_ENDPOINT = "https://api.openai.com/v1/chat/completions"
SYSTEM_PROMPT = (
    "Answer in JSON with keys: company, has_known_code, code_type, codes, confidence, notes. "
    "If unsure: has_known_code=false, code_type=\"unknown\", codes=[], confidence=\"low\", notes explain briefly. "
    "When multiple SIC codes exist choose the most specific one. If NAICS and SIC exist set code_type=\"both\" and list codes with NAICS first, SIC second. "
    "Keep notes concise (<=180 characters). No extra text outside JSON."
)
USER_PROMPT_TEMPLATE = (
    "Company: {company}\n"
    "Is there a known NAICS or SIC code for this company? Reply using the required JSON schema and keep the notes short."
)
REQUEST_HEADERS_TEMPLATE = {
    "Content-Type": "application/json",
}

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logger = logging.getLogger("code-tracker")
logger.setLevel(logging.DEBUG)
_handler = logging.StreamHandler()
_handler.setLevel(logging.DEBUG)
_handler.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s", "%H:%M:%S"))
logger.addHandler(_handler)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_env_file(path: Path | None) -> None:
    if not path:
        return

    env_path = path.expanduser()
    if not env_path.exists():
        logger.debug("Env file %s not found; skipping.", env_path)
        return

    try:
        with env_path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and value and key not in os.environ:
                    os.environ[key] = value
        logger.info("Loaded environment variables from %s", env_path)
    except Exception:  # pragma: no cover - defensive best effort
        logger.exception("Failed to load environment variables from %s", env_path)


def get_api_key() -> str:
    key = os.getenv("OPENAI_API_KEY") or DEFAULT_API_KEY
    if not key:
        raise RuntimeError("OpenAI API key not found in OPENAI_API_KEY env var or defaults.")
    if DEFAULT_API_KEY and key == DEFAULT_API_KEY:
        logger.warning("Using hard-coded API key; consider setting OPENAI_API_KEY instead.")
    return key


def load_companies(path: Path, column: str, limit: int) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    seen = set()
    companies: List[str] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get(column) or "").strip()
            if not name or name.lower() in {"n/a", "unknown"}:
                continue
            if name not in seen:
                seen.add(name)
                companies.append(name)
            if len(companies) >= limit:
                break

    if len(companies) < limit:
        logger.info(
            "Only %d unique companies found in %s (requested %d)",
            len(companies),
            path,
            limit,
        )
    return companies


def build_payload(model: str, company: str, temperature: float) -> Dict[str, Any]:
    user_prompt = USER_PROMPT_TEMPLATE.format(company=company)
    return {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
        "max_tokens": MAX_OUTPUT_TOKENS,
        "response_format": {"type": "json_object"},
    }


def call_model(
    model: str,
    company: str,
    api_key: str,
    temperature: float = 0.2,
    retries: int = 3,
    backoff_seconds: float = 2.0,
) -> Dict[str, Any]:
    payload = build_payload(model, company, temperature)
    headers = REQUEST_HEADERS_TEMPLATE | {"Authorization": f"Bearer {api_key}"}

    for attempt in range(1, retries + 1):
        try:
            response = requests.post(
                OPENAI_CHAT_COMPLETIONS_ENDPOINT,
                headers=headers,
                json=payload,
                timeout=60,
            )
            if response.status_code == 429 and attempt < retries:
                wait = backoff_seconds * attempt
                logger.warning("Rate limited by OpenAI (429). Sleeping %.1fs before retrying...", wait)
                time.sleep(wait)
                continue
            response.raise_for_status()
            data = response.json()
            choices = data.get("choices") or []
            if not choices:
                logger.error("No choices returned from model %s for %s: %s", model, company, data)
                raise ValueError("Empty choices in chat completion response")
            combined = (choices[0].get("message") or {}).get("content", "").strip()
            if not combined:
                raise ValueError("No textual content returned from the model.")
            try:
                result = json.loads(combined)
                return _enforce_notes_limit(result)
            except json.JSONDecodeError as exc:
                logger.error("Failed to parse JSON from model %s for %s: %s", model, company, combined)
                raise RuntimeError("Model response was not valid JSON") from exc
        except (requests.RequestException, ValueError) as exc:
            logger.error(
                "Error calling model %s for %s (attempt %d/%d): %s",
                model,
                company,
                attempt,
                retries,
                exc,
            )
            if attempt >= retries:
                raise
            wait = backoff_seconds * attempt
            time.sleep(wait)
    raise RuntimeError("Exceeded retry attempts")


def format_result(result: Dict[str, Any]) -> str:
    try:
        return json.dumps(result, ensure_ascii=False, indent=2)
    except (TypeError, ValueError):
        return str(result)


def _truncate(text: str | None, limit: int = 120) -> str:
    if not text:
        return "-"
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def _codes_to_string(codes: Iterable[str] | None) -> str:
    if not codes:
        return "-"
    # Filter out empty strings
    filtered = [c.strip() for c in codes if c and str(c).strip()]
    return ", ".join(filtered) if filtered else "-"


def _enforce_notes_limit(payload: Dict[str, Any]) -> Dict[str, Any]:
    notes = payload.get("notes")
    if isinstance(notes, str):
        clean = notes.strip()
        if len(clean) > NOTES_MAX_CHARS:
            truncated = clean[: max(1, NOTES_MAX_CHARS - 1)].rstrip()
            payload["notes"] = truncated + "…"
        else:
            payload["notes"] = clean
    return payload


def _normalise_row(
    *,
    company: str,
    model: str,
    payload: Dict[str, Any] | None,
    error: str | None,
) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "company": company,
        "model": model,
        "has_known_code": None,
        "code_type": None,
        "codes": [],
        "confidence": None,
        "notes": None,
        "error": error,
    }
    if payload:
        base.update(
            {
                "has_known_code": payload.get("has_known_code"),
                "code_type": payload.get("code_type"),
                "codes": payload.get("codes", []),
                "confidence": payload.get("confidence"),
                "notes": payload.get("notes"),
            }
        )
    return base


def _print_company_table(company: str, entries: List[Dict[str, Any]]) -> None:
    if not entries:
        print("(no model responses)")
        return

    headers = [
        "model",
        "has_known_code",
        "code_type",
        "codes",
        "confidence",
        "notes",
        "error",
    ]
    table: List[List[str]] = []
    for entry in entries:
        payload = entry.get("payload")
        error = entry.get("error")
        row = [
            entry.get("model", "-"),
            str(payload.get("has_known_code")) if payload else "-",
            payload.get("code_type", "-") if payload else "-",
            _codes_to_string(payload.get("codes")) if payload else "-",
            payload.get("confidence", "-") if payload else "-",
            _truncate(payload.get("notes")) if payload else "-",
            _truncate(error) if error else "-",
        ]
        table.append(row)

    col_widths = [max(len(str(value)) for value in [header] + [row[idx] for row in table]) for idx, header in enumerate(headers)]

    def _format_row(values: List[str]) -> str:
        return " | ".join(value.ljust(col_widths[idx]) for idx, value in enumerate(values))

    print(_format_row(headers))
    print("-+-".join("-" * w for w in col_widths))
    for row in table:
        print(_format_row(row))


def _write_json(path: Path, data: Dict[str, List[Dict[str, Any]]]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "company",
        "model",
        "has_known_code",
        "code_type",
        "codes",
        "confidence",
        "notes",
        "error",
    ]

    with output_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            row_out = row.copy()
            codes = row_out.get("codes")
            if isinstance(codes, Iterable) and not isinstance(codes, (str, bytes)):
                codes_list = [str(c).strip() for c in codes if str(c).strip()]
                row_out["codes"] = "; ".join(codes_list)
            else:
                row_out["codes"] = str(codes) if codes else ""

            for key in ("has_known_code", "code_type", "confidence", "notes", "error"):
                value = row_out.get(key)
                row_out[key] = "" if value in (None, "None") else value

            writer.writerow(row_out)


# ---------------------------------------------------------------------------
# Main routine
# ---------------------------------------------------------------------------

def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a multi-model NAICS/SIC lookup check.")
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help=f"CSV source of companies (default: env {CSV_PATH_ENV_VAR} or {DEFAULT_TARGET_FILE})",
    )
    parser.add_argument("--column", default=DEFAULT_TARGET_COLUMN, help="Column containing company names")
    parser.add_argument("--limit", type=int, default=10, help="Number of distinct companies to sample")
    parser.add_argument(
        "--models",
        nargs="+",
        default=DEFAULT_MODELS,
        help="OpenAI model IDs to query (space-separated)",
    )
    parser.add_argument("--temperature", type=float, default=0.2, help="Sampling temperature for models")
    parser.add_argument(
        "--shuffle",
        action="store_true",
        help="Shuffle the source list before picking companies",
    )
    parser.add_argument(
        "--show-raw",
        action="store_true",
        help="Print raw JSON output from each model in addition to comparison tables",
    )
    parser.add_argument("--save-json", type=Path, help="Optional path to save full results as JSON")
    parser.add_argument("--save-csv", type=Path, help="Optional path to save flattened results as CSV")
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env"),
        help="Optional path to a .env file with environment variables",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    load_env_file(args.env_file)
    api_key = get_api_key()

    csv_path = args.csv
    if csv_path is None:
        env_csv = os.getenv(CSV_PATH_ENV_VAR)
        if env_csv:
            csv_path = Path(env_csv).expanduser()
            logger.debug("Using CSV path from %s: %s", CSV_PATH_ENV_VAR, csv_path)
    if csv_path is None:
        csv_path = DEFAULT_TARGET_FILE
        logger.debug("Using default CSV path: %s", csv_path)

    companies = load_companies(Path(csv_path), args.column, args.limit)
    if args.shuffle:
        random.shuffle(companies)

    logger.info("Evaluating %d companies across %d model(s)", len(companies), len(args.models))

    summary: Dict[str, List[Dict[str, Any]]] = {}
    flattened_rows: List[Dict[str, Any]] = []

    for company in companies:
        print("\n===", company, "===")
        company_entries: List[Dict[str, Any]] = []

        for model in args.models:
            try:
                result = call_model(model, company, api_key, temperature=args.temperature)
                if args.show_raw:
                    print(f"-- {model} (raw) --")
                    print(format_result(result))
                company_entries.append({"model": model, "payload": result, "error": None})
                flattened_rows.append(
                    _normalise_row(company=company, model=model, payload=result, error=None)
                )
            except Exception as exc:  # noqa: BLE001
                error_message = str(exc)
                if args.show_raw:
                    print(f"-- {model} (raw) --")
                    print(f"ERROR: {error_message}")
                logger.exception("Model %s failed for %s", model, company)
                company_entries.append({"model": model, "payload": None, "error": error_message})
                flattened_rows.append(
                    _normalise_row(company=company, model=model, payload=None, error=error_message)
                )

        summary[company] = company_entries
        _print_company_table(company, company_entries)

    if args.save_json:
        _write_json(args.save_json, summary)
        logger.info("Saved JSON results to %s", args.save_json)

    if args.save_csv:
        _write_csv(args.save_csv, flattened_rows)
        logger.info("Saved CSV results to %s", args.save_csv)


if __name__ == "__main__":
    main()