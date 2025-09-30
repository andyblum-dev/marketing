"""Integration helpers for pulling jobs from hnhiring.com."""

from __future__ import annotations

import logging
from typing import Dict, List, Iterable

import pandas as pd

from hn_filter import HNFilter, HNTransformer


class HNHiringCollector:
    """Fetch and transform jobs from hnhiring.com using configuration settings."""

    def __init__(self, config: Dict):
        self.settings = config.get("hnhiring", {})
        self.enabled: bool = self.settings.get("enabled", False)
        self.filter = HNFilter(parser=None)
        self.transformer = HNTransformer()
        self._empty_df = pd.DataFrame(columns=self.transformer.columns)

    def scrape(self, _search_terms: Iterable[str]) -> pd.DataFrame:
        """Return a DataFrame of jobs that satisfy the configured filters."""
        if not self.enabled:
            logging.debug("HNHiring integration disabled; skipping scrape")
            return self._empty_df.copy()

        categories: List[str] = self.settings.get("categories", ["/locations/remote"])
        if not categories:
            logging.warning("HNHiring categories missing; skipping scrape")
            return self._empty_df.copy()

        days = self.settings.get("days")
        min_salary = self.settings.get("min_salary")
        max_salary = self.settings.get("max_salary")

        logging.info(
            "Fetching HN Hiring jobs for categories %s (days=%s, min_salary=%s, max_salary=%s)",
            categories,
            days,
            min_salary,
            max_salary,
        )

        try:
            raw_jobs = self.filter.search(
                categories=categories,
                days=days,
                min_salary=min_salary,
                max_salary=max_salary,
            )
        except Exception:  # pragma: no cover - network errors
            logging.exception("Failed to fetch jobs from hnhiring.com")
            return self._empty_df.copy()

        if not raw_jobs:
            logging.info("HNHiring returned 0 jobs before filtering")
            return self._empty_df.copy()

        deduped_jobs = self._deduplicate_jobs(raw_jobs)
        filtered_jobs = deduped_jobs

        transformed = [self.transformer.transform(job) for job in filtered_jobs]
        df = pd.DataFrame(transformed, columns=self.transformer.columns)
        logging.info("HNHiring scraper collected %d rows after filtering", len(df))
        return df

    def _deduplicate_jobs(self, jobs: List[Dict]) -> List[Dict]:
        """Collapse duplicate jobs by link, keeping the most recent post."""
        deduped: Dict[str, Dict] = {}
        for job in jobs:
            link = job.get("link")
            if not link:
                continue

            current = deduped.get(link)
            if current is None:
                deduped[link] = job
                continue

            if job.get("date_posted") and current.get("date_posted"):
                if job["date_posted"] > current["date_posted"]:
                    deduped[link] = job
            elif job.get("date_posted"):
                deduped[link] = job

        return list(deduped.values())

    # Keyword-based filtering intentionally removed; retain all posts within the time window.