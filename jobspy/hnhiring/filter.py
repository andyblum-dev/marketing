from __future__ import annotations

import csv
import uuid
from datetime import datetime, timedelta
from typing import Iterable, List

import requests
from bs4 import BeautifulSoup


class HNFilter:
    BASE_URL = "https://hnhiring.com"

    def __init__(self, parser):
        self.parser = parser

    def fetch_category_jobs(self, category_url: str) -> List[dict]:
        """Fetch jobs from a given category page."""
        resp = requests.get(self.BASE_URL + category_url)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        jobs: List[dict] = []
        for job in soup.select("li.job"):
            body = job.select_one("div.body")
            if body is None:
                continue

            # Title is often the first sentence before separators
            body_text = body.get_text(" ", strip=True)
            title = (
                body_text.split("|")[0].strip()
                if "|" in body_text
                else body_text[:180]
            )

            first_link = body.find("a", href=True)
            link = first_link["href"] if first_link else None

            date_tag = job.select_one(".type-info")
            date_posted = None
            if date_tag:
                try:
                    date_posted = datetime.strptime(
                        date_tag.get_text(strip=True), "%Y-%m-%d"
                    )
                except ValueError:
                    date_posted = None

            salary_tag = body.find("span", class_="salary")
            salary = salary_tag.get_text(strip=True) if salary_tag else None

            jobs.append(
                {
                    "title": title,
                    "link": link,
                    "date_posted": date_posted,
                    "salary": salary,
                    "raw_text": body_text,
                }
            )
        return jobs

    def search(
        self,
        categories: Iterable[str],
        days: int | None = None,
        min_salary: int | None = None,
        max_salary: int | None = None,
    ) -> List[dict]:
        """Query jobs based on categories and optional filters."""
        all_jobs: List[dict] = []
        for category_url in categories:
            jobs = self.fetch_category_jobs(category_url)
            for job in jobs:
                if days and job["date_posted"]:
                    cutoff_date = (datetime.utcnow() - timedelta(days=days)).date()
                    if job["date_posted"].date() < cutoff_date:
                        continue
                if job["salary"]:
                    salary_digits = [c for c in job["salary"] if c.isdigit()]
                    salary_num = int("".join(salary_digits)) if salary_digits else None
                    if salary_num is None:
                        continue
                    if min_salary and salary_num < min_salary:
                        continue
                    if max_salary and salary_num > max_salary:
                        continue
                all_jobs.append(job)
        return all_jobs


class HNTransformer:
    def __init__(self) -> None:
        self.columns = [
            "id",
            "site",
            "job_url",
            "job_url_direct",
            "title",
            "company",
            "location",
            "date_posted",
            "job_type",
            "salary_source",
            "interval",
            "min_amount",
            "max_amount",
            "currency",
            "is_remote",
            "job_level",
            "job_function",
            "listing_type",
            "emails",
            "description",
            "company_industry",
            "company_url",
            "company_logo",
            "company_url_direct",
            "company_addresses",
            "company_num_employees",
            "company_revenue",
            "company_description",
            "skills",
            "experience_range",
            "company_rating",
            "company_reviews_count",
            "vacancy_count",
            "work_from_home_type",
            "title_hash",
        ]

    def transform(self, hn_job: dict) -> dict:
        """Transform HN job dict into unified schema."""
        date_posted = hn_job.get("date_posted")
        if date_posted is not None:
            date_posted = date_posted.isoformat()

        title = hn_job.get("title") or ""

        return {
            "id": str(uuid.uuid4()),
            "site": "hnhiring",
            "job_url": hn_job.get("link"),
            "job_url_direct": hn_job.get("link"),
            "title": hn_job.get("title"),
            "company": None,
            "location": None,
            "date_posted": date_posted,
            "job_type": None,
            "salary_source": "hnhiring" if hn_job.get("salary") else None,
            "interval": None,
            "min_amount": None,
            "max_amount": None,
            "currency": None,
            "is_remote": "remote" in title.lower(),
            "job_level": None,
            "job_function": None,
            "listing_type": None,
            "emails": None,
            "description": None,
            "company_industry": None,
            "company_url": None,
            "company_logo": None,
            "company_url_direct": None,
            "company_addresses": None,
            "company_num_employees": None,
            "company_revenue": None,
            "company_description": None,
            "skills": None,
            "experience_range": None,
            "company_rating": None,
            "company_reviews_count": None,
            "vacancy_count": None,
            "work_from_home_type": None,
            "title_hash": None,
        }

    def to_csv(self, hn_jobs: Iterable[dict], filepath: str) -> None:
        """Save transformed jobs to CSV."""
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.columns)
            writer.writeheader()
            for job in hn_jobs:
                writer.writerow(self.transform(job))


__all__ = ["HNFilter", "HNTransformer"]
