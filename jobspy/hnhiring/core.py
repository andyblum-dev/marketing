from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional

import requests
from bs4 import BeautifulSoup


@dataclass
class Entry:
    score: Optional[int]
    text: str
    href: str


@dataclass
class Job:
    user: Optional[str]
    user_profile: Optional[str]
    date: Optional[str]
    description: str
    emails: List[str]
    links: List[str]


class EntryParser:
    BASE_URL = "https://hnhiring.com/"
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }

    def __init__(self) -> None:
        self.soup: BeautifulSoup | None = None

    def fetch(self) -> None:
        resp = requests.get(self.BASE_URL, headers=self.HEADERS, timeout=10)
        resp.raise_for_status()
        self.soup = BeautifulSoup(resp.text, "html.parser")

    def parse_table(self, title: str) -> List[Entry]:
        if not self.soup:
            raise RuntimeError("Call fetch() before parse_table().")

        section = self.soup.find("h2", string=title)
        if not section:
            return []

        table = section.find_next("table")
        if not table:
            return []

        entries: List[Entry] = []
        for row in table.select("tbody tr"):
            score = row.get("data-score")
            link = row.find("a")
            if not link:
                continue
            entries.append(
                Entry(
                    score=int(score) if score else None,
                    text=link.get_text(strip=True),
                    href=link.get("href"),
                )
            )
        return entries

    def get_technologies(self) -> List[Entry]:
        return self.parse_table("Technologies")

    def get_locations(self) -> List[Entry]:
        return self.parse_table("Locations")


class JobParser:
    BASE_URL = "https://hnhiring.com"
    HEADERS = EntryParser.HEADERS

    def __init__(self, slug: str):
        """slug: e.g. '/locations/remote' or '/technologies/python'"""
        self.slug = slug
        self.soup: BeautifulSoup | None = None

    def fetch(self) -> None:
        url = f"{self.BASE_URL}{self.slug}"
        resp = requests.get(url, headers=self.HEADERS, timeout=10)
        resp.raise_for_status()
        self.soup = BeautifulSoup(resp.text, "html.parser")

    def parse_jobs(self) -> List[Job]:
        if not self.soup:
            raise RuntimeError("Call fetch() before parse_jobs().")

        jobs: List[Job] = []
        for li in self.soup.select("ul.jobs li.job"):
            user_tag = li.select_one(".user a")
            date_tag = li.select_one(".user .type-info")
            body = li.select_one(".body")
            body_text = body.get_text(" ", strip=True) if body else ""

            emails = re.findall(r"[\w\.-]+@[\w\.-]+\.\w+", body_text)
            links = [a["href"] for a in body.find_all("a", href=True)] if body else []

            jobs.append(
                Job(
                    user=user_tag.get_text(strip=True) if user_tag else None,
                    user_profile=user_tag["href"] if user_tag else None,
                    date=date_tag.get_text(strip=True) if date_tag else None,
                    description=body_text,
                    emails=emails,
                    links=links,
                )
            )
        return jobs


__all__ = [
    "Entry",
    "Job",
    "EntryParser",
    "JobParser",
]
