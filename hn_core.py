import re
import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import Optional, List


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

    def __init__(self):
        self.soup = None

    def fetch(self):
        resp = requests.get(self.BASE_URL, headers=self.HEADERS, timeout=10)
        resp.raise_for_status()
        self.soup = BeautifulSoup(resp.text, "html.parser")

    def parse_table(self, title: str) -> List[Entry]:
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
        self.soup = None

    def fetch(self):
        url = f"{self.BASE_URL}{self.slug}"
        resp = requests.get(url, headers=self.HEADERS, timeout=10)
        resp.raise_for_status()
        self.soup = BeautifulSoup(resp.text, "html.parser")

    def parse_jobs(self) -> List[Job]:
        jobs: List[Job] = []
        for li in self.soup.select("ul.jobs li.job"):
            user_tag = li.select_one(".user a")
            date_tag = li.select_one(".user .type-info")
            body = li.select_one(".body")
            body_text = body.get_text(" ", strip=True) if body else ""

            # Extract all emails
            emails = re.findall(r"[\w\.-]+@[\w\.-]+\.\w+", body_text)
            # Extract all links
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


if __name__ == "__main__":
    # Example usage
    entry_parser = EntryParser()
    entry_parser.fetch()

    techs = entry_parser.get_technologies()
    locs = entry_parser.get_locations()

    print("=== First 3 Tech Entries ===")
    for t in techs[:3]:
        print(t)

    print("\n=== First 3 Location Entries ===")
    for l in locs[:3]:
        print(l)

    print("\n=== First 2 Remote Jobs ===")
    job_parser = JobParser("/locations/remote")
    job_parser.fetch()
    jobs = job_parser.parse_jobs()
    for j in jobs[:2]:
        print(j)
