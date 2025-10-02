import logging
import re
import uuid
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Set

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from urllib.parse import urlencode
import threading
import asyncio


class BuiltinURLBuilder:
    BASE_URL = "https://builtin.com/jobs"

    def __init__(self, keyword: str = "", remote: bool = True, all_locations: bool = True):
        self.keyword = keyword
        self.remote = remote
        self.all_locations = all_locations

    def build(self, page: int = 1) -> str:
        path = "/remote" if self.remote else ""
        params: Dict[str, str] = {"page": str(page)}
        if self.keyword:
            params["search"] = self.keyword
        if self.all_locations:
            params["allLocations"] = "true"
        return f"{self.BASE_URL}{path}?{urlencode(params)}"




class BuiltinCollector:
    def __init__(
        self,
        url_builder: BuiltinURLBuilder,
        max_age_hours: Optional[int] = None,
        *,
        headless: bool = True,
        slow_mo: int = 0,
        page_wait_ms: int = 0,
        max_pages: int = 200,
    ):
        self.url_builder = url_builder
        self.max_age_hours = max_age_hours
        self.headless = headless
        self.slow_mo = slow_mo
        self.page_wait_ms = page_wait_ms
        self.max_pages = max_pages

    def _parse_posted_text(self, text: str) -> datetime | None:
        """
        Convert BuiltIn's posted labels ("2 Hours Ago", "1 Day Ago", "30+ Days Ago", "Just Posted")
        into datetime. Returns UTC datetime.
        """
        if not text:
            return None

        text = (text or "").lower().strip()
        now = datetime.utcnow()

        # quick 'just posted' check
        if "just" in text:
            return now.replace(microsecond=0)

        # regex fallback: capture numbers like '30+ Days Ago' or '2 Hours Ago'
        m = re.search(r'(?P<n>\d+)\+?\s*(?P<unit>hour|hours|hr|hrs|day|days)', text, flags=re.I)
        if m:
            try:
                n = int(m.group('n'))
                unit = m.group('unit').lower()
                if 'hour' in unit or 'hr' in unit:
                    return (now - timedelta(hours=n)).replace(microsecond=0)
                else:
                    return (now - timedelta(days=n)).replace(microsecond=0)
            except Exception:
                # fall back to assuming now if parse fails
                return now.replace(microsecond=0)

        # fallback textual parsing
        if "hour" in text:
            try:
                n = int(text.split()[0])
                return (now - timedelta(hours=n)).replace(microsecond=0)
            except ValueError:
                return now.replace(microsecond=0)
        elif "day" in text:
            try:
                num = text.split()[0]
                n = int(num.replace("+", ""))  # handle "30+"
                return (now - timedelta(days=n)).replace(microsecond=0)
            except ValueError:
                return (now - timedelta(days=30)).replace(microsecond=0)

        # fallback: assume it's now if unknown format
        return now


    def _collect_job_urls_blocking(self, start_page: int = 1) -> List[Dict]:
        """
        The original blocking implementation extracted to a helper so it can run
        either on the current thread (normal) or inside a separate worker thread
        if the main thread has an event loop.
        """
        all_jobs: List[Dict] = []
        page_num = start_page
        cutoff: Optional[datetime] = None
        if self.max_age_hours:
            cutoff = datetime.utcnow() - timedelta(hours=self.max_age_hours)

        max_pages = max(1, self.max_pages)
        logging.debug("[builtin] Starting collection for keyword '%s' (cutoff=%s)", self.url_builder.keyword, cutoff)

        with sync_playwright() as playwright:
            browser = playwright.firefox.launch(headless=self.headless, slow_mo=self.slow_mo)
            page = browser.new_page()

            while True:
                if page_num - start_page >= max_pages:
                    logging.debug("[builtin] Reached max pages (%s), stopping", max_pages)
                    break

                url = self.url_builder.build(page_num)
                logging.debug("[builtin] Visiting %s", url)
                page.goto(url, wait_until="networkidle")
                try:
                    page.wait_for_selector("h2 a[data-id='job-card-title']", timeout=12000)
                except Exception:
                    logging.warning("[builtin] Timed out waiting for job cards on %s", url)
                if self.page_wait_ms:
                    page.wait_for_timeout(self.page_wait_ms)
                soup = BeautifulSoup(page.content(), "html.parser")

                links = soup.select("h2 a[data-id='job-card-title']")
                logging.debug("[builtin] Found %d job links on page %s", len(links), page_num)
                if not links:
                    logging.debug("[builtin] No job links on page %s, stopping", page_num)
                    break

                page_posted_ats: List[datetime] = []
                stop = False

                for link in links:
                    href_value = link.get("href")
                    if not href_value:
                        continue
                    href = f"https://builtin.com{href_value}"
                    title = link.get_text(strip=True)

                    posted_text = None
                    parent = link.find_parent("div", class_="left-side-tile-item-3")
                    if parent:
                        posted_el = parent.find_next(
                            "span",
                            class_="fs-xs fw-bold bg-gray-01 font-Montserrat text-gray-03",
                        )
                        if posted_el and posted_el.get_text(strip=True):
                            posted_text = posted_el.get_text(strip=True)

                    if not posted_text:
                        for span_candidate in link.find_all_next("span", limit=5):
                            span_text = span_candidate.get_text(strip=True)
                            if span_text and "ago" in span_text.lower():
                                posted_text = span_text
                                break

                    posted_at: Optional[datetime] = None
                    if posted_text:
                        parsed = self._parse_posted_text(posted_text)
                        if parsed:
                            if getattr(parsed, "tzinfo", None):
                                parsed = parsed.astimezone(tz=None).replace(tzinfo=None)
                            posted_at = parsed

                    if cutoff and posted_at and posted_at < cutoff:
                        logging.debug(
                            "[builtin] Reached cutoff at %s (parsed=%s) on page %s",
                            posted_text,
                            posted_at,
                            page_num,
                        )
                        stop = True
                        break

                    job = {
                        "url": href,
                        "title": title,
                        "posted_text": posted_text,
                        "posted_at": posted_at,
                        "search_keyword": self.url_builder.keyword,
                    }
                    all_jobs.append(job)

                    if posted_at:
                        page_posted_ats.append(posted_at)

                if stop:
                    break

                if cutoff and page_posted_ats:
                    oldest = min(page_posted_ats)
                    if oldest < cutoff:
                        logging.debug(
                            "[builtin] Oldest post on page %s is %s, before cutoff %s — stopping",
                            page_num,
                            oldest,
                            cutoff,
                        )
                        break

                page_num += 1

            browser.close()

        logging.debug("[builtin] Collected %d jobs for keyword '%s'", len(all_jobs), self.url_builder.keyword)
        return all_jobs


    def collect_job_urls_until_cutoff(self, start_page: int = 1) -> List[Dict]:
        """
        Public synchronous entrypoint that will run the blocking collector directly,
        or — if the calling thread already has an asyncio event loop — will run the
        blocking collector inside a separate worker thread to avoid the Playwright error.
        """
        try:
            loop = asyncio.get_running_loop()
            loop_running = True
        except RuntimeError:
            loop_running = False

        if not loop_running:
            # normal path: run directly (no event loop on this thread)
            return self._collect_job_urls_blocking(start_page)
        else:
            # we're inside an asyncio loop on this thread (that's the cause of your error).
            # run the blocking function in a worker thread and join synchronously.
            result_container: List[List[Dict]] = []
            exc_container: List[BaseException] = []

            def target():
                try:
                    res = self._collect_job_urls_blocking(start_page)
                    result_container.append(res)
                except BaseException as e:
                    exc_container.append(e)

            t = threading.Thread(target=target, daemon=True)
            t.start()
            t.join()

            if exc_container:
                # re-raise the original exception on the calling thread for clarity
                raise exc_container[0]
            return result_container[0] if result_container else []

class BuiltinScraper:
    def __init__(self, *, headless: bool = True, slow_mo: int = 0, detail_wait_ms: int = 2000):
        self._play = None
        self._browser = None
        self._page = None
        self.headless = headless
        self.slow_mo = slow_mo
        self.detail_wait_ms = detail_wait_ms

    def _ensure_browser(self):
        if not self._play:
            self._play = sync_playwright().start()
            self._browser = self._play.firefox.launch(headless=self.headless, slow_mo=self.slow_mo)
            self._page = self._browser.new_page()

    def close(self):
        if self._browser:
            self._browser.close()
        if self._play:
            self._play.stop()
        self._browser = None
        self._page = None
        self._play = None

    def fetch_and_parse(self, url: str) -> dict:
        self._ensure_browser()
        if not self._page:
            raise RuntimeError("Playwright page not initialized")
        logging.debug("[builtin] Fetching detail page %s", url)
        self._page.goto(url, wait_until="domcontentloaded")
        if self.detail_wait_ms:
            self._page.wait_for_timeout(self.detail_wait_ms)
        html = self._page.content()
        return self.parse_job_detail(html)

    def parse_job_detail(self, html: str) -> dict:
        soup = BeautifulSoup(html, "html.parser")

        # Title & company
        title = soup.select_one("h1.fw-extrabold")
        company = soup.select_one("h2.text-pretty-blue")

        # Posted / location / seniority
        posted = soup.select_one("span.font-barlow.fs-md")
        location = soup.select_one("div.font-barlow.text-gray-03")
        seniority = None
        for el in soup.select("div.font-barlow.text-gray-03"):
            txt = el.get_text().lower()
            if "senior" in txt or "trophy" in txt:
                seniority = el.get_text(strip=True)

        # Tags
        tags = [t.get_text(strip=True) for t in soup.select("div.font-barlow.fw-medium.mb-md")]

        # Description
        desc = soup.select_one("div.html-parsed-content")
        description = desc.get_text("\n", strip=True) if desc else None

        # Skills
        skills = [s.get_text(strip=True) for s in soup.select("div.d-flex.gap-sm.flex-wrap > div")]

        # Testimonials
        testimonials = []
        for t in soup.select("div.slick-slide"):
            quote = t.select_one("div.fs-md")
            name = t.select_one("div.fw-bold")
            role = t.select_one("div.fs-sm.text-gray-03")
            if quote and name:
                testimonials.append({
                    "name": name.get_text(strip=True),
                    "role": role.get_text(strip=True) if role else None,
                    "quote": quote.get_text(strip=True)
                })

        return {
            "title": title.get_text(strip=True) if title else None,
            "company": company.get_text(strip=True) if company else None,
            "posted": posted.get_text(strip=True) if posted else None,
            "location": location.get_text(strip=True) if location else None,
            "seniority": seniority,
            "tags": tags,
            "description": description,
            "skills": skills,
            "testimonials": testimonials
        }


class BuiltinTransformer:
    def __init__(self):
        self.columns = [
            "id", "site", "job_url", "job_url_direct", "title", "company", "location",
            "date_posted", "job_type", "salary_source", "interval", "min_amount",
            "max_amount", "currency", "is_remote", "job_level", "job_function",
            "listing_type", "emails", "description", "company_industry", "company_url",
            "company_logo", "company_url_direct", "company_addresses", "company_num_employees",
            "company_revenue", "company_description", "skills", "experience_range",
            "company_rating", "company_reviews_count", "vacancy_count", "work_from_home_type",
            "title_hash"
        ]

    def transform(self, job: Dict) -> Dict:
        posted_at = job.get("posted_at")
        if isinstance(posted_at, datetime):
            date_posted = posted_at.isoformat()
        elif isinstance(posted_at, str):
            date_posted = posted_at
        else:
            date_posted = None

        location = job.get("location") or job.get("posted_location")
        if isinstance(location, list):
            location_str = ", ".join(location)
        else:
            location_str = location

        tags = job.get("tags")
        if isinstance(tags, list):
            job_function = ", ".join(tags)
        else:
            job_function = tags

        skills = job.get("skills")
        if isinstance(skills, list):
            skills_value = ", ".join(skills)
        else:
            skills_value = skills

        description = job.get("description")
        if isinstance(description, list):
            description_value = "\n".join(description)
        else:
            description_value = description

        seniority = job.get("seniority")
        search_keyword = job.get("search_keyword")

        is_remote = None
        loc_lower = (location_str or "").lower()
        if "remote" in loc_lower:
            is_remote = True
        elif search_keyword and "remote" in search_keyword.lower():
            is_remote = True

        return {
            "id": str(uuid.uuid4()),
            "site": "builtin",
            "job_url": job.get("url"),
            "job_url_direct": job.get("url"),
            "title": job.get("title"),
            "company": job.get("company"),
            "location": location_str,
            "date_posted": date_posted,
            "job_type": None,
            "salary_source": "builtin" if job.get("salary") else None,
            "interval": None,
            "min_amount": None,
            "max_amount": None,
            "currency": None,
            "is_remote": is_remote,
            "job_level": seniority,
            "job_function": job_function,
            "listing_type": "job_board",
            "emails": None,
            "description": description_value,
            "company_industry": None,
            "company_url": None,
            "company_logo": None,
            "company_url_direct": None,
            "company_addresses": location_str,
            "company_num_employees": None,
            "company_revenue": None,
            "company_description": None,
            "skills": skills_value,
            "experience_range": seniority,
            "company_rating": None,
            "company_reviews_count": None,
            "vacancy_count": None,
            "work_from_home_type": "remote" if is_remote else None,
            "title_hash": None,
        }


class BuiltinJobsCollector:
    """High-level integration class for BuiltIn jobs."""

    def __init__(self, config: Dict):
        self.settings = config.get("builtin", {})
        self.enabled: bool = self.settings.get("enabled", False)
        self.max_age_hours: Optional[int] = self.settings.get("max_age_hours", 72)
        self.max_pages: int = self.settings.get("max_pages", 200)
        self.start_page: int = self.settings.get("start_page", 1)
        self.per_keyword_limit: Optional[int] = self.settings.get("per_keyword_limit")
        self.total_limit: Optional[int] = self.settings.get("total_limit")
        self.remote: bool = self.settings.get("remote", True)
        self.all_locations: bool = self.settings.get("all_locations", True)
        self.headless: bool = self.settings.get("headless", True)
        self.slow_mo: int = self.settings.get("slow_mo_ms", 0)
        self.page_wait_ms: int = self.settings.get("page_wait_ms", 0)
        self.detail_wait_ms: int = self.settings.get("detail_wait_ms", 1500)

        self.transformer = BuiltinTransformer()
        self._empty_df = pd.DataFrame(columns=self.transformer.columns)
        self._scraper: Optional[BuiltinScraper] = None

    def _get_scraper(self) -> BuiltinScraper:
        if not self._scraper:
            self._scraper = BuiltinScraper(
                headless=self.headless,
                slow_mo=self.slow_mo,
                detail_wait_ms=self.detail_wait_ms,
            )
        return self._scraper

    def _close_scraper(self):
        if self._scraper:
            self._scraper.close()
            self._scraper = None

    def _resolve_keywords(self, fallback_keywords: Iterable[str]) -> List[str]:
        raw_keywords = self.settings.get("keywords")
        if raw_keywords:
            candidates = raw_keywords
        else:
            candidates = fallback_keywords

        seen: Set[str] = set()
        resolved: List[str] = []
        for kw in candidates:
            if not kw:
                continue
            normalized = self._normalize_keyword(kw)
            if not normalized:
                continue
            key = normalized.lower()
            if key in seen:
                continue
            seen.add(key)
            resolved.append(normalized)
        logging.debug("BuiltIn resolved keywords: %s", resolved)
        return resolved

    @staticmethod
    def _normalize_keyword(keyword: str) -> str:
        cleaned = re.sub(r"\bremote\b", "", keyword, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned

    def scrape(self, fallback_keywords: Iterable[str], *, limit: Optional[int] = None) -> pd.DataFrame:
        if not self.enabled:
            logging.debug("BuiltIn integration disabled; skipping scrape")
            return self._empty_df.copy()

        keywords = self._resolve_keywords(fallback_keywords)
        if not keywords:
            logging.info("BuiltIn keywords missing; skipping scrape")
            return self._empty_df.copy()

        effective_limit = limit if limit is not None else self.total_limit

        records: List[Dict] = []
        seen_urls: Set[str] = set()
        total_collected = 0

        logging.info("Starting BuiltIn scrape for %d keywords (limit=%s)", len(keywords), effective_limit)
        scraper = self._get_scraper()

        try:
            for keyword in keywords:
                if effective_limit is not None and total_collected >= effective_limit:
                    break

                builder = BuiltinURLBuilder(
                    keyword=keyword,
                    remote=self.remote,
                    all_locations=self.all_locations,
                )
                collector = BuiltinCollector(
                    builder,
                    max_age_hours=self.max_age_hours,
                    headless=self.headless,
                    slow_mo=self.slow_mo,
                    page_wait_ms=self.page_wait_ms,
                    max_pages=self.max_pages,
                )

                logging.info("Collecting BuiltIn jobs for keyword '%s'", keyword)
                try:
                    jobs_meta = collector.collect_job_urls_until_cutoff(start_page=self.start_page)
                except Exception:
                    logging.exception("Failed to collect BuiltIn jobs for '%s'", keyword)
                    continue

                if not jobs_meta:
                    logging.info("BuiltIn returned 0 jobs for '%s' (meta list empty)", keyword)
                    continue

                per_keyword_count = 0

                for meta in jobs_meta:
                    if effective_limit is not None and total_collected >= effective_limit:
                        break
                    if self.per_keyword_limit is not None and per_keyword_count >= self.per_keyword_limit:
                        break

                    job_url = meta.get("url")
                    if not job_url or job_url in seen_urls:
                        continue

                    try:
                        detail = scraper.fetch_and_parse(job_url)
                    except Exception:
                        logging.exception("Failed to fetch BuiltIn detail for %s", job_url)
                        continue

                    combined = {**meta, **detail}
                    transformed = self.transformer.transform(combined)
                    records.append(transformed)
                    seen_urls.add(job_url)
                    total_collected += 1
                    per_keyword_count += 1

                logging.info(
                    "BuiltIn keyword '%s' produced %d transformed rows (meta=%d)",
                    keyword,
                    per_keyword_count,
                    len(jobs_meta),
                )

        finally:
            self._close_scraper()

        if not records:
            return self._empty_df.copy()

        df = pd.DataFrame(records, columns=self.transformer.columns)
        logging.info("BuiltIn scraper collected %d rows", len(df))
        return df

class BuiltinJobPipeline:
    """Simple wrapper for manual execution of the BuiltIn collector."""

    def __init__(
        self,
        keyword: str,
        *,
        max_age_hours: int = 72,
        headless: bool = False,
        slow_mo_ms: int = 200,
        detail_wait_ms: int = 1500,
    ):
        self.collector = BuiltinJobsCollector(
            {
                "builtin": {
                    "enabled": True,
                    "keywords": [keyword],
                    "max_age_hours": max_age_hours,
                    "headless": headless,
                    "slow_mo_ms": slow_mo_ms,
                    "detail_wait_ms": detail_wait_ms,
                }
            }
        )

    def run(self, limit: Optional[int] = None) -> List[Dict]:
        df = self.collector.scrape([], limit=limit)
        logging.info("[pipeline] BuiltIn returned %d rows", len(df))
        return df.to_dict(orient="records")


if __name__ == "__main__":
    pipeline = BuiltinJobPipeline(keyword="software engineer", max_age_hours=36)
    jobs_data = pipeline.run(limit=25)  # scrape 10 jobs for demo

    for job in jobs_data:
        print("=" * 80)
        print(job["title"], "at", job["company"])
        print("Location:", job["location"])
        print("Posted:", job["date_posted"])
        print("Skills:", job["skills"])
        print("URL:", job["job_url"])


__all__ = [
    "BuiltinURLBuilder",
    "BuiltinCollector",
    "BuiltinScraper",
    "BuiltinTransformer",
    "BuiltinJobsCollector",
    "BuiltinJobPipeline",
]
