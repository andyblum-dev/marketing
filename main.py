#!/usr/bin/env python3
"""
JobSpy Data Collector (CSV-first)
Fetch the requested job listings, write them to CSV, and notify ActiveMQ when the file is ready.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import subprocess
import sys
import time
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict

import pandas as pd
import schedule
import stomp
from jobspy import scrape_jobs
from jobspy.builtin import BuiltinJobsCollector
from jobspy.hnhiring import HNHiringCollector
from jobspy.util import desired_order


# Load environment variables at startup
def load_env_vars():
    """Load environment variables from .env file manually"""
    env_file = Path('.env')
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and '=' in line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    # Remove quotes if present
                    value = value.strip().strip('"').strip("'")
                    os.environ[key] = value

load_env_vars()


class ConfigManager:
    """Load configuration and set up logging."""

    def __init__(self, config_path: str | None = None):
        default_path = os.getenv("JOBSPY_CONFIG", "configs/data_integration.json")
        self.config_path = Path(config_path or default_path).expanduser()
        self.config_dir = self.config_path.parent.resolve()
        self.config = self.load_config()
        self.setup_logging()

    def load_config(self) -> Dict:
        try:
            config = self._load_config_recursive(self.config_path)
            logging.info("Configuration loaded from %s", self.config_path)
            return config
        except FileNotFoundError:
            logging.error("Configuration file %s not found", self.config_path)
            sys.exit(1)
        except json.JSONDecodeError as exc:
            logging.error("Invalid JSON in configuration file: %s", exc)
            sys.exit(1)

    def _load_config_recursive(self, path: Path, seen: set[Path] | None = None) -> Dict:
        seen = seen or set()
        resolved_path = path.resolve()
        if resolved_path in seen:
            raise ValueError(f"Circular config inheritance detected at {resolved_path}")
        seen.add(resolved_path)

        with resolved_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        inherits = data.pop("inherits", None)
        schema = data.pop("$schema", None)
        if schema:
            logging.debug("Ignoring $schema field in %s", resolved_path)

        if inherits:
            inherit_path = Path(inherits)
            if not inherit_path.is_absolute():
                inherit_path = (resolved_path.parent / inherit_path).resolve()
            base_config = self._load_config_recursive(inherit_path, seen)
        else:
            base_config = {}

        merged = self._merge_dicts(base_config, data)
        return merged

    def _merge_dicts(self, base: Dict, override: Dict) -> Dict:
        merged = deepcopy(base)
        for key, value in override.items():
            if (
                isinstance(value, dict)
                and isinstance(merged.get(key), dict)
            ):
                merged[key] = self._merge_dicts(merged[key], value)
            else:
                merged[key] = value
        return merged

    def setup_logging(self):
        log_config = self.config.get("logging", {})
        log_level = getattr(logging, log_config.get("level", "INFO").upper(), logging.INFO)

        log_file = log_config.get("file_path", "./logs/jobspy.log")
        os.makedirs(Path(log_file).parent, exist_ok=True)

        handlers: list[logging.Handler] = [logging.FileHandler(log_file)]
        if log_config.get("console_output", True):
            handlers.append(logging.StreamHandler())

        logging.basicConfig(
            level=log_level,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=handlers,
            force=True,
        )


class ActiveMQHandler:
    """Thin wrapper that keeps a STOMP connection alive and publishes JSON messages."""

    def __init__(self, config: Dict):
        self.config = config.get('messaging', {}).get('activemq', {})
        self.connection = None
        self.enabled = self.config.get('enabled', False)

        if self.enabled:
            self.setup_connection()

    def setup_connection(self):
        """Ensure a working STOMP connection."""
        try:
            host = self.config.get('host', 'localhost')
            port = self.config.get('port', 61616)
            username = self.config.get('username', os.getenv('ARTEMIS_USER', 'sample'))
            password = self.config.get('password', os.getenv('ARTEMIS_PASSWORD', 'sample'))

            if self.connection and self.connection.is_connected():
                self.connection.disconnect()

            self.connection = stomp.Connection([(host, port)])
            self.connection.connect(username, password, wait=True)
            self.enabled = True
            logging.info(f"Connected to ActiveMQ at {host}:{port}")

        except Exception:
            logging.exception("Failed to connect to ActiveMQ")
            self.enabled = False

    def send_message(self, payload: Dict):
        """Publish a JSON payload announcing that new data is available."""
        if not self.enabled:
            logging.debug("ActiveMQ messaging disabled; skipping notification")
            return

        if not self.connection or not self.connection.is_connected():
            self.setup_connection()
            if not self.enabled or not self.connection or not self.connection.is_connected():
                logging.error("ActiveMQ connection unavailable; message not sent")
                return

        try:
            queue_name = self.config.get('queue_name', 'job_updates')
            message = json.dumps(payload, default=str)

            self.connection.send(
                destination=f"/queue/{queue_name}",
                body=message,
                headers={'content-type': 'application/json'}
            )
            logging.debug("Published message to ActiveMQ queue %s", queue_name)

        except Exception:
            logging.exception("Failed to send message to ActiveMQ")
            self.setup_connection()

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.disconnect()
            logging.info("Disconnected from ActiveMQ")


class NetworkManager:
    """Handle VPN rotation via an external bash script."""

    def __init__(self, config: Dict, config_root: Path | None = None):
        network_config = config.get("network", {})
        raw_script_path = network_config.get("vpn_rotation_script", "")
        self.script_path = Path(raw_script_path).expanduser() if raw_script_path else None
        if self.script_path and not self.script_path.is_absolute() and config_root:
            self.script_path = (config_root / self.script_path).resolve()
        self.rotate_before_request = network_config.get("rotate_before_request", False)
        self.rotate_after_failure = network_config.get("rotate_after_failure", True)
        self.timeout_seconds = int(network_config.get("rotation_timeout_seconds", 60))
        self.cooldown_seconds = max(0, int(network_config.get("cooldown_seconds", 0)))
        self._script_available_logged = False

    def rotate_ip(self, reason: str = "manual") -> bool:
        """Execute the VPN reset script to obtain a new IP address."""
        if not self.script_path:
            if not self._script_available_logged:
                logging.debug("VPN rotation script not configured; skipping rotation")
                self._script_available_logged = True
            return False

        if not self.script_path.exists():
            if not self._script_available_logged:
                logging.error("VPN rotation script not found at %s", self.script_path)
                self._script_available_logged = True
            return False

        try:
            logging.info("Rotating VPN connection (reason: %s)", reason)
            result = subprocess.run(
                ["/bin/bash", str(self.script_path)],
                check=True,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
            )
            if result.stdout:
                logging.debug("VPN rotation stdout: %s", result.stdout.strip())
            if result.stderr:
                logging.debug("VPN rotation stderr: %s", result.stderr.strip())
            if self.cooldown_seconds:
                time.sleep(self.cooldown_seconds)
            return True
        except subprocess.CalledProcessError as exc:
            logging.error(
                "VPN rotation script exited with code %s: %s",
                exc.returncode,
                exc.stderr.strip() if exc.stderr else exc,
            )
        except subprocess.TimeoutExpired:
            logging.error(
                "VPN rotation script timed out after %s seconds",
                self.timeout_seconds,
            )
        except Exception:
            logging.exception("Unexpected error while running VPN rotation script")

        return False


class JobSpyScraper:
    """Fetch job data, save it untouched to CSV, and tell ActiveMQ when it's ready."""

    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager.config
        self.mq_handler = ActiveMQHandler(self.config)
        self.output_dir = self._prepare_output_directory()
        self.network_manager = NetworkManager(self.config, config_manager.config_dir)

        job_config = self.config.get('job_search', {})
        site_names = self._normalize_site_names(job_config.get('sites', ['indeed']))
        self._jobspy_handles_hnhiring = 'hnhiring' in site_names
        self._jobspy_handles_builtin = 'builtin' in site_names

        if self._jobspy_handles_hnhiring:
            logging.debug("HNHiring collector delegated to jobspy.scrape_jobs")
            self.hn_collector = None
        else:
            self.hn_collector = HNHiringCollector(self.config)

        if self._jobspy_handles_builtin:
            logging.debug("BuiltIn collector delegated to jobspy.scrape_jobs")
            self.builtin_collector = None
        else:
            self.builtin_collector = BuiltinJobsCollector(self.config)

        self.default_columns = self._resolve_default_columns()

    @staticmethod
    def _normalize_site_names(sites) -> set[str]:
        if isinstance(sites, (str, bytes)):
            iterable = [sites]
        else:
            iterable = sites or []

        normalized: set[str] = set()
        for site in iterable:
            if site is None:
                continue
            if isinstance(site, str):
                normalized.add(site.lower())
            elif isinstance(site, bytes):
                try:
                    normalized.add(site.decode("utf-8").lower())
                except Exception:
                    continue
            elif hasattr(site, "value"):
                normalized.add(str(site.value).lower())
            else:
                normalized.add(str(site).lower())
        return normalized

    def _resolve_default_columns(self) -> list[str]:
        for collector in (self.hn_collector, self.builtin_collector):
            transformer = getattr(collector, "transformer", None) if collector else None
            columns = getattr(transformer, "columns", None)
            if columns:
                base = list(columns)
                if "title_hash" not in base:
                    base.append("title_hash")
                return base

        base_columns = list(desired_order)
        if "title_hash" not in base_columns:
            base_columns.append("title_hash")
        return base_columns

    @staticmethod
    def _count_rows_for_site(df: pd.DataFrame | None, site_name: str) -> int:
        if df is None or df.empty or "site" not in df.columns:
            return 0
        try:
            site_series = df["site"].astype(str).str.lower()
        except Exception:
            return 0
        return int((site_series == site_name.lower()).sum())

    @staticmethod
    def _compute_title_hash(title) -> str | None:
        if not isinstance(title, str):
            return None
        snippet = title.strip()[:100]
        if not snippet:
            return None
        digest = hashlib.sha256(snippet.encode("utf-8")).hexdigest()
        return digest

    def _prepare_output_directory(self) -> Path:
        output_config = self.config.get('output', {})
        output_path = Path(output_config.get('file_path', './job_results'))
        output_path.mkdir(parents=True, exist_ok=True)
        Path('./logs').mkdir(exist_ok=True)
        return output_path

    def _guarded_collect(
        self,
        label: str,
        collector: Callable[..., pd.DataFrame],
        *args,
        **kwargs,
    ) -> pd.DataFrame:
        if self.network_manager.rotate_before_request:
            self.network_manager.rotate_ip(reason=f"pre_request:{label}")

        try:
            result = collector(*args, **kwargs)
            if result is None:
                return pd.DataFrame()
            return result
        except Exception:
            logging.exception("Error while running %s collector", label)
            if self.network_manager.rotate_after_failure:
                self.network_manager.rotate_ip(reason=f"failure:{label}")
            return pd.DataFrame()

    def scrape_jobs(self) -> pd.DataFrame:
        job_config = self.config.get('job_search', {})
        logging.info("Starting job scraping run")

        results = []
        sites = job_config.get('sites', ['indeed'])
        request_delay = job_config.get('request_delay_seconds', 8)
        error_delay = job_config.get('error_delay_seconds', 10)
        site_delay = job_config.get('site_delay_seconds', 15)

        for site in sites:
            logging.info("Scraping site %s", site)

            for search_term in job_config.get('search_terms', []):
                for location in job_config.get('locations', []):
                    if self.network_manager.rotate_before_request:
                        self.network_manager.rotate_ip(reason=f"pre_request:{site}:{search_term}:{location}")
                    try:
                        logging.info("Querying %s for '%s' in '%s'", site, search_term, location)
                        jobs_df = scrape_jobs(
                            site_name=[site],
                            search_term=search_term,
                            location=location,
                            results_wanted=job_config.get('results_wanted', 50),
                            hours_old=job_config.get('hours_old', 72),
                            country_indeed=job_config.get('country_indeed', 'USA'),
                            config=self.config,
                        )

                        if jobs_df is None or jobs_df.empty:
                            logging.info("No results for %s (%s | %s)", site, search_term, location)
                        else:
                            results.append(jobs_df)
                            logging.info("Collected %d rows from %s (%s | %s)", len(jobs_df), site, search_term, location)

                        time.sleep(request_delay)

                    except Exception:
                        logging.exception("Error scraping %s for '%s' in '%s'", site, search_term, location)
                        if self.network_manager.rotate_after_failure:
                            self.network_manager.rotate_ip(reason=f"failure:{site}:{search_term}:{location}")
                        time.sleep(error_delay)

            logging.info("Finished scraping site %s", site)
            time.sleep(site_delay)

        if results:
            combined = pd.concat(results, ignore_index=True, copy=False)
            logging.info("Total rows collected this run: %d", len(combined))
            return combined

        logging.info("No data collected in this run")
        return pd.DataFrame()

    def save_to_csv(self, jobs_df: pd.DataFrame) -> Path:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        csv_path = self.output_dir / f"jobs_{timestamp}.csv"
        jobs_df.to_csv(csv_path, index=False)
        logging.info("Saved %d rows to %s", len(jobs_df), csv_path)
        return csv_path

    def notify_csv_ready(self, csv_path: Path, row_count: int):
        message = {
            "type": "csv_ready",
            "csv_path": str(csv_path.resolve()),
            "row_count": row_count,
            "generated_at": datetime.utcnow().isoformat(),
        }
        self.mq_handler.send_message(message)

    def run_scraping_cycle(self):
        logging.info("=" * 50)
        logging.info("Starting new scraping cycle")
        logging.info("=" * 50)

        cycle_started = datetime.utcnow()

        try:
            jobspy_df = self.scrape_jobs()
            search_terms = self.config.get('job_search', {}).get('search_terms', [])

            logging.info("JobSpy source produced %d rows", len(jobspy_df) if jobspy_df is not None else 0)
            if self.hn_collector:
                hn_df = self._guarded_collect("hnhiring", self.hn_collector.scrape, search_terms)
                logging.info("HN Hiring source produced %d rows", len(hn_df))
            else:
                hn_df = pd.DataFrame()
                hn_rows = self._count_rows_for_site(jobspy_df, "hnhiring")
                logging.info("HN Hiring source produced %d rows via JobSpy", hn_rows)

            if self.builtin_collector:
                builtin_df = self._guarded_collect("builtin", self.builtin_collector.scrape, search_terms)
                logging.info("BuiltIn source produced %d rows", len(builtin_df))
            else:
                builtin_df = pd.DataFrame()
                builtin_rows = self._count_rows_for_site(jobspy_df, "builtin")
                logging.info("BuiltIn source produced %d rows via JobSpy", builtin_rows)

            dataframes = [df for df in (jobspy_df, hn_df, builtin_df) if not df.empty]
            if dataframes:
                combined_df = pd.concat(dataframes, ignore_index=True, sort=False)
            else:
                combined_df = pd.DataFrame(columns=self.default_columns)

            if "title_hash" not in combined_df.columns:
                combined_df["title_hash"] = None

            if not combined_df.empty and "title" in combined_df.columns:
                combined_df["title_hash"] = combined_df["title"].apply(self._compute_title_hash)

            csv_path = self.save_to_csv(combined_df)
            self.notify_csv_ready(csv_path, len(combined_df))
            logging.info(
                "Cycle finished successfully with %d rows in %s",
                len(combined_df),
                datetime.utcnow() - cycle_started,
            )

        except Exception as exc:
            logging.exception("Unexpected error during scraping cycle")
            error_message = {
                "type": "scraping_error",
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(exc),
            }
            self.mq_handler.send_message(error_message)


def setup_scheduler(scraper: JobSpyScraper, config: Dict):
    """Setup cron-like scheduling"""
    cron_config = config.get('cron_schedule', {})
    
    if not cron_config.get('enabled', False):
        logging.info("Cron scheduling disabled")
        return
    
    schedule_str = cron_config.get('schedule', '0 */6 * * *')  # Every 6 hours by default
    
    # Parse cron-like schedule (simplified version)
    # Format: minute hour day_of_month month day_of_week
    parts = schedule_str.split()
    if len(parts) >= 2:
        minute = parts[0]
        hour = parts[1]
        
        if hour.startswith('*/'):
            # Every N hours
            hours_interval = int(hour[2:])
            schedule.every(hours_interval).hours.do(scraper.run_scraping_cycle)
            logging.info(f"Scheduled to run every {hours_interval} hours")
        elif hour.isdigit():
            # Specific hour
            schedule.every().day.at(f"{hour.zfill(2)}:{minute.zfill(2)}").do(scraper.run_scraping_cycle)
            logging.info(f"Scheduled to run daily at {hour.zfill(2)}:{minute.zfill(2)}")
    
    logging.info(f"Scheduler setup complete: {cron_config.get('description', 'No description')}")


def parse_cli_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="JobSpy data collector")
    parser.add_argument(
        "--config",
        default=os.getenv("JOBSPY_CONFIG", "configs/data_integration.json"),
        help="Path to the configuration file to load",
    )
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Run a single scraping cycle immediately and exit",
    )
    parser.add_argument(
        "--no-schedule",
        action="store_true",
        help="Disable scheduler even if the config enables it",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None):
    """Main application entry point"""
    args = parse_cli_args(argv)
    logging.info("JobSpy Data Collector starting...")

    try:
        config_manager = ConfigManager(config_path=args.config)
        scraper = JobSpyScraper(config_manager)
        setup_scheduler(scraper, config_manager.config)

        if args.run_now:
            logging.info("Running immediate scraping cycle (via CLI flag)...")
            scraper.run_scraping_cycle()
            return

        cron_config = config_manager.config.get('cron_schedule', {})
        schedule_enabled = cron_config.get('enabled', False) and not args.no_schedule

        if not schedule_enabled:
            logging.info("Scheduling disabled; running single scraping cycle...")
            scraper.run_scraping_cycle()
            return

        logging.info("JobSpy Data Collector is running. Press Ctrl+C to stop.")
        while True:
            schedule.run_pending()
            time.sleep(60)

    except KeyboardInterrupt:
        logging.info("JobSpy Data Collector stopped by user")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if 'scraper' in locals():
            scraper.mq_handler.disconnect()
        logging.info("JobSpy Data Collector shutdown complete")


if __name__ == "__main__":
    main(sys.argv[1:])