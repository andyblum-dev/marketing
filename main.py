#!/usr/bin/env python3
"""
JobSpy Data Collector (CSV-first)
Fetch the requested job listings, write them to CSV, and notify ActiveMQ when the file is ready.
"""

import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd
import schedule
import stomp
from jobspy import scrape_jobs
from hnhiring_scraper import HNHiringCollector
from builtin import BuiltinJobsCollector

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

    def __init__(self, config_path: str = "config.json"):
        self.config_path = config_path
        self.config = self.load_config()
        self.setup_logging()

    def load_config(self) -> Dict:
        try:
            with open(self.config_path, "r") as file:
                config = json.load(file)
            logging.info("Configuration loaded from %s", self.config_path)
            return config
        except FileNotFoundError:
            logging.error("Configuration file %s not found", self.config_path)
            sys.exit(1)
        except json.JSONDecodeError as exc:
            logging.error("Invalid JSON in configuration file: %s", exc)
            sys.exit(1)

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


class JobSpyScraper:
    """Fetch job data, save it untouched to CSV, and tell ActiveMQ when it's ready."""

    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager.config
        self.mq_handler = ActiveMQHandler(self.config)
        self.output_dir = self._prepare_output_directory()
        self.hn_collector = HNHiringCollector(self.config)
        self.builtin_collector = BuiltinJobsCollector(self.config)

        hn_columns = getattr(self.hn_collector.transformer, "columns", [])
        builtin_columns = getattr(self.builtin_collector.transformer, "columns", [])
        self.default_columns = hn_columns or builtin_columns or []

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
                    try:
                        logging.info("Querying %s for '%s' in '%s'", site, search_term, location)
                        jobs_df = scrape_jobs(
                            site_name=[site],
                            search_term=search_term,
                            location=location,
                            results_wanted=job_config.get('results_wanted', 50),
                            hours_old=job_config.get('hours_old', 72),
                            country_indeed=job_config.get('country_indeed', 'USA'),
                        )

                        if jobs_df is None or jobs_df.empty:
                            logging.info("No results for %s (%s | %s)", site, search_term, location)
                        else:
                            results.append(jobs_df)
                            logging.info("Collected %d rows from %s (%s | %s)", len(jobs_df), site, search_term, location)

                        time.sleep(request_delay)

                    except Exception:
                        logging.exception("Error scraping %s for '%s' in '%s'", site, search_term, location)
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
            hn_df = self.hn_collector.scrape(search_terms)
            builtin_df = self.builtin_collector.scrape(search_terms)

            logging.info("JobSpy source produced %d rows", len(jobspy_df) if jobspy_df is not None else 0)
            logging.info("HN Hiring source produced %d rows", len(hn_df) if hn_df is not None else 0)
            logging.info("BuiltIn source produced %d rows", len(builtin_df) if builtin_df is not None else 0)

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


def main():
    """Main application entry point"""
    logging.info("JobSpy Data Collector starting...")
    
    try:
        # Load configuration
        config_manager = ConfigManager()
        
        # Initialize scraper
        scraper = JobSpyScraper(config_manager)
        
        # Setup scheduler
        setup_scheduler(scraper, config_manager.config)
        
        # Check if we should run immediately
        if len(sys.argv) > 1 and sys.argv[1] == '--run-now':
            logging.info("Running immediate scraping cycle...")
            scraper.run_scraping_cycle()
            return
        
        # Run scheduled jobs
        logging.info("JobSpy Data Collector is running. Press Ctrl+C to stop.")
        
        # Run once immediately if no schedule is set
        cron_config = config_manager.config.get('cron_schedule', {})
        if not cron_config.get('enabled', False):
            logging.info("No scheduling enabled, running once immediately...")
            scraper.run_scraping_cycle()
        else:
            # Keep the scheduler running
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
    
    except KeyboardInterrupt:
        logging.info("JobSpy Data Collector stopped by user")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        # Cleanup
        if 'scraper' in locals():
            scraper.mq_handler.disconnect()
        logging.info("JobSpy Data Collector shutdown complete")


if __name__ == "__main__":
    main()