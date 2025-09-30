# Local Setup Guide - JobSpy ETL Data Collector

This guide provides step-by-step instructions to replicate the JobSpy ETL Data Collector system on your local machine.

## System Overview

The system consists of:

1. **JobSpy Scraper** - Fetches job listings and saves to CSV
2. **ActiveMQ Artemis Broker** - Message queue running in Docker
3. **Queue Listener** - Monitors notifications from the scraper

## Prerequisites

- **Operating System**: Linux (Ubuntu/Debian preferred) or macOS
- **Docker**: Version 20.0+ with Docker Compose
- **Python**: Version 3.8 or higher
- **Conda**: Anaconda or Miniconda (recommended)
- **Git**: For cloning the repository
- **Internet Connection**: Required for job board scraping

## Step 1: System Preparation

### Install Docker

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install docker.io docker-compose-plugin
sudo systemctl start docker
sudo systemctl enable docker

# Add your user to docker group (avoid sudo)
sudo usermod -aG docker $USER
# Log out and back in, or run:
newgrp docker
```

### Install Conda (if not already installed)

```bash
# Download Miniconda
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
# Follow installation prompts, then restart terminal
```

### Verify Prerequisites

```bash
# Check Docker
docker --version
docker compose version

# Check Python
python3 --version

# Check Conda
conda --version
```

## Step 2: Project Setup

### Clone the Repository

### Create Python Environment

```bash
# Create conda environment
conda create -n jobspy-env python=3.9 -y

# Activate environment
conda activate jobspy-env

# Verify activation (should show jobspy-env in prompt)
which python
```

## Step 3: Install Dependencies

Install Python Packages

```bash
# Make sure you're in the jobspy-env environment
conda activate jobspy-env

# Install packages
pip install -r requirements.txt

# Verify installation
python -c "import jobspy, stomp, pandas, schedule; print('All packages installed successfully')"

# Install Playwright browsers (BuiltIn integration)
playwright install firefox
```

## Step 4: Docker Configuration

### Create Environment File

```bash
cat > .env << 'EOF'
ARTEMIS_USER="sample"
ARTEMIS_PASSWORD="sample"
EOF
```

### Start Docker Services

```bash
# Start ActiveMQ Artemis
sudo docker compose up -d

# Verify container is running
sudo docker ps

# Check logs (optional)
sudo docker logs activemq-artemis-queue
```

## Step 5: Running the System

Now you're ready to run the complete system with two terminals.

### Terminal 1: Start the Queue Listener (Run this first)

```bash
# Navigate to project directory
cd /path/to/DataColletor

# Activate conda environment
conda activate jobspy-env

# Start the continuous listener
python listen_queue.py
```

You should see:

```
Listening for messages on /queue/etl_job_leads (no timeout - continuous)...
Press Ctrl+C to stop
```

### Terminal 2: Start the Job Scraper

Open a new terminal and run:

```bash
# Navigate to project directory
cd /path/to/DataColletor

# Activate conda environment
conda activate jobspy-env

# Start the scraper (runs every 6 hours)
python main.py
```

You should see:

```
Connected to ActiveMQ at localhost:61616
Scheduled to run every 6 hours
JobSpy Data Collector is running. Press Ctrl+C to stop.
```

### Hacker News Hiring (HN Hiring) Source

The collector now also pulls matching roles from hnhiring.com. Configuration lives in the new `hnhiring` block of `config.json`:

```
"hnhiring": {
  "enabled": true,
  "categories": ["/locations/remote"],
  "days": 14,
  "min_salary": null,
  "max_salary": null
}
```

- Set `enabled` to `false` to disable this source.
- Adjust `categories` to target different HN collections (for example `/technologies/python`).
- Use `days`, `min_salary`, and `max_salary` to narrow the feed.

### BuiltIn.com Source

The scraper now collects additional roles directly from BuiltIn.com using Playwright-driven browsing. Configuration lives in the `builtin` block of `config.json`:

```
"builtin": {
  "enabled": true,
  "keywords": [],
  "max_age_hours": 72,
  "per_keyword_limit": 20,
  "total_limit": 60,
  "headless": true,
  "page_wait_ms": 500,
  "detail_wait_ms": 1500
}
```

- Leave `keywords` empty to reuse the JobSpy search terms, or provide a custom list.
- `per_keyword_limit` caps detailed scrapes per query; `total_limit` caps the entire BuiltIn run.
- `headless`, `page_wait_ms`, and `detail_wait_ms` control Playwright behaviour (tweak if pages load slowly).
- Run `playwright install firefox` once per machine so the collector has a browser available.

## Step 6: Verify Everything Works

After setup, ensure all components communicate and function as expected:

1. **Listener**: Check Terminal 1 for any error messages. It should be listening without interruptions.
2. **Scraper**: In Terminal 2, ensure the scraper runs without issues and connects to ActiveMQ.
3. **Docker**: Use `sudo docker ps` to confirm all necessary containers are active.
4. **Logs**: Monitor logs in both terminals. No critical errors should appear.

Once verified, the system is ready for use. The JobSpy ETL Data Collector will scrape, process, and store job listings as configured.
