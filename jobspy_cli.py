#!/usr/bin/env python3
"""
JobSpy CLI Management Tool
Provides command-line interface for managing JobSpy data collector
"""

import json
import sys
import subprocess
from pathlib import Path


def load_config():
    """Load configuration from config.json"""
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("❌ config.json not found. Please create one first.")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in config.json: {e}")
        return None


def show_status():
    """Show current configuration status"""
    config = load_config()
    if not config:
        return
    
    print("📊 JobSpy Configuration Status")
    print("=" * 40)
    
    # Job search config
    job_config = config.get('job_search', {})
    print(f"🔍 Search Terms: {len(job_config.get('search_terms', []))}")
    print(f"📍 Locations: {len(job_config.get('locations', []))}")
    print(f"🌐 Sites: {', '.join(job_config.get('sites', []))}")
    print(f"📈 Results per search: {job_config.get('results_wanted', 0)}")
    
    # Cron schedule
    cron_config = config.get('cron_schedule', {})
    print(f"\n⏰ Scheduling: {'Enabled' if cron_config.get('enabled') else 'Disabled'}")
    if cron_config.get('enabled'):
        print(f"   Schedule: {cron_config.get('schedule', 'Not set')}")
        print(f"   Description: {cron_config.get('description', 'No description')}")
    
    # ActiveMQ config
    mq_config = config.get('messaging', {}).get('activemq', {})
    print(f"\n📨 ActiveMQ: {'Enabled' if mq_config.get('enabled') else 'Disabled'}")
    if mq_config.get('enabled'):
        print(f"   Host: {mq_config.get('host', 'localhost')}:{mq_config.get('port', 61616)}")
        print(f"   Queue: {mq_config.get('queue_name', 'job_updates')}")
    
    # Recruiter info
    recruiter_info = config.get('recruiter_info', {})
    profile = recruiter_info.get('profile', {})
    print(f"\n👤 Recruiter Profile:")
    print(f"   Name: {profile.get('name', 'Not set')}")
    print(f"   Title: {profile.get('title', 'Not set')}")
    print(f"   Experience: {profile.get('experience_years', 0)} years")
    print(f"   Skills: {len(profile.get('skills', []))} listed")


def run_scraper(immediate=False):
    """Run the JobSpy scraper"""
    try:
        if immediate:
            print("🚀 Running JobSpy scraper immediately...")
            subprocess.run([sys.executable, "main.py", "--run-now"], check=True)
        else:
            print("🚀 Starting JobSpy scraper with scheduling...")
            subprocess.run([sys.executable, "main.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running scraper: {e}")
    except KeyboardInterrupt:
        print("\n⏹️  Scraper stopped by user")


def install_dependencies():
    """Install required dependencies"""
    print("📦 Installing dependencies...")
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], check=True)
        print("✅ Dependencies installed successfully")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error installing dependencies: {e}")


def update_recruiter_info():
    """Interactive update of recruiter information"""
    config = load_config()
    if not config:
        return
    
    print("✏️  Update Recruiter Information")
    print("=" * 30)
    
    recruiter_info = config.get('recruiter_info', {})
    profile = recruiter_info.get('profile', {})
    contact = recruiter_info.get('contact_methods', {})
    
    # Update profile
    name = input(f"Name ({profile.get('name', '')}): ").strip()
    if name:
        profile['name'] = name
    
    title = input(f"Title ({profile.get('title', '')}): ").strip()
    if title:
        profile['title'] = title
    
    try:
        experience = input(f"Years of experience ({profile.get('experience_years', 0)}): ").strip()
        if experience:
            profile['experience_years'] = int(experience)
    except ValueError:
        print("Invalid number for experience years")
    
    # Update contact info
    email = input(f"Email ({contact.get('email', '')}): ").strip()
    if email:
        contact['email'] = email
    
    linkedin = input(f"LinkedIn ({contact.get('linkedin', '')}): ").strip()
    if linkedin:
        contact['linkedin'] = linkedin
    
    phone = input(f"Phone ({contact.get('phone', '')}): ").strip()
    if phone:
        contact['phone'] = phone
    
    # Update skills
    skills_input = input(f"Skills (comma-separated, current: {', '.join(profile.get('skills', []))}): ").strip()
    if skills_input:
        profile['skills'] = [skill.strip() for skill in skills_input.split(',')]
    
    # Save updated config
    config['recruiter_info']['profile'] = profile
    config['recruiter_info']['contact_methods'] = contact
    
    try:
        with open('config.json', 'w') as f:
            json.dump(config, f, indent=2)
        print("✅ Recruiter information updated successfully")
    except Exception as e:
        print(f"❌ Error saving config: {e}")


def analyze_leads():
    """Run ETL leads analysis"""
    try:
        print("🔍 Running ETL leads analysis...")
        subprocess.run([sys.executable, "analyze_leads.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running analysis: {e}")
    except FileNotFoundError:
        print("❌ analyze_leads.py not found")

def show_leads_summary():
    """Show summary of collected leads"""
    try:
        from pathlib import Path
        import json
        
        results_dir = Path("./job_results")
        if not results_dir.exists():
            print("❌ No job results found. Run 'jobspy_cli.py run-now' first.")
            return
        
        json_files = list(results_dir.glob("jobs_*.json"))
        if not json_files:
            print("❌ No job result files found.")
            return
        
        total_leads = 0
        high_priority = 0
        companies = set()
        
        for file_path in json_files:
            with open(file_path, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    total_leads += len(data)
                    for lead in data:
                        if lead.get('lead_score', 0) >= 70:
                            high_priority += 1
                        companies.add(lead.get('company', 'Unknown'))
        
        print("📊 ETL Leads Summary")
        print("=" * 25)
        print(f"Total Leads: {total_leads}")
        print(f"High Priority: {high_priority}")
        print(f"Companies: {len(companies)}")
        print(f"Latest Collection: {max(json_files).name}")
        
    except Exception as e:
        print(f"❌ Error reading leads data: {e}")

def show_help():
    """Show help information"""
    print("ETL JobSpy CLI Management Tool")
    print("=" * 35)
    print("Available commands:")
    print("  status     - Show current configuration status")
    print("  run        - Start JobSpy with scheduling")
    print("  run-now    - Run JobSpy immediately (one-time)")
    print("  install    - Install required dependencies")
    print("  update     - Update recruiter information")
    print("  analyze    - Analyze collected ETL job leads")
    print("  summary    - Show quick summary of collected leads") 
    print("  help       - Show this help message")
    print("\nETL-Focused Commands:")
    print("  🎯 This tool is optimized for ETL job lead collection")
    print("  📊 Use 'analyze' to identify high-value prospects")
    print("  💼 Focus on companies actively hiring ETL talent")


def main():
    """Main CLI entry point"""
    if len(sys.argv) < 2:
        show_help()
        return
    
    command = sys.argv[1].lower()
    
    if command == "status":
        show_status()
    elif command == "run":
        run_scraper()
    elif command == "run-now":
        run_scraper(immediate=True)
    elif command == "install":
        install_dependencies()
    elif command == "update":
        update_recruiter_info()
    elif command == "analyze":
        analyze_leads()
    elif command == "summary":
        show_leads_summary()
    elif command == "help":
        show_help()
    else:
        print(f"❌ Unknown command: {command}")
        show_help()


if __name__ == "__main__":
    main()