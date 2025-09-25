#!/usr/bin/env python3
"""One-shot listener for the ActiveMQ etl_job_leads queue."""

import argparse
import json
import os
import threading
import time
from typing import Optional

import stomp

def load_env_vars():
    """Load environment variables from .env file"""
    env_vars = {}
    try:
        with open('.env', 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Remove quotes if present
                    value = value.strip('"\'')
                    env_vars[key] = value
                    os.environ[key] = value
    except FileNotFoundError:
        print("Warning: .env file not found")
    return env_vars

# Load environment variables
load_env_vars()

QUEUE_DESTINATION = "/queue/etl_job_leads"
BROKER_HOST = "localhost"
BROKER_PORT = 61613
USERNAME = os.getenv("ARTEMIS_USER", "sample")
PASSWORD = os.getenv("ARTEMIS_PASSWORD", "sample")


class SingleMessageListener(stomp.ConnectionListener):
    def __init__(self) -> None:
        self._event = threading.Event()
        self.body: Optional[str] = None
        self.headers: Optional[dict] = None
        self.error: Optional[Exception] = None

    def on_error(self, frame):  # type: ignore[override]
        details = {
            "headers": dict(frame.headers),
            "body": frame.body,
        }
        self.error = Exception(json.dumps(details, indent=2))
        self._event.set()

    def on_message(self, frame):  # type: ignore[override]
        self.headers = dict(frame.headers)
        self.body = frame.body
        self._event.set()

    def wait(self, timeout: Optional[float]) -> bool:
        return self._event.wait(timeout)


def parse_args():
    parser = argparse.ArgumentParser(description="Continuously listen for messages on the etl_job_leads queue")
    parser.add_argument("--timeout", type=float, default=None, help="Optional timeout in seconds (default: no timeout)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    listener = SingleMessageListener()
    # Add heartbeat to keep connection alive (send heartbeat every 30 seconds, expect one every 60 seconds)
    conn = stomp.Connection([(BROKER_HOST, BROKER_PORT)], heartbeats=(30000, 60000))
    conn.set_listener("one-shot", listener)
    conn.connect(USERNAME, PASSWORD, wait=True)
    conn.subscribe(destination=QUEUE_DESTINATION, id="listener-1", ack="auto")

    if args.timeout:
        print(f"Listening for messages on {QUEUE_DESTINATION} (timeout: {args.timeout}s)...")
    else:
        print(f"Listening for messages on {QUEUE_DESTINATION} (no timeout - continuous)...")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            # If no timeout specified, wait indefinitely
            wait_time = args.timeout if args.timeout else None
            
            if listener.wait(wait_time):
                if listener.error:
                    print("Received error frame:")
                    print(listener.error)
                    # Reset for next message
                    listener.error = None
                    listener._event.clear()
                    continue

                print("Received message headers:")
                print(json.dumps(listener.headers or {}, indent=2))
                print("\nReceived message body:")
                print(listener.body)
                print("\n" + "="*50)
                print("Waiting for next message...")
                
                # Reset for next message
                listener.body = None
                listener.headers = None
                listener._event.clear()
            else:
                # Only show timeout message if timeout was specified
                if args.timeout:
                    print(f"No message received within {args.timeout} seconds, continuing to listen...")
    except KeyboardInterrupt:
        print("\nStopping listener...")
        return 0
    finally:
        time.sleep(0.5)  # allow ack to flush
        conn.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
