"""Non-interactive snipe runner. Reads TARGET_DATE and TARGET_TIME from env or args.

Usage:
    python3 snipe_headless.py 06/14/2026 7:00AM [duration_minutes]

Credentials are loaded from .env (YC_EMAIL / YC_PASSWORD).
Output goes to stdout/stderr — redirect in the calling shell.
"""

import datetime as dt
import os
import sys
import time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

import yourcourts

POLL_INTERVAL = 15
BOOKING_WINDOW_DAYS = 10
COURT_OPEN_HOUR = 7
COURT_TIMEZONE = os.environ.get("COURT_TIMEZONE", "America/Los_Angeles").strip()


def activation_time(target_date: str) -> dt.datetime:
    """Return the booking-window open time for the requested reservation date."""
    reservation_date = dt.datetime.strptime(target_date, "%m/%d/%Y").date()
    open_date = reservation_date - dt.timedelta(days=BOOKING_WINDOW_DAYS)
    try:
        tz = ZoneInfo(COURT_TIMEZONE) if COURT_TIMEZONE else dt.timezone.utc
    except ZoneInfoNotFoundError:
        tz = dt.timezone.utc
    return dt.datetime.combine(open_date, dt.time(hour=COURT_OPEN_HOUR), tzinfo=tz)


def main():
    """Run the unattended polling flow for shell scripts or cron-like callers."""
    if len(sys.argv) < 3:
        print("Usage: snipe_headless.py MM/DD/YYYY H:MMAM [duration_minutes]", file=sys.stderr)
        sys.exit(1)

    target_date = sys.argv[1]
    target_time = sys.argv[2]
    duration_min = int(sys.argv[3]) if len(sys.argv) > 3 else 60

    print(f"[{time.strftime('%H:%M:%S')}] Headless snipe: {target_time} on {target_date} ({duration_min}min)")

    activation = activation_time(target_date)
    now = dt.datetime.now(activation.tzinfo)
    wait = (activation - now).total_seconds()
    if wait > 0:
        label = activation.strftime("%m/%d/%Y %I:%M%p")
        print(f"[{time.strftime('%H:%M:%S')}] Court window opens at {label}; sleeping {wait:.0f}s...")
        time.sleep(wait)

    session = yourcourts.make_session()
    print(f"[{time.strftime('%H:%M:%S')}] Logging in...")
    if not yourcourts.login(session):
        print(f"[{time.strftime('%H:%M:%S')}] Login failed.", file=sys.stderr)
        sys.exit(1)
    print(f"[{time.strftime('%H:%M:%S')}] Login OK.")

    attempt = 0
    while True:
        attempt += 1
        ts = time.strftime("%H:%M:%S")

        try:
            matches = yourcourts.find_slots(session, target_date, target_time, None)
        except requests.RequestException as e:
            print(f"[{ts}] #{attempt} Network error: {e}")
            time.sleep(POLL_INTERVAL)
            continue

        if not matches:
            print(f"[{ts}] #{attempt} No openings.", flush=True)
            if attempt % 40 == 0:
                print(f"[{ts}] Refreshing session...")
                yourcourts.login(session)
            time.sleep(POLL_INTERVAL)
            continue

        print(f"[{ts}] #{attempt} Found {len(matches)} open slot(s)!")
        for s in matches:
            print(f"  → {s['court']} @ {s['time']}")

        for s in matches:
            print(f"  Booking {s['court']}...", flush=True)
            ok, msg = yourcourts.book_slot(session, s, target_date, duration_min)
            if ok:
                print(f"[{ts}] SUCCESS: {msg}")
                sys.exit(0)
            print(f"  failed ({msg}), trying next.")

        print(f"[{ts}] All attempts failed. Continuing to poll...")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
