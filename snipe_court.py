import datetime as dt
import os
import re
import time
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from dotenv import load_dotenv

load_dotenv()

try:
    import pickle_bot.yourcourts as yourcourts
except ModuleNotFoundError:
    import yourcourts

TARGET_COURT = None      # e.g. "PB 4B" or None for any court at that time
POLL_INTERVAL = 15       # seconds between checks
BOOKING_WINDOW_DAYS = 10
COURT_OPEN_HOUR = 7
COURT_TIMEZONE = os.environ.get("COURT_TIMEZONE", "America/Los_Angeles").strip()


def prompt_date() -> str:
    """Collect a valid reservation date from the terminal user."""
    while True:
        raw = input("Date to snipe (MM/DD/YYYY): ").strip()
        if re.fullmatch(r"\d{2}/\d{2}/\d{4}", raw):
            try:
                dt.datetime.strptime(raw, "%m/%d/%Y")
                return raw
            except ValueError:
                pass
        print("  Invalid format. Example: 05/25/2026")


def prompt_time() -> str:
    """Collect a valid target time in the same format the web client uses."""
    while True:
        raw = input("Time to snipe (e.g. 7:00AM, 10:30AM, 2:00PM): ").strip().upper().replace(" ", "")
        m = re.fullmatch(r"(\d{1,2}):(\d{2})(AM|PM)", raw)
        if m:
            hour, minute, meridiem = m.group(1), m.group(2), m.group(3)
            return f"{int(hour)}:{minute}{meridiem}"
        print("  Invalid format. Example: 10:00AM")


def activation_time_for(target_date: str) -> dt.datetime:
    """Return when the site's booking window opens for a reservation date."""
    reservation_date = dt.datetime.strptime(target_date, "%m/%d/%Y").date()
    open_date = reservation_date - dt.timedelta(days=BOOKING_WINDOW_DAYS)
    tz = dt.datetime.now().astimezone().tzinfo or dt.timezone.utc
    if COURT_TIMEZONE:
        try:
            tz = ZoneInfo(COURT_TIMEZONE)
        except ZoneInfoNotFoundError:
            pass
    return dt.datetime.combine(open_date, dt.time(hour=COURT_OPEN_HOUR), tzinfo=tz)


def main():
    """Run the standalone polling loop without Telegram or SQLite state."""
    target_date = prompt_date()
    target_time = prompt_time()

    court_label = TARGET_COURT or "any court"
    print(f"\nSniping: {target_time} on {target_date} — {court_label}")
    print(f"Polling every {POLL_INTERVAL}s\n")

    # Mirror the same delayed-start behavior used by the Telegram snipe jobs.
    activation = activation_time_for(target_date)
    now = dt.datetime.now(activation.tzinfo)
    if activation > now:
        label = activation.strftime("%m/%d/%Y %I:%M%p").replace(" 0", " ")
        print(f"Court window opens at {label}; waiting to start polling...")
        time.sleep((activation - now).total_seconds())

    session = yourcourts.make_session()

    print("Logging in...")
    if not yourcourts.login(session):
        print("Login failed.")
        return

    attempt = 0
    while True:
        attempt += 1
        ts = time.strftime("%H:%M:%S")

        try:
            matches = yourcourts.find_slots(session, target_date, target_time, TARGET_COURT)
        except requests.RequestException as e:
            print(f"[{ts}] #{attempt} Network error: {e}")
            time.sleep(POLL_INTERVAL)
            continue

        if not matches:
            print(f"[{ts}] #{attempt} No openings.", end="", flush=True)
            if attempt % 40 == 0:
                print(" (refreshing session)", end="")
                yourcourts.login(session)
            print()
            time.sleep(POLL_INTERVAL)
            continue

        print(f"[{ts}] #{attempt} Found {len(matches)} open slot(s)!")
        for s in matches:
            print(f"  → {s['court']} @ {s['time']}")

        for s in matches:
            print(f"  Booking {s['court']}...", end=" ", flush=True)
            ok, msg = yourcourts.book_slot(session, s, target_date)
            if ok:
                print(msg)
                return
            print(f"failed ({msg}), trying next.")

        print("  All attempts failed. Continuing to poll...")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
