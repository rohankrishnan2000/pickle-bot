import re
import time

import requests

try:
    import pickle_bot.yourcourts as yourcourts
except ModuleNotFoundError:
    import yourcourts

TARGET_COURT = None      # e.g. "PB 4B" or None for any court at that time
POLL_INTERVAL = 15       # seconds between checks


def prompt_date() -> str:
    while True:
        raw = input("Date to snipe (MM/DD/YYYY): ").strip()
        if re.fullmatch(r"\d{2}/\d{2}/\d{4}", raw):
            return raw
        print("  Invalid format. Example: 05/25/2026")


def prompt_time() -> str:
    while True:
        raw = input("Time to snipe (e.g. 7:00AM, 10:30AM, 2:00PM): ").strip().upper().replace(" ", "")
        m = re.fullmatch(r"(\d{1,2}):(\d{2})(AM|PM)", raw)
        if m:
            hour, minute, meridiem = m.group(1), m.group(2), m.group(3)
            return f"{int(hour)}:{minute}{meridiem}"
        print("  Invalid format. Example: 10:00AM")


def main():
    target_date = prompt_date()
    target_time = prompt_time()

    court_label = TARGET_COURT or "any court"
    print(f"\nSniping: {target_time} on {target_date} — {court_label}")
    print(f"Polling every {POLL_INTERVAL}s\n")

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
