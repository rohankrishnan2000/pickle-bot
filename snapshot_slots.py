"""Snapshot available slots for a date — dumps raw HTML and parsed slots.

Usage:
    python3 snapshot_slots.py MM/DD/YYYY OUTPUT_DIR [label]
"""

import os
import sys
import time

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()

import yourcourts


def main():
    """Capture one schedule page plus a parsed slot dump for debugging parsers."""
    if len(sys.argv) < 3:
        print("Usage: snapshot_slots.py MM/DD/YYYY OUTPUT_DIR [label]", file=sys.stderr)
        sys.exit(1)

    target_date = sys.argv[1]
    output_dir = sys.argv[2]
    label = sys.argv[3] if len(sys.argv) > 3 else "snapshot"
    ts = time.strftime("%H%M%S")

    os.makedirs(output_dir, exist_ok=True)
    html_file = os.path.join(output_dir, f"{label}_{ts}_raw.html")
    parsed_file = os.path.join(output_dir, f"{label}_{ts}_slots.txt")

    session = yourcourts.make_session()
    if not yourcourts.login(session):
        print(f"[{label}] Login failed", file=sys.stderr)
        sys.exit(1)

    resp = session.get(
        f"{yourcourts.BASE_URL}/facility/viewschedule",
        params={"reservationDate": target_date, "facility_id": ""},
    )
    resp.raise_for_status()

    # Save the original schedule response so parser changes can be inspected later.
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(resp.text)

    # Repeat the same parsing shape as yourcourts.find_slots, but keep the
    # output in a file so humans can compare raw HTML to the extracted slots.
    import re
    from urllib.parse import parse_qs, urlparse

    raw = re.findall(
        r"href='(/reservation/newreservation/\?[^']*)'\s+title='([^']*)'",
        resp.text,
    )
    slots = []
    for href, title in raw:
        parts = title.rsplit(" @ ", 1)
        if len(parts) != 2:
            continue
        court, slot_time = parts
        qs = parse_qs(urlparse(href).query)
        slots.append({
            "court": court,
            "time": slot_time,
            "resource_id": qs.get("reservableResourceId", [""])[0],
            "token": qs.get("token", [""])[0],
            "href": href,
        })

    with open(parsed_file, "w", encoding="utf-8") as f:
        f.write(f"=== {label} @ {time.strftime('%H:%M:%S')} — {target_date} ===\n")
        f.write(f"Total slots found: {len(slots)}\n\n")
        for s in slots:
            f.write(f"{s}\n")

    print(f"[{label}] HTML -> {html_file}")
    print(f"[{label}] Slots -> {parsed_file} ({len(slots)} slots)")


if __name__ == "__main__":
    main()
