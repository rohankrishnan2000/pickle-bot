"""Benchmark the full manual reservation flow against the live site.

This is a measurement helper rather than an app entrypoint: it times the same
HTTP sequence the real client uses so you can see where latency is being spent
during login, schedule fetch, form load, and booking POST.
"""

import os
import re
import time
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://www.yourcourts.com"

EMAIL = os.environ["YC_EMAIL"]
PASSWORD = os.environ["YC_PASSWORD"]

FACILITY_ID = "1535"
RESERVATION_DATE = "05/24/2026"
DURATION = "30"
RESERVATION_TYPE_ID = "1200"


def extract_csrf(html: str) -> str | None:
    """Extract the CSRF token required by the final booking POST."""
    m = re.search(r'name="SYNCHRONIZER_TOKEN"[^>]*value="([^"]+)"', html)
    if not m:
        m = re.search(r'value="([^"]+)"[^>]*name="SYNCHRONIZER_TOKEN"', html)
    return m.group(1) if m else None


def extract_field(html: str, name: str) -> str | None:
    """Extract a hidden input value from the reservation form HTML."""
    m = re.search(rf'name="{name}"[^>]*value="([^"]*)"', html)
    if not m:
        m = re.search(rf'value="([^"]*)"[^>]*name="{name}"', html)
    return m.group(1) if m else None


def bench():
    """Replay the live booking sequence and print step-level timings."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/148.0.0.0 Safari/537.36"
        ),
    })

    timings = {}
    total_start = time.perf_counter()

    # The numbered phases line up with the same browser-ish flow used in
    # yourcourts.login(), find_slots(), and book_slot().
    # 1. GET login page
    t = time.perf_counter()
    session.get(f"{BASE_URL}/login")
    timings["1_login_page"] = time.perf_counter() - t

    # 2. POST login
    t = time.perf_counter()
    resp = session.post(f"{BASE_URL}/security/login/form-login", data={
        "username": EMAIL,
        "password": PASSWORD,
        "rememberMe": "on",
    }, allow_redirects=True)
    timings["2_login_post"] = time.perf_counter() - t

    if "showLogin" in resp.url:
        print("Login failed.")
        return

    # 3. GET schedule
    t = time.perf_counter()
    resp = session.get(f"{BASE_URL}/facility/viewschedule", params={
        "reservationDate": RESERVATION_DATE,
        "facility_id": "",
    })
    timings["3_schedule"] = time.perf_counter() - t

    # pick first available slot
    slots = re.findall(
        r"href='(/reservation/newreservation/\?[^']*)'\s+title='([^']*)'",
        resp.text,
    )
    if not slots:
        print("No open slots.")
        return

    href, title = slots[0]
    qs = parse_qs(urlparse(href).query)
    resource_id = qs.get("reservableResourceId", [""])[0]

    # 4. GET reservation form
    t = time.perf_counter()
    form_resp = session.get(f"{BASE_URL}{href}")
    timings["4_reservation_form"] = time.perf_counter() - t

    csrf = extract_csrf(form_resp.text)
    owner_id = extract_field(form_resp.text, "ownerId")
    start_time_id = extract_field(form_resp.text, "startTimeId")

    if not csrf or not owner_id or not start_time_id:
        print("Failed to parse reservation form.")
        return

    # 5. POST booking (don't follow redirect)
    t = time.perf_counter()
    book_resp = session.post(
        f"{BASE_URL}/reservation/bookcourtreservation",
        data={
            "SYNCHRONIZER_TOKEN": csrf,
            "SYNCHRONIZER_URI": "/reservation/newreservation/",
            "reservationDate": RESERVATION_DATE,
            "startTimeId": start_time_id,
            "facilityId": FACILITY_ID,
            "facility_id": "",
            "ownerId": owner_id,
            "secondaryOwnerId": "",
            "reservableResourceId": resource_id,
            "reservationTypeId": RESERVATION_TYPE_ID,
            "duration": DURATION,
            "tennisProId": "",
            "additionalPlayersId": "",
            "guests": "",
            "playerGroupId": "",
            "allowPlayerAddRemove": "false",
            "ownerAssumedPlayer": "true",
        },
        headers={
            "Origin": BASE_URL,
            "Referer": f"{BASE_URL}{href}",
        },
        allow_redirects=False,
    )
    timings["5_book_post"] = time.perf_counter() - t

    total = time.perf_counter() - total_start
    booked = book_resp.status_code == 302

    print(f"Slot:   {title}")
    print(f"Booked: {'yes' if booked else 'no'} (HTTP {book_resp.status_code})")
    print()
    print(f"{'Step':<25} {'Time (ms)':>10}")
    print("-" * 37)
    for step, dt in timings.items():
        print(f"{step:<25} {dt*1000:>10.0f}")
    print("-" * 37)
    print(f"{'TOTAL':<25} {total*1000:>10.0f}")


if __name__ == "__main__":
    bench()
