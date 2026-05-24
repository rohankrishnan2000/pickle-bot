"""Shared yourcourts.com client used by snipe_court.py, book_court.py, and the Telegram bot."""

import os
import re
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://www.yourcourts.com"

EMAIL = os.environ.get("YC_EMAIL", "")
PASSWORD = os.environ.get("YC_PASSWORD", "")

FACILITY_ID = "1535"
DURATION = "30"
RESERVATION_TYPE_ID = "1200"

TIME_ID_BASE = 13
TIME_LABELS: list[str] = []
for _h in range(7, 22):
    for _m in (0, 30):
        _suffix = "AM" if _h < 12 else "PM"
        _display_h = _h if _h <= 12 else _h - 12
        if _display_h == 0:
            _display_h = 12
        TIME_LABELS.append(f"{_display_h}:{_m:02d}{_suffix}")


def time_to_id(time_str: str) -> int:
    try:
        return TIME_LABELS.index(time_str) + TIME_ID_BASE
    except ValueError:
        return -1


def extract_csrf(html: str) -> str | None:
    m = re.search(r'name="SYNCHRONIZER_TOKEN"[^>]*value="([^"]+)"', html)
    if not m:
        m = re.search(r'value="([^"]+)"[^>]*name="SYNCHRONIZER_TOKEN"', html)
    return m.group(1) if m else None


def extract_field(html: str, name: str) -> str | None:
    m = re.search(rf'name="{name}"[^>]*value="([^"]*)"', html)
    if not m:
        m = re.search(rf'value="([^"]*)"[^>]*name="{name}"', html)
    return m.group(1) if m else None


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/148.0.0.0 Safari/537.36"
        ),
    })
    return session


def login(session: requests.Session) -> bool:
    if not EMAIL or not PASSWORD:
        raise RuntimeError("YC_EMAIL / YC_PASSWORD not set in environment")
    session.get(f"{BASE_URL}/login")
    resp = session.post(
        f"{BASE_URL}/security/login/form-login",
        data={"username": EMAIL, "password": PASSWORD, "rememberMe": "on"},
        allow_redirects=True,
    )
    return "showLogin" not in resp.url


def find_slots(
    session: requests.Session,
    date: str,
    target_time: str | None = None,
    target_court: str | None = None,
) -> list[dict]:
    """Fetch slots for `date`. If target_time/target_court are given, filter to matches."""
    resp = session.get(
        f"{BASE_URL}/facility/viewschedule",
        params={"reservationDate": date, "facility_id": ""},
    )
    resp.raise_for_status()

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
        if target_time and slot_time != target_time:
            continue
        if target_court and court != target_court:
            continue
        qs = parse_qs(urlparse(href).query)
        slots.append({
            "court": court,
            "time": slot_time,
            "resource_id": qs.get("reservableResourceId", [""])[0],
            "token": qs.get("token", [""])[0],
            "href": href,
        })

    return slots


def book_slot(session: requests.Session, slot: dict, date: str) -> tuple[bool, str]:
    """Book a slot. Returns (success, message)."""
    form_url = f"{BASE_URL}{slot['href']}"
    form_resp = session.get(form_url)
    form_resp.raise_for_status()

    csrf = extract_csrf(form_resp.text)
    owner_id = extract_field(form_resp.text, "ownerId")
    start_time_id = extract_field(form_resp.text, "startTimeId")
    if not start_time_id:
        tid = time_to_id(slot["time"])
        if tid > 0:
            start_time_id = str(tid)

    if not csrf or not owner_id or not start_time_id:
        return False, "Failed to parse reservation form"

    resp = session.post(
        f"{BASE_URL}/reservation/bookcourtreservation",
        data={
            "SYNCHRONIZER_TOKEN": csrf,
            "SYNCHRONIZER_URI": "/reservation/newreservation/",
            "reservationDate": date,
            "startTimeId": start_time_id,
            "facilityId": FACILITY_ID,
            "facility_id": "",
            "ownerId": owner_id,
            "secondaryOwnerId": "",
            "reservableResourceId": slot["resource_id"],
            "reservationTypeId": RESERVATION_TYPE_ID,
            "duration": DURATION,
            "tennisProId": "",
            "additionalPlayersId": "",
            "guests": "",
            "playerGroupId": "",
            "allowPlayerAddRemove": "false",
            "ownerAssumedPlayer": "true",
        },
        headers={"Origin": BASE_URL, "Referer": form_url},
        allow_redirects=False,
    )

    if resp.status_code == 302:
        return True, f"Booked {slot['court']} @ {slot['time']} on {date}"

    alerts = re.findall(r'class="alert[^"]*"[^>]*>(.*?)</div>', resp.text, re.DOTALL)
    for a in alerts:
        clean = re.sub(r"<[^>]+>", "", a).strip()
        if clean:
            return False, clean
    return False, f"Booking failed (status {resp.status_code})"
