"""HTTP client for the yourcourts.com reservation flow.

This module is the lowest layer in the project. Every higher-level workflow
eventually comes through here:

1. create a browser-like :class:`requests.Session`
2. log in with account credentials
3. fetch schedule pages and parse open slots
4. load the reservation form for a chosen slot
5. submit the booking or cancellation request

The CLI scripts call these helpers directly, and the Telegram bot wraps them in
async jobs via :mod:`snipe_job`.
"""

import logging
import os
import re
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse

import requests

# Child logger — records flow to the "snipe" root logger configured in snipe_job.
# Do NOT add handlers here; snipe_job owns the handler setup.
logger = logging.getLogger("snipe.yourcourts")

BASE_URL = "https://www.yourcourts.com"


class SessionExpired(requests.RequestException):
    """Raised when a request is redirected to the login page (session not authenticated)."""

FACILITY_ID = "1535"
RESERVATION_TYPE_ID = "1200"

TIME_ID_BASE = 13


def time_to_id(time_str: str) -> int:
    """Convert a human time like ``"7:30AM"`` into YourCourts' slot id.

    The rest of the codebase uses this to sort slots chronologically and to
    reconstruct a ``startTimeId`` when the reservation form omits it.
    """
    m = re.fullmatch(r"(\d{1,2}):(\d{2})(AM|PM)", time_str.strip().upper())
    if not m:
        return -1

    hour = int(m.group(1))
    minute = int(m.group(2))
    meridiem = m.group(3)
    if hour < 1 or hour > 12 or minute not in (0, 30):
        return -1

    hour_24 = hour % 12
    if meridiem == "PM":
        hour_24 += 12

    minutes = hour_24 * 60 + minute
    first_slot = 7 * 60
    last_slot = 21 * 60 + 30
    if minutes < first_slot or minutes > last_slot:
        return -1

    return TIME_ID_BASE + (minutes - first_slot) // 30


def extract_csrf(html: str) -> str | None:
    """Pull the reservation form CSRF token out of raw HTML."""
    m = re.search(r'name="SYNCHRONIZER_TOKEN"[^>]*value="([^"]+)"', html)
    if not m:
        m = re.search(r'value="([^"]+)"[^>]*name="SYNCHRONIZER_TOKEN"', html)
    return m.group(1) if m else None


def extract_field(html: str, name: str) -> str | None:
    """Extract a hidden input value by field name from a reservation form."""
    m = re.search(rf'name="{name}"[^>]*value="([^"]*)"', html)
    if not m:
        m = re.search(rf'value="([^"]*)"[^>]*name="{name}"', html)
    return m.group(1) if m else None


def make_session() -> requests.Session:
    """Create a session that looks enough like a normal browser to avoid friction."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/148.0.0.0 Safari/537.36"
        ),
    })
    return session


def login(session: requests.Session, email: str | None = None, password: str | None = None) -> bool:
    """Authenticate a session for all later schedule, booking, and cancel calls."""
    email = email or os.environ.get("YC_EMAIL", "")
    password = password or os.environ.get("YC_PASSWORD", "")
    if not email or not password:
        raise RuntimeError("No credentials provided and YC_EMAIL/YC_PASSWORD not set in environment")
    session.get(f"{BASE_URL}/login")
    resp = session.post(
        f"{BASE_URL}/security/login/form-login",
        data={"username": email, "password": password, "rememberMe": "on"},
        allow_redirects=True,
    )
    return "showLogin" not in resp.url


def find_slots(
    session: requests.Session,
    date: str,
    target_time: str | None = None,
    target_court: str | None = None,
) -> list[dict]:
    """Fetch and parse currently open reservation slots for a date.

    This is the discovery step for both main workflows:

    - direct booking: the bot or CLI lists whatever is open right now
    - sniping: the polling loop repeatedly calls this until the desired
      configuration appears
    """
    resp = session.get(
        f"{BASE_URL}/facility/viewschedule",
        params={"reservationDate": date, "facility_id": ""},
    )
    resp.raise_for_status()
    if "showLogin" in resp.url:
        raise SessionExpired("Redirected to login while fetching schedule")

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


def book_slot(session: requests.Session, slot: dict, date: str, duration_min: int = 30) -> tuple[bool, str]:
    """Submit the full reservation form for an open slot.

    This function is the final step in every successful path. Higher-level code
    decides *which* slot or slot-group to take; this helper performs the actual
    POST and translates the response into a ``(success, message)`` tuple that
    callers can surface to the user.
    """
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

    try:
        logger.info(
            "book_slot PRE-POST — court=%r time=%r date=%r duration=%d "
            "reservableResourceId=%r startTimeId=%r",
            slot.get("court"), slot.get("time"), date, duration_min,
            slot.get("resource_id"), start_time_id,
        )
    except Exception:
        pass

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
            "duration": str(duration_min),
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
        try:
            location = resp.headers.get("Location", "")
            logger.info(
                "book_slot POST 302 — court=%r time=%r date=%r duration=%d "
                "status=%d Location=%r",
                slot.get("court"), slot.get("time"), date, duration_min,
                resp.status_code, location,
            )
        except Exception:
            pass
        return True, f"Booked {slot['court']} @ {slot['time']} for {duration_min}m on {date}"

    alerts = re.findall(r'class="alert[^"]*"[^>]*>(.*?)</div>', resp.text, re.DOTALL)
    alert_texts: list[str] = []
    for a in alerts:
        clean = re.sub(r"<[^>]+>", "", a).strip()
        if clean:
            alert_texts.append(clean)
    try:
        logger.warning(
            "book_slot POST FAILED — court=%r time=%r date=%r duration=%d "
            "status=%d alerts=%r",
            slot.get("court"), slot.get("time"), date, duration_min,
            resp.status_code, alert_texts,
        )
    except Exception:
        pass
    if alert_texts:
        return False, alert_texts[0]
    return False, f"Booking failed (status {resp.status_code})"


def list_my_bookings(session: requests.Session, days_ahead: int = 30) -> list[dict]:
    """Fetch the current user's reservations for the Telegram ``/bookings`` flow."""
    now = datetime.now().astimezone()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=days_ahead)

    resp = session.get(
        f"{BASE_URL}/facility/getevents",
        params={
            "thisMemberOnly": "true",
            "facility_id": "",
            "readOnlyViewer": "false",
            "start": start.isoformat(timespec="seconds"),
            "end": end.isoformat(timespec="seconds"),
        },
        headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"},
    )
    resp.raise_for_status()
    events = resp.json() or []
    return [{
        "id": e["id"],
        "court": e.get("resourceName", "?"),
        "times": e.get("eventTimes", "?"),
        "start": e.get("start", ""),
        "event_type": e.get("eventType", ""),
        "is_past": bool(e.get("isPast", False)),
    } for e in events if e.get("id")]


def cancel_reservation(session: requests.Session, reservation_id: str) -> tuple[bool, str]:
    """Cancel a reservation chosen from ``list_my_bookings``.

    The bot uses this after the user confirms a cancellation button in the
    ``/bookings`` callback flow.
    """
    preview = session.get(f"{BASE_URL}/reservation/previewCancelReservation/{reservation_id}",
                          params={"facility_id": ""})
    if preview.status_code != 200:
        return False, f"Could not load cancel page (status {preview.status_code})"

    resp = session.post(
        f"{BASE_URL}/reservation/cancelReservation",
        data={
            "id": str(reservation_id),
            "facility_id": "",
            "reservationCancellationTypeId": "5",
        },
        headers={
            "Origin": BASE_URL,
            "Referer": f"{BASE_URL}/reservation/previewCancelReservation/{reservation_id}?facility_id=",
        },
        allow_redirects=False,
    )
    if resp.status_code == 302:
        return True, f"Cancelled reservation {reservation_id}"
    return False, f"Cancel failed (status {resp.status_code})"
