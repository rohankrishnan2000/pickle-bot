"""Async snipe orchestration plus lightweight persistence.

This module is the project's scheduling engine. ``bot.py`` translates Telegram
commands into :class:`SnipeJob` objects, and this module takes over from there:

1. persist job state in SQLite so restarts can recover cleanly
2. wait until the reservation window opens
3. poll YourCourts for matching availability
4. choose the best set of courts that satisfies the request
5. attempt the booking and report the outcome back through a notifier callback
"""

import asyncio
import datetime as dt
import os
import re
import sqlite3
import time
from dataclasses import dataclass, field
from itertools import groupby
from typing import Awaitable, Callable, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests

try:
    import pickle_bot.yourcourts as yourcourts
except ModuleNotFoundError:
    import yourcourts

DB_PATH = "bot_state.db"
POLL_INTERVAL = 15
REFRESH_LOGIN_EVERY = 40  # attempts
BOOKING_WINDOW_DAYS = 10
COURT_OPEN_HOUR = 7
COURT_TIMEZONE = os.environ.get("COURT_TIMEZONE", "America/Los_Angeles").strip()


@dataclass
class SnipeJob:
    chat_id: int
    date: str
    target_time: str
    court: Optional[str]
    email: str = ""
    password: str = ""
    status: str = "running"  # running | booked | cancelled | error | interrupted
    attempts: int = 0
    started_at: float = field(default_factory=time.time)
    last_message: str = ""
    count: int = 1
    partial: bool = False           # if True, accept fewer than `count` courts
    same_number: bool = False       # if True, all booked courts must share the same court number
    duration_min: int = 30          # play time in minutes; must be a multiple of 30
    chain_time: Optional[str] = None  # after success, auto-snipe this time on the same date
    task: Optional[asyncio.Task] = None
    session: Optional[requests.Session] = None
    # Runtime-only: per-chat slot index (0..MAX_JOBS_PER_USER-1) used in callback data.
    slot_index: int = 0
    # Runtime-only: tracking for the "found an alternative configuration" offers.
    pending_alt: Optional[dict] = None
    alt_event: Optional[asyncio.Event] = None
    alt_choice: Optional[str] = None
    rejected_alts: set = field(default_factory=set)


MAX_JOBS_PER_USER = 3


NotifyCb = Callable[..., Awaitable[None]]
SpawnCb = Callable[["SnipeJob"], Awaitable[None]]


def _parse_job_date(date_str: str) -> Optional[dt.date]:
    """Parse persisted ``MM/DD/YYYY`` strings back into dates."""
    try:
        return dt.datetime.strptime(date_str, "%m/%d/%Y").date()
    except ValueError:
        return None


def _court_timezone() -> dt.tzinfo:
    """Resolve the timezone used for booking-window calculations."""
    if COURT_TIMEZONE:
        try:
            return ZoneInfo(COURT_TIMEZONE)
        except ZoneInfoNotFoundError:
            pass
    return dt.datetime.now().astimezone().tzinfo or dt.timezone.utc


def activation_time_for(job: SnipeJob, now: Optional[dt.datetime] = None) -> Optional[dt.datetime]:
    """Return when polling should begin for a job.

    Snipes more than ``BOOKING_WINDOW_DAYS`` out do not poll immediately. The
    bot and the CLI both use this to explain when a future snipe will wake up.
    """
    target_date = _parse_job_date(job.date)
    if target_date is None:
        return None
    tz = now.tzinfo if now else _court_timezone()
    open_date = target_date - dt.timedelta(days=BOOKING_WINDOW_DAYS)
    return dt.datetime.combine(
        open_date,
        dt.time(hour=COURT_OPEN_HOUR),
        tzinfo=tz,
    )


def seconds_until_activation(job: SnipeJob, now: Optional[dt.datetime] = None) -> float:
    """Return the remaining delay before a job is allowed to start polling."""
    activation = activation_time_for(job, now)
    if activation is None:
        return 0
    now = now or dt.datetime.now(activation.tzinfo)
    return max(0, (activation - now).total_seconds())


def activation_label(job: SnipeJob, now: Optional[dt.datetime] = None) -> Optional[str]:
    """Format the activation time for user-facing status messages."""
    activation = activation_time_for(job, now)
    if activation is None:
        return None
    return activation.strftime("%m/%d/%Y %I:%M%p").replace(" 0", " ")


_JOBS_DDL = """
    CREATE TABLE IF NOT EXISTS jobs (
        chat_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        target_time TEXT NOT NULL,
        court TEXT,
        email TEXT NOT NULL DEFAULT '',
        password TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        started_at REAL NOT NULL,
        last_message TEXT,
        count INTEGER NOT NULL DEFAULT 1,
        partial INTEGER NOT NULL DEFAULT 0,
        same_number INTEGER NOT NULL DEFAULT 0,
        duration_min INTEGER NOT NULL DEFAULT 30,
        chain_time TEXT,
        PRIMARY KEY (chat_id, date, target_time)
    )
"""


def _db() -> sqlite3.Connection:
    """Open the SQLite state DB and ensure the schema is current.

    ``bot.py`` relies on this storage for three pieces of long-lived state:
    saved credentials, the allowlist, and persisted snipe jobs that can be
    resumed after a restart.
    """
    conn = sqlite3.connect(DB_PATH)
    info = conn.execute("PRAGMA table_info(jobs)").fetchall()
    if info:
        # Old versions only allowed one job per chat_id. The composite key lets
        # us persist multiple concurrent snipes for different date/time pairs.
        chat_id_pk_rank = next((c[5] for c in info if c[1] == "chat_id"), 0)
        target_pk_rank = next((c[5] for c in info if c[1] == "target_time"), 0)
        if chat_id_pk_rank == 1 and target_pk_rank == 0:
            conn.execute("ALTER TABLE jobs RENAME TO jobs_legacy")
            conn.execute(_JOBS_DDL)
            cols_legacy = [c[1] for c in info]
            cols_new = [c for c in (
                "chat_id", "date", "target_time", "court", "email", "password",
                "status", "attempts", "started_at", "last_message",
                "count", "partial", "same_number", "duration_min", "chain_time",
            ) if c in cols_legacy]
            placeholders = ", ".join(cols_new)
            conn.execute(f"INSERT INTO jobs ({placeholders}) SELECT {placeholders} FROM jobs_legacy")
            conn.execute("DROP TABLE jobs_legacy")
    else:
        conn.execute(_JOBS_DDL)

    for col, ddl in (
        ("count", "ALTER TABLE jobs ADD COLUMN count INTEGER NOT NULL DEFAULT 1"),
        ("partial", "ALTER TABLE jobs ADD COLUMN partial INTEGER NOT NULL DEFAULT 0"),
        ("same_number", "ALTER TABLE jobs ADD COLUMN same_number INTEGER NOT NULL DEFAULT 0"),
        ("duration_min", "ALTER TABLE jobs ADD COLUMN duration_min INTEGER NOT NULL DEFAULT 30"),
        ("chain_time", "ALTER TABLE jobs ADD COLUMN chain_time TEXT"),
    ):
        try:
            conn.execute(ddl)
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            chat_id INTEGER PRIMARY KEY,
            email TEXT NOT NULL,
            password TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS allowed_users (
            chat_id INTEGER PRIMARY KEY,
            display_name TEXT,
            approved_at REAL NOT NULL
        )
    """)
    conn.commit()
    return conn


def add_allowed_user(chat_id: int, display_name: str | None) -> None:
    """Persist a newly approved Telegram user."""
    conn = _db()
    conn.execute(
        "INSERT OR IGNORE INTO allowed_users (chat_id, display_name, approved_at) VALUES (?, ?, ?)",
        (chat_id, display_name, time.time()),
    )
    conn.commit()
    conn.close()


def remove_allowed_user(chat_id: int) -> None:
    """Remove a user from the persisted allowlist."""
    conn = _db()
    conn.execute("DELETE FROM allowed_users WHERE chat_id = ?", (chat_id,))
    conn.commit()
    conn.close()


def load_allowed_users() -> list[int]:
    """Load the allowlist during bot startup."""
    conn = _db()
    rows = conn.execute("SELECT chat_id FROM allowed_users").fetchall()
    conn.close()
    return [r[0] for r in rows]


def save_credentials(chat_id: int, email: str, password: str) -> None:
    """Store the YourCourts credentials that later snipes will use."""
    conn = _db()
    conn.execute(
        """
        INSERT INTO users (chat_id, email, password) VALUES (?, ?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET email=excluded.email, password=excluded.password
        """,
        (chat_id, email, password),
    )
    conn.commit()
    conn.close()


def get_credentials(chat_id: int) -> tuple[str, str] | None:
    """Fetch saved credentials for login and direct-booking flows."""
    conn = _db()
    row = conn.execute(
        "SELECT email, password FROM users WHERE chat_id = ?", (chat_id,)
    ).fetchone()
    conn.close()
    return (row[0], row[1]) if row else None


def delete_credentials(chat_id: int) -> None:
    """Forget saved credentials after logout."""
    conn = _db()
    conn.execute("DELETE FROM users WHERE chat_id = ?", (chat_id,))
    conn.commit()
    conn.close()


def persist(job: SnipeJob) -> None:
    """Mirror the current in-memory job state into SQLite."""
    conn = _db()
    conn.execute(
        """
        INSERT INTO jobs (chat_id, date, target_time, court, email, password, status, attempts, started_at, last_message, count, partial, same_number, duration_min, chain_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(chat_id, date, target_time) DO UPDATE SET
            court=excluded.court,
            email=excluded.email,
            password=excluded.password,
            status=excluded.status,
            attempts=excluded.attempts,
            started_at=excluded.started_at,
            last_message=excluded.last_message,
            count=excluded.count,
            partial=excluded.partial,
            same_number=excluded.same_number,
            duration_min=excluded.duration_min,
            chain_time=excluded.chain_time
        """,
        (job.chat_id, job.date, job.target_time, job.court, job.email, job.password,
         job.status, job.attempts, job.started_at, job.last_message,
         job.count, int(job.partial), int(job.same_number),
         job.duration_min, job.chain_time),
    )
    conn.commit()
    conn.close()


def remove(chat_id: int, date: str | None = None, target_time: str | None = None) -> None:
    """Delete one persisted job or every job owned by a chat id."""
    conn = _db()
    if date and target_time:
        conn.execute(
            "DELETE FROM jobs WHERE chat_id = ? AND date = ? AND target_time = ?",
            (chat_id, date, target_time),
        )
    else:
        conn.execute("DELETE FROM jobs WHERE chat_id = ?", (chat_id,))
    conn.commit()
    conn.close()


def load_persisted() -> list[SnipeJob]:
    """Reconstruct saved jobs from SQLite into runtime ``SnipeJob`` objects."""
    conn = _db()
    rows = conn.execute(
        "SELECT chat_id, date, target_time, court, email, password, status, attempts, started_at, last_message, count, partial, same_number, duration_min, chain_time FROM jobs"
    ).fetchall()
    conn.close()
    jobs = []
    for r in rows:
        jobs.append(SnipeJob(
            chat_id=r[0], date=r[1], target_time=r[2], court=r[3],
            email=r[4] or "", password=r[5] or "",
            status=r[6], attempts=r[7], started_at=r[8], last_message=r[9] or "",
            count=r[10] or 1, partial=bool(r[11]), same_number=bool(r[12]),
            duration_min=r[13] or 30, chain_time=r[14],
        ))
    return jobs


def mark_all_interrupted() -> list[SnipeJob]:
    """Mark in-flight jobs as interrupted during bot startup recovery.

    ``bot.post_init`` uses this so users can explicitly choose whether to
    resume or discard work that was alive before the process exited.
    """
    conn = _db()
    conn.execute("UPDATE jobs SET status='interrupted' WHERE status='running'")
    conn.commit()
    conn.close()
    return [j for j in load_persisted() if j.status == "interrupted"]


class JobRegistry:
    """Track the currently running jobs inside this Python process.

    SQLite keeps the durable copy, but the bot needs fast lookup by chat id and
    callback slot number while commands are being handled. This registry fills
    that runtime-only role.
    """

    def __init__(self) -> None:
        self._jobs: dict[int, list[SnipeJob]] = {}

    def jobs_for(self, chat_id: int) -> list[SnipeJob]:
        return list(self._jobs.get(chat_id, []))

    def active_for(self, chat_id: int) -> list[SnipeJob]:
        return [j for j in self._jobs.get(chat_id, []) if j.status == "running"]

    def active_count(self, chat_id: int) -> int:
        return len(self.active_for(chat_id))

    def find(self, chat_id: int, slot_index: int) -> Optional[SnipeJob]:
        for j in self._jobs.get(chat_id, []):
            if j.slot_index == slot_index and j.status == "running":
                return j
        return None

    def add(self, job: SnipeJob) -> bool:
        """Assign the next free slot 0..MAX_JOBS_PER_USER-1. Returns False if full."""
        existing = self._jobs.setdefault(job.chat_id, [])
        taken = {j.slot_index for j in existing if j.status == "running"}
        for i in range(MAX_JOBS_PER_USER):
            if i not in taken:
                job.slot_index = i
                existing.append(job)
                return True
        return False

    def drop(self, job: SnipeJob) -> None:
        bucket = self._jobs.get(job.chat_id)
        if not bucket:
            return
        self._jobs[job.chat_id] = [j for j in bucket if j is not job]
        if not self._jobs[job.chat_id]:
            self._jobs.pop(job.chat_id, None)

    def all(self) -> list[SnipeJob]:
        return [j for bucket in self._jobs.values() for j in bucket]


_COURT_RE = re.compile(r"^(.*?)\s*(\d+)\s*([A-Za-z]*)\s*$")


def _court_key(name: str) -> tuple[str, int, str]:
    """Normalize court names so multi-court selection can group nearby courts."""
    m = _COURT_RE.match(name.strip())
    if not m:
        return (name.strip(), 0, "")
    return (m.group(1).strip(), int(m.group(2)), m.group(3).upper())


def pick_group(
    matches: list[dict],
    count: int,
    same_number: bool,
    partial: bool,
) -> list[dict] | None:
    """Choose which slots to book given availability and the user's preferences.

    Strategy: prefer a group of `count` courts that share the same court number;
    otherwise fall back to the consecutive sorted window. Returns None if nothing
    satisfies the constraints.
    """
    if not matches:
        return None

    sorted_slots = sorted(matches, key=lambda s: _court_key(s["court"]))

    # Group by (facility, number); each group is a same-number cluster.
    same_num_groups = [
        list(g) for _, g in groupby(sorted_slots, key=lambda s: _court_key(s["court"])[:2])
    ]
    same_num_groups.sort(key=len, reverse=True)

    full = next((g for g in same_num_groups if len(g) >= count), None)
    if full:
        return full[:count]

    if same_number:
        # Must share court number. Only partial-OK can salvage this.
        if partial and same_num_groups:
            return same_num_groups[0]
        return None

    # No same-number group is large enough; fall back to closest sorted window.
    if len(sorted_slots) >= count:
        return sorted_slots[:count]
    if partial:
        return sorted_slots
    return None


SLOT_MINUTES = 30


def consecutive_runs(
    all_slots: list[dict],
    target_time: str,
    max_slots: int,
) -> dict[str, list[dict]]:
    """Group `all_slots` by court and, for each court, return up to `max_slots`
    consecutive slots starting at `target_time`. Courts whose first available
    slot is not exactly target_time are excluded."""
    target_id = yourcourts.time_to_id(target_time)
    if target_id < 0:
        return {}

    by_court: dict[str, dict[int, dict]] = {}
    for s in all_slots:
        sid = yourcourts.time_to_id(s["time"])
        if sid < 0:
            continue
        by_court.setdefault(s["court"], {})[sid] = s

    runs: dict[str, list[dict]] = {}
    for court, slot_map in by_court.items():
        if target_id not in slot_map:
            continue
        run = []
        for offset in range(max_slots):
            sid = target_id + offset
            if sid in slot_map:
                run.append(slot_map[sid])
            else:
                break
        if run:
            runs[court] = run
    return runs


def find_alternative_offer(
    all_slots: list[dict],
    count: int,
    same_number: bool,
    target_time: str,
    requested_slots: int,
) -> Optional[dict]:
    """Search for any book-able offer that doesn't match the user's exact ask.
    Tries larger configurations first (closer to the original) and returns the
    first hit. The returned offer is what the user could book if they chose to.

    Constraints we relax for alt-search: `partial=True` (fewer courts OK) and
    we also try `same_number=False` if the user originally required same-number.
    Returns None if no alternative is available.
    """
    runs = consecutive_runs(all_slots, target_time, requested_slots)
    if not runs:
        return None

    for d_slots in range(requested_slots, 0, -1):
        eligible = {c: r[:d_slots] for c, r in runs.items() if len(r) >= d_slots}
        if not eligible:
            continue
        first_slots = [r[0] for r in eligible.values()]
        for c_try in range(count, 0, -1):
            if c_try == count and d_slots == requested_slots:
                continue  # that's the user's original ask, not an alternative
            for sn_try in ([True, False] if same_number else [False]):
                chosen = pick_group(first_slots, c_try, sn_try, partial=True)
                if chosen and len(chosen) == c_try:
                    chosen_runs = [eligible[s["court"]] for s in chosen]
                    return {"duration_slots": d_slots, "runs": chosen_runs}
    return None


def alt_signature(offer: dict) -> tuple:
    """Create a stable identifier so skipped alternatives are not re-offered."""
    courts = tuple(sorted(r[0]["court"] for r in offer["runs"]))
    return (offer["duration_slots"], courts)


def pick_with_duration(
    all_slots: list[dict],
    count: int,
    same_number: bool,
    partial: bool,
    target_time: str,
    requested_slots: int,
) -> Optional[dict]:
    """Find COUNT courts that all have the full requested duration available
    as consecutive 30-min slots starting at target_time. No partial-duration
    fallback — if the full window isn't open, returns None and the caller
    keeps waiting.
    """
    runs = consecutive_runs(all_slots, target_time, requested_slots)
    eligible = {c: r for c, r in runs.items() if len(r) >= requested_slots}
    if not eligible:
        return None
    first_slots = [r[0] for r in eligible.values()]
    chosen_firsts = pick_group(first_slots, count, same_number, partial)
    if not chosen_firsts:
        return None
    chosen_runs = [eligible[s["court"]] for s in chosen_firsts]
    return {"duration_slots": requested_slots, "runs": chosen_runs}


async def _do_login(session: requests.Session, email: str = "", password: str = "") -> bool:
    """Run the blocking login call in a worker thread for the async loop."""
    return await asyncio.to_thread(yourcourts.login, session, email or None, password or None)


async def _find(session, date, target_time, court):
    """Async wrapper around ``yourcourts.find_slots``."""
    return await asyncio.to_thread(yourcourts.find_slots, session, date, target_time, court)


def _filter_by_court_prefix(matches: list[dict], court_filter: Optional[str]) -> list[dict]:
    """For multi-court jobs, treat the court arg as a prefix so a partial filter
    like 'PB' or 'PB 4' still allows several courts to match."""
    if not court_filter:
        return matches
    cf = court_filter.strip().casefold()
    return [s for s in matches if s["court"].casefold().startswith(cf)]


async def _book(session, slot, date, duration_min: int = 30):
    """Async wrapper around ``yourcourts.book_slot``."""
    return await asyncio.to_thread(yourcourts.book_slot, session, slot, date, duration_min)


async def _offer_alt(
    job: SnipeJob, alt: dict, notify: NotifyCb, spawn: Optional[SpawnCb],
) -> None:
    """Auto-book the best available configuration without asking the user.

    `find_alternative_offer` returns the largest available config (most courts
    and most time, closest to the original ask) first, so the alt passed here is
    already the best-available grab. We book it immediately and end the snipe."""
    got_min = alt["duration_slots"] * SLOT_MINUTES
    got_courts = len(alt["runs"])
    court_names = ", ".join(r[0]["court"] for r in alt["runs"])
    delta = []
    if got_min != job.duration_min:
        delta.append(f"{got_min} min (asked for {job.duration_min})")
    if got_courts != job.count:
        delta.append(f"{got_courts} court(s) (asked for {job.count})")
    delta_str = "; ".join(delta) or "different configuration"

    await notify(
        job,
        f"🤖 Auto-booking best available for {job.target_time} on {job.date}: "
        f"{delta_str}. Courts: {court_names}.",
    )
    await _book_offer(job, alt, notify, spawn)
    # _book_offer sets status="booked" on success; the snipe ends either way.


async def _book_offer(
    job: SnipeJob, offer: dict, notify: NotifyCb, spawn: Optional[SpawnCb],
) -> None:
    """Book the first slot of each run using the full duration in one request each.
    On success, optionally chain a follow-up snipe."""
    duration_min = offer["duration_slots"] * SLOT_MINUTES
    summary_lines = [f"{r[0]['court']} ({duration_min}m starting {r[0]['time']})"
                     for r in offer["runs"]]
    await notify(job, f"🎾 Booking {len(offer['runs'])} court(s) for {duration_min} min:\n" +
                 "\n".join(f"  • {s}" for s in summary_lines))

    booked_courts: list[str] = []
    failures: list[str] = []
    for run in offer["runs"]:
        first_slot = run[0]
        court = first_slot["court"]
        ok, msg = await _book(job.session, first_slot, job.date, duration_min)
        if ok:
            booked_courts.append(f"{court} @ {first_slot['time']} ({duration_min}m)")
        else:
            failures.append(f"{court} @ {first_slot['time']} ({msg})")

    if not booked_courts:
        job.last_message = "All booking attempts failed; resuming poll"
        await notify(job, f"⚠️ Booking failed for all courts: {'; '.join(failures)}. Resuming poll.")
        return

    summary = f"Booked {len(booked_courts)} court(s) for {duration_min}m on {job.date}: " + "; ".join(booked_courts)
    if failures:
        summary += f". Failed: {'; '.join(failures)}"
    job.status = "booked"
    job.last_message = summary
    persist(job)
    await notify(job, f"✅ {summary}")

    # Chain a follow-up snipe if requested.
    if job.chain_time and spawn:
        follow_up = SnipeJob(
            chat_id=job.chat_id,
            date=job.date,
            target_time=job.chain_time,
            court=job.court,
            email=job.email,
            password=job.password,
            count=job.count,
            partial=job.partial,
            same_number=job.same_number,
            duration_min=job.duration_min,
            chain_time=None,  # don't recursively chain
        )
        await notify(
            job,
            f"🔗 Starting chained snipe for {follow_up.target_time} on {follow_up.date}.",
        )
        await spawn(follow_up)


async def run_snipe(job: SnipeJob, notify: NotifyCb, spawn: Optional[SpawnCb] = None) -> None:
    """Polling loop. Notifies only on found/booked/error per user preference.

    - duration_min: requires consecutive 30-min slots per court.
    - If a shorter duration is the best we can do, ask the user (intervention).
    - If the full booking succeeds and chain_time is set, spawn a follow-up job.
    """
    requested_slots = max(1, job.duration_min // SLOT_MINUTES)

    try:
        # Future reservations cannot be booked until the site's booking window
        # opens, so jobs may spend a long time sleeping before the poll loop.
        delay = seconds_until_activation(job)
        if delay > 0:
            label = activation_label(job)
            job.last_message = f"Waiting to start polling until {label}"
            persist(job)
            await asyncio.sleep(delay)

        job.session = yourcourts.make_session()
        if not await _do_login(job.session, job.email, job.password):
            job.status = "error"
            job.last_message = "Login to yourcourts.com failed"
            persist(job)
            await notify(job, f"❌ Login failed for snipe on {job.date} {job.target_time}")
            return

        while True:
            job.attempts += 1

            try:
                # Fetch everything for the date first. Duration-aware snipes need
                # neighboring time slots to confirm the full requested run exists.
                all_slots = await _find(job.session, job.date, None, None)
                if job.count > 1 or job.court:
                    all_slots = _filter_by_court_prefix(all_slots, job.court)
            except yourcourts.SessionExpired:
                if await _do_login(job.session, job.email, job.password):
                    job.last_message = "session expired; re-authenticated"
                    persist(job)
                    continue  # retry the fetch immediately with a fresh session
                job.status = "error"
                job.last_message = "Session expired and re-login failed"
                persist(job)
                await notify(job, f"❌ Snipe stopped: session expired and re-login failed ({job.date} {job.target_time}).")
                return
            except requests.RequestException as e:
                job.last_message = f"network error: {e}"
                persist(job)
                await asyncio.sleep(POLL_INTERVAL)
                continue

            # First try to satisfy the user's original ask exactly.
            offer = pick_with_duration(
                all_slots, job.count, job.same_number, job.partial,
                job.target_time, requested_slots,
            )

            if offer:
                await _book_offer(job, offer, notify, spawn)
                if job.status == "booked":
                    return
                persist(job)
                await asyncio.sleep(POLL_INTERVAL)
                continue

            if job.attempts % REFRESH_LOGIN_EVERY == 0:
                await _do_login(job.session, job.email, job.password)
            persist(job)
            await asyncio.sleep(POLL_INTERVAL)

    except asyncio.CancelledError:
        job.status = "cancelled"
        job.last_message = "Cancelled by user"
        persist(job)
        await notify(job, f"🛑 Snipe cancelled ({job.date} {job.target_time})")
        raise
    except Exception as e:
        job.status = "error"
        job.last_message = f"unhandled: {e}"
        persist(job)
        await notify(job, f"❌ Snipe errored: {e}")
