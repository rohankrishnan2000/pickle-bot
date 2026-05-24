"""Async snipe job management. One active job per chat_id; SQLite-mirrored state."""

import asyncio
import re
import sqlite3
import time
from dataclasses import dataclass, field
from itertools import groupby
from typing import Awaitable, Callable, Optional

import requests

try:
    import pickle_bot.yourcourts as yourcourts
except ModuleNotFoundError:
    import yourcourts

DB_PATH = "bot_state.db"
POLL_INTERVAL = 15
REFRESH_LOGIN_EVERY = 40  # attempts


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
    # Runtime-only (not persisted) — intervention state for shorter-duration offers.
    pending_offer: Optional[dict] = None
    intervention_event: Optional[asyncio.Event] = None
    intervention_choice: Optional[str] = None
    rejected_offers: set = field(default_factory=set)


NotifyCb = Callable[..., Awaitable[None]]
SpawnCb = Callable[["SnipeJob"], Awaitable[None]]


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            chat_id INTEGER PRIMARY KEY,
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
            same_number INTEGER NOT NULL DEFAULT 0
        )
    """)
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
    conn = _db()
    conn.execute(
        "INSERT OR IGNORE INTO allowed_users (chat_id, display_name, approved_at) VALUES (?, ?, ?)",
        (chat_id, display_name, time.time()),
    )
    conn.commit()
    conn.close()


def remove_allowed_user(chat_id: int) -> None:
    conn = _db()
    conn.execute("DELETE FROM allowed_users WHERE chat_id = ?", (chat_id,))
    conn.commit()
    conn.close()


def load_allowed_users() -> list[int]:
    conn = _db()
    rows = conn.execute("SELECT chat_id FROM allowed_users").fetchall()
    conn.close()
    return [r[0] for r in rows]


def save_credentials(chat_id: int, email: str, password: str) -> None:
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
    conn = _db()
    row = conn.execute(
        "SELECT email, password FROM users WHERE chat_id = ?", (chat_id,)
    ).fetchone()
    conn.close()
    return (row[0], row[1]) if row else None


def delete_credentials(chat_id: int) -> None:
    conn = _db()
    conn.execute("DELETE FROM users WHERE chat_id = ?", (chat_id,))
    conn.commit()
    conn.close()


def persist(job: SnipeJob) -> None:
    conn = _db()
    conn.execute(
        """
        INSERT INTO jobs (chat_id, date, target_time, court, email, password, status, attempts, started_at, last_message, count, partial, same_number, duration_min, chain_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET
            date=excluded.date,
            target_time=excluded.target_time,
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


def remove(chat_id: int) -> None:
    conn = _db()
    conn.execute("DELETE FROM jobs WHERE chat_id = ?", (chat_id,))
    conn.commit()
    conn.close()


def load_persisted() -> list[SnipeJob]:
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
    """Called on startup. Any job that was 'running' didn't shut down cleanly."""
    conn = _db()
    conn.execute("UPDATE jobs SET status='interrupted' WHERE status='running'")
    conn.commit()
    conn.close()
    return [j for j in load_persisted() if j.status == "interrupted"]


class JobRegistry:
    """In-memory registry of active jobs. One job per chat_id."""

    def __init__(self) -> None:
        self._jobs: dict[int, SnipeJob] = {}

    def get(self, chat_id: int) -> Optional[SnipeJob]:
        return self._jobs.get(chat_id)

    def has_active(self, chat_id: int) -> bool:
        job = self._jobs.get(chat_id)
        return job is not None and job.status == "running"

    def add(self, job: SnipeJob) -> None:
        self._jobs[job.chat_id] = job

    def drop(self, chat_id: int) -> None:
        self._jobs.pop(chat_id, None)

    def all(self) -> list[SnipeJob]:
        return list(self._jobs.values())


_COURT_RE = re.compile(r"^(.*?)\s*(\d+)\s*([A-Za-z]*)\s*$")


def _court_key(name: str) -> tuple[str, int, str]:
    """Parse 'PB 4B' -> ('PB', 4, 'B'). Falls back gracefully for odd names."""
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


def pick_with_duration(
    all_slots: list[dict],
    count: int,
    same_number: bool,
    partial: bool,
    target_time: str,
    requested_slots: int,
) -> Optional[dict]:
    """Find the best offer: COUNT courts that all share the same duration,
    preferring the requested duration but falling back to shorter durations.

    Returns {"duration_slots": int, "runs": list[list[dict]]} or None.
    Every booked court is guaranteed to have the same duration_slots.
    """
    runs = consecutive_runs(all_slots, target_time, requested_slots)
    if not runs:
        return None

    for d_slots in range(requested_slots, 0, -1):
        eligible = {c: r[:d_slots] for c, r in runs.items() if len(r) >= d_slots}
        if not eligible:
            continue
        first_slots = [r[0] for r in eligible.values()]
        chosen_firsts = pick_group(first_slots, count, same_number, partial)
        if chosen_firsts:
            chosen_runs = [eligible[s["court"]] for s in chosen_firsts]
            return {"duration_slots": d_slots, "runs": chosen_runs}
    return None


def _offer_signature(offer: dict) -> tuple:
    """Stable hashable signature so we don't re-ask the user about the same offer."""
    courts = tuple(sorted(r[0]["court"] for r in offer["runs"]))
    return (offer["duration_slots"], courts)


async def _do_login(session: requests.Session, email: str = "", password: str = "") -> bool:
    return await asyncio.to_thread(yourcourts.login, session, email or None, password or None)


async def _find(session, date, target_time, court):
    return await asyncio.to_thread(yourcourts.find_slots, session, date, target_time, court)


def _filter_by_court_prefix(matches: list[dict], court_filter: Optional[str]) -> list[dict]:
    """For multi-court jobs, treat the court arg as a prefix so a partial filter
    like 'PB' or 'PB 4' still allows several courts to match."""
    if not court_filter:
        return matches
    cf = court_filter.strip().casefold()
    return [s for s in matches if s["court"].casefold().startswith(cf)]


async def _book(session, slot, date):
    return await asyncio.to_thread(yourcourts.book_slot, session, slot, date)


async def _book_offer(
    job: SnipeJob, offer: dict, notify: NotifyCb, spawn: Optional[SpawnCb],
) -> None:
    """Book every slot in the offer. On success, optionally chain a follow-up snipe."""
    duration_min = offer["duration_slots"] * SLOT_MINUTES
    summary_lines = [f"{r[0]['court']} ({len(r)} × {SLOT_MINUTES}m = {duration_min}m starting {r[0]['time']})"
                     for r in offer["runs"]]
    await notify(job, f"🎾 Booking {len(offer['runs'])} court(s) for {duration_min} min:\n" +
                 "\n".join(f"  • {s}" for s in summary_lines))

    booked_courts: list[str] = []
    failures: list[str] = []
    for run in offer["runs"]:
        court = run[0]["court"]
        court_ok = True
        booked_in_run: list[str] = []
        for slot in run:
            ok, msg = await _book(job.session, slot, job.date)
            if ok:
                booked_in_run.append(slot["time"])
            else:
                failures.append(f"{court} @ {slot['time']} ({msg})")
                court_ok = False
                break
        if court_ok:
            booked_courts.append(f"{court} ({', '.join(booked_in_run)})")

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
    job.session = yourcourts.make_session()
    requested_slots = max(1, job.duration_min // SLOT_MINUTES)

    try:
        if not await _do_login(job.session, job.email, job.password):
            job.status = "error"
            job.last_message = "Login to yourcourts.com failed"
            persist(job)
            await notify(job, f"❌ Login failed for snipe on {job.date} {job.target_time}")
            return

        while True:
            job.attempts += 1

            try:
                # Fetch all slots for the date (no time filter), then narrow ourselves.
                # We need other times too to detect consecutive duration runs.
                all_slots = await _find(job.session, job.date, None, None)
                if job.count > 1 or job.court:
                    all_slots = _filter_by_court_prefix(all_slots, job.court)
            except requests.RequestException as e:
                job.last_message = f"network error: {e}"
                persist(job)
                await asyncio.sleep(POLL_INTERVAL)
                continue

            offer = pick_with_duration(
                all_slots, job.count, job.same_number, job.partial,
                job.target_time, requested_slots,
            )

            if not offer:
                if job.attempts % REFRESH_LOGIN_EVERY == 0:
                    await _do_login(job.session, job.email, job.password)
                persist(job)
                await asyncio.sleep(POLL_INTERVAL)
                continue

            if offer["duration_slots"] < requested_slots:
                sig = _offer_signature(offer)
                if sig in job.rejected_offers:
                    persist(job)
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                got_min = offer["duration_slots"] * SLOT_MINUTES
                court_names = ", ".join(r[0]["court"] for r in offer["runs"])
                job.pending_offer = offer
                job.intervention_event = asyncio.Event()
                job.intervention_choice = None
                persist(job)
                await notify(
                    job,
                    f"⏸ *User intervention required.*\n"
                    f"Requested {job.duration_min} min for {job.target_time} on {job.date}, "
                    f"but only *{got_min} min* is currently available across "
                    f"{len(offer['runs'])} court(s): {court_names}.\n\n"
                    f"Accept the shorter booking, or skip and keep waiting for the full duration?",
                    markup="intervention",
                )
                try:
                    await asyncio.wait_for(job.intervention_event.wait(), timeout=3600)
                except asyncio.TimeoutError:
                    job.rejected_offers.add(sig)
                    job.pending_offer = None
                    job.intervention_event = None
                    await notify(job, "⌛ No response in 1h — skipping that offer and resuming poll.")
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                choice = job.intervention_choice
                job.pending_offer = None
                job.intervention_event = None
                if choice != "accept":
                    job.rejected_offers.add(sig)
                    await asyncio.sleep(POLL_INTERVAL)
                    continue
                # fall through to book the offer

            await _book_offer(job, offer, notify, spawn)
            if job.status == "booked":
                return

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
