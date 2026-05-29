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
    conn = sqlite3.connect(DB_PATH)
    info = conn.execute("PRAGMA table_info(jobs)").fetchall()
    if info:
        # Migrate old single-PK schema (chat_id was sole PK) -> composite PK.
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
    """In-memory registry of active jobs. Up to MAX_JOBS_PER_USER per chat_id."""

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


async def _offer_alt(
    job: SnipeJob, alt: dict, notify: NotifyCb, spawn: Optional[SpawnCb],
) -> None:
    """Notify the user that a different configuration is available and wait for
    a decision. Accepting books the alt and ends the snipe; skipping resumes
    polling for the original request."""
    got_min = alt["duration_slots"] * SLOT_MINUTES
    got_courts = len(alt["runs"])
    court_names = ", ".join(r[0]["court"] for r in alt["runs"])
    delta = []
    if got_min != job.duration_min:
        delta.append(f"{got_min} min (asked for {job.duration_min})")
    if got_courts != job.count:
        delta.append(f"{got_courts} court(s) (asked for {job.count})")
    delta_str = "; ".join(delta) or "different configuration"

    job.pending_alt = alt
    job.alt_event = asyncio.Event()
    job.alt_choice = None
    persist(job)
    await notify(
        job,
        f"💡 A different configuration is available for {job.target_time} on {job.date}: "
        f"{delta_str}. Courts: {court_names}.\n"
        f"Book it (this will end the snipe) or skip and keep waiting?",
        markup="alt",
    )
    try:
        await asyncio.wait_for(job.alt_event.wait(), timeout=3600)
    except asyncio.TimeoutError:
        job.rejected_alts.add(alt_signature(alt))
        job.pending_alt = None
        job.alt_event = None
        await notify(job, "⌛ No response in 1h — keeping the snipe running.")
        return

    choice = job.alt_choice
    job.pending_alt = None
    job.alt_event = None
    if choice != "accept":
        job.rejected_alts.add(alt_signature(alt))
        return

    await _book_offer(job, alt, notify, spawn)
    # _book_offer sets status="booked" on success; the snipe ends either way.


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

            # Exact request not available — see if a different configuration is.
            alt = find_alternative_offer(
                all_slots, job.count, job.same_number,
                job.target_time, requested_slots,
            )
            if alt and alt_signature(alt) not in job.rejected_alts:
                await _offer_alt(job, alt, notify, spawn)
                if job.status in ("booked", "cancelled"):
                    return
                # User skipped or timed out; loop continues waiting for exact match.

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
