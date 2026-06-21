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
import json
import logging
import logging.handlers
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

def _setup_snipe_logger() -> logging.Logger:
    """Configure and return the shared ``snipe`` logger.

    Sets up a single :class:`~logging.handlers.RotatingFileHandler` that writes
    to ``snipe.log`` in the project directory. The handler is added idempotently
    so repeated imports or calls do not duplicate output.
    """
    _log = logging.getLogger("snipe")
    if not any(isinstance(h, logging.handlers.RotatingFileHandler) for h in _log.handlers):
        _log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "snipe.log")
        _handler = logging.handlers.RotatingFileHandler(
            _log_path,
            maxBytes=2 * 1024 * 1024,  # 2 MB
            backupCount=5,
            encoding="utf-8",
        )
        _handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s.%(msecs)03d %(levelname)s %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        _log.addHandler(_handler)
        _log.setLevel(logging.INFO)
        _log.propagate = False
    return _log


logger = _setup_snipe_logger()


def _job_tag(job: "SnipeJob") -> str:
    """Return a compact per-job correlation tag for log lines.

    Example: ``[chat_id=12345 06/10/2026 7:00AM]``
    """
    return f"[chat_id={job.chat_id} {job.date} {job.target_time}]"


DROP_DEBUG_WINDOW_S = 120  # log every poll for this many seconds after window opens


def _drop_debug_path(job: "SnipeJob") -> str:
    """Return the .jsonl path for a drop snipe's debug log."""
    safe_date = job.date.replace("/", "_")
    safe_time = job.target_time.replace(":", "_")
    fname = f"{safe_date}_{safe_time}.jsonl"
    logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(logs_dir, exist_ok=True)
    return os.path.join(logs_dir, fname)


def _ts(tz: dt.tzinfo) -> str:
    """Current time as HH:MM:SS.mmm in the court timezone."""
    now = dt.datetime.now(tz)
    ms = now.microsecond // 1000
    return now.strftime(f"%H:%M:%S.{ms:03d}")


def _write_drop_debug(path: str, record: dict) -> None:
    """Append one JSON record to the drop snipe debug file."""
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
    except Exception:
        pass


DB_PATH = "bot_state.db"
POLL_INTERVAL = 15
REFRESH_LOGIN_EVERY = 40  # attempts
# Seconds past a slot's own window-open before we treat it as taken (not just
# "not opened yet"). A few poll cycles of slack so availability can propagate.
STUCK_GRACE_S = 75
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


def _avail_by_court(all_slots: list[dict]) -> dict[str, dict[int, dict]]:
    """Map each court to ``{slot_id: slot_dict}`` of its currently bookable slots."""
    out: dict[str, dict[int, dict]] = {}
    for s in all_slots:
        sid = yourcourts.time_to_id(s["time"])
        if sid < 0:
            continue
        out.setdefault(s["court"], {})[sid] = s
    return out


def _hhmm24_to_slot_id(hhmm: str) -> int:
    """Convert an ISO ``"HH:MM"`` (24h) clock time into a YourCourts slot id, or -1."""
    m = re.fullmatch(r"(\d{2}):(\d{2})", hhmm.strip())
    if not m:
        return -1
    hour, minute = int(m.group(1)), int(m.group(2))
    if minute not in (0, 30):
        return -1
    minutes = hour * 60 + minute
    first_slot, last_slot = 7 * 60, 21 * 60 + 30
    if minutes < first_slot or minutes > last_slot:
        return -1
    return yourcourts.TIME_ID_BASE + (minutes - first_slot) // 30


def _court_number_distance(a: tuple, b: tuple):
    """Distance between two _court_key tuples by court number, or None if different facility."""
    if a[0] != b[0]:
        return None
    return abs(a[1] - b[1])


def pick_incremental(first_slots: list[dict], remaining: int, booked_keys: list[tuple]) -> list[dict]:
    """Pick up to `remaining` courts to grab now, best grouping first.

    Preference: courts sharing a booked court's number (same number) first, then
    closest numbers, then — when nothing is booked yet — the best self-contained
    group, and separated courts only as a last resort.
    """
    if remaining <= 0 or not first_slots:
        return []
    if not booked_keys:
        # No anchor yet: fall back to the existing grouping picker (same-number
        # cluster first, else closest sorted window). partial=True so it returns
        # fewer than `remaining` when that's all that's open.
        return pick_group(first_slots, remaining, same_number=False, partial=True) or []

    def score(slot):
        k = _court_key(slot["court"])
        dists = [d for d in (_court_number_distance(k, b) for b in booked_keys) if d is not None]
        if dists:
            return (0, min(dists), k)   # same facility as an anchor; nearer number = better
        return (1, 0, k)               # different facility / separated — acceptable, last

    return sorted(first_slots, key=score)[:remaining]


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


async def _book_chunk(job, start_slot, n_slots) -> tuple[bool, str]:
    """Book ``n_slots`` consecutive 30-min slots starting at ``start_slot`` in one POST."""
    return await _book(job.session, start_slot, job.date, n_slots * SLOT_MINUTES)


async def _verify_durations(job, courts, requested_slots) -> None:
    """Best-effort log of how many reservations cover each court on the job date.

    Surfaces the booking-duration safety check (the bot historically under-booked
    duration); failures here are non-fatal.
    """
    tag = _job_tag(job)
    try:
        bookings = await asyncio.to_thread(yourcourts.list_my_bookings, job.session)
        job_date = _parse_job_date(job.date)
        iso_date = job_date.isoformat() if job_date else job.date
        for court in courts:
            mine = [b for b in bookings
                    if court.casefold() in b["court"].casefold() and iso_date in b["start"]]
            logger.info(
                "%s verify %s — %d reservation(s) eventTimes=%r (wanted %d slot(s))",
                tag, court, len(mine), [b.get("times") for b in mine], requested_slots,
            )
    except Exception as _ve:
        try:
            logger.warning("%s duration verification failed (non-fatal): %s", tag, _ve)
        except Exception:
            pass


async def run_snipe(job: SnipeJob, notify: NotifyCb, spawn: Optional[SpawnCb] = None) -> None:
    """Polling loop. Notifies only on found/booked/error per user preference.

    - duration_min: requires consecutive 30-min slots per court.
    - Multi-court: each poll grabs the best available subset (preferring the same
      court number, then the closest), accumulating across polls until `count`
      courts are booked.
    - If the full booking succeeds and chain_time is set, spawn a follow-up job.
    """
    requested_slots = max(1, job.duration_min // SLOT_MINUTES)
    duration_min = job.duration_min
    tag = _job_tag(job)

    try:
        logger.info(
            "%s job started — duration_min=%d requested_slots=%d count=%d court=%r "
            "chain_time=%r",
            tag, job.duration_min, requested_slots, job.count, job.court,
            job.chain_time,
        )
    except Exception:
        pass

    def _run_chain():
        """Build and return the chained follow-up job, or None if no chain set."""
        if not (job.chain_time and spawn):
            return None
        return SnipeJob(
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

    try:
        # Future reservations cannot be booked until the site's booking window
        # opens, so jobs may spend a long time sleeping before the poll loop.
        delay = seconds_until_activation(job)
        tz = _court_timezone()
        is_drop_snipe = delay > 0  # stitch-up-degrade only applies at exact window-open
        drop_debug_until: Optional[float] = None
        drop_debug_path: Optional[str] = None

        if delay > 0:
            label = activation_label(job)
            try:
                act_time = activation_time_for(job)
                logger.info(
                    "%s drop snipe — activation scheduled at %s (%.1f s from now)",
                    tag, act_time.isoformat() if act_time else label, delay,
                )
            except Exception:
                pass
            job.last_message = f"Drop snipe: waiting for the court window to open at {label}"
            persist(job)
            await asyncio.sleep(delay)
            wake_mono = time.monotonic()
            drop_debug_until = wake_mono + DROP_DEBUG_WINDOW_S
            drop_debug_path = _drop_debug_path(job)
            try:
                wake_time = dt.datetime.now(tz)
                act_time = activation_time_for(job)
                drift_s = (wake_time - act_time).total_seconds() if act_time else float("nan")
                logger.info(
                    "%s drop snipe woke at window open — actual_wake=%s drift=%.3f s",
                    tag, wake_time.isoformat(), drift_s,
                )
                _write_drop_debug(drop_debug_path, {
                    "event": "wake",
                    "job": {
                        "chat_id": job.chat_id,
                        "date": job.date,
                        "target_time": job.target_time,
                        "court": job.court,
                        "count": job.count,
                        "duration_min": job.duration_min,
                        "partial": job.partial,
                        "same_number": job.same_number,
                    },
                    "actual_wake": wake_time.isoformat(),
                    "drift_s": round(drift_s, 3),
                    "debug_window_s": DROP_DEBUG_WINDOW_S,
                })
            except Exception:
                pass

        job.session = yourcourts.make_session()
        if not await _do_login(job.session, job.email, job.password):
            job.status = "error"
            job.last_message = "Login to yourcourts.com failed"
            persist(job)
            await notify(job, f"❌ Login failed for snipe on {job.date} {job.target_time}")
            return

        # Per-court progress: court -> number of consecutive 30-min slots booked,
        # counting from target_time. A court is "complete" at requested_slots.
        # Because each slot enters the booking window 30 min before the previous
        # (rolling window), we grab the target slot at open and then extend into
        # later slots as each one's window rolls open ("grab now + stitch up").
        court_progress: dict[str, int] = {}

        target_id = yourcourts.time_to_id(job.target_time)
        if target_id < 0:
            job.status = "error"
            job.last_message = f"Invalid target time {job.target_time!r}"
            persist(job)
            await notify(job, f"❌ Invalid target time {job.target_time} for snipe on {job.date}")
            return

        # Slot k (0-indexed from target_time) opens k*30 min after the target slot.
        # Used to tell "not yet open" apart from "opened and already taken".
        activation_dt = activation_time_for(job)

        # Best-effort reconciliation: an interrupted+resumed job may already hold
        # some slots. Count the consecutive run already booked per court so we
        # resume extending rather than re-booking. Skipped for a drop snipe that
        # just woke (delay > 0): nothing could be booked yet and we don't want to
        # spend a round-trip while racing other bookers.
        if delay <= 0:
            try:
                job_date = _parse_job_date(job.date)
                iso_date = job_date.isoformat() if job_date else job.date
                cf = job.court.strip().casefold() if job.court else None
                existing = await asyncio.to_thread(yourcourts.list_my_bookings, job.session)
                booked_ids: dict[str, set[int]] = {}
                for b in existing:
                    start = b.get("start", "")
                    court_name = b.get("court", "")
                    if not court_name or iso_date not in start:
                        continue
                    if cf and not court_name.casefold().startswith(cf):
                        continue
                    sid = _hhmm24_to_slot_id(start[11:16])
                    if sid >= 0:
                        booked_ids.setdefault(court_name, set()).add(sid)
                for court_name, ids in booked_ids.items():
                    p = 0
                    while (target_id + p) in ids:
                        p += 1
                    if p > 0:
                        court_progress[court_name] = min(p, requested_slots)
                if court_progress:
                    logger.info(
                        "%s reconciliation seeded progress: %s", tag,
                        ", ".join(f"{c}={p}/{requested_slots}"
                                  for c, p in court_progress.items()),
                    )
            except Exception as _re:
                try:
                    logger.warning("%s reconciliation failed (non-fatal): %s", tag, _re)
                except Exception:
                    pass

        def _complete_courts() -> list[str]:
            return [c for c, p in court_progress.items() if p >= requested_slots]

        async def _finish() -> None:
            complete = sorted(_complete_courts())
            await _verify_durations(job, sorted(court_progress), requested_slots)
            summary = (f"Booked {len(complete)} court(s) for {duration_min}m on "
                       f"{job.date}: " + "; ".join(complete))
            job.status = "booked"
            job.last_message = summary
            persist(job)
            await notify(job, f"✅ {summary}")
            follow_up = _run_chain()
            if follow_up:
                await notify(
                    job,
                    f"🔗 Starting chained snipe for {follow_up.target_time} on {follow_up.date}.",
                )
                await spawn(follow_up)

        if len(_complete_courts()) >= job.count:
            await _finish()
            return

        while True:
            job.attempts += 1
            in_debug_window = (
                drop_debug_until is not None
                and time.monotonic() <= drop_debug_until
            )

            sent_ts = _ts(tz) if in_debug_window else None
            try:
                # Fetch everything for the date first. Duration-aware snipes need
                # neighboring time slots to extend into the full requested run.
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
            received_ts = _ts(tz) if in_debug_window else None

            avail = _avail_by_court(all_slots)
            now_dt = dt.datetime.now(tz)

            def _slot_settled(slot_index: int) -> bool:
                """True once slot `slot_index`'s window has been open past the grace."""
                if activation_dt is None:
                    return True
                open_at = activation_dt + dt.timedelta(minutes=SLOT_MINUTES * slot_index)
                return now_dt >= open_at + dt.timedelta(seconds=STUCK_GRACE_S)

            def _stuck(court: str) -> bool:
                """A held court whose next slot is open-but-unavailable (taken)."""
                p = court_progress.get(court, 0)
                if p >= requested_slots:
                    return False
                return (target_id + p) not in avail.get(court, {}) and _slot_settled(p)

            runs = consecutive_runs(all_slots, job.target_time, requested_slots)
            try:
                runs_summary = ", ".join(
                    f"{court}={len(slots)}" for court, slots in sorted(runs.items())
                ) or "none"
                logger.info(
                    "%s poll attempt=%d progress=[%s] runs: %s",
                    tag, job.attempts,
                    ", ".join(f"{c}:{p}/{requested_slots}"
                              for c, p in court_progress.items()) or "none",
                    runs_summary,
                )
            except Exception:
                pass

            # Build this poll's booking plan: (court, start_slot, n_slots, is_new_anchor).
            actions: list[tuple] = []

            # 1) Extend courts we already hold but haven't filled — grab the
            #    longest contiguous chunk now available from where we left off.
            for court in list(court_progress):
                p = court_progress[court]
                if p >= requested_slots:
                    continue
                ids = avail.get(court, {})
                n = 0
                while p + n < requested_slots and (target_id + p + n) in ids:
                    n += 1
                if n > 0:
                    actions.append((court, ids[target_id + p], n, False))

            # 2) Anchor new courts until we hold `count` still-extendable courts.
            #    A stuck court (next slot opened and got taken) no longer counts
            #    toward the goal, so we anchor a replacement.
            viable_held = [c for c in court_progress if not _stuck(c)]
            need_courts = job.count - len(viable_held)
            if need_courts > 0:
                candidates = {
                    c: ids for c, ids in avail.items()
                    if c not in court_progress and target_id in ids
                }
                if not is_drop_snipe:
                    # Outside the drop-snipe window the booking window is already
                    # open for all slots, so any missing slot is genuinely taken —
                    # don't anchor a court unless it has the full run available.
                    candidates = {
                        c: ids for c, ids in candidates.items()
                        if all((target_id + i) in ids for i in range(requested_slots))
                    }
                if candidates:
                    booked_keys = [_court_key(c) for c in court_progress]
                    first_slots = [ids[target_id] for ids in candidates.values()]
                    chosen = pick_incremental(first_slots, need_courts, booked_keys)
                    for s in chosen:
                        court = s["court"]
                        ids = candidates[court]
                        n = 0
                        while n < requested_slots and (target_id + n) in ids:
                            n += 1
                        if n > 0:
                            actions.append((court, ids[target_id], n, True))

            if in_debug_window:
                _write_drop_debug(drop_debug_path, {
                    "event": "poll",
                    "attempt": job.attempts,
                    "sent_at": sent_ts,
                    "received_at": received_ts,
                    "job": {
                        "chat_id": job.chat_id,
                        "date": job.date,
                        "target_time": job.target_time,
                        "court": job.court,
                        "count": job.count,
                        "duration_min": job.duration_min,
                        "partial": job.partial,
                        "same_number": job.same_number,
                    },
                    "all_slots": all_slots,
                    "consecutive_runs": {court: slots for court, slots in runs.items()},
                    "progress": dict(court_progress),
                    "plan": [(c, s["time"], n, is_new) for c, s, n, is_new in actions],
                })

            # Execute the plan.
            if actions:
                newly: list[tuple] = []
                for court, start_slot, n_slots, is_new in actions:
                    ok, msg = await _book_chunk(job, start_slot, n_slots)
                    if ok:
                        court_progress[court] = court_progress.get(court, 0) + n_slots
                        prog = court_progress[court]
                        newly.append((court, start_slot["time"], prog, is_new))
                        try:
                            logger.info(
                                "%s booked %s @ %s +%dm -> %d/%d slots",
                                tag, court, start_slot["time"], n_slots * SLOT_MINUTES,
                                prog, requested_slots,
                            )
                        except Exception:
                            pass
                    else:
                        try:
                            logger.warning(
                                "%s booking failed %s @ %s (%s)",
                                tag, court, start_slot["time"], msg,
                            )
                        except Exception:
                            pass

                if len(_complete_courts()) >= job.count:
                    await _finish()
                    return

                if newly:
                    lines = []
                    for court, t, prog, is_new in newly:
                        total = prog * SLOT_MINUTES
                        if prog >= requested_slots:
                            lines.append(f"✅ {court}: full {total}m secured")
                        elif is_new:
                            lines.append(f"🎾 {court}: grabbed {t} "
                                         f"({total}m, extending to {duration_min}m…)")
                        else:
                            lines.append(f"➕ {court}: extended to {total}m")
                    await notify(job, "\n".join(lines))
                    persist(job)

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
