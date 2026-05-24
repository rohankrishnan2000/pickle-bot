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
    task: Optional[asyncio.Task] = None
    session: Optional[requests.Session] = None


NotifyCb = Callable[[SnipeJob, str], Awaitable[None]]


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
        INSERT INTO jobs (chat_id, date, target_time, court, email, password, status, attempts, started_at, last_message, count, partial, same_number)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            same_number=excluded.same_number
        """,
        (job.chat_id, job.date, job.target_time, job.court, job.email, job.password,
         job.status, job.attempts, job.started_at, job.last_message,
         job.count, int(job.partial), int(job.same_number)),
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
        "SELECT chat_id, date, target_time, court, email, password, status, attempts, started_at, last_message, count, partial, same_number FROM jobs"
    ).fetchall()
    conn.close()
    jobs = []
    for r in rows:
        jobs.append(SnipeJob(
            chat_id=r[0], date=r[1], target_time=r[2], court=r[3],
            email=r[4] or "", password=r[5] or "",
            status=r[6], attempts=r[7], started_at=r[8], last_message=r[9] or "",
            count=r[10] or 1, partial=bool(r[11]), same_number=bool(r[12]),
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


async def run_snipe(job: SnipeJob, notify: NotifyCb) -> None:
    """Polling loop. Notifies only on found/booked/error per user preference."""
    job.session = yourcourts.make_session()

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
                # For multi-court jobs, court is treated as a prefix filter and
                # applied post-fetch so partial filters like 'PB' still match.
                court_filter = None if job.count > 1 else job.court
                matches = await _find(job.session, job.date, job.target_time, court_filter)
                if job.count > 1:
                    matches = _filter_by_court_prefix(matches, job.court)
            except requests.RequestException as e:
                job.last_message = f"network error: {e}"
                persist(job)
                await asyncio.sleep(POLL_INTERVAL)
                continue

            if job.count > 1:
                chosen = pick_group(matches, job.count, job.same_number, job.partial)
            else:
                chosen = matches[:1] if matches else None

            if not chosen:
                if job.attempts % REFRESH_LOGIN_EVERY == 0:
                    await _do_login(job.session, job.email, job.password)
                persist(job)
                await asyncio.sleep(POLL_INTERVAL)
                continue

            slot_summary = ", ".join(f"{s['court']} @ {s['time']}" for s in chosen)
            await notify(job, f"🎾 Found {len(chosen)} slot(s): {slot_summary}. Attempting to book...")

            booked: list[str] = []
            failures: list[str] = []
            for s in chosen:
                ok, msg = await _book(job.session, s, job.date)
                if ok:
                    booked.append(f"{s['court']} @ {s['time']}")
                else:
                    failures.append(f"{s['court']} ({msg})")
                    if job.count == 1:
                        # Single-court mode: keep trying other matches.
                        for alt in matches[1:]:
                            ok2, msg2 = await _book(job.session, alt, job.date)
                            if ok2:
                                booked.append(f"{alt['court']} @ {alt['time']}")
                                break
                            failures.append(f"{alt['court']} ({msg2})")
                        break

            if booked:
                summary = f"Booked {len(booked)} court(s) on {job.date}: " + ", ".join(booked)
                if failures:
                    summary += f". Failed: {', '.join(failures)}"
                job.status = "booked"
                job.last_message = summary
                persist(job)
                await notify(job, f"✅ {summary}")
                return

            job.last_message = "All booking attempts failed; resuming poll"
            persist(job)
            await notify(job, "⚠️ Found slots but all booking attempts failed. Resuming poll.")
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
