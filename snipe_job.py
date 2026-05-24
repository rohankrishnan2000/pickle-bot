"""Async snipe job management. One active job per chat_id; SQLite-mirrored state."""

import asyncio
import sqlite3
import time
from dataclasses import dataclass, field
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
    status: str = "running"  # running | booked | cancelled | error | interrupted
    attempts: int = 0
    started_at: float = field(default_factory=time.time)
    last_message: str = ""
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
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            started_at REAL NOT NULL,
            last_message TEXT
        )
    """)
    conn.commit()
    return conn


def persist(job: SnipeJob) -> None:
    conn = _db()
    conn.execute(
        """
        INSERT INTO jobs (chat_id, date, target_time, court, status, attempts, started_at, last_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET
            date=excluded.date,
            target_time=excluded.target_time,
            court=excluded.court,
            status=excluded.status,
            attempts=excluded.attempts,
            started_at=excluded.started_at,
            last_message=excluded.last_message
        """,
        (job.chat_id, job.date, job.target_time, job.court, job.status,
         job.attempts, job.started_at, job.last_message),
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
        "SELECT chat_id, date, target_time, court, status, attempts, started_at, last_message FROM jobs"
    ).fetchall()
    conn.close()
    jobs = []
    for r in rows:
        jobs.append(SnipeJob(
            chat_id=r[0], date=r[1], target_time=r[2], court=r[3],
            status=r[4], attempts=r[5], started_at=r[6], last_message=r[7] or "",
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


async def _do_login(session: requests.Session) -> bool:
    return await asyncio.to_thread(yourcourts.login, session)


async def _find(session, date, target_time, court):
    return await asyncio.to_thread(yourcourts.find_slots, session, date, target_time, court)


async def _book(session, slot, date):
    return await asyncio.to_thread(yourcourts.book_slot, session, slot, date)


async def run_snipe(job: SnipeJob, notify: NotifyCb) -> None:
    """Polling loop. Notifies only on found/booked/error per user preference."""
    job.session = yourcourts.make_session()

    try:
        if not await _do_login(job.session):
            job.status = "error"
            job.last_message = "Login to yourcourts.com failed"
            persist(job)
            await notify(job, f"❌ Login failed for snipe on {job.date} {job.target_time}")
            return

        while True:
            job.attempts += 1

            try:
                matches = await _find(job.session, job.date, job.target_time, job.court)
            except requests.RequestException as e:
                # Network errors are transient — don't notify on every one, just log + continue
                job.last_message = f"network error: {e}"
                persist(job)
                await asyncio.sleep(POLL_INTERVAL)
                continue

            if not matches:
                if job.attempts % REFRESH_LOGIN_EVERY == 0:
                    await _do_login(job.session)
                persist(job)
                await asyncio.sleep(POLL_INTERVAL)
                continue

            # Found something — notify and try to book.
            slot_summary = ", ".join(f"{s['court']} @ {s['time']}" for s in matches)
            await notify(job, f"🎾 Found {len(matches)} slot(s): {slot_summary}. Attempting to book...")

            for s in matches:
                ok, msg = await _book(job.session, s, job.date)
                if ok:
                    job.status = "booked"
                    job.last_message = msg
                    persist(job)
                    await notify(job, f"✅ {msg}")
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
