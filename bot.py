"""Telegram interface for court sniping, direct booking, and reservation cleanup.

At a high level this file is the conversation layer of the app:

1. validate that a Telegram user is approved
2. collect command arguments and any follow-up choices
3. translate those inputs into ``yourcourts`` calls or ``snipe_job`` tasks
4. send the resulting status, confirmation, and recovery messages back to chat
"""

import asyncio
import os
import re
import time
from collections import defaultdict

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

import snipe_job
import yourcourts

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
_raw_ids = os.environ.get("TELEGRAM_ALLOWED_CHAT_IDS", "").strip()
ALLOWED_CHAT_IDS: set[int] = {int(x) for x in _raw_ids.split(",") if x.strip()}
_admin_raw = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "").strip()
ADMIN_CHAT_ID: int | None = int(_admin_raw) if _admin_raw else None

# chat_id -> {"name": str, "ts": float} — users awaiting admin decision (in-memory; resets on restart)
PENDING_APPROVALS: dict[int, dict] = {}

REGISTRY = snipe_job.JobRegistry()

# Per-chat cache of the slot list shown in /list, so callback buttons can
# reference slots by short index instead of stuffing all fields into callback_data
# (which has a 64-byte limit).
LIST_CACHE: dict[int, dict] = {}  # chat_id -> {"date": str, "slots": list[dict], "ts": float}
LIST_CACHE_TTL = 600  # seconds

BOOKINGS_CACHE: dict[int, dict] = {}  # chat_id -> {"bookings": list[dict], "ts": float, "session": Session}
BOOKINGS_CACHE_TTL = 600  # seconds

PENDING_RESUME: dict[int, list[snipe_job.SnipeJob]] = {}  # chat_id -> interrupted jobs awaiting /resume


def _allowed(update: Update) -> bool:
    """Check whether the current Telegram chat is allowed to use the bot."""
    chat = update.effective_chat
    if chat is None:
        return False
    return chat.id in ALLOWED_CHAT_IDS


async def _deny(update: Update, ctx: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    """Route unauthorized activity into the approval flow.

    Regular chat messages can trigger an admin approval request. Callback taps
    only get a rejection because there is no useful context to forward.
    """
    if update.message and ctx is not None:
        await _request_access(update, ctx)
    elif update.message:
        await update.message.reply_text("Not authorized.")
    elif update.callback_query:
        await update.callback_query.answer("Not authorized.", show_alert=True)


def _display_name(update: Update) -> str:
    """Build a friendly label for approval notifications and audit storage."""
    user = update.effective_user
    if not user:
        return "?"
    parts = [user.first_name or "", user.last_name or ""]
    name = " ".join(p for p in parts if p).strip()
    if user.username:
        name = f"{name} (@{user.username})" if name else f"@{user.username}"
    return name or str(user.id)


async def _request_access(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Forward an access request to the admin and remember it in memory."""
    if not update.message or not update.effective_chat:
        return

    chat_id = update.effective_chat.id
    name = _display_name(update)
    print(f"[access] request from chat_id={chat_id} name={name!r} pending={chat_id in PENDING_APPROVALS}", flush=True)

    if ADMIN_CHAT_ID is None:
        await update.message.reply_text(
            f"Not authorized. Your chat_id is `{chat_id}`. Ask the admin to add it.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    is_retry = chat_id in PENDING_APPROVALS
    PENDING_APPROVALS[chat_id] = {"name": name, "ts": time.time()}
    await update.message.reply_text(
        "Access request sent to the admin. You'll get a message when you're approved."
        if not is_retry else
        "Still pending — I pinged the admin again."
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Approve", callback_data=f"approve:{chat_id}"),
         InlineKeyboardButton("❌ Deny", callback_data=f"deny:{chat_id}")],
    ])
    prefix = "🔁 Repeat access request" if is_retry else "🔑 Access request"
    try:
        await ctx.application.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=f"{prefix} from {name}\nchat_id: {chat_id}",
            reply_markup=kb,
        )
        print(f"[access] notified admin {ADMIN_CHAT_ID} about {chat_id}", flush=True)
    except Exception as e:
        print(f"[access] could not notify admin {ADMIN_CHAT_ID}: {e}", flush=True)


async def unknown_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Use unknown inbound messages as another entry into the approval flow."""
    if not update.effective_chat:
        return
    if _allowed(update):
        return  # allowed users hitting an unknown command — ignore silently
    if update.message:
        await _request_access(update, ctx)


def _parse_time(raw: str) -> str | None:
    """Normalize Telegram command time tokens into the format used everywhere else."""
    raw = raw.strip().upper().replace(" ", "")
    m = re.fullmatch(r"(\d{1,2}):(\d{2})(AM|PM)", raw)
    if not m:
        return None
    hour, minute, meridiem = m.group(1), m.group(2), m.group(3)
    return f"{int(hour)}:{minute}{meridiem}"


def _parse_date(raw: str) -> str | None:
    """Validate date strings without converting them away from user-facing format."""
    return raw if re.fullmatch(r"\d{2}/\d{2}/\d{4}", raw) else None


def _make_notifier(app: Application):
    """Build the callback that ``snipe_job.run_snipe`` uses for user-facing updates.

    This keeps the polling engine decoupled from Telegram specifics: the job
    layer only knows it can emit messages, while this closure handles the actual
    bot send and registry cleanup.
    """
    async def notify(job: snipe_job.SnipeJob, text: str, markup: str | None = None) -> None:
        try:
            await app.bot.send_message(
                chat_id=job.chat_id, text=text,
                parse_mode=ParseMode.MARKDOWN if markup else None,
            )
        except Exception as e:
            print(f"notify failed for {job.chat_id}: {e}")
        # Drop the job from the live registry once it terminates.
        if job.status in ("booked", "error", "cancelled"):
            REGISTRY.drop(job)
    return notify


def _make_spawner(app: Application):
    """Return a callback that can start chained follow-up jobs after a booking."""
    async def spawn(job: snipe_job.SnipeJob) -> None:
        if not REGISTRY.add(job):
            # User is already at the job cap — skip the chain.
            return
        snipe_job.persist(job)
        notify = _make_notifier(app)
        job.task = asyncio.create_task(snipe_job.run_snipe(job, notify, _make_spawner(app)))
    return spawn


# ---------- /start, /help ----------

async def start_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Entry point for new users: either request approval or explain the bot flow."""
    if not _allowed(update):
        await _request_access(update, ctx)
        return
    creds = snipe_job.get_credentials(update.effective_chat.id)
    if not creds:
        await update.message.reply_text(
            "🎾 *Court bot ready.*\n\n"
            "First, connect your yourcourts.com account:\n"
            "`/login your@email.com yourpassword`\n\n"
            "Then you can:\n"
            "`/snipe 05/25/2026 10:00AM` — auto-book when slot opens\n"
            "`/list 05/25/2026` — see & tap-to-book current openings\n\n"
            "/help for full reference.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    await update.message.reply_text(
        "🎾 *Court bot ready.*\n\n"
        "I do two things:\n"
        "1️⃣ *Snipe* — poll a date/time and auto-book it the second it opens.\n"
        "2️⃣ *Book directly* — show what's open right now and let you tap to book.\n\n"
        "*Try these:*\n"
        "`/snipe 05/25/2026 10:00AM`\n"
        "  → polls every 15s; pings you when booked\n"
        "`/snipe 05/25/2026 7:00AM PB 4B`\n"
        "  → only takes that exact court\n"
        "`/list 05/25/2026`\n"
        "  → shows all openings as tap-to-book buttons\n"
        "`/bookings`\n"
        "  → list your upcoming reservations; tap to cancel\n\n"
        "*Managing a running snipe:*\n"
        "/status — see how it's going\n"
        "/cancel — stop it\n"
        "/resume — restart one that got interrupted\n\n"
        "Rules: one active snipe at a time. You'll only get a ping on "
        "found / booked / error — not on every poll.\n\n"
        "/help for the full reference.",
        parse_mode=ParseMode.MARKDOWN,
    )


async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Send the condensed command reference once a user is authorized."""
    if not _allowed(update):
        await _deny(update, ctx); return
    await update.message.reply_text(
        "*Account:*\n"
        "`/login your@email.com yourpassword`\n"
        "`/logout`\n\n"
        "*Snipe* `DATE TIME [COUNT] [DURATION] [court] [+CHAIN_TIME]`:\n"
        "`/snipe 05/25/2026 10:00AM`\n"
        "`/snipe 05/25/2026 7:00AM PB 4B`\n"
        "`/snipe 05/25/2026 10:00AM 3` — book 3 courts\n"
        "`/snipe 05/25/2026 10:00AM 3 60 PB` — 3 PB courts, 60 min each\n"
        "`/snipe 05/25/2026 10:00AM 2 90 +11:30AM` — 2 courts 90 min, then chain another snipe at 11:30AM\n"
        "For multiple courts I grab the best available right away and keep polling\n"
        "to fill the rest, preferring the same court number then the closest\n"
        "(separated courts are fine if that's all that opens).\n"
        "DURATION must be a multiple of 30 (max 240). Each court is only booked\n"
        "when its full duration is available — otherwise I keep waiting.\n"
        "Dates more than 10 days out start polling at 7:00AM on the 10-day window date.\n\n"
        "*Direct booking* (book from current availability):\n"
        "`/list 05/25/2026`\n\n"
        "*Your reservations:*\n"
        "`/bookings` — list your upcoming reservations; tap to cancel\n\n"
        f"Up to {snipe_job.MAX_JOBS_PER_USER} active snipes at a time. /status to list, /cancel N to drop one.",
        parse_mode=ParseMode.MARKDOWN,
    )


# ---------- /login, /logout ----------

async def login_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Verify YourCourts credentials now so later snipes can run unattended."""
    if not _allowed(update):
        await _deny(update, ctx); return

    args = ctx.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /login your@email.com yourpassword")
        return

    email, password = args[0], args[1]
    await update.message.reply_text("Checking credentials...")

    session = yourcourts.make_session()
    ok = await asyncio.to_thread(yourcourts.login, session, email, password)
    if not ok:
        await update.message.reply_text("❌ Login failed — double-check your email and password.")
        return

    snipe_job.save_credentials(update.effective_chat.id, email, password)
    await update.message.reply_text(
        f"✅ Logged in as {email}. You're good to go — try /snipe or /list."
    )


async def logout_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove stored credentials after ensuring no active jobs still depend on them."""
    if not _allowed(update):
        await _deny(update, ctx); return

    chat_id = update.effective_chat.id
    if REGISTRY.active_count(chat_id) > 0:
        await update.message.reply_text("You have active snipes — /cancel them first.")
        return

    snipe_job.delete_credentials(chat_id)
    await update.message.reply_text("Credentials removed.")


# ---------- /snipe ----------

async def snipe_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Parse ``/snipe`` arguments and start a job immediately.

    This is the bridge from chat commands into :mod:`snipe_job`: it validates
    the request, loads credentials, builds a :class:`snipe_job.SnipeJob`, and
    launches it. Multi-court jobs grab the best available subset each poll and
    keep filling until `count` courts are booked.
    """
    if not _allowed(update):
        await _deny(update, ctx); return

    chat_id = update.effective_chat.id

    if REGISTRY.active_count(chat_id) >= snipe_job.MAX_JOBS_PER_USER:
        running = REGISTRY.active_for(chat_id)
        lines = ", ".join(f"{j.date} {j.target_time}" for j in running)
        await update.message.reply_text(
            f"You're at the {snipe_job.MAX_JOBS_PER_USER}-snipe limit. Active: {lines}.\n"
            f"Use /cancel N to drop one first (see /status)."
        )
        return

    args = ctx.args
    if len(args) < 2:
        await update.message.reply_text(
            "Usage: /snipe MM/DD/YYYY H:MMam/pm [COUNT] [DURATION] [court] [+CHAIN_TIME]"
        )
        return

    date = _parse_date(args[0])
    if not date:
        await update.message.reply_text("Bad date format. Use MM/DD/YYYY.")
        return

    target_time = _parse_time(args[1])
    if not target_time:
        await update.message.reply_text("Bad time format. Use H:MMam or H:MMpm.")
        return

    rest = list(args[2:])

    # Chain time is optional and can appear anywhere after the main date/time.
    chain_time: str | None = None
    chain_tokens = [t for t in rest if t.startswith("+")]
    rest = [t for t in rest if not t.startswith("+")]
    if chain_tokens:
        if len(chain_tokens) > 1:
            await update.message.reply_text("Only one +CHAIN_TIME is supported per /snipe.")
            return
        ct = _parse_time(chain_tokens[0][1:])
        if not ct:
            await update.message.reply_text("Bad +CHAIN_TIME format. Example: +11:30AM")
            return
        chain_time = ct

    # Numeric suffixes are positional: first court count, then requested duration.
    count = 1
    duration_min = 30
    if rest and rest[0].isdigit():
        count = int(rest.pop(0))
        if count < 1 or count > 20:
            await update.message.reply_text("COUNT must be between 1 and 20.")
            return
    if rest and rest[0].isdigit():
        duration_min = int(rest.pop(0))
        if duration_min < 30 or duration_min > 240 or duration_min % 30 != 0:
            await update.message.reply_text("DURATION must be 30, 60, 90, 120, ... up to 240.")
            return

    court = " ".join(rest).strip() or None

    creds = snipe_job.get_credentials(chat_id)
    if not creds:
        await update.message.reply_text("No account connected. Run /login first.")
        return
    email, password = creds

    if any(j.date == date and j.target_time == target_time for j in REGISTRY.active_for(chat_id)):
        await update.message.reply_text(
            f"You already have an active snipe for {target_time} on {date}."
        )
        return

    job = snipe_job.SnipeJob(
        chat_id=chat_id, date=date, target_time=target_time, court=court,
        email=email, password=password, count=count,
        duration_min=duration_min, chain_time=chain_time,
    )

    await _start_snipe(job, update, ctx)


async def _start_snipe(job: snipe_job.SnipeJob, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Register, persist, and launch a configured snipe job."""
    if not REGISTRY.add(job):
        msg = f"You're at the {snipe_job.MAX_JOBS_PER_USER}-snipe limit. /cancel one first."
        if update.callback_query:
            await update.callback_query.edit_message_text(msg)
        else:
            await update.message.reply_text(msg)
        return
    snipe_job.persist(job)
    notify = _make_notifier(ctx.application)
    spawn = _make_spawner(ctx.application)
    job.task = asyncio.create_task(snipe_job.run_snipe(job, notify, spawn))

    label = job.court or "any court"
    extras = []
    if job.count > 1:
        extras.append(f"{job.count} courts")
        extras.append("grabs best available, fills the rest (prefers same/closest court numbers)")
    if job.duration_min != 30:
        extras.append(f"{job.duration_min} min")
    if job.chain_time:
        extras.append(f"chain → {job.chain_time}")
    extras_str = f" ({'; '.join(extras)})" if extras else ""

    activation = snipe_job.activation_label(job) if snipe_job.seconds_until_activation(job) > 0 else None
    if activation:
        text = (
            f"🎯 Drop snipe [{job.slot_index + 1}] scheduled: {job.target_time} on {job.date} ({label}){extras_str}.\n"
            f"It will start polling at {activation}, when the court window opens."
        )
    else:
        text = (
            f"🎯 Snipe [{job.slot_index + 1}] started: {job.target_time} on {job.date} ({label}){extras_str}.\n"
            f"You'll get a ping when something opens up or it's booked."
        )
    if update.callback_query:
        await update.callback_query.edit_message_text(text)
    else:
        await update.message.reply_text(text)


# ---------- /status ----------

async def status_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Summarize the currently active in-memory jobs for this chat."""
    if not _allowed(update):
        await _deny(update, ctx); return

    chat_id = update.effective_chat.id
    jobs = sorted(REGISTRY.active_for(chat_id), key=lambda j: j.slot_index)
    if not jobs:
        await update.message.reply_text("No active snipes.")
        return

    out = [f"Active snipes ({len(jobs)}/{snipe_job.MAX_JOBS_PER_USER}):"]
    for j in jobs:
        runtime = int(time.time() - j.started_at)
        mins, secs = divmod(runtime, 60)
        label = j.court or "any court"
        out.append(f"\n[{j.slot_index + 1}] {j.target_time} on {j.date} ({label})")
        out.append(f"  • status: {j.status}; attempts: {j.attempts}; runtime: {mins}m {secs}s")
        activation = snipe_job.activation_label(j) if snipe_job.seconds_until_activation(j) > 0 else None
        if activation:
            out.append(f"  • drop snipe — starts polling: {activation}")
        if j.count > 1:
            out.append(f"  • {j.count} courts; grabbing best available, polling for the rest")
        if j.duration_min != 30:
            out.append(f"  • duration: {j.duration_min} min")
        if j.chain_time:
            out.append(f"  • chain on success → {j.chain_time}")
    await update.message.reply_text("\n".join(out))


# ---------- /cancel ----------

async def cancel_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Cancel one running snipe, or prompt the user to choose when several exist."""
    if not _allowed(update):
        await _deny(update, ctx); return

    chat_id = update.effective_chat.id
    jobs = sorted(REGISTRY.active_for(chat_id), key=lambda j: j.slot_index)
    if not jobs:
        await update.message.reply_text("No active snipes to cancel.")
        return

    if ctx.args:
        try:
            n = int(ctx.args[0]) - 1
        except ValueError:
            await update.message.reply_text("Usage: /cancel [N]  (see /status for N)")
            return
        target = REGISTRY.find(chat_id, n)
        if not target or not target.task:
            await update.message.reply_text(f"No active snipe at slot {n + 1}.")
            return
        target.task.cancel()
        await update.message.reply_text(f"Cancelling [{n + 1}] {target.date} {target.target_time}...")
        return

    if len(jobs) == 1:
        jobs[0].task.cancel()
        await update.message.reply_text("Cancelling...")
        return

    listing = "\n".join(f"  [{j.slot_index + 1}] {j.date} {j.target_time}" for j in jobs)
    await update.message.reply_text(
        f"You have {len(jobs)} active snipes. Use /cancel N to pick:\n{listing}"
    )


# ---------- /resume ----------

async def resume_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Restart a job that was marked interrupted during startup recovery."""
    if not _allowed(update):
        await _deny(update, ctx); return

    chat_id = update.effective_chat.id
    pending_list = PENDING_RESUME.get(chat_id) or []
    if not pending_list:
        await update.message.reply_text("Nothing to resume.")
        return

    if ctx.args:
        try:
            idx = int(ctx.args[0]) - 1
        except ValueError:
            await update.message.reply_text("Usage: /resume [N]")
            return
    elif len(pending_list) == 1:
        idx = 0
    else:
        listing = "\n".join(
            f"  [{i + 1}] {p.date} {p.target_time}" for i, p in enumerate(pending_list)
        )
        await update.message.reply_text(
            f"Multiple interrupted snipes — pick one with /resume N:\n{listing}"
        )
        return

    if idx < 0 or idx >= len(pending_list):
        await update.message.reply_text(f"No interrupted snipe at slot {idx + 1}.")
        return

    if REGISTRY.active_count(chat_id) >= snipe_job.MAX_JOBS_PER_USER:
        await update.message.reply_text(
            f"You're at the {snipe_job.MAX_JOBS_PER_USER}-snipe limit. /cancel one first."
        )
        return

    pending = pending_list.pop(idx)
    if not pending_list:
        PENDING_RESUME.pop(chat_id, None)

    pending.status = "running"
    pending.attempts = 0
    pending.started_at = time.time()
    if not REGISTRY.add(pending):
        await update.message.reply_text("Could not start — registry full.")
        return
    snipe_job.persist(pending)

    notify = _make_notifier(ctx.application)
    spawn = _make_spawner(ctx.application)
    pending.task = asyncio.create_task(snipe_job.run_snipe(pending, notify, spawn))
    activation = snipe_job.activation_label(pending) if snipe_job.seconds_until_activation(pending) > 0 else None
    if activation:
        await update.message.reply_text(
            f"Resumed drop snipe [{pending.slot_index + 1}]: {pending.target_time} on {pending.date}. "
            f"It will start polling at {activation}, when the court window opens."
        )
    else:
        await update.message.reply_text(
            f"Resumed snipe [{pending.slot_index + 1}]: {pending.target_time} on {pending.date}."
        )


# ---------- /list (direct booking) ----------

async def list_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Run the direct-booking flow: fetch openings and present them as buttons."""
    if not _allowed(update):
        await _deny(update, ctx); return

    args = ctx.args
    if not args:
        await update.message.reply_text("Usage: /list MM/DD/YYYY")
        return
    date = _parse_date(args[0])
    if not date:
        await update.message.reply_text("Bad date format. Use MM/DD/YYYY.")
        return

    creds = snipe_job.get_credentials(update.effective_chat.id)
    if not creds:
        await update.message.reply_text("No account connected. Run /login first.")
        return
    email, password = creds

    await update.message.reply_text(f"Fetching openings for {date}...")

    session = yourcourts.make_session()
    ok = await asyncio.to_thread(yourcourts.login, session, email, password)
    if not ok:
        await update.message.reply_text("Login to yourcourts.com failed — try /login again.")
        return

    slots = await asyncio.to_thread(yourcourts.find_slots, session, date)
    if not slots:
        await update.message.reply_text("No openings.")
        return

    # Cache slots by short index so callback_data stays within Telegram's size limit.
    chat_id = update.effective_chat.id
    LIST_CACHE[chat_id] = {"date": date, "slots": slots, "ts": time.time(), "session": session}

    # Group by time for readable buttons.
    by_time = defaultdict(list)
    for i, s in enumerate(slots):
        by_time[s["time"]].append((i, s))

    sorted_times = sorted(by_time.keys(), key=lambda t: yourcourts.time_to_id(t))

    # Telegram caps ~100 buttons per message; we'll send up to 60 per batch to be safe.
    BATCH = 60
    button_rows: list[list[InlineKeyboardButton]] = []
    for t in sorted_times:
        for i, s in sorted(by_time[t], key=lambda x: x[1]["court"]):
            label = f"{s['time']} — {s['court']}"
            button_rows.append([InlineKeyboardButton(label, callback_data=f"pick:{i}")])

    for start in range(0, len(button_rows), BATCH):
        chunk = button_rows[start:start + BATCH]
        await update.message.reply_text(
            f"Openings on {date} ({start + 1}–{start + len(chunk)} of {len(button_rows)}):",
            reply_markup=InlineKeyboardMarkup(chunk),
        )


# ---------- /bookings (list + cancel my reservations) ----------

def _bookings_keyboard(upcoming: list[dict], selected: set[int]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for i, b in enumerate(upcoming):
        start_dt = b["start"][:16].replace("-", "/")
        check = "☑" if i in selected else "☐"
        label = f"{check} {start_dt} — {b['court']} ({b['times']})"
        rows.append([InlineKeyboardButton(label, callback_data=f"cxltoggle:{i}")])
    bottom = []
    if selected:
        bottom.append(InlineKeyboardButton(
            f"🗑 Cancel {len(selected)} selected", callback_data="cxlbulk"
        ))
    bottom.append(InlineKeyboardButton("✖ Done", callback_data="cxlabort"))
    rows.append(bottom)
    return InlineKeyboardMarkup(rows)


async def bookings_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """List the user's upcoming reservations with multi-select cancellation."""
    if not _allowed(update):
        await _deny(update, ctx); return

    chat_id = update.effective_chat.id
    creds = snipe_job.get_credentials(chat_id)
    if not creds:
        await update.message.reply_text("No account connected. Run /login first.")
        return
    email, password = creds

    await update.message.reply_text("Fetching your reservations...")

    session = yourcourts.make_session()
    ok = await asyncio.to_thread(yourcourts.login, session, email, password)
    if not ok:
        await update.message.reply_text("Login to yourcourts.com failed — try /login again.")
        return

    try:
        bookings = await asyncio.to_thread(yourcourts.list_my_bookings, session)
    except Exception as e:
        await update.message.reply_text(f"Could not load bookings: {e}")
        return

    upcoming = [b for b in bookings if not b["is_past"]]
    if not upcoming:
        await update.message.reply_text("You have no upcoming reservations.")
        return

    upcoming.sort(key=lambda b: b["start"])
    BOOKINGS_CACHE[chat_id] = {"bookings": upcoming, "ts": time.time(), "session": session, "selected": set()}

    await update.message.reply_text(
        f"Your {len(upcoming)} upcoming reservation(s) — tap to select, then cancel all at once:",
        reply_markup=_bookings_keyboard(upcoming, set()),
    )


# ---------- Callback queries (book confirmation flow) ----------

async def callback_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle every inline-button flow in one place.

    The callbacks are grouped by prefix:

    - ``pick`` / ``confirm`` for direct booking
    - ``cxl*`` for reservation cancellation
    - ``approve`` / ``deny`` for admin access control
    """
    if not _allowed(update):
        await _deny(update, ctx); return

    query = update.callback_query
    await query.answer()
    data = query.data or ""
    chat_id = update.effective_chat.id

    if data.startswith("pick:"):
        idx = int(data.split(":", 1)[1])
        cache = LIST_CACHE.get(chat_id)
        if not cache or time.time() - cache["ts"] > LIST_CACHE_TTL:
            await query.edit_message_text("This list expired. Run /list again.")
            return
        if idx >= len(cache["slots"]):
            await query.edit_message_text("Slot not found. Run /list again.")
            return
        slot = cache["slots"][idx]
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Confirm", callback_data=f"confirm:{idx}"),
             InlineKeyboardButton("✖ Cancel", callback_data="abort")],
        ])
        await query.edit_message_text(
            f"Book *{slot['court']}* @ *{slot['time']}* on *{cache['date']}*?",
            reply_markup=kb,
            parse_mode=ParseMode.MARKDOWN,
        )

    elif data.startswith("confirm:"):
        idx = int(data.split(":", 1)[1])
        cache = LIST_CACHE.get(chat_id)
        if not cache or time.time() - cache["ts"] > LIST_CACHE_TTL:
            await query.edit_message_text("This list expired. Run /list again.")
            return
        slot = cache["slots"][idx]
        await query.edit_message_text(f"Booking {slot['court']} @ {slot['time']}...")
        ok, msg = await asyncio.to_thread(
            yourcourts.book_slot, cache["session"], slot, cache["date"]
        )
        await query.edit_message_text(("✅ " if ok else "❌ ") + msg)

    elif data == "abort":
        await query.edit_message_text("Cancelled.")

    elif data.startswith("cxltoggle:"):
        idx = int(data.split(":", 1)[1])
        cache = BOOKINGS_CACHE.get(chat_id)
        if not cache or time.time() - cache["ts"] > BOOKINGS_CACHE_TTL:
            await query.edit_message_text("This list expired. Run /bookings again.")
            return
        selected: set[int] = cache["selected"]
        if idx in selected:
            selected.discard(idx)
        else:
            selected.add(idx)
        await query.edit_message_reply_markup(
            reply_markup=_bookings_keyboard(cache["bookings"], selected)
        )

    elif data == "cxlbulk":
        cache = BOOKINGS_CACHE.get(chat_id)
        if not cache or time.time() - cache["ts"] > BOOKINGS_CACHE_TTL:
            await query.edit_message_text("This list expired. Run /bookings again.")
            return
        selected = cache["selected"]
        if not selected:
            await query.answer("No reservations selected.", show_alert=True)
            return
        to_cancel = sorted(selected)
        await query.edit_message_text(f"Cancelling {len(to_cancel)} reservation(s)…")
        results = []
        for idx in to_cancel:
            b = cache["bookings"][idx]
            start_dt = b["start"][:16].replace("-", "/")
            try:
                ok, msg = await asyncio.to_thread(
                    yourcourts.cancel_reservation, cache["session"], b["id"]
                )
                results.append(("✅" if ok else "❌") + f" {start_dt} — {b['court']}: {msg}")
            except Exception as exc:
                results.append(f"❌ {start_dt} — {b['court']}: {exc}")
        BOOKINGS_CACHE.pop(chat_id, None)
        await query.edit_message_text("\n".join(results))

    elif data == "cxlabort":
        await query.edit_message_text("Done.")

    elif data.startswith("approve:") or data.startswith("deny:"):
        if chat_id != ADMIN_CHAT_ID:
            await query.answer("Only the admin can do that.", show_alert=True)
            return
        action, _, target_raw = data.partition(":")
        target_id = int(target_raw)
        pending = PENDING_APPROVALS.pop(target_id, None)
        name = pending["name"] if pending else str(target_id)

        if action == "approve":
            snipe_job.add_allowed_user(target_id, name)
            ALLOWED_CHAT_IDS.add(target_id)
            await query.edit_message_text(f"✅ Approved {name} ({target_id}).")
            try:
                await ctx.application.bot.send_message(
                    chat_id=target_id,
                    text="✅ You're approved! Send /login your@email.com yourpassword to connect your yourcourts account, then try /start.",
                )
            except Exception as e:
                print(f"could not notify approved user {target_id}: {e}")
        else:
            await query.edit_message_text(f"❌ Denied {name} ({target_id}).")
            try:
                await ctx.application.bot.send_message(
                    chat_id=target_id, text="Your access request was denied."
                )
            except Exception as e:
                print(f"could not notify denied user {target_id}: {e}")


# ---------- Startup ----------

async def post_init(app: Application) -> None:
    """Load durable state once Telegram has initialized the app object.

    This is where the process-level bot reconnects to the persistent world:
    approved users are reloaded, and interrupted snipes are surfaced so people
    can consciously resume or cancel them after a restart.
    """
    for cid in snipe_job.load_allowed_users():
        ALLOWED_CHAT_IDS.add(cid)
    if ADMIN_CHAT_ID is not None:
        ALLOWED_CHAT_IDS.add(ADMIN_CHAT_ID)
    print(f"Allowed chat_ids ({len(ALLOWED_CHAT_IDS)}):", ALLOWED_CHAT_IDS)

    interrupted = snipe_job.mark_all_interrupted()
    notify = _make_notifier(app)
    for job in interrupted:
        if job.chat_id not in ALLOWED_CHAT_IDS:
            snipe_job.remove(job.chat_id, job.date, job.target_time)
            continue
        PENDING_RESUME.setdefault(job.chat_id, []).append(job)
        try:
            await notify(
                job,
                f"⚠️ Snipe was interrupted: {job.target_time} on {job.date}. "
                f"Send /resume to continue or /cancel to drop it."
            )
        except Exception as e:
            print(f"could not ping {job.chat_id} about interrupted job: {e}")


def main() -> None:
    """Wire up handlers and start Telegram long-polling."""
    if not TOKEN:
        raise SystemExit("TELEGRAM_BOT_TOKEN not set in .env")
    if not ALLOWED_CHAT_IDS and ADMIN_CHAT_ID is None:
        raise SystemExit("Set TELEGRAM_ADMIN_CHAT_ID (or TELEGRAM_ALLOWED_CHAT_IDS) in .env")

    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("login", login_cmd))
    app.add_handler(CommandHandler("logout", logout_cmd))
    app.add_handler(CommandHandler("snipe", snipe_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))
    app.add_handler(CommandHandler("resume", resume_cmd))
    app.add_handler(CommandHandler("list", list_cmd))
    app.add_handler(CommandHandler("book", list_cmd))
    app.add_handler(CommandHandler("bookings", bookings_cmd))
    app.add_handler(CallbackQueryHandler(callback_handler))
    # Catch-all: any other message (text, sticker, unknown command, etc.) from an
    # unauthorized user is routed through the access-request flow.
    app.add_handler(MessageHandler(filters.ALL, unknown_handler))

    async def _on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        print(f"[error] {type(context.error).__name__}: {context.error}", flush=True)
    app.add_error_handler(_on_error)

    print("Bot starting. Allowed chat_ids:", ALLOWED_CHAT_IDS)
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
