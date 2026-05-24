"""Telegram bot for managing court snipes and direct bookings."""

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
)

import snipe_job
import yourcourts

load_dotenv()

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

PENDING_RESUME: dict[int, snipe_job.SnipeJob] = {}  # chat_id -> interrupted job awaiting /resume

# chat_id -> partially-configured SnipeJob awaiting follow-up answers (count, partial, same_number).
PENDING_SNIPE: dict[int, snipe_job.SnipeJob] = {}
PENDING_SNIPE_TTL = 300


def _allowed(update: Update) -> bool:
    chat = update.effective_chat
    if chat is None:
        return False
    return chat.id in ALLOWED_CHAT_IDS


async def _deny(update: Update, ctx: ContextTypes.DEFAULT_TYPE | None = None) -> None:
    """For messages: forward to admin as access request. For callbacks: silent reject."""
    if update.message and ctx is not None:
        await _request_access(update, ctx)
    elif update.message:
        await update.message.reply_text("Not authorized.")
    elif update.callback_query:
        await update.callback_query.answer("Not authorized.", show_alert=True)


def _display_name(update: Update) -> str:
    user = update.effective_user
    if not user:
        return "?"
    parts = [user.first_name or "", user.last_name or ""]
    name = " ".join(p for p in parts if p).strip()
    if user.username:
        name = f"{name} (@{user.username})" if name else f"@{user.username}"
    return name or str(user.id)


async def _request_access(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Called when an unauthorized user messages the bot. Forwards request to admin."""
    if not update.message or not update.effective_chat:
        return

    chat_id = update.effective_chat.id
    name = _display_name(update)

    if chat_id in PENDING_APPROVALS:
        await update.message.reply_text("Your access request is still pending. The admin will let you in soon.")
        return

    if ADMIN_CHAT_ID is None:
        await update.message.reply_text(
            f"Not authorized. Your chat_id is `{chat_id}`. Ask the admin to add it.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    PENDING_APPROVALS[chat_id] = {"name": name, "ts": time.time()}
    await update.message.reply_text(
        "Access request sent to the admin. You'll get a message when you're approved."
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Approve", callback_data=f"approve:{chat_id}"),
         InlineKeyboardButton("❌ Deny", callback_data=f"deny:{chat_id}")],
    ])
    try:
        await ctx.application.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=f"🔑 Access request from *{name}*\nchat_id: `{chat_id}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb,
        )
    except Exception as e:
        print(f"could not notify admin {ADMIN_CHAT_ID}: {e}")


def _parse_time(raw: str) -> str | None:
    raw = raw.strip().upper().replace(" ", "")
    m = re.fullmatch(r"(\d{1,2}):(\d{2})(AM|PM)", raw)
    if not m:
        return None
    hour, minute, meridiem = m.group(1), m.group(2), m.group(3)
    return f"{int(hour)}:{minute}{meridiem}"


def _parse_date(raw: str) -> str | None:
    return raw if re.fullmatch(r"\d{2}/\d{2}/\d{4}", raw) else None


def _make_notifier(app: Application):
    async def notify(job: snipe_job.SnipeJob, text: str) -> None:
        try:
            await app.bot.send_message(chat_id=job.chat_id, text=text)
        except Exception as e:
            print(f"notify failed for {job.chat_id}: {e}")
        # Drop the job from the live registry once it terminates.
        if job.status in ("booked", "error", "cancelled"):
            REGISTRY.drop(job.chat_id)
    return notify


# ---------- /start, /help ----------

async def start_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
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
    if not _allowed(update):
        await _deny(update, ctx); return
    await update.message.reply_text(
        "*Account:*\n"
        "`/login your@email.com yourpassword`\n"
        "`/logout`\n\n"
        "*Snipe* (polls until a slot opens, then books):\n"
        "`/snipe 05/25/2026 10:00AM`\n"
        "`/snipe 05/25/2026 7:00AM PB 4B`\n"
        "`/snipe 05/25/2026 10:00AM 3` — book 3 courts together\n"
        "`/snipe 05/25/2026 10:00AM 3 PB` — 3 courts, PB facility only\n\n"
        "*Direct booking* (book from current availability):\n"
        "`/list 05/25/2026`\n\n"
        "*Your reservations:*\n"
        "`/bookings` — list your upcoming reservations; tap to cancel\n\n"
        "One active snipe per user. Use /cancel to stop it before starting another.",
        parse_mode=ParseMode.MARKDOWN,
    )


# ---------- /login, /logout ----------

async def login_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
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
    if not _allowed(update):
        await _deny(update, ctx); return

    chat_id = update.effective_chat.id
    if REGISTRY.has_active(chat_id):
        await update.message.reply_text("You have an active snipe — /cancel it first.")
        return

    snipe_job.delete_credentials(chat_id)
    await update.message.reply_text("Credentials removed.")


# ---------- /snipe ----------

async def snipe_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        await _deny(update, ctx); return

    chat_id = update.effective_chat.id

    if REGISTRY.has_active(chat_id):
        existing = REGISTRY.get(chat_id)
        await update.message.reply_text(
            f"You already have an active snipe: {existing.date} {existing.target_time}. "
            f"Run /cancel first."
        )
        return

    args = ctx.args
    if len(args) < 2:
        await update.message.reply_text(
            "Usage: /snipe MM/DD/YYYY H:MMam/pm [COUNT] [court name]"
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
    count = 1
    if rest and rest[0].isdigit():
        count = int(rest.pop(0))
        if count < 1 or count > 20:
            await update.message.reply_text("COUNT must be between 1 and 20.")
            return

    court = " ".join(rest).strip() or None

    creds = snipe_job.get_credentials(chat_id)
    if not creds:
        await update.message.reply_text("No account connected. Run /login first.")
        return
    email, password = creds

    job = snipe_job.SnipeJob(
        chat_id=chat_id, date=date, target_time=target_time, court=court,
        email=email, password=password, count=count,
    )

    if count == 1:
        await _start_snipe(job, update, ctx)
        return

    # Multi-court: ask follow-up questions before starting.
    PENDING_SNIPE[chat_id] = job
    job.started_at = time.time()  # used to expire the pending config
    label = court or "any court"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Yes, book what's available", callback_data="cfg_partial:1"),
         InlineKeyboardButton("No, wait for all", callback_data="cfg_partial:0")],
    ])
    await update.message.reply_text(
        f"Setting up a snipe for *{count}* courts at *{target_time}* on *{date}* ({label}).\n\n"
        f"If fewer than {count} courts open up, should I still book what's available?",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )


async def _start_snipe(job: snipe_job.SnipeJob, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    snipe_job.persist(job)
    REGISTRY.add(job)
    notify = _make_notifier(ctx.application)
    job.task = asyncio.create_task(snipe_job.run_snipe(job, notify))

    label = job.court or "any court"
    extras = []
    if job.count > 1:
        extras.append(f"{job.count} courts")
        extras.append("partial OK" if job.partial else "wait for full count")
        extras.append("same number required" if job.same_number else "prefer same number")
    extras_str = f" ({'; '.join(extras)})" if extras else ""

    text = (
        f"🎯 Sniping started: {job.target_time} on {job.date} ({label}){extras_str}.\n"
        f"You'll get a ping when something opens up or it's booked."
    )
    if update.callback_query:
        await update.callback_query.edit_message_text(text)
    else:
        await update.message.reply_text(text)


# ---------- /status ----------

async def status_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        await _deny(update, ctx); return

    chat_id = update.effective_chat.id
    job = REGISTRY.get(chat_id)
    if not job:
        await update.message.reply_text("No active snipe.")
        return

    runtime = int(time.time() - job.started_at)
    mins, secs = divmod(runtime, 60)
    label = job.court or "any court"
    lines = [
        "Active snipe:",
        f"  • {job.target_time} on {job.date} ({label})",
        f"  • status: {job.status}",
        f"  • attempts: {job.attempts}",
        f"  • runtime: {mins}m {secs}s",
    ]
    if job.count > 1:
        lines.append(f"  • courts wanted: {job.count}")
        lines.append(f"  • partial OK: {'yes' if job.partial else 'no'}")
        lines.append(f"  • same number required: {'yes' if job.same_number else 'no'}")
    await update.message.reply_text("\n".join(lines))


# ---------- /cancel ----------

async def cancel_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        await _deny(update, ctx); return

    chat_id = update.effective_chat.id
    job = REGISTRY.get(chat_id)
    if not job or not job.task:
        await update.message.reply_text("No active snipe to cancel.")
        return

    job.task.cancel()
    # The CancelledError path in run_snipe handles notify + persist + drop.
    await update.message.reply_text("Cancelling...")


# ---------- /resume ----------

async def resume_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        await _deny(update, ctx); return

    chat_id = update.effective_chat.id
    pending = PENDING_RESUME.pop(chat_id, None)
    if not pending:
        await update.message.reply_text("Nothing to resume.")
        return

    if REGISTRY.has_active(chat_id):
        await update.message.reply_text("You already have an active snipe. /cancel it first.")
        return

    pending.status = "running"
    pending.attempts = 0
    pending.started_at = time.time()
    snipe_job.persist(pending)
    REGISTRY.add(pending)

    notify = _make_notifier(ctx.application)
    pending.task = asyncio.create_task(snipe_job.run_snipe(pending, notify))
    await update.message.reply_text(
        f"Resumed snipe: {pending.target_time} on {pending.date}."
    )


# ---------- /list (direct booking) ----------

async def list_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
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

    # Cache slots by short index so callback_data stays small.
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

async def bookings_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
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

    BOOKINGS_CACHE[chat_id] = {"bookings": upcoming, "ts": time.time(), "session": session}

    upcoming.sort(key=lambda b: b["start"])
    rows: list[list[InlineKeyboardButton]] = []
    summary_lines = []
    for i, b in enumerate(upcoming):
        start_dt = b["start"][:16].replace("-", "/")  # 2026/05/25 10:30
        label = f"❌ {start_dt} — {b['court']} ({b['times']})"
        rows.append([InlineKeyboardButton(label, callback_data=f"cxlpick:{i}")])
        summary_lines.append(f"• {start_dt} — {b['court']} ({b['times']})")

    await update.message.reply_text(
        f"Your {len(upcoming)} upcoming reservation(s):\n" + "\n".join(summary_lines) +
        "\n\nTap one below to cancel.",
        reply_markup=InlineKeyboardMarkup(rows),
    )


# ---------- Callback queries (book confirmation flow) ----------

async def callback_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
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

    elif data.startswith("cxlpick:"):
        idx = int(data.split(":", 1)[1])
        cache = BOOKINGS_CACHE.get(chat_id)
        if not cache or time.time() - cache["ts"] > BOOKINGS_CACHE_TTL:
            await query.edit_message_text("This list expired. Run /bookings again.")
            return
        if idx >= len(cache["bookings"]):
            await query.edit_message_text("Reservation not found. Run /bookings again.")
            return
        b = cache["bookings"][idx]
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Yes, cancel it", callback_data=f"cxlconfirm:{idx}"),
             InlineKeyboardButton("✖ Keep it", callback_data="cxlabort")],
        ])
        await query.edit_message_text(
            f"Cancel *{b['court']}* on *{b['start'][:10]}* ({b['times']})?",
            reply_markup=kb,
            parse_mode=ParseMode.MARKDOWN,
        )

    elif data.startswith("cxlconfirm:"):
        idx = int(data.split(":", 1)[1])
        cache = BOOKINGS_CACHE.get(chat_id)
        if not cache or time.time() - cache["ts"] > BOOKINGS_CACHE_TTL:
            await query.edit_message_text("This list expired. Run /bookings again.")
            return
        b = cache["bookings"][idx]
        await query.edit_message_text(f"Cancelling {b['court']} on {b['start'][:10]}...")
        ok, msg = await asyncio.to_thread(
            yourcourts.cancel_reservation, cache["session"], b["id"]
        )
        await query.edit_message_text(("✅ " if ok else "❌ ") + msg)

    elif data == "cxlabort":
        await query.edit_message_text("Kept the reservation.")

    elif data.startswith("cfg_partial:"):
        pending = PENDING_SNIPE.get(chat_id)
        if not pending or time.time() - pending.started_at > PENDING_SNIPE_TTL:
            PENDING_SNIPE.pop(chat_id, None)
            await query.edit_message_text("This setup expired. Run /snipe again.")
            return
        pending.partial = data.endswith(":1")
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Yes, same number only", callback_data="cfg_same:1"),
             InlineKeyboardButton("No, just keep them close", callback_data="cfg_same:0")],
        ])
        await query.edit_message_text(
            f"Partial booking: *{'yes' if pending.partial else 'no'}*.\n\n"
            f"Should all {pending.count} courts share the same court number "
            f"(e.g. all PB 4)? If no, I'll just pick courts that sort close together.",
            reply_markup=kb,
            parse_mode=ParseMode.MARKDOWN,
        )

    elif data.startswith("cfg_same:"):
        pending = PENDING_SNIPE.pop(chat_id, None)
        if not pending or time.time() - pending.started_at > PENDING_SNIPE_TTL:
            await query.edit_message_text("This setup expired. Run /snipe again.")
            return
        if REGISTRY.has_active(chat_id):
            await query.edit_message_text("You already have an active snipe. /cancel it first.")
            return
        pending.same_number = data.endswith(":1")
        pending.started_at = time.time()
        await _start_snipe(pending, update, ctx)

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
    """Called once the application is set up. Load allowlist + surface interrupted snipes."""
    for cid in snipe_job.load_allowed_users():
        ALLOWED_CHAT_IDS.add(cid)
    if ADMIN_CHAT_ID is not None:
        ALLOWED_CHAT_IDS.add(ADMIN_CHAT_ID)
    print(f"Allowed chat_ids ({len(ALLOWED_CHAT_IDS)}):", ALLOWED_CHAT_IDS)

    interrupted = snipe_job.mark_all_interrupted()
    notify = _make_notifier(app)
    for job in interrupted:
        if job.chat_id not in ALLOWED_CHAT_IDS:
            snipe_job.remove(job.chat_id)
            continue
        PENDING_RESUME[job.chat_id] = job
        try:
            await notify(
                job,
                f"⚠️ Snipe was interrupted: {job.target_time} on {job.date}. "
                f"Send /resume to continue or /cancel to drop it."
            )
        except Exception as e:
            print(f"could not ping {job.chat_id} about interrupted job: {e}")


def main() -> None:
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

    print("Bot starting. Allowed chat_ids:", ALLOWED_CHAT_IDS)
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
