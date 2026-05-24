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
ALLOWED_CHAT_IDS = {int(x) for x in _raw_ids.split(",") if x.strip()}

REGISTRY = snipe_job.JobRegistry()

# Per-chat cache of the slot list shown in /list, so callback buttons can
# reference slots by short index instead of stuffing all fields into callback_data
# (which has a 64-byte limit).
LIST_CACHE: dict[int, dict] = {}  # chat_id -> {"date": str, "slots": list[dict], "ts": float}
LIST_CACHE_TTL = 600  # seconds

PENDING_RESUME: dict[int, snipe_job.SnipeJob] = {}  # chat_id -> interrupted job awaiting /resume


def _allowed(update: Update) -> bool:
    chat = update.effective_chat
    if chat is None:
        return False
    return chat.id in ALLOWED_CHAT_IDS


async def _deny(update: Update) -> None:
    if update.message:
        await update.message.reply_text("Not authorized.")
    elif update.callback_query:
        await update.callback_query.answer("Not authorized.", show_alert=True)


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
    chat_id = update.effective_chat.id
    if not _allowed(update):
        await update.message.reply_text(
            f"Not authorized. Your chat_id is `{chat_id}`. "
            f"Add it to TELEGRAM_ALLOWED_CHAT_IDS in .env to enable.",
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
        "  → shows all openings as tap-to-book buttons\n\n"
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
        await _deny(update); return
    await update.message.reply_text(
        "*Snipe* (polls until a slot opens, then books):\n"
        "`/snipe 05/25/2026 10:00AM`\n"
        "`/snipe 05/25/2026 7:00AM PB 4B`\n\n"
        "*Direct booking* (book from current availability):\n"
        "`/list 05/25/2026`\n\n"
        "One active snipe per user. Use /cancel to stop it before starting another.",
        parse_mode=ParseMode.MARKDOWN,
    )


# ---------- /snipe ----------

async def snipe_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        await _deny(update); return

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
            "Usage: /snipe MM/DD/YYYY H:MMam/pm [court name]"
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

    court = " ".join(args[2:]).strip() or None

    job = snipe_job.SnipeJob(
        chat_id=chat_id, date=date, target_time=target_time, court=court
    )
    snipe_job.persist(job)
    REGISTRY.add(job)

    notify = _make_notifier(ctx.application)
    job.task = asyncio.create_task(snipe_job.run_snipe(job, notify))

    label = court or "any court"
    await update.message.reply_text(
        f"🎯 Sniping started: {target_time} on {date} ({label}).\n"
        f"You'll get a ping when something opens up or it's booked."
    )


# ---------- /status ----------

async def status_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        await _deny(update); return

    chat_id = update.effective_chat.id
    job = REGISTRY.get(chat_id)
    if not job:
        await update.message.reply_text("No active snipe.")
        return

    runtime = int(time.time() - job.started_at)
    mins, secs = divmod(runtime, 60)
    label = job.court or "any court"
    await update.message.reply_text(
        f"Active snipe:\n"
        f"  • {job.target_time} on {job.date} ({label})\n"
        f"  • status: {job.status}\n"
        f"  • attempts: {job.attempts}\n"
        f"  • runtime: {mins}m {secs}s"
    )


# ---------- /cancel ----------

async def cancel_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        await _deny(update); return

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
        await _deny(update); return

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
        await _deny(update); return

    args = ctx.args
    if not args:
        await update.message.reply_text("Usage: /list MM/DD/YYYY")
        return
    date = _parse_date(args[0])
    if not date:
        await update.message.reply_text("Bad date format. Use MM/DD/YYYY.")
        return

    await update.message.reply_text(f"Fetching openings for {date}...")

    session = yourcourts.make_session()
    ok = await asyncio.to_thread(yourcourts.login, session)
    if not ok:
        await update.message.reply_text("Login to yourcourts.com failed.")
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


# ---------- Callback queries (book confirmation flow) ----------

async def callback_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        await _deny(update); return

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


# ---------- Startup ----------

async def post_init(app: Application) -> None:
    """Called once the application is set up. Surface interrupted snipes."""
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
    if not ALLOWED_CHAT_IDS:
        raise SystemExit("TELEGRAM_ALLOWED_CHAT_IDS empty — add your chat_id to .env")

    app = (
        ApplicationBuilder()
        .token(TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("snipe", snipe_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))
    app.add_handler(CommandHandler("resume", resume_cmd))
    app.add_handler(CommandHandler("list", list_cmd))
    app.add_handler(CommandHandler("book", list_cmd))
    app.add_handler(CallbackQueryHandler(callback_handler))

    print("Bot starting. Allowed chat_ids:", ALLOWED_CHAT_IDS)
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
