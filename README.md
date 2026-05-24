# pickle-bot

Telegram bot for sniping and booking pickleball courts on [yourcourts.com](https://www.yourcourts.com).

Two modes:
- **Snipe** — poll a date/time on a loop and auto-book it the moment it opens.
- **Direct book** — show what's open right now and tap a button to book.

## Setup

1. Clone the repo and install deps:
   ```
   pip install -r requirements.txt
   ```

2. Create a Telegram bot via [@BotFather](https://t.me/BotFather) and copy the token.

3. Find your numeric chat ID by messaging [@userinfobot](https://t.me/userinfobot).

4. Copy `.env.example` to `.env` and fill it in:
   ```
   YC_EMAIL=your-yourcourts-email
   YC_PASSWORD=your-yourcourts-password
   TELEGRAM_BOT_TOKEN=...
   TELEGRAM_ALLOWED_CHAT_IDS=123456789
   ```
   Comma-separate `TELEGRAM_ALLOWED_CHAT_IDS` to allow multiple users.

5. Start the bot:
   ```
   python3 bot.py
   ```

6. In Telegram, message your bot `/start`.

## Commands

| Command | Purpose |
|---|---|
| `/snipe MM/DD/YYYY H:MMam [court]` | Start a polling job. Pings only on found / booked / error. |
| `/list MM/DD/YYYY` | Show current openings as inline buttons. Tap → confirm → book. |
| `/status` | Show your active snipe. |
| `/cancel` | Stop your active snipe. |
| `/resume` | Resume a snipe that was interrupted by a restart. |
| `/help` | Command reference. |

Rules:
- One active snipe per user at a time.
- On bot restart, interrupted snipes get a DM with `/resume` or `/cancel` to clear them.
- Direct booking uses a two-step confirm to avoid fat-finger taps on mobile.

## Files

| File | Role |
|---|---|
| `yourcourts.py` | Shared HTTP client (login, find_slots, book_slot). |
| `snipe_court.py` | Standalone CLI: prompts for date/time and polls. |
| `book_court.py` | Standalone CLI: list current openings and book one. |
| `snipe_job.py` | Async snipe job + SQLite-mirrored registry. |
| `bot.py` | Telegram bot entry point. |
| `bench_booking.py` | Latency benchmark of the full booking flow. |

## Hosting

The bot uses Telegram long-polling, so no public IP or webhook is needed.
- **Laptop** — `python3 bot.py` in a terminal. Free, but bot dies when laptop sleeps.
- **Oracle Cloud Always Free** — 1 GB ARM VM, free forever.
- **Hetzner / DigitalOcean / Vultr** — ~$4.50/mo for a small VPS; easiest path with `systemd`.

Avoid free tiers that sleep idle apps (Render, Railway free, Fly autoscale) — a sleeping bot misses snipes.
