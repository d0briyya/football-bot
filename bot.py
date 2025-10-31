""" Refactored and hardened Telegram bot (aiogram 2.x) — improved by ChatGPT (senior-style)
Key improvements made: ...
Note: keep environment variables: TG_BOT_TOKEN, TG_CHAT_ID, TG_ADMIN_ID, PORT
"""
from __future__ import annotations

import os
import sys
import json
import time
import shutil
import asyncio
import logging
import signal
import atexit
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, Any

try:
    import psutil
except Exception:
    psutil = None

from logging.handlers import RotatingFileHandler
from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode
from aiogram.utils import exceptions
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone
from dotenv import load_dotenv
from aiohttp import web
import aiohttp
import aiofiles
import html
import random

WEATHER_MESSAGES = {
    'clear': [
        "🌞 Ну что, классика — солнце, мяч, поле! Плохая погода? Не, не слышали.",
        "😎 Солнышко светит — футбол сам по себе праздник. Плохой погоды для нас не придумали!",
        "☀️ На улице жара, а на поле будет ещё горячее. Берём воду, солнцезащитку и отличный настрой!",
        "🌻 Для футбола такой день — просто мечта. Главное не забыть улыбку!",
        "🌅 Погода топчик. Уже ощущаешь запах мяча в воздухе?",
    ],
    'cloud': [
        "☁️ Облака? Так даже эпичнее будут голы! В пасмурную погоду выигрывает желание.",
        "🌦 Немного пасмурно, но футболисты не сахар — не растаем точно!",
        "⛅️ Для футбола не важно сколько на небе солнца, важно кто стоит у ворот!",
        "🌥 Пасмурно, зато мяч будет хорошо видно — погнали!",
        "☁️ Не бывает плохой погоды для футбола, бывает неправильная экипировка!",
    ],
    'rain': [
        "☔️ Дождь — это бесплатный душ от вселенной! Поле мокрое, настрой — боевой!",
        "🌧 Мокрый мяч — крутые подкаты! Погода только добавляет драйва.",
        "🌦 На улице дождик? Считай повезло: мяч летит быстрее, эмоций — больше!",
        "☔️ Если бы футболисты боялись воды, мы бы смотрели другие виды спорта!",
        "💧 Дождь закаляет характер чемпионов — и очищает бутсы.",
    ],
    'storm': [
        "⛈ Даже мы признаём: молния — не повод геройствовать. Если гроза — футбол откладываем!",
        "🚨 Гроза на поле — момент, когда даже наш энтузиазм берёт паузу. Не рискуем!",
        "⚡️ Для футбола нет плохой погоды... кроме той, что с молнией. Будьте осторожны!",
        "⛈ В такую погоду даже VAR уходит в офлайн. Берегите себя!",
        "⚠️ Нам всем хочется поиграть, но электричество — только для эмоций, не для поля!",
    ],
    'wind': [
        "💨 Ветер? Это просто дополнительный игрок на поле! Ставим на точные подачи.",
        "🌪 Мяч иногда будет подыгрывать — тренируем навесы!",
        "🍃 Острота паса сегодня максимальная, ветер помогает атаке!",
        "💨 Когда ветер дует в спину — пора делать дальние удары!",
        "🥅 Главное, чтобы ворота не улетели — всё остальное не проблема!",
    ],
    'snow': [
        "❄️ Снег украсит матч, а пару голов от разогрева только плюс!",
        "☃️ Сугробы? Значит пора сыграть “ледяной финал”!",
        "🌨 Даже снег — не повод отменять футбол. Замерзают только болельщики!",
        "❄️ Белое поле — больше перчаток, больше эмоций, тот же футбол!",
        "🧤 Мороз и солнце — по футбольному темпераменту сочетаются идеально!",
    ],
    'extreme': [
        "🚨 Ливень и ветер сегодня сильнее всех на поле. Даже мы советуем остаться дома!",
        "🌀 Такой шторм не выдержит даже судья — футбол отменяется!",
        "🌊 “Для футбола нет плохой погоды” иногда требует здравого смысла. Сделаем паузу!",
        "🚩 Сегодня на поле может унести даже капитанов — безопаснее устроить тактический разбор дома!",
        "🛑 За окном апокалипсис, а мы слишком любим своих игроков, чтобы выпускать их в такое!",
    ],
}

def pick_weather_message(description: str) -> str:
    desc = description.lower()
    if any(w in desc for w in ["гроз", "бур", "шторм", "гроза", "ураган", "молни"]):
        cat = 'storm' if "гроза" in desc or "молни" in desc else 'extreme'
    elif any(w in desc for w in ["ливень", "ураган", "шторм", "апокалипсис", "ураган"]):
        cat = 'extreme'
    elif any(w in desc for w in ["дыру", "ветер", "порывист", "ветр"]):
        cat = 'wind'
    elif any(w in desc for w in ["снеж", "метел", "снег"]):
        cat = 'snow'
    elif any(w in desc for w in ["дожд", "морось", "ливень"]):
        cat = 'rain'
    elif any(w in desc for w in ["пасмурн", "облач", "туман"]):
        cat = 'cloud'
    elif any(w in desc for w in ["ясно", "солнеч", "ясн."]):
        cat = 'clear'
    else:
        cat = 'clear'
    return random.choice(WEATHER_MESSAGES[cat])

# -------------------- Configuration --------------------
load_dotenv()

# Defaults provided per user request; prefer env vars in production
TOKEN = os.getenv("TG_BOT_TOKEN", "8196071953:AAElW8XHm_y2NweYb3EOSlxsiUC3s9ijh48")
if not TOKEN:
    print("ERROR: TG_BOT_TOKEN is not set. Please export it and restart.")
    sys.exit(1)

try:
    CHAT_ID = int(os.getenv("TG_CHAT_ID", "-1002841862533"))
except Exception:
    print("ERROR: TG_CHAT_ID must be an integer (chat id).")
    sys.exit(1)

try:
    ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "914344682"))
except Exception:
    print("ERROR: TG_ADMIN_ID must be an integer (admin id).")
    sys.exit(1)

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "12f9f68ba8b0f873901522977cf20b5a")

DATA_FILE = os.getenv("DATA_FILE", "bot_data.json")
PORT = int(os.getenv("PORT", 8080))
LOCK_FILE = os.getenv("LOCK_FILE", "bot.lock")
LOG_FILE = os.getenv("LOG_FILE", "bot.log")

# -------------------- Logging --------------------
class StdoutFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < logging.ERROR

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.addFilter(StdoutFilter())

stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.ERROR)

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=5, encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[file_handler, stdout_handler, stderr_handler],
)

log = logging.getLogger("bot")

# -------------------- Single instance lock --------------------

def _read_pid_from_lock(path: str) -> Optional[int]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:
        return None

def ensure_single_instance(lock_path: str = LOCK_FILE) -> None:
    """Ensure only one instance runs. If stale lock exists, remove it.
    On failure, raise RuntimeError to prevent double startup.
    """
    if os.getenv("IGNORE_LOCK") == "1":
        log.warning("Ignoring lock file due to IGNORE_LOCK=1")
        return
    if os.path.exists(lock_path):
        pid = _read_pid_from_lock(lock_path)
        still_running = False
        if pid:
            if psutil:
                still_running = psutil.pid_exists(pid)
            else:
                try:
                    os.kill(pid, 0)
                    still_running = True
                except Exception:
                    still_running = False

        if still_running:
            log.error("Lock file exists and process %s is running. Refusing to start.", pid)
            raise RuntimeError("Another instance is already running")
        else:
            log.warning("Stale lock file found (pid=%s). Removing." , pid)
            try:
                os.remove(lock_path)
            except Exception as e:
                log.exception("Failed to remove stale lock: %s", e)

    with open(lock_path, "w", encoding="utf-8") as f:
        f.write(str(os.getpid()))

    def _cleanup():
        try:
            if os.path.exists(lock_path):
                os.remove(lock_path)
                log.info("Lock file removed on exit.")
        except Exception:
            log.exception("Failed to remove lock file at exit.")

    atexit.register(_cleanup)

ensure_single_instance()

# -------------------- Bot, scheduler, timezone --------------------
bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot)

KALININGRAD_TZ = timezone("Europe/Kaliningrad")
# Планировщик создадим внутри main(), чтобы он корректно работал в том же event loop, что и aiogram
scheduler: Optional[AsyncIOScheduler] = None


START_TIME = datetime.now()

# runtime state
active_polls: Dict[str, Dict[str, Any]] = {}
stats: Dict[str, int] = {}
disabled_days: set = set()
game_stats: Dict[str, Dict[str, int]] = {}
# penalty enforcement state
pending_penalties: Dict[str, Dict[str, Any]] = {}
banned_users: Dict[str, float] = {}

# -------------------- H2H Penalty Match State --------------------
active_match: Optional[Dict[str, Any]] = None
# per-user mini-game session visibility/flow
mini_game_sessions: Dict[str, Dict[str, Any]] = {}

def _mention(user_id: int, name: str) -> str:
    return f'<a href="tg://user?id={user_id}">{html.escape(name)}</a>'

def _now_ts() -> float:
    return time.time()

# polls config (modifiable)
polls_config = [
    {"day": "tue", "time_poll": "09:00", "time_game": "20:00",
     "question": "Сегодня собираемся на песчанке в 20:00?",
     "options": ["Да ✅", "Нет ❌", "Под вопросом ❔ (отвечу позже)"]},
    {"day": "thu", "time_poll": "09:00", "time_game": "20:00",
     "question": "Сегодня собираемся на песчанке в 20:00?",
     "options": ["Да ✅", "Нет ❌", "Под вопросом ❔ (отвечу позже)"]},
    {"day": "fri", "time_poll": "21:00", "time_game": "12:00",
     "question": "Завтра в 12:00 собираемся на песчанке?",
     "options": ["Да ✅", "Нет ❌"]}
]

WEEKDAY_MAP = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
TELEGRAM_MESSAGE_LIMIT = 4096

# -------------------- Helpers --------------------
def now_tz() -> datetime:
    return datetime.now(KALININGRAD_TZ)

def iso_now() -> str:
    return now_tz().isoformat()

def is_admin(user_id: int) -> bool:
    try:
        return int(user_id) == int(ADMIN_ID)
    except Exception:
        return False

def find_last_active_poll() -> Optional[Tuple[str, Dict[str, Any]]]:
    if not active_polls:
        return None
    items = sorted(active_polls.items(), key=lambda it: it[1].get("created_at", ""), reverse=True)
    for pid, data in items:
        if data.get("active"):
            return pid, data
    return None

def format_poll_votes(data: Dict[str, Any]) -> str:
    votes = data.get("votes", {})
    if not votes:
        return "— Никто ещё не голосовал."
    return "\n".join(f"{v.get('name')} — {v.get('answer')}" for v in votes.values())

def normalize_day_key(day_str: str) -> Optional[str]:
    if not day_str:
        return None
    s = day_str.strip().lower()
    # Accept English short and Russian forms
    ru_map = {
        "пн": "mon", "пон": "mon", "понедельник": "mon",
        "вт": "tue", "втор": "tue", "вторник": "tue",
        "ср": "wed", "среда": "wed",
        "чт": "thu", "чет": "thu", "четверг": "thu",
        "пт": "fri", "пят": "fri", "пятница": "fri",
        "сб": "sat", "суб": "sat", "суббота": "sat",
        "вс": "sun", "вос": "sun", "воскресенье": "sun",
    }
    if s in WEEKDAY_MAP:
        return s
    if s in ru_map:
        return ru_map[s]
    return None

# -------------------- Persistence --------------------
_next_save_allowed = 0
async def save_data() -> None:
    global _next_save_allowed
    if time.time() < _next_save_allowed:
        return
    _next_save_allowed = time.time() + 10
    try:
        payload = {"active_polls": active_polls, "stats": stats, "disabled_days": sorted(list(disabled_days)), "game_stats": game_stats, "pending_penalties": pending_penalties, "banned_users": banned_users}
        tmp = DATA_FILE + ".tmp"
        async with aiofiles.open(tmp, "w", encoding="utf-8") as f:
            await f.write(json.dumps(payload, ensure_ascii=False, indent=2))
        os.replace(tmp, DATA_FILE)
        log.debug("Data saved to %s", DATA_FILE)
    except Exception:
        log.exception("Failed to save data")

async def load_data() -> None:
    global active_polls, stats
    if os.path.exists(DATA_FILE):
        try:
            async with aiofiles.open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.loads(await f.read())
            active_polls = data.get("active_polls", {})
            stats = data.get("stats", {})
            gs = data.get("game_stats", {})
            if isinstance(gs, dict):
                game_stats.clear()
                for k, v in gs.items():
                    if isinstance(v, dict):
                        game_stats[str(k)] = {
                            "goals": int(v.get("goals", 0)),
                            "shots": int(v.get("shots", 0)),
                            "name": v.get("name", "") if isinstance(v.get("name", ""), str) else "",
                        }
            pp = data.get("pending_penalties", {})
            if isinstance(pp, dict):
                pending_penalties.clear()
                for k, v in pp.items():
                    if isinstance(v, dict):
                        pending_penalties[str(k)] = v
            bu = data.get("banned_users", {})
            if isinstance(bu, dict):
                banned_users.clear()
                for k, v in bu.items():
                    try:
                        banned_users[str(k)] = float(v)
                    except Exception:
                        continue
            dd = data.get("disabled_days", [])
            if isinstance(dd, list):
                for d in dd:
                    if isinstance(d, str):
                        disabled_days.add(d)
            log.info("Loaded data: active_polls=%s, stats=%s, disabled_days=%s, game_players=%s", len(active_polls), len(stats), sorted(list(disabled_days)), len(game_stats))
        except Exception:
            log.exception("Failed to load data — starting with empty state")
    else:
        log.info("No data file found — starting fresh")

def make_backup() -> None:
    try:
        if os.path.exists(DATA_FILE):
            bfile = f"bot_data_backup_{datetime.now():%Y%m%d}.json"
            shutil.copyfile(DATA_FILE, bfile)
            log.info("Backup created: %s", bfile)
    except Exception:
        log.exception("Failed to create backup")

# -------------------- Telegram wrapper --------------------
async def safe_telegram_call(func, *args, retries: int = 3, **kwargs):
    """A resilient wrapper for Telegram API calls.
    Handles FloodWait/RetryAfter specially and retries on transient errors.
    Returns the call result or None on failure.
    """
    for attempt in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)
        except exceptions.RetryAfter as e:
            wait = getattr(e, 'timeout', None) or getattr(e, 'retry_after', None) or 1
            log.warning("RetryAfter (flood) — sleeping %s seconds", wait)
            await asyncio.sleep(wait + 1)
        except exceptions.TelegramAPIError as e:
            log.warning("TelegramAPIError (attempt %s): %s", attempt, e)
            if attempt == retries:
                return None
            await asyncio.sleep(1 + attempt)
        except Exception:
            log.exception("Unexpected exception during Telegram call (attempt %s)", attempt)
            if attempt == retries:
                return None
            await asyncio.sleep(1 + attempt)

# -------------------- New helpers: compute poll close datetime & scheduling reminders --------------------
def compute_poll_close_dt(poll: Dict[str, Any], start_dt: datetime) -> datetime:
    """
    Compute poll closing datetime using poll['day'] and poll['time_game'].
    If poll['day']=='manual' or computation fails, fallback to start_dt + 24h.
    """
    try:
        day = poll.get("day")
        tg_hour, tg_minute = map(int, poll.get("time_game", "23:59").split(":"))
        if day not in WEEKDAY_MAP:
            # manual poll or unknown: default 24h lifetime
            return start_dt + timedelta(hours=24)

        target = WEEKDAY_MAP[day]
        days_ahead = (target - start_dt.weekday()) % 7
        base_date = start_dt.date() + timedelta(days=days_ahead)
        # For Tue/Thu close at 1 hour before game time (e.g., 19:00 if game at 20:00)
        close_hour = tg_hour - 1 if day in ("tue", "thu") else tg_hour
        close_minute = tg_minute
        if close_hour < 0:
            close_hour = 0
        base = datetime(base_date.year, base_date.month, base_date.day, close_hour, close_minute)
        base_local = KALININGRAD_TZ.localize(base) if base.tzinfo is None else base.astimezone(KALININGRAD_TZ)
        # Ensure close is after start; if not, assume next week
        if base_local <= start_dt:
            base_local = base_local + timedelta(days=7)
        return base_local
    except Exception:
        log.exception("Failed to compute poll close dt for poll: %s", poll)
        return start_dt + timedelta(hours=24)

async def send_reminder_if_needed(poll_id: str) -> None:
    """Send reminder to CHAT_ID if yes_count < 10 for the poll."""
    try:
        data = active_polls.get(poll_id)
        if not data or not data.get("active"):
            return
        votes = data.get("votes", {})
        yes_users = [v for v in votes.values() if v.get("answer", "").startswith("Да")]
        if len(yes_users) < 10:
            # send reminder
            question = data.get("poll", {}).get("question", "Пожалуйста, проголосуйте!")
            text = f"🔔 Напоминание: <b>{question}</b>\nПожалуйста, проголосуйте — нам нужно как минимум 10 'Да' для подтверждения."
            await safe_telegram_call(bot.send_message, CHAT_ID, text, parse_mode=ParseMode.HTML)
            log.info("Reminder sent for poll %s (yes=%s)", poll_id, len(yes_users))
    except Exception:
        log.exception("Error in send_reminder_if_needed for poll %s", poll_id)

async def tag_questionable_users(poll_id: str) -> None:
    """
    Tag users who voted 'Под вопросом' (or containing 'Под вопросом' substring).
    Use saved user_id to create mention via tg://user?id=...
    """
    try:
        data = active_polls.get(poll_id)
        if not data or not data.get("active"):
            return
        votes = data.get("votes", {})
        # find close_dt stored earlier (ISO)
        close_iso = data.get("close_dt")
        close_dt = None
        if close_iso:
            try:
                dt_obj = datetime.fromisoformat(close_iso.replace('Z', '+00:00'))
                if dt_obj.tzinfo is None:
                    close_dt = dt_obj.replace(tzinfo=KALININGRAD_TZ)
                else:
                    close_dt = dt_obj.astimezone(KALININGRAD_TZ)
            except Exception:
                close_dt = None

        now = now_tz()
        mins_left = int((close_dt - now).total_seconds() // 60) if close_dt else None

        for v in votes.values():
            answer = v.get("answer", "")
            if "под" in answer.lower() or "под вопрос" in answer.lower() or "?" in answer:
                user_id = v.get("user_id")
                name = v.get("name", "Участник")
                safe_name = html.escape(name)
                if not user_id:
                    # we can't mention without user_id; fallback to using plain name
                    text = f"{safe_name}, ⚠️ вы отметили 'Под вопросом'. Осталось {mins_left} минут до закрытия опроса. Пожалуйста, подтвердите участие."
                    await safe_telegram_call(bot.send_message, CHAT_ID, text, parse_mode=ParseMode.HTML)
                    await asyncio.sleep(0.3)
                    log.debug("Tagged by name (no user_id) for poll %s: %s", poll_id, name)
                else:
                    mention = f'<a href="tg://user?id={user_id}">{safe_name}</a>'
                    text = f"{mention}, ⚠️ вы отметили 'Под вопросом'. Осталось {mins_left} минут до закрытия опроса. Пожалуйста, подтвердите участие."
                    await safe_telegram_call(bot.send_message, CHAT_ID, text, parse_mode=ParseMode.HTML)
                    await asyncio.sleep(0.3)
                    log.debug("Mentioned user %s for poll %s", user_id, poll_id)
    except Exception:
        log.exception("Error in tag_questionable_users for poll %s", poll_id)

def schedule_poll_reminders(poll_id: str) -> None:
    """
    Schedule the two kinds of jobs for the given poll:
      - every 3 hours reminder if yes<10 (from start until close)
      - every 30 minutes tagging 'Под вопросом' users from close-2h until close
    Store close_dt in active_polls[poll_id]['close_dt'] as ISO.
    """
    try:
        data = active_polls.get(poll_id)
        if not data:
            return
        poll = data.get("poll", {})
        # Only for Tue and Thu per request
        if poll.get("day") not in ("tue", "thu"):
            return

        global scheduler
        if scheduler is None:
            log.error("Scheduler not initialized!")
            return
        loop = asyncio.get_event_loop()
        start_dt = now_tz()
        close_dt = compute_poll_close_dt(poll, start_dt)
        # safety: ensure at least 2 hours duration, otherwise fallback to start+24h
        if close_dt <= start_dt + timedelta(minutes=5):
            close_dt = start_dt + timedelta(hours=24)

        # store close timestamp for later use by tag job
        try:
            data["close_dt"] = close_dt.isoformat()
        except Exception:
            data["close_dt"] = None

        # Job ids
        reminder_job_id = f"reminder_{poll_id}"
        tag_job_id = f"tagq_{poll_id}"
        close_job_id = f"close_{poll_id}"

        # schedule reminder every 3 hours from start to close
        try:
            # remove previous if exists
            try:
                scheduler.remove_job(reminder_job_id)
            except Exception:
                pass
            scheduler.add_job(
                lambda pid=poll_id: asyncio.run_coroutine_threadsafe(send_reminder_if_needed(pid), loop),
                trigger="interval",
                hours=3,
                start_date=start_dt,
                end_date=close_dt,
                id=reminder_job_id,
            )
            log.info("Scheduled 3h reminders for poll %s from %s to %s", poll_id, start_dt, close_dt)
        except Exception:
            log.exception("Failed to schedule 3h reminders for poll %s", poll_id)

        # schedule tagging: for Tue/Thu from 17:40 to 19:00 every 20 minutes; otherwise 30 minutes last 2h
        try:
            poll_day = poll.get("day")
            if poll_day in ("tue", "thu"):
                # compute 17:40 same day (Kaliningrad)
                tag_start_local = close_dt.replace(hour=17, minute=40, second=0, microsecond=0)
                # ensure not before start
                tag_start = max(start_dt, tag_start_local)
                interval_minutes = 20
            else:
                tag_start = max(start_dt, close_dt - timedelta(hours=2))
                interval_minutes = 30
            try:
                scheduler.remove_job(tag_job_id)
            except Exception:
                pass
            scheduler.add_job(
                lambda pid=poll_id: asyncio.run_coroutine_threadsafe(tag_questionable_users(pid), loop),
                trigger="interval",
                minutes=interval_minutes,
                start_date=tag_start,
                end_date=close_dt,
                id=tag_job_id,
            )
            log.info("Scheduled tagging (%sm) for poll %s from %s to %s", interval_minutes, poll_id, tag_start, close_dt)
        except Exception:
            log.exception("Failed to schedule tagging for poll %s", poll_id)
        # Автоматическое закрытие опроса — добавить после всех scheduler.add_job
        try:
            try:
                scheduler.remove_job(close_job_id)
            except Exception:
                pass
            scheduler.add_job(
                lambda pid=poll_id: asyncio.run_coroutine_threadsafe(send_summary(pid), loop),
                trigger="date",
                run_date=close_dt,
                id=close_job_id,
            )
            log.info("Scheduled auto-close for poll %s at %s", poll_id, close_dt)
        except Exception:
            log.exception("Failed to schedule auto-close for poll %s", poll_id)
        asyncio.run_coroutine_threadsafe(save_data(), loop)
    except Exception:
        log.exception("Error in schedule_poll_reminders for poll %s", poll_id)

# -------------------- Poll lifecycle --------------------
# -------------------- Weather forecast --------------------
async def get_weather_forecast(target_dt: datetime) -> Optional[str]:
    """Возвращает краткий прогноз погоды с OpenWeather (3-hour forecast)."""
    try:
        api_key = OPENWEATHER_API_KEY
        if not api_key:
            log.warning("OPENWEATHER_API_KEY не установлен — прогноз погоды пропущен")
            return None
        city = "Zelenogradsk, Kaliningradskaya oblast, RU"
        url = f"https://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}&units=metric&lang=ru"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    log.warning("OpenWeather API returned %s", resp.status)
                    return None
                data = await resp.json()
        if not data.get("list"):
            return None
        # подбираем ближайший прогноз по времени
        target_ts = int(target_dt.timestamp())
        best = min(data["list"], key=lambda e: abs(e["dt"] - target_ts))
        temp = best["main"]["temp"]
        feels = best["main"].get("feels_like", temp)
        desc = best["weather"][0]["description"].capitalize()
        wind = best["wind"]["speed"]
        return f"{desc}, 🌡 {temp:+.0f}°C (ощущается {feels:+.0f}°C), 💨 {wind} м/с"
    except Exception:
        log.exception("Failed to fetch weather")
        return None
async def start_poll(poll: Dict[str, Any], from_admin: bool = False) -> None:
    """Create and register a poll. Ensures options count fits Telegram limits."""
    try:
        options = poll.get("options", [])[:10]
        if not options:
            log.warning("Poll has no options, skipping: %s", poll)
            return
        day = poll.get("day", "manual")
        now = now_tz()
        if day != "manual":
            target_weekday = WEEKDAY_MAP.get(day, None)
            hour, minute = map(int, poll.get("time_game", now.strftime('%H:%M')).split(':'))
            today_weekday = now.weekday()
            days_until_target = (target_weekday - today_weekday) % 7
            target_date = now.date() + timedelta(days=days_until_target)
            # локализация через pytz
            game_dt_naive = datetime(target_date.year, target_date.month, target_date.day, hour, minute)
            game_dt = KALININGRAD_TZ.localize(game_dt_naive)
        else:
            game_dt = now
        weather = await get_weather_forecast(game_dt)
        msg = await safe_telegram_call(
            bot.send_poll,
            chat_id=CHAT_ID,
            question=poll["question"],
            options=options,
            is_anonymous=False,
            allows_multiple_answers=False,
        )
        if not msg:
            log.error("send_poll returned None — poll not created: %s", poll.get("question"))
            return
        try:
            await safe_telegram_call(bot.pin_chat_message, CHAT_ID, msg.message_id, disable_notification=True)
            pinned_message_id = msg.message_id
            log.info("Pinned poll message %s", msg.message_id)
        except Exception as e:
            pinned_message_id = None
            log.exception("Failed to pin poll message: %s", e)
        poll_id = msg.poll.id
        active_polls[poll_id] = {
            "message_id": msg.message_id,
            "pinned_message_id": pinned_message_id,
            "poll": poll,
            "votes": {},
            "active": True,
            "created_at": iso_now(),
        }
        await save_data()
        if weather:
            weather_msg = pick_weather_message(weather)
            await safe_telegram_call(bot.send_message, CHAT_ID, f"<b>Погода на время игры:</b> {weather}\n\n{weather_msg}", parse_mode=ParseMode.HTML)
        await safe_telegram_call(bot.send_message, CHAT_ID, "📢 <b>Новый опрос!</b>\nПроголосуйте 👇", parse_mode=ParseMode.HTML)
        if from_admin:
            await safe_telegram_call(bot.send_message, ADMIN_ID, f"✅ Опрос вручную: {poll['question']}")
        log.info("Poll created: %s", poll.get("question"))
        try:
            schedule_poll_reminders(poll_id)
        except Exception:
            log.exception("Failed to setup reminders for poll %s", poll_id)
    except Exception:
        log.exception("Failed to start poll")

async def _chunk_and_send(chat_id: int, text: str, parse_mode=None) -> None:
    """Send text in chunks respecting TELEGRAM_MESSAGE_LIMIT."""
    if not text:
        return
    chunks = [text[i:i+TELEGRAM_MESSAGE_LIMIT] for i in range(0, len(text), TELEGRAM_MESSAGE_LIMIT)]
    for chunk in chunks:
        await safe_telegram_call(bot.send_message, chat_id, chunk, parse_mode=parse_mode)

async def send_summary(poll_id: str) -> None:
    data = active_polls.get(poll_id)
    if not data:
        return
    try:
        data["active"] = False
        votes = data.get("votes", {})
        yes_users = [html.escape(v["name"]) for v in votes.values() if v["answer"].startswith("Да")]
        no_users = [html.escape(v["name"]) for v in votes.values() if v["answer"].startswith("Нет")]
        if data["poll"].get("day") == "fri":
            status = (
                "📊 Итог субботнего опроса:\n\n"
                f"👥 Всего проголосовало: {len(votes)} человек(а).\n"
                "Решайте сами идти или нет — этот опрос просто для удобства, в субботу многие приходят без опроса ⚽"
            )
        else:
            total_yes = len(yes_users)
            status = (
                "⚠️ Сегодня не собираемся — меньше 10 участников."
                if total_yes < 10 else
                "✅ Сегодня собираемся на песчанке! ⚽"
            )
        day = data["poll"].get("day", "manual")
        now = now_tz()
        if day != "manual":
            target_weekday = WEEKDAY_MAP.get(day, None)
            hour, minute = map(int, data["poll"].get("time_game", now.strftime('%H:%M')).split(':'))
            today_weekday = now.weekday()
            days_until_target = (target_weekday - today_weekday) % 7
            target_date = now.date() + timedelta(days=days_until_target)
            game_dt_naive = datetime(target_date.year, target_date.month, target_date.day, hour, minute)
            game_dt = KALININGRAD_TZ.localize(game_dt_naive)
        else:
            game_dt = now
        weather = await get_weather_forecast(game_dt)
        weather_str = f"\n\n<b>Погода на момент игры:</b> {weather}" if weather else ""
        # ДОБАВЛЯЕМ блочок капитанов — если Вторник/Четверг и Да >=10
        captains_text = ""
        if data["poll"].get("day") in ("tue", "thu") and len(yes_users) >= 10:
            captains = random.sample(yes_users, 2)
            captains_text = (
                f"\n\n🏆 <b>КАПИТАНЫ ВЕЧЕРА:</b>\n"
                f"1. {captains[0]}\n"
                f"2. {captains[1]}"
            )
        text = (
            f"<b>{data['poll']['question']}</b>\n\n"
            f"✅ Да ({len(yes_users)}): {', '.join(yes_users) or '—'}\n"
            f"❌ Нет ({len(no_users)}): {', '.join(no_users) or '—'}\n\n"
            f"{status}" + weather_str + captains_text
        )
        await _chunk_and_send(CHAT_ID, text, parse_mode=ParseMode.HTML)
        pin_id = data.get("pinned_message_id") or data.get("message_id")
        if pin_id:
            try:
                await safe_telegram_call(bot.unpin_chat_message, CHAT_ID, pin_id)
                log.info("Unpinned poll message %s", pin_id)
            except Exception as e:
                log.exception("Failed to unpin poll message: %s", e)

        # update stats safely (only votes with user_id)
        for v in votes.values():
            if not v.get("user_id"):
                continue
            user_id = str(v["user_id"])
            name = v.get("name", "")
            if user_id not in stats:
                stats[user_id] = {"name": name, "count": 0}
            if stats[user_id]["name"] != name:
                stats[user_id]["name"] = name
            if str(v.get("answer", "")).startswith("Да"):
                stats[user_id]["count"] += 1

        # remove scheduled reminder/tag jobs for this poll if any
        try:
            reminder_job_id = f"reminder_{poll_id}"
            tag_job_id = f"tagq_{poll_id}"
            close_job_id = f"close_{poll_id}"
            try:
                scheduler.remove_job(reminder_job_id)
                log.info("Removed reminder job %s", reminder_job_id)
            except Exception:
                pass
            try:
                scheduler.remove_job(tag_job_id)
                log.info("Removed tag job %s", tag_job_id)
            except Exception:
                pass
            try:
                scheduler.remove_job(close_job_id)
                log.info("Removed close job %s", close_job_id)
            except Exception:
                pass
        except Exception:
            log.exception("Failed to remove scheduled jobs for poll %s", poll_id)

        active_polls.pop(poll_id, None)
        await save_data()
        log.info("Summary sent for poll: %s", data["poll"].get("question"))
    except Exception:
        log.exception("Failed to send summary for poll: %s", data["poll"].get("question"))

# -------------------- Poll answer handling --------------------
@dp.poll_answer_handler()
async def handle_poll_answer(poll_answer: types.PollAnswer) -> None:
    try:
        uid = poll_answer.user.id
        uname = poll_answer.user.full_name or poll_answer.user.first_name or str(uid)
        option_ids = poll_answer.option_ids
        for poll_id, data in list(active_polls.items()):
            if poll_answer.poll_id == poll_id:
                if not option_ids:
                    data["votes"].pop(str(uid), None)
                else:
                    answer = data["poll"]["options"][option_ids[0]]
                    # --- NEW: store user_id to enable direct mentions later ---
                    data["votes"][str(uid)] = {"name": uname, "answer": answer, "user_id": uid}
                # save asynchronously (fire-and-forget)
                asyncio.run_coroutine_threadsafe(save_data(), asyncio.get_event_loop())
                log.debug("Vote saved: %s -> %s", uname, data["votes"].get(str(uid)))
                return
    except Exception:
        log.exception("Error handling poll answer")

# -------------------- Bot commands --------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message) -> None:
    await message.reply("👋 Привет! Я бот для организации игр на песчанке. Напиши /commands для списка команд.")
    if not OPENWEATHER_API_KEY:
        await bot.send_message(ADMIN_ID, "⚠️ Внимание: нет API ключа погоды. Прогноз не будет показываться.")

@dp.message_handler(commands=["commands"])
async def cmd_commands(message: types.Message) -> None:
    uid = str(message.from_user.id)
    isadm = is_admin(message.from_user.id)
    lines = [
        "Список доступных команд:\n",
        "Для всех:",
        "/status — показать текущий опрос",
        "/stats — статистика «Да ✅»",
        "/nextpoll — когда следующий опрос",
        "/uptime — время работы бота",
        "/commands — справка",
    ]
    # Mini-game commands visibility
    session = mini_game_sessions.get(uid)
    if session:
        lines.append("")
        lines.append("Мини-игра:")
        # After start, show next logical steps
        # Always allow to cancel
        lines.append("/cancelgame — завершить мини-игру")
        # Solo flow
        lines.append("/penalty — пробить пенальти (личная статистика)")
        lines.append("/topscorers — таблица бомбардиров мини-игры")
        # H2H flow (creation requires challenger session)
        lines.append("/challenge — вызвать соперника (ответом на его сообщение)")
        lines.append("/accept — принять вызов (только сопернику)")
        lines.append("/decline — отклонить вызов")
        lines.append("/stake beer|pushups — выбрать ставку")
        lines.append("/kick — выполнить удар по очереди")
    else:
        lines.append("")
        lines.append("Мини-игра:")
        lines.append("/game — начать мини-игру и увидеть доступные действия")
    if isadm:
        lines.extend([
            "",
            "Для администратора:",
            "/startpoll Вопрос | Вариант1 | Вариант2 | ...",
            "/closepoll — закрыть опрос",
            "/addplayer Имя — добавить игрока",
            "/removeplayer Имя — удалить игрока",
            "/reload — обновить расписание",
            "/summary — отправить текущую сводку",
            "/backup — получить текущие данные (файл)",
            "/disablepoll <день> — отключить автоопрос (напр. вт/thu)",
            "/enablepoll <день> — включить автоопрос",
            "/pollsstatus — показать отключённые дни",
            "/forgive — снять бан за отжимания",
        ])
    await message.reply("\n".join(lines))

@dp.message_handler(commands=["nextpoll"])
async def cmd_nextpoll(message: types.Message) -> None:
    try:
        nxt = compute_next_poll_datetime()
        if not nxt:
            return await message.reply("ℹ️ Нет запланированных опросов.")
        dt, cfg = nxt
        fmt = dt.strftime("%Y-%m-%d %H:%M %Z")
        await message.reply(f"Следующий опрос: <b>{cfg['question']}</b>\nКогда: {fmt}")
    except Exception:
        log.exception("Error in /nextpoll")
        await message.reply("⚠️ Ошибка при определении следующего опроса. Проверьте логи.")

@dp.message_handler(commands=["status"])
async def cmd_status(message: types.Message) -> None:
    last = find_last_active_poll()
    if not last:
        return await message.reply("📭 Активных опросов нет.")
    _, data = last
    poll = data["poll"]
    await message.reply(f"<b>{poll['question']}</b>\n\n{format_poll_votes(data)}")

@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message) -> None:
    if not stats:
        return await message.reply("📊 Пока нет статистики.")
    stats_sorted = sorted(stats.values(), key=lambda x: -x["count"])
    text = "\n".join(f"{row['name']}: {row['count']}" for row in stats_sorted)
    await message.reply(f"📈 Статистика 'Да ✅':\n{text}")

@dp.message_handler(commands=["uptime"])
async def cmd_uptime(message: types.Message) -> None:
    uptime = datetime.now() - START_TIME
    hours, remainder = divmod(int(uptime.total_seconds()), 3600)
    minutes = (remainder // 60)
    await message.reply(f"⏱ Бот работает уже {hours} ч {minutes} мин.")

# -------------------- Admin commands --------------------
@dp.message_handler(commands=["startpoll"])
async def cmd_startpoll(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return await message.reply("❌ Нет прав.")
    parts = [p.strip() for p in message.get_args().split("|") if p.strip()]
    if len(parts) < 3:
        return await message.reply("Формат: /startpoll Вопрос | Вариант1 | Вариант2 | ...")
    poll = {"day": "manual", "time_poll": now_tz().strftime("%H:%M"), "question": parts[0], "options": parts[1:]}
    await start_poll(poll, from_admin=True)
    await message.reply("✅ Опрос создан вручную.")

# Вспомогательная для schedule_polls:
async def send_summary_by_day(poll: dict):
    for pid, data in list(active_polls.items()):
        if data["poll"]["day"] == poll["day"] and data.get("active"):
            await send_summary(pid)
            break

@dp.message_handler(commands=["closepoll"])
async def cmd_closepoll(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return await message.reply("❌ Нет прав.")
    last = find_last_active_poll()
    if not last:
        return await message.reply("📭 Нет активных опросов.")
    pid, data = last
    await send_summary(pid)
    await message.reply("✅ Опрос закрыт и итоги отправлены.")

@dp.message_handler(commands=["addplayer"])
async def cmd_addplayer(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return await message.reply("❌ Нет прав.")
    name = message.get_args().strip()
    if not name:
        return await message.reply("Использование: /addplayer Имя")
    last = find_last_active_poll()
    if not last:
        return await message.reply("📭 Нет активных опросов.")
    pid, data = last
    key = f"admin_{name}_{int(time.time())}"
    data["votes"][key] = {"name": name, "answer": "Да ✅ (добавлен вручную)"}
    await save_data()
    await message.reply(f"✅ Игрок '{name}' добавлен как 'Да ✅'.")

@dp.message_handler(commands=["removeplayer"])
async def cmd_removeplayer(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return await message.reply("❌ Нет прав.")
    name = message.get_args().strip()
    if not name:
        return await message.reply("Использование: /removeplayer Имя")
    last = find_last_active_poll()
    if not last:
        return await message.reply("📭 Нет активных опросов.")
    pid, data = last
    removed = 0
    for uid, v in list(data["votes"].items()):
        if v.get("name") == name:
            del data["votes"][uid]
            removed += 1
    await save_data()
    await message.reply(f"✅ Игрок '{name}' удалён (найдено: {removed}).")

@dp.message_handler(commands=["reload"])
async def cmd_reload(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return await message.reply("❌ Нет прав.")
    scheduler.remove_all_jobs()
    schedule_polls()
    for pid, data in list(active_polls.items()):
        if data.get("active"):
            schedule_poll_reminders(pid)
    await message.reply("✅ Расписание обновлено.")

@dp.message_handler(commands=["disablepoll"])
async def cmd_disablepoll(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return await message.reply("❌ Нет прав.")
    arg = message.get_args().strip()
    day_key = normalize_day_key(arg)
    if not day_key:
        return await message.reply("Использование: /disablepoll <день недели> (напр. вт, thu)")
    disabled_days.add(day_key)
    scheduler.remove_all_jobs()
    schedule_polls()
    await save_data()
    await message.reply(f"✅ Автоопрос для '{day_key}' отключён. Расписание обновлено.")

@dp.message_handler(commands=["enablepoll"])
async def cmd_enablepoll(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return await message.reply("❌ Нет прав.")
    arg = message.get_args().strip()
    day_key = normalize_day_key(arg)
    if not day_key:
        return await message.reply("Использование: /enablepoll <день недели> (напр. вт, thu)")
    if day_key in disabled_days:
        disabled_days.remove(day_key)
    scheduler.remove_all_jobs()
    schedule_polls()
    await save_data()
    await message.reply(f"✅ Автоопрос для '{day_key}' включён. Расписание обновлено.")

@dp.message_handler(commands=["pollsstatus"])
async def cmd_pollsstatus(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return await message.reply("❌ Нет прав.")
    if not disabled_days:
        return await message.reply("ℹ️ Все дни включены для автозапуска опросов.")
    days_txt = ", ".join(sorted(list(disabled_days)))
    await message.reply(f"⛔ Отключены дни: {days_txt}")

@dp.message_handler(commands=["summary"])
async def cmd_summary(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return await message.reply("❌ Нет прав.")
    last = find_last_active_poll()
    if not last:
        return await message.reply("📭 Нет активных опросов.")
    pid, data = last
    await send_summary(pid)
    await message.reply("✅ Итог отправлен вручную.")

@dp.message_handler(commands=["backup"])
async def cmd_backup(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return await message.reply("❌ Нет прав.")
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "rb") as f:
            await message.reply_document(f, caption="📦 Текущие данные бота")
    else:
        await message.reply("⚠️ Данных для бэкапа нет.")

@dp.message_handler(commands=["forgive"])
async def cmd_forgive(message: types.Message) -> None:
    try:
        if not is_admin(message.from_user.id):
            return await message.reply("❌ Нет прав.")
        # accept reply target or @username not supported => reply required
        target = None
        if message.reply_to_message and message.reply_to_message.from_user:
            target = message.reply_to_message.from_user
        if not target:
            return await message.reply("Ответьте на сообщение игрока и введите /forgive")
        uid = str(target.id)
        if uid in banned_users:
            banned_users.pop(uid, None)
        if uid in pending_penalties:
            pending_penalties.pop(uid, None)
        await save_data()
        await message.reply(f"✅ {html.escape(target.full_name or str(target.id))} реабилитирован и снова может играть.", parse_mode=ParseMode.HTML)
    except Exception:
        log.exception("Error in /forgive")
        await message.reply("⚠️ Ошибка при разблокировке")

# -------------------- Mini-game: Penalty --------------------
@dp.message_handler(commands=["game"])
async def cmd_game(message: types.Message) -> None:
    try:
        uid = str(message.from_user.id)
        if str(message.from_user.id) in banned_users and _now_ts() < banned_users[str(message.from_user.id)]:
            return await message.reply("⛔ Вы временно не можете играть в мини-игры. Обратитесь к администратору или выполните ранее назначенное наказание.")
        mini_game_sessions[uid] = {"started": True, "ts": _now_ts()}
        await message.reply(
            "🎮 Мини-игра запущена!\n\n"
            "Доступные действия теперь видны в /commands.\n"
            "• Для одиночной попытки — используйте /penalty\n"
            "• Для дуэли — ответьте на сообщение соперника и введите /challenge\n"
            "• Завершить мини-игру: /cancelgame"
        )
    except Exception:
        log.exception("Error in /game")
        await message.reply("⚠️ Ошибка при запуске мини-игры")

@dp.message_handler(commands=["cancelgame"])
async def cmd_cancelgame(message: types.Message) -> None:
    try:
        uid = str(message.from_user.id)
        mini_game_sessions.pop(uid, None)
        await message.reply("⏹ Мини-игра завершена. Чтобы начать снова — /game")
    except Exception:
        log.exception("Error in /cancelgame")
        await message.reply("⚠️ Ошибка при завершении мини-игры")
@dp.message_handler(commands=["penalty"])
async def cmd_penalty(message: types.Message) -> None:
    try:
        # block banned users
        if str(message.from_user.id) in banned_users and _now_ts() < banned_users[str(message.from_user.id)]:
            return await message.reply("⛔ Вы временно не можете играть в мини-игры. Обратитесь к администратору или выполните ранее назначенное наказание.")
        # require started session
        if str(message.from_user.id) not in mini_game_sessions:
            return await message.reply("Сначала начните мини-игру командой /game")
        uid = str(message.from_user.id)
        name = message.from_user.full_name or message.from_user.first_name or uid
        # initialize player
        if uid not in game_stats:
            game_stats[uid] = {"name": name, "goals": 0, "shots": 0}
        # simulate shot: ~70% goal
        import random as _rnd
        outcome = _rnd.random()
        game_stats[uid]["shots"] += 1
        is_goal = outcome < 0.7
        if is_goal:
            game_stats[uid]["goals"] += 1
            result_text = "⚽️ ГОООЛ! Вратарь даже не шелохнулся!"
        else:
            miss_variants = [
                "🥅 Штанга! Чуть-чуть не хватило…",
                "🧤 Вратарь берёт удар!",
                "🔺 Перекладина! Публика ахнула…",
                "↗️ Ушёл выше ворот…",
            ]
            result_text = _rnd.choice(miss_variants)
        g = game_stats[uid]["goals"]
        s = game_stats[uid]["shots"]
        await save_data()
        await message.reply(
            f"{result_text}\n\nТвоя статистика: {g} ⚽ из {s} ударов."
        )
    except Exception:
        log.exception("Error in /penalty")
        await message.reply("⚠️ Ошибка при выполнении удара. Попробуйте ещё раз.")

@dp.message_handler(commands=["topscorers"])
async def cmd_topscorers(message: types.Message) -> None:
    try:
        # allow viewing without session; but guide if empty
        if not game_stats:
            return await message.reply("Пока никто не пробивал пенальти. Наберите /game, затем /penalty")
        # sort by goals desc, then by shots asc
        top = sorted(game_stats.values(), key=lambda x: (-int(x.get("goals", 0)), int(x.get("shots", 0))))[:10]
        lines = []
        for i, p in enumerate(top, start=1):
            lines.append(f"{i}. {html.escape(p.get('name','Игрок'))}: {int(p.get('goals',0))} ⚽ из {int(p.get('shots',0))}")
        await message.reply("🏆 Таблица бомбардиров (мини-игра):\n" + "\n".join(lines))
    except Exception:
        log.exception("Error in /topscorers")
        await message.reply("⚠️ Ошибка при показе таблицы бомбардиров.")

# -------------------- H2H Penalty Match Commands --------------------
def _reset_active_match():
    global active_match
    active_match = None

def _format_match_score(m):
    a = m["a"]; b = m["b"]
    return f"{html.escape(a['name'])} {a['goals']}-{b['goals']} {html.escape(b['name'])}"

def _match_is_expired(m) -> bool:
    return _now_ts() - m.get("created_ts", _now_ts()) > 5*60

@dp.message_handler(commands=["challenge"])
async def cmd_challenge(message: types.Message) -> None:
    global active_match
    try:
        # bans
        if str(message.from_user.id) in banned_users and _now_ts() < banned_users[str(message.from_user.id)]:
            return await message.reply("⛔ Вы временно не можете играть в мини-игры.")
        # challenger must have a started session
        if str(message.from_user.id) not in mini_game_sessions:
            return await message.reply("Сначала начните мини-игру командой /game")
        if active_match and not _match_is_expired(active_match):
            return await message.reply("⚠️ Уже идёт серия пенальти. Дождитесь окончания.")
        # determine opponent from mention or reply
        opponent = None
        if message.reply_to_message and message.reply_to_message.from_user:
            opponent = message.reply_to_message.from_user
        else:
            args = message.get_args().strip()
            if args.startswith("@"):
                # aiogram can't resolve @username to id reliably here; require reply or tag+id not trivial
                # fallback: ask to reply
                return await message.reply("Ответьте на сообщение соперника и введите /challenge")
        if not opponent:
            return await message.reply("Ответьте на сообщение соперника и введите /challenge")
        if opponent.id == message.from_user.id:
            return await message.reply("Нельзя вызвать самого себя")
        challenger = message.from_user
        active_match = {
            "created_ts": _now_ts(),
            "status": "pending",
            "chat_id": message.chat.id,
            "a": {"id": challenger.id, "name": challenger.full_name or challenger.first_name, "goals": 0, "shots": 0, "stake": None},
            "b": {"id": opponent.id, "name": opponent.full_name or opponent.first_name, "goals": 0, "shots": 0, "stake": None},
            "turn": None,
            "sudden": False,
            "accept_deadline": _now_ts() + 60,
            "stake_deadline": None,
            "game_deadline": None,
            "last_action_ts": _now_ts(),
        }
        await message.reply(
            f"🎮 Вызов на серию пенальти! {_mention(opponent.id, opponent.full_name or opponent.first_name)}, примите вызов командой /accept или отклоните /decline.\n"
            f"Каждый выбирает ставку через /stake beer или /stake pushups. После подтверждения начнём. Время на игру — 5 минут."
            , parse_mode=ParseMode.HTML
        )
    except Exception:
        log.exception("Error in /challenge")
        await message.reply("⚠️ Ошибка при создании вызова")

@dp.message_handler(commands=["accept"])
async def cmd_accept(message: types.Message) -> None:
    global active_match
    try:
        if str(message.from_user.id) in banned_users and _now_ts() < banned_users[str(message.from_user.id)]:
            return await message.reply("⛔ Вы временно не можете играть в мини-игры.")
        if not active_match or _match_is_expired(active_match):
            _reset_active_match(); return await message.reply("Нет активного вызова")
        m = active_match
        if message.from_user.id != m["b"]["id"]:
            return await message.reply("Принять вызов может только соперник")
        if _now_ts() > m.get("accept_deadline", 0):
            _reset_active_match(); return await message.reply("⏰ Время на принятие вызова истекло")
        m["status"] = "accepted"
        m["stake_deadline"] = _now_ts() + 120
        await message.reply("✅ Вызов принят. Оба игрока выберите ставку: /stake beer или /stake pushups")
    except Exception:
        log.exception("Error in /accept")
        await message.reply("⚠️ Ошибка при принятии вызова")

@dp.message_handler(commands=["decline"])
async def cmd_decline(message: types.Message) -> None:
    global active_match
    try:
        if not active_match or _match_is_expired(active_match):
            _reset_active_match(); return await message.reply("Нет активного вызова")
        m = active_match
        if message.from_user.id not in (m["a"]["id"], m["b"]["id"]):
            return await message.reply("Отклонить может только участник пары")
        await message.reply("❌ Вызов отклонён")
        _reset_active_match()
    except Exception:
        log.exception("Error in /decline")
        await message.reply("⚠️ Ошибка при отклонении вызова")

@dp.message_handler(commands=["stake"])
async def cmd_stake(message: types.Message) -> None:
    global active_match
    try:
        if str(message.from_user.id) in banned_users and _now_ts() < banned_users[str(message.from_user.id)]:
            return await message.reply("⛔ Вы временно не можете играть в мини-игры.")
        if not active_match or _match_is_expired(active_match):
            _reset_active_match(); return await message.reply("Нет активной игры")
        m = active_match
        if m["status"] not in ("pending", "accepted"):
            return await message.reply("Ставку можно выбрать до старта игры")
        if m.get("stake_deadline") and _now_ts() > m["stake_deadline"]:
            _reset_active_match(); return await message.reply("⏰ Время на выбор ставки истекло")
        arg = (message.get_args() or "").strip().lower()
        if arg not in ("beer", "pushups"):
            return await message.reply("Использование: /stake beer | /stake pushups")
        player = None
        if message.from_user.id == m["a"]["id"]:
            player = m["a"]
        elif message.from_user.id == m["b"]["id"]:
            player = m["b"]
        else:
            return await message.reply("Только участники игры выбирают ставку")
        player["stake"] = arg
        await message.reply(f"✅ Ставка выбрана: {'🍺 пиво' if arg=='beer' else '💪 30 отжиманий'}")
        if m["status"] == "accepted" and m["a"]["stake"] and m["b"]["stake"]:
            # start game
            m["status"] = "running"
            # random who starts
            import random as _r
            m["turn"] = _r.choice(["a", "b"])
            m["created_ts"] = _now_ts()  # reset timer for 5 minutes window
            m["game_deadline"] = _now_ts() + 5*60
            m["last_action_ts"] = _now_ts()
            await message.reply(
                "🏁 Игра началась! По 5 ударов каждому, потом серия до промаха.\n"
                f"Первым бьёт: {html.escape(m[m['turn']]['name'])}. Используйте /kick по очереди.\n"
                f"Ставки: {html.escape(m['a']['name'])} — {'🍺' if m['a']['stake']=='beer' else '💪'}, {html.escape(m['b']['name'])} — {'🍺' if m['b']['stake']=='beer' else '💪'}",
                parse_mode=ParseMode.HTML
            )
            # Try to lock chat: disable messages for all, allow two players and admin
            try:
                await bot.set_chat_permissions(m["chat_id"], types.ChatPermissions(can_send_messages=False))
                for uid in (m["a"]["id"], m["b"]["id"], ADMIN_ID):
                    try:
                        await bot.restrict_chat_member(m["chat_id"], uid, types.ChatPermissions(can_send_messages=True))
                    except Exception:
                        pass
            except Exception:
                pass
    except Exception:
        log.exception("Error in /stake")
        await message.reply("⚠️ Ошибка при выборе ставки")

def _apply_kick(m: Dict[str, Any], side: str) -> bool:
    import random as _r
    m[side]["shots"] += 1
    is_goal = _r.random() < 0.7
    if is_goal:
        m[side]["goals"] += 1
    return is_goal

def _kicks_each_done(m) -> bool:
    return m["a"]["shots"] >= 5 and m["b"]["shots"] >= 5

def _winner_if_any(m) -> Optional[str]:
    # return 'a' or 'b' if decided
    if not _kicks_each_done(m):
        return None
    if m["a"]["goals"] != m["b"]["goals"]:
        return 'a' if m["a"]["goals"] > m["b"]["goals"] else 'b'
    return None

@dp.message_handler(commands=["kick"])
async def cmd_kick(message: types.Message) -> None:
    global active_match
    try:
        if str(message.from_user.id) in banned_users and _now_ts() < banned_users[str(message.from_user.id)]:
            return await message.reply("⛔ Вы временно не можете играть в мини-игры.")
        if not active_match or _match_is_expired(active_match):
            _reset_active_match(); return await message.reply("Нет активной игры")
        m = active_match
        if m["status"] != "running":
            return await message.reply("Игра ещё не началась. Выберите ставки: /stake beer|pushups")
        if m.get("game_deadline") and _now_ts() > m["game_deadline"]:
            # timeout finish
            await message.reply("⏰ Время игры истекло. Игра отменена.")
            try:
                await bot.set_chat_permissions(m["chat_id"], types.ChatPermissions(can_send_messages=True))
            except Exception:
                pass
            _reset_active_match()
            return
        # per-kick timeout 30s
        if _now_ts() - m.get("last_action_ts", _now_ts()) > 30:
            return await message.reply("⏰ Слишком долго ждём удар. Сделайте /kick сейчас.")
        # only current player can kick
        current_id = m[m["turn"]]["id"]
        if message.from_user.id != current_id:
            return await message.reply("Сейчас очередь соперника")
        side = m["turn"]
        is_goal = _apply_kick(m, side)
        # update global personal mini-game stats as well
        uid = str(current_id)
        if uid not in game_stats:
            game_stats[uid] = {"name": m[side]["name"], "goals": 0, "shots": 0}
        game_stats[uid]["shots"] += 1
        if is_goal:
            game_stats[uid]["goals"] += 1
        # build message
        shot_num = m[side]["shots"]
        text = ("⚽️ ГОООЛ!" if is_goal else "🧤 МИМО/СЕЙВ!") + f" Удар №{shot_num}. Счёт: {_format_match_score(m)}"
        await message.reply(text, parse_mode=ParseMode.HTML)
        m["last_action_ts"] = _now_ts()
        await save_data()
        # check decision after 5 each
        winner = _winner_if_any(m)
        if winner:
            loser = 'b' if winner=='a' else 'a'
            await message.reply(
                f"🏆 Победа: {html.escape(m[winner]['name'])}! Проигравший {html.escape(m[loser]['name'])} выполняет свою ставку ({'🍺' if m[loser]['stake']=='beer' else '💪 30 отжиманий'})",
                parse_mode=ParseMode.HTML
            )
            # enqueue penalty if pushups
            if m[loser]['stake'] == 'pushups':
                pending_penalties[str(m[loser]['id'])] = {
                    'due_ts': _now_ts() + 10*60,
                    'chat_id': m['chat_id'],
                    'name': m[loser]['name'],
                }
                try:
                    # schedule enforcement
                    if scheduler:
                        lid = f"penalty_{m[loser]['id']}_{int(_now_ts())}"
                        scheduler.add_job(lambda uid=m[loser]['id']: asyncio.run_coroutine_threadsafe(enforce_penalty(uid), asyncio.get_event_loop()), trigger='date', run_date=datetime.fromtimestamp(_now_ts()+10*60, tz=KALININGRAD_TZ), id=lid)
                except Exception:
                    pass
                await message.reply(f"🎥 {html.escape(m[loser]['name'])}, у вас 10 минут, чтобы скинуть видео отжиманий сюда.", parse_mode=ParseMode.HTML)
            try:
                await bot.set_chat_permissions(m["chat_id"], types.ChatPermissions(can_send_messages=True))
            except Exception:
                pass
            _reset_active_match()
            return
        # sudden death if 5 each and tie: need pair per round
        if _kicks_each_done(m) and not m["sudden"]:
            m["sudden"] = True
            await message.reply("⚠️ Ничья после 5 ударов. СЕРИЯ ДО ПРОМАХА: бьём по очереди!")
        elif m["sudden"]:
            # if it's end of pair (i.e., after both have taken equal number in sudden), evaluate
            a_shots = m["a"]["shots"]; b_shots = m["b"]["shots"]
            if a_shots == b_shots:
                if m["a"]["goals"] != m["b"]["goals"]:
                    winner = 'a' if m["a"]["goals"] > m["b"]["goals"] else 'b'
                    loser = 'b' if winner=='a' else 'a'
                    await message.reply(
                        f"🏆 Победа в серии до промаха: {html.escape(m[winner]['name'])}! Проигравший {html.escape(m[loser]['name'])} выполняет свою ставку ({'🍺' if m[loser]['stake']=='beer' else '💪 30 отжиманий'})",
                        parse_mode=ParseMode.HTML
                    )
                    if m[loser]['stake'] == 'pushups':
                        pending_penalties[str(m[loser]['id'])] = {
                            'due_ts': _now_ts() + 10*60,
                            'chat_id': m['chat_id'],
                            'name': m[loser]['name'],
                        }
                        try:
                            if scheduler:
                                lid = f"penalty_{m[loser]['id']}_{int(_now_ts())}"
                                scheduler.add_job(lambda uid=m[loser]['id']: asyncio.run_coroutine_threadsafe(enforce_penalty(uid), asyncio.get_event_loop()), trigger='date', run_date=datetime.fromtimestamp(_now_ts()+10*60, tz=KALININGRAD_TZ), id=lid)
                        except Exception:
                            pass
                        await message.reply(f"🎥 {html.escape(m[loser]['name'])}, у вас 10 минут, чтобы скинуть видео отжиманий сюда.", parse_mode=ParseMode.HTML)
                    try:
                        await bot.set_chat_permissions(m["chat_id"], types.ChatPermissions(can_send_messages=True))
                    except Exception:
                        pass
                    _reset_active_match()
                    return
        # switch turn
        m["turn"] = 'b' if side=='a' else 'a'
        await message.reply(f"Теперь бьёт: {html.escape(m[m['turn']]['name'])}", parse_mode=ParseMode.HTML)
    except Exception:
        log.exception("Error in /kick")
        await message.reply("⚠️ Ошибка при ударе")

# -------------------- Scheduler helpers --------------------
def compute_next_poll_datetime() -> Optional[Tuple[datetime, Dict[str, Any]]]:
    now = now_tz()
    candidates = []
    for cfg in polls_config:
        day = cfg.get("day")
        if day not in WEEKDAY_MAP:
            continue
        # skip disabled days in next poll calculation
        if day in disabled_days:
            continue
        hour, minute = map(int, cfg["time_poll"].split(":"))
        target = WEEKDAY_MAP[day]
        days_ahead = (target - now.weekday()) % 7
        dt = now_tz().replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=days_ahead)
        if dt <= now:
            dt += timedelta(days=7)
        candidates.append((dt, cfg))
    if not candidates:
        return None
    return sorted(candidates, key=lambda x: x[0])[0]

# Функции для APScheduler
# ---
def _schedule_poll_job(poll):
    asyncio.run_coroutine_threadsafe(start_poll(poll), asyncio.get_event_loop())

def _schedule_summary_job(poll):
    asyncio.run_coroutine_threadsafe(send_summary_by_day(poll), asyncio.get_event_loop())

def schedule_polls() -> None:
    if scheduler is None:
        log.error('Scheduler not initialized!')
        return
    scheduler.remove_all_jobs()
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()
    for idx, poll in enumerate(polls_config):
        try:
            # skip disabled days
            if poll.get("day") in disabled_days:
                log.info("⏭️ Skipping scheduling for %s (disabled)", poll.get("day"))
                continue
            tp = list(map(int, poll["time_poll"].split(":")))
            tg = list(map(int, poll["time_game"].split(":")))
            poll_job_id = f"poll_{poll['day']}_{idx}"
            scheduler.add_job(
                _schedule_poll_job,
                trigger=CronTrigger(
                    day_of_week=poll["day"],
                    hour=tp[0],
                    minute=tp[1],
                    timezone=KALININGRAD_TZ
                ),
                args=[poll],
                id=poll_job_id
            )
            # корректное вычисление времени итогов
            summary_hour = max(tg[0] - 1, 0)
            summary_job_id = f"summary_{poll['day']}_{idx}"
            if poll["day"] in ("tue", "thu"):
                # Итоги в тот же день за час до игры (например, 19:00)
                summary_dow = poll["day"]
            else:
                # Для пятницы: на следующий день (суббота) за час до игры
                day_index = WEEKDAY_MAP[poll["day"]]
                next_day_index = (day_index + 1) % 7
                summary_dow = list(WEEKDAY_MAP.keys())[next_day_index]

            scheduler.add_job(
                _schedule_summary_job,
                trigger=CronTrigger(
                    day_of_week=summary_dow,
                    hour=summary_hour,
                    minute=tg[1],
                    timezone=KALININGRAD_TZ
                ),
                args=[poll],
                id=summary_job_id
            )
            log.info(f"✅ Scheduled poll for {poll['day']} at {poll['time_poll']} (Kaliningrad)")
        except Exception:
            log.exception("Failed to schedule poll: %s", poll)

    try:
        scheduler.add_job(lambda: asyncio.run_coroutine_threadsafe(save_data(), loop), "interval", minutes=10)
    except Exception:
        log.exception("Failed to schedule autosave job")

    try:
        scheduler.add_job(make_backup, "cron", hour=3, minute=0, timezone=KALININGRAD_TZ)
    except Exception:
        log.exception("Failed to schedule backup job")

    log.info("Scheduler refreshed (timezone: Europe/Kaliningrad)")
    log.info("=== Запланированные задания ===")
    for job in scheduler.get_jobs():
        nxt = getattr(job, "next_run_time", None)
        log.info(f"Job: {job.id}, next run: {nxt}")


# -------------------- KeepAlive server for Railway --------------------
async def handle(request):
    return web.Response(text="✅ Bot is alive")

async def start_keepalive_server() -> None:
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
        log.info("KeepAlive server started on port %s", PORT)
    except OSError as e:
        if e.errno == 98:
            log.warning("⚠️ Port %s already in use, skipping KeepAlive server startup", PORT)
        else:
            log.exception("Failed to start KeepAlive server")
            raise

@dp.message_handler(content_types=[types.ContentType.VIDEO, types.ContentType.VIDEO_NOTE])
async def handle_video_penalty(message: types.Message) -> None:
    try:
        uid = str(message.from_user.id)
        pen = pending_penalties.get(uid)
        if not pen:
            return
        if _now_ts() > pen.get('due_ts', 0):
            return
        # mark done
        pending_penalties.pop(uid, None)
        await save_data()
        await message.reply("✅ Наказание выполнено. Спасибо!")
    except Exception:
        log.exception("Error in handle_video_penalty")

async def enforce_penalty(user_id: int) -> None:
    try:
        uid = str(user_id)
        pen = pending_penalties.get(uid)
        if not pen:
            return
        if _now_ts() <= pen.get('due_ts', 0):
            return
        pending_penalties.pop(uid, None)
        # ban from games for 24h (soft ban: block game commands)
        banned_users[uid] = _now_ts() + 24*3600
        await save_data()
        try:
            await bot.send_message(pen['chat_id'], f"⏰ Время вышло! {html.escape(pen.get('name','Игрок'))} не выполнил(а) отжимания. Доступ к мини-играм ограничен на 24 часа. Для снятия — пришлите видео или админ может /forgive.")
        except Exception:
            pass
    except Exception:
        log.exception("Error in enforce_penalty")


# -------------------- Errors and shutdown --------------------
@dp.errors_handler()
async def global_errors(update, exception):
    log.exception("Global error: %s", exception)
    try:
        await safe_telegram_call(bot.send_message, ADMIN_ID, f"⚠️ Ошибка: {exception}")
    except exceptions.BotBlocked:
        log.warning("Admin blocked the bot — can't send error message")
    except Exception:
        log.exception("Failed to notify admin about error")
    return True

async def shutdown() -> None:
    log.info("Shutting down...")
    try:
        await save_data()
    except Exception:
        log.exception("Error while saving data during shutdown")
    try:
        if scheduler and getattr(scheduler, 'running', False):
            scheduler.shutdown(wait=False)
    except Exception:
        log.exception("Error shutting down scheduler")
    try:
        await bot.session.close()
    except Exception:
        log.exception("Error closing aiohttp session")
    log.info("Shutdown complete.")

def _install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown()))
        except NotImplementedError:
            # on some platforms (Windows) add_signal_handler may not be implemented
            pass

# -------------------- Main --------------------
async def main() -> None:
    log.info("Starting bot...")
    await load_data()
    global scheduler
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_event_loop()
    scheduler = AsyncIOScheduler(timezone=KALININGRAD_TZ, event_loop=MAIN_LOOP)
    # Восстановление напоминаний
    for pid, data in list(active_polls.items()):
        try:
            if data.get("active"):
                schedule_poll_reminders(pid)
        except Exception:
            log.exception("Failed to restore reminders for poll %s", pid)

    # ensure polling mode
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    # schedule jobs and keepalive + scheduler
    await start_keepalive_server()

    # === ИНИЦИАЛИЗАЦИЯ ПЛАНИРОВЩИКА (Scheduler) ===
    try:
        # Получаем текущий активный event loop (тот же, что использует aiogram)
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()

    # Планируем опросы и запускаем планировщик
    schedule_polls()
    try:
        scheduler.start()
    except Exception:
        log.exception("Failed to start scheduler")

    # notify admin
    await safe_telegram_call(bot.send_message, ADMIN_ID, "✅ Бот запущен и готов к работе!")

    # add signal handlers
    loop = asyncio.get_event_loop()
    _install_signal_handlers(loop)

    log.info("Start polling...")
    await dp.start_polling()


if __name__ == "__main__":
    # robust restart loop
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("Stopped by KeyboardInterrupt")
            break
        except Exception:
            log.exception("Critical error in main — will attempt restart")
            time.sleep(5)
            continue
        else:
            break




