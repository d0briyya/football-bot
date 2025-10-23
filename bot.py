# -*- coding: utf-8 -*-
"""
Полная версия бота для aiogram 2.25.1
📅 Добавлено: в субботу бот просто сообщает статистику, без фразы "собираемся / не собираемся".
"""

import os
import json
import asyncio
import logging
import time
from datetime import datetime, time as dtime, timedelta
import signal
import atexit
from typing import Optional, Tuple

from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode
from aiogram.utils import exceptions
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone
from dotenv import load_dotenv
from aiohttp import web

# === Конфигурация ===
load_dotenv()
TOKEN = os.getenv("TG_BOT_TOKEN", "YOUR_TOKEN_HERE")
CHAT_ID = int(os.getenv("TG_CHAT_ID", "-1002841862533"))
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "914344682"))
DATA_FILE = os.getenv("DATA_FILE", "bot_data.json")
PORT = int(os.getenv("PORT", 8080))
LOCK_FILE = "bot.lock"
LOG_FILE = "bot.log"

# === Логирование ===
import sys

class StdoutFilter(logging.Filter):
    def filter(self, record):
        return record.levelno < logging.ERROR  # INFO и WARNING → stdout

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.addFilter(StdoutFilter())

stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.ERROR)

file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[file_handler, stdout_handler, stderr_handler]
)

log = logging.getLogger("bot")

# === Защита от дубликатов ===
if os.path.exists(LOCK_FILE):
    log.warning("⚠️ Обнаружен lock-файл. Возможно, бот уже запущен.")
    time.sleep(5)
    if os.path.exists(LOCK_FILE):
        raise RuntimeError("❌ Второй экземпляр бота запрещён (lock-файл найден).")
with open(LOCK_FILE, "w") as f:
    f.write(str(os.getpid()))
atexit.register(lambda: os.path.exists(LOCK_FILE) and os.remove(LOCK_FILE))

# === Telegram и окружение ===
bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot)

kaliningrad_tz = timezone("Europe/Kaliningrad")
scheduler = AsyncIOScheduler(timezone=kaliningrad_tz)

# === Состояние ===
active_polls = {}
stats = {}

# === Конфиг автоопросов ===
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

def now_tz():
    return datetime.now(kaliningrad_tz)

def iso_now():
    return now_tz().isoformat()

def find_last_active_poll() -> Optional[Tuple[str, dict]]:
    if not active_polls:
        return None
    items = sorted(active_polls.items(), key=lambda it: it[1].get("created_at", ""), reverse=True)
    for pid, data in items:
        if data.get("active"):
            return pid, data
    return None

def format_poll_votes(data: dict) -> str:
    votes = data.get("votes", {})
    if not votes:
        return "— Никто ещё не голосовал."
    return "\n".join(f"{v.get('name')} — {v.get('answer')}" for v in votes.values())

# === Сохранение и загрузка данных ===
async def save_data():
    try:
        data = {"active_polls": active_polls, "stats": stats}
        def _write():
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        await asyncio.to_thread(_write)
    except Exception as e:
        log.exception(f"Ошибка сохранения данных: {e}")

async def load_data():
    global active_polls, stats
    try:
        if os.path.exists(DATA_FILE):
            def _read():
                with open(DATA_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            data = await asyncio.to_thread(_read)
            active_polls = data.get("active_polls", {})
            stats = data.get("stats", {})
        else:
            log.info("ℹ️ Нет сохранённых данных — старт с нуля.")
    except Exception as e:
        log.exception(f"Ошибка загрузки данных: {e}")

# === Безопасные Telegram-запросы ===
async def safe_telegram_call(func, *args, retries=3, **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)
        except exceptions.RetryAfter as e:
            await asyncio.sleep(e.timeout + 1)
        except exceptions.TelegramAPIError:
            if attempt == retries:
                return None
            await asyncio.sleep(2 * attempt)
        except Exception:
            if attempt == retries:
                return None
            await asyncio.sleep(2 * attempt)

# === Создание опроса ===
async def start_poll(poll: dict, from_admin=False):
    options = poll.get("options") or []
    if len(options) < 2:
        return
    msg = await safe_telegram_call(bot.send_poll, chat_id=CHAT_ID, question=poll["question"], options=options,
                                   is_anonymous=False, allows_multiple_answers=False)
    if not msg:
        return
    poll_id = msg.poll.id
    active_polls[poll_id] = {
        "message_id": msg.message_id,
        "poll": poll,
        "votes": {},
        "active": True,
        "created_at": iso_now()
    }
    await save_data()
    await safe_telegram_call(bot.pin_chat_message, chat_id=CHAT_ID, message_id=msg.message_id, disable_notification=True)
    await safe_telegram_call(bot.send_message, CHAT_ID, "📢 <b>Новый опрос!</b>\nПроголосуйте 👇")

# === Итоги опросов ===
async def send_summary(poll):
    for poll_id, data in list(active_polls.items()):
        if data["poll"] == poll:
            data["active"] = False
            votes = data.get("votes", {})
            yes_users = [v["name"] for v in votes.values() if v["answer"].startswith("Да")]
            no_users = [v["name"] for v in votes.values() if v["answer"].startswith("Нет")]
            total_yes = len(yes_users)
            poll_day = poll.get("day")

            # 💡 Обновлённая логика для субботы
            if poll_day == "fri":
                status = (
                    "📊 Итог субботнего опроса:\n\n"
                    f"👥 Всего проголосовало: {len(votes)} человек(а).\n"
                    "Решайте сами идти или нет — этот опрос просто для удобства, "
                    "в субботу многие приходят без опроса ⚽"
                )
            else:
                status = (
                    "⚠️ Сегодня не собираемся — меньше 10 участников."
                    if total_yes < 10 else
                    "✅ Сегодня собираемся на песчанке! ⚽"
                )

            text = (
                f"<b>{poll['question']}</b>\n\n"
                f"✅ Да ({len(yes_users)}): {', '.join(yes_users) or '—'}\n"
                f"❌ Нет ({len(no_users)}): {', '.join(no_users) or '—'}\n\n"
                f"{status}"
            )

            await safe_telegram_call(bot.send_message, CHAT_ID, text)
            for v in votes.values():
                if v["answer"].startswith("Да"):
                    stats[v["name"]] = stats.get(v["name"], 0) + 1
            del active_polls[poll_id]
            await save_data()
            break

# === Обработка голосов ===
@dp.poll_answer_handler()
async def handle_poll_answer(poll_answer: types.PollAnswer):
    uid = poll_answer.user.id
    uname = poll_answer.user.first_name or poll_answer.user.username or str(uid)
    option_ids = poll_answer.option_ids
    for poll_id, data in active_polls.items():
        if poll_answer.poll_id == poll_id:
            if not option_ids:
                data["votes"].pop(str(uid), None)
            else:
                answer = data["poll"]["options"][option_ids[0]]
                data["votes"][str(uid)] = {"name": uname, "answer": answer}
            await save_data()
            return

# === Команды ===
@dp.message_handler(commands=["commands"])
async def cmd_commands(message: types.Message):
    await message.reply(
        "Список команд:\n\n"
        "/status — показать текущий опрос\n"
        "/stats — статистика 'Да ✅'\n"
        "/nextpoll — когда следующий опрос\n"
        "/commands — список команд\n\n"
        "Админ:\n"
        "/startpoll Вопрос | Вариант1 | Вариант2 | ... — создать опрос\n"
        "/closepoll — закрыть текущий опрос\n"
        "/addplayer Имя — добавить игрока как 'Да ✅'\n"
        "/removeplayer Имя — удалить вручную добавленного"
    )

@dp.message_handler(commands=["status"])
async def cmd_status(message: types.Message):
    last = find_last_active_poll()
    if not last:
        return await message.reply("📭 Активных опросов нет.")
    _, data = last
    poll = data["poll"]
    text = f"<b>{poll['question']}</b>\n\n{format_poll_votes(data)}"
    await message.reply(text)

@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if not stats:
        return await message.reply("📊 Пока нет статистики.")
    text = "\n".join([f"{n}: {c}" for n, c in sorted(stats.items(), key=lambda x: -x[1])])
    await message.reply(f"📈 Статистика 'Да ✅':\n{text}")

# === Планировщик ===
def compute_next_poll_datetime() -> Optional[Tuple[datetime, dict]]:
    now = now_tz()
    candidates = []
    for cfg in polls_config:
        day = cfg["day"]
        hour, minute = map(int, cfg["time_poll"].split(":"))
        target_weekday = WEEKDAY_MAP[day]
        days_ahead = (target_weekday - now.weekday()) % 7
        candidate = datetime(now.year, now.month, now.day, hour, minute, tzinfo=kaliningrad_tz) + timedelta(days=days_ahead)
        if candidate <= now:
            candidate += timedelta(days=7)
        candidates.append((candidate, cfg))
    if not candidates:
        return None
    candidates.sort(key=lambda it: it[0])
    return candidates[0]

def schedule_polls():
    scheduler.remove_all_jobs()
    loop = asyncio.get_event_loop()
    for poll in polls_config:
        tp = list(map(int, poll["time_poll"].split(":")))
        tg = list(map(int, poll["time_game"].split(":")))
        scheduler.add_job(
            lambda p=poll: asyncio.run_coroutine_threadsafe(start_poll(p), loop),
            trigger=CronTrigger(day_of_week=poll["day"], hour=tp[0], minute=tp[1]),
            id=f"poll_{poll['day']}", replace_existing=True
        )
        next_day = "sat" if poll["day"] == "fri" else poll["day"]
        scheduler.add_job(
            lambda p=poll: asyncio.run_coroutine_threadsafe(send_summary(p), loop),
            trigger=CronTrigger(day_of_week=next_day, hour=max(tg[0] - 1, 0), minute=tg[1]),
            id=f"summary_{poll['day']}", replace_existing=True
        )
    scheduler.add_job(lambda: asyncio.run_coroutine_threadsafe(save_data(), loop),
                      "interval", minutes=5, id="autosave", replace_existing=True)
    log.info("✅ Планировщик обновлён (Europe/Kaliningrad)")

# === KeepAlive server ===
async def handle(request):
    return web.Response(text="✅ Bot is alive and running!")

async def start_keepalive_server():
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("🌐 KeepAlive server started.")

# === Завершение ===
async def shutdown():
    log.info("🛑 Завершение работы...")
    try:
        await save_data()
        scheduler.shutdown(wait=False)
        await bot.close()
    except Exception:
        pass
    log.info("✅ Бот остановлен.")

# === Основной запуск ===
async def main():
    await load_data()
    await bot.delete_webhook(drop_pending_updates=True)
    schedule_polls()
    scheduler.start()
    await start_keepalive_server()
    log.info(f"🚀 Бот запущен {datetime.now(kaliningrad_tz):%Y-%m-%d %H:%M:%S %Z}")
    await bot.send_message(ADMIN_ID, "✅ Бот запущен и готов к работе!")
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_event_loop().add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown()))
        except NotImplementedError:
            pass
    await dp.start_polling()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        log.exception(f"Критическая ошибка при старте: {e}")
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
        raise






