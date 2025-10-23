# -*- coding: utf-8 -*-
import os
import json
import asyncio
import functools
import logging
import time
from datetime import datetime, time as dtime
import signal
import atexit

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
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("bot")

# === Проверка токена ===
if TOKEN == "YOUR_TOKEN_HERE" or not TOKEN:
    raise RuntimeError("❌ TG_BOT_TOKEN не задан в .env! Укажите корректный токен Telegram бота.")

# === Защита от дубликатов ===
if os.path.exists(LOCK_FILE):
    log.warning("⚠️ Обнаружен lock-файл. Возможно, бот уже запущен.")
    # ждём 30 секунд, вдруг предыдущий инстанс ещё завершается
    time.sleep(30)
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

active_polls = {}
stats = {}

# === Конфигурация опросов ===
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

# === Сохранение / загрузка ===
async def save_data():
    try:
        data = {"active_polls": active_polls, "stats": stats}
        def _write():
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        await asyncio.to_thread(_write)
        log.info("💾 Данные сохранены.")
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
            log.info("✅ Данные восстановлены из файла.")
        else:
            log.info("ℹ️ Нет сохранённых данных — старт с нуля.")
    except Exception as e:
        log.exception(f"Ошибка загрузки данных: {e}")

# === Безопасный запрос к Telegram (с повтором при сбое) ===
async def safe_telegram_call(func, *args, retries=3, **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)
        except exceptions.TelegramAPIError as e:
            if attempt == retries:
                log.error(f"TelegramAPIError (после {attempt} попыток): {e}")
                return None
            await asyncio.sleep(2 * attempt)
        except Exception as e:
            log.error(f"Ошибка при запросе к Telegram: {e}")
            if attempt == retries:
                return None
            await asyncio.sleep(2 * attempt)

# === Сброс апдейтов ===
async def reset_updates():
    try:
        await safe_telegram_call(bot.get_updates, offset=-1)
        log.info("✅ Очистка апдейтов завершена.")
    except Exception as e:
        log.warning(f"Ошибка при очистке апдейтов: {e}")

# === Создание опроса ===
async def start_poll(poll: dict, from_admin=False):
    try:
        msg = await safe_telegram_call(bot.send_poll,
            chat_id=CHAT_ID,
            question=poll["question"],
            options=poll["options"],
            is_anonymous=False,
            allows_multiple_answers=False
        )
        if not msg:
            log.error("Не удалось создать опрос.")
            return
        poll_id = msg.poll.id
        active_polls[poll_id] = {
            "message_id": msg.message_id,
            "poll": poll,
            "votes": {},
            "active": True
        }
        await save_data()
        await safe_telegram_call(bot.pin_chat_message, chat_id=CHAT_ID, message_id=msg.message_id, disable_notification=True)
        await safe_telegram_call(bot.send_message, CHAT_ID, "📢 <b>Новый опрос!</b>\nПроголосуйте, чтобы подтвердить участие 👇")
        if from_admin:
            await safe_telegram_call(bot.send_message, ADMIN_ID, f"✅ Опрос запущен вручную: {poll['question']}")
        log.info(f"🗳 Опрос запущен: {poll['question']}")
    except Exception as e:
        log.exception(f"Ошибка при запуске опроса: {e}")

# === Восстановление опросов ===
async def restore_active_polls():
    if not active_polls:
        return
    cleaned = []
    for poll_id, data in list(active_polls.items()):
        try:
            msg_id = data.get("message_id")
            try:
                await safe_telegram_call(bot.edit_message_reply_markup, chat_id=CHAT_ID, message_id=msg_id, reply_markup=None)
            except exceptions.BadRequest:
                del active_polls[poll_id]
                cleaned.append(poll_id)
        except Exception as e:
            log.warning(f"Ошибка восстановления {poll_id}: {e}")
            del active_polls[poll_id]
            cleaned.append(poll_id)
    if cleaned:
        log.info(f"🗑 Удалены устаревшие опросы: {cleaned}")
        await save_data()

# === Напоминания ===
async def remind_if_needed():
    now = datetime.now(kaliningrad_tz)
    if not (dtime(9, 0) <= now.time() <= dtime(19, 0)):
        return
    weekday = now.strftime("%a").lower()
    if weekday not in ["tue", "thu"]:
        return
    for data in active_polls.values():
        if not data.get("active"):
            continue
        yes_count = sum(1 for v in data["votes"].values() if v["answer"] == "Да ✅")
        if yes_count < 10:
            await safe_telegram_call(bot.send_message,
                CHAT_ID,
                f"⏰ Напоминание: пока только {yes_count} человек(а) ответили 'Да ✅'. Не забудьте проголосовать!"
            )
            log.info(f"🔔 Напоминание отправлено ({yes_count} 'Да')")

# === Итоги ===
async def send_summary(poll):
    for poll_id, data in list(active_polls.items()):
        if data["poll"] == poll:
            data["active"] = False
            votes = data.get("votes", {})
            yes_users = [v["name"] for v in votes.values() if v["answer"] == "Да ✅"]
            no_users = [v["name"] for v in votes.values() if v["answer"] == "Нет ❌"]
            total_yes = len(yes_users)
            poll_day = poll.get("day")
            if poll_day == "fri":
                status = "📊 Итог субботнего опроса:"
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
                f"{status}\n\n<i>Всего проголосовало: {len(votes)}</i>"
            )
            await safe_telegram_call(bot.send_message, CHAT_ID, text)
            for v in votes.values():
                if v["answer"] == "Да ✅":
                    stats[v["name"]] = stats.get(v["name"], 0) + 1
            del active_polls[poll_id]
            await save_data()
            log.info(f"📈 Итоги опроса отправлены: {poll['question']}")
            break

# === Обработка ответов ===
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

# === Планировщик ===
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
    scheduler.add_job(lambda: asyncio.run_coroutine_threadsafe(remind_if_needed(), loop),
                      "interval", hours=2, id="reminder", replace_existing=True)
    scheduler.add_job(lambda: asyncio.run_coroutine_threadsafe(save_data(), loop),
                      "interval", minutes=5, id="autosave", replace_existing=True)
    log.info("✅ Планировщик обновлён (Europe/Kaliningrad)")

# === Keep-alive ===
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
    except Exception:
        pass
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    try:
        await bot.close()
    except Exception:
        pass
    log.info("✅ Бот остановлен и данные сохранены.")

# === Основной запуск ===
async def main():
    await load_data()
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        log.info("🔁 Webhook удалён, polling активирован.")
    except Exception as e:
        log.warning(f"Не удалось удалить webhook: {e}")
    try:
        await reset_updates()
    except Exception as e:
        log.warning(f"Ошибка reset_updates: {e}")
    schedule_polls()
    scheduler.start()
    await restore_active_polls()
    await start_keepalive_server()
    log.info(f"🚀 Бот запущен {datetime.now(kaliningrad_tz):%Y-%m-%d %H:%M:%S %Z}")
    try:
        await bot.send_message(ADMIN_ID, "✅ Бот запущен и готов к работе!")
    except Exception:
        pass
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown()))
        except NotImplementedError:
            pass
    await dp.start_polling()

if __name__ == "__main__":
    asyncio.run(main())



