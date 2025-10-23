# -*- coding: utf-8 -*-
"""
Улучшенная версия bot.py для aiogram 2.25.1
Реализованы все команды:
 - /status, /stats, /nextpoll, /commands
 - Админ: /startpoll, /closepoll, /addplayer, /removeplayer
Усилена обработка ошибок, автосохранение, восстановление опросов.
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
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("bot")

# Быстрая проверка токена
if TOKEN == "YOUR_TOKEN_HERE" or not TOKEN:
    raise RuntimeError("❌ TG_BOT_TOKEN не задан в .env! Укажите корректный токен Telegram бота.")

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

# Состояние
active_polls = {}  # {poll_id: {"message_id":..., "poll": poll_dict, "votes": {...}, "active": True, "created_at": iso}}
stats = {}         # {name: yes_count}

# === Конфиг опросов (авто) ===
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

WEEKDAY_MAP = {
    "mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6
}

# === Helpers ===
def now_tz():
    return datetime.now(kaliningrad_tz)

def iso_now():
    return now_tz().isoformat()

def find_last_active_poll() -> Optional[Tuple[str, dict]]:
    """Возвращает (poll_id, data) последнего активного опроса или None."""
    if not active_polls:
        return None
    # Выберем по времени создания (created_at) если есть
    items = list(active_polls.items())
    items_sorted = sorted(items, key=lambda it: it[1].get("created_at", ""), reverse=True)
    for pid, data in items_sorted:
        if data.get("active"):
            return pid, data
    return None

def format_poll_votes(data: dict) -> str:
    votes = data.get("votes", {})
    if not votes:
        return "— Никто ещё не голосовал."
    lines = []
    for uid, v in votes.items():
        lines.append(f"{v.get('name')} — {v.get('answer')}")
    return "\n".join(lines)

# === Сохранение / загрузка данных ===
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

# === Безопасный запрос к Telegram (с ретраями) ===
async def safe_telegram_call(func, *args, retries=3, **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)
        except exceptions.RetryAfter as e:
            wait = e.timeout + 1
            log.warning(f"Flood control: спим {wait}s")
            await asyncio.sleep(wait)
        except exceptions.TelegramAPIError as e:
            log.error(f"TelegramAPIError (попытка {attempt}): {e}")
            if attempt == retries:
                try:
                    await bot.send_message(ADMIN_ID, f"⚠️ Ошибка TelegramAPI: {e}")
                except Exception:
                    pass
                return None
            await asyncio.sleep(2 * attempt)
        except Exception as e:
            log.exception(f"Ошибка при запросе к Telegram: {e}")
            if attempt == retries:
                return None
            await asyncio.sleep(2 * attempt)

# === Очистка апдейтов ===
async def reset_updates():
    try:
        await safe_telegram_call(bot.get_updates, offset=-1)
        log.info("✅ Очистка апдейтов завершена.")
    except Exception as e:
        log.warning(f"Ошибка при очистке апдейтов: {e}")

# === Создание опроса ===
async def start_poll(poll: dict, from_admin=False):
    try:
        options = poll.get("options") or []
        if not options or len(options) < 2:
            log.warning("Недостаточно вариантов для опроса.")
            return
        msg = await safe_telegram_call(bot.send_poll,
            chat_id=CHAT_ID,
            question=poll["question"],
            options=options,
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
            "active": True,
            "created_at": iso_now()
        }
        await save_data()
        # попробуем закрепить и написать анонс
        await safe_telegram_call(bot.pin_chat_message, chat_id=CHAT_ID, message_id=msg.message_id, disable_notification=True)
        await safe_telegram_call(bot.send_message, CHAT_ID, "📢 <b>Новый опрос!</b>\nПроголосуйте 👇")
        if from_admin:
            await safe_telegram_call(bot.send_message, ADMIN_ID, f"✅ Опрос запущен вручную: {poll['question']}")
        log.info(f"🗳 Опрос запущен: {poll['question']}")
    except Exception as e:
        log.exception(f"Ошибка при запуске опроса: {e}")

# === Восстановление опросов (очистка reply_markup для старых) ===
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
                # сообщение могло удалиться
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
    now = now_tz()
    if not (dtime(9, 0) <= now.time() <= dtime(19, 0)):
        return
    weekday = now.strftime("%a").lower()
    if weekday not in ["tue", "thu"]:
        return
    for data in active_polls.values():
        if not data.get("active"):
            continue
        yes_count = sum(1 for v in data["votes"].values() if v["answer"].startswith("Да"))
        if yes_count < 10:
            await safe_telegram_call(bot.send_message,
                CHAT_ID,
                f"⏰ Напоминание: пока только {yes_count} человек(а) ответили 'Да ✅'. Не забудьте проголосовать!"
            )
            log.info(f"🔔 Напоминание отправлено ({yes_count} 'Да')")

# === Итоги ===
async def send_summary(poll):
    # найдем соответствующий poll_id
    for poll_id, data in list(active_polls.items()):
        try:
            if data["poll"] == poll:
                data["active"] = False
                votes = data.get("votes", {})
                yes_users = [v["name"] for v in votes.values() if v["answer"].startswith("Да")]
                no_users = [v["name"] for v in votes.values() if v["answer"].startswith("Нет")]
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
                    if v["answer"].startswith("Да"):
                        stats[v["name"]] = stats.get(v["name"], 0) + 1
                del active_polls[poll_id]
                await save_data()
                log.info(f"📈 Итоги опроса отправлены: {poll['question']}")
                break
        except Exception as e:
            log.exception(f"Ошибка при отправке итога опроса: {e}")
            if poll_id in active_polls:
                del active_polls[poll_id]

# === Обработка ответов ===
@dp.poll_answer_handler()
async def handle_poll_answer(poll_answer: types.PollAnswer):
    try:
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
    except Exception as e:
        log.exception(f"Ошибка в handle_poll_answer: {e}")

# === Команды для всех пользователей ===
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await message.answer(
        "👋 Привет! Я бот для организации игр на песчанке.\n"
        "Напиши /commands чтобы увидеть список команд."
    )

@dp.message_handler(commands=["commands"])
async def cmd_commands(message: types.Message):
    text = (
        "Список доступных команд:\n\n"
        "Для всех пользователей:\n"
        "/status — показать текущий активный опрос и кто что ответил.\n"
        "/stats — статистика «Да ✅» по именам.\n"
        "/nextpoll — время следующего запланированного опроса.\n"
        "/commands — список всех команд и краткая справка.\n\n"
        "Для администратора (ADMIN_ID):\n"
        "/startpoll Вопрос | Вариант1 | Вариант2 | ... — запустить опрос вручную.\n"
        "  Пример: /startpoll Сегодня в 19:00? | Да | Нет | Может быть\n"
        "/closepoll — досрочно закрыть последний активный опрос и отправить итог.\n"
        "/addplayer Имя — вручную добавить игрока как «Да ✅».\n"
        "/removeplayer Имя — удалить вручную добавленного игрока.\n\n"
        "Авто-опросы запускаются по расписанию из polls_config."
    )
    await message.reply(text)

@dp.message_handler(commands=["status"])
async def cmd_status(message: types.Message):
    last = find_last_active_poll()
    if not last:
        return await message.reply("📭 В данный момент активных опросов нет.")
    _, data = last
    poll = data["poll"]
    text = (
        f"<b>{poll.get('question')}</b>\n\n"
        f"{format_poll_votes(data)}"
    )
    await message.reply(text)

@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if not stats:
        return await message.reply("📊 Пока нет статистики по 'Да'.")
    text = "\n".join([f"{name}: {count}" for name, count in sorted(stats.items(), key=lambda x: -x[1])])
    await message.reply(f"📈 Статистика «Да ✅» по именам:\n{text}")

@dp.message_handler(commands=["nextpoll"])
async def cmd_nextpoll(message: types.Message):
    nxt = compute_next_poll_datetime()
    if not nxt:
        return await message.reply("ℹ️ Нет запланированных опросов.")
    dt, cfg = nxt
    fmt = dt.strftime("%Y-%m-%d %H:%M %Z")
    await message.reply(f"Следующий запланированный опрос: <b>{cfg['question']}</b>\nКогда: {fmt}")

# === Админ команды ===
def is_admin(user_id: int) -> bool:
    return int(user_id) == int(ADMIN_ID)

@dp.message_handler(commands=["startpoll"])
async def cmd_startpoll(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.reply("❌ У вас нет прав на использование этой команды.")
    # Собираем аргументы
    text = message.get_args()
    if not text:
        return await message.reply("Использование:\n/startpoll Вопрос | Вариант1 | Вариант2 | ...")
    parts = [p.strip() for p in text.split("|") if p.strip()]
    if len(parts) < 3:
        return await message.reply("Нужно как минимум: Вопрос и 2 варианта. Пример:\n/startpoll Сегодня в 19:00? | Да | Нет")
    question = parts[0]
    options = parts[1:]
    if len(options) < 2 or len(options) > 10:
        return await message.reply("Telegram разрешает от 2 до 10 вариантов.")
    poll = {"day": "manual", "time_poll": now_tz().strftime("%H:%M"), "time_game": now_tz().strftime("%H:%M"),
            "question": question, "options": options}
    await start_poll(poll, from_admin=True)
    await message.reply("✅ Опрос создан вручную.")

@dp.message_handler(commands=["closepoll"])
async def cmd_closepoll(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.reply("❌ У вас нет прав на использование этой команды.")
    last = find_last_active_poll()
    if not last:
        return await message.reply("📭 Нет активных опросов для закрытия.")
    _, data = last
    try:
        await send_summary(data["poll"])
        await message.reply("✅ Опрос закрыт и итоги отправлены.")
    except Exception as e:
        log.exception(f"Ошибка при досрочном закрытии опроса: {e}")
        await message.reply("❌ Произошла ошибка при закрытии опроса.")

@dp.message_handler(commands=["addplayer"])
async def cmd_addplayer(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.reply("❌ У вас нет прав на использование этой команды.")
    name = message.get_args().strip()
    if not name:
        return await message.reply("Использование: /addplayer Имя")
    last = find_last_active_poll()
    if not last:
        return await message.reply("📭 Нет активных опросов для добавления игрока.")
    pid, data = last
    # Добавим как "Да ✅"
    data["votes"][f"admin_{name}"] = {"name": name, "answer": "Да ✅ (добавлен вручную)"}
    # не инкрементим stats сразу — инкрементируем при отправке итогов
    await save_data()
    await message.reply(f"✅ Игрок '{name}' добавлен как 'Да ✅'.")

@dp.message_handler(commands=["removeplayer"])
async def cmd_removeplayer(message: types.Message):
    if not is_admin(message.from_user.id):
        return await message.reply("❌ У вас нет прав на использование этой команды.")
    name = message.get_args().strip()
    if not name:
        return await message.reply("Использование: /removeplayer Имя")
    last = find_last_active_poll()
    if not last:
        return await message.reply("📭 Нет активных опросов.")
    pid, data = last
    removed = False
    for uid, v in list(data["votes"].items()):
        if v.get("name") == name:
            del data["votes"][uid]
            removed = True
    if removed:
        await save_data()
        await message.reply(f"✅ Игрок '{name}' удалён из голосов.")
    else:
        await message.reply(f"ℹ️ Игрок с именем '{name}' не найден среди голосовавших.")

# === Планировщик ===
def schedule_polls():
    scheduler.remove_all_jobs()
    loop = asyncio.get_event_loop()
    for poll in polls_config:
        try:
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
        except Exception as e:
            log.exception(f"Ошибка при планировании для {poll}: {e}")
    scheduler.add_job(lambda: asyncio.run_coroutine_threadsafe(remind_if_needed(), loop),
                      "interval", hours=2, id="reminder", replace_existing=True)
    scheduler.add_job(lambda: asyncio.run_coroutine_threadsafe(save_data(), loop),
                      "interval", minutes=5, id="autosave", replace_existing=True)
    log.info("✅ Планировщик обновлён (Europe/Kaliningrad)")

# === Compute next poll datetime helper ===
def compute_next_poll_datetime() -> Optional[Tuple[datetime, dict]]:
    now = now_tz()
    candidates = []
    for cfg in polls_config:
        day = cfg.get("day")
        t_str = cfg.get("time_poll", "00:00")
        try:
            hour, minute = map(int, t_str.split(":"))
        except Exception:
            hour, minute = 0, 0
        if day not in WEEKDAY_MAP:
            continue
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

# === Keep-alive server ===
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

# === Error handler (глобальный) ===
@dp.errors_handler()
async def global_error_handler(update, exception):
    try:
        log.exception(f"Глобальная ошибка: {exception} — update: {update}")
        # уведомим админа коротко
        await safe_telegram_call(bot.send_message, ADMIN_ID, f"⚠️ Исключение в боте: {exception}")
    except Exception:
        pass
    # возвращаем True — чтобы aiogram не логал лишний трейс
    return True

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
    try:
        asyncio.run(main())
    except Exception as e:
        log.exception(f"Критическая ошибка при старте: {e}")
        # Не забываем удалить lock при критическом падении
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
        raise




