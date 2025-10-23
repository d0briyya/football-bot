# -*- coding: utf-8 -*-
import os
import json
import asyncio
from datetime import datetime, time as dtime
import signal

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

bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot)

kaliningrad_tz = timezone("Europe/Kaliningrad")
scheduler = AsyncIOScheduler(timezone=kaliningrad_tz)

# структура:
# active_polls: { poll_id(str): { "message_id": int, "poll": {...}, "votes": {uid_or_manual: {"name": str, "answer": str}}, "active": bool } }
active_polls = {}
stats = {}

# === Настройки опросов по расписанию ===
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

# === Сохранение / загрузка данных ===
async def save_data():
    try:
        data = {"active_polls": active_polls, "stats": stats}
        def _write():
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        await asyncio.to_thread(_write)
        print(f"[💾 Данные сохранены {datetime.now(kaliningrad_tz):%H:%M:%S}]")
    except Exception as e:
        print(f"[⚠️ Ошибка сохранения: {e}]")

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
            print("[✅ Данные восстановлены из файла]")
        else:
            print("[ℹ️ Нет сохранённых данных — старт с нуля]")
    except Exception as e:
        print(f"[⚠️ Ошибка загрузки данных: {e}]")

# === Сброс апдейтов ===
async def reset_updates():
    try:
        updates = await bot.get_updates()
        if updates:
            await bot.get_updates(offset=updates[-1].update_id + 1)
        print("[✅ Апдейты очищены]")
    except Exception as e:
        print(f"[⚠️ Ошибка очистки апдейтов: {e}]")

# === Создание опроса ===
async def start_poll(poll: dict, from_admin=False):
    try:
        msg = await bot.send_poll(
            chat_id=CHAT_ID,
            question=poll["question"],
            options=poll["options"],
            is_anonymous=False,
            allows_multiple_answers=False
        )
        poll_id = msg.poll.id
        active_polls[poll_id] = {
            "message_id": msg.message_id,
            "poll": poll,
            "votes": {},
            "active": True
        }
        await save_data()

        try:
            await bot.pin_chat_message(chat_id=CHAT_ID, message_id=msg.message_id, disable_notification=True)
        except exceptions.TelegramAPIError:
            pass

        await bot.send_message(CHAT_ID, "📢 <b>Новый опрос!</b>\nПроголосуйте, чтобы подтвердить участие 👇")
        if from_admin:
            await bot.send_message(ADMIN_ID, f"✅ Опрос запущен вручную: {poll['question']}")
        print(f"[🗳 Опрос запущен: {poll['question']}]")
    except Exception as e:
        print(f"[❌ Ошибка запуска опроса: {e}]")

# === Восстановление активных опросов ===
async def restore_active_polls():
    if not active_polls:
        return
    restored = []
    removed = []
    for poll_id, data in list(active_polls.items()):
        try:
            msg_id = data.get("message_id")
            try:
                await bot.edit_message_reply_markup(chat_id=CHAT_ID, message_id=msg_id, reply_markup=None)
                restored.append((poll_id, msg_id))
            except exceptions.BadRequest as br:
                err_text = str(br)
                if "message to edit not found" in err_text or "message can't be edited" in err_text or "message is not modified" in err_text:
                    del active_polls[poll_id]
                    removed.append((poll_id, "message not found/edited"))
                else:
                    restored.append((poll_id, msg_id))
            except exceptions.TelegramAPIError:
                del active_polls[poll_id]
                removed.append((poll_id, "telegram api error"))
        except Exception as e:
            print(f"[⚠️ Ошибка при восстановлении {poll_id}: {e}]")
            if poll_id in active_polls:
                del active_polls[poll_id]
            removed.append((poll_id, "unknown error"))
    if restored:
        print(f"[🔁 Восстановлены опросы: {restored}]")
    if removed:
        print(f"[🗑 Удалены/не найдены опросы: {removed}]")
    await save_data()

# === Напоминания ===
async def remind_if_needed():
    now = datetime.now(kaliningrad_tz)
    if not (dtime(9, 0) <= now.time() <= dtime(19, 0)):
        return
    weekday = now.strftime("%a").lower()
    if weekday not in ["tue", "thu"]:
        return
    for poll_id, data in active_polls.items():
        if not data.get("active", False):
            continue
        poll_day = data.get("poll", {}).get("day")
        if poll_day and poll_day != weekday:
            continue
        yes_count = sum(1 for v in data["votes"].values() if v["answer"] == "Да ✅")
        if yes_count < 10:
            try:
                await bot.send_message(
                    CHAT_ID,
                    f"⏰ Напоминание: пока только {yes_count} человек(а) ответили 'Да ✅'. Не забудьте проголосовать!"
                )
                print(f"[🔔 Напоминание отправлено ({yes_count} 'Да')]")
            except Exception:
                print("[⚠️ Не удалось отправить напоминание]")

# === Итоги опроса ===
async def send_summary(poll):
    for poll_id, data in list(active_polls.items()):
        if data["poll"] == poll:
            data["active"] = False
            votes = data.get("votes", {})
            yes_users = [f"{v['name']} (ID:{uid})" for uid, v in votes.items() if v["answer"] == "Да ✅"]
            no_users = [f"{v['name']} (ID:{uid})" for uid, v in votes.items() if v["answer"] == "Нет ❌"]

            try:
                await bot.unpin_chat_message(chat_id=CHAT_ID, message_id=data["message_id"])
            except exceptions.TelegramAPIError:
                pass

            total_yes = len(yes_users)
            poll_day = poll.get("day")

            if poll_day == "fri":
                text = (
                    "📊 <b>Итог опроса (суббота):</b>\n\n"
                    f"<b>{poll['question']}</b>\n\n"
                    f"✅ Да ({total_yes}): {', '.join(yes_users) or '—'}\n"
                    f"❌ Нет ({len(no_users)}): {', '.join(no_users)}\n\n"
                    f"ℹ️ За 'Да' проголосовало <b>{total_yes}</b> человек(а).\n"
                    f"Решайте сами, стоит ли идти — по субботам многие ходят без голосовалки.\n"
                    f"<i>Опрос создан лишь для удобства и координации.</i>\n\n"
                    f"<i>Всего проголосовало: {len(votes)}</i>"
                )
            else:
                status = (
                    "⚠️ Сегодня на футбол <b>не собираемся</b> — участников меньше 10."
                    if total_yes < 10 else
                    "✅ Сегодня <b>собираемся на песчанке</b>! ⚽"
                )
                text = (
                    "📊 <b>Итог опроса:</b>\n\n"
                    f"<b>{poll['question']}</b>\n\n"
                    f"✅ Да ({total_yes}): {', '.join(yes_users) or '—'}\n"
                    f"❌ Нет ({len(no_users)}): {', '.join(no_users)}\n\n"
                    f"{status}\n\n"
                    f"<i>Всего проголосовало: {len(votes)}</i>"
                )

            try:
                await bot.send_message(CHAT_ID, text)
            except exceptions.TelegramAPIError:
                pass

            for v in votes.values():
                if v["answer"] == "Да ✅":
                    stats[v["name"]] = stats.get(v["name"], 0) + 1

            del active_polls[poll_id]
            await save_data()
            print(f"[📈 Итоги опроса отправлены: {poll['question']}]")
            break

# === Остальные команды ===
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
# (остальные /addplayer, /removeplayer, /status, /stats, /nextpoll, /startpoll, /closepoll, /commands остаются без изменений — как у тебя сейчас)

# === Планировщик ===
def schedule_polls():
    scheduler.remove_all_jobs()
    loop = asyncio.get_event_loop()
    for poll in polls_config:
        tp = list(map(int, poll["time_poll"].split(":")))
        tg = list(map(int, poll["time_game"].split(":")))
        scheduler.add_job(
            lambda p=poll: asyncio.run_coroutine_threadsafe(start_poll(p), loop),
            trigger=CronTrigger(day_of_week=poll["day"], hour=tp[0], minute=tp[1], timezone=kaliningrad_tz),
            id=f"poll_{poll['day']}", replace_existing=True
        )
        hour_before = max(tg[0] - 1, 0)
        next_day = "sat" if poll["day"] == "fri" else poll["day"]
        scheduler.add_job(
            lambda p=poll: asyncio.run_coroutine_threadsafe(send_summary(p), loop),
            trigger=CronTrigger(day_of_week=next_day, hour=hour_before, minute=tg[1], timezone=kaliningrad_tz),
            id=f"summary_{poll['day']}", replace_existing=True
        )
    scheduler.add_job(lambda: asyncio.run_coroutine_threadsafe(remind_if_needed(), loop),
                      "interval", hours=2, id="reminder", replace_existing=True)
    scheduler.add_job(lambda: asyncio.run_coroutine_threadsafe(save_data(), loop),
                      "interval", minutes=5, id="autosave", replace_existing=True)
    print("[✅ Планировщик обновлён (Europe/Kaliningrad)]")

# === KeepAlive HTTP ===
async def handle(request):
    return web.Response(text="✅ Bot is alive and running!")

async def start_keepalive_server():
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print("[🌐 KeepAlive server started]")

# === Завершение ===
async def shutdown():
    print("[🛑 Завершение работы...]")
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
    print("[✅ Данные сохранены, бот остановлен]")

# === Запуск ===
async def main():
    await load_data()
    await reset_updates()
    schedule_polls()
    scheduler.start()
    await restore_active_polls()
    await start_keepalive_server()
    print(f"[🚀 Бот запущен {datetime.now(kaliningrad_tz):%Y-%m-%d %H:%M:%S %Z}]")
    try:
        await bot.send_message(ADMIN_ID, "✅ Бот запущен и данные восстановлены!")
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
