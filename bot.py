# -*- coding: utf-8 -*-
import os
import json
import asyncio
from datetime import datetime, time as dtime
from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode
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
DATA_FILE = "bot_data.json"

bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot)

kaliningrad_tz = timezone("Europe/Kaliningrad")
scheduler = AsyncIOScheduler(timezone=kaliningrad_tz)

active_polls = {}
stats = {}

# === Настройки опросов ===
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

# === Работа с данными ===
def save_data():
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump({"active_polls": active_polls, "stats": stats}, f, ensure_ascii=False, indent=2)
        print(f"[💾 Данные сохранены {datetime.now(kaliningrad_tz):%H:%M:%S}]")
    except Exception as e:
        print(f"[⚠️ Ошибка сохранения: {e}]")

def load_data():
    global active_polls, stats
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            active_polls = data.get("active_polls", {})
            stats = data.get("stats", {})
            print("[✅ Данные восстановлены]")
        else:
            print("[ℹ️ Нет сохранённых данных — старт с нуля]")
    except Exception as e:
        print(f"[⚠️ Ошибка загрузки: {e}]")

# === Служебные функции ===
async def reset_updates():
    try:
        updates = await bot.get_updates()
        if updates:
            await bot.get_updates(offset=updates[-1].update_id + 1)
        print("[✅ Апдейты очищены]")
    except Exception as e:
        print(f"[⚠️ Ошибка очистки апдейтов: {e}]")

# === Создание опроса ===
async def start_poll(poll):
    try:
        msg = await bot.send_poll(
            chat_id=CHAT_ID,
            question=poll["question"],
            options=poll["options"],
            is_anonymous=False,
            allows_multiple_answers=False
        )
        active_polls[msg.poll.id] = {
            "message_id": msg.message_id,
            "poll": poll,
            "votes": {},
            "active": True
        }
        save_data()

        await bot.pin_chat_message(chat_id=CHAT_ID, message_id=msg.message_id, disable_notification=True)
        await bot.send_message(CHAT_ID, "📢 <b>Новый опрос!</b>\nПроголосуйте, чтобы подтвердить участие 👇")
        print(f"[{datetime.now(kaliningrad_tz):%H:%M:%S}] 🗳 Опрос запущен: {poll['question']}")
    except Exception as e:
        print(f"[❌ Ошибка запуска опроса: {e}]")

# === Напоминания (вт/чт 9–19, каждые 2 часа, если 'Да' < 10) ===
async def remind_if_needed():
    now = datetime.now(kaliningrad_tz)
    if not (dtime(9, 0) <= now.time() <= dtime(19, 0)):
        return
    weekday = now.strftime("%a").lower()
    if weekday not in ["tue", "thu"]:
        return

    for poll_id, data in active_polls.items():
        poll_day = data["poll"]["day"]
        if not data.get("active", False):
            continue
        if poll_day not in ["tue", "thu"]:
            continue

        votes = data.get("votes", {})
        yes_count = sum(1 for v in votes.values() if v["answer"] == "Да ✅")
        if yes_count < 10:
            await bot.send_message(
                CHAT_ID,
                f"⏰ Напоминание: пока только {yes_count} человек(а) ответили 'Да ✅'. "
                "Не забудьте проголосовать, чтобы подтвердить участие!"
            )
            print(f"[🔔 Напоминание отправлено ({yes_count} 'Да')]")

# === Итоги ===
async def send_summary(poll):
    for poll_id, data in list(active_polls.items()):
        if data["poll"] == poll:
            data["active"] = False
            votes = data["votes"]

            yes_users = [f"{v['name']} (ID:{uid})" for uid, v in votes.items() if v["answer"] == "Да ✅"]
            no_users = [f"{v['name']} (ID:{uid})" for uid, v in votes.items() if v["answer"] == "Нет ❌"]

            try:
                await bot.unpin_chat_message(chat_id=CHAT_ID, message_id=data["message_id"])
            except:
                pass

            total_yes = len(yes_users)
            status = (
                "⚠️ Сегодня на футбол <b>не собираемся</b> — участников меньше 10."
                if total_yes < 10 else
                "✅ Сегодня <b>собираемся на песчанке</b>! ⚽"
            )

            text = (
                f"📊 <b>Итог опроса:</b>\n\n"
                f"<b>{poll['question']}</b>\n\n"
                f"✅ Да ({total_yes}): {', '.join(yes_users) or '—'}\n"
                f"❌ Нет ({len(no_users)}): {', '.join(no_users) or '—'}\n\n"
                f"{status}\n\n"
                f"<i>Всего проголосовало: {len(votes)}</i>"
            )
            await bot.send_message(CHAT_ID, text)
            del active_polls[poll_id]
            save_data()
            break

# === Добавление игрока вручную (только админ) ===
@dp.message_handler(commands=["addplayer"])
async def cmd_addplayer(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return await message.reply("⛔ Только администратор может добавлять игроков.")

    name = message.get_args().strip()
    if not name:
        return await message.reply("✍️ Укажи имя игрока: /addplayer Вася")

    if not active_polls:
        return await message.reply("⚠️ Сейчас нет активного опроса.")

    poll_id, data = list(active_polls.items())[-1]
    key = f"manual_{len([k for k in data['votes'] if str(k).startswith('manual_')]) + 1}"
    data["votes"][key] = {"name": name, "answer": "Да ✅"}
    save_data()

    yes_users = [v["name"] for v in data["votes"].values() if v["answer"] == "Да ✅"]
    text = (
        f"✅ <b>{name}</b> добавлен как 'Да ✅' вручную.\n\n"
        f"<b>Обновлённый список участников (Да ✅):</b>\n"
        f"{', '.join(yes_users) or '—'}"
    )
    await bot.send_message(CHAT_ID, text, parse_mode="HTML")

# === Удаление игрока вручную (только админ) ===
@dp.message_handler(commands=["removeplayer"])
async def cmd_removeplayer(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return await message.reply("⛔ Только администратор может удалять игроков.")

    name = message.get_args().strip()
    if not name:
        return await message.reply("✍️ Укажи имя игрока: /removeplayer Вася")

    if not active_polls:
        return await message.reply("⚠️ Сейчас нет активного опроса.")

    poll_id, data = list(active_polls.items())[-1]
    removed = False
    for key in list(data["votes"].keys()):
        if str(key).startswith("manual_") and data["votes"][key]["name"].lower() == name.lower():
            del data["votes"][key]
            removed = True
            break

    if not removed:
        return await message.reply(f"⚠️ Игрок <b>{name}</b> не найден среди вручную добавленных.", parse_mode="HTML")

    save_data()

    yes_users = [v["name"] for v in data["votes"].values() if v["answer"] == "Да ✅"]
    text = (
        f"❌ <b>{name}</b> удалён из списка участников.\n\n"
        f"<b>Обновлённый список (Да ✅):</b>\n"
        f"{', '.join(yes_users) or '—'}"
    )
    await bot.send_message(CHAT_ID, text, parse_mode="HTML")

# === Список команд ===
@dp.message_handler(commands=["commands"])
async def cmd_help(message: types.Message):
    text = (
        "📋 <b>Доступные команды:</b>\n\n"
        "🗳 <b>Автоопросы:</b>\n"
        " • Вторник и четверг — 9:00 (игра в 20:00)\n"
        " • Пятница — 21:00 (игра в субботу 12:00)\n\n"
        "🔔 <b>Напоминания:</b>\n"
        " • Вт/Чт каждые 2 часа с 9:00 до 19:00, если 'Да ✅' < 10\n\n"
        "⚙️ <b>Команды администратора:</b>\n"
        " • /addplayer Имя — добавить игрока вручную (Да ✅)\n"
        " • /removeplayer Имя — удалить игрока из списка\n\n"
        "👁 <b>Все участники видят обновлённые списки в чате.</b>\n"
    )
    await message.reply(text, parse_mode="HTML")

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
    scheduler.add_job(save_data, "interval", minutes=5, id="autosave", replace_existing=True)
    print("[✅ Планировщик обновлён (Europe/Kaliningрад)]")

# === Обработка голосов ===
@dp.poll_answer_handler()
async def handle_poll_answer(poll_answer: types.PollAnswer):
    uid = poll_answer.user.id
    uname = poll_answer.user.first_name
    option_ids = poll_answer.option_ids
    for poll_id, data in active_polls.items():
        if poll_answer.poll_id == poll_id:
            if not option_ids:
                data["votes"].pop(uid, None)
            else:
                answer = data["poll"]["options"][option_ids[0]]
                data["votes"][uid] = {"name": uname, "answer": answer}
            save_data()
            break

# === KEEP-ALIVE ===
async def handle(request):
    return web.Response(text="✅ Bot is alive and running!")

async def start_keepalive_server():
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 8080)))
    await site.start()
    print("[🌐 KeepAlive server started]")

# === Основной запуск ===
async def main():
    load_data()
    await reset_updates()
    schedule_polls()
    scheduler.start()
    await start_keepalive_server()
    print(f"[🚀 Бот запущен {datetime.now(kaliningrad_tz):%Y-%m-%d %H:%M:%S %Z}]")
    await bot.send_message(ADMIN_ID, "✅ Бот запущен и данные восстановлены!")
    await dp.start_polling()

if __name__ == "__main__":
    asyncio.run(main())









