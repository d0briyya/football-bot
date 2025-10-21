# -*- coding: utf-8 -*-
import os
import json
import asyncio
import aiohttp
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode
from aiogram.utils import executor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

# === Конфигурация ===
load_dotenv()
TOKEN = os.getenv("TG_BOT_TOKEN", "8196071953:AAF2wQ19VOkfPKvPfPaG0YoX33eVcWaC_yU")
CHAT_ID = int(os.getenv("TG_CHAT_ID", "-1002841862533"))
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "914344682"))

bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot)
scheduler = AsyncIOScheduler()
active_polls = {}
STATS_FILE = "stats.json"

# === Загрузка статистики ===
def load_stats():
    if os.path.exists(STATS_FILE):
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_stats(data):
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

stats = load_stats()

# === Настройки опросов ===
polls_config = [
    {
        "day": "tue",
        "time_poll": "10:00",
        "time_game": "20:00",
        "question": "Сегодня собираемся на песчанке в 20:00?",
        "options": ["Да ✅", "Нет ❌", "Под вопросом 🤔, отвечу позже"]
    },
    {
        "day": "thu",
        "time_poll": "10:00",
        "time_game": "20:00",
        "question": "Сегодня собираемся на песчанке в 20:00?",
        "options": ["Да ✅", "Нет ❌", "Под вопросом 🤔, отвечу позже"]
    },
    {
        "day": "fri",
        "time_poll": "21:00",
        "time_game": "12:00",
        "question": "Завтра в 12:00 собираемся на песчанке?",
        "options": ["Да ✅", "Нет ❌"]
    }
]

# === Служебные функции ===
async def reset_updates():
    try:
        updates = await bot.get_updates()
        if updates:
            last = updates[-1].update_id
            await bot.get_updates(offset=last + 1)
        print("[✅ Апдейты очищены]")
    except Exception as e:
        print(f"[⚠️] Не удалось очистить апдейты: {e}")

async def start_poll(poll):
    try:
        msg = await bot.send_poll(
            chat_id=CHAT_ID,
            question=poll["question"],
            options=poll["options"],
            is_anonymous=False
        )
        active_polls[msg.poll.id] = {
            "message_id": msg.message_id,
            "poll": poll,
            "votes": {}
        }
        print(f"[{datetime.now():%H:%M:%S}] 🗳 Опрос запущен: {poll['question']}")
    except Exception as e:
        print(f"[❌ Ошибка при запуске опроса: {e}]")

@dp.poll_answer_handler()
async def handle_poll_answer(poll_answer: types.PollAnswer):
    user_id = str(poll_answer.user.id)
    username = poll_answer.user.first_name or "Без имени"
    poll_id = poll_answer.poll_id
    if poll_id in active_polls:
        option = active_polls[poll_id]["poll"]["options"][poll_answer.option_ids[0]]
        active_polls[poll_id]["votes"][user_id] = {
            "name": username,
            "answer": option
        }

async def send_summary(poll):
    poll_entry = next((v for v in active_polls.values() if v["poll"] == poll), None)
    if not poll_entry:
        print(f"[info] Нет активного опроса для '{poll['question']}'")
        return

    poll_id = poll_entry["poll"]
    votes = poll_entry["votes"]
    yes_votes = [v["name"] for v in votes.values() if "Да" in v["answer"]]

    text = f"<b>📊 Итоги опроса:</b>\n{poll['question']}\n\n" \
           f"✅ <b>Придут:</b> {len(yes_votes)}\n" \
           + "\n".join(f"— {name}" for name in yes_votes) if yes_votes else "— Никто не записался 😅"

    try:
        await bot.send_message(CHAT_ID, text, parse_mode="HTML")
        print(f"[{datetime.now():%H:%M:%S}] 📊 Сводка отправлена.")
    except Exception as e:
        print(f"[❌ Ошибка при отправке сводки: {e}")

    # Обновляем статистику
    for uid, v in votes.items():
        if "Да" in v["answer"]:
            stats[uid] = stats.get(uid, {"name": v["name"], "count": 0})
            stats[uid]["count"] += 1
    save_stats(stats)

async def keep_alive():
    url = os.getenv("KEEPALIVE_URL")
    if not url:
        return
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                await session.get(url)
            print(f"[{datetime.now():%H:%M:%S}] 🌐 Keep-alive ping sent")
        except Exception as e:
            print(f"[⚠️ KeepAlive Error]: {e}")
        await asyncio.sleep(59 * 60)

def schedule_polls():
    scheduler.remove_all_jobs()
    for poll in polls_config:
        tp = list(map(int, poll["time_poll"].split(":")))
        tg = list(map(int, poll["time_game"].split(":")))
        scheduler.add_job(lambda p=poll: asyncio.create_task(start_poll(p)),
                          trigger="cron", day_of_week=poll["day"],
                          hour=tp[0], minute=tp[1], id=f"poll_{poll['day']}", replace_existing=True)
        scheduler.add_job(lambda p=poll: asyncio.create_task(send_summary(p)),
                          trigger="cron", day_of_week=poll["day"],
                          hour=max(tg[0]-1, 0), minute=tg[1],
                          id=f"summary_{poll['day']}", replace_existing=True)
    print("[✅ Планировщик заданий обновлён]")

# === Команды ===
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("⛔ У вас нет прав для управления ботом.")
        return
    schedule_polls()
    if not scheduler.running:
        scheduler.start()
    await message.reply("✅ Бот активирован!\nПланировщик запущен и работает 24/7.\n"
                        "Команда /summary — показать активные опросы.")
    print(f"[{datetime.now():%H:%M:%S}] /start от {message.from_user.id}")

@dp.message_handler(commands=["poll"])
async def manual_poll(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("⛔ У вас нет прав для этой команды.")
        return

    args = message.get_args().strip().lower()
    if args not in ["tue", "thu", "fri"]:
        await message.reply("⚙️ Укажите день: /poll tue | /poll thu | /poll fri")
        return

    mapping = {"tue": "вторник", "thu": "четверг", "fri": "субботу"}
    poll = next((p for p in polls_config if p["day"] == args), None)
    await start_poll(poll)
    await message.answer(f"✅ Опрос на {mapping[args]} запущен вручную.")

@dp.message_handler(commands=["summary"])
async def cmd_summary(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("⛔ Нет прав.")
        return
    if not active_polls:
        await message.reply("📭 Нет активных опросов.")
        return
    for entry in active_polls.values():
        await send_summary(entry["poll"])

@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if not stats:
        await message.reply("📊 Пока нет данных для статистики.")
        return

    top = sorted(stats.values(), key=lambda x: x["count"], reverse=True)[:5]
    text = "<b>🏆 Топ-5 игроков по посещаемости:</b>\n\n"
    for i, p in enumerate(top, 1):
        text += f"{i}. {p['name']} — <b>{p['count']}</b> ⚽\n"
    await message.reply(text, parse_mode="HTML")

@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    await message.reply("⚙️ Команды:\n"
                        "/stats — показать топ-5 по посещаемости\n"
                        "/start — запустить бота (только для админа)\n"
                        "/poll tue|thu|fri — запустить опрос вручную (только админ)\n"
                        "/summary — сводка опросов (только админ)")

# === Основной запуск ===
async def on_startup(_):
    await reset_updates()
    schedule_polls()
    scheduler.start()
    asyncio.create_task(keep_alive())
    print("[🚀 Бот запущен и работает автономно]")
    try:
        await bot.send_message(ADMIN_ID, "✅ Бот запущен и планировщик активирован!")
    except:
        pass

if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)




