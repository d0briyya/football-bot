# -*- coding: utf-8 -*-
import os
import asyncio
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

# === Настройки опросов ===
polls_config = [
    {
        "day": "tue",
        "time_poll": "10:00",
        "time_game": "20:00",
        "question": "Собираемся на футбол во вторник?",
        "options": ["Да", "Нет", "Под ?, в 18:30 отвечу точно"]
    },
    {
        "day": "thu",
        "time_poll": "10:00",
        "time_game": "20:00",
        "question": "Собираемся на футбол в четверг?",
        "options": ["Да", "Нет", "Под ?, в 18:30 отвечу точно"]
    },
    {
        "day": "sat",
        "time_poll": "21:00",
        "time_game": "12:00",
        "question": "Собираемся на футбол в субботу?",
        "options": ["Да", "Нет"]
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
        print(f"[warning] Не удалось очистить апдейты: {e}")

async def start_poll(poll):
    try:
        msg = await bot.send_poll(
            chat_id=CHAT_ID,
            question=poll["question"],
            options=poll["options"],
            is_anonymous=False,
            allows_multiple_answers=False
        )
        active_polls[msg.message_id] = poll
        print(f"[{datetime.now():%H:%M:%S}] 🗳 Опрос запущен: {poll['question']}")
    except Exception as e:
        print(f"[error] Ошибка при запуске опроса: {e}")

async def send_summary(poll):
    poll_id = next((m for m, p in active_polls.items() if p == poll), None)
    if not poll_id:
        print(f"[info] Нет активного опроса для '{poll['question']}'")
        return
    try:
        await bot.forward_message(CHAT_ID, CHAT_ID, poll_id)
        print(f"[{datetime.now():%H:%M:%S}] 📊 Сводка отправлена: {poll['question']}")
    except Exception as e:
        print(f"[error] Ошибка при отправке сводки: {e}")

def schedule_polls():
    scheduler.remove_all_jobs()
    for poll in polls_config:
        tp = list(map(int, poll["time_poll"].split(":")))
        tg = list(map(int, poll["time_game"].split(":")))
        scheduler.add_job(lambda p=poll: asyncio.create_task(start_poll(p)),
                          trigger="cron", day_of_week=poll["day"],
                          hour=tp[0], minute=tp[1],
                          id=f"poll_{poll['day']}", replace_existing=True)
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
    await message.reply("Бот активирован ✅\n"
                        "Планировщик запущен — опросы будут появляться в назначенные дни.\n"
                        "Команда /summary — показать текущие опросы вручную.")
    print(f"[{datetime.now():%H:%M:%S}] /start от {message.from_user.id}")

@dp.message_handler(commands=["summary"])
async def cmd_summary(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("⛔ Нет прав.")
        return
    if not active_polls:
        await message.reply("Нет активных опросов сейчас.")
        return
    await message.reply("📊 Активные опросы:")
    for msg_id in list(active_polls.keys()):
        try:
            await bot.forward_message(message.chat.id, CHAT_ID, msg_id)
        except Exception as e:
            print(f"[warning] Не удалось переслать msg_id={msg_id}: {e}")
            active_polls.pop(msg_id, None)

# === Основной запуск ===
async def main():
    await reset_updates()
    schedule_polls()
    scheduler.start()
    print("[🚀 Бот запущен и работает автономно]")
    try:
        await bot.send_message(ADMIN_ID, "✅ Бот запущен и планировщик активирован!")
    except Exception as e:
        print(f"[warning] Не удалось уведомить админа: {e}")
    await dp.start_polling()

if __name__ == "__main__":
    asyncio.run(main())
