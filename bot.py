# -*- coding: utf-8 -*-
import os
import asyncio
import json
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode
from aiogram.utils import executor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
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
player_stats = {}

# === Функции работы со статистикой ===
def load_stats():
    global player_stats
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE, "r", encoding="utf-8") as f:
                player_stats = json.load(f)
            print(f"[📊] Загружено {len(player_stats)} записей статистики.")
        except Exception as e:
            print(f"[warning] Ошибка загрузки статистики: {e}")
            player_stats = {}
    else:
        player_stats = {}

def save_stats():
    try:
        with open(STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(player_stats, f, ensure_ascii=False, indent=2)
        print("[💾] Статистика сохранена в stats.json")
    except Exception as e:
        print(f"[error] Ошибка сохранения статистики: {e}")

# === Очистка старых апдейтов ===
async def reset_updates():
    try:
        updates = await bot.get_updates()
        if updates:
            last = updates[-1].update_id
            await bot.get_updates(offset=last + 1)
        print("Updates were skipped successfully.")
    except Exception as e:
        print(f"[warning] Не удалось очистить апдейты: {e}")

# === Настройки опросов ===
polls_config = [
    {
        "day": "tue",
        "time_poll": "10:00",
        "time_game": "20:00",
        "question": "Сегодня собираемся на песчанке в 20:00?",
        "options": ["Да ✅", "Нет ❌", "Под вопросом 🤔"]
    },
    {
        "day": "thu",
        "time_poll": "10:00",
        "time_game": "20:00",
        "question": "Сегодня собираемся на песчанке в 20:00?",
        "options": ["Да ✅", "Нет ❌", "Под вопросом 🤔"]
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
async def start_poll(poll):
    try:
        msg = await bot.send_poll(
            chat_id=CHAT_ID,
            question=poll["question"],
            options=poll["options"],
            is_anonymous=False,
            allows_multiple_answers=False
        )
        active_polls[msg.message_id] = {"poll": poll, "answers": {}}
        print(f"[{datetime.now():%H:%M:%S}] 🗳 Опрос запущен: {poll['question']}")
        await bot.pin_chat_message(CHAT_ID, msg.message_id, disable_notification=False)
    except Exception as e:
        print(f"[error] Ошибка при запуске опроса: {e}")

async def send_summary(poll):
    poll_id = next((m for m, p in active_polls.items() if p["poll"] == poll), None)
    if not poll_id:
        return
    try:
        poll_data = active_polls[poll_id]
        answers = poll_data["answers"]
        yes_voters = [u for u, ans in answers.items() if ans == "Да ✅"]
        no_voters = [u for u, ans in answers.items() if ans == "Нет ❌"]
        maybe_voters = [u for u, ans in answers.items() if ans == "Под вопросом 🤔"]

        summary_text = f"📊 <b>Сводка по опросу:</b>\n{poll['question']}\n\n"
        summary_text += f"✅ <b>Да ({len(yes_voters)}):</b> {', '.join(yes_voters) or '—'}\n"
        summary_text += f"❌ <b>Нет ({len(no_voters)}):</b> {', '.join(no_voters) or '—'}\n"
        summary_text += f"🤔 <b>Под вопросом ({len(maybe_voters)}):</b> {', '.join(maybe_voters) or '—'}"

        await bot.send_message(CHAT_ID, summary_text, parse_mode="HTML")

        # обновляем статистику
        for user in yes_voters:
            player_stats[user] = player_stats.get(user, 0) + 1
        save_stats()  # сохраняем статистику после каждого завершения опроса

        del active_polls[poll_id]
        print(f"[{datetime.now():%H:%M:%S}] 📊 Сводка отправлена и статистика обновлена.")
    except Exception as e:
        print(f"[error] Ошибка при отправке сводки: {e}")

# === Обработка ответов пользователей ===
@dp.poll_answer_handler()
async def handle_poll_answer(poll_answer: types.PollAnswer):
    user_name = poll_answer.user.full_name
    answer = poll_answer.option_ids[0]
    for poll_id, data in active_polls.items():
        if poll_answer.poll_id == data.get("poll_id", poll_id):
            options = data["poll"]["options"]
            selected = options[answer]
            data["answers"][user_name] = selected
            print(f"[vote] {user_name} -> {selected}")
            break

# === Команды ===
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    schedule_polls()
    if not scheduler.running:
        scheduler.start()
    await message.reply("✅ Бот активирован и работает 24/7!\n"
                        "Опросы создаются автоматически в назначенные дни.\n"
                        "Команда /poll tue|thu|fri — запустить вручную.\n"
                        "Команда /stats — показать статистику игроков.")

@dp.message_handler(commands=["poll"])
async def manual_poll(message: types.Message):
    args = message.get_args().strip().lower()
    poll = next((p for p in polls_config if p["day"] == args), None)
    if not poll:
        await message.reply("⚙️ Укажи день: /poll tue | /poll thu | /poll fri")
        return
    await start_poll(poll)
    await message.reply(f"✅ Опрос на {args.upper()} запущен вручную.")

@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if not player_stats:
        await message.reply("📉 Пока нет статистики.")
        return
    sorted_players = sorted(player_stats.items(), key=lambda x: x[1], reverse=True)
    top = "\n".join([f"{i+1}. {u} — {c} ⚽" for i, (u, c) in enumerate(sorted_players[:5])])
    await message.reply(f"🏆 <b>Топ-5 по посещаемости:</b>\n{top}", parse_mode="HTML")

# === Планировщик опросов ===
def schedule_polls():
    scheduler.remove_all_jobs()
    for poll in polls_config:
        hour, minute = map(int, poll["time_poll"].split(":"))
        scheduler.add_job(lambda p=poll: asyncio.run_coroutine_threadsafe(start_poll(p), asyncio.get_event_loop()),
                          trigger="cron", day_of_week=poll["day"], hour=hour, minute=minute)
        tg_hour, tg_minute = map(int, poll["time_game"].split(":"))
        scheduler.add_job(lambda p=poll: asyncio.run_coroutine_threadsafe(send_summary(p), asyncio.get_event_loop()),
                          trigger="cron", day_of_week=poll["day"], hour=max(tg_hour - 1, 0), minute=tg_minute)
    # Keep-alive ping каждые 59 минут
    scheduler.add_job(lambda: asyncio.run_coroutine_threadsafe(bot.get_me(), asyncio.get_event_loop()),
                      trigger=IntervalTrigger(minutes=59))
    print("[✅ Планировщик заданий обновлён]")

# === Запуск ===
async def on_startup(dp):
    load_stats()
    await reset_updates()
    schedule_polls()
    scheduler.start()
    print("[🚀 Бот запущен и работает автономно]")
    try:
        await bot.send_message(ADMIN_ID, "✅ Бот запущен и планировщик активирован!")
    except:
        pass

if __name__ == "__main__":
    print("[start] Bot is starting...")
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)



