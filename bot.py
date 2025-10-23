# -*- coding: utf-8 -*-
import os
import json
import asyncio
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone
from dotenv import load_dotenv
from aiohttp import web

# === Конфигурация ===
load_dotenv()
TOKEN = os.getenv("TG_BOT_TOKEN", "8196071953:AAF2wQ19VOkfPKvPfPaG0YoX33eVcWaC_yU")
CHAT_ID = int(os.getenv("TG_CHAT_ID", "-1002841862533"))
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "914344682"))
DATA_FILE = "bot_data.json"

bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot)

# === Временная зона Калининграда ===
kaliningrad_tz = timezone("Europe/Kaliningrad")
scheduler = AsyncIOScheduler(timezone=kaliningrad_tz)

active_polls = {}
stats = {}

# === Настройки опросов ===
polls_config = [
    {
        "day": "tue",
        "time_poll": "10:40",
        "time_game": "20:00",
        "question": "Сегодня собираемся на песчанке в 20:00?",
        "options": ["Да ✅", "Нет ❌", "Под вопросом ❔ (отвечу позже)"]
    },
    {
        "day": "thu",
        "time_poll": "10:40",
        "time_game": "20:00",
        "question": "Сегодня собираемся на песчанке в 20:00?",
        "options": ["Да ✅", "Нет ❌", "Под вопросом ❔ (отвечу позже)"]
    },
    {
        "day": "fri",
        "time_poll": "21:00",
        "time_game": "12:00",
        "question": "Завтра в 12:00 собираемся на песчанке?",
        "options": ["Да ✅", "Нет ❌"]
    }
]

# === Работа с данными ===
def save_data():
    """Сохранение данных в файл"""
    try:
        data = {"active_polls": active_polls, "stats": stats}
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"[💾 Данные сохранены {datetime.now(kaliningrad_tz):%H:%M:%S}]")
    except Exception as e:
        print(f"[⚠️ Ошибка при сохранении данных: {e}]")

def load_data():
    """Загрузка данных при старте"""
    global active_polls, stats
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                active_polls = data.get("active_polls", {})
                stats = data.get("stats", {})
            print("[✅ Данные успешно восстановлены из файла]")
        else:
            print("[ℹ️ Нет сохранённых данных — старт с нуля]")
    except Exception as e:
        print(f"[⚠️ Ошибка при загрузке данных: {e}]")

# === Служебные функции ===
async def reset_updates():
    try:
        updates = await bot.get_updates()
        if updates:
            last = updates[-1].update_id
            await bot.get_updates(offset=last + 1)
        print("[✅ Апдейты очищены]")
    except Exception as e:
        print(f"[⚠️ Не удалось очистить апдейты: {e}]")

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
            "votes": {}
        }
        save_data()
        print(f"[{datetime.now(kaliningrad_tz):%H:%M:%S}] 🗳 Опрос запущен: {poll['question']}")
    except Exception as e:
        print(f"[❌ Ошибка при запуске опроса: {e}]")

async def send_summary(poll):
    for poll_id, data in list(active_polls.items()):
        if data["poll"] == poll:
            votes = data["votes"]
            yes = [u for u, v in votes.items() if v == "Да ✅"]
            no = [u for u, v in votes.items() if v == "Нет ❌"]

            for user in yes:
                stats.setdefault(user, {"yes": 0, "no": 0})["yes"] += 1
            for user in no:
                stats.setdefault(user, {"yes": 0, "no": 0})["no"] += 1

            text = (
                f"📊 <b>Итог опроса:</b>\n\n"
                f"<b>{poll['question']}</b>\n\n"
                f"✅ Да ({len(yes)}): {', '.join(yes) or '—'}\n"
                f"❌ Нет ({len(no)}): {', '.join(no) or '—'}\n\n"
                f"<i>Всего проголосовало: {len(votes)}</i>"
            )
            await bot.send_message(CHAT_ID, text)
            del active_polls[poll_id]
            save_data()
            break

# === Исправленный планировщик ===
def schedule_polls():
    scheduler.remove_all_jobs()
    loop = asyncio.get_event_loop()  # главный event loop

    for poll in polls_config:
        tp = list(map(int, poll["time_poll"].split(":")))
        tg = list(map(int, poll["time_game"].split(":")))

        # Запуск опроса
        scheduler.add_job(
            lambda p=poll: asyncio.run_coroutine_threadsafe(start_poll(p), loop),
            trigger=CronTrigger(
                day_of_week=poll["day"],
                hour=tp[0],
                minute=tp[1],
                timezone=kaliningrad_tz
            ),
            id=f"poll_{poll['day']}",
            replace_existing=True
        )

        # Авто-сводка
        hour_before = max(tg[0] - 1, 0)
        next_day = "sat" if poll["day"] == "fri" else poll["day"]

        scheduler.add_job(
            lambda p=poll: asyncio.run_coroutine_threadsafe(send_summary(p), loop),
            trigger=CronTrigger(
                day_of_week=next_day,
                hour=hour_before,
                minute=tg[1],
                timezone=kaliningrad_tz
            ),
            id=f"summary_{poll['day']}",
            replace_existing=True
        )

    # Автосохранение каждые 5 минут
    scheduler.add_job(save_data, "interval", minutes=5, id="autosave", replace_existing=True)
    print("[✅ Планировщик заданий обновлён (Europe/Kaliningrad)]")

# === Обработчики ===
@dp.poll_answer_handler()
async def handle_poll_answer(poll_answer: types.PollAnswer):
    user = poll_answer.user.first_name
    option_ids = poll_answer.option_ids
    for poll_id, data in active_polls.items():
        if poll_answer.poll_id == poll_id:
            if not option_ids:
                data["votes"].pop(user, None)
            else:
                choice = data["poll"]["options"][option_ids[0]]
                data["votes"][user] = choice
            save_data()
            break

# === Команды ===
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return await message.reply("✅ Бот уже активен. Голосуй в опросах!")
    schedule_polls()
    if not scheduler.running:
        scheduler.start()
    await message.reply(
        "✅ Бот активирован.\nОпросы будут появляться по расписанию.\n"
        "Команды:\n"
        "• /poll [tue|thu|fri] — запустить вручную\n"
        "• /stats — статистика игроков"
    )
    print(f"[{datetime.now(kaliningrad_tz):%H:%M:%S}] /start от {message.from_user.id}")

@dp.message_handler(commands=["poll"])
async def manual_poll(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return await message.answer("⛔ Нет прав.")
    args = message.get_args().strip().lower()
    if args not in ["tue", "thu", "fri"]:
        return await message.answer("⚙️ Укажи день: /poll tue | /poll thu | /poll fri")
    poll = next((p for p in polls_config if p["day"] == args), None)
    if poll:
        await start_poll(poll)
        await message.answer(f"✅ Опрос на {args.upper()} запущен вручную!")

@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if not stats:
        return await message.answer("📊 Пока нет данных по посещаемости.")
    sorted_players = sorted(stats.items(), key=lambda x: x[1]["yes"], reverse=True)
    top = "\n".join(
        [f"{i+1}. {u} — ✅ {d['yes']} / ❌ {d['no']}"
         for i, (u, d) in enumerate(sorted_players[:5])]
    )
    await message.answer(f"📈 <b>ТОП-5 игроков по посещаемости:</b>\n\n{top}", parse_mode="HTML")

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
    print("[🌐 KeepAlive] Web server started on port", os.getenv("PORT", 8080))

# === Основной запуск ===
async def main():
    load_data()
    await reset_updates()
    schedule_polls()
    scheduler.start()
    await start_keepalive_server()
    print(f"[🚀 Бот запущен {datetime.now(kaliningrad_tz):%Y-%m-%d %H:%M:%S %Z}]")
    try:
        await bot.send_message(ADMIN_ID, "✅ Бот запущен и данные восстановлены!")
    except Exception as e:
        print(f"[warning] Не удалось уведомить админа: {e}")
    await dp.start_polling()

if __name__ == "__main__":
    asyncio.run(main())








