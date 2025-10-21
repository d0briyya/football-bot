# -*- coding: utf-8 -*-
import os
import json
import asyncio
import aiohttp
from datetime import datetime
from collections import defaultdict
from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode, PollAnswer
from aiogram.utils import executor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

# === Загрузка конфигов ===
load_dotenv()
TOKEN = os.getenv("TG_BOT_TOKEN", "")
CHAT_ID = int(os.getenv("TG_CHAT_ID", "0"))
ADMIN_ID = int(os.getenv("TG_ADMIN_ID", "0"))

bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(bot)
scheduler = AsyncIOScheduler()

# === Персистентные файлы ===
STATS_FILE = "stats.json"

# === Временные структуры ===
# active_polls_by_msg: message_id -> poll_config
active_polls_by_msg = {}         # message_id -> poll_config dict
active_polls_by_pollid = {}      # poll_id -> message_id
poll_votes = defaultdict(dict)   # poll_id -> { user_id: option_index }
user_cooldowns = {}              # user_id -> datetime for stats button cooldown

# === Утилиты для статистики ===
def load_stats():
    if not os.path.exists(STATS_FILE):
        return {}
    try:
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_stats(stats):
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

def add_attendance(user_repr):
    stats = load_stats()
    counts = stats.get("attendance", {})
    counts[user_repr] = counts.get(user_repr, 0) + 1
    stats["attendance"] = counts
    save_stats(stats)

# === Клавиатура (под опрос) ===
def stats_keyboard():
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("📊 Посмотреть статистику", callback_data="show_stats"))
    kb.add(types.InlineKeyboardButton("🏆 Топ-5", callback_data="show_top"))
    return kb

# === Keep-alive (пингер) ===
async def keep_alive():
    """Пингуем внешний сайт каждые 59 минут, чтобы Railway не спал."""
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                # лёгкий запрос к Google — цель: внешний outbound, чтобы контейнер был активен
                await session.get("https://www.google.com", timeout=20)
            print(f"[{datetime.now():%H:%M:%S}] 🔄 keep-alive ping ok")
        except Exception as e:
            print(f"[{datetime.now():%H:%M:%S}] [warning] keep-alive failed: {e}")
        await asyncio.sleep(59 * 60)  # 59 minutes

# === Опросы: send_poll / specific senders ===
async def send_poll_generic(question, options):
    """Отправляет опрос с клавиатурой и возвращает (message, poll_id)."""
    msg = await bot.send_poll(
        chat_id=CHAT_ID,
        question=question,
        options=options,
        is_anonymous=False,                # важно — нужен неанонимный опрос для индивидуального учёта
        allows_multiple_answers=False,
        reply_markup=stats_keyboard(),
        disable_notification=False
    )
    # попытка закрепить, если есть права
    try:
        await bot.pin_chat_message(CHAT_ID, msg.message_id)
    except Exception:
        # не критично
        pass
    return msg, msg.poll.id

async def send_tuesday_poll():
    question = "⚽ Сегодня собираемся на песчанке в 20:00?"
    options = ["✅ Да, иду", "❌ Нет, не смогу", "🤔 Под?, в 18:30 отвечу точно"]
    msg, poll_id = await send_poll_generic(question, options)
    # запоминаем
    active_polls_by_msg[msg.message_id] = {"day": "tue", "question": question, "options": options}
    active_polls_by_pollid[poll_id] = msg.message_id
    # чистим старые голосования (на всякий)
    poll_votes.pop(poll_id, None)
    print(f"[{datetime.now():%H:%M:%S}] Опрос вторник запущен (msg={msg.message_id}, poll={poll_id})")

async def send_thursday_poll():
    question = "⚽ Сегодня собираемся на песчанке в 20:00?"
    options = ["✅ Да, иду", "❌ Нет, не смогу", "🤔 Под?, в 18:30 отвечу точно"]
    msg, poll_id = await send_poll_generic(question, options)
    active_polls_by_msg[msg.message_id] = {"day": "thu", "question": question, "options": options}
    active_polls_by_pollid[poll_id] = msg.message_id
    poll_votes.pop(poll_id, None)
    print(f"[{datetime.now():%H:%M:%S}] Опрос четверг запущен (msg={msg.message_id}, poll={poll_id})")

async def send_friday_poll():
    question = "⚽ Завтра в 12:00 собираемся на песчанке?"
    options = ["✅ Да, иду", "❌ Нет, не смогу"]
    msg, poll_id = await send_poll_generic(question, options)
    active_polls_by_msg[msg.message_id] = {"day": "fri", "question": question, "options": options}
    active_polls_by_pollid[poll_id] = msg.message_id
    poll_votes.pop(poll_id, None)
    print(f"[{datetime.now():%H:%M:%S}] Опрос пятница запущен (msg={msg.message_id}, poll={poll_id})")

# === Обработчик PollAnswer (сохраняем голоса пользователей) ===
@dp.poll_answer_handler()
async def handle_poll_answer(poll_answer: PollAnswer):
    """
    Этот хендлер вызывается когда пользователь голосует (update PollAnswer).
    poll_answer.user -> User
    poll_answer.option_ids -> список выбранных индексов
    """
    try:
        poll_id = poll_answer.poll_id
        user = poll_answer.user
        option_ids = poll_answer.option_ids or []
        # сохраняем последний выбор пользователя для данного poll_id
        if option_ids:
            # берём первый (поскольку allows_multiple_answers=False)
            poll_votes[poll_id][user.id] = option_ids[0]
        else:
            # если пользователь снял голос (опция пустая) — удаляем запись
            if user.id in poll_votes[poll_id]:
                poll_votes[poll_id].pop(user.id, None)
        # логируем
        print(f"[{datetime.now():%H:%M:%S}] PollAnswer: poll={poll_id} user={user.id} option_ids={option_ids}")
    except Exception as e:
        print(f"[warning] handle_poll_answer error: {e}")

# === Подведение итогов (вызывается через scheduler) ===
async def check_poll_results(poll_id, day):
    """
    Вызывается, когда пора подводить итоги:
     - останавливает poll (stop_poll),
     - читает poll_votes[poll_id] для детальной информации,
     - фиксирует статистику (только 'Да' option_index == 0),
     - отправляет сводку в чат,
     - удаляет записи о poll из памяти.
    """
    try:
        # Попробуем остановить опрос, чтобы получить итоговые counts
        stopped = await bot.stop_poll(CHAT_ID, poll_id)
    except Exception as e:
        print(f"[warning] stop_poll failed for poll {poll_id}: {e}")
        stopped = None

    # Собираем результаты из poll_votes (индивидуальные)
    votes = poll_votes.get(poll_id, {})  # {user_id: option_index}
    yes_count = sum(1 for v in votes.values() if v == 0)
    no_count = sum(1 for v in votes.values() if v == 1)

    # Если stop_poll вернул counts, можно использовать их (fallback)
    if stopped:
        try:
            # find options by text to map indexes (but usually 0 is yes)
            opt0 = stopped.options[0].voter_count if len(stopped.options) > 0 else None
            opt1 = stopped.options[1].voter_count if len(stopped.options) > 1 else None
            # if our counts smaller than actual, prefer stopped counts
            if isinstance(opt0, int) and opt0 > yes_count:
                yes_count = opt0
            if isinstance(opt1, int) and opt1 > no_count:
                no_count = opt1
        except Exception:
            pass

    # Фиксируем статистику: добавляем +1 каждому пользователю, кто в votes выбрал option 0 ("Да")
    stats = load_stats()
    attendance = stats.get("attendance", {})  # mapping user_repr -> int
    for uid, opt in votes.items():
        if opt == 0:
            try:
                # получим имя пользователя — по API пока не сохраняется в poll_answer, но у poll_answer.user есть username/full_name
                # NOTE: poll_answer.user в handler имел User объект — но здесь мы только имеем uid.
                # Чтобы получить username, попробуем попросить бота получить chat member (может быть дорогим).
                member = None
                try:
                    member = await bot.get_chat_member(CHAT_ID, uid)
                    user_repr = member.user.full_name if member.user.full_name else str(uid)
                    if member.user.username:
                        user_repr = f"{member.user.full_name} (@{member.user.username})"
                except Exception:
                    user_repr = str(uid)
            except Exception:
                user_repr = str(uid)

            attendance[user_repr] = attendance.get(user_repr, 0) + 1

    stats["attendance"] = attendance
    save_stats(stats)

    # Формируем и отправляем итоговое сообщение
    text = f"📊 <b>Итоги опроса ({day}):</b>\n✅ Придут: {yes_count}\n❌ Не придут: {no_count}\n\nСпасибо за участие!"
    await bot.send_message(CHAT_ID, text, parse_mode=ParseMode.HTML)

    # чистим память по этому опросу
    poll_votes.pop(poll_id, None)
    if poll_id in active_polls_by_pollid:
        msg_id = active_polls_by_pollid.pop(poll_id, None)
        if msg_id:
            active_polls_by_msg.pop(msg_id, None)

    # уведомляем админа (если можно)
    try:
        await bot.send_message(ADMIN_ID, f"✅ Опрос ({day}) завершён — статистика обновлена.")
    except Exception:
        print("[info] can't notify admin in private (maybe hasn't started chat)")

    print(f"[{datetime.now():%H:%M:%S}] check_poll_results done for poll {poll_id}")

# === Планировщик (с безопасным loop.create_task) ===
def schedule_polls():
    scheduler.remove_all_jobs()
    loop = asyncio.get_event_loop()

    # конфиг опросов
    schedule_map = [
        ("tue", 10, 0, send_tuesday_poll, check_poll_results, 20),
        ("thu", 10, 0, send_thursday_poll, check_poll_results, 20),
        ("fri", 21, 0, send_friday_poll, check_poll_results, 12)  # сводка за 1 час до time_game
    ]

    for day, hour_p, min_p, send_func, check_func, game_hour in schedule_map:
        # планируем отправку опроса
        scheduler.add_job(
            lambda f=send_func: loop.create_task(f()),
            trigger="cron",
            day_of_week=day,
            hour=hour_p,
            minute=min_p,
            id=f"poll_{day}",
            replace_existing=True
        )
        # планируем подведение итогов за 1 час до игры (game_hour - 1)
        scheduler.add_job(
            lambda func=check_func, d=day: loop.create_task(func(active_polls_by_pollid.get(next(iter(active_polls_by_msg), None)), d)),
            trigger="cron",
            day_of_week=day,
            hour=max(game_hour - 1, 0),
            minute=0,
            id=f"summary_{day}",
            replace_existing=True
        )

    print("[✅ Планировщик заданий обновлён]")

# === Команды: start, poll (manual), stats, top, resetstats, summary ===
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("⛔ У вас нет прав для управления ботом.")
        return
    schedule_polls()
    if not scheduler.running:
        scheduler.start()
    # запускаем keep-alive (если не запущен) — безопасно
    try:
        asyncio.create_task(keep_alive())
    except Exception:
        pass
    await message.reply(
        "✅ Бот активирован.\n"
        "Планировщик запущен — опросы будут по расписанию.\n\n"
        "Команды:\n"
        "/poll tue|thu|fri — запустить опрос вручную (админ)\n"
        "/stats — общая статистика (всем)\n"
        "/top — топ-5 игроков (всем)\n"
        "/resetstats — сброс статистики (админ)"
    )
    print(f"[{datetime.now():%H:%M:%S}] /start от {message.from_user.id}")

@dp.message_handler(commands=["poll"])
async def cmd_poll_manual(message: types.Message):
    # ручной запуск: только админ
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ У вас нет прав для этой команды.")
        return
    args = message.get_args().strip().lower()
    if args == "tue":
        await send_tuesday_poll()
        await message.answer("✅ Опрос вторник запущен вручную.")
    elif args == "thu":
        await send_thursday_poll()
        await message.answer("✅ Опрос четверг запущен вручную.")
    elif args == "fri":
        await send_friday_poll()
        await message.answer("✅ Опрос пятница (на субботу) запущен вручную.")
    else:
        await message.answer("⚙️ Укажите день: /poll tue | /poll thu | /poll fri")

@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    # антиспам по пользователю
    now = datetime.now()
    last = user_cooldowns.get(message.from_user.id)
    if last and (now - last).total_seconds() < 30:
        return
    user_cooldowns[message.from_user.id] = now

    stats = load_stats()
    attendance = stats.get("attendance", {})
    if not attendance:
        await message.answer("📊 Пока нет данных по посещаемости.")
        return
    text = "📈 <b>Статистика посещений (суммарно):</b>\n"
    sorted_items = sorted(attendance.items(), key=lambda x: x[1], reverse=True)
    for i, (name, cnt) in enumerate(sorted_items, start=1):
        text += f"{i}. {name} — {cnt}\n"
    await message.answer(text, parse_mode=ParseMode.HTML)

@dp.message_handler(commands=["top"])
async def cmd_top(message: types.Message):
    stats = load_stats().get("attendance", {})
    if not stats:
        await message.answer("📊 Пока нет данных по посещаемости.")
        return
    sorted_items = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:5]
    text = "🏆 <b>Топ-5 по посещаемости:</b>\n"
    for i, (name, cnt) in enumerate(sorted_items, start=1):
        text += f"{i}. {name} — {cnt}\n"
    await message.answer(text, parse_mode=ParseMode.HTML)

@dp.message_handler(commands=["resetstats"])
async def cmd_resetstats(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("⛔ Только админ может сбрасывать статистику.")
        return
    save_stats({})
    await message.reply("🧹 Статистика очищена.")

@dp.message_handler(commands=["summary"])
async def cmd_summary(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("⛔ Нет прав.")
        return
    if not active_polls_by_msg:
        await message.reply("Нет активных опросов.")
        return
    text = "📊 Активные опросы (message_id -> question):\n"
    for mid, info in active_polls_by_msg.items():
        text += f"{mid} — {info.get('question')}\n"
    await message.reply(text)

# === Callback handlers for inline buttons ===
@dp.callback_query_handler(lambda c: c.data == "show_stats")
async def cb_show_stats(query: types.CallbackQuery):
    await cmd_stats(query.message)

@dp.callback_query_handler(lambda c: c.data == "show_top")
async def cb_show_top(query: types.CallbackQuery):
    await cmd_top(query.message)

# === Startup / main ===
async def on_startup(_):
    # очистка апдейтов и планирование
    await reset_updates()
    schedule_polls()
    if not scheduler.running:
        scheduler.start()
    # запускаем keepalive
    try:
        asyncio.create_task(keep_alive())
    except Exception:
        pass
    # уведомление админу (если он открыл чат с ботом)
    try:
        await bot.send_message(ADMIN_ID, "✅ Бот запущен и планировщик активирован.")
    except Exception:
        print("[info] Не удалось уведомить админа в личке (нужно нажать Start)")

if __name__ == "__main__":
    print("[start] Bot is starting...")
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)



