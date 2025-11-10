from __future__ import annotations

from typing import Optional, Dict, Any, Set
import os
import asyncio
import random
import logging
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode
import html

from state import KALININGRAD_TZ

log = logging.getLogger("bot")

# Configurable timeouts (minutes)
DUEL_PENDING_MINUTES = int(os.getenv("DUEL_PENDING_MINUTES", "5"))
DUEL_BETTING_MINUTES = 2  # Время на выбор стороны болельщиками
DUEL_MAX_DURATION_MINUTES = 3  # Максимальная длительность дуэли

# Глобальное состояние дуэлей
active_duel: Optional[Dict[str, Any]] = None
duel_timeouts: Dict[str, float] = {}  # user_id -> timestamp окончания таймаута
username_to_userid: Dict[str, int] = {}  # username (lower, без @) -> user_id
duel_daily_count: Dict[str, Dict[str, Any]] = {}  # user_id -> {date: 'YYYYMMDD', count: int}
duels_enabled: bool = True  # Флаг включения/выключения дуэлей (админ может управлять)
_main_loop = None  # Основной event loop для выполнения асинхронных задач

def _now_ts() -> float:
    """Текущий timestamp."""
    import time
    return time.time()

def _mention(user_id: int, name: str) -> str:
    """Создать упоминание пользователя."""
    return f'<a href="tg://user?id={user_id}">{html.escape(name)}</a>'

def is_user_in_timeout(user_id: int) -> bool:
    """Проверить, находится ли пользователь в таймауте."""
    uid = str(user_id)
    if uid not in duel_timeouts:
        return False
    if _now_ts() >= duel_timeouts[uid]:
        # Таймаут истёк — удаляем
        duel_timeouts.pop(uid, None)
        return False
    return True

async def remove_timeout(user_id: int) -> None:
    """Снять таймаут с пользователя."""
    uid = str(user_id)
    if uid in duel_timeouts:
        duel_timeouts.pop(uid, None)

async def enforce_timeout(user_id: int, chat_id: int, name: str, scheduler, bot, timeout_minutes: int) -> None:
    """Установить таймаут на указанное количество минут."""
    global _main_loop
    uid = str(user_id)
    timeout_end = _now_ts() + timeout_minutes * 60
    duel_timeouts[uid] = timeout_end
    # Запланировать автоматическое снятие таймаута
    if scheduler:
        try:
            timeout_job_id = f"timeout_{uid}_{int(_now_ts())}"
            if _main_loop:
                scheduler.add_job(
                    lambda uid=user_id, chat_id=chat_id, name=name: asyncio.run_coroutine_threadsafe(
                        async_remove_timeout_notify(uid, chat_id, name, bot), _main_loop
                    ),
                trigger='date',
                run_date=datetime.fromtimestamp(timeout_end, tz=KALININGRAD_TZ),
                id=timeout_job_id,
            )
        except Exception:
            log.exception("Failed to schedule timeout removal for user %s", uid)

async def async_remove_timeout_notify(user_id: int, chat_id: int, name: str, bot) -> None:
    """Автоматически снять таймаут и уведомить пользователя."""
    await remove_timeout(user_id)
    from tg_utils import safe_telegram_call
    await safe_telegram_call(
        bot.send_message,
        chat_id,
        f"💪 {_mention(user_id, name)} восстановился и снова готов к дуэлям!",
        parse_mode=ParseMode.HTML,
    )

async def _finish_duel_auto(bot: Bot, chat_id: int, scheduler) -> None:
    """Автоматически завершить дуэль через максимальное время (3 минуты)."""
    global active_duel
    try:
        if not active_duel:
            return
        
        if active_duel.get("status") in ("finished", "cancelled"):
            return
        
        # Если дуэль в стадии болельщиков, завершаем её принудительно
        if active_duel.get("status") == "betting":
            await _resolve_duel_with_fans(bot, chat_id, scheduler)
        elif active_duel.get("status") == "accepted":
            # Если дуэль принята, но болельщики не выбрали стороны, завершаем без болельщиков
            await _resolve_duel_without_fans(bot, chat_id, scheduler)
    except Exception:
        log.exception("Error in _finish_duel_auto")

async def _resolve_duel_with_fans(bot: Bot, chat_id: int, scheduler) -> None:
    """Разрешить дуэль с учётом болельщиков."""
    global active_duel
    try:
        if not active_duel:
            return
        
        challenger_id = active_duel["challenger_id"]
        opponent_id = active_duel["opponent_id"]
        challenger_fans: Set[int] = active_duel.get("challenger_fans", set())
        opponent_fans: Set[int] = active_duel.get("opponent_fans", set())
        
        # Убираем дуэлянтов из списка болельщиков, если они там случайно оказались
        challenger_fans.discard(challenger_id)
        challenger_fans.discard(opponent_id)
        opponent_fans.discard(challenger_id)
        opponent_fans.discard(opponent_id)
        
        # Расчет шансов
        challenger_bonus = min(len(challenger_fans) * 2, 30)  # +2% за болельщика, макс +30%
        opponent_bonus = min(len(opponent_fans) * 2, 30)
        
        challenger_chance = 50 + challenger_bonus
        opponent_chance = 50 + opponent_bonus
        
        # Нормализация (чтобы сумма была 100%)
        total_chance = challenger_chance + opponent_chance
        if total_chance > 0:
            challenger_chance_normalized = challenger_chance / total_chance * 100
        else:
            challenger_chance_normalized = 50
        
        # Определение победителя
        winner_is_challenger = random.random() * 100 < challenger_chance_normalized
        
        if winner_is_challenger:
            winner_id = challenger_id
            winner_name = active_duel["challenger_name"]
            loser_id = opponent_id
            loser_name = active_duel["opponent_name"]
            winner_fans = challenger_fans
            loser_fans = opponent_fans
        else:
            winner_id = opponent_id
            winner_name = active_duel["opponent_name"]
            loser_id = challenger_id
            loser_name = active_duel["challenger_name"]
            winner_fans = opponent_fans
            loser_fans = challenger_fans
        
        # Наказания
        # Проигравший дуэлянт: 30 мин + 5 мин за каждого болельщика соперника
        loser_timeout = 30 + len(winner_fans) * 5
        await enforce_timeout(loser_id, chat_id, loser_name, scheduler, bot, loser_timeout)
        
        # Болельщики проигравшего: 10 мин + 5 мин за каждого болельщика соперника
        for fan_id in loser_fans:
            fan_timeout = 10 + len(winner_fans) * 5
            fan_name = active_duel.get("fan_names", {}).get(str(fan_id), f"Болельщик {fan_id}")
            await enforce_timeout(fan_id, chat_id, fan_name, scheduler, bot, fan_timeout)
        
        # Объявление результата
        result_text = (
            f"🎯 <b>ПОБЕДИТЕЛЬ ДУЭЛИ:</b> {_mention(winner_id, winner_name)}\n\n"
        )
        
        if winner_fans:
            fan_mentions = ", ".join([_mention(fid, active_duel.get("fan_names", {}).get(str(fid), f"Болельщик {fid}")) for fid in winner_fans])
            result_text += f"🎉 <b>Болельщики {winner_name}:</b> {fan_mentions}\n\n"
            result_text += f"🏆 Вы празднуете победу и отправили своих оппонентов-неудачников отдыхать!\n\n"
        
        result_text += (
            f"😵 <b>Проигравший:</b> {_mention(loser_id, loser_name)} получает таймаут на {loser_timeout} минут\n"
        )
        
        if loser_fans:
            fan_mentions = ", ".join([_mention(fid, active_duel.get("fan_names", {}).get(str(fid), f"Болельщик {fid}")) for fid in loser_fans])
            result_text += f"😞 <b>Болельщики {loser_name}:</b> {fan_mentions} получают таймаут на {10 + len(winner_fans) * 5} минут\n"
        
        await bot.send_message(chat_id, result_text, parse_mode=ParseMode.HTML)
        
        # Фиксируем статистику
        try:
            _inc_duel_count(challenger_id, opponent_id)
        except Exception:
            pass
        
        # Отменяем запланированные задачи
        try:
            if scheduler:
                if active_duel.get("betting_end_job_id"):
                    try:
                        scheduler.remove_job(active_duel["betting_end_job_id"])
                    except Exception:
                        pass
                if active_duel.get("max_duration_job_id"):
                    try:
                        scheduler.remove_job(active_duel["max_duration_job_id"])
                    except Exception:
                        pass
        except Exception:
            pass
        
        # Очистка
        active_duel = None
        
    except Exception:
        log.exception("Error in _resolve_duel_with_fans")
        active_duel = None

async def _resolve_duel_without_fans(bot: Bot, chat_id: int, scheduler) -> None:
    """Разрешить дуэль без болельщиков (старая механика как fallback)."""
    global active_duel
    try:
        if not active_duel:
            return
        
        winner_id, winner_name = random.choice([
            (active_duel["challenger_id"], active_duel["challenger_name"]),
            (active_duel["opponent_id"], active_duel["opponent_name"]),
        ])
        
        if winner_id == active_duel["challenger_id"]:
            loser_id, loser_name = active_duel["opponent_id"], active_duel["opponent_name"]
        else:
            loser_id, loser_name = active_duel["challenger_id"], active_duel["challenger_name"]
        
        await enforce_timeout(loser_id, chat_id, loser_name, scheduler, bot, 30)
        
        await bot.send_message(
            chat_id,
            f"🎯 <b>Победитель:</b> {_mention(winner_id, winner_name)}\n\n"
            f"😵 Проигравший {_mention(loser_id, loser_name)} получает таймаут на 30 минут!",
            parse_mode=ParseMode.HTML,
        )
        
        try:
            _inc_duel_count(active_duel["challenger_id"], active_duel["opponent_id"])
        except Exception:
            pass
        
        # Отменяем запланированные задачи
        try:
            if scheduler:
                if active_duel.get("betting_end_job_id"):
                    try:
                        scheduler.remove_job(active_duel["betting_end_job_id"])
                    except Exception:
                        pass
                if active_duel.get("max_duration_job_id"):
                    try:
                        scheduler.remove_job(active_duel["max_duration_job_id"])
                    except Exception:
                        pass
        except Exception:
            pass
        
        active_duel = None
        
    except Exception:
        log.exception("Error in _resolve_duel_without_fans")
        active_duel = None

def setup_duel_handlers(dp: Dispatcher, bot: Bot, scheduler, safe_telegram_call_func, check_active_poll_func=None, main_loop=None) -> None:
    """Регистрация всех хендлеров для дуэлей.
    
    Args:
        check_active_poll_func: функция, возвращающая True если есть активный опрос для вторника/четверга
        main_loop: основной event loop для выполнения асинхронных задач
    """
    # Сохраняем event loop в глобальную переменную для использования в других функциях
    global _main_loop
    if main_loop is None:
        try:
            _main_loop = getattr(scheduler, "_eventloop", None) or asyncio.get_event_loop()
        except Exception:
            _main_loop = None
    else:
        _main_loop = main_loop
    def _is_admin(uid: int) -> bool:
        try:
            return str(uid) == str(os.getenv("TG_ADMIN_ID", ""))
        except Exception:
            return False

    def _date_key() -> str:
        from datetime import datetime as _dt
        return _dt.now(KALININGRAD_TZ).strftime('%Y%m%d')

    def _can_start_duel(uid: int) -> bool:
        if _is_admin(uid):
            return True
        info = duel_daily_count.get(str(uid))
        if not info or info.get('date') != _date_key():
            return True
        return int(info.get('count', 0)) < 10

    def _inc_duel_count(u1: int, u2: int) -> None:
        for uid in (u1, u2):
            if _is_admin(uid):
                continue
            key = str(uid)
            info = duel_daily_count.get(key)
            if not info or info.get('date') != _date_key():
                duel_daily_count[key] = {'date': _date_key(), 'count': 1}
            else:
                info['count'] = int(info.get('count', 0)) + 1

    async def _expire_duel_if_pending(bot: Bot) -> None:
        global active_duel
        try:
            if active_duel and active_duel.get("status") == "pending":
                chat_id = active_duel.get("chat_id")
                await bot.send_message(chat_id, "⌛ Вызов на дуэль просрочен (5 минут). Дуэль отменена.")
                active_duel = None
        except Exception:
            log.exception("Failed to expire pending duel")

    @dp.message_handler(commands=["duel"])
    async def cmd_duel(message: types.Message) -> None:
        """Команда вызова на дуэль: /duel"""
        global active_duel, duels_enabled
        try:
            # Проверка, включены ли дуэли
            if not duels_enabled:
                return await message.reply("⛔ Дуэли временно отключены администратором.")
            
            # Проверка активных опросов для вторника/четверга
            if check_active_poll_func and check_active_poll_func():
                return await message.reply("⛔ Во время активного опроса дуэли временно запрещены.")
            
            # Проверка на активную дуэль
            if active_duel:
                return await message.reply("⚔️ Сейчас уже идёт дуэль! Подожди окончания боя, чтобы начать новую.")
            
            challenger = message.from_user
            
            # Лимит на дуэли в сутки (кроме администратора)
            if not _can_start_duel(challenger.id):
                return await message.reply("⛔ Лимит дуэлей на сегодня исчерпан (10 в сутки).")

            # Проверка таймаута вызывающего
            if is_user_in_timeout(challenger.id):
                try:
                    await bot.delete_message(message.chat.id, message.message_id)
                except Exception:
                    pass
                return
            
            # Определение соперника
            opponent = None
            if message.reply_to_message and message.reply_to_message.from_user:
                opponent = message.reply_to_message.from_user

            # Обновляем карту username -> user_id
            try:
                if getattr(challenger, 'username', None):
                    username_to_userid[str(challenger.username).lower()] = int(challenger.id)
            except Exception:
                pass
            if opponent:
                try:
                    if getattr(opponent, 'username', None):
                        username_to_userid[str(opponent.username).lower()] = int(opponent.id)
                except Exception:
                    pass
            
            if not opponent:
                return await message.reply(
                    "❓ Нужно указать соперника!\n\n"
                    "Просто ответьте (Reply) на любое сообщение пользователя и напишите <code>/duel</code>",
                    parse_mode=ParseMode.HTML
                )
            
            if opponent.id == challenger.id:
                return await message.reply("Нельзя вызвать самого себя!")
            
            # Проверка таймаута соперника
            if is_user_in_timeout(opponent.id):
                return await message.reply("⛔ Соперник сейчас в таймауте и не может принять вызов!")
            
            # Создание вызова
            active_duel = {
                "challenger_id": challenger.id,
                "challenger_name": challenger.full_name or challenger.first_name,
                "challenger_username": getattr(challenger, 'username', None),
                "opponent_id": opponent.id,
                "opponent_name": opponent.full_name or opponent.first_name,
                "opponent_username": getattr(opponent, 'username', None),
                "chat_id": message.chat.id,
                "status": "pending",
                "created_ts": _now_ts(),
            }
            
            # Кнопки принятия/отклонения
            kb = types.InlineKeyboardMarkup()
            kb.add(
                types.InlineKeyboardButton(text="✅ Принять", callback_data=f"duel_accept:{challenger.id}"),
                types.InlineKeyboardButton(text="❌ Отклонить", callback_data=f"duel_decline:{challenger.id}"),
            )
            
            await message.reply(
                f"⚔️ {_mention(challenger.id, challenger.full_name or challenger.first_name)} вызывает "
                f"{_mention(opponent.id, opponent.full_name or opponent.first_name)} на дуэль!\n\n"
                f"Принять вызов?",
                reply_markup=kb,
                parse_mode=ParseMode.HTML,
            )
            
            # Запланировать авто-сброс вызова через DUEL_PENDING_MINUTES, если не принят
            if scheduler:
                try:
                    expire_job_id = f"duel_expire_{int(active_duel['created_ts'])}"
                    active_duel["expire_job_id"] = expire_job_id
                    run_dt = datetime.fromtimestamp(active_duel["created_ts"] + DUEL_PENDING_MINUTES*60, tz=KALININGRAD_TZ)
                    if _main_loop:
                        scheduler.add_job(
                            lambda: asyncio.run_coroutine_threadsafe(_expire_duel_if_pending(bot), _main_loop),
                        trigger='date',
                        run_date=run_dt,
                        id=expire_job_id,
                    )
                except Exception:
                    log.exception("Failed to schedule duel expire job")
        except Exception:
            log.exception("Error in /duel")
            await message.reply("⚠️ Ошибка при создании вызова")
    
    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("duel_accept:"))
    async def cb_duel_accept(call: types.CallbackQuery) -> None:
        """Обработка принятия вызова на дуэль."""
        global active_duel
        try:
            if not active_duel or active_duel["status"] != "pending":
                return await call.answer("Нет активного вызова", show_alert=True)
            
            challenger_id_from_callback = int(call.data.split(":")[1])
            if call.from_user.id != active_duel["opponent_id"]:
                return await call.answer("Принять вызов может только вызванный игрок", show_alert=True)
            
            if challenger_id_from_callback != active_duel["challenger_id"]:
                return await call.answer("Этот вызов не для тебя", show_alert=True)
            
            # Проверка таймаутов ещё раз
            if is_user_in_timeout(active_duel["challenger_id"]) or is_user_in_timeout(active_duel["opponent_id"]):
                active_duel = None
                return await call.answer("Один из игроков в таймауте", show_alert=True)
            
            active_duel["status"] = "accepted"
            active_duel["accepted_ts"] = _now_ts()
            await call.answer()
            
            # Удаляем кнопки
            try:
                await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
            except Exception:
                pass
            
            # Отменяем джобу истечения ожидания принятия
            try:
                if scheduler and active_duel.get("expire_job_id"):
                    scheduler.remove_job(active_duel["expire_job_id"]) 
                    active_duel.pop("expire_job_id", None)
            except Exception:
                pass
            
            # Объявляем старт дуэли и фазу болельщиков
            chat_id = active_duel["chat_id"]
            active_duel["challenger_fans"] = set()
            active_duel["opponent_fans"] = set()
            active_duel["fan_names"] = {}
            active_duel["status"] = "betting"
            active_duel["betting_start_ts"] = _now_ts()
            
            await bot.send_message(
                chat_id,
                f"🗡️ <b>Дуэль началась!</b>\n"
                f"{_mention(active_duel['challenger_id'], active_duel['challenger_name'])} vs "
                f"{_mention(active_duel['opponent_id'], active_duel['opponent_name'])}\n\n"
                f"⏱️ <b>Время на выбор стороны: {DUEL_BETTING_MINUTES} минуты</b>\n"
                f"Выберите, за кого вы болеете! Каждый болельщик добавляет +2% шанса (макс +30%).\n"
                f"Болельщики разделяют судьбу своего чемпиона!",
                parse_mode=ParseMode.HTML,
            )
            
            # Кнопки для выбора стороны
            kb = types.InlineKeyboardMarkup()
            kb.add(
                types.InlineKeyboardButton(
                    text=f"⚔️ За {active_duel['challenger_name']}",
                    callback_data=f"duel_fan:{active_duel['challenger_id']}"
                ),
                types.InlineKeyboardButton(
                    text=f"⚔️ За {active_duel['opponent_name']}",
                    callback_data=f"duel_fan:{active_duel['opponent_id']}"
                ),
            )
            
            betting_msg = await bot.send_message(
                chat_id,
                f"👥 <b>Выберите сторону:</b>",
                reply_markup=kb,
                parse_mode=ParseMode.HTML,
            )
            
            active_duel["betting_message_id"] = betting_msg.message_id
            
            # Планируем завершение фазы болельщиков через 2 минуты
            if scheduler:
                try:
                    betting_end_job_id = f"duel_betting_end_{int(_now_ts())}"
                    active_duel["betting_end_job_id"] = betting_end_job_id
                    run_dt = datetime.fromtimestamp(_now_ts() + DUEL_BETTING_MINUTES*60, tz=KALININGRAD_TZ)
                    if _main_loop:
                        scheduler.add_job(
                            lambda: asyncio.run_coroutine_threadsafe(_end_betting_phase(bot, chat_id, scheduler), _main_loop),
                        trigger='date',
                        run_date=run_dt,
                        id=betting_end_job_id,
                    )
                except Exception:
                    log.exception("Failed to schedule betting end job")
            
            # Планируем автоматическое завершение дуэли через 3 минуты максимум
            if scheduler:
                try:
                    max_duration_job_id = f"duel_max_duration_{int(_now_ts())}"
                    active_duel["max_duration_job_id"] = max_duration_job_id
                    run_dt = datetime.fromtimestamp(_now_ts() + DUEL_MAX_DURATION_MINUTES*60, tz=KALININGRAD_TZ)
                    if _main_loop:
                        scheduler.add_job(
                            lambda: asyncio.run_coroutine_threadsafe(_finish_duel_auto(bot, chat_id, scheduler), _main_loop),
                        trigger='date',
                        run_date=run_dt,
                        id=max_duration_job_id,
                    )
                except Exception:
                    log.exception("Failed to schedule max duration job")
            
        except Exception:
            log.exception("Error in duel_accept callback")
            active_duel = None
            try:
                await call.answer("Ошибка", show_alert=True)
            except Exception:
                pass

    async def _end_betting_phase(bot: Bot, chat_id: int, scheduler) -> None:
        """Завершить фазу болельщиков и начать бой."""
        global active_duel
        try:
            if not active_duel or active_duel.get("status") != "betting":
                return
            
            # Убираем кнопки
            try:
                if active_duel.get("betting_message_id"):
                    await bot.edit_message_reply_markup(
                        chat_id,
                        active_duel["betting_message_id"],
                        reply_markup=None
                    )
            except Exception:
                pass
            
            challenger_fans_count = len(active_duel.get("challenger_fans", set()))
            opponent_fans_count = len(active_duel.get("opponent_fans", set()))
            
            await bot.send_message(
                chat_id,
                f"⏱️ Время на выбор стороны истекло!\n\n"
                f"📊 <b>Статистика поддержки:</b>\n"
                f"{_mention(active_duel['challenger_id'], active_duel['challenger_name'])}: {challenger_fans_count} болельщиков (+{min(challenger_fans_count * 2, 30)}% шанса)\n"
                f"{_mention(active_duel['opponent_id'], active_duel['opponent_name'])}: {opponent_fans_count} болельщиков (+{min(opponent_fans_count * 2, 30)}% шанса)\n\n"
                f"⚔️ Бой начинается...",
                parse_mode=ParseMode.HTML,
            )
            
            # Отменяем задачу максимальной длительности, так как дуэль завершается сейчас
            try:
                if scheduler and active_duel.get("max_duration_job_id"):
                    scheduler.remove_job(active_duel["max_duration_job_id"])
                    active_duel.pop("max_duration_job_id", None)
            except Exception:
                pass
            
            # Пауза для драматизма
            await asyncio.sleep(2)
            
            # Разрешаем дуэль
            await _resolve_duel_with_fans(bot, chat_id, scheduler)
            
        except Exception:
            log.exception("Error in _end_betting_phase")

    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("duel_fan:"))
    async def cb_duel_fan(call: types.CallbackQuery) -> None:
        """Обработка выбора стороны болельщиком."""
        global active_duel
        try:
            if not active_duel or active_duel.get("status") != "betting":
                return await call.answer("Фаза выбора стороны уже завершена", show_alert=True)
            
            fan_id = call.from_user.id
            fan_name = call.from_user.full_name or call.from_user.first_name
            
            # Проверка таймаута болельщика
            if is_user_in_timeout(fan_id):
                return await call.answer("Вы в таймауте и не можете поддерживать дуэлянтов", show_alert=True)
            
            # Нельзя поддерживать, если ты один из дуэлянтов
            if fan_id in (active_duel["challenger_id"], active_duel["opponent_id"]):
                return await call.answer("Дуэлянты не могут поддерживать себя", show_alert=True)
            
            # Получаем выбранную сторону
            parts = call.data.split(":")
            if len(parts) < 2:
                return await call.answer("Ошибка данных", show_alert=True)
            
            chosen_side_id = int(parts[1])
            
            # Проверяем, не выбрал ли уже сторону
            if fan_id in active_duel.get("challenger_fans", set()) or fan_id in active_duel.get("opponent_fans", set()):
                return await call.answer("Вы уже выбрали сторону!", show_alert=True)
            
            # Добавляем болельщика
            if chosen_side_id == active_duel["challenger_id"]:
                active_duel["challenger_fans"].add(fan_id)
                side_name = active_duel["challenger_name"]
            elif chosen_side_id == active_duel["opponent_id"]:
                active_duel["opponent_fans"].add(fan_id)
                side_name = active_duel["opponent_name"]
            else:
                return await call.answer("Ошибка: неизвестная сторона", show_alert=True)
            
            active_duel["fan_names"][str(fan_id)] = fan_name
            
            await call.answer(f"✅ Вы поддержали {side_name}!", show_alert=False)
            
            # Обновляем сообщение с кнопками (можно показать текущий счет)
            challenger_fans_count = len(active_duel.get("challenger_fans", set()))
            opponent_fans_count = len(active_duel.get("opponent_fans", set()))
            
            try:
                if active_duel.get("betting_message_id"):
                    kb = types.InlineKeyboardMarkup()
                    kb.add(
                        types.InlineKeyboardButton(
                            text=f"⚔️ За {active_duel['challenger_name']} ({challenger_fans_count})",
                            callback_data=f"duel_fan:{active_duel['challenger_id']}"
                        ),
                        types.InlineKeyboardButton(
                            text=f"⚔️ За {active_duel['opponent_name']} ({opponent_fans_count})",
                            callback_data=f"duel_fan:{active_duel['opponent_id']}"
                        ),
                    )
                    await bot.edit_message_reply_markup(
                        active_duel["chat_id"],
                        active_duel["betting_message_id"],
                        reply_markup=kb
                    )
            except Exception:
                pass  # Игнорируем ошибки редактирования
            
        except Exception:
            log.exception("Error in duel_fan callback")
            try:
                await call.answer("Ошибка", show_alert=True)
            except Exception:
                pass
    
    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("duel_decline:"))
    async def cb_duel_decline(call: types.CallbackQuery) -> None:
        """Обработка отклонения вызова на дуэль."""
        global active_duel
        try:
            if not active_duel or active_duel["status"] != "pending":
                return await call.answer("Нет активного вызова", show_alert=True)
            
            if call.from_user.id not in (active_duel["challenger_id"], active_duel["opponent_id"]):
                return await call.answer("Отклонить может только участник дуэли", show_alert=True)
            
            await call.answer()
            
            # Удаляем кнопки
            try:
                await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
            except Exception:
                pass
            
            await bot.send_message(
                active_duel["chat_id"],
                f"❌ {_mention(call.from_user.id, call.from_user.full_name or call.from_user.first_name)} отклонил вызов на дуэль.",
                parse_mode=ParseMode.HTML,
            )
            
            # Отменяем задачи
            try:
                if scheduler and active_duel.get("expire_job_id"):
                    scheduler.remove_job(active_duel["expire_job_id"])
            except Exception:
                pass
            
            active_duel = None
            
        except Exception:
            log.exception("Error in duel_decline callback")
            active_duel = None
    
    @dp.message_handler(commands=["mute"])    
    async def cmd_mute(message: types.Message) -> None:
        """Админ-команда: /mute [minutes] по реплаю на сообщение."""
        try:
            if not _is_admin(message.from_user.id):
                return
            
            if not message.reply_to_message or not message.reply_to_message.from_user:
                return await message.reply(
                    "Ответьте на сообщение пользователя, которого хотите замутить, и укажите время: /mute [минуты]\n"
                    "Например: /mute 60"
                )
            
            target_user = message.reply_to_message.from_user
            minutes: int = 30  # По умолчанию
            
            # Парсим аргументы
            parts = (message.get_args() or "").strip().split()
            for p in parts:
                try:
                    minutes = max(1, int(p))
                    break
                except Exception:
                    continue
            
            await enforce_timeout(
                target_user.id,
                message.chat.id,
                target_user.full_name or target_user.first_name,
                scheduler,
                bot,
                minutes
            )
            await message.reply(
                f"🔇 Пользователь {_mention(target_user.id, target_user.full_name or target_user.first_name)} замьючен на {minutes} минут.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            log.exception("Error in /mute")
            try:
                await message.reply("Ошибка выполнения команды /mute")
            except Exception:
                pass
    
    @dp.message_handler(commands=["unmute"])    
    async def cmd_unmute(message: types.Message) -> None:
        """Админ-команда: /unmute по реплаю на сообщение."""
        try:
            if not _is_admin(message.from_user.id):
                return
            
            if not message.reply_to_message or not message.reply_to_message.from_user:
                return await message.reply(
                    "Ответьте на сообщение пользователя, которого хотите размутить: /unmute"
                )
            
            target_user = message.reply_to_message.from_user
            
            await remove_timeout(target_user.id)
            await message.reply(
                f"✅ Пользователь {_mention(target_user.id, target_user.full_name or target_user.first_name)} размучен.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            log.exception("Error in /unmute")
            try:
                await message.reply("Ошибка выполнения команды /unmute")
            except Exception:
                pass
    
    @dp.message_handler(content_types=types.ContentType.ANY)
    async def handle_timeout_messages(message: types.Message) -> None:
        """Блокировать новые сообщения от пользователей в таймауте (кроме команд)."""
        # Актуализируем карту username -> user_id при любом сообщении
        try:
            if getattr(message.from_user, 'username', None):
                username_to_userid[str(message.from_user.username).lower()] = int(message.from_user.id)
        except Exception:
            pass
        # Проверяем таймаут только для НЕ команд (команды обрабатываются другими handlers)
        if is_user_in_timeout(message.from_user.id):
            # Пропускаем команды - они обрабатываются специальными handlers
            if message.text and message.text.startswith('/'):
                return
            # Блокируем любые другие сообщения от пользователя в таймауте
            try:
                await bot.delete_message(message.chat.id, message.message_id)
            except Exception:
                pass

# Функции для управления флагом дуэлей (используются из bot.py)
def set_duels_enabled(enabled: bool) -> None:
    """Установить состояние дуэлей (включено/выключено)."""
    global duels_enabled
    duels_enabled = bool(enabled)

def get_duels_enabled() -> bool:
    """Получить текущее состояние дуэлей."""
    return duels_enabled
