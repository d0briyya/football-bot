from __future__ import annotations

from typing import Optional, Dict, Any
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
DUEL_PENDING_MINUTES = int(os.getenv("DUEL_PENDING_MINUTES", "10"))
REVANCH_DECISION_MINUTES = int(os.getenv("DUEL_REMATCH_MINUTES", "5"))

# Глобальное состояние дуэлей (инициализируется из bot.py)
active_duel: Optional[Dict[str, Any]] = None
duel_timeouts: Dict[str, float] = {}  # user_id -> timestamp окончания таймаута
username_to_userid: Dict[str, int] = {}  # username (lower, без @) -> user_id
revanch_used_for_duel: Dict[str, float] = {}  # duel_key -> timestamp использования (для очистки старых)
duel_daily_count: Dict[str, Dict[str, Any]] = {}  # user_id -> {date: 'YYYYMMDD', count: int}
revanch_pending: Optional[Dict[str, Any]] = None  # Ожидающее предложение реванша
revange_used: Dict[str, bool] = {}  # user_id -> использовал ли этот пользователь право на реванш (одноразовое)
duels_enabled: bool = True  # Флаг включения/выключения дуэлей (админ может управлять)

def _now_ts() -> float:
    """Текущий timestamp."""
    import time
    return time.time()

def _cleanup_old_revanch_records() -> None:
    """Очистить старые записи о реваншах (старше 7 дней)."""
    global revanch_used_for_duel
    now = _now_ts()
    cutoff = now - (7 * 24 * 60 * 60)  # 7 дней назад
    keys_to_remove = [k for k, ts in revanch_used_for_duel.items() if ts < cutoff]
    for k in keys_to_remove:
        revanch_used_for_duel.pop(k, None)
    if keys_to_remove:
        log.debug(f"Cleaned up {len(keys_to_remove)} old revanch records")

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

async def enforce_timeout(user_id: int, chat_id: int, name: str, scheduler, bot, timeout_minutes: int = 30) -> None:
    """Установить таймаут на указанное количество минут для проигравшего."""
    uid = str(user_id)
    timeout_end = _now_ts() + timeout_minutes * 60
    duel_timeouts[uid] = timeout_end
    # Запланировать автоматическое снятие таймаута через 30 минут
    if scheduler:
        try:
            timeout_job_id = f"timeout_{uid}_{int(_now_ts())}"
            loop = getattr(scheduler, "_eventloop", None)
            scheduler.add_job(
                lambda uid=user_id, chat_id=chat_id, name=name: asyncio.run_coroutine_threadsafe(
                    async_remove_timeout_notify(uid, chat_id, name, bot), loop or asyncio.get_event_loop()
                ),
                trigger='date',
                run_date=datetime.fromtimestamp(timeout_end, tz=KALININGRAD_TZ),
                id=timeout_job_id,
            )
        except Exception:
            import logging
            logging.getLogger("bot").exception("Failed to schedule timeout removal for user %s", uid)

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

def setup_duel_handlers(dp: Dispatcher, bot: Bot, scheduler, safe_telegram_call_func, check_active_poll_func=None) -> None:
    """Регистрация всех хендлеров для дуэлей.
    
    Args:
        check_active_poll_func: функция, возвращающая True если есть активный опрос для вторника/четверга
    """
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
        return int(info.get('count', 0)) < 3

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

    """Регистрация всех хендлеров для дуэлей."""
    
    async def _expire_duel_if_pending(bot: Bot) -> None:
        global active_duel
        try:
            if active_duel and active_duel.get("status") == "pending":
                chat_id = active_duel.get("chat_id")
                await bot.send_message(chat_id, "⌛ Вызов на дуэль просрочен (10 минут). Дуэль отменена.")
                active_duel = None
        except Exception:
            import logging
            logging.getLogger("bot").exception("Failed to expire pending duel")

    async def _expire_revanch_if_pending(bot: Bot) -> None:
        global revanch_pending
        try:
            if revanch_pending:
                chat_id = revanch_pending.get("chat_id")
                winner_id = revanch_pending.get("winner_id")
                await bot.send_message(chat_id, f"⏳ {_mention(winner_id, revanch_pending.get('winner_name','Игрок'))} не решился на реванш... Дуэль окончена.", parse_mode=ParseMode.HTML)
                revanch_pending = None
        except Exception:
            import logging
            logging.getLogger("bot").exception("Failed to expire pending revanch")

    @dp.message_handler(commands=["duel"])
    async def cmd_duel(message: types.Message) -> None:
        """Команда вызова на дуэль: /duel @username"""
        global active_duel, revanch_pending, duels_enabled
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
            # Новые дуэли разрешены, даже если в чате ожидается решение по другому реваншу
            
            challenger = message.from_user
            
            # Лимит на дуэли в сутки (кроме администратора)
            if not _can_start_duel(challenger.id):
                return await message.reply("⛔ Лимит дуэлей на сегодня исчерпан (3 в сутки).")

            # Проверка таймаута вызывающего
            if is_user_in_timeout(challenger.id):
                # Полный запрет на взаимодействие: удаляем сообщение с командой
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
                    loop = getattr(scheduler, "_eventloop", None)
                    scheduler.add_job(
                        lambda: asyncio.run_coroutine_threadsafe(_expire_duel_if_pending(bot), loop or asyncio.get_event_loop()),
                        trigger='date',
                        run_date=run_dt,
                        id=expire_job_id,
                    )
                except Exception:
                    import logging
                    logging.getLogger("bot").exception("Failed to schedule duel expire job")
        except Exception:
            import logging
            logging.getLogger("bot").exception("Error in /duel")
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
            
            # Компактно объявляем старт дуэли в беседе
            try:
                await bot.send_message(
                    active_duel["chat_id"],
                    f"🗡️ Дуэль: {_mention(active_duel['challenger_id'], active_duel['challenger_name'])} vs "
                    f"{_mention(active_duel['opponent_id'], active_duel['opponent_name'])} — старт!",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
            
            # Пауза для драматизма
            await asyncio.sleep(2)
            
            # Случайное определение победителя
            winner_id, winner_name = random.choice([
                (active_duel["challenger_id"], active_duel["challenger_name"]),
                (active_duel["opponent_id"], active_duel["opponent_name"]),
            ])
            
            if winner_id == active_duel["challenger_id"]:
                loser_id, loser_name = active_duel["opponent_id"], active_duel["opponent_name"]
            else:
                loser_id, loser_name = active_duel["challenger_id"], active_duel["challenger_name"]
            
            # Установка таймаута для проигравшего
            await enforce_timeout(loser_id, active_duel["chat_id"], loser_name, scheduler, bot, 30)
            # Фиксируем начало/окончание дуэли для статистики/ограничений
            try:
                _inc_duel_count(active_duel["challenger_id"], active_duel["opponent_id"])
            except Exception:
                pass
            
            # Проверяем, использовал ли проигравший право на реванш
            loser_uid_str = str(loser_id)
            has_revanch_right = not revange_used.get(loser_uid_str, False)
            
            # Объявление результата — в общем чате (видят все)
            await bot.send_message(
                active_duel["chat_id"],
                f"🎯 <b>Победитель:</b> {_mention(winner_id, winner_name)}\n\n"
                f"😵 Проигравший {_mention(loser_id, loser_name)} получает таймаут на 30 минут!",
                parse_mode=ParseMode.HTML,
            )
            
            # Сохраняем информацию о дуэли для возможного реванша
            if has_revanch_right and not active_duel.get("is_revanch"):
                # Сохраняем данные для реванша в active_duel
                active_duel["winner_id"] = winner_id
                active_duel["winner_name"] = winner_name
                active_duel["loser_id"] = loser_id
                active_duel["loser_name"] = loser_name
                active_duel["finished"] = True
                # Публикуем в беседе компактное приглашение на реванш с кнопкой (нажать сможет только проигравший)
                kb_revanch = types.InlineKeyboardMarkup()
                token = str(int(_now_ts()))
                kb_revanch.add(types.InlineKeyboardButton(
                    text="⚔️ Реванш!",
                    callback_data=f"revanch_request:{loser_id}:{winner_id}:{token}"
                ))
                try:
                    await bot.send_message(
                        active_duel["chat_id"],
                        f"↩️ {_mention(loser_id, loser_name)}, можешь взять реванш (один раз).",
                        reply_markup=kb_revanch,
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass
            # Планируем истечение права реванша через REVANCH_DECISION_MINUTES
            if scheduler:
                try:
                    rev_expire_job_id = f"rev_expire_{int(_now_ts())}"
                    active_duel["rev_expire_job_id"] = rev_expire_job_id
                    run_dt = datetime.fromtimestamp(_now_ts() + REVANCH_DECISION_MINUTES*60, tz=KALININGRAD_TZ)
                    loop = getattr(scheduler, "_eventloop", None)
                    scheduler.add_job(
                        lambda: asyncio.run_coroutine_threadsafe(_expire_revanch_if_pending(bot), loop or asyncio.get_event_loop()),
                        trigger='date',
                        run_date=run_dt,
                        id=rev_expire_job_id,
                    )
                except Exception:
                    pass
            else:
                # Сброс активной дуэли
                active_duel = None
            
        except Exception:
            import logging
            logging.getLogger("bot").exception("Error in duel_accept callback")
            active_duel = None
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
            
            active_duel = None
            
        except Exception:
            import logging
            logging.getLogger("bot").exception("Error in duel_decline callback")
            active_duel = None
    
    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("revanch_request:"))
    async def cb_revanch_request(call: types.CallbackQuery) -> None:
        """Обработка предложения реванша от проигравшего."""
        global revanch_pending, active_duel
        try:
            # Разбираем callback_data: revanch_request:loser_id:winner_id
            parts = call.data.split(":")
            if len(parts) < 3:
                return await call.answer("Ошибка данных", show_alert=True)
            
            loser_id = int(parts[1])
            winner_id = int(parts[2])
            token = parts[3] if len(parts) > 3 else str(call.message.message_id)
            duel_key = f"{loser_id}:{winner_id}:{token}"
            
            # Очищаем старые записи перед проверкой
            _cleanup_old_revanch_records()
            
            if duel_key in revanch_used_for_duel:
                return await call.answer("Право на реванш для этой дуэли уже использовано", show_alert=True)
            
            # Проверяем, что это проигравший запросил реванш
            if call.from_user.id != loser_id:
                return await call.answer("Только проигравший может запросить реванш", show_alert=True)
            
            # Проверяем таймауты: проигравший может быть в таймауте и всё равно запросить реванш;
            # соперник не должен быть в таймауте
            if is_user_in_timeout(winner_id):
                return await call.answer("Соперник сейчас в таймауте", show_alert=True)
            
            await call.answer()
            
            # Логируем запрос реванша
            log.info(f"revanch_request: user_id={call.from_user.id}, loser_id={loser_id}, winner_id={winner_id}, duel_key={duel_key}")
            
            # Получаем имена участников из active_duel
            loser_name = call.from_user.full_name or call.from_user.first_name
            winner_name = active_duel.get("winner_name") if active_duel else "Соперник"
            
            # Создаём предложение реванша
            revanch_pending = {
                "loser_id": loser_id,
                "loser_name": loser_name,
                "winner_id": winner_id,
                "winner_name": winner_name,
                "chat_id": call.message.chat.id,
                "message_id": call.message.message_id,
                "duel_key": duel_key,
            }
            # Отметим, что право на реванш по этой дуэли использовано (повторно нельзя) с timestamp
            revanch_used_for_duel[duel_key] = _now_ts()
            
            # Очищаем active_duel только после создания revanch_pending
            active_duel = None
            
            # Показываем правила реванша с индикацией времени
            rules_text = (
                f"⚔️ {_mention(loser_id, loser_name)} просит реванш у {_mention(winner_id, winner_name)}!\n\n"
                f"📋 <b>Правила реванша:</b>\n\n"
                f"🔸 <b>Если {_mention(loser_id, loser_name)} выиграет:</b>\n"
                f"   ✅ Снимается штраф в 30 минут\n"
                f"   ⏱️ {_mention(winner_id, winner_name)} получит таймаут 1 час\n\n"
                f"🔸 <b>Если {_mention(winner_id, winner_name)} снова выиграет:</b>\n"
                f"   😞 {_mention(loser_id, loser_name)} получит таймаут 2 часа\n\n"
                f"⏳ У победителя есть {REVANCH_DECISION_MINUTES} минут, чтобы ответить.\n\n"
                f"⚠️ {_mention(winner_id, winner_name)}, ты согласен(на)?"
            )
            
            kb = types.InlineKeyboardMarkup()
            kb.add(
                types.InlineKeyboardButton(text="✅ Согласен на реванш", callback_data=f"revanch_accept:{loser_id}"),
                types.InlineKeyboardButton(text="❌ Отказаться", callback_data=f"revanch_decline:{loser_id}"),
            )
            
            # Удаляем кнопку "Реванш!" из старого сообщения
            try:
                await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
            except Exception:
                pass
            
            # Публикуем правила реванша в беседе
            try:
                await bot.send_message(call.message.chat.id, rules_text, reply_markup=kb, parse_mode=ParseMode.HTML)
            except Exception:
                pass
            # Планируем истечение времени на принятие реванша (REVANCH_DECISION_MINUTES для победителя)
            if scheduler:
                try:
                    rev_decision_job_id = f"rev_decide_{int(_now_ts())}"
                    # сохраним, чтобы отменить при accept/decline
                    revanch_pending["rev_decision_job_id"] = rev_decision_job_id
                    run_dt = datetime.fromtimestamp(_now_ts() + REVANCH_DECISION_MINUTES*60, tz=KALININGRAD_TZ)
                    loop = getattr(scheduler, "_eventloop", None)
                    scheduler.add_job(
                        lambda: asyncio.run_coroutine_threadsafe(_expire_revanch_if_pending(bot), loop or asyncio.get_event_loop()),
                        trigger='date',
                        run_date=run_dt,
                        id=rev_decision_job_id,
                    )
                except Exception:
                    pass
            
        except Exception:
            import logging
            logging.getLogger("bot").exception("Error in revanch_request callback")
            revanch_pending = None
            try:
                await call.answer("Ошибка", show_alert=True)
            except Exception:
                pass
    
    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("revanch_accept:"))
    async def cb_revanch_accept(call: types.CallbackQuery) -> None:
        """Обработка принятия реванша."""
        global active_duel, revanch_pending
        try:
            if not revanch_pending:
                return await call.answer("Нет активного запроса реванша", show_alert=True)
            
            parts = call.data.split(":")
            if len(parts) != 2:
                return await call.answer("Ошибка данных", show_alert=True)
            
            loser_id_from_cb = int(parts[1])
            
            # Проверяем, что это победитель принял реванш
            if call.from_user.id != revanch_pending["winner_id"]:
                return await call.answer("Только победитель может принять реванш", show_alert=True)
            
            if loser_id_from_cb != revanch_pending["loser_id"]:
                return await call.answer("Этот запрос не для тебя", show_alert=True)
            
            # Проверяем таймауты ещё раз: проигравший МОЖЕТ быть в таймауте, победитель — нет
            if is_user_in_timeout(revanch_pending["winner_id"]):
                return await call.answer("Соперник сейчас в таймауте", show_alert=True)
            
            await call.answer()
            
            # Логируем принятие реванша
            log.info(f"revanch_accept: user_id={call.from_user.id}, winner_id={revanch_pending['winner_id']}, loser_id={revanch_pending['loser_id']}, duel_key={revanch_pending.get('duel_key', 'unknown')}")
            
            # Отменяем таймер решения реванша (если был)
            try:
                if scheduler and revanch_pending.get("rev_decision_job_id"):
                    scheduler.remove_job(revanch_pending["rev_decision_job_id"]) 
            except Exception:
                pass
            # Удаляем кнопки из сообщения с правилами
            try:
                await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
            except Exception:
                pass
            
            # Создаём новую дуэль-реванш
            active_duel = {
                "challenger_id": revanch_pending["loser_id"],
                "challenger_name": revanch_pending["loser_name"],
                "opponent_id": revanch_pending["winner_id"],
                "opponent_name": revanch_pending["winner_name"],
                "chat_id": revanch_pending["chat_id"],
                "status": "accepted",
                "created_ts": _now_ts(),
                "is_revanch": True,
                "finished": False,
            }
            # Отменяем джобу истечения реванша, если была
            try:
                if scheduler and active_duel.get("rev_expire_job_id"):
                    scheduler.remove_job(active_duel["rev_expire_job_id"]) 
                    active_duel.pop("rev_expire_job_id", None)
            except Exception:
                pass
            
            # Помечаем, что право на реванш использовано
            revange_used[str(revanch_pending["loser_id"])] = True
            
            # Компактно объявляем старт реванша в беседе
            try:
                await bot.send_message(
                    active_duel["chat_id"],
                    f"🗡️ Реванш: {_mention(active_duel['challenger_id'], active_duel['challenger_name'])} vs "
                    f"{_mention(active_duel['opponent_id'], active_duel['opponent_name'])} — старт!",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
            
            # Пауза для драматизма
            await asyncio.sleep(2)
            
            # Случайное определение победителя реванша
            winner_id, winner_name = random.choice([
                (revanch_pending["loser_id"], revanch_pending["loser_name"]),
                (revanch_pending["winner_id"], revanch_pending["winner_name"]),
            ])
            
            if winner_id == revanch_pending["loser_id"]:
                loser_id, loser_name = revanch_pending["winner_id"], revanch_pending["winner_name"]
                # Проигравший выиграл реванш — снимаем его таймаут, даём час победителю
                await remove_timeout(revanch_pending["loser_id"])
                await enforce_timeout(revanch_pending["winner_id"], revanch_pending["chat_id"], 
                                    revanch_pending["winner_name"], scheduler, bot, 60)
                
                result_text = (
                    f"🎉 <b>Реванш выигран!</b> {_mention(winner_id, winner_name)}\n\n"
                    f"✅ Штраф в 30 минут снят с {_mention(winner_id, winner_name)}\n"
                    f"⏱️ {_mention(loser_id, loser_name)} получает таймаут на 1 час"
                )
            else:
                loser_id, loser_name = revanch_pending["loser_id"], revanch_pending["loser_name"]
                # Победитель снова выиграл — 2 часа проигравшему
                await enforce_timeout(revanch_pending["loser_id"], revanch_pending["chat_id"], 
                                    revanch_pending["loser_name"], scheduler, bot, 120)
                
                result_text = (
                    f"🎯 <b>Победитель реванша:</b> {_mention(winner_id, winner_name)}\n\n"
                    f"😞 {_mention(loser_id, loser_name)} получает таймаут на 2 часа\n"
                    f"🔒 Право на реванш потеряно"
                )
            
            await bot.send_message(
                revanch_pending["chat_id"],
                result_text,
                parse_mode=ParseMode.HTML,
            )
            
            # Очистка
            revanch_pending = None
            active_duel = None
            
        except Exception:
            import logging
            logging.getLogger("bot").exception("Error in revanch_accept callback")
            revanch_pending = None
            active_duel = None
            try:
                await call.answer("Ошибка", show_alert=True)
            except Exception:
                pass
    
    @dp.callback_query_handler(lambda c: c.data and c.data.startswith("revanch_decline:"))
    async def cb_revanch_decline(call: types.CallbackQuery) -> None:
        """Обработка отклонения реванша."""
        global revanch_pending
        try:
            if not revanch_pending:
                return await call.answer("Нет активного запроса реванша", show_alert=True)
            
            parts = call.data.split(":")
            if len(parts) != 2:
                return await call.answer("Ошибка данных", show_alert=True)
            
            loser_id_from_cb = int(parts[1])
            
            if loser_id_from_cb != revanch_pending["loser_id"]:
                return await call.answer("Этот запрос не для тебя", show_alert=True)

            # Отклонить реванш может только победитель (к кому реванш обращён)
            if call.from_user.id != revanch_pending["winner_id"]:
                return await call.answer("Отклонить может только соперник, к которому обращён реванш", show_alert=True)
            
            await call.answer()
            
            # Логируем отклонение реванша
            log.info(f"revanch_decline: user_id={call.from_user.id}, winner_id={revanch_pending['winner_id']}, loser_id={revanch_pending['loser_id']}, duel_key={revanch_pending.get('duel_key', 'unknown')}")
            
            # Удаляем кнопки
            try:
                await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
            except Exception:
                pass
            # Отменяем таймер решения реванша (если был)
            try:
                if scheduler and revanch_pending.get("rev_decision_job_id"):
                    scheduler.remove_job(revanch_pending["rev_decision_job_id"]) 
            except Exception:
                pass
            
            await bot.send_message(
                revanch_pending["chat_id"],
                f"❌ {_mention(call.from_user.id, call.from_user.full_name or call.from_user.first_name)} отклонил реванш.",
                parse_mode=ParseMode.HTML,
            )
            
            revanch_pending = None
            
        except Exception:
            import logging
            logging.getLogger("bot").exception("Error in revanch_decline callback")
            revanch_pending = None
    
    @dp.message_handler(commands=["mute"])    
    async def cmd_mute(message: types.Message) -> None:
        """Админ-команда: /mute [minutes] по реплаю или /mute @username [minutes]."""
        try:
            if not _is_admin(message.from_user.id):
                return
            target_user: Optional[types.User] = None
            minutes: int = 30
            parts = (message.get_args() or "").split()
            # Если есть реплай — это приоритетная цель
            if message.reply_to_message and message.reply_to_message.from_user:
                target_user = message.reply_to_message.from_user
                # Попробуем извлечь число минут из аргументов
                for p in parts:
                    try:
                        minutes = max(1, int(p))
                        break
                    except Exception:
                        continue
            else:
                # Пытаемся распарсить @username и минуты из аргументов в любом порядке
                username: Optional[str] = None
                for p in parts:
                    if p.startswith("@"):
                        username = p[1:].lower()
                    else:
                        try:
                            minutes = max(1, int(p))
                        except Exception:
                            pass
                if username:
                    uid = username_to_userid.get(username)
                    if uid:
                        # Построим фейкового пользователя с id (для упоминания возьмем текст из команды)
                        class _U:
                            id = uid
                            full_name = username
                            first_name = username
                        target_user = _U()  # type: ignore
            if not target_user:
                return await message.reply(
                    "Укажите пользователя: ответьте на его сообщение или /mute @username [минуты]",
                )
            await enforce_timeout(target_user.id, message.chat.id, getattr(target_user, 'full_name', str(target_user.id)), scheduler, bot, minutes)
            await message.reply(
                f"🔇 Пользователь {_mention(target_user.id, getattr(target_user, 'full_name', str(target_user.id)))} замьючен на {minutes} мин.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            import logging
            logging.getLogger("bot").exception("Error in /mute")
            try:
                await message.reply("Ошибка выполнения команды /mute")
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
        if is_user_in_timeout(message.from_user.id):
            # Блокируем любые сообщения и команды от пользователя в таймауте
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

