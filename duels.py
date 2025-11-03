from __future__ import annotations

from typing import Optional, Dict, Any
import asyncio
import random
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.types import ParseMode
import html

from state import KALININGRAD_TZ

# Глобальное состояние дуэлей (инициализируется из bot.py)
active_duel: Optional[Dict[str, Any]] = None
duel_timeouts: Dict[str, float] = {}  # user_id -> timestamp окончания таймаута
username_to_userid: Dict[str, int] = {}  # username (lower, без @) -> user_id
revanch_pending: Optional[Dict[str, Any]] = None  # Ожидающее предложение реванша
revange_used: Dict[str, bool] = {}  # user_id -> использовал ли этот пользователь право на реванш (одноразовое)

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

async def enforce_timeout(user_id: int, chat_id: int, name: str, scheduler, bot, timeout_minutes: int = 30) -> None:
    """Установить таймаут на указанное количество минут для проигравшего."""
    uid = str(user_id)
    timeout_end = _now_ts() + timeout_minutes * 60
    duel_timeouts[uid] = timeout_end
    # Запланировать автоматическое снятие таймаута через 30 минут
    if scheduler:
        try:
            timeout_job_id = f"timeout_{uid}_{int(_now_ts())}"
            scheduler.add_job(
                lambda uid=user_id, chat_id=chat_id, name=name: asyncio.run_coroutine_threadsafe(
                    async_remove_timeout_notify(uid, chat_id, name, bot), asyncio.get_event_loop()
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

def setup_duel_handlers(dp: Dispatcher, bot: Bot, scheduler, safe_telegram_call_func) -> None:
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
                loser_name = revanch_pending.get("loser_name", "Проигравший")
                winner_name = revanch_pending.get("winner_name", "Победитель")
                await bot.send_message(
                    chat_id,
                    (
                        f"⏳ {_mention(revanch_pending['winner_id'], winner_name)} не ответил на реванш вовремя.\n"
                        f"{_mention(revanch_pending['winner_id'], winner_name)} решил не рисковать и остаётся победителем."
                    ),
                    parse_mode=ParseMode.HTML,
                )
                revanch_pending = None
        except Exception:
            import logging
            logging.getLogger("bot").exception("Failed to expire pending revanch")

    @dp.message_handler(commands=["duel"])
    async def cmd_duel(message: types.Message) -> None:
        """Команда вызова на дуэль: /duel @username"""
        global active_duel, revanch_pending
        try:
            # Проверка на активную дуэль
            if active_duel:
                return await message.reply("⚔️ Сейчас уже идёт дуэль! Подожди окончания боя, чтобы начать новую.")
            # Новые дуэли разрешены, даже если в чате ожидается решение по другому реваншу
            
            challenger = message.from_user
            
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
            # Запланировать авто-сброс вызова через 10 минут, если не принят
            if scheduler:
                try:
                    expire_job_id = f"duel_expire_{int(active_duel['created_ts'])}"
                    active_duel["expire_job_id"] = expire_job_id
                    run_dt = datetime.fromtimestamp(active_duel["created_ts"] + 10*60, tz=KALININGRAD_TZ)
                    scheduler.add_job(
                        lambda: asyncio.run_coroutine_threadsafe(_expire_duel_if_pending(bot), asyncio.get_event_loop()),
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
                kb_revanch.add(types.InlineKeyboardButton(
                    text="⚔️ Реванш!",
                    callback_data=f"revanch_request:{loser_id}:{winner_id}"
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
                # Планируем истечение права реванша через 1 минуту
                if scheduler:
                    try:
                        rev_expire_job_id = f"rev_expire_{int(_now_ts())}"
                        active_duel["rev_expire_job_id"] = rev_expire_job_id
                        run_dt = datetime.fromtimestamp(_now_ts() + 60, tz=KALININGRAD_TZ)
                        scheduler.add_job(
                            lambda: asyncio.run_coroutine_threadsafe(_expire_revanch_if_pending(bot), asyncio.get_event_loop()),
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
            if len(parts) != 3:
                return await call.answer("Ошибка данных", show_alert=True)
            
            loser_id = int(parts[1])
            winner_id = int(parts[2])
            
            # Проверяем, что это проигравший запросил реванш
            if call.from_user.id != loser_id:
                return await call.answer("Только проигравший может запросить реванш", show_alert=True)
            
            # Проверяем, что он ещё не использовал право на реванш
            if revange_used.get(str(loser_id), False):
                return await call.answer("Ты уже использовал право на реванш!", show_alert=True)
            
            # Проверяем таймауты: проигравший может быть в таймауте и всё равно запросить реванш;
            # соперник не должен быть в таймауте
            if is_user_in_timeout(winner_id):
                return await call.answer("Соперник сейчас в таймауте", show_alert=True)
            
            await call.answer()
            
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
            }
            
            # Очищаем active_duel только после создания revanch_pending
            active_duel = None
            
            # Показываем правила реванша
            rules_text = (
                f"⚔️ {_mention(loser_id, loser_name)} просит реванш у {_mention(winner_id, winner_name)}!\n\n"
                f"📋 <b>Правила реванша:</b>\n\n"
                f"🔸 <b>Если {_mention(loser_id, loser_name)} выиграет:</b>\n"
                f"   ✅ Снимается штраф в 30 минут\n"
                f"   ⏱️ {_mention(winner_id, winner_name)} получит таймаут 1 час\n\n"
                f"🔸 <b>Если {_mention(winner_id, winner_name)} снова выиграет:</b>\n"
                f"   😞 {_mention(loser_id, loser_name)} получит таймаут 2 часа\n"
                f"   🔒 Право на реванш теряется навсегда\n\n"
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
            # Планируем истечение времени на принятие реванша (1 минута для победителя)
            if scheduler:
                try:
                    rev_decision_job_id = f"rev_decide_{int(_now_ts())}"
                    # сохраним, чтобы отменить при accept/decline
                    revanch_pending["rev_decision_job_id"] = rev_decision_job_id
                    run_dt = datetime.fromtimestamp(_now_ts() + 60, tz=KALININGRAD_TZ)
                    scheduler.add_job(
                        lambda: asyncio.run_coroutine_threadsafe(_expire_revanch_if_pending(bot), asyncio.get_event_loop()),
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
            
            # Проверяем таймауты ещё раз
            if is_user_in_timeout(revanch_pending["loser_id"]) or is_user_in_timeout(revanch_pending["winner_id"]):
                revanch_pending = None
                return await call.answer("Кто-то из игроков в таймауте", show_alert=True)
            
            await call.answer()
            
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
            
            # Удаляем кнопки
            try:
                await bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
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
    
    @dp.message_handler(content_types=types.ContentType.ANY)
    async def handle_timeout_messages(message: types.Message) -> None:
        """Блокировать новые сообщения от пользователей в таймауте (кроме команд)."""
        if is_user_in_timeout(message.from_user.id):
            # Блокируем любые сообщения и команды от пользователя в таймауте
            try:
                await bot.delete_message(message.chat.id, message.message_id)
            except Exception:
                pass

