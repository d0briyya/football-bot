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

async def enforce_timeout(user_id: int, chat_id: int, name: str, scheduler, bot) -> None:
    """Установить таймаут на 30 минут для проигравшего."""
    uid = str(user_id)
    timeout_end = _now_ts() + 30 * 60  # 30 минут
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
    
    @dp.message_handler(commands=["duel"])
    async def cmd_duel(message: types.Message) -> None:
        """Команда вызова на дуэль: /duel @username"""
        global active_duel
        try:
            # Проверка на активную дуэль
            if active_duel:
                return await message.reply("⚔️ Сейчас уже идёт дуэль! Подожди окончания боя, чтобы начать новую.")
            
            challenger = message.from_user
            
            # Проверка таймаута вызывающего
            if is_user_in_timeout(challenger.id):
                return await message.reply("⛔ Ты сейчас в таймауте и не можешь вызывать на дуэль!")
            
            # Определение соперника
            opponent = None
            if message.reply_to_message and message.reply_to_message.from_user:
                opponent = message.reply_to_message.from_user
            else:
                args = message.get_args().strip()
                if args.startswith("@"):
                    return await message.reply("Ответьте на сообщение соперника и введите /duel или укажите @username")
            
            if not opponent:
                return await message.reply("Ответьте на сообщение соперника и введите /duel или укажите @username")
            
            if opponent.id == challenger.id:
                return await message.reply("Нельзя вызвать самого себя!")
            
            # Проверка таймаута соперника
            if is_user_in_timeout(opponent.id):
                return await message.reply("⛔ Соперник сейчас в таймауте и не может принять вызов!")
            
            # Создание вызова
            active_duel = {
                "challenger_id": challenger.id,
                "challenger_name": challenger.full_name or challenger.first_name,
                "opponent_id": opponent.id,
                "opponent_name": opponent.full_name or opponent.first_name,
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
            
            # Объявляем начало боя
            await bot.send_message(
                active_duel["chat_id"],
                f"🗡️ Дуэль между {_mention(active_duel['challenger_id'], active_duel['challenger_name'])} "
                f"и {_mention(active_duel['opponent_id'], active_duel['opponent_name'])} начинается!",
                parse_mode=ParseMode.HTML,
            )
            
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
            await enforce_timeout(loser_id, active_duel["chat_id"], loser_name, scheduler, bot)
            
            # Объявление результата
            await bot.send_message(
                active_duel["chat_id"],
                f"🎯 <b>Победитель:</b> {_mention(winner_id, winner_name)}\n\n"
                f"😵 Проигравший {_mention(loser_id, loser_name)} получает таймаут на 30 минут!",
                parse_mode=ParseMode.HTML,
            )
            
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
    
    @dp.message_handler(content_types=types.ContentType.ANY)
    async def handle_timeout_messages(message: types.Message) -> None:
        """Блокировать новые сообщения от пользователей в таймауте (кроме команд)."""
        # Пропускаем команды
        if message.text and message.text.startswith('/'):
            return
        
        if is_user_in_timeout(message.from_user.id):
            # Блокируем только новые сообщения (удаляем их, старые не трогаем)
            try:
                await bot.delete_message(message.chat.id, message.message_id)
            except Exception:
                pass

