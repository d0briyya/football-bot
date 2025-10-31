from __future__ import annotations

from typing import Any
from aiogram import Dispatcher
from aiogram.utils import exceptions

def setup_error_handler(dp: Dispatcher, bot, admin_id: int, log) -> None:
	"""Установить глобальный обработчик ошибок aiogram."""
	@dp.errors_handler()
	async def global_errors(update: Any, exception: Exception):
		log.exception("Global error: %s", exception)
		try:
			await bot.send_message(admin_id, f"⚠️ Ошибка: {exception}")
		except exceptions.BotBlocked:
			log.warning("Admin blocked the bot — can't send error message")
		except Exception:
			log.exception("Failed to notify admin about error")
		return True



