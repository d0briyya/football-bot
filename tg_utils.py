from __future__ import annotations

from typing import Any, Awaitable, Callable, Optional
import asyncio
from aiogram.utils import exceptions

async def safe_telegram_call(func: Callable[..., Awaitable[Any]], *args: Any, retries: int = 3, **kwargs: Any) -> Optional[Any]:
	"""Надёжный вызов методов Telegram API с повторными попытками.

	Обрабатывает FloodWait/RetryAfter и временные ошибки. Возвращает результат или None.
	"""
	for attempt in range(1, retries + 1):
		try:
			return await func(*args, **kwargs)
		except exceptions.RetryAfter as e:
			wait = getattr(e, 'timeout', None) or getattr(e, 'retry_after', None) or 1
			await asyncio.sleep(wait + 1)
		except exceptions.TelegramAPIError:
			if attempt == retries:
				return None
			await asyncio.sleep(1 + attempt)
		except Exception:
			if attempt == retries:
				return None
			await asyncio.sleep(1 + attempt)
	return None



