from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional
from pytz import timezone

# Общая таймзона проекта
KALININGRAD_TZ = timezone("Europe/Kaliningrad")

# Отображение дней недели
WEEKDAY_MAP: Dict[str, int] = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}

def now_tz() -> datetime:
	"""Текущее время в таймзоне Калининграда."""
	return datetime.now(KALININGRAD_TZ)

def iso_now() -> str:
	"""ISO-строка текущего времени (в таймзоне Калининграда)."""
	return now_tz().isoformat()

def normalize_day_key(day_str: str) -> Optional[str]:
	"""Привести рус/англ сокращения дня недели к ключу из WEEKDAY_MAP."""
	if not day_str:
		return None
	s = day_str.strip().lower()
	ru_map = {
		"пн": "mon", "пон": "mon", "понедельник": "mon",
		"вт": "tue", "втор": "tue", "вторник": "tue",
		"ср": "wed", "среда": "wed",
		"чт": "thu", "чет": "thu", "четверг": "thu",
		"пт": "fri", "пят": "fri", "пятница": "fri",
		"сб": "sat", "суб": "sat", "суббота": "sat",
		"вс": "sun", "вос": "sun", "воскресенье": "sun",
	}
	if s in WEEKDAY_MAP:
		return s
	if s in ru_map:
		return ru_map[s]
	return None





