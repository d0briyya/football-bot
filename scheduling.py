from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
from state import WEEKDAY_MAP, KALININGRAD_TZ, now_tz

def compute_poll_close_dt(poll: Dict[str, Any], start_dt: datetime) -> datetime:
	"""Рассчитать время закрытия опроса.

	Для вт/чт — за час до времени игры, иначе — по времени игры.
	Если расчёт невозможен — fallback start_dt + 24h.
	"""
	try:
		day = poll.get("day")
		tg_hour, tg_minute = map(int, poll.get("time_game", "23:59").split(":"))
		if day not in WEEKDAY_MAP:
			return start_dt + timedelta(hours=24)
		target = WEEKDAY_MAP[day]
		days_ahead = (target - start_dt.weekday()) % 7
		base_date = start_dt.date() + timedelta(days=days_ahead)
		close_hour = tg_hour - 1 if day in ("tue", "thu") else tg_hour
		if close_hour < 0:
			close_hour = 0
		base = datetime(base_date.year, base_date.month, base_date.day, close_hour, tg_minute)
		base_local = KALININGRAD_TZ.localize(base) if base.tzinfo is None else base.astimezone(KALININGRAD_TZ)
		if base_local <= start_dt:
			base_local = base_local + timedelta(days=7)
		return base_local
	except Exception:
		return start_dt + timedelta(hours=24)

def compute_next_poll_datetime(polls_config: Any, disabled_days: set) -> Optional[Tuple[datetime, Dict[str, Any]]]:
	"""Найти ближайший по времени автозапуск опроса с учётом отключённых дней."""
	now = now_tz()
	candidates = []
	for cfg in polls_config:
		day = cfg.get("day")
		if day not in WEEKDAY_MAP:
			continue
		if day in disabled_days:
			continue
		hour, minute = map(int, cfg["time_poll"].split(":"))
		target = WEEKDAY_MAP[day]
		days_ahead = (target - now.weekday()) % 7
		dt = now_tz().replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=days_ahead)
		if dt <= now:
			dt += timedelta(days=7)
		candidates.append((dt, cfg))
	if not candidates:
		return None
	return sorted(candidates, key=lambda x: x[0])[0]





