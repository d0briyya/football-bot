from __future__ import annotations

from typing import Dict, Any, Optional, Tuple

def find_last_active_poll(active_polls: Dict[str, Dict[str, Any]]) -> Optional[Tuple[str, Dict[str, Any]]]:
	"""Найти последний активный опрос (по времени создания)."""
	if not active_polls:
		return None
	items = sorted(active_polls.items(), key=lambda it: it[1].get("created_at", ""), reverse=True)
	for pid, data in items:
		if data.get("active"):
			return pid, data
	return None

def format_poll_votes(data: Dict[str, Any]) -> str:
	"""Сформировать текст со списком голосов (имя — ответ)."""
	votes = data.get("votes", {})
	if not votes:
		return "— Никто ещё не голосовал."
	return "\n".join(f"{v.get('name')} — {v.get('answer')}" for v in votes.values())




