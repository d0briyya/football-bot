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
	# Печатаем единым форматом с иконками статуса: Да/Нет/Под вопросом
	yes_list = []
	no_list = []
	maybe_list = []
	for v in votes.values():
		name = v.get("name")
		answer = str(v.get("answer", ""))
		low = answer.lower()
		if answer.startswith("Да"):
			yes_list.append(f"✅ {name}")
		elif answer.startswith("Нет"):
			# используем грустный смайлик для наглядности
			no_list.append(f"😞 {name}")
		elif "вопрос" in low or "?" in answer:
			maybe_list.append(f"❔ {name}")
		else:
			# по умолчанию без иконки
			maybe_list.append(f"❔ {name}")
	lines = []
	for lst in (yes_list, maybe_list, no_list):
		lines.extend(lst)
	return "\n".join(lines)





