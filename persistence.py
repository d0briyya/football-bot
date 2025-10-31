from __future__ import annotations

from typing import Dict, Any, Tuple, Set
import os
import json
import aiofiles

async def save_data(path: str, active_polls: Dict[str, Dict[str, Any]], stats: Dict[str, Any], disabled_days: Set[str]) -> None:
	"""Сохранить основные данные бота в JSON-файл."""
	payload = {
		"active_polls": active_polls,
		"stats": stats,
		"disabled_days": sorted(list(disabled_days)),
	}
	tmp = path + ".tmp"
	async with aiofiles.open(tmp, "w", encoding="utf-8") as f:
		await f.write(json.dumps(payload, ensure_ascii=False, indent=2))
	os.replace(tmp, path)

async def load_data(path: str) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any], Set[str]]:
	"""Загрузить данные из JSON-файла. Если файла нет — вернуть пустые структуры."""
	if not os.path.exists(path):
		return {}, {}, set()
	async with aiofiles.open(path, "r", encoding="utf-8") as f:
		data = json.loads(await f.read())
	active_polls = data.get("active_polls", {})
	stats = data.get("stats", {})
	disabled_days = set(d for d in data.get("disabled_days", []) if isinstance(d, str))
	return active_polls, stats, disabled_days



