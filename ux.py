from typing import Dict, Any

def format_status_overview(poll_data: Dict[str, Any]) -> str:
	"""Return a header line with emoji counts for Yes/No/Maybe.

	Yes identified by answer starting with 'Да', No by 'Нет', Maybe by 'под вопросом' substring.
	"""
	votes = poll_data.get("votes", {})
	yes = sum(1 for v in votes.values() if str(v.get("answer", "")).startswith("Да"))
	no = sum(1 for v in votes.values() if str(v.get("answer", "")).startswith("Нет"))
	maybe = sum(1 for v in votes.values() if "вопрос" in str(v.get("answer", "")).lower())
	return f"✅ Да: {yes}    ❌ Нет: {no}    ❔ Под вопросом: {maybe}\n\n"





