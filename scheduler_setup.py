from __future__ import annotations

from typing import Any, Callable
from apscheduler.triggers.cron import CronTrigger

from state import WEEKDAY_MAP

def setup_scheduler_jobs(
	scheduler,
	polls_config: list,
	disabled_days: set,
	tz,
	start_poll_cb: Callable[[dict], Any],
	send_summary_by_day_cb: Callable[[dict], Any],
	save_data_cb: Callable[[], Any],
	log,
) -> None:
	"""Зарегистрировать все плановые задания (опросы, итоги, автосейв, бэкап)."""
	scheduler.remove_all_jobs()

	def _schedule_poll_job(poll):
		start_poll_cb(poll)

	def _schedule_summary_job(poll):
		send_summary_by_day_cb(poll)

	for idx, poll in enumerate(polls_config):
		try:
			if poll.get("day") in disabled_days:
				log.info("⏭️ Skipping scheduling for %s (disabled)", poll.get("day"))
				continue
			tp = list(map(int, poll["time_poll"].split(":")))
			tg = list(map(int, poll["time_game"].split(":")))
			poll_job_id = f"poll_{poll['day']}_{idx}"
			scheduler.add_job(
				_schedule_poll_job,
				trigger=CronTrigger(
					day_of_week=poll["day"],
					hour=tp[0],
					minute=tp[1],
					timezone=tz,
				),
				args=[poll],
				id=poll_job_id,
			)
			summary_hour = max(tg[0] - 1, 0)
			summary_job_id = f"summary_{poll['day']}_{idx}"
			if poll["day"] in ("tue", "thu"):
				summary_dow = poll["day"]
			else:
				day_index = WEEKDAY_MAP[poll["day"]]
				next_day_index = (day_index + 1) % 7
				summary_dow = list(WEEKDAY_MAP.keys())[next_day_index]

			scheduler.add_job(
				_schedule_summary_job,
				trigger=CronTrigger(
					day_of_week=summary_dow,
					hour=summary_hour,
					minute=tg[1],
					timezone=tz,
				),
				args=[poll],
				id=summary_job_id,
			)
			log.info("✅ Scheduled poll for %s at %s (Kaliningrad)", poll['day'], poll['time_poll'])
		except Exception:
			log.exception("Failed to schedule poll: %s", poll)

	try:
		scheduler.add_job(lambda: save_data_cb(), "interval", minutes=10)
	except Exception:
		log.exception("Failed to schedule autosave job")

	try:
		scheduler.add_job(lambda: None, "cron", hour=3, minute=0, timezone=tz)
	except Exception:
		log.exception("Failed to schedule backup job")





