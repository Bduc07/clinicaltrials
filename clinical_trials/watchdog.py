"""Record failed refreshes and gaps, and deliver optional Discord alerts."""
import argparse
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from .pipeline import cohort_dir


def record_alerts(log, checked_at):
    log.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            alert_key TEXT PRIMARY KEY, detected_at TEXT NOT NULL, message TEXT NOT NULL,
            delivered_at TEXT
        )
    """)
    for run_id, started_at in log.execute("SELECT run_id, started_at FROM runs WHERE status = 'failed'"):
        log.execute("INSERT OR IGNORE INTO alerts VALUES (?, ?, ?, NULL)", (
            f"failure:{run_id}", checked_at.isoformat(),
            f"ClinicalTrials.gov daily refresh failed. Run {run_id} started at {started_at}.",
        ))
    last_start, = log.execute("SELECT max(started_at) FROM runs").fetchone()
    if last_start and checked_at - datetime.fromisoformat(last_start) >= timedelta(hours=26):
        log.execute("INSERT OR IGNORE INTO alerts VALUES (?, ?, ?, NULL)", (
            f"gap:{last_start}", checked_at.isoformat(),
            f"ClinicalTrials.gov refresh missed its schedule. No run has started in 26 hours. "
            f"Last start: {last_start}.",
        ))
    log.commit()


def deliver_alerts(log, webhook):
    endpoint = urlsplit(webhook)
    if endpoint.scheme != "https" or endpoint.netloc != "discord.com" or not endpoint.path.startswith("/api/webhooks/"):
        raise ValueError("Use an HTTPS Discord webhook URL from discord.com")
    for key, message in log.execute("SELECT alert_key, message FROM alerts WHERE delivered_at IS NULL").fetchall():
        request = Request(webhook, method="POST", headers={"Content-Type": "application/json",
                                                           "User-Agent": "ClinicalTrialsWatchdog/1.0"},
                          data=json.dumps({"content": message, "allowed_mentions": {"parse": []}}).encode())
        try:
            with urlopen(request, timeout=15) as response:
                if not 200 <= response.status < 300:
                    raise RuntimeError("Discord did not accept the alert")
        except Exception as error:
            # Keep the destination URL out of logs and leave the alert queued.
            raise RuntimeError(f"Discord delivery failed ({type(error).__name__})") from None
        log.execute("UPDATE alerts SET delivered_at = ? WHERE alert_key = ?",
                    (datetime.now(timezone.utc).isoformat(), key))
        log.commit()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    operations = cohort_dir(parser.parse_args().data_dir, None) / "operations"
    with closing(sqlite3.connect(operations / "runs.sqlite")) as log:
        record_alerts(log, datetime.now(timezone.utc))
        config = operations / "discord-webhook.txt"
        if config.is_file():
            deliver_alerts(log, config.read_text(encoding="utf-8-sig").strip())
        pending, = log.execute("SELECT count(*) FROM alerts WHERE delivered_at IS NULL").fetchone()
        print(f"Watchdog completed. Pending alerts: {pending}")


if __name__ == "__main__":
    main()
