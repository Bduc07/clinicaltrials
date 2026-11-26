"""Run and record the daily incremental refresh on a persistent host."""
import argparse
import json
import logging
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import duckdb

from .api import Client
from .pipeline import REPORTS, cohort_dir, read_sql, sync
from .watchdog import record_alerts


def now():
    return datetime.now(timezone.utc)


def refresh(root):
    cohort = cohort_dir(root, None)
    database = cohort / "trials.duckdb"
    operations = cohort / "operations"
    operations.mkdir(parents=True, exist_ok=True)
    run_id = uuid4().hex
    started = now()
    clock = perf_counter()
    sync_result = {}
    changes = None
    counts = {}
    with closing(sqlite3.connect(operations / "runs.sqlite")) as log:
        log.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, status TEXT NOT NULL,
                finished_at TEXT, duration_seconds REAL, source_timestamp TEXT,
                rows_fetched INTEGER, status_changes INTEGER, error TEXT
            )
        """)
        record_alerts(log, started)
        log.execute("INSERT INTO runs (run_id, started_at, status) VALUES (?, ?, 'running')",
                    (run_id, started.isoformat()))
        log.commit()
        status = "failed"
        error = None
        try:
            with duckdb.connect(str(database), read_only=True) as con:
                previous, = con.execute("SELECT max(source_timestamp) FROM pipeline_runs").fetchone()
            if previous is None:
                raise RuntimeError("A completed registry bootstrap is required before scheduling daily refreshes")
            sync_result = sync(Client(), root, None, page_size=1000, engine="duckdb")
            with duckdb.connect(str(database), read_only=True) as con:
                snapshot, = con.execute("SELECT max(source_timestamp) FROM pipeline_runs").fetchone()
                if (started.date() - snapshot.date()).days > 5:
                    raise RuntimeError(f"Warehouse freshness check failed: latest source snapshot is {snapshot}")
                issues = con.execute(read_sql("quality")).fetchall()
                if issues:
                    raise RuntimeError(f"Warehouse quality checks failed: {issues}")
                changes, = con.execute(
                    "SELECT count(*) FROM dim_trial_status WHERE valid_to > ?",
                    [previous],
                ).fetchone()
                for name in REPORTS:
                    sql = read_sql(name).strip().rstrip(";")
                    counts[name], = con.execute(f"SELECT count(*) FROM ({sql})").fetchone()
                duration_report = con.execute(read_sql("average_duration_by_phase")).fetchall()
                monthly_report = con.execute(read_sql("monthly_status_changes")).fetchall()
            summary = {
                "run_id": run_id, "source": "ClinicalTrials.gov",
                "data_timestamp": snapshot.isoformat(), "source_unchanged": sync_result.get("skipped", False),
                "rows_fetched": sync_result.get("studies", 0), "status_changes": changes,
                "report_row_counts": counts, "average_duration_by_phase": duration_report,
                "monthly_status_changes": monthly_report,
            }
            pending = operations / f"{run_id}.tmp"
            pending.write_text(json.dumps(summary, indent=2, default=str) + "\n", encoding="utf-8")
            pending.replace(operations / "latest.json")
            logging.info("Completed %s at source snapshot %s", run_id, snapshot)
            status = "succeeded"
        except Exception as exc:
            error = str(exc)
            logging.exception("Daily registry run %s failed", run_id)
            raise
        finally:
            log.execute("""
                UPDATE runs SET status = ?, finished_at = ?, duration_seconds = ?,
                    source_timestamp = ?, rows_fetched = ?, status_changes = ?, error = ?
                WHERE run_id = ?
            """, (status, now().isoformat(), round(perf_counter() - clock, 2),
                  sync_result.get("data_timestamp"), sync_result.get("studies", 0) if sync_result else None,
                  changes, error, run_id))
            log.commit()
            record_alerts(log, now())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args()
    operations = cohort_dir(args.data_dir, None) / "operations"
    operations.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(filename=operations / "daily.log", level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", encoding="utf-8")
    refresh(args.data_dir)


if __name__ == "__main__":
    main()
