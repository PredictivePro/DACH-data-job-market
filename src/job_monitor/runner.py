from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from .connectors import AdzunaConnector, ArbeitnowConnector, JobicyConnector, JoobleConnector
from .config import load_env
from .model import normalize
from .notifications import send_email
from .store import RunLock, Store, write_gold

LOG = logging.getLogger("job_monitor")


def collect(db_path="data/monitor.sqlite", bronze_path="data/bronze", gold_path="docs/data/monitoring.json",
            send=send_email, connectors=None, notifications_enabled=True):
    lock = RunLock(Path(db_path).with_suffix(".lock"))
    with lock:
        store = Store(db_path)
        run_id = str(uuid.uuid4())
        started = datetime.now(timezone.utc).isoformat(timespec="seconds")
        store.start_run(run_id, started)
        stats, errors, all_records = {}, [], []
        now = datetime.now(timezone.utc)
        for connector in connectors or [AdzunaConnector(), ArbeitnowConnector(), JobicyConnector(), JoobleConnector()]:
            source = connector.source
            try:
                result = connector.collect()
                source_dir = Path(bronze_path) / source / now.strftime("%Y/%m/%d")
                source_dir.mkdir(parents=True, exist_ok=True)
                for index, raw in enumerate(result.raw_pages or getattr(connector, "raw_pages", []), 1):
                    raw_file = source_dir / f"{run_id}-{index:05d}.json"
                    raw_file.write_text(json.dumps({"source": source, "collected_at": started,
                                                    "run_id": run_id, "page": raw}, ensure_ascii=False), encoding="utf-8")
                all_records.extend(result.jobs)
                source_status = "partial_failure" if result.errors else "success"
                stats[source] = {"status": source_status, "requests": getattr(connector, "request_count", result.requests),
                                 "raw_records": sum(1 for page in result.raw_pages for item in (page.get("payload", {}).get("results") or page.get("payload", {}).get("data") or page.get("payload", {}).get("jobs") or []) if isinstance(item, dict)),
                                 "normalized_records": len(result.jobs), "errors": result.errors}
                errors.extend(f"{source}: {message}" for message in result.errors)
            except Exception as exc:
                LOG.error("source failed: %s (%s)", source, type(exc).__name__)
                message = f"{source}: {type(exc).__name__}"
                errors.append(message)
                stats[source] = {"status": "failed", "requests": getattr(connector, "request_count", 0), "errors": [message]}
                partial_pages = getattr(connector, "raw_pages", [])
                if partial_pages:
                    source_dir = Path(bronze_path) / source / now.strftime("%Y/%m/%d")
                    source_dir.mkdir(parents=True, exist_ok=True)
                    for index, raw in enumerate(partial_pages, 1):
                        raw_file = source_dir / f"{run_id}-{index:05d}.json"
                        raw_file.write_text(json.dumps({"source": source, "collected_at": started,
                                                        "run_id": run_id, "page": raw}, ensure_ascii=False), encoding="utf-8")
        observed = datetime.now(timezone.utc).isoformat(timespec="seconds")
        try:
            new_jobs, duplicates = store.ingest(all_records, observed, notify=notifications_enabled)
            for source in stats:
                stats[source]["new_jobs"] = sum(1 for j in new_jobs if j["source"] == source)
                stats[source]["duplicate_jobs"] = store.last_duplicates.get(source, 0)
            pending = store.pending_jobs() if notifications_enabled else []
            notification_status = "suppressed" if not notifications_enabled else "not_needed"
            if pending:
                batch_key = hashlib.sha256("|".join(sorted(j["job_id"] for j in pending)).encode()).hexdigest()
                try:
                    send(pending)
                    store.mark_batch_sent(batch_key, pending)
                    notification_status = "sent"
                except Exception as exc:
                    # Do not log SMTP error text: providers sometimes echo addresses or auth detail.
                    safe_error = type(exc).__name__
                    LOG.error("notification delivery failed (%s)", safe_error)
                    store.mark_batch_failed(batch_key, pending, safe_error)
                    notification_status = "failed"
                    errors.append(f"email: {safe_error}")
            finished = datetime.now(timezone.utc).isoformat(timespec="seconds")
            successful_sources = sum(1 for item in stats.values() if item["status"] in {"success", "partial_failure"})
            status = "success" if not errors else ("partial_failure" if successful_sources else "failed")
            store.finish_run(run_id, started, finished, status, stats, len(new_jobs), duplicates, notification_status, errors)
            write_gold(store, gold_path)
            LOG.info(json.dumps({"run_id": run_id, "status": status, "source_stats": stats,
                                 "new_jobs": len(new_jobs), "duplicates": duplicates,
                                 "notification_status": notification_status, "errors": errors}))
            return 0 if successful_sources else 1
        except Exception as exc:
            safe_error = type(exc).__name__
            errors.append(f"pipeline: {safe_error}")
            finished = datetime.now(timezone.utc).isoformat(timespec="seconds")
            try:
                store.finish_run(run_id, started, finished, "failed", stats, 0, 0, "not_completed", errors)
            except Exception:
                pass
            LOG.error("collection pipeline failed (%s)", safe_error)
            return 1
        finally:
            store.close()


DEFAULT_WEEKDAY_HOURS = {6, 9, 12, 15, 18}
DEFAULT_WEEKEND_HOURS = {9}


def _hours(env_name, default):
    value = os.getenv(env_name)
    if not value:
        return default
    try:
        hours = {int(x.strip()) for x in value.split(",") if x.strip()}
        if not hours or min(hours) < 0 or max(hours) > 23:
            raise ValueError
        return hours
    except ValueError:
        raise ValueError(f"{env_name} must be a comma-separated list of hours 0..23") from None


def is_scheduled_slot(now=None):
    local = (now or datetime.now(timezone.utc)).astimezone(ZoneInfo("Europe/Berlin"))
    allowed = _hours("COLLECTION_WEEKDAY_HOURS", DEFAULT_WEEKDAY_HOURS) if local.weekday() < 5 else _hours("COLLECTION_WEEKEND_HOURS", DEFAULT_WEEKEND_HOURS)
    return local.minute < 5 and local.hour in allowed


def reprocess_bronze(db_path, bronze_path, gold_path):
    """Re-normalize preserved monitor Bronze payloads without replaying email."""
    with RunLock(Path(db_path).with_suffix(".lock")):
        store = Store(db_path)
        run_id = str(uuid.uuid4())
        started = datetime.now(timezone.utc).isoformat(timespec="seconds")
        store.start_run(run_id, started)
        records, stats = [], {}
        for raw_file in sorted(Path(bronze_path).rglob("*.json")):
            try:
                entry = json.loads(raw_file.read_text(encoding="utf-8"))
                source, page = entry["source"], entry["page"]
                observed = entry.get("collected_at")
                if not observed:
                    continue
                payload = page.get("payload", {})
                items = payload.get("results") or payload.get("data") or payload.get("jobs") or []
                if not isinstance(items, list):
                    continue
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    value = dict(item)
                    if source == "adzuna" and page.get("country"):
                        value["country"] = page["country"]
                    elif source == "jobicy":
                        value["source_url"] = value.get("url")
                        value["application_url"] = value.get("application_url") or value.get("url")
                    records.append((observed, normalize(value, source, observed)))
                    stats.setdefault(source, {"status":"success", "requests":0, "raw_records":0, "normalized_records":0, "new_jobs":0})["normalized_records"] += 1
                stats.setdefault(source, {"status":"success", "requests":0, "raw_records":0, "normalized_records":0, "new_jobs":0})["raw_records"] += len(items)
                stats[source]["requests"] += 1
            except (OSError, ValueError, KeyError, TypeError):
                LOG.warning("skipped unreadable Bronze file: %s", raw_file.name)
        records.sort(key=lambda item: item[0])
        added = 0
        for observed, record in records:
            new, _ = store.ingest([record], observed, notify=False)
            added += len(new)
        finished = datetime.now(timezone.utc).isoformat(timespec="seconds")
        store.finish_run(run_id, started, finished, "success", stats, added, 0, "not_replayed", [])
        write_gold(store, gold_path)
        store.close()
        return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="Collect and monitor relevant DACH data/AI jobs")
    parser.add_argument("--db", default=os.getenv("JOB_MONITOR_DB", "data/monitor.sqlite"))
    parser.add_argument("--bronze", default=os.getenv("JOB_MONITOR_BRONZE", "data/bronze"))
    parser.add_argument("--gold", default=os.getenv("JOB_MONITOR_GOLD", "docs/data/monitoring.json"))
    parser.add_argument("--scheduled", action="store_true", help="run only if current Berlin hour is a configured collection hour")
    parser.add_argument("--serve", action="store_true", help="run the timezone-aware schedule as a persistent service")
    parser.add_argument("--healthcheck", action="store_true", help="exit nonzero if no successful collection in the last 48 hours")
    parser.add_argument("--reprocess-bronze", action="store_true", help="re-normalize preserved local Bronze snapshots without sending email")
    parser.add_argument("--bootstrap", action="store_true", help="save the current listings as a baseline without sending email")
    args = parser.parse_args(argv)
    load_env()
    if args.healthcheck:
        store = Store(args.db)
        latest = store.db.execute("SELECT finished_at,status FROM runs WHERE status IN ('success','partial_failure') ORDER BY finished_at DESC LIMIT 1").fetchone()
        store.close()
        if not latest or not latest["finished_at"]:
            LOG.error("health check failed: no completed collection run")
            return 1
        age = datetime.now(timezone.utc) - datetime.fromisoformat(latest["finished_at"])
        if age.total_seconds() > 48 * 3600:
            LOG.error("health check failed: last run is older than 48 hours")
            return 1
        LOG.info("health check ok: latest run %s (%s)", latest["finished_at"], latest["status"])
        return 0
    if args.reprocess_bronze:
        return reprocess_bronze(args.db, args.bronze, args.gold)
    if args.bootstrap and args.serve:
        parser.error("--bootstrap is a one-time run; omit --serve")
    if args.scheduled and not is_scheduled_slot():
        LOG.info("no collection slot at this local time")
        return 0
    if args.serve:
        schedule_service(args.db, args.bronze, args.gold)
        return 0
    return collect(args.db, args.bronze, args.gold, notifications_enabled=not args.bootstrap)


def schedule_service(db_path, bronze_path, gold_path, poll_seconds=20):
    """Long-running timezone-aware scheduler; the host should supervise/restart it."""
    zone = ZoneInfo("Europe/Berlin")
    LOG.info("scheduler started with timezone Europe/Berlin")
    while True:
        local = datetime.now(zone)
        allowed = _hours("COLLECTION_WEEKDAY_HOURS", DEFAULT_WEEKDAY_HOURS) if local.weekday() < 5 else _hours("COLLECTION_WEEKEND_HOURS", DEFAULT_WEEKEND_HOURS)
        if local.hour in allowed and local.minute < 5:
            slot = f"{local.date().isoformat()}T{local.hour:02d}:00 Europe/Berlin"
            try:
                store = Store(db_path)
                claimed = store.claim_slot(slot)
                store.close()
                if claimed:
                    LOG.info("starting scheduled slot %s", slot)
                    collect(db_path, bronze_path, gold_path)
            except Exception as exc:
                LOG.error("scheduled slot failed (%s)", type(exc).__name__)
        time.sleep(poll_seconds)


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    sys.exit(main())

