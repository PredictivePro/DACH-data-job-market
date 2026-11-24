from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from .model import canonical_url

FIELDS = ("source source_job_id title company country city location_raw description application_url source_url salary_min salary_max salary_currency employment_type remote_type publication_date first_seen_at last_seen_at snapshot_date is_new description_hash record_hash role_category classification_reason source_metadata match_reason match_score")
FIELD_LIST = FIELDS.split()


class RunLock:
    def __init__(self, path):
        self.path = Path(path)
        self.fd = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(self.fd, str(os.getpid()).encode())
        except FileExistsError as exc:
            try:
                pid = int(self.path.read_text(encoding="ascii").strip())
                os.kill(pid, 0)
            except ProcessLookupError:
                self.path.unlink(missing_ok=True)
                return self.__enter__()
            except (ValueError, OSError):
                pass
            raise RuntimeError("another collection run holds the lock") from exc
        return self

    def __exit__(self, *_):
        if self.fd is not None:
            os.close(self.fd)
            self.path.unlink(missing_ok=True)


class Store:
    def __init__(self, db_path="data/monitor.sqlite"):
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self._init()

    def _init(self):
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
          job_id TEXT PRIMARY KEY, canonical_id TEXT NOT NULL,
          source TEXT NOT NULL, source_job_id TEXT, title TEXT, company TEXT,
          country TEXT, city TEXT, location_raw TEXT, description TEXT,
          application_url TEXT, source_url TEXT, salary_min REAL, salary_max REAL,
          salary_currency TEXT, employment_type TEXT, remote_type TEXT,
          publication_date TEXT, first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
          snapshot_date TEXT, is_new INTEGER NOT NULL DEFAULT 0,
          description_hash TEXT, record_hash TEXT, role_category TEXT,
          classification_reason TEXT, source_metadata TEXT, match_reason TEXT, match_score REAL,
          notified_at TEXT, UNIQUE(source, source_job_id)
        );
        CREATE INDEX IF NOT EXISTS jobs_canonical_id ON jobs(canonical_id);
        CREATE TABLE IF NOT EXISTS runs (
          run_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
          duration_seconds REAL, status TEXT NOT NULL, source_stats TEXT,
          new_jobs INTEGER DEFAULT 0, duplicate_jobs INTEGER DEFAULT 0,
          notification_status TEXT, errors TEXT
        );
        CREATE TABLE IF NOT EXISTS notification_outbox (
          job_id TEXT PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending',
          attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT, sent_at TEXT,
          FOREIGN KEY(job_id) REFERENCES jobs(job_id)
        );
        CREATE TABLE IF NOT EXISTS notification_batches (
          batch_key TEXT PRIMARY KEY, job_ids TEXT NOT NULL, status TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT, sent_at TEXT
        );
        CREATE TABLE IF NOT EXISTS scheduled_slots (
          slot_key TEXT PRIMARY KEY, claimed_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS observations (
          source TEXT NOT NULL, source_job_id TEXT NOT NULL, job_id TEXT NOT NULL,
          observed_at TEXT NOT NULL, record_hash TEXT,
          PRIMARY KEY(source, source_job_id, observed_at)
        );
        """)
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(jobs)")}
        for name, kind in (("match_reason", "TEXT"), ("match_score", "REAL")):
            if name not in columns:
                self.db.execute(f"ALTER TABLE jobs ADD COLUMN {name} {kind}")
        self.db.commit()

    def close(self):
        self.db.close()

    def start_run(self, run_id, started_at):
        self.db.execute("INSERT INTO runs(run_id, started_at, status) VALUES(?,?,?)", (run_id, started_at, "running"))
        self.db.commit()

    @staticmethod
    def _sim(a, b):
        return SequenceMatcher(None, str(a or "").casefold(), str(b or "").casefold()).ratio()

    def _find_duplicate(self, job):
        self.last_match_reason, self.last_match_score = "new", None
        if job.get("source_job_id"):
            row = self.db.execute("SELECT canonical_id FROM jobs WHERE source=? AND source_job_id=?",
                                  (job["source"], job["source_job_id"])).fetchone()
            if row:
                self.last_match_reason, self.last_match_score = "same_source_id", 1.0
                return row[0]
        urls = {canonical_url(u) for u in (job.get("application_url"), job.get("source_url")) if u}
        candidates = self.db.execute("SELECT * FROM jobs").fetchall()
        # Exact canonical URL across source references.
        for row in candidates:
            row_urls = {canonical_url(u) for u in (row["application_url"], row["source_url"]) if u}
            if urls.intersection(row_urls):
                self.last_match_reason, self.last_match_score = "canonical_url", 1.0
                return row["canonical_id"]
        # Strong fuzzy match: title, company, compatible location, and description.
        # Missing evidence never increases the chance of a merge.
        for row in candidates:
            if row["source"] == job["source"]:
                continue
            if not job.get("title") or not job.get("company") or not job.get("description"):
                continue
            if not row["title"] or not row["company"] or not row["description"]:
                continue
            title_score = self._sim(job["title"], row["title"])
            company_score = self._sim(job["company"], row["company"])
            if title_score < .92 or company_score < .9:
                continue
            if job.get("country") and row["country"] and job["country"] != row["country"]:
                continue
            if job.get("city") and row["city"] and self._sim(job["city"], row["city"]) < .85:
                continue
            description_score = self._sim(job["description"], row["description"])
            if description_score >= .84:
                self.last_match_reason = "title_company_location_description_similarity"
                self.last_match_score = round((title_score + company_score + description_score) / 3, 4)
                return row["canonical_id"]
        return None

    def ingest(self, records, observed_at, notify=True):
        new_jobs, duplicate_count = [], 0
        self.last_duplicates = {}
        for job in records:
            canonical = self._find_duplicate(job)
            job["match_reason"] = self.last_match_reason
            job["match_score"] = self.last_match_score
            existing_canonical = self.db.execute("SELECT 1 FROM jobs WHERE canonical_id=? LIMIT 1", (canonical,)).fetchone() if canonical else None
            if canonical:
                duplicate_count += 1
                self.last_duplicates[job["source"]] = self.last_duplicates.get(job["source"], 0) + 1
            else:
                canonical = job["job_id"]
            if job.get("source_job_id"):
                seen = self.db.execute("SELECT job_id FROM jobs WHERE source=? AND source_job_id=?",
                                       (job["source"], job["source_job_id"])).fetchone()
            else:
                seen = self.db.execute("SELECT job_id FROM jobs WHERE source=? AND source_job_id IS NULL AND job_id=?",
                                       (job["source"], job["job_id"])).fetchone()
            if seen:
                existing = self.db.execute("SELECT last_seen_at FROM jobs WHERE job_id=?", (seen[0],)).fetchone()
                latest_seen = max(existing[0], observed_at)
                updates = [f"{f}=?" for f in FIELD_LIST if f not in {"job_id", "source", "source_job_id", "first_seen_at", "last_seen_at", "is_new"}]
                vals = [job.get(f) for f in FIELD_LIST if f not in {"job_id", "source", "source_job_id", "first_seen_at", "last_seen_at", "is_new"}]
                self.db.execute(f"UPDATE jobs SET last_seen_at=?,{','.join(updates)} WHERE job_id=?",
                                [latest_seen, *vals, seen[0]])
                self.db.execute("INSERT OR IGNORE INTO observations VALUES(?,?,?,?,?)",
                                (job["source"], job.get("source_job_id") or job["job_id"], seen[0], observed_at, job.get("record_hash")))
                continue
            job["first_seen_at"] = job["last_seen_at"] = observed_at
            job["is_new"] = not bool(existing_canonical)
            values = [job.get(f) for f in FIELD_LIST]
            self.db.execute(f"INSERT OR IGNORE INTO jobs(job_id,canonical_id,{','.join(FIELD_LIST)}) VALUES(?,{','.join('?' for _ in range(len(FIELD_LIST)+1))})",
                            [job["job_id"], canonical, *values])
            self.db.execute("INSERT OR IGNORE INTO observations VALUES(?,?,?,?,?)",
                            (job["source"], job.get("source_job_id") or job["job_id"], job["job_id"], observed_at, job.get("record_hash")))
            if job["is_new"]:
                new_jobs.append(job)
                if notify and job.get("role_category"):
                    self.db.execute("INSERT OR IGNORE INTO notification_outbox(job_id) VALUES(?)", (job["job_id"],))
        self.db.commit()
        return new_jobs, duplicate_count

    def finish_run(self, run_id, started, finished, status, stats, new_count, duplicates, notification_status, errors):
        start = datetime.fromisoformat(started)
        end = datetime.fromisoformat(finished)
        duration = (end - start).total_seconds()
        self.db.execute("UPDATE runs SET finished_at=?, duration_seconds=?, status=?, source_stats=?, new_jobs=?, duplicate_jobs=?, notification_status=?, errors=? WHERE run_id=?",
                        (finished, duration, status, json.dumps(stats), new_count, duplicates, notification_status, json.dumps(errors), run_id))
        self.db.commit()

    def list_jobs(self):
        rows = self.db.execute("SELECT * FROM jobs ORDER BY first_seen_at ASC").fetchall()
        jobs = {}
        for r in rows:
            canonical = r["canonical_id"]
            if canonical in jobs:
                continue
            job = dict(r)
            job["is_new"] = bool(job["is_new"])
            refs = [dict(x) for x in self.db.execute("SELECT source,source_job_id,source_url,application_url,first_seen_at,last_seen_at,match_reason,match_score FROM jobs WHERE canonical_id=? ORDER BY source", (canonical,))]
            job["source_references"] = refs
            job["sources"] = sorted({x["source"] for x in refs})
            job["dedup_explanations"] = [x for x in refs if x["match_reason"] not in (None, "new")]
            job["first_seen_at"] = min(x["first_seen_at"] for x in refs)
            job["last_seen_at"] = max(x["last_seen_at"] for x in refs)
            job["application_urls"] = sorted({x["application_url"] for x in refs if x["application_url"]})
            jobs[canonical] = job
        return sorted(jobs.values(), key=lambda item: item["first_seen_at"], reverse=True)

    def pending_jobs(self, job_ids=None):
        if job_ids is None:
            rows = self.db.execute("SELECT j.* FROM jobs j JOIN notification_outbox o USING(job_id) WHERE o.status='pending' ORDER BY j.first_seen_at")
        else:
            marks = ",".join("?" for _ in job_ids)
            rows = self.db.execute(f"SELECT j.* FROM jobs j JOIN notification_outbox o USING(job_id) WHERE o.status='pending' AND j.job_id IN ({marks}) ORDER BY j.first_seen_at", list(job_ids))
        return [dict(r) for r in rows]

    def mark_batch_sent(self, batch_key, jobs):
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        ids = [j["job_id"] for j in jobs]
        self.db.execute("INSERT INTO notification_batches(batch_key,job_ids,status,attempts,sent_at) VALUES(?,?, 'sent', 1, ?) ON CONFLICT(batch_key) DO UPDATE SET status='sent', attempts=notification_batches.attempts+1, last_error=NULL, sent_at=excluded.sent_at",
                        (batch_key, json.dumps(ids), now))
        for job_id in ids:
            self.db.execute("UPDATE notification_outbox SET status='sent', attempts=attempts+1, sent_at=?, last_error=NULL WHERE job_id=?", (now, job_id))
            self.db.execute("UPDATE jobs SET notified_at=? WHERE job_id=?", (now, job_id))
        self.db.commit()

    def mark_batch_failed(self, batch_key, jobs, error):
        ids = [j["job_id"] for j in jobs]
        self.db.execute("INSERT INTO notification_batches(batch_key,job_ids,status,attempts,last_error) VALUES(?,?, 'failed', 1, ?) ON CONFLICT(batch_key) DO UPDATE SET status='failed', attempts=notification_batches.attempts+1, last_error=excluded.last_error",
                        (batch_key, json.dumps(ids), error[:500]))
        for job_id in ids:
            self.db.execute("UPDATE notification_outbox SET attempts=attempts+1,last_error=? WHERE job_id=?", (error[:500], job_id))
        self.db.commit()

    def metrics(self):
        return [dict(r) for r in self.db.execute("SELECT run_id, started_at, finished_at, duration_seconds, status, source_stats, new_jobs, duplicate_jobs, notification_status, errors FROM runs ORDER BY started_at DESC")]

    def claim_slot(self, slot_key):
        try:
            self.db.execute("INSERT INTO scheduled_slots VALUES(?,?)", (slot_key, datetime.now(timezone.utc).isoformat(timespec="seconds")))
            self.db.commit()
            return True
        except sqlite3.IntegrityError:
            return False


def write_gold(store, output):
    """Export an additive dashboard view without altering existing datasets."""
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    jobs, runs = store.list_jobs(), store.metrics()
    def counts(field):
        totals = {}
        for job in jobs:
            value = job.get(field)
            if value:
                totals[str(value)] = totals.get(str(value), 0) + 1
        return dict(sorted(totals.items(), key=lambda item: (-item[1], item[0].casefold())))
    by_source = {}
    for job in jobs:
        for source in job.get("sources", []):
            by_source[source] = by_source.get(source, 0) + 1
    reliability = {}
    daily_runs = {}
    for run in runs:
        day = (run.get("started_at") or "")[:10]
        daily_runs.setdefault(day, {"runs":0, "new_jobs":0, "duplicates":0, "errors":0})
        daily_runs[day]["runs"] += 1
        daily_runs[day]["new_jobs"] += run.get("new_jobs", 0) or 0
        daily_runs[day]["duplicates"] += run.get("duplicate_jobs", 0) or 0
        daily_runs[day]["errors"] += len(json.loads(run.get("errors") or "[]"))
        for source, metrics in json.loads(run.get("source_stats") or "{}").items():
            item = reliability.setdefault(source, {"runs":0, "successes":0, "failures":0, "new_jobs":0, "last_success_at":None, "last_error":None})
            item["runs"] += 1
            item["successes"] += metrics.get("status") == "success"
            item["failures"] += metrics.get("status") != "success"
            item["new_jobs"] += metrics.get("new_jobs", 0) or 0
            if metrics.get("status") == "success" and not item["last_success_at"]:
                item["last_success_at"] = run.get("finished_at")
            if metrics.get("status") == "failed" and not item["last_error"]:
                item["last_error"] = (metrics.get("errors") or ["source failed"])[0]
    for item in reliability.values():
        item["success_rate"] = round(item["successes"] / item["runs"], 4) if item["runs"] else 0
    summary = {"observed_jobs":len(jobs), "newly_discovered_jobs":sum(bool(j.get("is_new")) for j in jobs),
               "cross_source_duplicate_rate": round(sum(len(j.get("sources", [])) > 1 for j in jobs) / len(jobs), 4) if jobs else 0,
               "jobs_by_country":counts("country"), "jobs_by_source":by_source,
               "jobs_by_company":counts("company"), "jobs_by_role_category":counts("role_category"),
               "jobs_by_publication_date":counts("publication_date"), "jobs_by_first_seen_date":counts("snapshot_date"),
               "jobs_by_remote_type":counts("remote_type"), "jobs_by_employment_type":counts("employment_type"),
               "daily_collection_statistics":dict(sorted(daily_runs.items())), "source_reliability":reliability}
    tmp.write_text(json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(),
                               "summary": summary, "jobs": jobs, "runs": runs},
                              ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

