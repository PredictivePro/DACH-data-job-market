import json
import pathlib
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

from job_monitor.connectors import AdzunaConnector, ArbeitnowConnector, JobicyConnector, SourceError
from job_monitor.model import canonical_url, classify, clean_text, country_code, normalize, parse_date
from job_monitor.runner import collect, is_scheduled_slot, reprocess_bronze
from job_monitor.store import RunLock, Store


def test_normalize_jobicy_api_shape_and_freshness():
    row = normalize({"id": "42", "jobTitle": "Data Engineer", "companyName": "Example GmbH",
                     "jobGeo": "Berlin, Germany", "jobDescription": "<p>Use Python &amp; SQL</p>",
                     "jobType": "Full-time", "annualSalaryMin": 70000, "salaryCurrency": "EUR",
                     "pubDate": "2026-11-23T10:00:00Z", "url": "https://jobicy.com/jobs/42"}, "jobicy",
                    "2026-11-24T08:00:00+00:00")
    assert row["country"] == "DE"
    assert row["title"] == "Data Engineer"
    assert row["application_url"] == row["source_url"]
    assert row["salary_min"] == 70000
    assert row["publication_date"] == "2026-11-23T10:00:00+00:00"
    assert row["first_seen_at"] == "2026-11-24T08:00:00+00:00"
    assert "Use Python & SQL" == row["description"]


def test_normalize_adzuna_company_object_and_redirect_url():
    row = normalize({"id": "adz-1", "title": "Data Engineer",
                     "company": {"__CLASS__": "Company", "display_name": "Example GmbH"},
                     "location": {"display_name": "Berlin, Germany"},
                     "redirect_url": "https://adzuna.example/job/1?utm_source=api"}, "adzuna")
    assert row["company"] == "Example GmbH"
    assert row["application_url"] == "https://adzuna.example/job/1?utm_source=api"


def test_missing_fields_remain_missing():
    row = normalize({"id": "empty", "title": "Data Analyst"}, "arbeitnow")
    assert row["company"] is None
    assert row["salary_min"] is None
    assert row["publication_date"] is None
    assert row["location_raw"] is None


@pytest.mark.parametrize("value, expected", [("Germany", "DE"), ("Wien, Austria", "AT"), ("Zürich, Switzerland", "CH"), ("Hamburg", "DE"), ("Graz", "AT"), ("Basel", "CH"), ("Europe", None)])
def test_country_normalization(value, expected):
    assert country_code(value) == expected


def test_clean_and_canonical_url():
    assert clean_text("<p>Data&nbsp;Engineer</p>") == "Data Engineer"
    assert canonical_url("http://www.example.com/jobs/1/?utm_source=x&ref=y&job=3#top") == "https://example.com/jobs/1?job=3"


def test_classifier_uses_description_and_returns_reason():
    category, reason = classify("Forecasting Analyst", "Build models using Python")
    assert category == "Forecasting analyst"
    assert reason.startswith("matched keyword:")


def test_role_keywords_are_configurable(monkeypatch):
    monkeypatch.setenv("ROLE_KEYWORDS_JSON", '{"Energy data analyst":["grid forecasting analyst"]}')
    category, reason = classify("Grid Forecasting Analyst", "energy market")
    assert category == "Energy data analyst"
    assert reason == "matched keyword: grid forecasting analyst"


def test_parse_timestamps_and_rejects_invalid():
    assert parse_date("2026-11-24T12:00:00+02:00") == "2026-11-24T10:00:00+00:00"
    assert parse_date("n/a") is None


class FakeResponse:
    def __init__(self, payload, status=200, headers=None):
        self.payload, self.status_code, self.headers = payload, status, headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(str(self.status_code))

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def no_sleep(_):
    pass


def test_adzuna_pagination():
    page_one = [{"id": n, "title": "Data Engineer"} for n in range(50)]
    session = FakeSession([FakeResponse({"results": page_one}), FakeResponse({"results": []})] * 3)
    c = AdzunaConnector(session=session, app_id="id", app_key="key", roles=["data engineer"],
                        max_pages=3, sleep=no_sleep)
    result = c.collect()
    assert result.requests == 6
    assert len(result.jobs) == 150  # two pages for each of DE, AT, and CH
    assert len(session.calls) == 6


def test_adzuna_gone_is_end_of_pagination():
    session = FakeSession([FakeResponse({}, 410), FakeResponse({}, 410), FakeResponse({}, 410)])
    result = AdzunaConnector(session=session, app_id="id", app_key="key", roles=["data engineer"], sleep=no_sleep).collect()
    assert result.requests == 3
    assert result.jobs == []


def test_arbeitnow_empty_and_invalid_response():
    empty = ArbeitnowConnector(session=FakeSession([FakeResponse({"data": [], "links": {"next": None}})]), sleep=no_sleep)
    assert empty.collect().jobs == []
    bad = ArbeitnowConnector(session=FakeSession([FakeResponse({"oops": []})]), sleep=no_sleep)
    with pytest.raises(SourceError, match="missing data"):
        bad.collect()


def test_jobicy_cursor_pagination_and_dach_filter():
    session = FakeSession([FakeResponse({"jobs": [{"id": 1, "jobTitle": "Data Analyst", "jobGeo": "Vienna, Austria", "url": "https://jobicy.com/jobs/1"}], "nextCursor": "next"}),
                           FakeResponse({"jobs": [{"id": 2, "jobTitle": "Data Scientist", "jobGeo": "Europe", "url": "https://jobicy.com/jobs/2"}], "nextCursor": None})])
    result = JobicyConnector(session=session, tags=["data"], max_pages=2, sleep=no_sleep).collect()
    assert result.requests == 2
    assert len(result.jobs) == 1
    assert result.jobs[0]["country"] == "AT"
    assert result.jobs[0]["application_url"] == "https://jobicy.com/jobs/1"


def test_retry_after_rate_limit():
    session = FakeSession([FakeResponse({}, 429, {"Retry-After": "2"}), FakeResponse({"data": []})])
    waits = []
    c = ArbeitnowConnector(session=session, sleep=waits.append)
    assert c.collect().requests == 1
    assert 2 in waits


def test_timeout_retries_then_fails_without_leaking_request_details():
    import requests
    session = FakeSession([requests.Timeout("private request detail"), requests.Timeout("private request detail")])
    c = ArbeitnowConnector(session=session, retries=2, sleep=no_sleep)
    with pytest.raises(SourceError, match="Timeout") as exc:
        c.collect()
    assert exc.value.__cause__ is None
    assert c.request_count == 2


def test_run_lock_prevents_overlap(tmp_path):
    lock_path = tmp_path / "run.lock"
    with RunLock(lock_path):
        with pytest.raises(RuntimeError, match="another collection"):
            with RunLock(lock_path):
                pass
    assert not lock_path.exists()


def sample_job(source="arbeitnow", source_id="1", url="https://example.com/jobs/1", city="Berlin", company="Acme"):
    return normalize({"id": source_id, "title": "Senior Data Engineer", "company": company,
                      "location": f"{city}, Germany", "country": "DE", "description": "Build Python data pipelines and analytics systems for our platform.",
                      "url": url}, source, "2026-11-24T08:00:00+00:00")


def test_dedup_exact_url_and_same_company_different_role(tmp_path):
    store = Store(tmp_path / "jobs.sqlite")
    first, _ = store.ingest([sample_job("arbeitnow")], "2026-11-24T08:00:00+00:00")
    duplicate = sample_job("jobicy", "2", "https://example.com/jobs/1?utm_source=x")
    new, dups = store.ingest([duplicate], "2026-11-24T09:00:00+00:00")
    assert first and not new and dups == 1
    assert store.list_jobs()[0]["sources"] == ["arbeitnow", "jobicy"]
    assert store.list_jobs()[0]["dedup_explanations"][0]["match_reason"] == "canonical_url"
    assert store.list_jobs()[0]["dedup_explanations"][0]["match_score"] == 1.0
    different = sample_job("adzuna", "3", "https://example.com/jobs/3", company="Another GmbH")
    new2, _ = store.ingest([different], "2026-11-24T10:00:00+00:00")
    assert len(new2) == 1
    store.close()


def test_fuzzy_match_requires_description_company_and_compatible_location(tmp_path):
    store = Store(tmp_path / "jobs.sqlite")
    store.ingest([sample_job()], "2026-11-24T08:00:00+00:00")
    other = sample_job("jobicy", "2", "https://jobicy.com/2")
    other["city"] = "Berlin"
    new, dups = store.ingest([other], "2026-11-24T09:00:00+00:00")
    assert not new and dups == 1
    distant = sample_job("adzuna", "3", "https://example.org/3", city="Munich")
    new, dups = store.ingest([distant], "2026-11-24T10:00:00+00:00")
    assert new and dups == 0
    store.close()


def test_notifications_idempotent_and_failure_retry(tmp_path):
    db, calls = tmp_path / "jobs.sqlite", []
    connectors = [type("FakeConnector", (), {"source": "arbeitnow", "collect": lambda self: type("R", (), {"jobs": [sample_job()], "raw_pages": [], "requests": 1, "errors": []})()})()]
    assert collect(db, tmp_path / "bronze", tmp_path / "gold.json", send=lambda jobs: (_ for _ in ()).throw(RuntimeError("smtp failure")), connectors=connectors) == 0
    store = Store(db)
    assert len(store.pending_jobs()) == 1
    store.close()
    assert collect(db, tmp_path / "bronze", tmp_path / "gold.json", send=lambda jobs: calls.append(jobs), connectors=connectors) == 0
    assert len(calls) == 1
    assert collect(db, tmp_path / "bronze", tmp_path / "gold.json", send=lambda jobs: calls.append(jobs), connectors=connectors) == 0
    assert len(calls) == 1
    store = Store(db)
    assert not store.pending_jobs()
    store.close()


def test_bootstrap_saves_baseline_without_mail_then_alerts_only_new(tmp_path):
    db = tmp_path / "jobs.sqlite"
    current = sample_job("arbeitnow", "baseline", "https://arbeitnow.com/baseline")
    baseline_connector = [type("FakeConnector", (), {"source":"arbeitnow", "collect":lambda self: type("R", (), {"jobs":[current],"raw_pages":[],"requests":1,"errors":[]})()})()]
    assert collect(db, tmp_path / "bronze", tmp_path / "gold.json", send=lambda jobs: pytest.fail("bootstrap must not send email"), connectors=baseline_connector, notifications_enabled=False) == 0
    store = Store(db)
    assert not store.pending_jobs()
    assert store.metrics()[0]["notification_status"] == "suppressed"
    store.close()
    newly_found = sample_job("arbeitnow", "later", "https://arbeitnow.com/later", company="New Co")
    connector = [type("FakeConnector", (), {"source":"arbeitnow", "collect":lambda self: type("R", (), {"jobs":[current,newly_found],"raw_pages":[],"requests":1,"errors":[]})()})()]
    sent = []
    assert collect(db, tmp_path / "bronze", tmp_path / "gold.json", send=lambda jobs: sent.extend(jobs), connectors=connector) == 0
    assert [j["source_job_id"] for j in sent] == ["later"]


def test_failed_source_keeps_partial_bronze_and_other_source_runs(tmp_path):
    class Broken:
        source, request_count = "broken", 1
        raw_pages = [{"payload": {"results": [{"id": 1}]}}]
        def collect(self):
            raise SourceError("broken: request failed (Timeout)")
    class Healthy:
        source = "arbeitnow"
        def collect(self):
            return type("R", (), {"jobs": [], "raw_pages": [], "requests": 1, "errors": []})()
    db, bronze, gold = tmp_path / "db.sqlite", tmp_path / "bronze", tmp_path / "gold.json"
    assert collect(db, bronze, gold, send=lambda jobs: pytest.fail("no new jobs should email"), connectors=[Broken(), Healthy()]) == 0
    assert list(bronze.rglob("*.json"))
    export = json.loads(gold.read_text(encoding="utf-8"))
    assert export["runs"][0]["status"] == "partial_failure"
    assert json.loads(export["runs"][0]["source_stats"])["arbeitnow"]["status"] == "success"


def test_bronze_reprocess_preserves_first_seen_without_replaying_email(tmp_path):
    bronze = tmp_path / "bronze" / "arbeitnow" / "2026" / "11" / "24"
    bronze.mkdir(parents=True)
    entry = {"source":"arbeitnow", "collected_at":"2026-11-24T08:00:00+00:00", "run_id":"r1",
             "page":{"payload":{"data":[{"id":"9", "title":"Data Engineer", "company_name":"A",
                                           "location":"Berlin, Germany", "url":"https://arbeitnow.com/9",
                                           "description":"Build reliable Python data pipelines."}]}}}
    (bronze / "snapshot.json").write_text(json.dumps(entry), encoding="utf-8")
    db, gold = tmp_path / "db.sqlite", tmp_path / "gold.json"
    assert reprocess_bronze(db, tmp_path / "bronze", gold) == 0
    store = Store(db)
    assert store.list_jobs()[0]["first_seen_at"] == "2026-11-24T08:00:00+00:00"
    assert not store.pending_jobs()
    store.close()
    exported = json.loads(gold.read_text(encoding="utf-8"))
    assert exported["summary"]["jobs_by_country"] == {"DE": 1}
    assert exported["summary"]["jobs_by_source"] == {"arbeitnow": 1}


def test_schedule_berlin_weekday_weekend_and_dst():
    berlin = ZoneInfo("Europe/Berlin")
    assert is_scheduled_slot(datetime(2026, 11, 23, 8, 0, tzinfo=timezone.utc))  # Monday 09:00 CET
    assert not is_scheduled_slot(datetime(2026, 11, 23, 6, 0, tzinfo=timezone.utc))
    assert is_scheduled_slot(datetime(2026, 11, 22, 8, 0, tzinfo=timezone.utc))  # Sunday 09:00 CET
    assert not is_scheduled_slot(datetime(2026, 11, 22, 7, 0, tzinfo=timezone.utc))
    assert datetime(2026, 3, 30, 6, tzinfo=timezone.utc).astimezone(berlin).hour == 8
    assert datetime(2026, 10, 26, 7, tzinfo=timezone.utc).astimezone(berlin).hour == 8
    assert is_scheduled_slot(datetime(2026, 3, 29, 7, 0, tzinfo=timezone.utc))  # DST Sunday 09:00 CEST
    assert is_scheduled_slot(datetime(2026, 10, 25, 8, 0, tzinfo=timezone.utc))  # standard Sunday 09:00 CET


@pytest.mark.parametrize("utc_hour, local_hour", [(4, 6), (7, 9), (10, 12), (13, 15), (16, 18),
                                                   (5, 6), (8, 9), (11, 12), (14, 15), (17, 18)])
def test_all_weekday_slots_follow_berlin_wall_clock(utc_hour, local_hour):
    summer = utc_hour in {4, 7, 10, 13, 16}
    day = datetime(2026, 6, 1 if summer else 11, utc_hour, tzinfo=timezone.utc) if summer else datetime(2026, 11, 2, utc_hour, tzinfo=timezone.utc)
    assert day.astimezone(ZoneInfo("Europe/Berlin")).hour == local_hour
    assert is_scheduled_slot(day)


def test_schedule_hours_are_configurable(monkeypatch):
    monkeypatch.setenv("COLLECTION_WEEKDAY_HOURS", "7, 10")
    assert is_scheduled_slot(datetime(2026, 11, 23, 6, 0, tzinfo=timezone.utc))
    assert not is_scheduled_slot(datetime(2026, 11, 23, 8, 0, tzinfo=timezone.utc))

