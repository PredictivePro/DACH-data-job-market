from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass, field

import requests

from .model import country_code, normalize

LOG = logging.getLogger(__name__)


class SourceError(RuntimeError):
    pass


@dataclass
class Result:
    source: str
    jobs: list[dict] = field(default_factory=list)
    raw_pages: list[dict] = field(default_factory=list)
    requests: int = 0
    errors: list[str] = field(default_factory=list)


class Connector:
    source = "unknown"
    base_url = ""

    def __init__(self, session=None, timeout=20, retries=4, sleep=time.sleep):
        self.session = session or requests.Session()
        self.timeout, self.retries, self.sleep = timeout, retries, sleep
        self.request_count = 0
        self.raw_pages = []
        self.min_interval = 0
        self._last_request = 0.0

    def get_json(self, url, params=None, headers=None):
        for attempt in range(self.retries):
            try:
                wait = self.min_interval - (time.monotonic() - self._last_request)
                if wait > 0:
                    self.sleep(wait)
                self.request_count += 1
                response = self.session.get(url, params=params, headers=headers, timeout=self.timeout)
                self._last_request = time.monotonic()
                if self.source == "adzuna" and response.status_code == 410:
                    return {"results": []}
                if response.status_code == 429 or response.status_code >= 500:
                    if attempt + 1 == self.retries:
                        response.raise_for_status()
                    retry = response.headers.get("Retry-After")
                    wait = float(retry) if retry and retry.isdigit() else min(30, 2 ** attempt) + random.random()
                    self.sleep(wait)
                    continue
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise SourceError(f"{self.source}: expected an object response")
                safe_params = {k: v for k, v in (params or {}).items() if k.casefold() not in {"app_id", "app_key", "key", "token", "api_key"}}
                self.raw_pages.append({"endpoint": url, "params": safe_params, "payload": payload})
                return payload
            except requests.RequestException as exc:
                if attempt + 1 == self.retries:
                    raise SourceError(f"{self.source}: request failed ({type(exc).__name__})") from None
                self.sleep(min(30, 2 ** attempt) + random.random())
            except ValueError as exc:
                raise SourceError(f"{self.source}: invalid JSON response") from None
        raise SourceError(f"{self.source}: request retries exhausted")

    def post_json(self, url, body, headers=None, redact_url_suffix=None):
        """POST JSON with the same timeout, retry and redacted-logging policy as GET."""
        for attempt in range(self.retries):
            try:
                wait = self.min_interval - (time.monotonic() - self._last_request)
                if wait > 0:
                    self.sleep(wait)
                self.request_count += 1
                response = self.session.post(url, json=body, headers=headers, timeout=self.timeout)
                self._last_request = time.monotonic()
                if response.status_code == 429 or response.status_code >= 500:
                    if attempt + 1 == self.retries:
                        response.raise_for_status()
                    retry = response.headers.get("Retry-After")
                    wait = float(retry) if retry and retry.isdigit() else min(30, 2 ** attempt) + random.random()
                    self.sleep(wait)
                    continue
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise SourceError(f"{self.source}: expected an object response")
                safe_body = {k: v for k, v in body.items() if k.casefold() not in {"key", "token", "api_key"}}
                safe_url = url[:-len(redact_url_suffix)] + "{redacted}" if redact_url_suffix else url
                self.raw_pages.append({"endpoint": safe_url, "params": safe_body, "payload": payload})
                return payload
            except requests.RequestException as exc:
                if attempt + 1 == self.retries:
                    raise SourceError(f"{self.source}: request failed ({type(exc).__name__})") from None
                self.sleep(min(30, 2 ** attempt) + random.random())
            except ValueError:
                raise SourceError(f"{self.source}: invalid JSON response") from None
        raise SourceError(f"{self.source}: request retries exhausted")

    def collect(self):
        raise NotImplementedError


class AdzunaConnector(Connector):
    source = "adzuna"
    base_url = "https://api.adzuna.com/v1/api/jobs"
    countries = {"DE": "de", "AT": "at", "CH": "ch"}
    default_roles = ("data engineer", "data analyst", "data scientist", "analytics engineer",
                     "machine learning engineer", "AI engineer", "MLOps engineer", "BI developer",
                     "data architect", "data platform engineer", "quantitative analyst", "forecasting analyst")

    def __init__(self, *args, app_id=None, app_key=None, roles=None, max_pages=5, **kwargs):
        super().__init__(*args, **kwargs)
        self.app_id = app_id or os.getenv("ADZUNA_APP_ID")
        self.app_key = app_key or os.getenv("ADZUNA_APP_KEY")
        configured_roles = os.getenv("ADZUNA_ROLES")
        self.roles = roles or (tuple(x.strip() for x in configured_roles.split(",") if x.strip()) if configured_roles else self.default_roles)
        self.max_pages = max(1, int(os.getenv("ADZUNA_MAX_PAGES", max_pages)))

    def collect(self):
        if not self.app_id or not self.app_key:
            raise SourceError("adzuna: ADZUNA_APP_ID and ADZUNA_APP_KEY are required")
        result = Result(self.source)
        for cc, code in self.countries.items():
            for role in self.roles:
                for page in range(1, self.max_pages + 1):
                    payload = self.get_json(f"{self.base_url}/{code}/search/{page}", {
                        "app_id": self.app_id, "app_key": self.app_key, "results_per_page": 50,
                        "what": role, "content-type": "application/json"})
                    result.requests += 1
                    result.raw_pages.append({"country": cc, "query": role, "page": page, "payload": payload})
                    items = payload.get("results")
                    if not isinstance(items, list):
                        raise SourceError("adzuna: missing results array")
                    result.jobs.extend(normalize({**item, "country": cc}, self.source) for item in items if isinstance(item, dict))
                    if len(items) < 50:
                        break
        return result


class JoobleConnector(Connector):
    """Jooble's country-scoped job-search REST API connector."""
    source = "jooble"
    endpoints = {
        "DE": ("https://de.jooble.org/api/", "Germany"),
        "AT": ("https://at.jooble.org/api/", "Austria"),
        "CH": ("https://ch.jooble.org/api/", "Switzerland"),
    }
    default_roles = AdzunaConnector.default_roles

    def __init__(self, *args, api_keys=None, roles=None, max_pages=2, page_size=50, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_keys = api_keys or {cc: os.getenv(f"JOOBLE_API_KEY_{cc}") for cc in self.endpoints}
        configured_roles = os.getenv("JOOBLE_ROLES")
        self.roles = roles or (tuple(x.strip() for x in configured_roles.split(",") if x.strip())
                               if configured_roles else self.default_roles)
        self.max_pages = max(1, int(os.getenv("JOOBLE_MAX_PAGES", max_pages)))
        self.page_size = max(1, min(50, int(page_size)))

    def collect(self):
        configured = [(cc, self.api_keys.get(cc)) for cc in self.endpoints if self.api_keys.get(cc)]
        if not configured:
            raise SourceError("jooble: JOOBLE_API_KEY_DE, JOOBLE_API_KEY_AT and JOOBLE_API_KEY_CH are not configured")
        result = Result(self.source)
        result.errors.extend(f"{cc}: JOOBLE_API_KEY_{cc} is not configured"
                             for cc in self.endpoints if not self.api_keys.get(cc))
        successful_regions = 0
        keywords = ", ".join(self.roles)
        for cc, key in configured:
            endpoint, location = self.endpoints[cc]
            try:
                for page in range(1, self.max_pages + 1):
                    payload = self.post_json(endpoint + key, {
                        "keywords": keywords, "location": location, "page": str(page),
                        "ResultOnPage": self.page_size, "companysearch": False,
                    }, headers={"Content-Type": "application/json"}, redact_url_suffix=key)
                    result.requests += 1
                    result.raw_pages.append({"country": cc, "page": page, "payload": payload})
                    items = payload.get("jobs")
                    if not isinstance(items, list):
                        raise SourceError("jooble: missing jobs array")
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        row = normalize({
                            "id": item.get("id"), "title": item.get("title"),
                            "company": item.get("company"), "country": cc,
                            "location": item.get("location"), "description": item.get("snippet"),
                            "source_url": item.get("link"), "employment_type": item.get("type"),
                        }, self.source)
                        # Jooble's `link` is a Jooble listing URL, not the employer's application URL.
                        row["application_url"] = None
                        result.jobs.append(row)
                    total = payload.get("totalCount")
                    if not items or len(items) < self.page_size or (isinstance(total, int) and page * self.page_size >= total):
                        break
                successful_regions += 1
            except SourceError as exc:
                result.errors.append(f"{cc}: {exc}")
        if successful_regions == 0:
            if not result.errors:
                raise SourceError("jooble: no country search succeeded")
            raise SourceError("jooble: all configured country searches failed")
        return result


class ArbeitnowConnector(Connector):
    source = "arbeitnow"
    base_url = "https://www.arbeitnow.com/api/job-board-api"

    def __init__(self, *args, max_pages=10, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_pages = max(1, int(max_pages))

    def collect(self):
        result, page = Result(self.source), 1
        while page <= self.max_pages:
            payload = self.get_json(self.base_url, params={"page": page})
            result.requests += 1
            result.raw_pages.append({"page": page, "payload": payload})
            items = payload.get("data")
            if not isinstance(items, list):
                raise SourceError("arbeitnow: missing data array")
            for item in items:
                if not isinstance(item, dict):
                    continue
                location = item.get("location") or ""
                cc = country_code(location)
                # The API is DACH-relevant, but remote listings are only attributed to DACH
                # when the source itself names a DACH country.
                if cc:
                    item = {**item, "country": cc, "application_url": item.get("url")}
                    result.jobs.append(normalize(item, self.source))
            links = payload.get("links") or {}
            if isinstance(links, dict) and links.get("next"):
                page += 1
            elif payload.get("next_page_url"):
                page += 1
            else:
                break
        return result


class JobicyConnector(Connector):
    source = "jobicy"
    base_url = "https://jobicy.com/api/jobs"

    def __init__(self, *args, max_pages=3, tags=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_pages = max(1, int(max_pages))
        self.tags = tags or ("data", "analytics", "machine learning", "artificial intelligence")
        self.min_interval = 1.0

    def collect(self):
        result = Result(self.source)
        seen = set()
        for tag in self.tags:
            cursor = None
            for _ in range(self.max_pages):
                params = {"count": 100, "geo": "europe", "tag": tag}
                if cursor:
                    params["cursor"] = cursor
                payload = self.get_json(self.base_url, params=params)
                result.requests += 1
                result.raw_pages.append({"tag": tag, "cursor": cursor, "payload": payload})
                items = payload.get("jobs")
                if not isinstance(items, list):
                    raise SourceError("jobicy: missing jobs array")
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    identity = str(item.get("id") or item.get("url") or "")
                    if identity in seen:
                        continue
                    seen.add(identity)
                    # Jobicy public API returns its own listing URL. The original ATS URL
                    # requires the paid commercial endpoint, so preserve the public URL.
                    location = item.get("jobGeo") or item.get("geo") or item.get("location") or ""
                    cc = country_code(str(location))
                    item = {**item, "country": cc or country_code(item.get("geo")),
                            "source_url": item.get("url"), "application_url": item.get("url")}
                    normalized = normalize(item, self.source)
                    if normalized.get("country"):
                        result.jobs.append(normalized)
                cursor = payload.get("nextCursor")
                if not cursor:
                    break
        return result


class BundesagenturConnector(Connector):
    """Reserved interface; not enabled pending authoritative API terms and SLA verification."""
    source = "bundesagentur"

    def collect(self):
        raise SourceError("bundesagentur: connector disabled pending verified official access terms and API support")


def enabled_connectors():
    return [AdzunaConnector(), ArbeitnowConnector(), JobicyConnector()]

