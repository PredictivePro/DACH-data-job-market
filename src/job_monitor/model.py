from __future__ import annotations

import hashlib
import html
import json
import os
import re
from functools import lru_cache
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

DACH = {"DE", "AT", "CH"}
CITY_COUNTRY = {
    **{norm: "DE" for norm in ("berlin", "hamburg", "munich", "munchen", "muenchen", "cologne", "koln", "koeln", "frankfurt", "frankfurt am main", "stuttgart", "dusseldorf", "duesseldorf", "leipzig", "dresden", "bremen", "hannover", "nuremberg", "nurnberg", "nuernberg", "dortmund", "essen", "mannheim", "karlsruhe", "freiburg", "bonn", "munster", "muenster", "wiesbaden", "aachen", "augsburg", "bochum", "regensburg", "mainz", "rostock", "oldenburg", "heidelberg", "ulm", "saarbrucken", "saarbruecken", "potsdam", "chemnitz", "wurzburg", "würzburg", "ingolstadt", "jena", "trier", "konstanz", "bielefeld", "osnabruck", "osnabrueck", "erlangen", "bayreuth", "flensburg", "kiel", "lubeck", "lübeck", "cuxhaven")},
    **{norm: "AT" for norm in ("vienna", "wien", "graz", "linz", "salzburg", "innsbruck", "klagenfurt", "villach", "st polten", "sankt polten", "wels", "dornbirn", "feldkirch", "bregenz", "leoben", "baden bei wien")},
    **{norm: "CH" for norm in ("zurich", "zuerich", "zürich", "geneva", "geneve", "genf", "basel", "bern", "lausanne", "lugano", "winterthur", "lucerne", "luzern", "st gallen", "sankt gallen", "zug", "neuchatel", "neuenburg", "fribourg", "freiburg im uchtland", "biel", "bienne", "schaffhausen", "aarau", "chur", "bellinzona", "thun", "uster", "yverdon")},
}
TRACKING = {"fbclid", "gclid", "mc_cid", "mc_eid", "ref", "source"}
ROLE_TERMS = {
    "data scientist": ("data scientist", "data science"),
    "data analyst": ("data analyst", "analytics analyst", "datenanalyst"),
    "data engineer": ("data engineer", "dateningenieur"),
    "analytics engineer": ("analytics engineer",),
    "machine learning engineer": ("machine learning engineer", "ml engineer"),
    "AI engineer": ("ai engineer", "artificial intelligence engineer", "ki engineer"),
    "MLOps engineer": ("mlops", "ml ops"),
    "BI developer": ("business intelligence", "bi developer", "power bi"),
    "data architect": ("data architect", "datenarchitekt"),
    "data platform engineer": ("data platform",),
    "Energy data analyst": ("energy data", "energiedaten"),
    "Quantitative analyst": ("quantitative analyst", "quant analyst", "quant analyst"),
    "Energy trading analyst": ("energy trading", "power trading analyst"),
    "Forecasting analyst": ("forecasting analyst", "forecast analyst"),
}


class _Text(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)


def clean_text(value):
    if not value:
        return ""
    parser = _Text()
    try:
        parser.feed(str(value))
        value = " ".join(parser.parts)
    except Exception:
        pass
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


def norm(value):
    value = clean_text(value).casefold()
    value = value.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    return re.sub(r"[^a-z0-9]+", " ", value).strip()


def canonical_url(value):
    if not value:
        return None
    try:
        p = urlsplit(value.strip())
        if p.scheme.lower() not in {"http", "https"} or not p.netloc:
            return value.strip()
        query = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
                 if not k.casefold().startswith("utm_") and k.casefold() not in TRACKING]
        return urlunsplit(("https", p.netloc.casefold().removeprefix("www."),
                           p.path.rstrip("/"), urlencode(sorted(query)), ""))
    except ValueError:
        return value.strip()


def parse_date(value):
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    else:
        text = str(value).strip()
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            try:
                dt = parsedate_to_datetime(text)
            except (TypeError, ValueError, OverflowError):
                return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def country_code(raw):
    text = clean_text(raw).casefold()
    if re.search(r"\b(germany|deutschland|de)\b", text):
        return "DE"
    if re.search(r"\b(austria|oesterreich|österreich|at)\b", text):
        return "AT"
    if re.search(r"\b(switzerland|schweiz|suisse|svizzera|ch)\b", text):
        return "CH"
    folded = norm(text)
    for city, cc in CITY_COUNTRY.items():
        if re.search(r"\b" + re.escape(city) + r"\b", folded):
            return cc
    return None


def classify(title, description="", keywords=None):
    haystack = norm(f"{title or ''} {description or ''}")
    terms = keywords or _configured_role_terms(os.getenv("ROLE_KEYWORDS_JSON", ""))
    for category, aliases in terms.items():
        for alias in aliases:
            if norm(alias) and re.search(r"\b" + re.escape(norm(alias)) + r"\b", haystack):
                return category, f"matched keyword: {alias}"
    return None, None


@lru_cache(maxsize=4)
def _configured_role_terms(setting):
    if not setting.strip():
        return ROLE_TERMS
    try:
        value = json.loads(setting)
        if not isinstance(value, dict) or not value:
            raise ValueError
        terms = {}
        for category, aliases in value.items():
            if not isinstance(category, str) or not isinstance(aliases, list) or not aliases or not all(isinstance(x, str) and x.strip() for x in aliases):
                raise ValueError
            terms[category] = tuple(aliases)
        return terms
    except (ValueError, TypeError):
        raise ValueError("ROLE_KEYWORDS_JSON must be a JSON object of role names to non-empty keyword arrays") from None


def normalize(raw, source, collected_at=None):
    """Convert a source record to the common schema without inventing values."""
    source = source.casefold()
    location = raw.get("location") or raw.get("location_raw") or raw.get("jobGeo") or raw.get("geo") or raw.get("arbeitsort") or ""
    if isinstance(location, dict):
        location = location.get("display_name") or location.get("name") or location.get("ort") or ""
    title = clean_text(raw.get("title") or raw.get("jobTitle") or raw.get("stellenangebotsTitel") or raw.get("beruf")) or None
    company = clean_text(raw.get("company") or raw.get("company_name") or raw.get("companyName") or raw.get("employer") or raw.get("arbeitgeber")) or None
    description = clean_text(raw.get("description") or raw.get("description_text") or raw.get("jobDescription") or raw.get("stellenangebotsBeschreibung")) or None
    app_url = raw.get("application_url") or raw.get("apply_url")
    source_url = raw.get("url") or raw.get("source_url") or raw.get("job_url")
    if source == "jobicy":
        source_url = source_url or raw.get("job_url")
        # The public Jobicy API's supported destination is the Jobicy listing;
        # its direct ATS target is available only via the separately priced API.
        app_url = app_url or source_url
    else:
        app_url = app_url or source_url
    country = raw.get("country")
    country = (str(country).upper() if country and str(country).upper() in DACH else country_code(f"{location} {raw.get('country_name', '')} {country or ''}"))
    if not country and raw.get("remote") and re.search(r"\b(germany|deutschland|austria|oesterreich|switzerland|schweiz)\b", f"{location} {description or ''}", re.I):
        country = country_code(f"{location} {description or ''}")
    city = clean_text(raw.get("city") or raw.get("location_city")) or None
    if not city and location:
        city = clean_text(str(location).split(",")[0]) or None
        if city and norm(city) in {"remote", "europe", "worldwide", "hybrid"}:
            city = None
    tags = raw.get("tags") or raw.get("categories") or []
    workplace = norm(f"{location} {raw.get('remote_type') or ''} {raw.get('workplace_type') or ''} {raw.get('workplace') or ''}")
    remote_flag = raw.get("remote")
    if remote_flag is True or str(remote_flag).casefold() in {"true", "yes", "remote"} or "remote" in workplace or "anywhere" in workplace or "worldwide" in workplace:
        remote_type = "remote"
    elif "hybrid" in workplace:
        remote_type = "hybrid"
    elif "on site" in workplace or "onsite" in workplace or "vor ort" in workplace:
        remote_type = "on-site"
    else:
        remote_type = None
    category, reason = classify(title or "", description or "")
    pub = raw.get("publication_date") or raw.get("created") or raw.get("created_at") or raw.get("date") or raw.get("pubDate") or raw.get("aktuelleVeroeffentlichungsdatum")
    source_id = raw.get("id") or raw.get("source_job_id") or raw.get("slug") or raw.get("refnr") or raw.get("referenznummer")
    desc_hash = hashlib.sha256(norm(description or "").encode()).hexdigest() if description else None
    fallback_url = canonical_url(source_url) or canonical_url(app_url)
    identity = (f"{source}:{source_id}" if source_id is not None else
                f"{source}:{fallback_url}" if fallback_url else
                f"{source}:payload:{hashlib.sha256(json.dumps(raw, sort_keys=True, default=str).encode()).hexdigest()}")
    employment_type = raw.get("employment_type") or raw.get("job_type") or raw.get("jobType") or raw.get("job_types") or raw.get("contract_type")
    if isinstance(employment_type, (list, dict)):
        employment_type = json.dumps(employment_type, ensure_ascii=False)
    record = {
        "job_id": hashlib.sha256(identity.encode()).hexdigest(), "source": source,
        "source_job_id": str(source_id) if source_id is not None else None,
        "title": title, "company": company, "country": country if country in DACH else None,
        "city": city, "location_raw": clean_text(location) or None,
        "description": description, "application_url": app_url, "source_url": source_url,
        "salary_min": raw.get("salary_min") if raw.get("salary_min") is not None else raw.get("annualSalaryMin"),
        "salary_max": raw.get("salary_max") if raw.get("salary_max") is not None else raw.get("annualSalaryMax"),
        "salary_currency": raw.get("salary_currency") or raw.get("currency") or raw.get("salaryCurrency"),
        "employment_type": employment_type,
        "remote_type": remote_type, "publication_date": parse_date(pub),
        "first_seen_at": collected_at or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "last_seen_at": collected_at or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "snapshot_date": date.fromisoformat((collected_at or datetime.now(timezone.utc).isoformat())[:10]).isoformat(),
        "is_new": False, "description_hash": desc_hash,
        "record_hash": hashlib.sha256(json.dumps({k: v for k, v in raw.items() if k not in {"id", "url"}}, sort_keys=True, default=str).encode()).hexdigest(),
        "role_category": category, "classification_reason": reason,
        "source_metadata": json.dumps({"tags": tags} if tags else {}, ensure_ascii=False),
    }
    return record

