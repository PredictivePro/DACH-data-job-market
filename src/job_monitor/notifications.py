import html
import os
import smtplib
from email.message import EmailMessage


def build_email(jobs):
    subject = f"[NEW JOBS] {len(jobs)} new DACH Data & AI opportunities discovered"
    cards = []
    for j in jobs:
        location = ", ".join(x for x in (j.get("city"), j.get("country")) if x) or j.get("location_raw") or "Location not supplied"
        salary = ""
        if j.get("salary_min") or j.get("salary_max"):
            salary = f"<p>Salary: {html.escape(str(j.get('salary_min') or ''))}–{html.escape(str(j.get('salary_max') or ''))} {html.escape(str(j.get('salary_currency') or ''))}</p>"
        urls = j.get("application_urls") or ([j["application_url"]] if j.get("application_url") else [])
        link = urls[0] if urls else j.get("source_url")
        link_html = f'<p><a href="{html.escape(link, quote=True)}">View application</a></p>' if link else ""
        other = ", ".join(j.get("sources", []))
        cards.append("<article style='padding:16px 0;border-bottom:1px solid #ddd'>"
                     f"<h2 style='font-size:18px;margin:0 0 8px'>{html.escape(j.get('title') or 'Untitled role')}</h2>"
                     f"<p>{html.escape(j.get('company') or 'Employer not supplied')} · {html.escape(location)}</p>"
                     f"<p>Source: {html.escape(j.get('source') or '')} · Also found on: {html.escape(other)}</p>"
                     f"<p>Aggregator publication date: {html.escape(j.get('publication_date') or 'Not supplied')}<br>"
                     f"First discovered: {html.escape(j.get('first_seen_at') or '')} UTC<br>"
                     f"Workplace: {html.escape(j.get('remote_type') or 'Not supplied')}</p>{salary}{link_html}</article>")
    body = "<!doctype html><html><body style='font:15px Arial,sans-serif;color:#222;max-width:680px;margin:auto;padding:20px'>" + "".join(cards) + "</body></html>"
    return subject, body


def send_email(jobs, environ=None):
    env = environ or os.environ
    required = ("SMTP_HOST", "SMTP_PORT", "EMAIL_FROM", "EMAIL_TO")
    missing = [key for key in required if not env.get(key)]
    if missing:
        raise RuntimeError("email configuration missing: " + ", ".join(missing))
    message = EmailMessage()
    message["Subject"], message["From"], message["To"] = build_email(jobs)[0], env["EMAIL_FROM"], env["EMAIL_TO"]
    message.set_content("New DACH job opportunities are available. View this message as HTML.")
    message.add_alternative(build_email(jobs)[1], subtype="html")
    port = int(env["SMTP_PORT"])
    client = smtplib.SMTP_SSL if port == 465 else smtplib.SMTP
    with client(env["SMTP_HOST"], port, timeout=20) as smtp:
        if port != 465:
            smtp.starttls()
        if env.get("SMTP_USERNAME"):
            smtp.login(env["SMTP_USERNAME"], env.get("SMTP_PASSWORD", ""))
        smtp.send_message(message)

