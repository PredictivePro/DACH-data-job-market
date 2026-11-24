(() => {
  const $ = id => document.getElementById(id);
  const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]));
  const dateValue = v => v ? new Date(v) : null;
  const berlin = v => {
    const d = dateValue(v);
    return d ? new Intl.DateTimeFormat("en-GB", {dateStyle:"medium", timeStyle:"short", timeZone:"Europe/Berlin"}).format(d) : "Not supplied";
  };
  fetch("data/monitoring.json", {cache:"no-store"}).then(r => r.ok ? r.json() : null).then(data => {
    if (!data || !Array.isArray(data.jobs)) return;
    const jobs = data.jobs;
    const now = Date.now(), day = 86400000;
    const firstSeen = jobs.map(j => dateValue(j.first_seen_at)?.getTime()).filter(Number.isFinite);
    $("monitor-24h").textContent = firstSeen.filter(t => now-t < day).length;
    $("monitor-7d").textContent = firstSeen.filter(t => now-t < 7*day).length;
    $("monitor-freshness").textContent = `Observed job records from ${data.generated_at ? berlin(data.generated_at) : "the latest collection"} (Berlin time). Publication dates come from aggregators; first seen is when this monitor discovered the listing.`;
    const runStats = (data.runs||[]).map(r => ({run:r, stats: (()=>{try{return JSON.parse(r.source_stats||"{}")}catch{return {}}})()}));
    const sources = [...new Set([...jobs.flatMap(j => j.sources || [j.source]), ...runStats.flatMap(x => Object.keys(x.stats))].filter(Boolean))].sort();
    const categories = [...new Set(jobs.map(j => j.role_category).filter(Boolean))].sort();
    const employment = [...new Set(jobs.map(j => j.employment_type).filter(Boolean))].sort();
    sources.forEach(x => $("monitor-source").insertAdjacentHTML("beforeend", `<option>${esc(x)}</option>`));
    categories.forEach(x => $("monitor-category").insertAdjacentHTML("beforeend", `<option>${esc(x)}</option>`));
    employment.forEach(x => $("monitor-employment").insertAdjacentHTML("beforeend", `<option>${esc(x)}</option>`));
    const counts = Object.fromEntries(sources.map(s => [s, {observed:0, duplicates:0, fresh:0}]));
    jobs.forEach(j => {
      const refs = j.source_references || [];
      (j.sources || [j.source]).forEach(s => {
        counts[s].observed++;
        if ((j.sources || []).length > 1) counts[s].duplicates++;
      });
      refs.forEach(ref => {
        const t = dateValue(ref.first_seen_at)?.getTime();
        if (t && now-t < day && counts[ref.source]) counts[ref.source].fresh++;
      });
    });
    $("monitor-source-stats").innerHTML = sources.map(s => {
      const observed = counts[s].observed, dupRate = observed ? Math.round(100*counts[s].duplicates/observed) : 0;
      const attempts = runStats.filter(x => x.stats[s]);
      const successes = attempts.filter(x => x.stats[s].status === "success").length;
      const latest = attempts.find(x => x.stats[s].status === "success");
      const error = attempts.find(x => x.stats[s].status === "failed");
      return `<span>${esc(s)}: ${observed} observed · ${counts[s].fresh} new/24h · ${dupRate}% cross-source duplicates · ${attempts.length ? Math.round(successes*100/attempts.length) : 0}% success · last success ${esc(latest?.run.finished_at ? berlin(latest.run.finished_at) : "none")}${error ? ` · error ${esc((JSON.parse(error.run.errors||"[]").join(", "))||"source failed")}` : ""}</span>`;
    }).join("");
    const controls = ["monitor-title-search","monitor-company-search","monitor-country","monitor-source","monitor-category","monitor-remote","monitor-employment","monitor-first-date","monitor-pub-date"].map($);
    const draw = () => {
      const [titleQuery, companyQuery, country, source, category, remote, employmentType, firstDate, pubDate] = controls.map(e => e.value);
      const filtered = jobs.filter(j => {
        return (!titleQuery || String(j.title||"").toLocaleLowerCase().includes(titleQuery.toLocaleLowerCase())) &&
          (!companyQuery || String(j.company||"").toLocaleLowerCase().includes(companyQuery.toLocaleLowerCase())) && (!country || j.country===country) &&
          (!source || (j.sources||[j.source]).includes(source)) && (!category || j.role_category===category) &&
          (!remote || j.remote_type===remote) && (!employmentType || j.employment_type===employmentType) &&
          (!firstDate || String(j.first_seen_at||"").slice(0,10)>=firstDate) &&
          (!pubDate || String(j.publication_date||"").slice(0,10)>=pubDate);
      });
      $("monitor-jobs").innerHTML = filtered.filter(j => j.role_category).slice(0,300).map(j => {
        const loc = [j.city,j.country].filter(Boolean).join(", ") || j.location_raw || "Location not supplied";
        const salary = j.salary_min || j.salary_max ? `<p>Salary: ${esc(j.salary_min||"")}–${esc(j.salary_max||"")} ${esc(j.salary_currency||"")}</p>` : "";
        const refs = j.source_references||[];
        const links = refs.map(x => x.application_url||x.source_url).filter(Boolean);
        const explain = (j.dedup_explanations||[]).map(x => ({same_source_id:"same source ID",canonical_url:"same canonical URL",title_company_location_description_similarity:"title, employer, location and description similarity"}[x.match_reason]||x.match_reason));
        return `<article class="monitor-job"><h3>${esc(j.title||"Untitled role")}</h3><p><b>${esc(j.company||"Employer not supplied")}</b> · ${esc(loc)}</p>`+
          `<span class="monitor-badge">${esc(j.role_category)}</span>${(j.sources||[j.source]).map(s=>`<span class="monitor-badge">${esc(s)}</span>`).join("")}`+
          (explain.length ? `<p>Grouped by ${[...new Set(explain)].map(esc).join(", ")}</p>` : "") +
          `<p>First seen: ${esc(berlin(j.first_seen_at))} Berlin<br>Aggregator publication: ${esc(berlin(j.publication_date))}<br>Last observed: ${esc(berlin(j.last_seen_at))} Berlin`+
          `${j.remote_type?`<br>Workplace: ${esc(j.remote_type)}`:""}${j.employment_type?`<br>Employment: ${esc(j.employment_type)}`:""}</p>${salary}`+
          (links.length?`<p>${[...new Set(links)].map((u,i)=>`<a href="${esc(u)}" target="_blank" rel="noopener">${i?`Source ${i+1}`:"View listing / apply"}</a>`).join(" · ")}</p>`:"")+`</article>`;
      }).join("") || "<p>No matching monitored jobs. Collection data appears after the collector is deployed.</p>";
    };
    controls.forEach(e => e.addEventListener("input", draw));
    draw();
  }).catch(() => {});
})();
