#!/usr/bin/env python3
"""
Reddit Demand-Signal Scanner (RSS + comments)
=============================================
Repurposed from the original Reddit Job Hunter.

Market-research tool: sweeps subreddits for posts expressing an unmet need
-- "I wish this app existed", "is there a tool that...", "someone should
build...", "I'd pay for..." -- AND pulls the post's full body + top comments
so the demand can be validated by what other people say underneath.

No Reddit API key needed. Public search RSS + public .json comment endpoint.

Pipeline:
  1. For each subreddit, run Reddit's search RSS for each "signal" (literal
     demand phrases joined with OR). This FINDS candidate posts.
  2. A curated regex per signal runs over title+body to CONFIRM the hit and
     LABEL which phrase fired (kills Reddit's fuzzy-stemming false positives).
  3. For every unique confirmed post, fetch full selftext + top comments
     from reddit.com/comments/<id>.json (this is the "read the comments" pass).
  4. Append everything to demand_signals.csv.

Run:  python3 main.py     (re-running only appends new posts)
"""

import calendar
import csv
import html
import json
import os
import re
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import requests

# ============== CONFIGURATION ==============

# Subreddits to scan (no "r/" prefix). Public, no auth needed.
SUBREDDITS = [
    # --- core web/backend dev (where PDF-gen pain lives) ---
    "webdev", "node", "javascript", "reactjs", "nextjs", "typescript",
    "Python", "django", "flask", "PHP", "laravel", "rubyonrails", "ruby",
    "dotnet", "csharp", "golang", "java", "rust",
    # --- infra / serverless / where headless-chrome pain bites ---
    "aws", "serverless", "devops", "docker", "selfhosted", "webhosting",
    # --- builders / buyers who ship documents ---
    "SaaS", "SideProject", "indiehackers", "webdevelopment", "flutterdev",
    # --- automation / no-code (can't run a browser, need a PDF step) ---
    "n8n", "nocode", "Zapier", "automation", "make",
]

OUTPUT_CSV = Path(__file__).parent / "doclinth_signals.csv"

# Durable dedup memory (post_id -> first-seen ISO timestamp). The GitHub Actions
# workflow commits this file back to the repo after each run, so it survives the
# ephemeral runner between 10-minute scans. That's what stops the same post from
# being pushed to Discord more than once, even though a fresh runner starts with
# no other state and the "hour" scan window deliberately overlaps between runs.
SEEN_IDS_JSON = Path(__file__).parent / "seen_ids.json"
# How long to remember a post id. Only needs to outlast how long a post can keep
# showing up in the scan window (~1h with t=hour); a few days is a safe cushion
# and keeps the file to a few hundred ids at most.
SEEN_ID_RETENTION_DAYS = 3

# Reddit search time window: "all", "year", "month", "week", "day".
# Local first sweep: "month". On Render (daily cron), set env REDDIT_TIME_WINDOW=day
# so each run only sees the last 24h -> no duplicate Discord alerts, no state file.
TIME_WINDOW = os.environ.get("REDDIT_TIME_WINDOW", "month")

# Discord webhook for push alerts (required on Render, where the CSV is ephemeral).
# Set it as an env var in the Render dashboard; never hard-code it here.
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

# Hard date floor (YYYY-MM-DD). None to disable (rely on TIME_WINDOW + dedupe).
MIN_POST_DATE = None

# Keep posts newer than N hours (None = ignore, rely on MIN_POST_DATE).
MAX_POST_AGE_HOURS = None

# Parallelism. Reddit throttles a single IP under sustained sequential load
# (we measured ~12s/post late in a long run), but serves several concurrent
# RSS requests fine. Keep workers modest to stay polite / avoid 429s.
SEARCH_WORKERS = 4
COMMENT_WORKERS = 4

# Comment-reading pass.
FETCH_COMMENTS = False
MAX_COMMENTS_PER_POST = 15      # top comments to keep per post
MAX_COMMENT_CHARS = 1800        # truncate stored comment blob
MAX_SELFTEXT_CHARS = 1500       # truncate stored body
MAX_COMMENT_FETCHES = 6000      # safety cap on total comment requests
CHECKPOINT_EVERY = 100          # rewrite CSV after this many enriched posts

USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# ---- Doclinth signals: queries cast the net, pattern confirms the hit ----
# Every signal is scoped to PDF generation so we only surface people who are
# actually fighting the pain Doclinth solves (template/JSON -> PDF over HTTP).
SIGNALS = {
    # The infra pain: headless-browser / library route hurting.
    "pdf_tech_pain": {
        "queries": [
            "wkhtmltopdf", "puppeteer pdf", "playwright pdf", "html to pdf",
            "chromium pdf", "headless chrome pdf", "weasyprint", "gotenberg",
            "generate pdf serverless", "pdf lambda", "pdf generation",
        ],
        "pattern": r"(wkhtmltopdf|weasyprint|gotenberg|prince\s?xml|(puppeteer|playwright|chromium|headless\s+chrom\w+)[\s\S]{0,40}pdf|pdf[\s\S]{0,40}(puppeteer|playwright|chromium|headless|lambda|serverless|docker|memory|ram)|html\s*(to|2|->)\s*pdf|pdf\s+(generation|rendering|from\s+html))",
    },
    # Actively shopping: recommendation / "what do you use" for PDF.
    "pdf_tool_request": {
        "queries": [
            "best way to generate pdf", "pdf generation library",
            "how to generate pdf", "pdf api", "generate pdf from html",
            "what do you use to generate pdf", "pdf generation service",
            "recommend pdf library", "generate pdf from json",
        ],
        "pattern": r"(how (do|to|are)[\s\S]{0,30}generat\w*[\s\S]{0,15}pdf|best[\s\S]{0,25}(way|library|tool|service|api)[\s\S]{0,15}pdf|pdf[\s\S]{0,15}(api|library|service|generator|recommendation)|what[\s\S]{0,25}use[\s\S]{0,15}pdf|generat\w*[\s\S]{0,15}pdf[\s\S]{0,15}(from\s+html|from\s+json|from\s+data|template))",
    },
    # Document jobs: invoices / receipts / reports / certificates as PDFs.
    "pdf_document_job": {
        "queries": [
            "generate invoice pdf", "pdf invoice", "invoice generation",
            "receipt pdf", "generate report pdf", "certificate pdf",
            "billing pdf", "packing slip pdf",
        ],
        "pattern": r"((invoice|receipt|report|certificate|statement|billing|packing\s+slip|quote|estimate|contract)[\s\S]{0,20}pdf|pdf[\s\S]{0,20}(invoice|receipt|report|certificate|statement))",
    },
}

# ============== END CONFIGURATION ==============

CSV_COLUMNS = [
    "found_at", "subreddit", "signal", "matched_phrase",
    "title", "author", "created", "link",
    "num_comments", "snippet", "selftext", "top_comments",
]


class DemandSignalScanner:
    def __init__(self):
        self.csv_path = OUTPUT_CSV
        self.seen_path = SEEN_IDS_JSON
        self.compiled = {n: re.compile(s["pattern"], re.I) for n, s in SIGNALS.items()}
        self.existing_ids = self._load_existing_ids()
        # Fold the durable memory into the skip-set so notify only fires for
        # posts we've genuinely never alerted on before.
        self._seen = self._load_seen_ids()
        self.existing_ids.update(self._seen.keys())
        self.hits = {}  # post_id -> row dict (deduped across signals/subs)
        self.comment_fetches = 0
        self._lock = threading.Lock()          # guards self.hits + counters
        self._local = threading.local()        # per-thread requests.Session
        self._min_ts = None
        if MIN_POST_DATE:
            self._min_ts = datetime.strptime(MIN_POST_DATE, "%Y-%m-%d").replace(
                tzinfo=timezone.utc).timestamp()

    def _session(self):
        """One requests.Session per worker thread (Sessions aren't thread-safe)."""
        s = getattr(self._local, "session", None)
        if s is None:
            s = requests.Session()
            s.headers.update({"User-Agent": USER_AGENT})
            self._local.session = s
        return s

    def _get_feed(self, url, tries=4):
        """Fetch a URL and parse as a feed, with 429/backoff retries."""
        for attempt in range(tries):
            try:
                resp = self._session().get(url, timeout=20)
                if resp.status_code == 429:
                    wait = int(resp.headers.get("retry-after", 4)) + 2 * attempt + 1
                    time.sleep(wait)
                    continue
                if resp.status_code != 200:
                    return None
                return feedparser.parse(resp.content)
            except requests.RequestException:
                time.sleep(1.5 * (attempt + 1))
        return None

    def _load_existing_ids(self):
        ids = set()
        if self.csv_path.exists():
            try:
                with open(self.csv_path, "r", newline="", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        ids.add(self._extract_post_id(row.get("link", "")))
            except (IOError, csv.Error) as e:
                print(f"  [!] Could not read existing CSV: {e}")
        return ids

    def _load_seen_ids(self):
        """Load the durable dedup memory written by the previous run."""
        if not self.seen_path.exists():
            return {}
        try:
            with open(self.seen_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except (IOError, ValueError) as e:
            print(f"  [!] Could not read {self.seen_path.name}: {e}")
            return {}

    def _save_seen_ids(self):
        """Fold this run's new ids into the memory, prune old ones, write it out.

        Only called when there are new hits, so a quiet run leaves the file
        byte-for-byte unchanged and the workflow's commit step is a no-op.
        """
        now = datetime.now(timezone.utc)
        cutoff = now.timestamp() - SEEN_ID_RETENTION_DAYS * 86400
        stamp = now.isoformat(timespec="seconds")
        merged = dict(self._seen)
        for pid in self.hits:
            merged.setdefault(pid, stamp)
        pruned = {}
        for pid, ts in merged.items():
            try:
                keep = datetime.fromisoformat(ts).timestamp() >= cutoff
            except (ValueError, TypeError):
                keep = True  # keep unparseable entries rather than risk a dupe
            if keep:
                pruned[pid] = ts
        tmp = self.seen_path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(pruned, f, indent=0, sort_keys=True)
        tmp.replace(self.seen_path)
        print(f"[=] {self.seen_path.name} updated ({len(pruned)} ids retained).")

    @staticmethod
    def _extract_post_id(link):
        m = re.search(r"/comments/([a-zA-Z0-9]+)", link or "")
        return m.group(1) if m else (link or "")

    @staticmethod
    def _clean(text):
        text = re.sub(r"<[^>]+>", " ", text or "")
        text = html.unescape(text)
        return re.sub(r"\s+", " ", text).strip()

    def _build_search_url(self, subreddit, queries):
        q = " OR ".join(f'"{p}"' for p in queries)
        params = urllib.parse.urlencode({
            "q": q, "restrict_sr": "1", "sort": "new",
            "t": TIME_WINDOW, "limit": "100", "include_over_18": "on",
        })
        return f"https://www.reddit.com/r/{subreddit}/search.rss?{params}"

    def _too_old(self, entry):
        published = entry.get("published_parsed") or entry.get("updated_parsed")
        if not published:
            return self._min_ts is not None
        ts = calendar.timegm(published)
        if self._min_ts is not None and ts < self._min_ts:
            return True
        if MAX_POST_AGE_HOURS is not None:
            if (datetime.now(timezone.utc).timestamp() - ts) / 3600 > MAX_POST_AGE_HOURS:
                return True
        return False

    def _scan_signal(self, subreddit, signal_name):
        """Search one (subreddit, signal); record confirmed hits. Thread-safe."""
        regex = self.compiled[signal_name]
        url = self._build_search_url(subreddit, SIGNALS[signal_name]["queries"])
        feed = self._get_feed(url)
        if feed is None or (feed.bozo and not feed.entries):
            return 0
        new = 0
        for entry in feed.entries:
            link, title = entry.get("link"), entry.get("title")
            if not link or not title or self._too_old(entry):
                continue
            pid = self._extract_post_id(link)
            body = self._clean(entry.get("summary", ""))
            m = regex.search(f"{title}\n{body}")
            if not m:
                continue
            with self._lock:
                if pid in self.existing_ids or pid in self.hits:
                    continue
                self.hits[pid] = {
                    "found_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "subreddit": subreddit, "signal": signal_name,
                    "matched_phrase": self._clean(m.group(0))[:120],
                    "title": self._clean(title)[:300], "author": entry.get("author", ""),
                    "created": entry.get("published", ""), "link": link,
                    "num_comments": "", "snippet": body[:300],
                    "selftext": "", "top_comments": "",
                }
                new += 1
        return new

    def _fetch_comments(self, pid):
        """Pull full body + comments from the public comments .rss feed.

        Reddit blocks the .json endpoint for non-browser clients (403), but the
        comments RSS feed is open. Layout: entry[0] is the post itself (body in
        its summary); entry[1:] are comments, titled "/u/<author> on <post>".
        """
        url = f"https://www.reddit.com/comments/{pid}.rss?sort=top&limit={MAX_COMMENTS_PER_POST}"
        feed = self._get_feed(url)
        if feed is None or not feed.entries:
            return None
        entries = feed.entries
        selftext = self._clean(entries[0].get("summary", ""))
        comments = []
        for e in entries[1:]:
            if "AutoModerator" in e.get("author", "") or "AutoModerator" in e.get("title", ""):
                continue
            body = self._clean(e.get("summary", ""))
            if body:
                comments.append(body)
            if len(comments) >= MAX_COMMENTS_PER_POST:
                break
        return {
            "num_comments": len(comments),
            "selftext": selftext[:MAX_SELFTEXT_CHARS],
            "top_comments": " ||| ".join(comments)[:MAX_COMMENT_CHARS],
        }

    def _search_pass(self):
        total_subs = len(SUBREDDITS)
        print(f"[*] Search pass: {total_subs} subreddits x {len(SIGNALS)} signals, "
              f"{SEARCH_WORKERS} workers per sub | checkpointing after each sub...\n")
        for si, subreddit in enumerate(SUBREDDITS, 1):
            pre = len(self.hits)
            with ThreadPoolExecutor(max_workers=SEARCH_WORKERS) as ex:
                futs = {ex.submit(self._scan_signal, subreddit, sig): sig for sig in SIGNALS}
                for fut in as_completed(futs):
                    try:
                        fut.result()
                    except Exception as e:
                        print(f"    [!] r/{subreddit}/{futs[fut]}: {e}")
            gained = len(self.hits) - pre
            self._write_csv()   # <-- checkpoint after every subreddit
            print(f"  [{si:2}/{total_subs}] r/{subreddit:<22} +{gained:4} hits "
                  f"| total {len(self.hits):5} | CSV saved")

    def _enrich_one(self, pid, row):
        data = self._fetch_comments(pid)
        if data:
            row.update(data)
        return data is not None

    def _comment_pass(self):
        # Prioritise question/demand-framed posts so an early stop keeps signal.
        demand = re.compile(r"\b(how|why|is there|are there|anyone|what|which|looking for|recommend|best|wish|need)\b", re.I)
        targets = sorted(self.hits.items(),
                         key=lambda kv: 0 if demand.search(kv[1]["title"]) else 1)
        targets = targets[:MAX_COMMENT_FETCHES]
        total = len(targets)
        print(f"\n[*] Comment pass: reading {total} threads, {COMMENT_WORKERS} workers "
              f"(checkpoint every {CHECKPOINT_EVERY})...")
        done = ok = 0
        with ThreadPoolExecutor(max_workers=COMMENT_WORKERS) as ex:
            futs = {ex.submit(self._enrich_one, pid, row): pid for pid, row in targets}
            for fut in as_completed(futs):
                done += 1
                try:
                    if fut.result():
                        ok += 1
                except Exception:
                    pass
                if done % CHECKPOINT_EVERY == 0 or done == total:
                    self._write_csv()  # crash-safe: full snapshot to disk
                    print(f"    ...{done}/{total} read ({ok} with comments) | checkpoint saved")
        self.comment_fetches = done

    def _write_csv(self):
        """Atomic full-snapshot write (safe to call repeatedly as a checkpoint)."""
        with self._lock:
            rows = list(self.hits.values())
        tmp = self.csv_path.with_suffix(".tmp")
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            w.writeheader()
            w.writerows(rows)
        tmp.replace(self.csv_path)

    def _notify_discord(self):
        """Push each new hit to Discord. One compact message per post.

        Only fires for posts discovered this run (self.hits already excludes
        anything in existing_ids). Safe no-op if no webhook is configured.
        """
        if not DISCORD_WEBHOOK_URL:
            return
        rows = list(self.hits.values())
        if not rows:
            return
        sess = requests.Session()
        sess.headers.update({"User-Agent": USER_AGENT})
        sent = 0
        for r in rows:
            content = (
                f"**PDF pain thread** · r/{r['subreddit']} · `{r['signal']}`\n"
                f"{r['title']}\n{r['link']}"
            )[:1900]
            for attempt in range(4):
                try:
                    resp = sess.post(DISCORD_WEBHOOK_URL,
                                     data=json.dumps({"content": content}),
                                     headers={"Content-Type": "application/json"},
                                     timeout=15)
                    if resp.status_code == 429:  # rate limited
                        wait = resp.json().get("retry_after", 1.5)
                        time.sleep(float(wait) + 0.5)
                        continue
                    break
                except requests.RequestException:
                    time.sleep(1.5 * (attempt + 1))
            sent += 1
            time.sleep(0.5)  # stay under Discord's ~5/sec webhook limit
        print(f"[=] Discord: pushed {sent} new thread(s).")

    def run(self):
        print("""
╔══════════════════════════════════════════════════╗
║   DOCLINTH PDF-PAIN SCANNER  (Reddit RSS)        ║
╚══════════════════════════════════════════════════╝
        """)
        if not SUBREDDITS:
            print("[!] No subreddits configured.\n")
            return
        print(f"Subreddits : {len(SUBREDDITS)}  |  Signals: {len(SIGNALS)}  |  Window: {TIME_WINDOW}")
        print(f"Date floor : {MIN_POST_DATE}  |  Comments: {FETCH_COMMENTS}")
        print(f"Already seen: {len(self.existing_ids)} post(s) "
              f"({len(self._seen)} from durable memory)\n")

        t0 = time.time()
        self._search_pass()
        print(f"\n[=] Search pass done in {time.time()-t0:.0f}s. "
              f"{len(self.hits)} unique new posts.")

        # Write title-level results immediately so they're never lost.
        if self.hits:
            self._write_csv()
            print(f"[=] Title-level CSV written ({len(self.hits)} rows).")

        # Push new threads to Discord (the durable output on Render).
        self._notify_discord()

        # Record what we just alerted on so the next run won't repeat it.
        # Skipped entirely on quiet runs to keep the committed file stable.
        if self.hits:
            self._save_seen_ids()

        if FETCH_COMMENTS and self.hits:
            self._comment_pass()

        print(f"\n{'='*52}")
        print(f"Done in {time.time()-t0:.0f}s. {len(self.hits)} posts -> {self.csv_path.name}")
        print(f"Comments fetched for {self.comment_fetches} posts.")
        print('='*52)


if __name__ == "__main__":
    DemandSignalScanner().run()
