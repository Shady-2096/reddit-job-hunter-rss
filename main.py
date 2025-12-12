#!/usr/bin/env python3
"""
Reddit Job Hunter (RSS Version)
No Reddit API needed! Uses public RSS feeds.
"""

import calendar
import feedparser
import requests
import time
import json
import re
from datetime import datetime, timezone
from pathlib import Path

# ============== CONFIGURATION ==============

# Your Discord webhook URL
DISCORD_WEBHOOK_URL = "your_discord_webhook"

# Subreddits to monitor (RSS feeds are public, no auth needed!)
SUBREDDITS = [ # enter subs below this bracket, use double quotation marks
    
]

# Keywords to match (case-insensitive)
KEYWORDS = { # remove these and add your own keywords in the same format
    "web_scraping": [
        "scrape", "scraping", "scraper", "web scraping", "data extraction",
        "crawl", "crawler", "beautifulsoup", "selenium"
    ],
    "python": [
        "python", "python script", "python automation", "python developer"
        # Removed generic "script", "automation", "automate"
    ],
    
}

# How often to check (in seconds)
CHECK_INTERVAL = 180  # 3 minutes (be nice to Reddit's servers)

# Maximum age of posts to consider (in hours)
MAX_POST_AGE_HOURS = 12

# ============== END CONFIGURATION ==============


class RedditRSSMonitor:
    def __init__(self):
        self.seen_posts_file = Path(__file__).parent / "seen_posts.json"
        self.seen_posts = self._load_seen_posts()
        self.all_keywords = self._flatten_keywords()
        
    def _flatten_keywords(self):
        """Flatten keyword categories into a single list."""
        all_kw = []
        for category_keywords in KEYWORDS.values():
            all_kw.extend(category_keywords)
        return list(set(all_kw))
    
    def _load_seen_posts(self):
        """Load previously seen post IDs."""
        if self.seen_posts_file.exists():
            try:
                with open(self.seen_posts_file, 'r') as f:
                    data = json.load(f)
                    # Clean old entries (older than 7 days)
                    week_ago = datetime.now(timezone.utc).timestamp() - (7 * 24 * 60 * 60)
                    return {k: v for k, v in data.items() if v > week_ago}
            except (json.JSONDecodeError, IOError) as e:
                print(f"  [!] Warning: Could not load seen posts: {e}")
                return {}
        return {}
    
    def _save_seen_posts(self):
        """Save seen post IDs (atomic write to prevent corruption)."""
        temp_file = self.seen_posts_file.with_suffix('.tmp')
        try:
            with open(temp_file, 'w') as f:
                json.dump(self.seen_posts, f)
            temp_file.replace(self.seen_posts_file)
        except IOError as e:
            print(f"  [!] Warning: Could not save seen posts: {e}")
    
    def _extract_post_id(self, link):
        """Extract Reddit post ID from URL."""
        # URL format: https://www.reddit.com/r/subreddit/comments/POST_ID/title/
        match = re.search(r'/comments/([a-zA-Z0-9]+)/', link)
        return match.group(1) if match else link
    
    def _check_keywords(self, text):
        """Check if text contains any keywords. Returns matched keywords."""
        text_lower = text.lower()
        matched = []
        for keyword in self.all_keywords:
            if keyword.lower() in text_lower:
                matched.append(keyword)
        return matched
    
    def _get_keyword_categories(self, matched_keywords):
        """Get which categories the matched keywords belong to."""
        categories = set()
        for category, keywords in KEYWORDS.items():
            for kw in matched_keywords:
                if kw.lower() in [k.lower() for k in keywords]:
                    categories.add(category)
        return list(categories)
    
    def _is_job_offer(self, title, subreddit):
        """Check if post is a job offer (not someone looking for work)."""
        title_lower = title.lower()

        # Universal check - these tags mean someone is offering their services (skip)
        offering_services_tags = [
            "[for hire]",
            "[offer]",
            "[selling]",
            "for hire",  # Without brackets too
        ]
        for tag in offering_services_tags:
            if tag in title_lower:
                return False

        # Universal check - these tags mean someone is hiring (want this!)
        hiring_tags = [
            "[hiring]",
            "[task]",
            "[buying]",
            "[paid]",
        ]
        for tag in hiring_tags:
            if tag in title_lower:
                return True

        # Subreddit-specific defaults for ambiguous posts
        if subreddit.lower() in ["slavelabour", "forhire"]:
            # These subreddits mix both, so skip ambiguous posts
            return False

        return True  # Default for other subreddits (like r/hiring, r/jobbit)
    
    def _send_discord_notification(self, title, link, subreddit, matched_keywords, categories, description=""):
        """Send a Discord webhook notification."""
        if DISCORD_WEBHOOK_URL == "YOUR_WEBHOOK_URL_HERE":
            print(f"  [!] Discord webhook not configured!")
            print(f"      Would notify: {title[:60]}...")
            return
        
        # Color based on primary category
        colors = {
            "web_scraping": 0x00FF00,  # Green - your specialty
            "python": 0x3776AB,         # Python blue
            "discord": 0x5865F2,        # Discord blurple
            "flutter_mobile": 0x02569B, # Flutter blue
            "general_dev": 0xFFAA00,    # Orange
            "data": 0x00AAFF,           # Light blue
            "bot": 0x9B59B6,            # Purple
        }
        color = colors.get(categories[0] if categories else "general_dev", 0x808080)
        
        # Create embed
        embed = {
            "title": f"🚨 {title[:200]}",
            "url": link,
            "color": color,
            "fields": [
                {
                    "name": "Subreddit",
                    "value": f"r/{subreddit}",
                    "inline": True
                },
                {
                    "name": "Keywords",
                    "value": ", ".join(matched_keywords[:5]) or "N/A",
                    "inline": True
                },
                {
                    "name": "Categories",
                    "value": ", ".join(categories) or "General",
                    "inline": True
                }
            ],
            "footer": {
                "text": "Reddit Job Hunter (RSS)"
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        # Add description preview if available
        if description:
            # Clean HTML tags
            clean_desc = re.sub(r'<[^>]+>', '', description)
            preview = clean_desc[:300]
            if len(clean_desc) > 300:
                preview += "..."
            embed["description"] = preview
        
        payload = {"embeds": [embed]}
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
                if response.status_code == 204:
                    print(f"  [OK] Discord notification sent!")
                    return
                elif response.status_code == 429:
                    retry_after = response.json().get('retry_after', 5)
                    print(f"  [!] Discord rate limited. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue  # Retry
                else:
                    print(f"  [!] Discord error {response.status_code}")
                    return
            except requests.exceptions.Timeout:
                print(f"  [!] Discord request timed out (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(2)
            except Exception as e:
                print(f"  [!] Failed to send notification: {e}")
                return
    
    def check_subreddit(self, subreddit):
        """Check a single subreddit's RSS feed."""
        url = f"https://www.reddit.com/r/{subreddit}/new.rss"

        try:
            # Add user agent to avoid blocks
            feed = feedparser.parse(url, agent="RedditJobHunter/1.0")

            if feed.bozo and not feed.entries:
                error_msg = getattr(feed, 'bozo_exception', 'Unknown error')
                print(f"  [!] Failed to fetch r/{subreddit}: {error_msg}")
                return

            new_matches = 0

            for entry in feed.entries:
                # Validate entry has required fields
                if not hasattr(entry, 'link') or not entry.link:
                    continue
                if not hasattr(entry, 'title') or not entry.title:
                    continue

                # Check post age - skip if older than MAX_POST_AGE_HOURS
                published = entry.get('published_parsed') or entry.get('updated_parsed')
                if published:
                    post_timestamp = calendar.timegm(published)
                    now_timestamp = datetime.now(timezone.utc).timestamp()
                    age_hours = (now_timestamp - post_timestamp) / 3600
                    if age_hours > MAX_POST_AGE_HOURS:
                        continue

                post_id = self._extract_post_id(entry.link)

                # Skip if already seen
                if post_id in self.seen_posts:
                    continue

                # Mark as seen
                self.seen_posts[post_id] = datetime.now(timezone.utc).timestamp()

                # Check if it's a job offer (not someone looking for work)
                if not self._is_job_offer(entry.title, subreddit):
                    continue

                # Check for keyword matches
                full_text = f"{entry.title} {entry.get('summary', '')}"
                matched = self._check_keywords(full_text)

                if matched:
                    new_matches += 1
                    categories = self._get_keyword_categories(matched)

                    print(f"\n  [MATCH] {entry.title[:60]}...")
                    print(f"          Keywords: {', '.join(matched[:5])}")
                    print(f"          Link: {entry.link}")

                    self._send_discord_notification(
                        title=entry.title,
                        link=entry.link,
                        subreddit=subreddit,
                        matched_keywords=matched,
                        categories=categories,
                        description=entry.get('summary', '')
                    )

                    # Small delay between notifications to avoid Discord rate limits
                    time.sleep(1)

            if new_matches == 0:
                print(f"  No new matches")

        except Exception as e:
            print(f"  [!] Error checking r/{subreddit}: {e}")
    
    def check_all(self):
        """Check all subreddits."""
        print(f"\n{'='*50}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Checking subreddits...")
        print('='*50)
        
        for subreddit in SUBREDDITS:
            print(f"\n[>] r/{subreddit}")
            self.check_subreddit(subreddit)
            time.sleep(2)  # Be nice to Reddit
        
        self._save_seen_posts()
    
    def run(self):
        """Main loop."""
        print("""
╔══════════════════════════════════════════════════╗
║         REDDIT JOB HUNTER (RSS Edition)          ║
║              No API Key Required!                ║
╚══════════════════════════════════════════════════╝
        """)
        print(f"Monitoring: {', '.join(SUBREDDITS)}")
        print(f"Keywords: {len(self.all_keywords)} total")
        print(f"Check interval: {CHECK_INTERVAL} seconds")
        
        if DISCORD_WEBHOOK_URL == "YOUR_WEBHOOK_URL_HERE":
            print("\n[!] WARNING: Discord webhook not configured!")
            print("    Edit the script and add your webhook URL.\n")
        
        while True:
            try:
                self.check_all()
                print(f"\n[*] Next check in {CHECK_INTERVAL} seconds...")
                time.sleep(CHECK_INTERVAL)
            except KeyboardInterrupt:
                print("\n\n[*] Shutting down...")
                self._save_seen_posts()
                break
            except Exception as e:
                print(f"\n[!] Error: {e}")
                print(f"Retrying in {CHECK_INTERVAL} seconds...")
                time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    monitor = RedditRSSMonitor()
    monitor.run()
