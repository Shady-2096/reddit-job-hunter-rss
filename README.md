# Reddit Job Hunter (RSS Edition) 🎯

**No Reddit API needed!** Uses public RSS feeds that require zero authentication.

## Setup (2 minutes)

### 1. Create Discord Webhook

1. Open Discord → Your server → Create channel `#job-alerts`
2. Edit Channel → Integrations → Webhooks → New Webhook
3. Copy the webhook URL

### 2. Configure Script

Open `main.py` and paste your webhook URL:

```python
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/your/webhook/url"
```

### 3. Run

```bash
pip install -r requirements.txt
python main.py
```

---

## How It Works

- Checks RSS feeds of job subreddits every 3 minutes
- Filters for posts matching your skills (scraping, python, discord, etc.)
- Sends Discord notifications with direct links
- Automatically skips people *looking* for work (you want job *offers*)

---

## Customization

**Add/remove subreddits:**
```python
SUBREDDITS = [
    "slavelabour",
    "forhire",
    # add more here
]
```

**Add/remove keywords:**
```python
KEYWORDS = {
    "web_scraping": ["scrape", "scraping", ...],
    "your_skill": ["keyword1", "keyword2"],
}
```

---

## Run 24/7 (Free Options)

**PythonAnywhere:** Upload files, run in always-on task

**Railway.app:** Connect GitHub, add webhook as environment variable

**Your PC:** Just leave terminal open, or use `tmux`/`screen`

---

## Notes

- RSS feeds are public and don't require authentication
- Be nice to Reddit's servers (don't lower CHECK_INTERVAL below 120)
- Enable Discord mobile notifications for instant alerts
