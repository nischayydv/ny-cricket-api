# Live Cricket Score API

Free Cricket API — scrapes data using BeautifulSoup and serves it via FastAPI, deployable on Vercel as a serverless function.

## Disclaimer

This is **not an official API provided by Cricbuzz**. This is an unofficial API that retrieves data by scraping publicly available content from Cricbuzz and is **not affiliated with, authorized, sponsored, or endorsed by Cricbuzz**.

This project is created **strictly for educational and personal development purposes only**. Use in any production or commercial environment is entirely at your own risk.

All credits go to <https://www.cricbuzz.com/>

---

## Deploy to Vercel

### One-click deploy

[![Deploy with Vercel](https://vercel.com/button)](https://vercel.com/new)

### Manual deploy

```sh
# Install Vercel CLI
npm install -g vercel

# Clone and enter the repo
git clone https://github.com/your-username/live-cricket-score-api
cd live-cricket-score-api

# Deploy
vercel

# Production deploy
vercel --prod
```

Vercel automatically detects the `api/index.py` entry point and `vercel.json` routing. No extra configuration needed.

---

## Local Development

```sh
# Create virtual environment
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start dev server
uvicorn api.index:app --host 0.0.0.0 --port 6020 --reload
```

---

## API Usage

### Live Score (JSON)

```
GET /?score=150294
```

```json
{
  "status": "success",
  "title": "Sri Lanka A vs New Zealand A, 1st unofficial ODI",
  "score": "NZA 86/4 (17)",
  "current_batsmen": [
    { "name": "Simon Keene", "score": "6(7)" },
    { "name": "Muhammad Abbas", "score": "10(29)" }
  ],
  "current_bowler": { "name": "Wanuja Sahan" }
}
```

### Live Score (Tree view)

```
GET /?score=150294&text=true
```

```
🏏 Live Score
│
├── Match    : Sri Lanka A vs New Zealand A, 1st unofficial ODI
├── Score    : NZA 86/4 (17)
├── Bowler   : Wanuja Sahan
├── Batsmen
│   ├── Simon Keene : 6(7)
│   ├── Muhammad Abbas : 10(29)
```

### Schedule

```
GET /schedule
```

### Match Details

```
GET /match/{match_id}/details
```

### Swagger Docs

```
GET /docs
```

---

## CLI (local use)

```sh
# Tree view (default)
python cli.py 12345

# JSON output
python cli.py 12345 --json

# Text/tree output
python cli.py 12345 --text
```

---

## Project Structure

```
.
├── api/
│   └── index.py       # FastAPI app (Vercel entry point)
├── cli.py             # Standalone CLI tool
├── vercel.json        # Vercel routing config
├── requirements.txt   # Python dependencies
└── README.md
```

## LICENSE

MIT
