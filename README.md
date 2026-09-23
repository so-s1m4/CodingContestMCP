# CodingContest MCP

Multi-account Streamable HTTP MCP server for [codingcontest.org](https://codingcontest.org), covering challenges, competitions, files, submissions, and results.

![CodingContest dashboard](docs/codingcontest-dashboard.png)

## Quick start

```sh
cp .env.example .env
# Set BOT_TOKEN and BOT_CHAT_ID in .env to receive accepted solutions in Telegram.
docker compose up --build -d
```

When both Telegram settings are present, each accepted contest, level and input-file combination is sent to the configured Telegram chat once. The server stores these sent-file keys in `CCC_DATA_DIR/telegram-sent.sqlite3` by default, and adds contest, level and file hashtags to each message. Set `BOT_DEDUPE_DB` to an absolute path to override the database location.

Or with Python 3.12+: `pip install -r requirements.txt && python server.py`.

## Connect and use

Connect to `http://localhost:8000/mcp` with `X-CCC-Session: <CCC SESSION cookie>` (omit `SESSION=`). Public deployments require HTTPS and `MCP_PUBLIC_ORIGIN`.
