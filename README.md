# CodingContest MCP

Multi-account Streamable HTTP MCP server for [codingcontest.org](https://codingcontest.org), covering challenges, competitions, files, submissions, and results.

![CodingContest dashboard](docs/codingcontest-dashboard.png)

## Quick start

```sh
cp .env.example .env
# Set BOT_TOKEN and BOT_CHAT_ID in .env to receive accepted solutions in Telegram.
docker compose up --build -d
```

When both Telegram settings are present, the server sends a startup message and keeps one editable solution message per contest and level. Accepted outputs are collected into a ZIP attached to that message; each later accepted file updates the same Telegram message and archive. The server stores sent-file keys in `CCC_DATA_DIR/telegram-sent.sqlite3` by default. `submit_solution` returns `telegram_notification` with `sent`, `updated`, `duplicate`, `disabled`, or a failure reason. Set `BOT_DEDUPE_DB` to an absolute path to override the database location.

Team account pools are isolated by Telegram room. Set `BOT_SESSION_ENCRYPTION_KEY` to a persistent Fernet key (generate one with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`) and set `MCP_PUBLIC_ORIGIN` to the public HTTPS origin. In the bot's private chat, create a room with `/create_room <room> <password>`. Each participant runs `/connect <room> <password>` and receives a one-time HTTPS form for their CCC `SESSION` cookie; the bot never asks for the cookie in Telegram. The server verifies the CCC identity and encrypts the session at rest. `/disconnect <room>` revokes that participant's stored session. To select a pool, add `X-CCC-Team-Room: <room>` beside `X-CCC-Session` in the MCP connection headers. The submitting CCC account must itself be linked to that room. Fanout is queued only after CCC reports `evaluation.isCorrect: true`, and submissions are spaced by random 60–180 second intervals per target account. In Telegram, `/queue <room>` shows each waiting target account and its ETA; each row has a button to move only that account's submission to the front of the queue. The bot uses Telegram long polling, so only one running server instance may poll this bot token at a time.

Or with Python 3.12+: `pip install -r requirements.txt && python server.py`.

## Connect and use

Connect to `http://localhost:8000/mcp` with `X-CCC-Session: <CCC SESSION cookie>` (omit `SESSION=`). For a team pool, add `X-CCC-Team-Room: <room>` to the same MCP connection's headers. Public deployments require HTTPS and `MCP_PUBLIC_ORIGIN`.

The `python -m ccc_mcp ... upload <path>` command streams file bytes directly to the authenticated MCP server, avoiding base64 conversion. Large task files can be downloaded directly into the MCP client's workspace: call `get_artifact_download_url` with the returned `artifact_id` and filename, then fetch the returned URL with the client's file or shell tools. Links are single-use, expire after five minutes, and support files up to 30 MiB. For a remote MCP server, set `MCP_PUBLIC_ORIGIN` to its public HTTPS origin so the client can reach the link.
