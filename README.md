# BSE Trades Stream

Asynchronous BSE trade pull. A mock BSE API accepts a pull request with `202 Accepted`, a background worker streams trades over time, every chunk is persisted, and the dashboard receives it instantly over WebSocket. No HTTP connection is held open longer than a few milliseconds, so the 30 second network limit is never reached.

See `ARCHITECTURE_NOTE.md` for the diagram and design rationale.

## Project Structure

```
bse-trades-stream/
  package.json
  server.js                 Express app, Mock BSE API, WebSocket hub, ingestion pipeline
  lib/
    config.js               Environment-driven configuration
    tradeGenerator.js       Seeded mock trade generator
    worker.js               Background pull worker (chunked, timer-driven)
    db.js                   In-memory store with NDJSON file persistence
  public/
    index.html              Single-page real-time dashboard (Tailwind CSS + native WebSocket)
  ARCHITECTURE_NOTE.md
  README.md
```

## Prerequisites

- Node.js 18 or newer (`node --version`)
- npm (bundled with Node.js)
- Internet access in the browser, because the dashboard loads Tailwind CSS from `https://cdn.tailwindcss.com`

## Setup

1. Open a terminal in the project folder:

   ```bash
   cd bse-trades-stream
   ```

2. Install dependencies:

   ```bash
   npm install
   ```

3. Start the server with the default 15 minute pull profile:

   ```bash
   npm start
   ```

   Or start with the 30 second test profile:

   ```bash
   npm run start:fast
   ```

4. Open the dashboard at <http://localhost:3000>.

5. Click **Start BSE Pull**. The acknowledgment line shows the `202` response time, and trades begin to stream into the grid, stat cards and symbol bars without any refresh.

6. Stop the server with `Ctrl+C`.

## Configuration

All settings are environment variables.

| Variable | Default | SPEED_UP default | Description |
|----------|---------|------------------|-------------|
| `PORT` | `3000` | `3000` | HTTP and WebSocket port |
| `SPEED_UP` | `false` | `true` | `true` shortens the pull profile (the `--speed-up` flag does the same) |
| `PULL_DURATION_MS` | `900000` (15 min) | `30000` (30 s) | Total duration of one pull |
| `CHUNK_INTERVAL_MS` | `5000` | `500` | Time between trade chunks |
| `TOTAL_TRADES` | `9000` | `3000` | Trades generated per pull |
| `SEED` | `20260101` | `20260101` | Seed for the trade generator |
| `DATA_FILE` | `./data/trades.ndjson` | same | Persistence file |
| `WS_HEARTBEAT_MS` | `15000` | `15000` | WebSocket ping interval (keep below 30000) |
| `SNAPSHOT_LIMIT` | `500` | `500` | Trades sent to a dashboard when it connects |

Examples:

```bash
# macOS / Linux
SPEED_UP=true PORT=4000 npm start
PULL_DURATION_MS=120000 CHUNK_INTERVAL_MS=2000 TOTAL_TRADES=5000 npm start
```

```powershell
# Windows PowerShell
$env:SPEED_UP = "true"; $env:PORT = "4000"; npm start
```

```cmd
:: Windows cmd
set SPEED_UP=true&& set PORT=4000&& npm start
```

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/bse/pull` | Start a pull. Responds `202 Accepted` immediately with the job and `Location` header. Returns the running job with `alreadyRunning: true` if one is active. |
| `GET` | `/api/bse/pull` | List jobs, newest first |
| `GET` | `/api/bse/pull/:id` | Job status and progress |
| `DELETE` | `/api/bse/pull/:id` | Cancel a running job |
| `GET` | `/api/trades?limit=100&offset=0&jobId=` | Paged stored trades (limit 1 to 1000) |
| `GET` | `/api/stats` | Aggregate statistics |
| `DELETE` | `/api/trades` | Clear stored trades (409 while a pull is running) |
| `GET` | `/health` | Process health |
| `WS` | `/ws` | Real-time stream |

### WebSocket messages (server to client)

| `type` | Payload |
|--------|---------|
| `snapshot` | `trades`, `stats`, `job`, `config` (sent on every connect) |
| `job_started` | `job` |
| `trades` | `jobId`, `trades`, `stats`, `job` |
| `job_completed` / `job_cancelled` / `job_failed` | `job`, `stats` |
| `cleared` | `stats` |

### Trade object

```json
{
  "id": "<jobId>-0000001",
  "jobId": "<jobId>",
  "seq": 1,
  "scripCode": "500325",
  "symbol": "RELIANCE",
  "side": "BUY",
  "price": 2897.41,
  "quantity": 112,
  "value": 324509.92,
  "tradeTime": "2026-01-01T10:00:00.000Z",
  "receivedAt": "2026-01-01T10:00:05.003Z"
}
```

## Verifying the 30 Second Constraint

1. Run `npm start` (default 15 minute profile).
2. Request a pull and time the response:

   ```bash
   curl -i -X POST http://localhost:3000/api/bse/pull
   ```

   The response is `HTTP/1.1 202 Accepted` and returns within milliseconds.

3. Check progress at any time:

   ```bash
   curl http://localhost:3000/api/bse/pull
   ```

4. Leave the dashboard open: trades keep arriving for the full 15 minutes over a single WebSocket that is pinged every 15 seconds.

## Data

Trades are appended to `data/trades.ndjson` (one JSON object per line) and loaded back into memory at startup. Use the dashboard **Clear Trades** button or `DELETE /api/trades` to empty the store, or delete the file while the server is stopped.

## Troubleshooting

- **Port already in use:** start with another port, for example `PORT=4000 npm start`.
- **Dashboard unstyled:** the browser cannot reach `cdn.tailwindcss.com`; check internet access.
- **Badge shows "Reconnecting...":** the server is not running or the WebSocket path is blocked; the page reconnects automatically with backoff.
