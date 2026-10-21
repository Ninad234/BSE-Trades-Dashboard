# ARCHITECTURE_NOTE

## 1. Architecture Diagram

```
 +-----------------------------------------------------------------------------+
 |                        BROWSER  (public/index.html)                         |
 |                                                                             |
 |   [Start BSE Pull]      Progress bar | Stat cards | Symbol bars | Trade grid |
 |         |                                    ^                               |
 +---------|------------------------------------|-------------------------------+
           |                                    |
           | (1) POST /api/bse/pull             | (6) WebSocket frames (push)
           |     short HTTP request             |     snapshot | trades |
           |                                    |     job_started | job_completed
           v                                    |     job_failed | job_cancelled
 +-----------------------------------------------------------------------------+
 |                   NODE.JS PROCESS  (server.js, Express + ws)                |
 |                                                                             |
 |  +------------------------------+      +---------------------------------+  |
 |  | MOCK BSE API (HTTP)          |      | WEBSOCKET HUB  (ws, path /ws)   |  |
 |  |  POST   /api/bse/pull        |      |  - snapshot sent on connect     |  |
 |  |  GET    /api/bse/pull/:id    |      |  - broadcast() to all clients   |  |
 |  |  DELETE /api/bse/pull/:id    |      |  - ping every 15 s (heartbeat)  |  |
 |  +--------------+---------------+      +----------------^----------------+  |
 |                 |                                       |                   |
 |                 | (2) worker.start()                    | (5) broadcast     |
 |                 |     returns job id                    |     after persist |
 |                 |     -> HTTP 202 Accepted (instant)    |                   |
 |                 v                                       |                   |
 |  +------------------------------+      +----------------+----------------+  |
 |  | BACKGROUND WORKER            |      | INGESTION PIPELINE              |  |
 |  | (lib/worker.js)              | (4)  |  worker.on('chunk'):            |  |
 |  |  - setImmediate -> run()     +----->|    1. store.insertTrades()      |  |
 |  |  - setTimeout chain, one     | event|    2. broadcast(trades + stats)  |  |
 |  |    chunk every CHUNK_INTERVAL|      +----------------+----------------+  |
 |  |  - TradeGenerator (seeded)   |                       |                   |
 |  |    (lib/tradeGenerator.js)   |                       | (3) persist       |
 |  |  - events: started, chunk,   |                       v                   |
 |  |    completed, failed,        |      +---------------------------------+  |
 |  |    cancelled                 |      | TRADE STORE (lib/db.js)         |  |
 |  +------------------------------+      |  - in-memory array (fast reads) |  |
 |                                        |  - incremental aggregates       |  |
 |                                        |  - append-only NDJSON file      |  |
 |                                        +----------------+----------------+  |
 +-----------------------------------------------------------|-----------------+
                                                             |
                                                             v
                                              data/trades.ndjson (disk)

 Timeline of one pull (default profile: 15 minutes, chunk every 5 s)

 t = 0 ms       POST /api/bse/pull  --> 202 Accepted, connection closed
 t = 0 ms       WebSocket already open, receives job_started
 t = 5 s        chunk 1 persisted --> pushed to dashboard
 t = 10 s       chunk 2 persisted --> pushed to dashboard
 ...            (180 chunks, 9000 trades by default)
 t = 15 min     chunk 180 persisted --> pushed, then job_completed
```

## 2. Why This Design

### The constraint

The network terminates any HTTP connection that stays open longer than 30 seconds. A BSE pull takes up to 15 minutes. A request/response design in which the dashboard waits for the pull to finish is therefore guaranteed to fail: the connection would be cut roughly 14.5 minutes before the data is ready.

### How the design removes the constraint

The design never lets any single connection stay busy for longer than the network allows. Work duration and connection duration are decoupled.

1. **Acknowledge, do not wait.** `POST /api/bse/pull` only registers a job and schedules the worker with `setImmediate`. It responds with `202 Accepted` and the job descriptor within milliseconds. The HTTP connection is closed long before the 30 second limit. 202 is the semantically correct status: the request was accepted, processing has not finished.

2. **The long-running work lives outside the request lifecycle.** The background worker runs on timers inside the Node.js event loop. It is not attached to any socket, so the network cannot kill it. It emits one event per chunk of trades, simulating a 15 minute stream.

3. **Persist first, then push.** The `chunk` listener writes the trades to the store (memory plus an append-only NDJSON file) and only afterwards broadcasts them. Anything a user sees on screen is already durable, and a crash can never show data that was not saved.

4. **Push instead of poll.** The dashboard opens a WebSocket once. Every chunk is delivered the instant it is saved. No client polling loop exists, so there is no request that waits, no request that can time out, and no polling latency.

5. **The long-lived socket is kept alive deliberately.** The server sends a protocol-level ping every 15 seconds (`WS_HEARTBEAT_MS`), which is half of the 30 second limit. Even during quiet periods, traffic crosses the connection often enough that an idle-connection timeout does not fire. Browsers answer pings automatically, and the server terminates sockets that stop answering.

6. **Failure of the socket does not lose data.** If a connection is dropped anyway, the client reconnects with exponential backoff (1 s up to 10 s). On every connect the server sends a `snapshot` containing the latest trades, the aggregate statistics and the current job state read from the store. The dashboard converges to the correct state without a page refresh. The worker keeps running and persisting throughout the outage.

### Event-driven decoupling

The worker, the persistence layer and the WebSocket hub communicate only through events:

| Producer | Event | Consumer action |
|----------|-------|-----------------|
| Worker | `started` | Broadcast `job_started` |
| Worker | `chunk` | Persist trades, broadcast `trades` with updated stats and job progress |
| Worker | `completed` / `failed` / `cancelled` | Broadcast terminal job state |

Replacing the simulated worker with a real BSE client requires only that the new producer emit the same events; the store, the hub and the dashboard stay unchanged.

### Other behaviours worth noting

- **Idempotent start.** Calling `POST /api/bse/pull` while a pull is running returns 202 for the existing job (`alreadyRunning: true`) instead of starting a second one.
- **Error isolation.** If persistence throws inside the `chunk` listener, the exception is caught by the worker, the job is marked `failed` and clients receive `job_failed`.
- **Drift-free timing.** Each chunk is scheduled against the original start time, so timer jitter does not accumulate over 15 minutes.
- **Configurable profile.** `SPEED_UP=true` (or `--speed-up`) shortens a pull to 30 seconds with a chunk every 500 ms. `PULL_DURATION_MS`, `CHUNK_INTERVAL_MS`, `TOTAL_TRADES` and `SEED` override individual values. The architecture is identical in both profiles.
- **Deterministic data.** The trade generator uses a seeded PRNG; the same `SEED` yields the same price walk and quantities.
- **Graceful shutdown.** SIGINT and SIGTERM stop the worker timers, close WebSocket clients with code 1001, flush the data file and close the HTTP server.

### Limits of this self-contained build

- A single Node.js process holds the WebSocket clients, the worker and the store. Scaling out horizontally requires moving the worker to a queue consumer and replacing in-process broadcast with a shared pub/sub channel (for example Redis), and replacing the NDJSON file with a database such as PostgreSQL or SQLite.
- Pull jobs are held in memory. Trades persist across restarts; job history does not.
- There is no authentication on the HTTP or WebSocket endpoints.
