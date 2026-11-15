# Design: upstream calls in skein (fast by default, no backend named)

Status: proposal from the million-users entry (Milli) to skein. The entry's own Postgres glue (`server.goose`, about 330 lines) is the evidence and is meant to be thrown away once this exists. Claims tagged **[M]** are measured, **[?]** are unverified Goose questions for the skein agent.

## 1. The goal

An app that talks to a backend (Postgres, Redis, another HTTP service) should write its routes and nothing else, and be fast without tuning:

```goose
fn routes(r: skein::Request&) {
    r.get("/feed") { req =>
        req.call(DB, pg::query(FEED, 20)) { rows => req.ok_list_in("posts", rows) }
    };
    r.post(api, "/posts/:id/like") { req =>
        req.call(DB, pg::exec(LIKE, req.user.id, req.int("id"))) { res => /* 201 or 200 by res.affected */ }
    };
}
```

skein knows what an upstream *is* (a pool of pipelined, in-order connections). It never knows what Postgres is.

## 2. What the entry taught us

- **[M]** One blocking query per worker: ~450 req/s per worker, worker 94% idle. The wait model is what matters, not the driver.
- **[M]** Dialled, pipelined connections: 5,240 req/s per worker at 14,000 users, p95 equal to the epoll entry, 0 errors.
- **[M]** The batching window (tcprouter v0.5.0) takes CPU from 50% to 36% at 16 events / 1,000 µs, p95 unchanged. Fewer, larger sends and fewer wakeups are the main efficiency lever; per-request work is the rest.
- **[M]** `perf`: 57% of samples are kernel (send/recv/TCP), 32% user space. The biggest single user function is my row decode and JSON render (9.7%).
- About 70% of the glue is generic (connection lifecycle, job table, queues, backpressure, pending sends, matching answers to parked requests). About 30% is protocol (statements, Bind/Sync, row decoding, error codes).
- plod (HttpArena) writes none of this: its pool (`pg.pool` in goose-postgres) and its re-entrant handler (`pg::query(pool, ticket, ...) { r => ... }` returns WAITS, the route is re-run at each event) are library features. We need the same, without tying skein to one backend or one loop.

## 3. The pieces

### 3.1 Upstream (skein core, protocol-free)

A per-worker pool of links to one backend, declared once:

```goose
var app = skein::Config { .. };
let DB = app.upstream("DATABASE_URL", pg::Adapter { }, skein::Pool { conns: 128, lanes: [Lane { share: 75 }, Lane { share: 25 }] });
```

- **Connections are per worker**, `conns / workers` each (at least 1), dialled at worker start (name resolved once, blocking, at start), redialled with exponential backoff and jitter, never blocking the loop.
- **Pipelined, in-order.** Each connection has a FIFO of in-flight calls. A reply completes the oldest. Cap per connection (`max_inflight`, default from the adapter), then calls wait in a bounded queue.
- **Lanes.** The adapter classifies a call (`lane(call) -> i64`). Typical use: reads share lanes and pack into one batch; writes use their own lane so one slow commit cannot stall reads. A call picks the least-loaded ready connection in its lane. (The entry's epoll version did exactly this: last quarter of connections for writes.)
- **Per-pass batching.** Calls issued while the worker processes one event-loop pass are encoded into the chosen connection's buffer and sent once at the end of the pass (one `send` per connection per pass), not one per call. Needs an end-of-pass hook from tcprouter (§5). The adapter gets `batch_end(out)` per connection per flush (Postgres adds one Sync after a packed batch of reads).
- **Backpressure and admission, built in.** Bounded waiting (`max_waiting`, exists). Over it, 503 + `Retry-After: 1`. A call has a deadline (default 5 s); a call that finds no ready connection by then gets 503. A full send queue (`send` returns false) holds the bytes for the connection and retries on Writable. The calling request stays parked and unparsed, never a 500 (the lesson of the full-ring bug in the epoll entry).
- **Failure.** Connection lost: in-flight calls complete with a `Lost` error, and reads (adapter says `idempotent(call)`) are retried once on another connection; others get 503. Redial after backoff. Late events for a reused link number are dropped by generation (skein already has this).
- **Draining.** After SIGTERM no new calls; in-flight calls finish; links close.
- **Stats** per upstream: in-flight, queued, batch size histogram, reconnects, timeouts, sheds.

### 3.2 Adapter (a package per protocol, small)

Overloads on one fixed-size type `A`, resolved like goose-postgres's facade resolves its transport. skein calls them by name:

```goose
fn up::open(a: A&, c: ConnState&, out: u8[>..]&)                 // link opened: handshake, auth, prepare statements
fn up::ready(a: const A&, c: const ConnState&) -> bool           // handshake done
fn up::encode(a: A&, c: ConnState&, call: const Call&, out: u8[>..]&) -> bool   // one call's bytes; false = refuse (too big, bad)
fn up::lane(a: const A&, call: const Call&) -> i64
fn up::idempotent(a: const A&, call: const Call&) -> bool
fn up::batch_end(a: A&, c: ConnState&, out: u8[>..]&)            // optional: after a batch
fn up::feed(a: A&, c: ConnState&, bytes: const u8[:], out: u8[>..]&, done: Done&) -> i64
        // consume what arrived; for each finished reply, done.reply(view) completes the oldest call; extra bytes to send go to out
```

`Call` and `Reply` are small fixed-size types the adapter defines. The pg adapter lives beside goose-postgres's core (pipelining verified: 500 queries in one write, in order [M]), not in skein.

### 3.3 Calls without coroutines: replay (re-entry)

Goose has no coroutines (docs/design/coroutines.md is a note), blocks can't be stored. So `req.call(DB, c) { reply => ... }`:

1. **First run**, in the request's own pass: the route runs up to `req.call`. skein encodes the call into the upstream, parks the request, and stores the request's *actual bytes* (head + body, typically 300–600 B) in a slab keyed by the ticket. The block is skipped.
2. **Reply**: when the adapter reports the reply complete, skein runs the route again from the stored bytes in a "resumed" mode: the route's matching runs as usual, everything before `req.call` repeats, and now `req.call` returns true and runs the block with the `reply` view. The response is built **straight from the reply bytes into the response buffer**, and the reply bytes (in the connection's input buffer) are released afterwards.
3. Rule for apps: code before `req.call` is cheap and free of side effects (it runs twice). Guards and middleware (auth, validation, error texts) run in both runs; `before` middleware must be idempotent, `after` middleware runs once, on the final response (this already exists as `async_finish`).
4. A request may chain calls: `req.call` nests; each level has its own stored position (a small counter), replayed in order. Support at least 3 levels, document the cost.

What this removes from the entry: the job table, free list, wait queues, done queue, `OUT` buffer, the second copy of the answer, the park/finish split, `async_ready` polling.

**No intermediate copy [?]**: today a row goes DataRow → `Post` struct (2 KB inline) → JSON → per-connection result buffer → `OUT` → `body_out`: four copies. With the reply view, the JSON writer reads columns as slices straight out of the pinned buffer and writes into `body_out`. The reply view is valid only inside the block.

### 3.4 Results: how big, how held

- The reply view points into the connection's input buffer; the adapter keeps whole replies (up to a limit) contiguous. A reply larger than the limit is an adapter error (500 or 502), not a crash.
- The only per-request storage is the request-bytes slab and a ticket (`conn, gen, user, flags`, 16 bytes). Both sized by `max_waiting`, allocated at start. No per-request allocation anywhere on the path.

## 4. Efficiency requirements (checked against numbers)

- One `send` per upstream connection per pass; one `recv` per readable connection per pass.
- Zero heap allocation per call. Slabs and rings sized at start.
- No second encode: adapter writes the call once, straight into the connection's out buffer.
- Replay costs one extra head parse (~1 µs) per call. Cheaper than the four copies it removes; measure it against the entry (§6).
- Prepared statements are the adapter's job at `open` (a statement table fixed at declaration, so the SQL strings are constants and the hot path sends only Bind/Execute).
- The loop must never block: name resolution at start only; no `getaddrinfo`, file or DNS call after.

## 5. What skein needs from tcprouter (for Charlie/Brian)

- An **end-of-pass hook** (after all events of one pass were delivered, before the next wait). The batching window already creates passes; this lets skein flush upstream buffers once per pass. If it costs too much, skein can flush on the last event of a pass if tcprouter says "this is the last event".
- Nothing else: dial, send, Writable, close, link generation checks and the batching window are all there (v0.5.0).

## 6. Acceptance: the million-users entry as the benchmark

Success criteria, measured the same way as before (`real.sh`: 1 worker, real Postgres 18 in Docker, mload, 14,000 users):
1. The entry is routes, SQL and row mapping only, about 100 lines, and no `tcprouter` name and no upstream bookkeeping.
2. `test/test.sh` 42/42, 0 errors in the load run.
3. CPU busy within 15% of the epoll entry's 27% at its window; p95 within 1.5x.
4. Fault tests in skein's own suite, against a fake upstream (like `dial_test`): upstream killed mid-flight (503, redial, reads retried once); stalled upstream (queue fills, 503 not 500, no memory growth); slow reply; reply split over many reads; a client that leaves while its call is in flight (ticket dropped, slab freed); pipelined requests from one client (order kept); reconnect storm.
5. A second adapter (a toy line-protocol or Redis `GET`/`SET`) proves skein is protocol-free.

## 7. Non-goals and later

- Transactions: a `req.tx(DB) { ... }` that pins a connection for several calls. Later; needs a pinned lane, not a different pool.
- Caching: not skein's job (the challenge forbids caching across requests).
- TLS to the backend: later, same adapter surface.

## 8. Open questions for skein (the Goose [?] items)

1. Can `up::*` overloads on a fixed-size type `A` carry all of §3.2 (as `pg::send/receive/close` do for the facade)? What does the compiler do with several generic parameters (`Call`, `Reply`)?
2. Can replay select the same route block a second time cheaply (stored route id + captured params, rather than a full re-match)? If a full re-match is cheap enough, simpler is better.
3. Slab of variable-size request bytes: a fixed-size block per request (`INBOX`-style, 1–2 KB) with a spill block for rare larger ones, or a ring? Memory at 1M connections matters less than at 1M parked calls (only parked calls need it).
4. Can the reply view be a plain `const u8[:]` plus an adapter accessor type, so the JSON writer takes slices without a struct?
