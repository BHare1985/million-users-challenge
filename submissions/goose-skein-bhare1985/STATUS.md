# goose-skein-bhare1985: status (work in progress, not ready for a PR)

The feed API (SPEC.md, 5 routes) on skein, a Goose web library, over tcprouter, with a small Postgres adapter on goose-postgres's core. `server.goose` is the whole app; `DESIGN-upstream.md` is the design that skein's upstream pool follows.

## What works
- `server.goose` builds and passes `test/test.sh` 42/42. About 350 lines: routes, SQL, spec error texts and ISA-L gzip as skein middleware, and a ~150-line Postgres adapter (login through goose-postgres's core; Bind/Execute/DataRow handled in a few helpers).
- It uses only skein names; the one tcprouter mention is the `import tcprouter …@v0.6.0` line Goose requires in the root.

## Build (as it stands)
```
LIBRARY_PATH=$PWD/deps/lib ~/dev/tcprouter/goose-kb/bin/goose build server.goose
```
- `deps` is a symlink to the epoll entry's `deps` (libpq is not used here; ISA-L is: `deps/lib/libisal.a`).
- Imports skein by relative path, `import skein from "../../../skein"` (skein lives at `~/dev/skein` and is not moving). It must be pinned to a git reference for a real submission.
- Tested against skein `99ef142` and tcprouter `v0.6.0`.
- Config in `main`: 2 lanes (reads 0, writes 1), `upstream_conns: 128` split over the workers, `upstream_max_queue: 4096`, `upstream_deadline_ms: 800`, `upstream_in_max: 131072` (see below), `parked_request_max` default. Env: `DATABASE_URL`, `JWT_SECRET`, `HOST`, `PORT`, `EXTRA_PORT`; `BATCH_MIN_EVENTS`/`BATCH_MAX_WAIT_US` (tcprouter's batching window, 32 / 2000 used in the ladder runs).

## Measurements (one core, real Postgres 18 in Docker, mload; the challenge's pass rule: p95 < 500 ms, p99 < 1 s, < 1% errors)
Scratch scripts were in the session scratchpad (`real.sh`, `ladder.sh`); the numbers:
| Entry | Passes | Fails |
|---|---|---|
| epoll entry (window 32 / 2000 µs) | 30,000 (CPU 45%, p99 943 ms) | 35,000: p95 1.8 s, 0 errors |
| skein, `upstream_in_max` 131072, window 32 / 2000 | 25,000 (CPU 57%, p95 31 ms, 0 errors) | 30,000: 15% errors (503), p99 1.07 s |
| skein, default link buffer (32 KiB) | 20,000 | 25,000: 52–87% errors, intermittent |
So skein is at about 83% of the epoll entry's user count on one core. CPU at 20,000 users was 1.4x the epoll entry's before the per-pass flush; not re-measured since.

## Findings
- **Link buffer:** skein's default 32 KiB per-link receive buffer is smaller than a burst of pipelined 5 KB feed replies, so links were closed and redialled under load (stats showed `redials` growing ~8 per second, each costing a SCRAM login and dropping in-flight calls). `upstream_in_max: 131072` removed the collapse.
- **Shedding:** with a 256 queue cap the pool turned overload into instant 503s. Fixed in skein 99ef142 (`upstream_max_queue` up to 4096, shed by deadline).
- **Per-pass flush** is in skein 99ef142 (tcprouter v0.6.0 PassEnd): `batch_max` 64, about 3 calls per flush. Packing reads under one Sync through `upstream_batch_end` is not done yet (reads still have a Sync each).

## Left to do
1. A real submission: `build.sh` (libisal pinned and built from the same tarball as the epoll entry), `install.sh`, `start.sh`, README, a pinned skein reference instead of the relative path.
2. Pack reads under one Sync with `upstream_batch_end` (needs per-statement completion, from CommandComplete) and re-measure.
3. `/health` when Postgres is down: use `req.try_call` to answer the spec's degraded 503 body (skein has it since ef9ecd7; the entry does not use it yet).
4. Profile again (CPU gap to the epoll entry) and re-run the ladder from 25,000 to 30,000 in steps of 1,000.
5. Run the 500-character UTF-8 body check with the parked-request block size.
6. `server_blocking.goose` was left uncommitted: it is the old blocking baseline and no longer builds.

`pseudocode.goose` is the original sketch the entry grew from.
