// The feed API on skein, with Postgres through goose-postgres-with-io's pool. skein owns the sockets (its worker's event loop); the pool is told what happened
// on each connection (pool_opened/data/closed) and lends four operations back (pg::host_*, below). A request parks (req.park()), the pool runs its query, and
// async_ready answers it (req.resume + the reply) once the rows are in.
// Config: DATABASE_URL, JWT_SECRET, HOST, PORT, EXTRA_PORT (skein reads them).
import tcprouter from "github.com/bhare1985/tcprouter@v0.6.0";
import gslib from "github.com/SorinS/gslib@024b252e1d519df971b142146239d5cd3cde5e8d";
import hash from "github.com/kaitk/goose-hash@v0.0.4";
import pg from "/home/bhare/dev/goose-io/.implementations/goose-postgres-with-io";
import pg.pool;
import json;
import skein from "../../../skein";
import skein.deferred;
import skein.auth;

extern fn isal_gzip(out: u8[>..]&, src: const u8&, len: i64) -> i32;      // isal.c: appends a gzip stream, or returns 0

// ---- the host: skein's worker lends the pool its sockets ----

// A skein::Link as the pool's handle: nonzero (bit 62), slot and generation inside.
fn handle_of(l: skein::Link) -> i64 { (1 << 62) | ((l.slot as i64) << 32) | (l.gen as i64) }
fn link_of(h: i64) -> skein::Link { skein::Link { slot: ((h >> 32) & 0x3fffffff) as u32, gen: (h & 0xffffffff) as u32 } }

fn pg::host_dial(w: skein::Worker&, where: const u8[:], ms: i64) -> i64 { handle_of(skein::connect(w, where, ms)) }
fn pg::host_send(w: skein::Worker&, h: i64, bytes: const u8[:]) -> bool { skein::send(w, link_of(h), bytes) }
fn pg::host_close(w: skein::Worker&, h: i64) { skein::close(w, link_of(h)); }
fn pg::host_now_ms(w: skein::Worker&) -> i64 { skein::now_ms() }

// ---- the requests that wait for the pool ----

let POST = "SELECT p.id, p.body, p.created_at, u.username, (SELECT count(*) FROM likes l WHERE l.post_id = p.id)::int AS like_count FROM posts p JOIN users u ON u.id = p.user_id";
let FEED_SQL = str(POST, " ORDER BY p.created_at DESC, p.id DESC LIMIT $1");
let POST_SQL = str(POST, " WHERE p.id = $1");
let FEED = 0;
let ONE_POST = 1;
let HEALTH = 2;
let CREATE = 3;
let LIKE = 4;
let MAX_REQS = 1024;                 // parked requests at once (Config.max_waiting stays below it)
let DEADLINE_MS = 800;

// A request waiting for the pool: what it needs to run its query again each time the pool wakes it.
struct Waiting { used: bool, kind: u8, id: i64, user: i64, t: skein::Ticket, pt: pg::Ticket, text: u8[..2048], username: u8[..64] }

var REQS: Waiting[1024] = [Waiting { used: false, kind: 0, id: 0, user: 0, t: skein::Ticket { ok: false, conn: tcprouter::Conn { slot: 0, gen: 0 }, user: 0, head_only: false, gzip: false, close: false, slab: -1 }, pt: pg::Ticket { .. }, text: [], username: [] }; 1024];
var FRESH: i32[1024] = [0; 1024];    // requests parked in this pass, not yet sent
var FRESH_N: i64 = 0;
var BY_KEY: i32[8192] = [-1; 8192];  // pool key (mod 8192) -> index in REQS
var DB: pg::Pool = pg::Pool { cfg: pg::Config { .. }, .. };
var STARTED_MS: i64 = 0;
var READY = false;

let GZIP_MIN = 256;                  // smaller bodies go out as they are

// Postgres sends "2025-12-31 23:59:19.409123+00" (zeros after the point are trimmed, the session runs in UTC): the API wants "2025-12-31T23:59:19.409Z".
fn iso(text: const u8[:], out: u8[>..]&) {
    guard text.len >= 19 else { out.append(text); return; }
    out.append(text[..10]); out.append("T"); out.append(text[11..19]); out.append(".");
    var i = 0;
    while i < 3 { let at = 20 + i; out.append(if at < text.len && text[at] >= 48 && text[at] <= 57 { text[at..at + 1] } else { "0" }); i += 1; }
    out.append("Z");
}

// {"id":..,"body":..,"created_at":..,"author":..,"like_count":..} from a row of POST's columns.
fn write_post(out: u8[>..]&, r: pg::Row) {
    out.append("{\"id\":"); format(out, pg::int(r, 0));
    out.append(",\"body\":"); json::format_string(out, pg::text(r, 1));
    out.append(",\"created_at\":\""); iso(pg::text(r, 2), out);
    out.append("\",\"author\":"); json::format_string(out, pg::text(r, 3));
    out.append(",\"like_count\":"); format(out, pg::int(r, 4));
    out.push('}');
}

// Characters, not bytes: the continuation bytes of UTF-8 (10xxxxxx) do not count. The limit matches Postgres char_length().
fn code_points(text: const u8[:]) -> i64 {
    var n = 0;
    for b in text { if (b & 0xC0) != 0x80 { n += 1; } }
    n
}

// Parks the request and queues its query: it is sent from async_ready, with the rest of the pass's.
fn start(req: skein::Request&, kind: u8, id: i64, user: i64, text: const u8[:], username: const u8[:]) {
    let t = req.park();
    guard t.ok else { return; }
    var i = 0;
    while i < MAX_REQS && REQS[i].used { i += 1; }
    if i == MAX_REQS { req.resume(t); req.header("Retry-After", "1"); req.fail(503, "too many waiting requests"); return; }
    let r .= REQS[i];
    r.used = true; r.kind = kind; r.id = id; r.user = user; r.t = t;
    r.pt = pg::Ticket { .. };
    r.text.clear(); r.text.append(text);
    r.username.clear(); r.username.append(username[..min(username.len, 64)]);
    FRESH[FRESH_N] = i as i32;
    FRESH_N += 1;
}

fn feed(req: skein::Request&) { start(req, FEED, 0, 0, "", ""); }
fn one_post(req: skein::Request&) { start(req, ONE_POST, req.int("id"), 0, "", ""); }
fn health(req: skein::Request&) { start(req, HEALTH, 0, 0, "", ""); }

fn create(req: skein::Request&) {
    guard req.json_valid() else { req.bad("malformed JSON body"); return; }
    let raw, ok = req.json_string("body");
    guard ok else { req.bad("body is required"); return; }
    let text = raw.trim();
    guard text.len > 0 else { req.bad("body is required"); return; }
    guard code_points(text) <= 500 else { req.bad("body must be at most 500 characters"); return; }
    start(req, CREATE, 0, req.user.id, text, req.user.username[..]);
}

fn like(req: skein::Request&) { start(req, LIKE, req.int("id"), req.user.id, "", ""); }

// Runs request i's query once more. true: it is finished and the answer is built (the request was resumed); false: it waits.
fn drive(w: skein::Worker&, req: skein::Request&, i: i64) -> bool {
    let r .= REQS[i];
    var body: u8[>..] = [];
    var n: i64 = 0;
    var rows = 0;
    if r.kind == FEED {
        body.append("{\"posts\":[");
        n = pg::query(DB, r.pt, w, DEADLINE_MS, FEED_SQL, 20) { row => if rows > 0 { body.push(','); } write_post(body, row); rows += 1; };
        body.append("]}");
    } else if r.kind == ONE_POST {
        body.append("{\"post\":");
        n = pg::query(DB, r.pt, w, DEADLINE_MS, POST_SQL, r.id) { row => write_post(body, row); rows += 1; };
        body.push('}');
    } else if r.kind == HEALTH {
        n = pg::query(DB, r.pt, w, DEADLINE_MS, "SELECT 1") { row => rows += 1; };
    } else if r.kind == CREATE {
        n = pg::query(DB, r.pt, w, DEADLINE_MS, "INSERT INTO posts (user_id, body) VALUES ($1, $2) RETURNING id, created_at", r.user, r.text[..]) { row =>
            body.append("{\"post\":{\"id\":"); format(body, pg::int(row, 0));
            body.append(",\"body\":"); json::format_string(body, r.text[..]);
            body.append(",\"created_at\":\""); iso(pg::text(row, 1), body);
            body.append("\",\"author\":"); json::format_string(body, r.username[..]);
            body.append(",\"like_count\":0}}");
            rows += 1;
        };
    } else {
        n = pg::query(DB, r.pt, w, DEADLINE_MS, "INSERT INTO likes (user_id, post_id) VALUES ($1, $2) ON CONFLICT (user_id, post_id) DO NOTHING", r.user, r.id) { row => };
    }
    if r.pt.key != 0 { BY_KEY[r.pt.key & 8191] = i as i32; }
    if n == pg::WAITS { return false; }
    // finished
    let key = r.pt.key;
    if key != 0 && BY_KEY[key & 8191] == i as i32 { BY_KEY[key & 8191] = -1; }
    req.resume(r.t);
    r.used = false;
    if n == pg::FAILS {
        let code = r.pt.code[..];
        if code == "23503" { req.not_found("post not found"); }
        else if code == "53300" || (code.len >= 2 && code[..2] == "08") { req.header("Retry-After", "1"); req.fail(503, "upstream unavailable"); }
        else { req.fail(500, "internal error"); }
        return true;
    }
    if r.kind == ONE_POST && rows == 0 { req.not_found("post not found"); return true; }
    if r.kind == HEALTH {
        body.append("{\"status\":\"ok\",\"db\":\"ok\",\"uptime_s\":"); format(body, (skein::now_ms() - STARTED_MS) / 1000); body.push('}');
        req.reply(200, body[..]);
    } else if r.kind == LIKE {
        let first = n == 1;
        body.append("{\"liked\":true,\"already_liked\":"); body.append(if first { "false" } else { "true" });
        body.append(",\"post_id\":"); format(body, r.id); body.push('}');
        req.reply(if first { 201 } else { 200 }, body[..]);
    } else {
        req.reply(if r.kind == CREATE { 201 } else { 200 }, body[..]);
    }
    true
}

// The challenge's exact error texts (SPEC.md). skein reports an error as data (req.err); this middleware says it in the spec's words.
struct SpecErrors { }
fn after(m: SpecErrors, req: skein::Request&) {
    if req.err.kind == skein::ERR_NONE { return; }
    var msg: const u8[:] = "";
    if req.err.kind == skein::ERR_AUTH {
        msg = if req.err.jwt == skein::JWT_MISSING { "missing bearer token" } else if req.err.jwt == skein::JWT_CLAIMS { "invalid token payload" } else { "invalid or expired token" };
    } else if req.err.kind == skein::ERR_BAD_PARAM { msg = "invalid post id"; }
    else { return; }
    var body: u8[>..] = [];
    skein::write_error(body, msg);
    req.replace(req.err.status, "application/json", body[..]);
}

// Compression with ISA-L (isal.c) instead of skein's own deflate: nothing is kept between requests.
struct IsaGzip { }
fn after(m: IsaGzip, req: skein::Request&) {
    let body = req.response_body();
    if req.res.encoded || req.res.status != 200 || body.len < GZIP_MIN || !skein::accepts_gzip(req.header("Accept-Encoding")) { return; }
    req.zbody.clear();
    if isal_gzip(req.zbody, body[0], body.len) == 1 { req.use_encoded("gzip"); }
}

fn routes(r: skein::Request&) {
    let api = skein::scope("", skein::JwtAuth { });
    r.use(IsaGzip { }) { r.use(SpecErrors { }) {       // afters run innermost first: SpecErrors rewrites an error, then IsaGzip sees the final body
        r.get("/health") { req => health(req) };
        r.get("/feed") { req => feed(req) };
        r.get("/posts/:id") { req => one_post(req) };
        r.post(api, "/posts") { req => create(req) };
        r.post(api, "/posts/:id/like") { req => like(req) };
    }; };
}

// ---- skein's hooks ----

fn async_start(w: skein::Worker&, req: skein::Request&) {
    STARTED_MS = skein::now_ms();
    var cfg, ok = pg::parse_url(skein::UP_URL[..]);
    if !ok { write_stderr("DATABASE_URL: not a postgres:// URL\n"); return; }
    cfg.time_zone.append("UTC");                      // the API's timestamps are UTC
    var host: u8[>..] = [];
    if skein::resolve_ipv4(cfg.host[..], host) { cfg.host.clear(); cfg.host.append(host[..]); }
    DB = pg::pool(cfg, max(1, min(16, skein::UP_CONNS / skein::UP_WORKERS)));
    READY = true;
}

fn failure_of(why: u8) -> pg::PoolFailure { if why == skein::LINK_CONNECT_FAILED { pg::PoolFailure.Refused } else { pg::PoolFailure.Other } }

fn async_event(w: skein::Worker&, ev: skein::LinkEvent, req: skein::Request&) {
    if !READY { return; }
    match ev {
        Opened e => { pg::pool_opened(DB, handle_of(e.link)); },
        Data e => { pg::pool_data(DB, w, handle_of(e.link), e.bytes); },
        Closed e => { pg::pool_closed(DB, handle_of(e.link), failure_of(e.why)); },
        _ => {},
    }
}

// Once per pass: sends the queries of the requests parked in it, then calls each request the pool names, until one is finished (true: its answer is built).
fn async_ready(w: skein::Worker&, req: skein::Request&) -> bool {
    if !READY { return false; }
    pg::pool_tick(DB, skein::now_ms());
    loop {
        var i = -1;
        if FRESH_N > 0 {
            FRESH_N -= 1;
            i = FRESH[FRESH_N] as i64;
        } else {
            let key = pg::wake(DB);
            if key == 0 { return false; }
            let j = BY_KEY[key & 8191];
            if j < 0 || !REQS[j].used || REQS[j].pt.key != key { continue; }
            i = j as i64;
        }
        if drive(w, req, i) { return true; }
    }
}
fn async_finish(w: skein::Worker&, req: skein::Request&) { after(IsaGzip { }, req); }

fn main() {
    var app = skein::Config { require_jwt: true, require_db: true, jwt_min_secret: 1, max_waiting: 1000, upstream_conns: 128, upstream_deadline_ms: 800, .. };
    app.env();
    app.upstream_url.clear();                        // the one URL: DATABASE_URL
    app.upstream_url.append(app.database_url[..]);
    app.run_async();
}
