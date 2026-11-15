// The feed API on skein (a Goose web library, think Fastify). Routes, JSON, JWT, gzip and the pool of Postgres connections come from skein; this file
// is the routes, the SQL, and a small Postgres adapter on goose-postgres's core (startup and SCRAM; the rest of the protocol is a few lines below).
// Config: DATABASE_URL, JWT_SECRET, HOST, PORT, EXTRA_PORT (skein reads them).
import tcprouter from "github.com/bhare1985/tcprouter@v0.6.0";
import gslib from "github.com/SorinS/gslib@024b252e1d519df971b142146239d5cd3cde5e8d";
import hash from "github.com/kaitk/goose-hash@v0.0.4";
import pg from "github.com/kaitk/goose-postgres@c89f8b421b0aad75e7dda1a34197ca910ce7137d";
import json;
import skein from "../../../skein";
import skein.upstream;
import skein.auth;

extern fn secure_random(buf: u8&, n: i64) -> i32;                          // entropy.c: SCRAM's nonce
extern fn isal_gzip(out: u8[>..]&, src: const u8&, len: i64) -> i32;      // isal.c: appends a gzip stream, or returns 0

// ---- Postgres adapter for skein.upstream (the whole protocol this app speaks) ----

let POST = "SELECT p.id, p.body, p.created_at, u.username, (SELECT count(*) FROM likes l WHERE l.post_id = p.id)::int AS like_count FROM posts p JOIN users u ON u.id = p.user_id";
let FEED = 0;
let ONE_POST = 1;
let HEALTH = 2;
let CREATE = 3;
let LIKE = 4;

struct PgAdapter { cfg: pg::Config }
struct PgConn { c: pg::Conn, phase: u8 }                       // 0 startup and login (the core), 1 statements being set up, 2 ready

// The core reads login bytes from, and writes its answers to, buffers of the worker that it can see are not part of its own state.
var STAGE: u8[..4096] = [];
var LOGIN: u8[>..] = [];
struct PgCall { kind: u8, id: i64, user: i64, text: u8[..2048] }

fn upstream_open(a: PgAdapter&, s: PgConn&, out: u8[>..]&) {
    var nonce: u8[24] = [0; 24];
    if secure_random(nonce[0], 24) != 1 { return; }
    pg::start(s.c, out, a.cfg, nonce[..]);
}

fn upstream_link_ready(a: PgAdapter&, s: PgConn&) -> bool { s.phase == 2 }
fn upstream_idempotent(a: PgAdapter&, c: PgCall&) -> bool { c.kind != CREATE && c.kind != LIKE }
fn upstream_lane(a: PgAdapter&, c: PgCall&) -> i64 { if c.kind == CREATE || c.kind == LIKE { 1 } else { 0 } }      // writes have their own connections

// The statements are prepared once per connection, and the session runs in UTC as the API's timestamps are. The core's queue is not used for these
// entries (the replies are read by reply_end below), so its count is put back after each one.
fn warm(s: PgConn&, out: u8[>..]&) {
    pg::parse(s.c, out, "health", "SELECT 1");
    pg::parse(s.c, out, "feed", str(POST, " ORDER BY p.created_at DESC, p.id DESC LIMIT $1"));
    pg::parse(s.c, out, "post", str(POST, " WHERE p.id = $1"));
    pg::parse(s.c, out, "insert_post", "INSERT INTO posts (user_id, body) VALUES ($1, $2) RETURNING id, created_at");
    pg::parse(s.c, out, "insert_like", "INSERT INTO likes (user_id, post_id) VALUES ($1, $2) ON CONFLICT (user_id, post_id) DO NOTHING");
    pg::parse(s.c, out, "", "SET TimeZone TO 'UTC'");
    let b = pg::bind(out, "");
    pg::execute(s.c, out, b);
    pg::sync(s.c, out);
    s.c.count = 0;
    s.c.head = 0;
}

fn upstream_encode(a: PgAdapter&, s: PgConn&, c: PgCall&, out: u8[>..]&) -> bool {
    if c.kind == FEED {
        let b = pg::bind(out, "feed");
        pg::param(out, b, 20);
        pg::execute(s.c, out, b);
    } else if c.kind == ONE_POST {
        let b = pg::bind(out, "post");
        pg::param(out, b, c.id);
        pg::execute(s.c, out, b);
    } else if c.kind == HEALTH {
        let b = pg::bind(out, "health");
        pg::execute(s.c, out, b);
    } else if c.kind == CREATE {
        let b = pg::bind(out, "insert_post");
        pg::param(out, b, c.user);
        pg::param(out, b, c.text[..]);
        pg::execute(s.c, out, b);
    } else {
        let b = pg::bind(out, "insert_like");
        pg::param(out, b, c.user);
        pg::param(out, b, c.id);
        pg::execute(s.c, out, b);
    }
    pg::sync(s.c, out);
    s.c.count = 0;
    s.c.head = 0;
    true
}

fn be32(b: const u8[:], at: i64) -> i64 { ((b[at] as i64) << 24) | ((b[at + 1] as i64) << 16) | ((b[at + 2] as i64) << 8) | (b[at + 3] as i64) }
fn be16(b: const u8[:], at: i64) -> i64 { ((b[at] as i64) << 8) | (b[at + 1] as i64) }

// Where the first reply ends: just after its ReadyForQuery ('Z'). 0 if it has not all arrived, -1 if the bytes are not Postgres messages.
fn reply_end(b: const u8[:]) -> i64 {
    var at = 0;
    loop {
        if b.len - at < 5 { return 0; }
        let size = be32(b, at + 1);
        if size < 4 || size > 1048576 { return -1; }
        if b.len - at < 1 + size { return 0; }
        let kind = b[at];
        at += 1 + size;
        if kind == 'Z' { return at; }
    }
}

fn upstream_feed(a: PgAdapter&, s: PgConn&, bytes: const u8[:], out: u8[>..]&) -> i64, i64, i64, i64 {
    if s.phase == 0 {
        STAGE.clear();
        STAGE.append(bytes[..min(bytes.len, 4096)]);
        LOGIN.clear();
        let e, body = pg::step(s.c, STAGE[..], LOGIN);
        out.append(LOGIN[..]);
        let used = s.c.used;
        if e == pg::FAILED || e == pg::ERROR { return used, 2, 0, 0; }
        if e == pg::READY { s.phase = 1; LOGIN.clear(); warm(s, LOGIN); out.append(LOGIN[..]); }
        return used, 0, 0, 0;
    }
    let end = reply_end(bytes);
    if end < 0 { return 0, 2, 0, 0; }
    if end == 0 { return 0, 0, 0, 0; }
    if s.phase == 1 { s.phase = 2; return end, 0, 0, 0; }          // the statements' replies: nobody asked for them
    return end, 1, 0, end
}

// Reading a reply: its DataRow messages (text format), its error, its command tag. The reply is valid inside the call block only.
struct PgRow { n: i64, at: i32[16], len: i32[16], body: const u8[:] }

fn pg_rows<F>(reply: const u8[:]) {
    var at = 0;
    while at + 5 <= reply.len {
        let size = be32(reply, at + 1);
        if reply[at] == 'D' {
            let body = reply[at + 5..at + 1 + size];
            var row = PgRow { n: be16(body, 0), at: [0; 16], len: [0; 16], body: body };
            var p = 2;
            var i = 0;
            while i < row.n && i < 16 {
                let l = be32(body, p);
                p += 4;
                if l >= 0 { row.at[i] = p as i32; row.len[i] = l as i32; p += l; } else { row.at[i] = 0; row.len[i] = -1; }
                i += 1;
            }
            F(row);
        }
        at += 1 + size;
    }
}

fn col(r: PgRow, i: i64) -> const u8[:] { if r.len[i] < 0 { r.body[..0] } else { r.body[r.at[i]..r.at[i] + r.len[i]] } }
fn col_int(r: PgRow, i: i64) -> i64 {
    var v = 0;
    var neg = false;
    for b in col(r, i) { if b == '-' { neg = true; } else if b >= '0' && b <= '9' { v = v * 10 + (b - '0') as i64; } }
    if neg { -v } else { v }
}

// The SQLSTATE of the reply's ErrorResponse, or "" for none.
fn pg_error(reply: const u8[:]) -> u8[..5] {
    var code: u8[..5] = [];
    var at = 0;
    while at + 5 <= reply.len {
        let size = be32(reply, at + 1);
        if reply[at] == 'E' {
            var p = at + 5;
            while p < at + 1 + size && reply[p] != 0 {
                let field = reply[p];
                var q = p + 1;
                while reply[q] != 0 { q += 1; }
                if field == 'C' { code.append(reply[p + 1..min(q, p + 6)]); return code; }
                p = q + 1;
            }
            code.append("XX000");
            return code;
        }
        at += 1 + size;
    }
    code
}

// The number at the end of the CommandComplete tag ("INSERT 0 1": 1).
fn pg_affected(reply: const u8[:]) -> i64 {
    var at = 0;
    while at + 5 <= reply.len {
        let size = be32(reply, at + 1);
        if reply[at] == 'C' {
            var v = 0;
            var scale = 1;
            var p = at + 1 + size - 2;
            while p > at + 4 && reply[p] >= '0' && reply[p] <= '9' { v += (reply[p] - '0') as i64 * scale; scale *= 10; p -= 1; }
            return v;
        }
        at += 1 + size;
    }
    0
}

// ---- the app ----

let GZIP_MIN = 256;                  // smaller bodies go out as they are
var DB: skein::Upstream<PgAdapter, PgConn, PgCall> = skein::Upstream<PgAdapter, PgConn, PgCall> { .. };
var STARTED_MS: i64 = 0;

// Postgres sends "2025-12-31 23:59:19.409123+00" (zeros after the point are trimmed, the session runs in UTC): the API wants "2025-12-31T23:59:19.409Z".
fn iso(text: const u8[:], out: u8[>..]&) {
    guard text.len >= 19 else { out.append(text); return; }
    out.append(text[..10]); out.append("T"); out.append(text[11..19]); out.append(".");
    var i = 0;
    while i < 3 { let at = 20 + i; out.append(if at < text.len && text[at] >= 48 && text[at] <= 57 { text[at..at + 1] } else { "0" }); i += 1; }
    out.append("Z");
}

// {"id":..,"body":..,"created_at":..,"author":..,"like_count":..} from a row of POST's columns.
fn write_post(out: u8[>..]&, r: PgRow) {
    out.append("{\"id\":"); format(out, col_int(r, 0));
    out.append(",\"body\":"); json::format_string(out, col(r, 1));
    out.append(",\"created_at\":\""); iso(col(r, 2), out);
    out.append("\",\"author\":"); json::format_string(out, col(r, 3));
    out.append(",\"like_count\":"); format(out, col_int(r, 4));
    out.push('}');
}

fn failed(req: skein::Request&, reply: const u8[:]) -> bool {
    let code = pg_error(reply);
    if code.len == 0 { return false; }
    if code[..] == "23503" { req.not_found("post not found"); } else { req.fail(500, "internal error"); }       // 23503: the foreign key, no such post
    true
}

fn feed(req: skein::Request&) {
    req.call(DB, PgCall { kind: FEED, id: 0, user: 0, text: "" }) { reply =>
        if failed(req, reply) { return; }
        var body: u8[>..] = [];
        body.append("{\"posts\":[");
        var n = 0;
        pg_rows(reply) { r => if n > 0 { body.push(','); } write_post(body, r); n += 1; };
        body.append("]}");
        req.reply(200, body[..]);
    };
}

fn one_post(req: skein::Request&) {
    req.call(DB, PgCall { kind: ONE_POST, id: req.int("id"), user: 0, text: "" }) { reply =>
        if failed(req, reply) { return; }
        var body: u8[>..] = [];
        var n = 0;
        body.append("{\"post\":");
        pg_rows(reply) { r => write_post(body, r); n += 1; };
        if n == 0 { req.not_found("post not found"); return; }
        body.push('}');
        req.reply(200, body[..]);
    };
}

fn health(req: skein::Request&) {
    req.call(DB, PgCall { kind: HEALTH, id: 0, user: 0, text: "" }) { reply =>
        if failed(req, reply) { return; }
        var body: u8[>..] = [];
        body.append("{\"status\":\"ok\",\"db\":\"ok\",\"uptime_s\":"); format(body, (skein::now_ms() - STARTED_MS) / 1000); body.push('}');
        req.reply(200, body[..]);
    };
}

// Characters, not bytes: the continuation bytes of UTF-8 (10xxxxxx) do not count. The limit matches Postgres char_length().
fn code_points(text: const u8[:]) -> i64 {
    var n = 0;
    for b in text { if (b & 0xC0) != 0x80 { n += 1; } }
    n
}

fn create(req: skein::Request&) {
    guard req.json_valid() else { req.bad("malformed JSON body"); return; }
    let raw, ok = req.json_string("body");
    guard ok else { req.bad("body is required"); return; }
    let text = raw.trim();
    guard text.len > 0 else { req.bad("body is required"); return; }
    guard code_points(text) <= 500 else { req.bad("body must be at most 500 characters"); return; }
    var c = PgCall { kind: CREATE, id: 0, user: req.user.id, text: "" };
    c.text.append(text);
    req.call(DB, c) { reply =>
        if failed(req, reply) { return; }
        var body: u8[>..] = [];
        pg_rows(reply) { r =>
            body.append("{\"post\":{\"id\":"); format(body, col_int(r, 0));
            body.append(",\"body\":"); json::format_string(body, text);
            body.append(",\"created_at\":\""); iso(col(r, 1), body);
            body.append("\",\"author\":"); json::format_string(body, req.user.username[..]);
            body.append(",\"like_count\":0}}");
        };
        req.reply(201, body[..]);
    };
}

fn like(req: skein::Request&) {
    let id = req.int("id");
    req.call(DB, PgCall { kind: LIKE, id: id, user: req.user.id, text: "" }) { reply =>
        if failed(req, reply) { return; }
        let first = pg_affected(reply) == 1;
        var body: u8[>..] = [];
        body.append("{\"liked\":true,\"already_liked\":"); body.append(if first { "false" } else { "true" });
        body.append(",\"post_id\":"); format(body, id); body.push('}');
        req.reply(if first { 201 } else { 200 }, body[..]);
    };
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

// ---- skein's hooks: the pool does the rest ----

fn async_start(w: skein::Worker&, req: skein::Request&) {
    STARTED_MS = skein::now_ms();
    let cfg, why = pg::url_settings(skein::UP_URL[..]);
    if why.len > 0 { write_stderr(str("DATABASE_URL: ", why, "\n")); return; }
    DB.a.cfg = cfg;
    skein::upstream_start(DB, w, req);
}
fn async_event(w: skein::Worker&, ev: skein::LinkEvent, req: skein::Request&) { skein::upstream_event(DB, w, ev, req); }
fn async_ready(w: skein::Worker&, req: skein::Request&) -> bool { skein::upstream_ready(DB, w, req) }
fn async_finish(w: skein::Worker&, req: skein::Request&) { after(IsaGzip { }, req); }

fn main() {
    var app = skein::Config { require_jwt: true, require_db: true, jwt_min_secret: 1, max_waiting: 1000, upstream_conns: 128, upstream_lanes: 2, upstream_max_queue: 4096, upstream_deadline_ms: 800, upstream_in_max: 131072, .. };
    app.env();
    app.upstream_url.clear();                        // the one URL: DATABASE_URL
    app.upstream_url.append(app.database_url[..]);
    app.run_async();
}
