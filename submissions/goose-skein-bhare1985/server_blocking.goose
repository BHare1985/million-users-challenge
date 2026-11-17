// The feed API on skein (a Goose web library, think Fastify) over tcprouter's epoll workers. Routes, JSON, JWT and gzip come from skein;
// Postgres is goose-postgres through skein.pgblocking, so a worker waits for each query (skein has no deferred reply yet).
// Config: DATABASE_URL, JWT_SECRET, HOST, PORT, EXTRA_PORT (skein reads them). Design sketch: pseudocode.goose.
import tcprouter from "github.com/bhare1985/tcprouter@caece7283f413c53500dc3a474c229503cb0210f";
import gslib from "github.com/SorinS/gslib@714f87b387f2f9de6a4c7a77d9abc9db2516ee80";
import hash from "github.com/kaitk/goose-hash@v0.0.4";
import pg from "github.com/kaitk/goose-postgres@c89f8b421b0aad75e7dda1a34197ca910ce7137d";
import skein from "../../../skein";
import skein.pgblocking;

struct Post { id: i64, body: u8[..2048], created_at: u8[..32], author: u8[..64], like_count: i64 }
struct One  { post: Post }
struct Health { status: u8[..8], db: u8[..8], uptime_s: i64 }
struct Liked { liked: bool, already_liked: bool, post_id: i64 }

var DB: pg::Db<skein::Blocking> = pg::Db { t: skein::Blocking { .. }, .. };    // one per worker: globals are copied per worker thread
var STARTED_MS: i64 = 0;

let POST = "SELECT p.id, p.body, p.created_at, u.username, (SELECT count(*) FROM likes l WHERE l.post_id = p.id)::int AS like_count FROM posts p JOIN users u ON u.id = p.user_id";

// Characters, not bytes: the continuation bytes of UTF-8 (10xxxxxx) do not count. The limit matches Postgres char_length().
fn code_points(text: const u8[:]) -> i64 {
    var n = 0;
    for b in text { if (b & 0xC0) != 0x80 { n += 1; } }
    n
}

fn read_post(r: pg::Row) -> Post {
    var p = Post { id: pg::int(r, 0), body: "", created_at: "", author: "", like_count: pg::int(r, 4) };
    p.body.append(pg::text(r, 1));
    var at: u8[>..] = [];
    skein::iso_timestamp(pg::text(r, 2), at);
    p.created_at.append(at[..]);
    p.author.append(pg::text(r, 3));
    p
}

fn feed(req: skein::Request&) {
    var rows: Post[>..] = [];
    let failed = pg::run() {
        skein::connect_db(DB);
        pg::query(DB, str(POST, " ORDER BY p.created_at DESC, p.id DESC LIMIT $1"), 20) { r => rows.push(read_post(r)); };
    };
    if failed.len > 0 { req.database_error(failed); return; }
    req.ok_list_in("posts", rows[..]);
}

fn one_post(req: skein::Request&) {
    var found = false;
    var out = One { post: Post { id: 0, body: "", created_at: "", author: "", like_count: 0 } };
    let failed = pg::run() {
        skein::connect_db(DB);
        pg::query(DB, str(POST, " WHERE p.id = $1"), req.int("id")) { r => out.post = read_post(r); found = true; };
    };
    if failed.len > 0 { req.database_error(failed); return; }
    if !found { req.not_found("post not found"); return; }
    req.ok(out);
}

fn health(req: skein::Request&) {
    var up = false;
    let failed = pg::run() {
        skein::connect_db(DB);
        pg::query(DB, "SELECT 1") { r => up = pg::int(r, 0) == 1; };
    };
    if failed.len > 0 || !up { req.reply(503, "{\"status\":\"degraded\",\"db\":\"unreachable\",\"error\":\"database unreachable\"}"); return; }
    req.ok(Health { status: "ok", db: "ok", uptime_s: (tcprouter::c_boottime_ms() - STARTED_MS) / 1000 });
}

fn create(req: skein::Request&) {
    guard req.json_valid() else { req.bad("malformed JSON body"); return; }
    let raw, ok = req.json_string("body");
    guard ok else { req.bad("body is required"); return; }
    let text = raw.trim();
    guard text.len > 0 else { req.bad("body is required"); return; }
    guard code_points(text) <= 500 else { req.bad("body must be at most 500 characters"); return; }
    var out = One { post: Post { id: 0, body: "", created_at: "", author: "", like_count: 0 } };
    let failed = pg::run() {
        skein::connect_db(DB);
        pg::query(DB, "INSERT INTO posts (user_id, body) VALUES ($1, $2) RETURNING id, created_at", req.user.id, text) { r =>
            out.post.id = pg::int(r, 0);
            var at: u8[>..] = [];
            skein::iso_timestamp(pg::text(r, 1), at);
            out.post.created_at.append(at[..]);
        };
    };
    if failed.len > 0 { req.database_error(failed); return; }
    out.post.body.append(text);
    out.post.author.append(req.user.username[..]);
    req.created(out);
}

fn like(req: skein::Request&) {
    let id = req.int("id");
    var affected: i64 = 0;
    let failed = pg::run() {
        skein::connect_db(DB);
        affected = pg::exec(DB, "INSERT INTO likes (user_id, post_id) VALUES ($1, $2) ON CONFLICT (user_id, post_id) DO NOTHING", req.user.id, id);
    };
    if failed.len > 0 {
        if skein::error_class(failed)[..] == "23" { req.not_found("post not found"); return; }      // the foreign key: no such post
        req.database_error(failed);
        return;
    }
    let body = Liked { liked: true, already_liked: affected != 1, post_id: id };
    if affected == 1 { req.created(body); } else { req.ok(body); }
}

fn routes(r: skein::Request&) {
    r.get("/health") { req => health(req) };
    r.get("/feed") { req => feed(req) };
    r.get("/posts/:id") { req => one_post(req) };
    r.auth() { g =>
        g.post("/posts") { req => create(req) };
        g.post("/posts/:id/like") { req => like(req) };
    };
}

fn main() {
    var app = skein::Config { require_jwt: true, require_db: true, jwt_min_secret: 1, jwt_spec_errors: true, jwt_sub_string_only: true, bad_param_message: "invalid post id", .. };
    app.env();
    STARTED_MS = tcprouter::c_boottime_ms();
    app.run();
}
