// PSEUDOCODE: this does not compile yet. It is the API of `skein`, a Goose web library (think Fastify), as the final
// server would use it. Skein owns everything below the routes: epoll loops per CPU, the SO_REUSEPORT listeners on PORT
// and EXTRA_PORT, keep-alive, HTTP parsing, gzip, JSON in and out, JWT checking, the pipelined Postgres connections and
// their prepared statements, and the HTTP 500/503 paths. This file only says what the API is.
// Config (DATABASE_URL, JWT_SECRET, HOST, PORT, EXTRA_PORT) is read by skein::App::from_env(), which exits if a required one is missing.
import skein;

// What a row of each query looks like. Skein maps columns to fields by name and writes the struct as JSON in field order.
struct Post   { id: i64, body: string, created_at: time, author: string, like_count: i32 }
struct Health { status: string, db: string, uptime_s: i64 }

let POST = "SELECT p.id, p.body, p.created_at, u.username AS author,
                   (SELECT count(*) FROM likes l WHERE l.post_id = p.id)::int AS like_count
              FROM posts p JOIN users u ON u.id = p.user_id";

fn main() {
    let app = skein::App::from_env();                 // gzip on, errors as {"error": "..."}, unknown paths 404 "not found"

    // Plain GETs: a typed query result is the response.
    app.get("/feed", |req| {
        ok({ posts: req.db.all<Post>(POST + " ORDER BY p.created_at DESC, p.id DESC LIMIT $1", 20) })
    });

    app.get("/posts/:id", |req| {                     // :id is an i64 > 0, or skein answers 400 "invalid post id"
        let post = req.db.one<Post>(POST + " WHERE p.id = $1", req.id)
            or return not_found("post not found");
        ok({ post })
    });

    app.get("/health", |req| {
        if !req.db.ping() { return unavailable({ status: "degraded", db: "unreachable" }); }
        ok(Health { "ok", "ok", app.uptime_s() })
    });

    // Routes behind .auth() get req.user { id, username } from the HS256 token; bad or expired token is 401 before the handler runs.
    app.post("/posts", |req| {
        let text = req.json.body.string()?.trim()     // missing or not a string: 400 "body is required"
            or return bad("body is required");
        if text.chars() > 500 { return bad("body must be at most 500 characters"); }

        let row = req.db.one<{ id: i64, created_at: time }>(
            "INSERT INTO posts (user_id, body) VALUES ($1, $2) RETURNING id, created_at", req.user.id, text);
        created({ post: Post { row.id, text, row.created_at, req.user.username, 0 } })
    }).auth();

    app.post("/posts/:id/like", |req| {
        let res = req.db.exec(
            "INSERT INTO likes (user_id, post_id) VALUES ($1, $2) ON CONFLICT (user_id, post_id) DO NOTHING",
            req.user.id, req.id);
        if res.error == FOREIGN_KEY { return not_found("post not found"); }
        // one new row: 201; already there: 200
        reply(if res.affected == 1 { 201 } else { 200 }, { liked: true, already_liked: res.affected != 1, post_id: req.id })
    }).auth();

    app.run();                                        // blocks; skein picks the threads and the connection counts
}
