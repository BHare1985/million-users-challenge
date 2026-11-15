// Does goose-postgres's sans-I/O core pipeline? Sends one Parse and N Bind/Execute/Sync entries in a single write, then reads the answers
// and checks that they come back one per entry and in the order queued. Compares the time with N separate round trips (pg::query).
// Usage: DATABASE_URL=postgres://... ./pg_pipeline [N]     (built with goose build from this folder: see the imports)
import tcprouter from "github.com/bhare1985/tcprouter@caece7283f413c53500dc3a474c229503cb0210f";
import gslib from "github.com/SorinS/gslib@714f87b387f2f9de6a4c7a77d9abc9db2516ee80";
import hash from "github.com/kaitk/goose-hash@v0.0.4";
import pg from "github.com/kaitk/goose-postgres@c89f8b421b0aad75e7dda1a34197ca910ce7137d";
import skein from "../../../../skein";
import skein.pgblocking;

var DB: pg::Db<skein::Blocking> = pg::Db { t: skein::Blocking { .. }, .. };

let N = 500;

fn routes(r: skein::Request&) { }      // skein needs the function to exist; this program never serves a request

fn main() {
    var cfg = skein::Config { require_db: true, .. };
    cfg.env();
    skein::DATABASE_URL.append(cfg.database_url[..]);

    var piped_ms: i64 = 0;
    var serial_ms: i64 = 0;
    var rows = 0;
    var readies = 0;
    var out_of_order = 0;
    let failed = pg::run() {
        skein::connect_db(DB);

        // N entries, one Sync each, one write.
        var out: u8[>..] = [];
        pg::parse(DB.c, out, "double", "SELECT $1::int8 * 2");
        for i in N {
            let b = pg::bind(out, "double");
            pg::param(out, b, i as i64);
            pg::execute(DB.c, out, b);
            pg::sync(DB.c, out);
        }
        let t0 = tcprouter::c_boottime_ms();
        guard pg::send(DB.t, out[..]) else { pg::fail("08006", "send failed"); }
        loop {
            let e, body = pg::next(DB);
            if e == pg::ROW {
                if pg::int(pg::Row { c: DB.c, body: body }, 0) != rows * 2 { out_of_order += 1; }
                rows += 1;
            } else if e == pg::READY {
                readies += 1;
                if readies == N { break; }
            } else if e == pg::FAILED { pg::fail("08006", "connection failed"); }
        }
        piped_ms = tcprouter::c_boottime_ms() - t0;

        // The same N queries, one round trip each.
        let t1 = tcprouter::c_boottime_ms();
        for i in N { pg::query(DB, "SELECT $1::int8 * 2", i as i64) { r => let v = pg::int(r, 0); }; }
        serial_ms = tcprouter::c_boottime_ms() - t1;
    };
    for e in failed { print("error ", e.code[..], ": ", e.message, "\n"); }
    print("entries ", N, ": rows ", rows, ", syncs answered ", readies, ", out of order ", out_of_order, "\n");
    print("pipelined (1 write): ", piped_ms, " ms;  one round trip each: ", serial_ms, " ms\n");
}
