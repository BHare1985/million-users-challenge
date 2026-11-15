# goose-skein-bhare1985

Design sketch, not a submission yet. `server.goose` is pseudocode for the final server written against `skein`, a Goose
web library in the spirit of Fastify: the app declares routes, SQL and checks, and skein supplies the event loops, HTTP,
gzip, JSON, JWT and the Postgres driver (the pipelined libpq one from `goose-epoll-api-bhare1985`).

The model was the Fastify/Node entry, shortened: about 55 lines against its 230, because the server plumbing (cluster,
pool sizing, two listeners, error handler, schemas) moves into the library.

Open questions for the library: the route and row types (`req.db.all<T>`, `one<T>`), where `req.id` gets its type, and how
`.auth()` and the HTTP 400/401 messages stay byte-identical to `test/golden`.
