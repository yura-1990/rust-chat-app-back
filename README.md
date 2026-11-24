# media — realtime chat backend

Telegram-scale realtime chat backend. See the companion plan for full
architecture reasoning (DB choices, scaling path, security model).

## What's here (Phase 0 + Phase 1)

- `proto/chat.proto` — the wire schema shared with the Next.js frontend
  (client <-> gateway WebSocket protocol, binary protobuf frames). Single
  source of truth; the frontend repo keeps a synced copy under `proto/` and
  codegens TypeScript from it with `ts-proto`.
- `crates/proto` — compiles `chat.proto` into Rust types via `prost-build`.
- `crates/common` — the Snowflake ID generator (time-sortable, no
  cross-process coordination needed) shared by every service.
- `crates/auth` — registration/login, Argon2 password hashing, HS256 JWTs.
  `Hello.token` on the WebSocket resolves to a real, verified user ID.
- `crates/chat` — Postgres-backed persistence: direct chats, membership,
  messages. Indexed `(chat_id, id desc)` — `id` is a time-sortable Snowflake,
  so this is the same partition/clustering shape a future move to ScyllaDB
  would use, without a `created_at` sort key to keep in sync.
- `crates/gateway` — the Axum/Tokio WebSocket + HTTP server. Verifies the
  caller on `Hello`, enforces chat membership on `SendMessage`, persists the
  message, then **publishes to NATS instead of replying directly** — the
  sender gets their own copy back through the same subscription every other
  member uses. Multiple connections (other tabs, other users, eventually
  other gateway nodes) all receive one consistent delivery path.
- `migrations/` — `users`, `chats`, `chat_members`, `messages`. Applied
  automatically on gateway startup via `sqlx::migrate!`.

### HTTP surface (not part of the realtime hot path)

- `POST /auth/register`, `POST /auth/login` — `{username, password}` ->
  `{user_id, token}`.
- `POST /chats/direct` (Bearer token) — `{with_user_id}` -> `{chat_id}`,
  reusing an existing direct chat if one exists.
- `GET /chats/:id/messages?limit=50` (Bearer token) — recent messages,
  rejects non-members with 403.

### WebSocket protocol

`Hello{token, device_id}` -> `HelloAck`. Then `Heartbeat` <-> `HeartbeatAck`,
and `SendMessage{chat_id, text, client_nonce}` -> every member's connection
(including the sender's) gets a `MessageCreated` via the NATS fan-out.
Sending to a chat you're not a member of gets you an `Error` envelope, not a
dropped connection.

## Known Phase 1 simplifications (intentional, flagged for later)

- **JWT is HS256**, not the ES256/JWKS setup the architecture doc
  recommends for a multi-service deployment. Fine for one process holding
  the only signing key; revisit before auth becomes its own service.
- **No outbox/retry if NATS publish fails** after a message is already
  persisted — the sender gets a 502-style error envelope, but there's no
  automatic redelivery. Fine for one gateway node; needs a real outbox
  pattern once delivery has to survive a NATS or gateway restart.
- **No membership cache** — `chats_for_user` and `is_member` hit Postgres on
  every connect/send. Fine at Phase 1 traffic; this is exactly what
  Dragonfly (Phase 2) is for.
- **A connection only subscribes to the chats it had at `Hello` time** —
  joining a new chat mid-connection needs a reconnect to pick up the NATS
  subscription. Needs a `Subscribe`/`Unsubscribe` op once group chats with
  dynamic membership land.
- **No media crate yet** — presigned MinIO uploads are next.

## Running locally

This machine runs infra as native binaries rather than Docker (no Docker
Desktop installed here). `docker-compose.yml` is kept as the portable
alternative for anyone who does have Docker — `docker compose up -d` gives
you Postgres/Dragonfly/MinIO/NATS on their default ports instead.

Native setup used for development on this machine:

```sh
# Postgres 17 (bundled with Laragon), dedicated data dir + port so it never
# touches any other project's database:
PGBIN="C:/laragon/bin/postgresql/postgresql-17.2-3/bin"
"$PGBIN/initdb.exe" -D .data/postgres -U chat -A trust -E UTF8   # once
"$PGBIN/pg_ctl.exe" -D .data/postgres -l .data/postgres.log -o "-p 5433" start
"$PGBIN/createdb.exe" -p 5433 -U chat chat                        # once

# NATS and MinIO, installed via `scoop install nats-server minio`:
nats-server -p 4222
minio server .data/minio --console-address ":9001"   # not wired in yet
```

Then:

```sh
export DATABASE_URL="postgres://chat@127.0.0.1:5433/chat"
export NATS_URL="127.0.0.1:4222"
export JWT_SECRET="change-me"   # omit and it'll warn + use an insecure dev default
cargo run -p gateway
# GET  http://127.0.0.1:8080/health  -> "ok"
# WS   ws://127.0.0.1:8080/ws
```

`GATEWAY_ADDR` overrides the bind address (default `127.0.0.1:8080`).

## Next steps (Phase 2)

1. Fix the Phase 1 simplifications above as traffic/features demand it,
   roughly in the order listed.
2. `crates/media` — presigned uploads to MinIO/R2, async transcoding.
3. Dragonfly for membership/presence caching and session registry, so a
   second gateway node is just "point it at the same Postgres/NATS/Dragonfly".
4. Multiple gateway nodes behind a load balancer — the NATS fan-out design
   already makes this a non-event; nothing in `ws.rs` assumes single-node.
