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
  subscription, UNLESS that chat was created via `POST /chats/direct`:
  that handler now publishes to `user.{id}.new_chat`, which every
  connection also subscribes to at `Hello`, so it can add the new chat's
  subscription live (see `subscribe_to_chat`/`subscribe_to_memberships` in
  `crates/gateway/src/ws.rs`). `get_or_create_saved_messages` doesn't send
  this signal yet, and there's still no real `Subscribe`/`Unsubscribe` op
  for group chats with dynamic membership.
- **No media crate yet** — presigned MinIO uploads are next.

## Running locally

### Rust toolchain

Needs `cargo`. If missing (`cargo --version` fails):

```powershell
Invoke-WebRequest -Uri https://win.rustup.rs -OutFile rustup-init.exe
.\rustup-init.exe -y
```

Open a new shell afterward so `cargo` lands on PATH.

### Infra (DB, NATS, MinIO)

This machine runs infra as native binaries rather than Docker (no Docker
Desktop installed here). `docker-compose.yml` is kept as the portable
alternative for anyone who does have Docker — `docker compose up -d` gives
you Postgres/Dragonfly/MinIO/NATS on their default ports instead.

Native setup used for development on this machine:

```sh
# Postgres 17 (bundled with Laragon), dedicated data dir + port so it never
# touches any other project's database:
PGBIN="C:/laragon/bin/postgresql/postgresql/bin"
"$PGBIN/initdb.exe" -D .data/postgres -U chat -A trust -E UTF8   # once
"$PGBIN/pg_ctl.exe" -D .data/postgres -l .data/postgres.log -o "-p 5433" start
"$PGBIN/createdb.exe" -p 5433 -U chat chat                        # once
```

NATS and MinIO aren't bundled with Laragon. If `scoop` isn't installed yet:

```powershell
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser
irm get.scoop.sh | iex
scoop install nats-server minio
```

Then:

```sh
nats-server -p 4222
minio server .data/minio --console-address ":9001"   # not wired in yet
```

### Config

```sh
cp .env.example .env
# edit .env — at least DATABASE_URL if your DB differs from the default
```

Loaded automatically via `dotenvy` at gateway startup (`crates/gateway/src/main.rs`).
Env vars set in the shell override `.env`. All vars are optional — unset ones
fall back to the defaults in `crates/gateway/src/config.rs`.

| Var | Default | Notes |
| --- | --- | --- |
| `GATEWAY_ADDR` | `127.0.0.1:8080` | HTTP + WebSocket bind address |
| `DATABASE_URL` | `postgres://chat@127.0.0.1:5433/chat` | Postgres connection string |
| `NATS_URL` | `127.0.0.1:4222` | host:port, no scheme |
| `JWT_SECRET` | insecure dev default (warns if unset) | HS256 signing secret |
| `MEDIA_DIR` | `.data/media` | local upload storage dir |

### Run

```sh
cargo run -p gateway
# GET  http://127.0.0.1:8080/health  -> "ok"
# WS   ws://127.0.0.1:8080/ws
```

### Migrate

No manual step — `main.rs` runs `sqlx::migrate!("../../migrations")` against
`DATABASE_URL` on every `cargo run -p gateway` start, applying whatever in
`migrations/*.sql` hasn't run yet.

To apply migrations without starting the gateway (e.g. CI), install
`sqlx-cli` and run it against the same `DATABASE_URL`:

```sh
cargo install sqlx-cli --no-default-features --features postgres,rustls
sqlx migrate run --source migrations
```

### Seed data

No seed script/SQL — there's no `crates/*/seed` or fixture file. Seed through
the HTTP API once the gateway is running:

```sh
# two users
curl -s -X POST http://127.0.0.1:8080/auth/register \
  -H 'content-type: application/json' \
  -d '{"username":"alice","password":"hunter2hunter2"}'
# -> {"user_id":"...","token":"..."}

curl -s -X POST http://127.0.0.1:8080/auth/register \
  -H 'content-type: application/json' \
  -d '{"username":"bob","password":"hunter2hunter2"}'

# direct chat between them (as alice, bearer = alice's token)
curl -s -X POST http://127.0.0.1:8080/chats/direct \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer <alice_token>' \
  -d '{"with_user_id":"<bob_user_id>"}'
# -> {"chat_id":"..."}
```

Send a message over the `/ws` protobuf protocol from there (`Hello{token}` ->
`SendMessage{chat_id, text, client_nonce}`) — there's no HTTP endpoint for
sending, only reading (`GET /chats/{id}/messages`).

`GATEWAY_ADDR` overrides the bind address (default `127.0.0.1:8080`).

## Troubleshooting

### `CRYPT_E_NO_REVOCATION_CHECK` / `UNABLE_TO_VERIFY_LEAF_SIGNATURE` on `cargo`, `bun`, `curl`, etc.

This network sits behind a corp TLS-intercept proxy — every HTTPS connection
is re-signed with a local root CA (seen here as `Organization Certificate
Authority 3`), which has no real OCSP/CRL endpoint, so Windows schannel's
revocation check fails.

Fix for `cargo` (per-session):

```powershell
$env:CARGO_HTTP_CHECK_REVOKE = "false"
```

Fix for `cargo` (permanent), add to `%USERPROFILE%\.cargo\config.toml`:

```toml
[http]
check-revoke = false
```

Fix for `bun install` (same root cause, different symptom — leaf signature,
not revocation): export the corp root CA and pass it explicitly.

```powershell
$cert = Get-ChildItem Cert:\LocalMachine\Root | Where-Object { $_.Subject -eq "CN=Organization Certificate Authority 3, C=EN" }
Export-Certificate -Cert $cert -FilePath "$env:TEMP\corp-proxy-ca.cer" | Out-Null
certutil -encode "$env:TEMP\corp-proxy-ca.cer" "$env:TEMP\corp-proxy-ca.pem" | Out-Null
```

```sh
bun install --cafile "$TEMP/corp-proxy-ca.pem"
```

Other tools hitting the same wall: same idea — either point the tool at the
exported corp root CA, or (schannel-based tools only) skip revocation
checking. Never disable full certificate verification, only revocation.

### `scoop`/`minio`/`nats-server` "not recognized" right after install

PATH is updated for new shells only. Close the terminal, open a new one.

### `minio : ... not recognized` after a `scoop install` that said the dir "is not empty"

Means it's already installed from an earlier run — check
`%USERPROFILE%\scoop\shims` for the `.exe`/`.shim` files before reinstalling.

### `cargo run`/`cargo build` fails compiling build scripts (`proc-macro2`, `quote`, `serde`, `getrandom`, ...)

`error: could not compile ... (build script) due to 1 previous error` on
several unrelated pure-Rust crates at once, right after a fresh `rustup`
install, usually means the MSVC linker (`link.exe`) isn't present — the
default `stable-x86_64-pc-windows-msvc` toolchain needs it even to link a
build script.

Fix — install Visual Studio Build Tools, "Desktop development with C++"
workload, then open a new terminal and retry:

```
https://visualstudio.microsoft.com/visual-cpp-build-tools/
```

Alternative, no VS install needed — switch to the GNU toolchain:

```powershell
rustup toolchain install stable-x86_64-pc-windows-gnu
rustup default stable-x86_64-pc-windows-gnu
```

### `failed to compile chat.proto: ... Could not find 'protoc'`

`chat-proto`'s build script needs the Protobuf compiler. Try scoop first:

```powershell
scoop bucket add main
scoop update
scoop install protobuf
```

If `scoop install protobuf` still says "Couldn't find manifest" (stale/
corrupt bucket, or try `scoop bucket rm main` then `scoop bucket add main`
and retry), skip scoop and install `protoc` directly:

```powershell
Invoke-WebRequest -Uri "https://github.com/protocolbuffers/protobuf/releases/download/v29.1/protoc-29.1-win64.zip" -OutFile "$env:TEMP\protoc.zip"
Expand-Archive -Path "$env:TEMP\protoc.zip" -DestinationPath "$env:USERPROFILE\protoc" -Force
$env:PROTOC = "$env:USERPROFILE\protoc\bin\protoc.exe"
```

Set in the same window before `cargo run -p gateway`. To persist across
windows, add `PROTOC` = `%USERPROFILE%\protoc\bin\protoc.exe` as a permanent
user environment variable (System Properties -> Environment Variables).

## Next steps (Phase 2)

1. Fix the Phase 1 simplifications above as traffic/features demand it,
   roughly in the order listed.
2. `crates/media` — presigned uploads to MinIO/R2, async transcoding.
3. Dragonfly for membership/presence caching and session registry, so a
   second gateway node is just "point it at the same Postgres/NATS/Dragonfly".
4. Multiple gateway nodes behind a load balancer — the NATS fan-out design
   already makes this a non-event; nothing in `ws.rs` assumes single-node.
