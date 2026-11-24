# API reference

Base URL (local dev): `http://127.0.0.1:8080`. A Postman collection covering
every HTTP endpoint below lives at `postman/media-chat.postman_collection.json`
— import it, run **Auth > Register** or **Login** first (it captures
`{{token}}` and `{{user_id}}` into collection variables automatically), then
everything else just works.

Postman can't speak the realtime protocol (binary protobuf over WebSocket),
so that part is documented at the bottom of this file instead.

**Every ID in this API is a JSON string**, not a number — `user_id`,
`chat_id`, `author_id`, `with_user_id`, `media_id`. They're 64-bit Snowflake IDs, well
past `Number.MAX_SAFE_INTEGER`; as JSON numbers, `JSON.parse` in any JS
client would silently round them. `created_at_ms` is a plain number (a
millisecond timestamp fits safely for millennia).

## Auth

All protected endpoints take `Authorization: Bearer <token>`. Tokens are
JWTs (HS256), valid for 7 days, carrying `user_id` and `username`.

### `POST /auth/register`

```json
// request
{ "username": "alice", "password": "correcthorse" }

// 201 response
{ "user_id": "383339900428095488", "token": "eyJ..." }
```

| Status | Meaning |
|---|---|
| 201 | created |
| 409 | username already taken |
| 400 | password shorter than 8 characters |

### `POST /auth/login`

Same request/response shape as register. `401` on wrong username or password.

### `GET /me`

```json
// 200 response
{ "user_id": "383339900428095488", "username": "alice" }
```

## Users

### `GET /users/lookup?username=bob`

Look up a user by exact username — this is how the UI turns "start a chat
with bob" into a `with_user_id` for the next endpoint.

| Status | Meaning |
|---|---|
| 200 | `{ "user_id": ..., "username": "bob" }` |
| 404 | no user with that username |

## Chats

### `GET /chats`

Every direct chat the caller is in, newest activity first — what a sidebar
renders directly. Always includes Saved Messages (see `POST /chats/saved`
below), auto-created the first time this endpoint — or anything else that
touches chats — is called for a given user.

```json
[
  {
    "chat_id": "383339928404107264",
    "other_user_id": "383339902315532288",
    "other_username": "bob",
    "last_message": { "text": "hi bob, from alice", "created_at_ms": 1795462580793 },
    "is_self": false
  }
]
```

`last_message` is `null` for a chat with no messages yet. `is_self: true`
marks Saved Messages — `other_user_id`/`other_username` are then the
caller's own identity (there's no second participant to report), and a
client should render it distinctly rather than as if it were a chat with
another person.

### `POST /chats/direct`

```json
// request
{ "with_user_id": "383339902315532288" }

// 200 response
{ "chat_id": "383339928404107264" }
```

Idempotent — calling it again for the same pair of users returns the same
`chat_id` instead of creating a duplicate chat. `with_user_id` equal to the
caller's own id redirects to `POST /chats/saved` instead of erroring — a
"direct chat with yourself" is exactly what Saved Messages already is.

### `POST /chats/saved`

```json
// 200 response
{ "chat_id": "383339928404107264" }
```

No request body. Idempotent, like `/chats/direct` — returns the same
`chat_id` every time for this caller, creating their "Saved Messages" chat
the first time it's asked for. It has no second member, shows up in
`GET /chats` with `is_self: true` (see below), and otherwise behaves like
any other chat — send/edit/delete/clear all work on it unchanged. Since it
fans out over the same per-chat WS subject as everything else, writing a
note from one logged-in session appears in every other session of the same
user too.

### `GET /chats/:chat_id/messages?limit=50`

Recent messages, newest first. `limit` defaults to 50, clamped to 1–200.

```json
[
  {
    "id": "383340011241611264",
    "chat_id": "383339928404107264",
    "author_id": "383339900428095488",
    "text": "hi bob, from alice",
    "created_at_ms": 1795462580793,
    "media": null,
    "reply_to_id": null,
    "reply_preview": null,
    "forwarded_from": null,
    "edited_at_ms": null,
    "deleted": false
  }
]
```

`media` is present for a message that was sent with an attachment (see
**Media** below) and `null` otherwise. `reply_to_id`/`reply_preview` and
`forwarded_from` are covered under **Replies and forwarding** below,
`edited_at_ms`/`deleted` under **Editing and deleting messages**. Only
messages the caller hasn't cleared (see **Clearing and leaving chats**) are
returned. `403` if the caller isn't a member of that chat.

## Media

Photo, file, and round-video (Telegram-style circular video message)
attachments. There's no S3/MinIO — the gateway stores the bytes on local
disk and gates access with a short-lived HMAC-signed URL instead, minted
fresh every time a message is broadcast or history is re-fetched. The DB
never stores a URL, only `media_id`; a stale/expired link just means asking
for the message again.

Sending an attachment is a two-step dance: upload the bytes over HTTP to get
a `media_id`, then send a `SendMessage` over the WebSocket referencing it
(see **Realtime protocol** below) — uploading alone doesn't create a
message or notify anyone.

### `POST /chats/:chat_id/media?kind=image&name=photo.png`

Body is the raw file bytes (not multipart, not JSON) with `Content-Type`
set to the real mime type. Query params:

| Param | Required | Meaning |
|---|---|---|
| `kind` | yes | `image`, `file`, or `round_video` |
| `name` | only for `kind=file` | original filename; checked against a dangerous-extension blocklist (`.exe`, `.js`, `.html`, `.svg`, …) |

```json
// 200 response
{
  "media_id": "383387716202541056",
  "kind": "image",
  "mime": "image/png",
  "name": null,
  "size": 68
}
```

| Status | Meaning |
|---|---|
| 200 | stored |
| 400 | invalid `kind`, or `name` has a dangerous extension |
| 403 | caller isn't a member of `chat_id` |
| 413 | body exceeds the 25 MB cap |
| 415 | `Content-Type` isn't supported for the given `kind` (images: jpeg/png/webp/gif; round_video: webm/mp4) |

`kind: "file"` is deliberately permissive about content otherwise ("like
anything") — the real backstop is at serve time below.

### `GET /media/:chat_id/:media_id?exp=...&sig=...`

Fetches the actual bytes. `exp`/`sig` come from a `Media.url` you already
received (in a `MessageCreated` or a messages-history response) — this
endpoint is never called with hand-built query params.

| Status | Meaning |
|---|---|
| 200 | the file, with `Content-Type`/`Content-Disposition` set appropriately |
| 403 | signature invalid or `exp` has passed (signed URLs live 1 hour) |
| 404 | no message in that chat references this `media_id` |

A `kind: "file"` attachment is always served as
`Content-Type: application/octet-stream` with
`Content-Disposition: attachment`, regardless of what mime type was
declared at upload — the server never lets a browser render/execute
arbitrary uploaded content inline, even if the client lied about the
content type. Images and round videos are served `inline` with their real
mime type, since both are allow-listed to types that are safe to render
directly. Every response also carries `X-Content-Type-Options: nosniff`.

### `POST /chats/:dest_chat_id/media/forward`

Forwarding a photo/file/round-video to another chat never round-trips the
bytes through the client — this copies the file server-side and mints it a
fresh `media_id` scoped to `dest_chat_id`. The source media must already be
attached to a real message (this looks it up the same way `GET
/media/...` does); you can't forward a bare upload that was never sent.

```json
// request
{ "source_chat_id": "383339928404107264", "media_id": "383387716202541056" }

// 200 response
{
  "media_id": "383394690587701248",
  "kind": "image",
  "mime": "image/png",
  "name": null,
  "size": 68
}
```

As with upload, the response's `media_id` is then referenced in a
`SendMessage` sent to `dest_chat_id` (see **Replies and forwarding** below)
— forwarding is a two-step dance too.

| Status | Meaning |
|---|---|
| 200 | copied |
| 400 | `source_chat_id`/`media_id` not numeric |
| 403 | caller isn't a member of `source_chat_id` **or** `dest_chat_id` (both are checked) |
| 404 | no message in `source_chat_id` references this `media_id` |

## Replies and forwarding

Both ride on `SendMessage`/`MessageCreated` (see **Realtime protocol**
below) — there's no dedicated HTTP endpoint for either, except forwarding a
media attachment needing the copy call just above.

- **Reply**: set `reply_to_id` on `SendMessage` to the message you're
  replying to (same chat only). The server resolves it into a
  `reply_preview` — `{ message_id, author_id, text, media_kind }`, the
  original message's author/snippet/attachment-kind — via one primary-key
  lookup, and stamps that onto the outgoing `MessageCreated` and into every
  later `GET /chats/:chat_id/messages` response for that message. A
  `reply_to_id` pointing at nothing (or at a message in a different chat)
  just comes back with no `reply_preview` — it's recorded but not resolved,
  not an error.
- **Forward**: set `forwarded_from` on `SendMessage` to `{ user_id,
  username }` — the *original* sender, not whoever is doing the forwarding
  (that's still `author_id` on the resulting message, same as any other
  message they send). Forwarding a message that's already a forward should
  reuse its existing `forwarded_from` rather than pointing at the
  forwarder, so attribution never chains through intermediaries. For a
  media message, forward the bytes first via `POST
  /chats/:dest_chat_id/media/forward` and reference the *new* `media_id` —
  the original's `media_id` belongs to the source chat and won't resolve
  in the destination.

## Editing and deleting messages

Both ride on the WebSocket (`EditMessage`/`MessageEdited`,
`DeleteMessage`/`MessageDeleted` — see **Realtime protocol** below), author-
only, no HTTP endpoint. A delete is "for everyone" and soft: the row stays
(so a reply quote pointing at it still resolves, just to a tombstone — see
`reply_preview.deleted`/`text: ""`), but its text and media are scrubbed,
and the media's underlying file is best-effort removed from disk too. There
is no edit history — the new text simply replaces the old.

## Clearing and leaving chats

Both are entirely one-sided — they only ever change what *the caller* sees,
never the other participant's copy of the chat or its messages.

### `POST /chats/:chat_id/clear`

Hides every message currently in the chat from the caller's own future
`GET /chats/:chat_id/messages` calls. The chat itself stays in
`GET /chats`; its preview just goes blank until a new message arrives.

| Status | Meaning |
|---|---|
| 204 | cleared |
| 403 | caller isn't a member of `chat_id` |

### `DELETE /chats/:chat_id`

"Deletes" the chat for the caller only: they stop being an active member,
so it drops out of their `GET /chats` and they stop receiving its WS
fan-out — same as if they'd never joined. Messaging the same person again
later (`POST /chats/direct`) starts a brand new chat; the old one, with all
of the other participant's messages, is untouched on their side.

| Status | Meaning |
|---|---|
| 204 | left |
| 403 | caller isn't a member of `chat_id` |

## Blocking and spam

### `POST /users/:user_id/block` / `POST /users/:user_id/unblock`

Blocking is checked in both directions: once either side has blocked the
other, `POST /chats/direct` between them and `SendMessage` into any
existing chat between them both return/error `403`. No request body.

| Status | Meaning |
|---|---|
| 204 | done |
| 400 (`block` only) | tried to block yourself |

### `GET /blocks`

Everyone the caller has blocked.

```json
[{ "user_id": "383339902315532288", "username": "carol" }]
```

### `POST /users/:user_id/report-spam`

```json
// request — chat_id is optional
{ "chat_id": "383339928404107264" }
```

Records the report and **always** blocks `user_id` too — there's no
"report without blocking." When `chat_id` is given, it's also left for the
caller (see `DELETE /chats/:chat_id` above) — the one-tap "block, report,
and get rid of this conversation" action for an unwanted contact.

| Status | Meaning |
|---|---|
| 204 | reported (and blocked, and left the chat if `chat_id` was given) |

## Realtime protocol (not HTTP, not Postman-testable)

`ws://127.0.0.1:8080/ws`. Every frame is a binary WebSocket message
containing one `Envelope` protobuf message (schema: `proto/chat.proto`).
There is no JSON mode — Postman's WebSocket tab can open the connection but
can't construct or decode these frames; use the Next.js app's
`lib/ws/client.ts`, or the test scripts under this repo's development
history, to actually exercise it.

1. Client opens the socket, immediately sends `Hello { token, device_id }`
   using the JWT from `/auth/login` or `/auth/register`.
2. Server replies `HelloAck { session_id, server_time_ms }`, or closes the
   connection with no reply if the token is missing/invalid/expired.
3. Server subscribes that connection to every chat the user is a member of
   (as of connect time — joining a new chat needs a reconnect, see
   `README.md`'s known Phase 1 limitations).
4. Client sends `Heartbeat { client_time_ms }` periodically (the bundled
   client does this every 15s); server replies `HeartbeatAck`.
5. Client sends `SendMessage { chat_id, text, client_nonce, media,
   reply_to_id, forwarded_from }`. `media`, `reply_to_id`, and
   `forwarded_from` are all optional (omit for a plain, non-reply,
   non-forward message); see **Replies and forwarding** above for what
   they mean and how a media forward's `media_id` is obtained first. The
   server:
   - checks chat membership — not a member gets `Error { code: 403 }` back
     directly on this connection, nothing is persisted;
   - persists the message, including the media/reply/forward references
     (Postgres);
   - publishes it, and **every** member's connection — including the
     sender's — receives `MessageCreated { id, chat_id, author_id, text,
     created_at_ms, client_nonce, media, reply_to_id, reply_preview,
     forwarded_from }` through the same path. `media` (when present) now
     has `url` filled in — a signed link good for 1 hour, minted fresh for
     this broadcast; re-fetching history later gets a different, equally
     fresh one. `reply_preview` (when `reply_to_id` resolved to a real
     message) is likewise filled in server-side. There is no separate
     "ack to sender" message; match `client_nonce` against your
     optimistic UI entry to reconcile it.
6. Client sends `EditMessage { chat_id, message_id, text }` to change a
   message's text — author-only. Not the author, not found, already
   deleted, or wrong chat all collapse into one `Error { code: 404 }`
   (distinguishing them would leak which messages exist in chats the
   caller can't see). On success, every member's connection receives
   `MessageEdited { id, chat_id, text, edited_at_ms }`.
7. Client sends `DeleteMessage { chat_id, message_id }` — author-only,
   deletes for everyone (see **Editing and deleting messages** above).
   Same `404` collapsing as edit. On success, every member's connection
   receives `MessageDeleted { id, chat_id, deleted_at_ms }`.
8. Client sends `Typing { chat_id }` while the user is composing (the
   bundled frontend throttles this to at most once every 1.5s per chat).
   The server checks membership (same 403 as above), then relays
   `Typing { chat_id, user_id }` to every member's connection through the
   same fan-out path as messages — including back to the sender, who is
   expected to ignore a `Typing` whose `user_id` is its own. Never
   persisted, and there's no explicit "stopped typing" event: the frontend
   just expires its indicator ~4s after the last `Typing` it received for
   that chat.
9. Calling (1:1 audio/video) is pure WebRTC signaling over this same
   connection — the server only checks membership and relays, it never
   looks inside SDP or ICE payloads, and none of this is persisted.
   `call_id` is a client-generated UUID identifying one call attempt;
   every message carries `chat_id` so the server stays stateless about
   calls, and `user_id` is server-stamped from the sender's session (same
   as `Typing`).
   - `CallInvite { chat_id, call_id, video }` — rings the other
     participant(s). Relayed as-is.
   - `CallAnswer { chat_id, call_id, accepted }` — the callee's response.
     `accepted: false` means "declined" (before negotiation) or "hung up"
     (after). Relayed as-is.
   - `CallSignal { chat_id, call_id, kind, data }` — the actual WebRTC
     negotiation, relayed verbatim between the two participants once a
     call is accepted. `kind` is `"offer"`, `"answer"`, or `"ice"`; `data`
     is that payload JSON-encoded (an SDP string, or an
     `RTCIceCandidateInit`). The caller creates the offer once `CallAnswer
     { accepted: true }` arrives; the callee answers once it receives
     that offer.
   - `CallEnd { chat_id, call_id }` — either side can send this at any
     point to hang up.
   - No TURN server is configured (only Google's public STUN) — calls
     between peers on the same network or behind simple NATs work; calls
     across stricter/symmetric NATs can fail to connect. That's a Phase 2
     infra addition, not something to fake here.
