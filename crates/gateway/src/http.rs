use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Json, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use chat_auth::AuthError;
use chat_proto::chat::v1::envelope::Payload;
use chat_proto::chat::v1::{Envelope, MessageCreated, Op};
use serde::{Deserialize, Serialize};

use crate::media;
use crate::state::AppState;
use crate::ws::publish_to_chat;

// Snowflake IDs are 64-bit and routinely exceed Number.MAX_SAFE_INTEGER
// (2^53). Returned as JSON numbers they'd get silently rounded by every JS
// client's JSON.parse, so every ID crossing this HTTP boundary — in either
// direction — is a string. The WS/protobuf side doesn't have this problem
// (binary varints, decoded as bigint), only this JSON surface does.

#[derive(Deserialize)]
pub struct Credentials {
    pub username: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct AuthResponse {
    pub user_id: String,
    pub token: String,
}

pub async fn register(State(state): State<Arc<AppState>>, Json(body): Json<Credentials>) -> Response {
    match state.auth.register(&body.username, &body.password).await {
        Ok((user_id, token)) => (
            StatusCode::CREATED,
            Json(AuthResponse {
                user_id: user_id.to_string(),
                token,
            }),
        )
            .into_response(),
        Err(err) => auth_error_response(err),
    }
}

pub async fn login(State(state): State<Arc<AppState>>, Json(body): Json<Credentials>) -> Response {
    match state.auth.login(&body.username, &body.password).await {
        Ok((user_id, token)) => Json(AuthResponse {
            user_id: user_id.to_string(),
            token,
        })
        .into_response(),
        Err(err) => auth_error_response(err),
    }
}

async fn notify_new_chat(state: &Arc<AppState>, chat_id: i64, member_ids: &[i64]) {
    for member_id in member_ids {
        let subject = format!("user.{member_id}.new_chat");
        if let Err(err) = state.nats.publish(subject.clone(), chat_id.to_string().into()).await {
            tracing::warn!(?err, %subject, "failed to notify member of new chat");
        }
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

fn auth_error_response(err: AuthError) -> Response {
    let status = match err {
        AuthError::UsernameTaken | AuthError::HandleTaken => StatusCode::CONFLICT,
        AuthError::InvalidCredentials | AuthError::InvalidToken => StatusCode::UNAUTHORIZED,
        AuthError::WeakPassword | AuthError::InvalidHandle | AuthError::BioTooLong => StatusCode::BAD_REQUEST,
        AuthError::Database(_) | AuthError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, err.to_string()).into_response()
}

#[derive(Deserialize)]
pub struct CreateDirectChat {
    pub with_user_id: String,
}

#[derive(Serialize)]
pub struct ChatResponse {
    pub chat_id: String,
}

pub async fn create_direct_chat(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CreateDirectChat>,
) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    let Ok(with_user_id) = body.with_user_id.parse::<i64>() else {
        return (StatusCode::BAD_REQUEST, "with_user_id must be a numeric id").into_response();
    };

    match state.chat.is_blocked(user_id, with_user_id).await {
        Ok(true) => return (StatusCode::FORBIDDEN, "blocked").into_response(),
        Ok(false) => {}
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }

    match state.chat.get_or_create_direct_chat(user_id, with_user_id).await {
        Ok(chat_id) => {
            // Tells any already-connected sockets for either member to
            // subscribe to this chat's NATS subject — without this, a
            // connection opened before the chat existed never learns about
            // it (it only snapshots memberships once, at `Hello`), so the
            // other side wouldn't see messages until they reconnect.
            notify_new_chat(&state, chat_id, &[user_id, with_user_id]).await;
            Json(ChatResponse {
                chat_id: chat_id.to_string(),
            })
            .into_response()
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// Idempotent, like `/chats/direct` — always returns the same chat_id for
/// this caller, creating it the first time it's asked for.
pub async fn get_or_create_saved_messages(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    match state.chat.get_or_create_saved_messages(user_id).await {
        Ok(chat_id) => Json(ChatResponse {
            chat_id: chat_id.to_string(),
        })
        .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Deserialize)]
pub struct CreateGroupChat {
    pub title: String,
    pub description: Option<String>,
    pub avatar_media_id: Option<String>,
    pub member_user_ids: Vec<String>,
}

/// "Alice", "Alice and Bob", "Alice, Bob, and Carol" — the small
/// comma-plus-"and" join every system-message sentence below uses to list
/// the people involved.
fn join_names(names: &[String]) -> String {
    match names {
        [] => String::new(),
        [a] => a.clone(),
        [a, b] => format!("{a} and {b}"),
        _ => {
            let (last, rest) = names.split_last().expect("non-empty slice");
            format!("{}, and {last}", rest.join(", "))
        }
    }
}

/// Builds a `MessageCreated{ kind: "system" }` envelope from an already-
/// persisted system message and broadcasts it to the chat's NATS subject —
/// the same delivery path `ws::handle_send_message` uses, just invoked from
/// the HTTP side for group creation, where there's no open WS frame to
/// react to in the first place.
async fn broadcast_system_message(state: &Arc<AppState>, message: chat_service::Message) {
    let envelope = Envelope {
        op: Op::MessageCreated as i32,
        seq: 0,
        payload: Some(Payload::MessageCreated(MessageCreated {
            id: message.id as u64,
            chat_id: message.chat_id as u64,
            author_id: message.author_id as u64,
            text: message.text,
            created_at_ms: message.created_at_ms,
            client_nonce: String::new(),
            media: None,
            reply_to_id: 0,
            reply_preview: None,
            forwarded_from: None,
            kind: message.kind,
        })),
    };
    if let Err(err) = publish_to_chat(state, message.chat_id, &envelope).await {
        tracing::warn!(?err, chat_id = message.chat_id, "failed to broadcast group system message");
    }
}

/// Creates a group chat: the caller becomes its owner, every id in
/// `member_user_ids` joins as a plain member. A "<creator> created the
/// group and added <names>" system message is persisted and broadcast, and
/// every member (including the creator, for consistency with how
/// `create_direct_chat` already notifies both sides) gets a `new_chat`
/// ping so an already-open session picks it up without reconnecting.
pub async fn create_group_chat(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CreateGroupChat>,
) -> Response {
    let claims = match bearer_claims(&headers, &state.auth) {
        Ok(claims) => claims,
        Err(status) => return status.into_response(),
    };
    let creator_id = claims.user_id;

    let title = body.title.trim();
    if title.is_empty() {
        return (StatusCode::BAD_REQUEST, "title is required").into_response();
    }
    let description = body.description.as_deref().map(str::trim).filter(|d| !d.is_empty());

    let avatar_media_id = match body.avatar_media_id.map(|id| id.parse::<i64>()) {
        Some(Ok(id)) => Some(id),
        Some(Err(_)) => return (StatusCode::BAD_REQUEST, "avatar_media_id must be a numeric id").into_response(),
        None => None,
    };

    let mut member_ids = Vec::new();
    for raw in &body.member_user_ids {
        let Ok(id) = raw.parse::<i64>() else {
            return (StatusCode::BAD_REQUEST, "member_user_ids must be numeric ids").into_response();
        };
        if id != creator_id && !member_ids.contains(&id) {
            member_ids.push(id);
        }
    }

    for &member_id in &member_ids {
        match state.chat.is_blocked(creator_id, member_id).await {
            Ok(true) => return (StatusCode::FORBIDDEN, "blocked").into_response(),
            Ok(false) => {}
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }

    let chat_id = match state
        .chat
        .create_group_chat(creator_id, title, description, avatar_media_id, &member_ids)
        .await
    {
        Ok(chat_id) => chat_id,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let member_names = match state.auth.usernames_for_ids(&member_ids).await {
        Ok(users) => users.into_iter().map(|u| u.username).collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };
    let body_text = if member_names.is_empty() {
        format!("{} created the group", claims.username)
    } else {
        format!("{} created the group and added {}", claims.username, join_names(&member_names))
    };
    match state.chat.insert_system_message(chat_id, creator_id, body_text).await {
        Ok(message) => broadcast_system_message(&state, message).await,
        Err(err) => tracing::warn!(?err, chat_id, "failed to persist group-created system message"),
    }

    let mut all_member_ids = member_ids;
    all_member_ids.push(creator_id);
    notify_new_chat(&state, chat_id, &all_member_ids).await;

    Json(ChatResponse {
        chat_id: chat_id.to_string(),
    })
    .into_response()
}

#[derive(Deserialize)]
pub struct CreateChannelChat {
    pub title: String,
    pub description: Option<String>,
    pub avatar_media_id: Option<String>,
    pub handle: Option<String>,
    #[serde(default)]
    pub member_user_ids: Vec<String>,
}

fn chat_error_response(err: chat_service::ChatError) -> Response {
    use chat_service::ChatError;
    match err {
        ChatError::HandleTaken => (StatusCode::CONFLICT, "handle already taken").into_response(),
        ChatError::InvalidHandle => (
            StatusCode::BAD_REQUEST,
            "handle must be 3-32 lowercase letters, digits, or underscores, starting with a letter",
        )
            .into_response(),
        ChatError::NotOwner => (StatusCode::FORBIDDEN, "only the owner can do that").into_response(),
        ChatError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// Creates a channel: the caller becomes its owner, every id in
/// `member_user_ids` joins as a plain subscriber (`member_user_ids` may be
/// empty — unlike a group, a channel can start with zero subscribers). An
/// optional public `@handle` can be set at creation time, same validation
/// as a user's own handle. Otherwise identical to `create_group_chat`: same
/// system-message + `notify_new_chat` sequence.
pub async fn create_channel_chat(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CreateChannelChat>,
) -> Response {
    let claims = match bearer_claims(&headers, &state.auth) {
        Ok(claims) => claims,
        Err(status) => return status.into_response(),
    };
    let creator_id = claims.user_id;

    let title = body.title.trim();
    if title.is_empty() {
        return (StatusCode::BAD_REQUEST, "title is required").into_response();
    }
    let description = body.description.as_deref().map(str::trim).filter(|d| !d.is_empty());
    let handle = body.handle.as_deref().map(|h| h.trim_start_matches('@')).filter(|h| !h.is_empty());

    let avatar_media_id = match body.avatar_media_id.map(|id| id.parse::<i64>()) {
        Some(Ok(id)) => Some(id),
        Some(Err(_)) => return (StatusCode::BAD_REQUEST, "avatar_media_id must be a numeric id").into_response(),
        None => None,
    };

    let mut member_ids = Vec::new();
    for raw in &body.member_user_ids {
        let Ok(id) = raw.parse::<i64>() else {
            return (StatusCode::BAD_REQUEST, "member_user_ids must be numeric ids").into_response();
        };
        if id != creator_id && !member_ids.contains(&id) {
            member_ids.push(id);
        }
    }

    for &member_id in &member_ids {
        match state.chat.is_blocked(creator_id, member_id).await {
            Ok(true) => return (StatusCode::FORBIDDEN, "blocked").into_response(),
            Ok(false) => {}
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }

    let chat_id = match state
        .chat
        .create_channel_chat(creator_id, title, description, avatar_media_id, handle, &member_ids)
        .await
    {
        Ok(chat_id) => chat_id,
        Err(err) => return chat_error_response(err),
    };

    let member_names = match state.auth.usernames_for_ids(&member_ids).await {
        Ok(users) => users.into_iter().map(|u| u.username).collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };
    let body_text = if member_names.is_empty() {
        format!("{} created the channel", claims.username)
    } else {
        format!("{} created the channel and added {}", claims.username, join_names(&member_names))
    };
    match state.chat.insert_system_message(chat_id, creator_id, body_text).await {
        Ok(message) => broadcast_system_message(&state, message).await,
        Err(err) => tracing::warn!(?err, chat_id, "failed to persist channel-created system message"),
    }

    let mut all_member_ids = member_ids;
    all_member_ids.push(creator_id);
    notify_new_chat(&state, chat_id, &all_member_ids).await;

    Json(ChatResponse {
        chat_id: chat_id.to_string(),
    })
    .into_response()
}

#[derive(Deserialize)]
pub struct SetChannelHandleRequest {
    pub handle: Option<String>,
}

pub async fn set_channel_handle(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(chat_id): Path<i64>,
    Json(body): Json<SetChannelHandleRequest>,
) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    let handle = body.handle.as_deref().map(|h| h.trim_start_matches('@'));
    match state.chat.set_channel_handle(chat_id, user_id, handle.filter(|h| !h.is_empty())).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => chat_error_response(err),
    }
}

/// Owner-only. `user_id` in the path is the member being promoted to admin
/// (able to post in the channel, same as the owner) — 404 if they're not
/// currently an active member.
/// Broadcasts "<target> is now a channel admin"/"<target> is no longer a
/// channel admin" after a successful promote/demote — the same system-
/// message path group/channel creation and joining already use. This is
/// also how the *target's own* already-open session learns its role (and
/// therefore whether it can post) just changed, without it having to
/// reconnect or reload — the WS dispatcher refreshes chat summaries on any
/// system message for a chat it already knows about.
async fn broadcast_role_change(state: &Arc<AppState>, chat_id: i64, target_user_id: i64, promoted: bool) {
    let username = match state.auth.usernames_for_ids(&[target_user_id]).await {
        Ok(users) => users.into_iter().next().map(|u| u.username),
        Err(_) => None,
    };
    let Some(username) = username else { return };
    let body_text = if promoted {
        format!("{username} is now a channel admin")
    } else {
        format!("{username} is no longer a channel admin")
    };
    match state.chat.insert_system_message(chat_id, target_user_id, body_text).await {
        Ok(message) => broadcast_system_message(state, message).await,
        Err(err) => tracing::warn!(?err, chat_id, "failed to persist role-change system message"),
    }
}

pub async fn promote_member(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((chat_id, target_user_id)): Path<(i64, i64)>,
) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    match state.chat.promote_member(chat_id, user_id, target_user_id).await {
        Ok(true) => {
            broadcast_role_change(&state, chat_id, target_user_id, true).await;
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => chat_error_response(err),
    }
}

/// Owner-only — the inverse of `promote_member`, back to a plain subscriber.
pub async fn demote_member(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((chat_id, target_user_id)): Path<(i64, i64)>,
) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    match state.chat.demote_member(chat_id, user_id, target_user_id).await {
        Ok(true) => {
            broadcast_role_change(&state, chat_id, target_user_id, false).await;
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => chat_error_response(err),
    }
}

#[derive(Serialize)]
pub struct ChannelPreviewView {
    pub chat_id: String,
    pub title: String,
    pub description: Option<String>,
    pub avatar_url: Option<String>,
    pub member_count: i64,
    pub handle: String,
}

/// Public lookup by `@handle` — any logged-in user, no membership required.
/// Only ever finds a channel that has chosen to set a public handle (see
/// `ChatService::find_channel_by_handle`); a private/invite-only channel
/// simply isn't reachable this way.
pub async fn find_channel_by_handle(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(handle): Path<String>,
) -> Response {
    if bearer_user(&headers, &state.auth).is_err() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let handle = handle.trim_start_matches('@');
    match state.chat.find_channel_by_handle(handle).await {
        Ok(Some(preview)) => Json(ChannelPreviewView {
            chat_id: preview.chat_id.to_string(),
            title: preview.title,
            description: preview.description,
            avatar_url: preview.avatar_media_id.map(|id| state.media.avatar_url(id)),
            member_count: preview.member_count,
            handle: preview.handle,
        })
        .into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// Subscribes the caller to a public channel found via `find_channel_by_handle`
/// — 404 if `chat_id` isn't actually a public (handle-having) channel.
pub async fn join_channel(State(state): State<Arc<AppState>>, headers: HeaderMap, Path(chat_id): Path<i64>) -> Response {
    let claims = match bearer_claims(&headers, &state.auth) {
        Ok(claims) => claims,
        Err(status) => return status.into_response(),
    };
    let user_id = claims.user_id;
    match state.chat.join_channel(chat_id, user_id).await {
        Ok(true) => {
            // So an already-open session (this user's other tabs/devices)
            // picks up the new subscription without reconnecting — same
            // ping `create_group_chat`/`create_channel_chat` send every
            // member for the same reason.
            notify_new_chat(&state, chat_id, &[user_id]).await;
            // A system notice broadcast to the channel's existing
            // subscribers (the same path group/channel creation already
            // uses) — this is also how an already-open owner/admin session
            // learns the subscriber count just changed, since nothing else
            // pings them when someone joins on their own via handle.
            let body_text = format!("{} joined the channel", claims.username);
            match state.chat.insert_system_message(chat_id, user_id, body_text).await {
                Ok(message) => broadcast_system_message(&state, message).await,
                Err(err) => tracing::warn!(?err, chat_id, "failed to persist channel-joined system message"),
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Serialize)]
pub struct AvatarUploadResponse {
    pub avatar_media_id: String,
}

/// Validates (real image decode, not just a declared Content-Type) and
/// optimizes an avatar image — see `media::optimize_avatar` — then stores
/// it under its own media id. Returned `avatar_media_id` is passed into
/// `POST /chats/group`'s `avatar_media_id` field; it isn't attached to a
/// group until that call succeeds, so an avatar uploaded but never used in
/// a successful group creation is simply orphaned on disk (no cleanup job
/// exists yet — same category of simplification as this repo's other
/// Phase 1 gaps).
pub async fn upload_group_avatar(State(state): State<Arc<AppState>>, headers: HeaderMap, body: Bytes) -> Response {
    if bearer_user(&headers, &state.auth).is_err() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if body.len() > media::AVATAR_MAX_UPLOAD_BYTES {
        return (StatusCode::PAYLOAD_TOO_LARGE, "avatar exceeds the 8 MB limit").into_response();
    }

    let optimized = match media::optimize_avatar(&body) {
        Ok(bytes) => bytes,
        Err(_) => return (StatusCode::BAD_REQUEST, "that doesn't look like a valid image").into_response(),
    };

    let media_id = state.media_ids.next_id();
    if let Err(err) = state.media.store_avatar(media_id, &optimized).await {
        tracing::error!(?err, "failed to store group avatar");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    Json(AvatarUploadResponse {
        avatar_media_id: media_id.to_string(),
    })
    .into_response()
}

/// Unauthenticated by design — see `MediaStore::avatar_url`'s doc comment.
pub async fn serve_avatar(State(state): State<Arc<AppState>>, Path(media_id): Path<i64>) -> Response {
    match state.media.load_avatar(media_id).await {
        Ok(bytes) => (
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "image/webp".to_string()),
                (header::X_CONTENT_TYPE_OPTIONS, "nosniff".to_string()),
                (header::CACHE_CONTROL, "public, max-age=86400".to_string()),
            ],
            bytes,
        )
            .into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

#[derive(Serialize)]
pub struct AvatarView {
    pub media_id: String,
    pub avatar_url: String,
    pub is_active: bool,
    pub created_at_ms: i64,
}

/// Uploads a new photo into the caller's personal gallery and makes it
/// the active avatar — same validate/resize/WebP pipeline as
/// `upload_group_avatar`, just filed under the uploader's own gallery
/// (`chat_auth::AuthService::add_avatar`) instead of a group.
pub async fn upload_my_avatar(State(state): State<Arc<AppState>>, headers: HeaderMap, body: Bytes) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    if body.len() > media::AVATAR_MAX_UPLOAD_BYTES {
        return (StatusCode::PAYLOAD_TOO_LARGE, "avatar exceeds the 8 MB limit").into_response();
    }

    let optimized = match media::optimize_avatar(&body) {
        Ok(bytes) => bytes,
        Err(_) => return (StatusCode::BAD_REQUEST, "that doesn't look like a valid image").into_response(),
    };

    let media_id = state.media_ids.next_id();
    if let Err(err) = state.media.store_avatar(media_id, &optimized).await {
        tracing::error!(?err, "failed to store avatar");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    if let Err(err) = state.auth.add_avatar(user_id, media_id).await {
        tracing::error!(?err, "failed to record avatar");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    Json(AvatarView {
        media_id: media_id.to_string(),
        avatar_url: state.media.avatar_url(media_id),
        is_active: true,
        created_at_ms: now_ms(),
    })
    .into_response()
}

pub async fn list_my_avatars(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    match state.auth.list_avatars(user_id).await {
        Ok(avatars) => Json(
            avatars
                .into_iter()
                .map(|a| AvatarView {
                    media_id: a.media_id.to_string(),
                    avatar_url: state.media.avatar_url(a.media_id),
                    is_active: a.is_active,
                    created_at_ms: a.created_at_ms,
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

pub async fn activate_my_avatar(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(media_id): Path<i64>,
) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    match state.auth.set_active_avatar(user_id, media_id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

pub async fn delete_my_avatar(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(media_id): Path<i64>,
) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    if let Err(err) = state.auth.delete_avatar(user_id, media_id).await {
        tracing::error!(?err, "failed to delete avatar");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    state.media.delete_avatar(media_id).await;
    StatusCode::NO_CONTENT.into_response()
}

#[derive(Serialize)]
pub struct GroupMemberView {
    pub user_id: String,
    pub username: String,
    pub role: String,
}

pub async fn list_group_members(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(chat_id): Path<i64>,
) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    match state.chat.is_member(chat_id, user_id).await {
        Ok(true) => {}
        Ok(false) => return StatusCode::FORBIDDEN.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
    match state.chat.group_members(chat_id).await {
        Ok(members) => Json(
            members
                .into_iter()
                .map(|(id, username, role)| GroupMemberView {
                    user_id: id.to_string(),
                    username,
                    role,
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Serialize)]
pub struct MediaView {
    pub media_id: String,
    pub kind: String,
    pub mime: String,
    pub name: Option<String>,
    pub size: i64,
    pub duration_ms: Option<i64>,
    pub url: String,
}

#[derive(Serialize)]
pub struct ReplyPreviewView {
    pub message_id: String,
    pub author_id: String,
    pub text: String,
    pub media_kind: Option<String>,
    pub deleted: bool,
}

#[derive(Serialize)]
pub struct ForwardedFromView {
    pub user_id: String,
    pub username: String,
}

#[derive(Serialize)]
pub struct MessageView {
    pub id: String,
    pub chat_id: String,
    pub author_id: String,
    pub text: String,
    pub created_at_ms: i64,
    pub media: Option<MediaView>,
    pub reply_to_id: Option<String>,
    pub reply_preview: Option<ReplyPreviewView>,
    pub forwarded_from: Option<ForwardedFromView>,
    pub edited_at_ms: Option<i64>,
    pub deleted: bool,
    pub kind: String,
}

#[derive(Deserialize, Default)]
pub struct ListMessagesQuery {
    pub limit: Option<i64>,
    /// Snowflake ID as a string (see the module-level note on IDs) — fetch
    /// the page older than this message, for infinite scroll upward.
    pub before: Option<String>,
}

pub async fn list_messages(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(chat_id): Path<i64>,
    Query(query): Query<ListMessagesQuery>,
) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };

    match state.chat.is_member(chat_id, user_id).await {
        Ok(true) => {}
        Ok(false) => return StatusCode::FORBIDDEN.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }

    let before = match query.before.map(|b| b.parse::<i64>()) {
        Some(Ok(before)) => Some(before),
        Some(Err(_)) => return (StatusCode::BAD_REQUEST, "before must be a numeric id").into_response(),
        None => None,
    };

    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    match state.chat.recent_messages(chat_id, user_id, limit, before).await {
        Ok(messages) => Json(
            messages
                .into_iter()
                .map(|m| MessageView {
                    id: m.id.to_string(),
                    chat_id: m.chat_id.to_string(),
                    author_id: m.author_id.to_string(),
                    text: m.text,
                    created_at_ms: m.created_at_ms,
                    media: m.media.map(|media| MediaView {
                        media_id: media.media_id.to_string(),
                        kind: media.kind,
                        mime: media.mime,
                        name: media.name,
                        size: media.size,
                        duration_ms: media.duration_ms,
                        url: state.media.signed_url(chat_id, media.media_id),
                    }),
                    reply_to_id: m.reply_to_id.map(|id| id.to_string()),
                    reply_preview: m.reply_preview.map(|r| ReplyPreviewView {
                        message_id: r.message_id.to_string(),
                        author_id: r.author_id.to_string(),
                        text: r.text,
                        media_kind: r.media_kind,
                        deleted: r.deleted,
                    }),
                    forwarded_from: m.forwarded_from.map(|f| ForwardedFromView {
                        user_id: f.user_id.to_string(),
                        username: f.username,
                    }),
                    edited_at_ms: m.edited_at_ms,
                    deleted: m.deleted_at_ms.is_some(),
                    kind: m.kind,
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Serialize)]
pub struct MeResponse {
    pub user_id: String,
    pub username: String,
    pub handle: Option<String>,
    pub bio: Option<String>,
    pub avatar_url: Option<String>,
}

pub async fn me(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let claims = match bearer_claims(&headers, &state.auth) {
        Ok(claims) => claims,
        Err(status) => return status.into_response(),
    };
    match state.auth.find_by_id(claims.user_id).await {
        Ok(Some(user)) => Json(MeResponse {
            user_id: user.user_id.to_string(),
            username: user.username,
            handle: user.handle,
            bio: user.bio,
            avatar_url: user.active_avatar_media_id.map(|id| state.media.avatar_url(id)),
        })
        .into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Deserialize)]
pub struct SetHandleRequest {
    pub handle: Option<String>,
}

pub async fn set_handle(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<SetHandleRequest>,
) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    // Normalize the common "@handle" shorthand to the bare handle this is
    // actually stored as — stripping a leading "@" here means callers
    // never have to remember whether to include it.
    let handle = body.handle.as_deref().map(|h| h.trim_start_matches('@'));
    match state.auth.set_handle(user_id, handle.filter(|h| !h.is_empty())).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(AuthError::HandleTaken) => (StatusCode::CONFLICT, "handle already taken").into_response(),
        Err(AuthError::InvalidHandle) => (
            StatusCode::BAD_REQUEST,
            "handle must be 3-32 lowercase letters, digits, or underscores, starting with a letter",
        )
            .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Deserialize)]
pub struct SetBioRequest {
    pub bio: Option<String>,
}

pub async fn set_bio(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<SetBioRequest>,
) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    let bio = body.bio.as_deref().map(str::trim).filter(|b| !b.is_empty());
    match state.auth.set_bio(user_id, bio).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(AuthError::BioTooLong) => (StatusCode::BAD_REQUEST, "bio must be 500 characters or fewer").into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Serialize)]
pub struct ChatSummaryView {
    pub chat_id: String,
    /// `null` for a group/channel chat — there's no single "other" participant.
    pub other_user_id: Option<String>,
    pub other_username: Option<String>,
    pub last_message: Option<LastMessageView>,
    pub is_self: bool,
    pub is_archived: bool,
    pub is_group: bool,
    pub is_channel: bool,
    pub title: Option<String>,
    pub description: Option<String>,
    pub avatar_url: Option<String>,
    pub member_count: i64,
    pub handle: Option<String>,
    pub my_role: Option<String>,
    pub unread_count: i64,
}

#[derive(Serialize)]
pub struct LastMessageView {
    pub text: String,
    pub created_at_ms: i64,
    pub media_kind: Option<String>,
    pub deleted: bool,
}

pub async fn list_chats(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };

    match state.chat.list_chats_for_user(user_id).await {
        Ok(chats) => Json(
            chats
                .into_iter()
                .map(|c| ChatSummaryView {
                    chat_id: c.chat_id.to_string(),
                    other_user_id: c.other_user_id.map(|id| id.to_string()),
                    other_username: c.other_username,
                    last_message: c.last_message.map(|m| LastMessageView {
                        text: m.text,
                        created_at_ms: m.created_at_ms,
                        media_kind: m.media_kind,
                        deleted: m.deleted,
                    }),
                    is_self: c.is_self,
                    is_archived: c.is_archived,
                    is_group: c.is_group,
                    is_channel: c.is_channel,
                    title: c.title,
                    description: c.description,
                    avatar_url: c.avatar_media_id.map(|id| state.media.avatar_url(id)),
                    member_count: c.member_count,
                    handle: c.handle,
                    my_role: c.my_role,
                    unread_count: c.unread_count,
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// Clears the caller's own view of this chat's history — see
/// `ChatService::clear_chat`. The other member's copy is untouched.
pub async fn clear_chat(State(state): State<Arc<AppState>>, headers: HeaderMap, Path(chat_id): Path<i64>) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    match state.chat.is_member(chat_id, user_id).await {
        Ok(true) => {}
        Ok(false) => return StatusCode::FORBIDDEN.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
    match state.chat.clear_chat(chat_id, user_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// Marks this chat as read up to its current latest message — called when
/// the caller opens/focuses it. Affects `unread_count` in the next
/// `GET /chats`, including from another of this caller's own sessions.
pub async fn mark_chat_read(State(state): State<Arc<AppState>>, headers: HeaderMap, Path(chat_id): Path<i64>) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    match state.chat.is_member(chat_id, user_id).await {
        Ok(true) => {}
        Ok(false) => return StatusCode::FORBIDDEN.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
    match state.chat.mark_chat_read(chat_id, user_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

pub async fn archive_chat(State(state): State<Arc<AppState>>, headers: HeaderMap, Path(chat_id): Path<i64>) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    match state.chat.is_member(chat_id, user_id).await {
        Ok(true) => {}
        Ok(false) => return StatusCode::FORBIDDEN.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
    match state.chat.archive_chat(chat_id, user_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

pub async fn unarchive_chat(State(state): State<Arc<AppState>>, headers: HeaderMap, Path(chat_id): Path<i64>) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    match state.chat.is_member(chat_id, user_id).await {
        Ok(true) => {}
        Ok(false) => return StatusCode::FORBIDDEN.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
    match state.chat.unarchive_chat(chat_id, user_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// "Deletes" this chat for the caller only — see `ChatService::leave_chat`.
pub async fn leave_chat(State(state): State<Arc<AppState>>, headers: HeaderMap, Path(chat_id): Path<i64>) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    match state.chat.is_member(chat_id, user_id).await {
        Ok(true) => {}
        Ok(false) => return StatusCode::FORBIDDEN.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
    match state.chat.leave_chat(chat_id, user_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Deserialize)]
pub struct LookupUserQuery {
    pub username: String,
}

#[derive(Serialize)]
pub struct UserView {
    pub user_id: String,
    pub username: String,
    pub handle: Option<String>,
    pub bio: Option<String>,
    pub avatar_url: Option<String>,
}

fn user_view(state: &AppState, user: chat_auth::UserSummary) -> UserView {
    UserView {
        user_id: user.user_id.to_string(),
        username: user.username,
        handle: user.handle,
        bio: user.bio,
        avatar_url: user.active_avatar_media_id.map(|id| state.media.avatar_url(id)),
    }
}

pub async fn lookup_user(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<LookupUserQuery>,
) -> Response {
    if bearer_user(&headers, &state.auth).is_err() {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    match state.auth.find_by_username(&query.username).await {
        Ok(Some(user)) => Json(user_view(&state, user)).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Deserialize)]
pub struct SearchUsersQuery {
    pub q: String,
}

pub async fn search_users(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<SearchUsersQuery>,
) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    // A leading "@" is how a handle is normally shown/typed ("@alice") —
    // strip it before matching so searching "@alice" and "alice" behave
    // identically against both the username and handle columns.
    let q = query.q.trim().trim_start_matches('@');
    if q.is_empty() {
        return Json(Vec::<UserView>::new()).into_response();
    }

    match state.auth.search_users(q, user_id, 10).await {
        Ok(users) => Json(users.into_iter().map(|u| user_view(&state, u)).collect::<Vec<_>>()).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

pub async fn block_user(State(state): State<Arc<AppState>>, headers: HeaderMap, Path(target_id): Path<i64>) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    if target_id == user_id {
        return (StatusCode::BAD_REQUEST, "can't block yourself").into_response();
    }
    match state.chat.block_user(user_id, target_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

pub async fn unblock_user(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(target_id): Path<i64>,
) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    match state.chat.unblock_user(user_id, target_id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Serialize)]
pub struct BlockedUserView {
    pub user_id: String,
    pub username: String,
}

pub async fn list_blocked(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };
    match state.chat.list_blocked(user_id).await {
        Ok(rows) => Json(
            rows.into_iter()
                .map(|(id, username)| BlockedUserView {
                    user_id: id.to_string(),
                    username,
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Deserialize, Default)]
pub struct ReportSpamRequest {
    /// Optional — when given, this chat is also left for the reporter (see
    /// `ChatService::leave_chat`), matching the common "block, report, and
    /// get rid of the conversation" single action for unwanted contacts.
    pub chat_id: Option<String>,
}

/// Records a spam report and always blocks the reported user — reporting
/// someone and continuing to receive their messages isn't a real use case
/// this exposes separately.
pub async fn report_spam(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(target_id): Path<i64>,
    Json(body): Json<ReportSpamRequest>,
) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };

    let chat_id = match body.chat_id.map(|c| c.parse::<i64>()) {
        Some(Ok(chat_id)) => Some(chat_id),
        Some(Err(_)) => return (StatusCode::BAD_REQUEST, "chat_id must be a numeric id").into_response(),
        None => None,
    };

    if state.chat.report_spam(user_id, target_id, chat_id).await.is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    if state.chat.block_user(user_id, target_id).await.is_err() {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    if let Some(chat_id) = chat_id {
        // Best-effort: the report + block above already succeeded, and
        // leaving the chat is a courtesy cleanup, not the core guarantee.
        let _ = state.chat.leave_chat(chat_id, user_id).await;
    }
    StatusCode::NO_CONTENT.into_response()
}

pub fn bearer_user(headers: &HeaderMap, auth: &chat_auth::AuthService) -> Result<i64, StatusCode> {
    bearer_claims(headers, auth).map(|claims| claims.user_id)
}

fn bearer_claims(headers: &HeaderMap, auth: &chat_auth::AuthService) -> Result<chat_auth::Claims, StatusCode> {
    let header = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let token = header.strip_prefix("Bearer ").ok_or(StatusCode::UNAUTHORIZED)?;
    auth.verify_token(token).map_err(|_| StatusCode::UNAUTHORIZED)
}
