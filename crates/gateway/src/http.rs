use std::sync::Arc;

use axum::extract::{Json, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use chat_auth::AuthError;
use serde::{Deserialize, Serialize};

use crate::state::AppState;

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

fn auth_error_response(err: AuthError) -> Response {
    let status = match err {
        AuthError::UsernameTaken => StatusCode::CONFLICT,
        AuthError::InvalidCredentials | AuthError::InvalidToken => StatusCode::UNAUTHORIZED,
        AuthError::WeakPassword => StatusCode::BAD_REQUEST,
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
        Ok(chat_id) => Json(ChatResponse {
            chat_id: chat_id.to_string(),
        })
        .into_response(),
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
}

pub async fn me(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    match bearer_claims(&headers, &state.auth) {
        Ok(claims) => Json(MeResponse {
            user_id: claims.user_id.to_string(),
            username: claims.username,
        })
        .into_response(),
        Err(status) => status.into_response(),
    }
}

#[derive(Serialize)]
pub struct ChatSummaryView {
    pub chat_id: String,
    pub other_user_id: String,
    pub other_username: String,
    pub last_message: Option<LastMessageView>,
    pub is_self: bool,
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

    match state.chat.list_direct_chats_for_user(user_id).await {
        Ok(chats) => Json(
            chats
                .into_iter()
                .map(|c| ChatSummaryView {
                    chat_id: c.chat_id.to_string(),
                    other_user_id: c.other_user_id.to_string(),
                    other_username: c.other_username,
                    last_message: c.last_message.map(|m| LastMessageView {
                        text: m.text,
                        created_at_ms: m.created_at_ms,
                        media_kind: m.media_kind,
                        deleted: m.deleted,
                    }),
                    is_self: c.is_self,
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
        Ok(Some(user)) => Json(UserView {
            user_id: user.user_id.to_string(),
            username: user.username,
        })
        .into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
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
