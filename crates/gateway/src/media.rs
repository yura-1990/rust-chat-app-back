//! Local-disk media storage for photo/file/round-video attachments.
//!
//! Deliberately not S3/MinIO: this is a single-gateway dev setup, and the
//! "secure" requirement is about access control and content handling, not
//! which storage backend holds the bytes. Swapping `store`/`load` for an
//! object-store client later is a contained change — nothing above this
//! module (ws.rs, http.rs) would need to know.
//!
//! Security model:
//! - Uploads are membership-checked (same as everything else) and
//!   size/mime validated against allow-lists per `kind`.
//! - Reads are via a short-lived HMAC-signed URL (`exp` + `sig`), minted
//!   fresh by the server whenever a message is broadcast or history is
//!   fetched — the raw file is never served from a guessable or permanent
//!   public path, and the DB never stores a URL, only the `media_id`.
//! - `kind: "file"` is always served as `application/octet-stream` with
//!   `Content-Disposition: attachment`, regardless of the uploaded mime —
//!   a browser never gets a chance to render an arbitrary uploaded file as
//!   HTML/SVG/script, even if the declared Content-Type was wrong or lied
//!   about. Images and round videos are allow-listed to types that are
//!   safe to render inline.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Json, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::http::bearer_user;
use crate::state::AppState;

pub const MAX_UPLOAD_BYTES: usize = 25 * 1024 * 1024; // 25 MB
const SIGNED_URL_TTL_MS: i64 = 60 * 60 * 1000; // 1 hour

/// Extensions rejected for `kind: "file"` uploads — anything that a
/// browser, OS, or script runtime might execute rather than just display.
/// Deliberately permissive otherwise ("like anything") since the
/// Content-Disposition/octet-stream handling above is the real backstop.
const DANGEROUS_EXTENSIONS: &[&str] = &[
    "exe", "bat", "cmd", "com", "scr", "msi", "msp", "dll", "sh", "bash", "ps1", "psm1", "vbs",
    "vbe", "js", "mjs", "jar", "app", "apk", "html", "htm", "svg", "php", "py", "rb", "wsf",
    "reg", "lnk",
];

fn image_mime_to_ext(mime: &str) -> Option<&'static str> {
    match mime {
        "image/jpeg" => Some("jpg"),
        "image/png" => Some("png"),
        "image/webp" => Some("webp"),
        "image/gif" => Some("gif"),
        _ => None,
    }
}

fn video_mime_to_ext(mime: &str) -> Option<&'static str> {
    match mime {
        "video/webm" => Some("webm"),
        "video/mp4" => Some("mp4"),
        _ => None,
    }
}

fn audio_mime_to_ext(mime: &str) -> Option<&'static str> {
    match mime {
        "audio/webm" => Some("webm"),
        "audio/ogg" => Some("ogg"),
        "audio/mpeg" => Some("mp3"),
        "audio/mp4" => Some("m4a"),
        _ => None,
    }
}

fn extension_for(kind: &str, mime: &str) -> Option<&'static str> {
    match kind {
        "image" => image_mime_to_ext(mime),
        "round_video" => video_mime_to_ext(mime),
        "voice" => audio_mime_to_ext(mime),
        "file" => Some("bin"),
        _ => None,
    }
}

fn file_extension_is_safe(name: &str) -> bool {
    match name.rsplit('.').next() {
        Some(ext) if ext != name => !DANGEROUS_EXTENSIONS.contains(&ext.to_lowercase().as_str()),
        _ => true, // no extension at all — nothing to sniff/execute by name
    }
}

pub struct MediaStore {
    base_dir: PathBuf,
    secret: String,
}

/// Longest edge a group avatar is resized to before re-encoding. Avatars
/// are shown small (list rows, headers, info panels) — there's no reason
/// to keep more resolution than that around.
pub const AVATAR_MAX_EDGE: u32 = 512;
/// Raw upload size cap for an avatar, *before* optimization — generous
/// enough for an un-cropped phone photo, far smaller than the 25 MB general
/// attachment cap since this is a single small image, not a video/file.
pub const AVATAR_MAX_UPLOAD_BYTES: usize = 8 * 1024 * 1024;
const AVATAR_WEBP_QUALITY: f32 = 90.0;

#[derive(Debug)]
pub struct AvatarError;

impl std::fmt::Display for AvatarError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "could not decode that as an image")
    }
}

/// Validates the upload is actually a decodable image (never trust the
/// client's declared Content-Type/extension — same principle as the `file`
/// kind's extension check above), resizes it to at most `AVATAR_MAX_EDGE`
/// on the longest side, and re-encodes it as lossy WebP — a small file at
/// visually-unchanged quality, regardless of what format/size came in.
pub fn optimize_avatar(bytes: &[u8]) -> Result<Vec<u8>, AvatarError> {
    let img = image::load_from_memory(bytes).map_err(|_| AvatarError)?;
    let img = if img.width() > AVATAR_MAX_EDGE || img.height() > AVATAR_MAX_EDGE {
        img.resize(AVATAR_MAX_EDGE, AVATAR_MAX_EDGE, image::imageops::FilterType::Lanczos3)
    } else {
        img
    };
    let encoder = webp::Encoder::from_image(&img).map_err(|_| AvatarError)?;
    Ok(encoder.encode(AVATAR_WEBP_QUALITY).to_vec())
}

impl MediaStore {
    pub fn new(base_dir: PathBuf, secret: String) -> Self {
        Self { base_dir, secret }
    }

    fn path_for(&self, chat_id: i64, media_id: i64, ext: &str) -> PathBuf {
        self.base_dir.join(chat_id.to_string()).join(format!("{media_id}.{ext}"))
    }

    /// Avatars aren't scoped to one chat_id (a group's avatar is set at
    /// creation time, independent of any one message), so they live in
    /// their own namespace directory instead of under a per-chat one.
    fn avatar_path_for(&self, media_id: i64) -> PathBuf {
        self.base_dir.join("avatars").join(format!("{media_id}.webp"))
    }

    pub async fn store_avatar(&self, media_id: i64, bytes: &[u8]) -> std::io::Result<()> {
        let dir = self.base_dir.join("avatars");
        tokio::fs::create_dir_all(&dir).await?;
        tokio::fs::write(self.avatar_path_for(media_id), bytes).await
    }

    pub async fn load_avatar(&self, media_id: i64) -> std::io::Result<Vec<u8>> {
        tokio::fs::read(self.avatar_path_for(media_id)).await
    }

    /// Avatars are served unsigned, unlike chat media — low-sensitivity,
    /// meant to be visible to anyone who can see the group exists, the same
    /// way a Telegram group avatar isn't access-controlled per viewer.
    pub fn avatar_url(&self, media_id: i64) -> String {
        format!("/avatars/{media_id}")
    }

    async fn store(&self, chat_id: i64, media_id: i64, ext: &str, bytes: &[u8]) -> std::io::Result<()> {
        let dir = self.base_dir.join(chat_id.to_string());
        tokio::fs::create_dir_all(&dir).await?;
        tokio::fs::write(self.path_for(chat_id, media_id, ext), bytes).await
    }

    async fn load(&self, chat_id: i64, media_id: i64, ext: &str) -> std::io::Result<Vec<u8>> {
        tokio::fs::read(self.path_for(chat_id, media_id, ext)).await
    }

    /// Forwarding a message copies the file on disk into the destination
    /// chat's own directory under a freshly minted media_id — never a
    /// client-side download-then-reupload round trip, and never two
    /// messages sharing one file path (so each chat's media stays
    /// independently deletable/expirable later).
    async fn copy(
        &self,
        source_chat_id: i64,
        source_media_id: i64,
        dest_chat_id: i64,
        dest_media_id: i64,
        ext: &str,
    ) -> std::io::Result<()> {
        let dest_dir = self.base_dir.join(dest_chat_id.to_string());
        tokio::fs::create_dir_all(&dest_dir).await?;
        tokio::fs::copy(
            self.path_for(source_chat_id, source_media_id, ext),
            self.path_for(dest_chat_id, dest_media_id, ext),
        )
        .await?;
        Ok(())
    }

    /// Best-effort removal of the underlying file once a message
    /// referencing it is deleted. The DB-level access control (media
    /// lookups failing once the message's media_* columns are cleared) is
    /// what actually revokes access; this is just disk hygiene, so a
    /// failure here is logged, never propagated.
    pub async fn delete(&self, chat_id: i64, media_id: i64, kind: &str, mime: &str) {
        let Some(ext) = extension_for(kind, mime) else { return };
        if let Err(err) = tokio::fs::remove_file(self.path_for(chat_id, media_id, ext)).await {
            tracing::warn!(?err, chat_id, media_id, "failed to remove deleted media file");
        }
    }

    /// Best-effort removal of an avatar file (group or personal-gallery)
    /// once its DB row is deleted — same disk-hygiene-only contract as
    /// `delete` above.
    pub async fn delete_avatar(&self, media_id: i64) {
        if let Err(err) = tokio::fs::remove_file(self.avatar_path_for(media_id)).await {
            tracing::warn!(?err, media_id, "failed to remove deleted avatar file");
        }
    }

    /// A relative URL the client can use directly (image `src`, video
    /// `src`, or a download link) — valid for `SIGNED_URL_TTL_MS`.
    pub fn signed_url(&self, chat_id: i64, media_id: i64) -> String {
        let expires_at_ms = now_ms() + SIGNED_URL_TTL_MS;
        let sig = self.sign(chat_id, media_id, expires_at_ms);
        format!("/media/{chat_id}/{media_id}?exp={expires_at_ms}&sig={sig}")
    }

    fn sign(&self, chat_id: i64, media_id: i64, expires_at_ms: i64) -> String {
        let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes()).expect("HMAC accepts a key of any length");
        mac.update(format!("{chat_id}.{media_id}.{expires_at_ms}").as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    fn verify(&self, chat_id: i64, media_id: i64, expires_at_ms: i64, sig: &str) -> bool {
        if now_ms() > expires_at_ms {
            return false;
        }
        let Ok(expected) = hex::decode(sig) else {
            return false;
        };
        let mut mac = HmacSha256::new_from_slice(self.secret.as_bytes()).expect("HMAC accepts a key of any length");
        mac.update(format!("{chat_id}.{media_id}.{expires_at_ms}").as_bytes());
        mac.verify_slice(&expected).is_ok()
    }
}

type HmacSha256 = Hmac<Sha256>;

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

#[derive(Deserialize)]
pub struct UploadQuery {
    pub kind: String,
    pub name: Option<String>,
}

#[derive(Serialize)]
pub struct UploadResponse {
    pub media_id: String,
    pub kind: String,
    pub mime: String,
    pub name: Option<String>,
    pub size: i64,
}

pub async fn upload(
    State(state): State<Arc<AppState>>,
    Path(chat_id): Path<i64>,
    headers: HeaderMap,
    Query(query): Query<UploadQuery>,
    body: Bytes,
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

    if !["image", "file", "round_video", "voice"].contains(&query.kind.as_str()) {
        return (StatusCode::BAD_REQUEST, "kind must be image, file, round_video, or voice").into_response();
    }
    if body.len() > MAX_UPLOAD_BYTES {
        return (StatusCode::PAYLOAD_TOO_LARGE, "file exceeds the 25 MB limit").into_response();
    }
    if let Some(name) = &query.name
        && !file_extension_is_safe(name)
    {
        return (StatusCode::BAD_REQUEST, "that file type isn't allowed").into_response();
    }

    // `MediaRecorder.mimeType` (round-video/voice recordings) typically
    // comes back with a codecs parameter, e.g. "audio/webm;codecs=opus" —
    // strip it before matching, since extension_for/*_mime_to_ext compare
    // the bare type.
    let mime = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .split(';')
        .next()
        .unwrap_or("application/octet-stream")
        .trim()
        .to_string();

    let Some(ext) = extension_for(&query.kind, &mime) else {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "that content type isn't supported for this kind of attachment",
        )
            .into_response();
    };

    let media_id = state.media_ids.next_id();
    if let Err(err) = state.media.store(chat_id, media_id, ext, &body).await {
        tracing::error!(?err, "failed to store uploaded media");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    axum::Json(UploadResponse {
        media_id: media_id.to_string(),
        kind: query.kind,
        mime,
        name: query.name,
        size: body.len() as i64,
    })
    .into_response()
}

#[derive(Deserialize)]
pub struct ServeQuery {
    pub exp: i64,
    pub sig: String,
}

pub async fn serve(
    State(state): State<Arc<AppState>>,
    Path((chat_id, media_id)): Path<(i64, i64)>,
    Query(query): Query<ServeQuery>,
) -> Response {
    if !state.media.verify(chat_id, media_id, query.exp, &query.sig) {
        return StatusCode::FORBIDDEN.into_response();
    }

    // The signed URL only vouches for chat_id+media_id — the kind/mime used
    // to pick the right file is looked up from the message that referenced
    // it, so a forged/mismatched extension can't be requested directly.
    let Some((kind, mime, name, _size)) = state.chat.media_lookup(chat_id, media_id).await.unwrap_or(None) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(ext) = extension_for(&kind, &mime) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let bytes = match state.media.load(chat_id, media_id, ext).await {
        Ok(bytes) => bytes,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };

    let (content_type, disposition_kind) = if kind == "file" {
        ("application/octet-stream".to_string(), "attachment")
    } else {
        (mime, "inline")
    };
    let filename = name.unwrap_or_else(|| format!("{media_id}.{ext}"));

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, content_type),
            (header::CONTENT_DISPOSITION, format!("{disposition_kind}; filename=\"{filename}\"")),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff".to_string()),
            (header::CACHE_CONTROL, "private, max-age=3600".to_string()),
        ],
        bytes,
    )
        .into_response()
}

#[derive(Deserialize)]
pub struct ForwardMediaRequest {
    pub source_chat_id: String,
    pub media_id: String,
}

/// Forwarding a media message never re-downloads and re-uploads the bytes
/// through the client — it copies the file server-side into the
/// destination chat and mints it a fresh media_id there. The caller must
/// be a member of both chats: the source (to read the file at all) and the
/// destination (to be allowed to post into it).
pub async fn forward(
    State(state): State<Arc<AppState>>,
    Path(dest_chat_id): Path<i64>,
    headers: HeaderMap,
    Json(body): Json<ForwardMediaRequest>,
) -> Response {
    let user_id = match bearer_user(&headers, &state.auth) {
        Ok(id) => id,
        Err(status) => return status.into_response(),
    };

    let Ok(source_chat_id) = body.source_chat_id.parse::<i64>() else {
        return (StatusCode::BAD_REQUEST, "source_chat_id must be a numeric id").into_response();
    };
    let Ok(media_id) = body.media_id.parse::<i64>() else {
        return (StatusCode::BAD_REQUEST, "media_id must be a numeric id").into_response();
    };

    for chat_id in [source_chat_id, dest_chat_id] {
        match state.chat.is_member(chat_id, user_id).await {
            Ok(true) => {}
            Ok(false) => return StatusCode::FORBIDDEN.into_response(),
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }

    let Some((kind, mime, name, size)) = state
        .chat
        .media_lookup(source_chat_id, media_id)
        .await
        .unwrap_or(None)
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(ext) = extension_for(&kind, &mime) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let new_media_id = state.media_ids.next_id();
    if let Err(err) = state
        .media
        .copy(source_chat_id, media_id, dest_chat_id, new_media_id, ext)
        .await
    {
        tracing::error!(?err, "failed to copy forwarded media");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    axum::Json(UploadResponse {
        media_id: new_media_id.to_string(),
        kind,
        mime,
        name,
        size,
    })
    .into_response()
}
