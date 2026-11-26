mod error;
mod jwt;
mod password;

pub use error::AuthError;

use chat_common::snowflake::SnowflakeGenerator;
use sqlx::PgPool;

pub struct Claims {
    pub user_id: i64,
    pub username: String,
}

pub struct UserSummary {
    pub user_id: i64,
    pub username: String,
    pub handle: Option<String>,
    pub bio: Option<String>,
    pub active_avatar_media_id: Option<i64>,
}

pub struct AvatarSummary {
    pub media_id: i64,
    pub is_active: bool,
    pub created_at_ms: i64,
}

pub struct AuthService {
    pool: PgPool,
    ids: SnowflakeGenerator,
    jwt_secret: String,
}

impl AuthService {
    pub fn new(pool: PgPool, ids: SnowflakeGenerator, jwt_secret: String) -> Self {
        Self {
            pool,
            ids,
            jwt_secret,
        }
    }

    pub async fn register(&self, username: &str, password: &str) -> Result<(i64, String), AuthError> {
        password::validate_strength(password)?;
        let password_hash = password::hash(password)?;
        let user_id = self.ids.next_id();

        sqlx::query("insert into users (id, username, password_hash) values ($1, $2, $3)")
            .bind(user_id)
            .bind(username)
            .bind(&password_hash)
            .execute(&self.pool)
            .await
            .map_err(|err| {
                if is_unique_violation(&err) {
                    AuthError::UsernameTaken
                } else {
                    AuthError::Database(err)
                }
            })?;

        let token = jwt::issue(&self.jwt_secret, user_id, username)?;
        Ok((user_id, token))
    }

    pub async fn login(&self, username: &str, password: &str) -> Result<(i64, String), AuthError> {
        let row: Option<(i64, String)> =
            sqlx::query_as("select id, password_hash from users where username = $1")
                .bind(username)
                .fetch_optional(&self.pool)
                .await?;
        let (user_id, password_hash) = row.ok_or(AuthError::InvalidCredentials)?;
        password::verify(password, &password_hash)?;

        let token = jwt::issue(&self.jwt_secret, user_id, username)?;
        Ok((user_id, token))
    }

    pub fn verify_token(&self, token: &str) -> Result<Claims, AuthError> {
        jwt::verify(&self.jwt_secret, token)
    }

    pub async fn find_by_username(&self, username: &str) -> Result<Option<UserSummary>, AuthError> {
        let row: Option<UserRow> = sqlx::query_as(
            "select id, username, handle, bio, active_avatar_media_id from users where username = $1",
        )
        .bind(username)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(UserRow::into_summary))
    }

    pub async fn find_by_id(&self, user_id: i64) -> Result<Option<UserSummary>, AuthError> {
        let row: Option<UserRow> = sqlx::query_as(
            "select id, username, handle, bio, active_avatar_media_id from users where id = $1",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(UserRow::into_summary))
    }

    /// Matches on `username` OR the public `handle` — a caller typing
    /// "@alice" should find the same person as typing "alice" (stripping
    /// the leading `@`, if any, is the HTTP layer's job, not this query's).
    pub async fn search_users(&self, query: &str, exclude_user_id: i64, limit: i64) -> Result<Vec<UserSummary>, AuthError> {
        // Escape ILIKE wildcards so a literal `%`/`_` in the query doesn't act as one.
        let escaped = query.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
        let pattern = format!("{escaped}%");
        let rows: Vec<UserRow> = sqlx::query_as(
            "select id, username, handle, bio, active_avatar_media_id from users
             where (username ilike $1 or handle ilike $1) and id != $2
             order by username limit $3",
        )
        .bind(pattern)
        .bind(exclude_user_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(UserRow::into_summary).collect())
    }

    /// Batch username lookup by id — used to compose group system-message
    /// text ("Alice added Bob and Carol") server-side from ids the caller
    /// already validated as real members, rather than trusting any
    /// client-supplied display name.
    pub async fn usernames_for_ids(&self, ids: &[i64]) -> Result<Vec<UserSummary>, AuthError> {
        let rows: Vec<UserRow> = sqlx::query_as(
            "select id, username, handle, bio, active_avatar_media_id from users where id = any($1)",
        )
        .bind(ids)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(UserRow::into_summary).collect())
    }

    /// Sets (or, with `None`, clears) the caller's bio. Length is validated
    /// here (≤500 chars, same cap the client-side composer enforces) rather
    /// than left to a DB constraint.
    pub async fn set_bio(&self, user_id: i64, bio: Option<&str>) -> Result<(), AuthError> {
        if let Some(bio) = bio
            && bio.chars().count() > 500
        {
            return Err(AuthError::BioTooLong);
        }
        sqlx::query("update users set bio = $1 where id = $2")
            .bind(bio)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Sets (or, with `None`, clears) the caller's public `@handle`.
    /// Format is validated here rather than left to a DB constraint: 3-32
    /// chars, lowercase ascii letters/digits/underscore, must start with a
    /// letter — the same shape Telegram-style handles use.
    pub async fn set_handle(&self, user_id: i64, handle: Option<&str>) -> Result<(), AuthError> {
        if let Some(handle) = handle
            && !is_valid_handle(handle)
        {
            return Err(AuthError::InvalidHandle);
        }
        sqlx::query("update users set handle = $1 where id = $2")
            .bind(handle)
            .bind(user_id)
            .execute(&self.pool)
            .await
            .map_err(|err| {
                if is_unique_violation(&err) {
                    AuthError::HandleTaken
                } else {
                    AuthError::Database(err)
                }
            })?;
        Ok(())
    }

    /// Adds a photo to the caller's gallery and makes it the active one —
    /// uploading a new photo is the common "I want this to be my avatar
    /// now" action; switching back to an older one is a separate explicit
    /// `set_active_avatar` call.
    pub async fn add_avatar(&self, user_id: i64, media_id: i64) -> Result<(), AuthError> {
        let id = self.ids.next_id();
        let created_at_ms = now_ms();
        let mut tx = self.pool.begin().await?;
        sqlx::query("insert into user_avatars (id, user_id, media_id, created_at_ms) values ($1, $2, $3, $4)")
            .bind(id)
            .bind(user_id)
            .bind(media_id)
            .bind(created_at_ms)
            .execute(&mut *tx)
            .await?;
        sqlx::query("update users set active_avatar_media_id = $1 where id = $2")
            .bind(media_id)
            .bind(user_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn list_avatars(&self, user_id: i64) -> Result<Vec<AvatarSummary>, AuthError> {
        let rows: Vec<(i64, i64, Option<i64>)> = sqlx::query_as(
            "select ua.media_id, ua.created_at_ms, u.active_avatar_media_id
             from user_avatars ua
             join users u on u.id = ua.user_id
             where ua.user_id = $1
             order by ua.id desc",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(media_id, created_at_ms, active_media_id)| AvatarSummary {
                media_id,
                is_active: active_media_id == Some(media_id),
                created_at_ms,
            })
            .collect())
    }

    /// `Ok(false)` if `media_id` isn't actually one of this user's own
    /// gallery photos — callers should treat that the same as "not found".
    pub async fn set_active_avatar(&self, user_id: i64, media_id: i64) -> Result<bool, AuthError> {
        let owns: Option<(i32,)> =
            sqlx::query_as("select 1 from user_avatars where user_id = $1 and media_id = $2")
                .bind(user_id)
                .bind(media_id)
                .fetch_optional(&self.pool)
                .await?;
        if owns.is_none() {
            return Ok(false);
        }
        sqlx::query("update users set active_avatar_media_id = $1 where id = $2")
            .bind(media_id)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(true)
    }

    /// Removes a photo from the gallery. If it was the active one, the
    /// next-most-recent remaining photo (if any) becomes active instead —
    /// deleting your current avatar shouldn't silently leave you with a
    /// broken reference, nor should it require a separate follow-up call.
    pub async fn delete_avatar(&self, user_id: i64, media_id: i64) -> Result<(), AuthError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("delete from user_avatars where user_id = $1 and media_id = $2")
            .bind(user_id)
            .bind(media_id)
            .execute(&mut *tx)
            .await?;

        let was_active: Option<(i64,)> =
            sqlx::query_as("select active_avatar_media_id from users where id = $1 and active_avatar_media_id = $2")
                .bind(user_id)
                .bind(media_id)
                .fetch_optional(&mut *tx)
                .await?;
        if was_active.is_some() {
            let fallback: Option<(i64,)> = sqlx::query_as(
                "select media_id from user_avatars where user_id = $1 order by id desc limit 1",
            )
            .bind(user_id)
            .fetch_optional(&mut *tx)
            .await?;
            sqlx::query("update users set active_avatar_media_id = $1 where id = $2")
                .bind(fallback.map(|(id,)| id))
                .bind(user_id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

type UserRow = (i64, String, Option<String>, Option<String>, Option<i64>);

trait IntoUserSummary {
    fn into_summary(self) -> UserSummary;
}

impl IntoUserSummary for UserRow {
    fn into_summary(self) -> UserSummary {
        let (user_id, username, handle, bio, active_avatar_media_id) = self;
        UserSummary {
            user_id,
            username,
            handle,
            bio,
            active_avatar_media_id,
        }
    }
}

fn is_valid_handle(handle: &str) -> bool {
    let mut chars = handle.chars();
    let Some(first) = chars.next() else { return false };
    if !first.is_ascii_lowercase() {
        return false;
    }
    (3..=32).contains(&handle.len())
        && handle.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    matches!(
        err.as_database_error().and_then(|db_err| db_err.code()),
        Some(code) if code == "23505"
    )
}
