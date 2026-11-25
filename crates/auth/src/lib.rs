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
        let row: Option<(i64, String)> = sqlx::query_as("select id, username from users where username = $1")
            .bind(username)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|(user_id, username)| UserSummary { user_id, username }))
    }

    pub async fn search_users(&self, query: &str, exclude_user_id: i64, limit: i64) -> Result<Vec<UserSummary>, AuthError> {
        // Escape ILIKE wildcards so a literal `%`/`_` in the query doesn't act as one.
        let escaped = query.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_");
        let pattern = format!("{escaped}%");
        let rows: Vec<(i64, String)> = sqlx::query_as(
            "select id, username from users where username ilike $1 and id != $2 order by username limit $3",
        )
        .bind(pattern)
        .bind(exclude_user_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(user_id, username)| UserSummary { user_id, username })
            .collect())
    }
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    matches!(
        err.as_database_error().and_then(|db_err| db_err.code()),
        Some(code) if code == "23505"
    )
}
