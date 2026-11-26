#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("username already taken")]
    UsernameTaken,
    #[error("handle already taken")]
    HandleTaken,
    #[error("invalid handle")]
    InvalidHandle,
    #[error("bio must be 500 characters or fewer")]
    BioTooLong,
    #[error("invalid username or password")]
    InvalidCredentials,
    #[error("invalid or expired token")]
    InvalidToken,
    #[error("password must be at least 8 characters")]
    WeakPassword,
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("internal error: {0}")]
    Internal(String),
}
