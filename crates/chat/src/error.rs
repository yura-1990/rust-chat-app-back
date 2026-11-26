#[derive(Debug, thiserror::Error)]
pub enum ChatError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("handle already taken")]
    HandleTaken,
    #[error("invalid handle")]
    InvalidHandle,
    #[error("only the owner can do that")]
    NotOwner,
}
