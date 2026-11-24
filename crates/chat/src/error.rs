#[derive(Debug, thiserror::Error)]
pub enum ChatError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}
