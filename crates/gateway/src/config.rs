pub struct Config {
    pub addr: String,
    pub database_url: String,
    pub nats_url: String,
    pub jwt_secret: String,
    pub media_dir: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            addr: std::env::var("GATEWAY_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string()),
            database_url: std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://chat@127.0.0.1:5433/chat".to_string()),
            nats_url: std::env::var("NATS_URL").unwrap_or_else(|_| "127.0.0.1:4222".to_string()),
            jwt_secret: std::env::var("JWT_SECRET").unwrap_or_else(|_| {
                tracing::warn!("JWT_SECRET not set — using an insecure development default");
                "dev-only-insecure-secret-change-me".to_string()
            }),
            media_dir: std::env::var("MEDIA_DIR").unwrap_or_else(|_| ".data/media".to_string()),
        }
    }
}
