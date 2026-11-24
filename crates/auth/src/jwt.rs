use std::time::{SystemTime, UNIX_EPOCH};

use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

use crate::error::AuthError;
use crate::Claims;

const TOKEN_TTL_SECS: usize = 60 * 60 * 24 * 7; // 7 days

#[derive(Serialize, Deserialize)]
struct TokenClaims {
    sub: String,
    username: String,
    iat: usize,
    exp: usize,
}

pub fn issue(secret: &str, user_id: i64, username: &str) -> Result<String, AuthError> {
    let now = now_secs();
    let claims = TokenClaims {
        sub: user_id.to_string(),
        username: username.to_string(),
        iat: now,
        exp: now + TOKEN_TTL_SECS,
    };
    let key = EncodingKey::from_secret(secret.as_bytes())
        .map_err(|_| AuthError::Internal("failed to build signing key".into()))?;
    encode(&Header::new(Algorithm::HS256), &claims, &key)
        .map_err(|_| AuthError::Internal("failed to sign token".into()))
}

pub fn verify(secret: &str, token: &str) -> Result<Claims, AuthError> {
    let key = DecodingKey::from_secret(secret.as_bytes())
        .map_err(|_| AuthError::Internal("failed to build verification key".into()))?;
    let data = decode::<TokenClaims>(token, &key, &Validation::new(Algorithm::HS256))
        .map_err(|_| AuthError::InvalidToken)?;
    let user_id = data
        .claims
        .sub
        .parse::<i64>()
        .map_err(|_| AuthError::InvalidToken)?;
    Ok(Claims {
        user_id,
        username: data.claims.username,
    })
}

fn now_secs() -> usize {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as usize
}
