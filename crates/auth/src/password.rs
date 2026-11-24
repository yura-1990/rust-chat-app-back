use argon2::{Argon2, PasswordHasher, PasswordVerifier};

use crate::error::AuthError;

const MIN_PASSWORD_LEN: usize = 8;

pub fn validate_strength(password: &str) -> Result<(), AuthError> {
    if password.chars().count() < MIN_PASSWORD_LEN {
        return Err(AuthError::WeakPassword);
    }
    Ok(())
}

pub fn hash(password: &str) -> Result<String, AuthError> {
    Argon2::default()
        .hash_password(password.as_bytes())
        .map(|hash| hash.to_string())
        .map_err(|_| AuthError::Internal("failed to hash password".into()))
}

pub fn verify(password: &str, hash: &str) -> Result<(), AuthError> {
    Argon2::default()
        .verify_password(password.as_bytes(), hash)
        .map_err(|_| AuthError::InvalidCredentials)
}
