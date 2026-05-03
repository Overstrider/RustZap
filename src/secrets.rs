use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use thiserror::Error;

const ENVELOPE_PREFIX: &str = "rzsec:v1";
const NONCE_LEN: usize = 12;
const KEY_LEN: usize = 32;

#[derive(Clone)]
pub struct SecretMasterKey([u8; KEY_LEN]);

impl std::fmt::Debug for SecretMasterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretMasterKey(<redacted>)")
    }
}

impl SecretMasterKey {
    pub fn from_raw_32(bytes: [u8; KEY_LEN]) -> Self {
        Self(bytes)
    }

    pub fn from_env_value(value: &str) -> Result<Self, SecretError> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(SecretError::InvalidMasterKey);
        }
        if trimmed.len() == KEY_LEN * 2 && trimmed.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            let decoded = hex::decode(trimmed).map_err(|_| SecretError::InvalidMasterKey)?;
            return Self::from_bytes(&decoded);
        }
        if let Ok(decoded) = BASE64_STANDARD.decode(trimmed)
            && decoded.len() == KEY_LEN
        {
            return Self::from_bytes(&decoded);
        }
        Self::from_bytes(trimmed.as_bytes())
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, SecretError> {
        let raw: [u8; KEY_LEN] = bytes
            .try_into()
            .map_err(|_| SecretError::InvalidMasterKey)?;
        Ok(Self(raw))
    }
}

#[derive(Debug, Error)]
pub enum SecretError {
    #[error("RUSTZAP_SECRET_MASTER_KEY must decode to exactly 32 bytes")]
    InvalidMasterKey,
    #[error("failed to generate secret nonce")]
    NonceGeneration,
    #[error("secret encryption failed")]
    Encrypt,
    #[error("secret envelope is invalid")]
    InvalidEnvelope,
    #[error("secret decryption failed")]
    Decrypt,
    #[error("decrypted secret is not valid UTF-8")]
    Utf8,
}

pub fn encrypt_secret(key: &SecretMasterKey, plaintext: &str) -> Result<String, SecretError> {
    let mut nonce = [0_u8; NONCE_LEN];
    getrandom::fill(&mut nonce).map_err(|_| SecretError::NonceGeneration)?;
    let cipher = Aes256Gcm::new_from_slice(&key.0).map_err(|_| SecretError::InvalidMasterKey)?;
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), plaintext.as_bytes())
        .map_err(|_| SecretError::Encrypt)?;
    Ok(format!(
        "{ENVELOPE_PREFIX}:{}:{}",
        BASE64_STANDARD.encode(nonce),
        BASE64_STANDARD.encode(ciphertext)
    ))
}

pub fn decrypt_secret(key: &SecretMasterKey, envelope: &str) -> Result<String, SecretError> {
    let parts: Vec<&str> = envelope.split(':').collect();
    if parts.len() != 4 || parts[0] != "rzsec" || parts[1] != "v1" {
        return Err(SecretError::InvalidEnvelope);
    }
    let nonce = BASE64_STANDARD
        .decode(parts[2])
        .map_err(|_| SecretError::InvalidEnvelope)?;
    if nonce.len() != NONCE_LEN {
        return Err(SecretError::InvalidEnvelope);
    }
    let ciphertext = BASE64_STANDARD
        .decode(parts[3])
        .map_err(|_| SecretError::InvalidEnvelope)?;
    let cipher = Aes256Gcm::new_from_slice(&key.0).map_err(|_| SecretError::InvalidMasterKey)?;
    let plaintext = cipher
        .decrypt(Nonce::from_slice(&nonce), ciphertext.as_ref())
        .map_err(|_| SecretError::Decrypt)?;
    String::from_utf8(plaintext).map_err(|_| SecretError::Utf8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_envelope_round_trips_and_hides_plaintext() {
        let key = SecretMasterKey::from_raw_32(*b"0123456789abcdef0123456789abcdef");

        let encrypted = encrypt_secret(&key, "webhook_secret").unwrap();

        assert!(encrypted.starts_with("rzsec:v1:"));
        assert!(!encrypted.contains("webhook_secret"));
        assert_eq!(decrypt_secret(&key, &encrypted).unwrap(), "webhook_secret");
    }

    #[test]
    fn master_key_accepts_hex_base64_and_raw_32_bytes() {
        assert!(SecretMasterKey::from_env_value("0123456789abcdef0123456789abcdef").is_ok());
        assert!(
            SecretMasterKey::from_env_value(
                "3031323334353637383961626364656630313233343536373839616263646566"
            )
            .is_ok()
        );
        assert!(
            SecretMasterKey::from_env_value("MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=").is_ok()
        );
        assert!(SecretMasterKey::from_env_value("too-short").is_err());
    }
}
