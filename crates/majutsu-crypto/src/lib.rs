use age::secrecy::ExposeSecret;
use anyhow::{Context, Result, anyhow, bail};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;

const ENC_MAGIC: &[u8] = b"MJENC1\n";
const AGE_MAGIC: &[u8] = b"age-encryption.org/v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionMode {
    None,
    Age,
    ChaCha20Poly1305,
}

impl EncryptionMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "" | "none" => Ok(Self::None),
            "age" => Ok(Self::Age),
            "chacha20poly1305" => Ok(Self::ChaCha20Poly1305),
            _ => bail!("security encryption must be none, age, or chacha20poly1305"),
        }
    }

    pub fn enabled(self) -> bool {
        !matches!(self, Self::None)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Age => "age",
            Self::ChaCha20Poly1305 => "chacha20poly1305",
        }
    }
}

pub fn encryption_enabled(value: &str) -> Result<bool> {
    Ok(EncryptionMode::parse(value)?.enabled())
}

pub fn default_security_key_id() -> &'static str {
    "default"
}

pub fn default_security_hash() -> &'static str {
    "blake3-keyed"
}

pub fn validate_security_hash(hash: &str) -> Result<()> {
    match hash {
        "blake3-keyed" | "blake3" | "sha256" => Ok(()),
        _ => bail!("security hash must be blake3-keyed, blake3, or sha256"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyMaterial {
    pub key_id: String,
    pub hex_key: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AgeKeyring {
    #[serde(default)]
    pub recipients: Vec<String>,
    #[serde(default)]
    pub identities: Vec<String>,
}

pub fn encrypted_object_header() -> &'static [u8] {
    b"MJENC1"
}

pub fn age_object_header() -> &'static [u8] {
    AGE_MAGIC
}

pub fn encode_object(
    bytes: &[u8],
    encryption: EncryptionMode,
    master_key_path: &Path,
    recipients_path: &Path,
) -> Result<Vec<u8>> {
    match encryption {
        EncryptionMode::None => Ok(bytes.to_vec()),
        EncryptionMode::Age => {
            if let Some(ciphertext) = age_encrypt_object(recipients_path, bytes)? {
                Ok(ciphertext)
            } else {
                encode_legacy_envelope(bytes, master_key_path)
            }
        }
        EncryptionMode::ChaCha20Poly1305 => encode_legacy_envelope(bytes, master_key_path),
    }
}

pub fn decode_object(
    bytes: &[u8],
    master_key_path: &Path,
    recipients_path: &Path,
) -> Result<Vec<u8>> {
    if bytes.starts_with(AGE_MAGIC) {
        return age_decrypt_object(recipients_path, bytes);
    }
    if !bytes.starts_with(ENC_MAGIC) {
        return Ok(bytes.to_vec());
    }
    decode_legacy_envelope(bytes, master_key_path)
}

pub fn random_key_hex() -> Result<String> {
    let mut key = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    Ok(hex::encode(key))
}

pub fn validate_key_hex(hex_key: &str) -> Result<()> {
    let bytes = hex::decode(hex_key.trim())?;
    if bytes.len() != 32 {
        bail!("master key must be 32 bytes encoded as 64 hex characters");
    }
    Ok(())
}

pub fn read_master_key(master_key_path: &Path) -> Result<String> {
    if let Ok(key) = env::var("MAJUTSU_MASTER_KEY") {
        validate_key_hex(&key)?;
        return Ok(key);
    }
    let key = fs::read_to_string(master_key_path)
        .with_context(|| format!("missing master key: {}", master_key_path.display()))?;
    validate_key_hex(key.trim())?;
    Ok(key.trim().to_string())
}

pub fn write_master_key(master_key_path: &Path, hex_key: &str) -> Result<()> {
    validate_key_hex(hex_key)?;
    if let Some(parent) = master_key_path.parent() {
        fs::create_dir_all(parent)?;
        restrict_key_parent_permissions(parent)?;
    }
    fs::write(master_key_path, format!("{}\n", hex_key.trim()))?;
    restrict_key_file_permissions(master_key_path)?;
    Ok(())
}

#[cfg(unix)]
fn restrict_key_parent_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("set key directory permissions {}", path.display()))
}

#[cfg(not(unix))]
fn restrict_key_parent_permissions(_: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn restrict_key_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("set master key permissions {}", path.display()))
}

#[cfg(not(unix))]
fn restrict_key_file_permissions(_: &Path) -> Result<()> {
    Ok(())
}

pub fn read_age_keyring(recipients_path: &Path) -> Result<AgeKeyring> {
    if !recipients_path.exists() {
        return Ok(AgeKeyring::default());
    }
    toml::from_str(&fs::read_to_string(recipients_path)?)
        .with_context(|| format!("parse age keyring {}", recipients_path.display()))
}

pub fn write_age_keyring(recipients_path: &Path, keyring: &AgeKeyring) -> Result<()> {
    if let Some(parent) = recipients_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(recipients_path, toml::to_string_pretty(keyring)?)?;
    Ok(())
}

pub fn ensure_age_keyring(recipients_path: &Path) -> Result<()> {
    let mut keyring = read_age_keyring(recipients_path)?;
    if keyring.recipients.is_empty() && keyring.identities.is_empty() {
        let identity = age::x25519::Identity::generate();
        keyring.recipients.push(identity.to_public().to_string());
        keyring
            .identities
            .push(identity.to_string().expose_secret().to_string());
        write_age_keyring(recipients_path, &keyring)?;
    }
    Ok(())
}

fn encode_legacy_envelope(bytes: &[u8], master_key_path: &Path) -> Result<Vec<u8>> {
    let key_hex = read_master_key(master_key_path)?;
    let key_bytes = hex::decode(key_hex.trim())?;
    let key = Key::from_slice(&key_bytes);
    let cipher = ChaCha20Poly1305::new(key);
    let mut nonce_bytes = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce_bytes), bytes)
        .map_err(|_| anyhow!("object encryption failed"))?;
    let mut out = Vec::with_capacity(ENC_MAGIC.len() + nonce_bytes.len() + ciphertext.len());
    out.extend_from_slice(ENC_MAGIC);
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

fn decode_legacy_envelope(bytes: &[u8], master_key_path: &Path) -> Result<Vec<u8>> {
    let start = ENC_MAGIC.len();
    if bytes.len() < start + 12 {
        bail!("encrypted object is truncated");
    }
    let nonce = &bytes[start..start + 12];
    let ciphertext = &bytes[start + 12..];
    let key_hex = read_master_key(master_key_path)?;
    let key_bytes = hex::decode(key_hex.trim())?;
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&key_bytes));
    cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|_| anyhow!("object decryption failed"))
}

fn age_encrypt_object(recipients_path: &Path, bytes: &[u8]) -> Result<Option<Vec<u8>>> {
    let recipients = read_age_recipients(recipients_path)?;
    if recipients.is_empty() {
        return Ok(None);
    }
    let recipient_refs = recipients
        .iter()
        .map(|recipient| recipient as &dyn age::Recipient)
        .collect::<Vec<_>>();
    let encryptor = age::Encryptor::with_recipients(recipient_refs.into_iter())?;
    let mut ciphertext = Vec::with_capacity(bytes.len());
    let mut writer = encryptor.wrap_output(&mut ciphertext)?;
    writer.write_all(bytes)?;
    writer.finish()?;
    Ok(Some(ciphertext))
}

fn age_decrypt_object(recipients_path: &Path, bytes: &[u8]) -> Result<Vec<u8>> {
    let identities = read_age_identities(recipients_path)?;
    if identities.is_empty() {
        bail!("age encrypted object requires an identity in keys/recipients.toml");
    }
    let identity_refs = identities
        .iter()
        .map(|identity| identity as &dyn age::Identity)
        .collect::<Vec<_>>();
    let decryptor = age::Decryptor::new_buffered(bytes)?;
    let mut reader = decryptor.decrypt(identity_refs.into_iter())?;
    let mut plaintext = Vec::new();
    reader.read_to_end(&mut plaintext)?;
    Ok(plaintext)
}

fn read_age_recipients(recipients_path: &Path) -> Result<Vec<age::x25519::Recipient>> {
    read_age_keyring(recipients_path)?
        .recipients
        .into_iter()
        .map(|recipient| {
            recipient
                .parse()
                .map_err(|err| anyhow!("invalid age recipient {recipient}: {err}"))
        })
        .collect()
}

fn read_age_identities(recipients_path: &Path) -> Result<Vec<age::x25519::Identity>> {
    read_age_keyring(recipients_path)?
        .identities
        .into_iter()
        .map(|identity| {
            identity
                .parse()
                .map_err(|err| anyhow!("invalid age identity: {err}"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_encryption_modes() {
        assert_eq!(EncryptionMode::parse("").unwrap(), EncryptionMode::None);
        assert_eq!(EncryptionMode::parse("none").unwrap(), EncryptionMode::None);
        assert_eq!(EncryptionMode::parse("age").unwrap(), EncryptionMode::Age);
        assert_eq!(
            EncryptionMode::parse("chacha20poly1305").unwrap(),
            EncryptionMode::ChaCha20Poly1305
        );
        assert!(EncryptionMode::parse("aes").is_err());
    }

    #[test]
    fn encryption_enabled_matches_mode() {
        assert!(!encryption_enabled("none").unwrap());
        assert!(encryption_enabled("age").unwrap());
        assert!(encryption_enabled("chacha20poly1305").unwrap());
    }

    #[test]
    fn security_defaults_match_config_defaults() {
        assert_eq!(default_security_key_id(), "default");
        assert_eq!(default_security_hash(), "blake3-keyed");
    }

    #[test]
    fn validates_supported_security_hashes() {
        for hash in ["blake3-keyed", "blake3", "sha256"] {
            validate_security_hash(hash).unwrap();
        }
        assert!(validate_security_hash("md5").is_err());
    }
}
