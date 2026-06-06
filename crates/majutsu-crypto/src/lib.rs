#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncryptionMode {
    None,
    ChaCha20Poly1305,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyMaterial {
    pub key_id: String,
    pub hex_key: String,
}

pub fn encrypted_object_header() -> &'static [u8] {
    b"MJENC1"
}
