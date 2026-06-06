use majutsu_core::ObjectKey;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackEntry {
    pub oid: String,
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackIndex {
    pub pack_id: String,
    pub pack_key: ObjectKey,
    pub entries: Vec<PackEntry>,
}
