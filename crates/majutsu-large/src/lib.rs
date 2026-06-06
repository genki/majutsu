use majutsu_core::ObjectKey;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Chunking {
    Fixed { size: usize },
    FastCdc { average_size: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargeObjectPointer {
    pub version: u32,
    pub oid: String,
    pub size: u64,
    pub binary: bool,
    pub chunking: Chunking,
    pub chunks_manifest: ObjectKey,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargeChunkRef {
    pub index: usize,
    pub offset: u64,
    pub len: u64,
    pub object_key: ObjectKey,
}
