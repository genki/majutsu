use std::ops::Range;

use majutsu_core::ObjectKey;

pub trait ObjectStore {
    type Error;

    fn put(&self, key: &ObjectKey, body: &[u8]) -> Result<(), Self::Error>;
    fn get(&self, key: &ObjectKey) -> Result<Vec<u8>, Self::Error>;
    fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<Vec<u8>, Self::Error>;
    fn exists(&self, key: &ObjectKey) -> Result<bool, Self::Error>;
    fn delete(&self, key: &ObjectKey) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteCapabilities {
    pub lifecycle_rules: bool,
    pub object_tags: bool,
    pub storage_class_on_put: bool,
    pub restore_archived_object: bool,
    pub multipart_upload: bool,
    pub range_get: bool,
    pub conditional_put: bool,
}
