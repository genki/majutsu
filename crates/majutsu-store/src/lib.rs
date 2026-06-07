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

impl RemoteCapabilities {
    pub fn file() -> Self {
        Self {
            lifecycle_rules: false,
            object_tags: false,
            storage_class_on_put: false,
            restore_archived_object: true,
            multipart_upload: false,
            range_get: true,
            conditional_put: true,
        }
    }

    pub fn s3(signature_v2: bool, multipart_enabled: bool) -> Self {
        Self {
            lifecycle_rules: true,
            object_tags: !signature_v2,
            storage_class_on_put: !signature_v2,
            restore_archived_object: true,
            multipart_upload: multipart_enabled && !signature_v2,
            range_get: true,
            conditional_put: !signature_v2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_remote_supports_local_recovery_capabilities() {
        let capabilities = RemoteCapabilities::file();

        assert!(!capabilities.lifecycle_rules);
        assert!(!capabilities.object_tags);
        assert!(!capabilities.storage_class_on_put);
        assert!(capabilities.restore_archived_object);
        assert!(!capabilities.multipart_upload);
        assert!(capabilities.range_get);
        assert!(capabilities.conditional_put);
    }

    #[test]
    fn s3_v4_supports_policy_tags_multipart_and_conditional_put() {
        let capabilities = RemoteCapabilities::s3(false, true);

        assert!(capabilities.lifecycle_rules);
        assert!(capabilities.object_tags);
        assert!(capabilities.storage_class_on_put);
        assert!(capabilities.restore_archived_object);
        assert!(capabilities.multipart_upload);
        assert!(capabilities.range_get);
        assert!(capabilities.conditional_put);
    }

    #[test]
    fn s3_v2_disables_unsigned_capabilities() {
        let capabilities = RemoteCapabilities::s3(true, true);

        assert!(capabilities.lifecycle_rules);
        assert!(!capabilities.object_tags);
        assert!(!capabilities.storage_class_on_put);
        assert!(capabilities.restore_archived_object);
        assert!(!capabilities.multipart_upload);
        assert!(capabilities.range_get);
        assert!(!capabilities.conditional_put);
    }

    #[test]
    fn s3_multipart_follows_large_object_policy() {
        assert!(!RemoteCapabilities::s3(false, false).multipart_upload);
    }
}
