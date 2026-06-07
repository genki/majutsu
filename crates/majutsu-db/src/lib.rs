pub const ROOTS_TABLE: &str = "roots";
pub const SNAPSHOTS_TABLE: &str = "snapshots";
pub const OPERATIONS_TABLE: &str = "operations";
pub const BLOBS_TABLE: &str = "blobs";
pub const LARGE_OBJECTS_TABLE: &str = "large_objects";
pub const CHUNKS_TABLE: &str = "chunks";
pub const UPLOAD_QUEUE_TABLE: &str = "upload_queue";
pub const RESTORE_QUEUE_TABLE: &str = "restore_queue";
pub const REFS_TABLE: &str = "refs";
pub const PACKS_TABLE: &str = "packs";
pub const LARGE_PINS_TABLE: &str = "large_pins";
pub const REMOTE_REFS_TABLE: &str = "remote_refs";

pub const SCHEMA_SQL: &str = "
create table if not exists roots(id text primary key, data_json text not null);
create table if not exists snapshots(
  id text primary key,
  parent_id text,
  op_id text not null,
  created_at text not null,
  manifest_key text not null,
  manifest_json text not null
);
create table if not exists operations(
  id text primary key,
  parent_op text,
  kind text not null,
  actor text not null default 'local',
  status text not null default 'done',
  before_snapshot text,
  after_snapshot text,
  created_at text not null,
  message text
);
create table if not exists refs(name text primary key, value text not null);
create table if not exists blobs(oid text primary key, size integer not null, object_key text not null);
create table if not exists packs(pack_id text primary key, pack_key text not null, index_key text not null, object_count integer not null, size integer not null);
create table if not exists large_objects(oid text primary key, size integer not null, chunk_count integer not null, manifest_key text not null);
create table if not exists chunks(oid text primary key, size integer not null, object_key text not null);
create table if not exists large_pins(oid text primary key, pinned_at text not null, reason text);
create table if not exists remote_refs(
  remote text not null,
  name text not null,
  value text not null,
  observed_at text not null,
  primary key(remote, name)
);
";

pub const COMPAT_MIGRATIONS: &[&str] = &[
    "alter table blobs add column pack_id text",
    "alter table blobs add column pack_offset integer",
    "alter table blobs add column pack_len integer",
    "alter table operations add column parent_op text",
    "alter table operations add column actor text not null default 'local'",
    "alter table operations add column status text not null default 'done'",
];

pub fn schema_sql() -> &'static str {
    SCHEMA_SQL
}

pub fn compat_migrations() -> &'static [&'static str] {
    COMPAT_MIGRATIONS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_defines_required_spec_tables() {
        for table in [
            ROOTS_TABLE,
            SNAPSHOTS_TABLE,
            OPERATIONS_TABLE,
            BLOBS_TABLE,
            PACKS_TABLE,
            LARGE_OBJECTS_TABLE,
            CHUNKS_TABLE,
            LARGE_PINS_TABLE,
            REFS_TABLE,
            REMOTE_REFS_TABLE,
        ] {
            assert!(
                SCHEMA_SQL.contains(&format!("create table if not exists {table}")),
                "schema should define {table}"
            );
        }
    }

    #[test]
    fn schema_preserves_operation_log_columns() {
        for column in [
            "parent_op",
            "kind text not null",
            "actor text not null default 'local'",
            "status text not null default 'done'",
            "before_snapshot",
            "after_snapshot",
            "created_at text not null",
        ] {
            assert!(
                SCHEMA_SQL.contains(column),
                "missing operation column {column}"
            );
        }
    }

    #[test]
    fn compat_migrations_cover_existing_legacy_columns() {
        assert!(COMPAT_MIGRATIONS.iter().any(|sql| sql.contains("pack_id")));
        assert!(
            COMPAT_MIGRATIONS
                .iter()
                .any(|sql| sql.contains("parent_op"))
        );
        assert!(COMPAT_MIGRATIONS.iter().any(|sql| sql.contains("actor")));
        assert!(COMPAT_MIGRATIONS.iter().any(|sql| sql.contains("status")));
    }
}
