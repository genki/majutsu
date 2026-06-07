use anyhow::{Context, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TieringRule {
    pub name: String,
    pub prefix: String,
    pub after_days: Option<u32>,
    pub storage: StorageTier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageTier {
    Standard,
    Infrequent,
    Archive,
    DeepArchive,
}

pub fn is_hot_metadata_prefix(prefix: &str) -> bool {
    prefix.starts_with("hosts/")
        || prefix.starts_with("metadata/")
        || prefix.starts_with("trees/")
        || prefix.starts_with("large/manifests/")
        || prefix.starts_with("indexes/")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyConfig {
    pub enabled: bool,
    pub rules: Vec<PolicyRule>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyRule {
    pub name: String,
    pub prefix: String,
    pub after: Option<String>,
    pub storage: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransitionRule {
    name: String,
    prefix: String,
    after_days: u32,
    storage: String,
}

pub fn gcs_lifecycle_policy(tiering: &PolicyConfig) -> Result<serde_json::Value> {
    let mut rules = Vec::new();
    if tiering.enabled {
        for rule in transition_tiering_rules(tiering)? {
            rules.push(serde_json::json!({
                "action": {
                    "type": "SetStorageClass",
                    "storageClass": gcs_storage_class(&rule.storage)
                },
                "condition": {
                    "age": rule.after_days,
                    "matchesPrefix": [rule.prefix]
                }
            }));
        }
    }
    Ok(serde_json::json!({ "rule": rules }))
}

pub fn s3_lifecycle_policy(tiering: &PolicyConfig) -> Result<serde_json::Value> {
    let mut rules = Vec::new();
    if tiering.enabled {
        for rule in transition_tiering_rules(tiering)? {
            rules.push(serde_json::json!({
                "ID": sanitize_lifecycle_rule_id(&rule.name),
                "Status": "Enabled",
                "Filter": { "Prefix": rule.prefix },
                "Transitions": [
                    {
                        "Days": rule.after_days,
                        "StorageClass": s3_storage_class(&rule.storage)
                    }
                ]
            }));
        }
    }
    Ok(serde_json::json!({ "Rules": rules }))
}

fn transition_tiering_rules(tiering: &PolicyConfig) -> Result<Vec<TransitionRule>> {
    let mut out = Vec::new();
    for rule in &tiering.rules {
        let Some(after) = &rule.after else {
            continue;
        };
        let Some(storage) = &rule.storage else {
            continue;
        };
        if is_hot_storage(storage) {
            continue;
        }
        out.push(TransitionRule {
            name: rule.name.clone(),
            prefix: rule.prefix.clone(),
            after_days: parse_days(after)?,
            storage: storage.clone(),
        });
    }
    Ok(out)
}

fn parse_days(input: &str) -> Result<u32> {
    let trimmed = input.trim();
    if let Some(days) = trimmed.strip_suffix('d') {
        return days
            .parse::<u32>()
            .with_context(|| format!("invalid tiering duration: {input}"));
    }
    trimmed
        .parse::<u32>()
        .with_context(|| format!("invalid tiering duration: {input}"))
}

fn is_hot_storage(storage: &str) -> bool {
    matches!(
        normalize_storage_name(storage).as_str(),
        "standard" | "hot" | "keep" | "none"
    )
}

fn gcs_storage_class(storage: &str) -> &'static str {
    match normalize_storage_name(storage).as_str() {
        "infrequent" | "ia" | "nearline" => "NEARLINE",
        "coldline" => "COLDLINE",
        "archive" | "archive-instant" | "deep-archive" => "ARCHIVE",
        _ => "STANDARD",
    }
}

fn s3_storage_class(storage: &str) -> &'static str {
    match normalize_storage_name(storage).as_str() {
        "infrequent" | "ia" | "standard-ia" => "STANDARD_IA",
        "onezone-ia" => "ONEZONE_IA",
        "archive-instant" | "glacier-ir" => "GLACIER_IR",
        "archive" | "glacier" => "GLACIER",
        "deep-archive" => "DEEP_ARCHIVE",
        _ => "STANDARD",
    }
}

fn normalize_storage_name(storage: &str) -> String {
    storage.trim().to_ascii_lowercase().replace('_', "-")
}

fn sanitize_lifecycle_rule_id(name: &str) -> String {
    let sanitized = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "majutsu-tiering-rule".into()
    } else {
        sanitized
    }
}
