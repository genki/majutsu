use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Utc};
use std::collections::BTreeSet;

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
pub struct SnapshotPruneInput {
    pub id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotPrunePlan {
    pub keep: Vec<String>,
    pub delete: Vec<String>,
}

pub fn build_snapshot_prune_plan(
    snapshots_newest_first: &[SnapshotPruneInput],
    current: Option<&str>,
    keep_daily: u32,
    keep_monthly: u32,
) -> SnapshotPrunePlan {
    let mut keep = BTreeSet::new();
    if let Some(current) = current {
        keep.insert(current.to_string());
    }
    let mut daily = BTreeSet::new();
    let mut monthly = BTreeSet::new();
    for snapshot in snapshots_newest_first {
        let day = format!(
            "{:04}-{:02}-{:02}",
            snapshot.created_at.year(),
            snapshot.created_at.month(),
            snapshot.created_at.day()
        );
        if daily.len() < keep_daily as usize && daily.insert(day) {
            keep.insert(snapshot.id.clone());
        }
        let month = format!(
            "{:04}-{:02}",
            snapshot.created_at.year(),
            snapshot.created_at.month()
        );
        if monthly.len() < keep_monthly as usize && monthly.insert(month) {
            keep.insert(snapshot.id.clone());
        }
    }
    let keep = keep.into_iter().collect::<Vec<_>>();
    let delete = snapshots_newest_first
        .iter()
        .map(|snapshot| snapshot.id.clone())
        .filter(|id| !keep.binary_search(id).is_ok())
        .collect::<Vec<_>>();
    SnapshotPrunePlan { keep, delete }
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

pub fn default_tiering_rules() -> Vec<PolicyRule> {
    vec![
        PolicyRule {
            name: "keep-host-metadata-hot".into(),
            prefix: "hosts/".into(),
            after: None,
            storage: Some("standard".into()),
        },
        PolicyRule {
            name: "keep-bootstrap-metadata-hot".into(),
            prefix: "metadata/".into(),
            after: None,
            storage: Some("standard".into()),
        },
        PolicyRule {
            name: "keep-trees-hot".into(),
            prefix: "trees/".into(),
            after: None,
            storage: Some("standard".into()),
        },
        PolicyRule {
            name: "keep-large-manifests-hot".into(),
            prefix: "large/manifests/".into(),
            after: None,
            storage: Some("standard".into()),
        },
        PolicyRule {
            name: "keep-indexes-hot".into(),
            prefix: "indexes/".into(),
            after: None,
            storage: Some("standard".into()),
        },
        PolicyRule {
            name: "packs-to-ia".into(),
            prefix: "packs/normal/".into(),
            after: Some("30d".into()),
            storage: Some("infrequent".into()),
        },
        PolicyRule {
            name: "fixed-large-chunks-to-archive".into(),
            prefix: "large/chunks/fixed-8m/".into(),
            after: Some("180d".into()),
            storage: Some("archive".into()),
        },
        PolicyRule {
            name: "fastcdc-large-chunks-to-archive".into(),
            prefix: "large/chunks/fastcdc/".into(),
            after: Some("180d".into()),
            storage: Some("archive".into()),
        },
    ]
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

pub fn s3_lifecycle_configuration_xml(policy: &serde_json::Value) -> Result<String> {
    let rules = policy
        .get("Rules")
        .and_then(|rules| rules.as_array())
        .context("S3 lifecycle policy must contain Rules array")?;
    let mut out = String::from("<LifecycleConfiguration>");
    for rule in rules {
        let id = rule
            .get("ID")
            .and_then(|value| value.as_str())
            .context("S3 lifecycle rule is missing ID")?;
        let status = rule
            .get("Status")
            .and_then(|value| value.as_str())
            .context("S3 lifecycle rule is missing Status")?;
        let prefix = rule
            .pointer("/Filter/Prefix")
            .and_then(|value| value.as_str())
            .context("S3 lifecycle rule is missing Filter.Prefix")?;
        let transition = rule
            .get("Transitions")
            .and_then(|value| value.as_array())
            .and_then(|values| values.first())
            .context("S3 lifecycle rule is missing first Transition")?;
        let days = transition
            .get("Days")
            .and_then(|value| value.as_u64())
            .context("S3 lifecycle transition is missing Days")?;
        let storage_class = transition
            .get("StorageClass")
            .and_then(|value| value.as_str())
            .context("S3 lifecycle transition is missing StorageClass")?;
        out.push_str("<Rule>");
        out.push_str("<ID>");
        out.push_str(&xml_escape(id));
        out.push_str("</ID>");
        out.push_str("<Status>");
        out.push_str(&xml_escape(status));
        out.push_str("</Status>");
        out.push_str("<Filter><Prefix>");
        out.push_str(&xml_escape(prefix));
        out.push_str("</Prefix></Filter>");
        out.push_str("<Transition>");
        out.push_str(&format!("<Days>{days}</Days>"));
        out.push_str("<StorageClass>");
        out.push_str(&xml_escape(storage_class));
        out.push_str("</StorageClass>");
        out.push_str("</Transition>");
        out.push_str("</Rule>");
    }
    out.push_str("</LifecycleConfiguration>");
    Ok(out)
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
        if is_hot_storage(storage) || is_hot_metadata_prefix(&rule.prefix) {
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

fn xml_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn default_rules_keep_metadata_hot_and_tier_large_payloads() {
        let rules = default_tiering_rules();

        assert!(
            rules
                .iter()
                .any(|rule| rule.prefix == "hosts/" && rule.after.is_none())
        );
        assert!(
            rules.iter().any(|rule| rule.prefix == "large/manifests/"
                && rule.storage.as_deref() == Some("standard"))
        );
        assert!(
            rules
                .iter()
                .any(|rule| rule.prefix == "packs/normal/" && rule.after.as_deref() == Some("30d"))
        );
        assert!(
            rules
                .iter()
                .any(|rule| rule.prefix == "large/chunks/fixed-8m/"
                    && rule.storage.as_deref() == Some("archive"))
        );
    }

    #[test]
    fn lifecycle_policies_skip_hot_metadata_defaults() {
        let config = PolicyConfig {
            enabled: true,
            rules: default_tiering_rules(),
        };

        let s3 = s3_lifecycle_policy(&config).unwrap();
        let s3_text = serde_json::to_string(&s3).unwrap();
        assert!(s3_text.contains("packs/normal/"));
        assert!(s3_text.contains("large/chunks/fixed-8m/"));
        assert!(!s3_text.contains("hosts/"));
        assert!(!s3_text.contains("large/manifests/"));

        let gcs = gcs_lifecycle_policy(&config).unwrap();
        let gcs_text = serde_json::to_string(&gcs).unwrap();
        assert!(gcs_text.contains("large/chunks/fastcdc/"));
        assert!(!gcs_text.contains("metadata/"));
        assert!(!gcs_text.contains("trees/"));
    }

    #[test]
    fn lifecycle_policies_reject_custom_hot_metadata_transitions() {
        let config = PolicyConfig {
            enabled: true,
            rules: vec![
                PolicyRule {
                    name: "bad-hosts-to-archive".into(),
                    prefix: "hosts/".into(),
                    after: Some("1d".into()),
                    storage: Some("archive".into()),
                },
                PolicyRule {
                    name: "bad-trees-to-archive".into(),
                    prefix: "trees/".into(),
                    after: Some("1d".into()),
                    storage: Some("deep-archive".into()),
                },
                PolicyRule {
                    name: "large-ok".into(),
                    prefix: "large/chunks/fixed-8m/".into(),
                    after: Some("30d".into()),
                    storage: Some("archive".into()),
                },
            ],
        };

        let s3_text = serde_json::to_string(&s3_lifecycle_policy(&config).unwrap()).unwrap();
        assert!(s3_text.contains("large/chunks/fixed-8m/"));
        assert!(!s3_text.contains("hosts/"));
        assert!(!s3_text.contains("trees/"));

        let gcs_text = serde_json::to_string(&gcs_lifecycle_policy(&config).unwrap()).unwrap();
        assert!(gcs_text.contains("large/chunks/fixed-8m/"));
        assert!(!gcs_text.contains("hosts/"));
        assert!(!gcs_text.contains("trees/"));
    }

    #[test]
    fn s3_lifecycle_policy_can_render_rest_xml() {
        let config = PolicyConfig {
            enabled: true,
            rules: vec![PolicyRule {
                name: "packs-&-large".into(),
                prefix: "objects/packs/&normal/".into(),
                after: Some("14d".into()),
                storage: Some("infrequent".into()),
            }],
        };

        let policy = s3_lifecycle_policy(&config).unwrap();
        let xml = s3_lifecycle_configuration_xml(&policy).unwrap();

        assert!(xml.starts_with("<LifecycleConfiguration>"));
        assert!(xml.contains("<ID>packs---large</ID>"));
        assert!(xml.contains("<Prefix>objects/packs/&amp;normal/</Prefix>"));
        assert!(xml.contains("<Days>14</Days>"));
        assert!(xml.contains("<StorageClass>STANDARD_IA</StorageClass>"));
    }

    #[test]
    fn snapshot_prune_plan_keeps_current_daily_and_monthly_snapshots() {
        let snapshots = vec![
            SnapshotPruneInput {
                id: "snap-4".into(),
                created_at: Utc.with_ymd_and_hms(2026, 6, 7, 12, 0, 0).unwrap(),
            },
            SnapshotPruneInput {
                id: "snap-3".into(),
                created_at: Utc.with_ymd_and_hms(2026, 6, 6, 12, 0, 0).unwrap(),
            },
            SnapshotPruneInput {
                id: "snap-2".into(),
                created_at: Utc.with_ymd_and_hms(2026, 5, 30, 12, 0, 0).unwrap(),
            },
            SnapshotPruneInput {
                id: "snap-1".into(),
                created_at: Utc.with_ymd_and_hms(2026, 5, 1, 12, 0, 0).unwrap(),
            },
        ];

        let plan = build_snapshot_prune_plan(&snapshots, Some("snap-1"), 2, 1);

        assert_eq!(
            plan.keep,
            vec![
                "snap-1".to_string(),
                "snap-3".to_string(),
                "snap-4".to_string(),
            ]
        );
        assert_eq!(plan.delete, vec!["snap-2".to_string()]);
    }

    #[test]
    fn snapshot_prune_plan_can_delete_everything_except_current_when_limits_are_zero() {
        let snapshots = vec![
            SnapshotPruneInput {
                id: "snap-2".into(),
                created_at: Utc.with_ymd_and_hms(2026, 6, 7, 12, 0, 0).unwrap(),
            },
            SnapshotPruneInput {
                id: "snap-1".into(),
                created_at: Utc.with_ymd_and_hms(2026, 6, 6, 12, 0, 0).unwrap(),
            },
        ];

        let plan = build_snapshot_prune_plan(&snapshots, Some("snap-1"), 0, 0);

        assert_eq!(plan.keep, vec!["snap-1".to_string()]);
        assert_eq!(plan.delete, vec!["snap-2".to_string()]);
    }
}
