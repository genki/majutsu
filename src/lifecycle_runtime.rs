use anyhow::{Result, anyhow, bail};
use chrono::Utc;

use crate::cli::LifecycleCommand;
use crate::config::{Config, Paths, policy_config, read_config};
use crate::remote_store::{RemoteStore, open_remote_with_upload_policy};

pub(crate) fn lifecycle_cmd(paths: &Paths, command: LifecycleCommand) -> Result<()> {
    crate::ensure_ready(paths)?;
    let config = read_config(paths)?;
    match command {
        LifecycleCommand::Policy { provider } => {
            let policy = lifecycle_policy_for_provider(&config, &provider)?;
            println!("{}", serde_json::to_string_pretty(&policy)?);
        }
        LifecycleCommand::Status => {
            let remote_config = config
                .remote
                .as_ref()
                .ok_or_else(|| anyhow!("remote is not configured; run `mj init --remote ...`"))?;
            let remote = open_remote_with_upload_policy(
                remote_config,
                config.large.multipart,
                config.large.max_parallel_uploads,
            )?;
            let capabilities = remote.capabilities();
            println!("remote {}", remote.describe());
            println!("tiering_enabled {}", config.tiering.enabled);
            println!("lifecycle_rules {}", capabilities.lifecycle_rules);
            println!("object_tags {}", capabilities.object_tags);
            println!("storage_class_on_put {}", capabilities.storage_class_on_put);
            println!("policy_rules_s3 {}", lifecycle_rule_count(&config, "s3")?);
            println!("policy_rules_gcs {}", lifecycle_rule_count(&config, "gcs")?);
        }
        LifecycleCommand::Apply { provider, dry_run } => {
            let remote_config = config
                .remote
                .as_ref()
                .ok_or_else(|| anyhow!("remote is not configured; run `mj init --remote ...`"))?;
            let remote = open_remote_with_upload_policy(
                remote_config,
                config.large.multipart,
                config.large.max_parallel_uploads,
            )?;
            let policy = lifecycle_policy_for_provider(&config, &provider)?;
            let policy_json = serde_json::to_vec_pretty(&policy)?;
            println!("remote {}", remote.describe());
            println!("provider {}", normalize_lifecycle_provider(&provider)?);
            println!("policy_bytes {}", policy_json.len());
            println!("dry_run {dry_run}");
            if dry_run {
                print_lifecycle_apply_hint(&remote, &provider)?;
                println!("{}", String::from_utf8(policy_json)?);
            } else {
                let provider = normalize_lifecycle_provider(&provider)?;
                let provider_applied = if provider == "s3" {
                    remote.apply_s3_lifecycle_policy(&policy)?
                } else {
                    false
                };
                let policy_key = format!("lifecycle/policy-{provider}.json");
                let status_key = "lifecycle/status.json";
                remote.put(&policy_key, &policy_json)?;
                let status = serde_json::json!({
                    "provider": provider.clone(),
                    "remote": remote.describe(),
                    "policy_key": policy_key.clone(),
                    "provider_applied": provider_applied,
                    "applied_at": Utc::now().to_rfc3339(),
                    "note": "desired lifecycle policy artifact stored by majutsu"
                });
                remote.put(status_key, &serde_json::to_vec_pretty(&status)?)?;
                println!("policy_key {policy_key}");
                println!("status_key {status_key}");
                println!("provider_applied {provider_applied}");
                println!("applied true");
            }
        }
    }
    Ok(())
}

fn lifecycle_policy_for_provider(config: &Config, provider: &str) -> Result<serde_json::Value> {
    match normalize_lifecycle_provider(provider)?.as_str() {
        "gcs" => majutsu_policy::gcs_lifecycle_policy(&policy_config(&config.tiering)),
        "s3" => majutsu_policy::s3_lifecycle_policy(&policy_config(&config.tiering)),
        other => bail!("unsupported lifecycle provider: {other}"),
    }
}

fn lifecycle_rule_count(config: &Config, provider: &str) -> Result<usize> {
    let policy = lifecycle_policy_for_provider(config, provider)?;
    let key = if normalize_lifecycle_provider(provider)? == "s3" {
        "Rules"
    } else {
        "rule"
    };
    Ok(policy
        .get(key)
        .and_then(|rules| rules.as_array())
        .map(Vec::len)
        .unwrap_or(0))
}

fn normalize_lifecycle_provider(provider: &str) -> Result<String> {
    match provider {
        "aws" | "s3" => Ok("s3".into()),
        "gcs" => Ok("gcs".into()),
        other => bail!("unsupported lifecycle provider: {other}"),
    }
}

fn print_lifecycle_apply_hint(remote: &RemoteStore, provider: &str) -> Result<()> {
    let provider = normalize_lifecycle_provider(provider)?;
    match provider.as_str() {
        "s3" => {
            println!(
                "apply_hint aws s3api put-bucket-lifecycle-configuration --bucket <bucket> --lifecycle-configuration file://policy.json"
            );
            if !remote.capabilities().lifecycle_rules {
                println!("apply_warning remote does not advertise lifecycle rule support");
            }
        }
        "gcs" => {
            println!(
                "apply_hint gcloud storage buckets update gs://<bucket> --lifecycle-file=policy.json"
            );
        }
        _ => unreachable!(),
    }
    Ok(())
}
