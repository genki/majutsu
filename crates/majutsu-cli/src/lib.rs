use anyhow::{Context, Result, anyhow, bail};

pub const BIN_NAME: &str = "mj";

pub fn command_name() -> &'static str {
    BIN_NAME
}

pub fn parse_byte_size(input: &str) -> Result<u64> {
    let normalized = input.trim().replace('_', "");
    if normalized.is_empty() {
        bail!("size must not be empty");
    }
    if let Ok(value) = normalized.parse::<u64>() {
        return Ok(value);
    }
    let split_at = normalized
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .ok_or_else(|| anyhow!("size is missing a unit: {input}"))?;
    let number = normalized[..split_at].trim();
    let unit = normalized[split_at..]
        .trim()
        .replace(' ', "")
        .to_ascii_lowercase();
    let value: f64 = number
        .parse()
        .with_context(|| format!("invalid size number: {input}"))?;
    if !value.is_finite() || value < 0.0 {
        bail!("invalid size number: {input}");
    }
    let multiplier = match unit.as_str() {
        "b" | "byte" | "bytes" => 1.0,
        "k" | "kb" => 1_000.0,
        "m" | "mb" => 1_000_000.0,
        "g" | "gb" => 1_000_000_000.0,
        "t" | "tb" => 1_000_000_000_000.0,
        "kib" => 1024.0,
        "mib" => 1024.0 * 1024.0,
        "gib" => 1024.0 * 1024.0 * 1024.0,
        "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => bail!("unsupported size unit in {input}"),
    };
    let bytes = value * multiplier;
    if bytes > u64::MAX as f64 {
        bail!("size is too large: {input}");
    }
    Ok(bytes.round() as u64)
}

pub fn parse_duration_millis(input: &str) -> Result<u64> {
    let normalized = input.trim().replace('_', "");
    if normalized.is_empty() {
        bail!("duration must not be empty");
    }
    if let Ok(value) = normalized.parse::<u64>() {
        return Ok(value);
    }
    let split_at = normalized
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .ok_or_else(|| anyhow!("duration is missing a unit: {input}"))?;
    let number = normalized[..split_at].trim();
    let unit = normalized[split_at..]
        .trim()
        .replace(' ', "")
        .to_ascii_lowercase();
    let value: f64 = number
        .parse()
        .with_context(|| format!("invalid duration number: {input}"))?;
    if !value.is_finite() || value < 0.0 {
        bail!("invalid duration number: {input}");
    }
    let multiplier = match unit.as_str() {
        "ms" | "millisecond" | "milliseconds" => 1.0,
        "s" | "sec" | "secs" | "second" | "seconds" => 1000.0,
        "m" | "min" | "mins" | "minute" | "minutes" => 60_000.0,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3_600_000.0,
        "d" | "day" | "days" => 86_400_000.0,
        _ => bail!("unsupported duration unit in {input}"),
    };
    let millis = value * multiplier;
    if millis > u64::MAX as f64 {
        bail!("duration is too large: {input}");
    }
    Ok(millis.round() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_byte_sizes() {
        assert_eq!(parse_byte_size("64").unwrap(), 64);
        assert_eq!(parse_byte_size("4 B").unwrap(), 4);
        assert_eq!(parse_byte_size("1 KiB").unwrap(), 1024);
        assert_eq!(parse_byte_size("16 MiB").unwrap(), 16 * 1024 * 1024);
        assert_eq!(parse_byte_size("1.5 MB").unwrap(), 1_500_000);
        assert_eq!(parse_byte_size("1_024").unwrap(), 1024);
    }

    #[test]
    fn rejects_invalid_byte_sizes() {
        assert!(parse_byte_size("").is_err());
        assert!(parse_byte_size("-1 B").is_err());
        assert!(parse_byte_size("1 XB").is_err());
    }

    #[test]
    fn parses_duration_millis() {
        assert_eq!(parse_duration_millis("25").unwrap(), 25);
        assert_eq!(parse_duration_millis("25ms").unwrap(), 25);
        assert_eq!(parse_duration_millis("1.5s").unwrap(), 1500);
        assert_eq!(parse_duration_millis("2 min").unwrap(), 120_000);
        assert_eq!(parse_duration_millis("1h").unwrap(), 3_600_000);
        assert_eq!(parse_duration_millis("1_000ms").unwrap(), 1000);
    }

    #[test]
    fn rejects_invalid_durations() {
        assert!(parse_duration_millis("").is_err());
        assert!(parse_duration_millis("-1s").is_err());
        assert!(parse_duration_millis("1 fortnight").is_err());
    }
}
