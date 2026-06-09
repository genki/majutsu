use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, params};
use std::fmt::Write as _;
use std::fs;
use std::io::{self, IsTerminal};
#[cfg(unix)]
use std::mem;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::Path;
use std::process::{Command, Stdio};
use walkdir::WalkDir;

use crate::cli::{DiffArgs, LogArgs, OpCommand};
use crate::config::{Config, Paths, read_config};
use crate::operation_log::{query_operation, record_op};
use crate::queue_runtime::{event_journal_records, upload_queue_stats};
use crate::remote_store::open_remote;
use crate::root_state::roots;
use crate::snapshot_state::{
    current_snapshot, load_snapshot_by_id, snapshot_contains_root, snapshot_file_map,
    snapshot_id_at,
};

pub(crate) fn status_cmd(paths: &Paths) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    let config = read_config(paths)?;
    let roots = roots(&conn)?;
    let current = current_snapshot(&conn)?;
    let current_label = current.as_deref().unwrap_or("(none)");
    let remote = read_remote_status(&config)?;
    let db_stats = read_status_db_stats(&conn)?;
    let storage = read_storage_stats(paths)?;
    let upload_stats = upload_queue_stats(paths)?;
    let event_count = event_journal_records(paths)?.len();
    let restore_queue_count = count_json_files(&paths.home.join("queue/restores"))?;
    let width = terminal_width();
    let height = terminal_height();
    let ui = StatusUi::new();
    let mut output = String::new();

    writeln!(output, "{}", ui.heading("Status")).expect("write status output");
    print_kv(&mut output, width, "Current snapshot", current_label);
    print_kv(&mut output, width, "Roots", &roots.len().to_string());
    print_kv(&mut output, width, "Remote", remote.summary());
    print_kv(
        &mut output,
        width,
        "Queued uploads",
        &upload_stats.total.to_string(),
    );
    print_kv(
        &mut output,
        width,
        "State usage",
        &format_bytes(storage.state_bytes),
    );
    writeln!(output).expect("write status output");

    print_status_overview(
        &mut output,
        width,
        &ui,
        StatusOverview {
            current: current_label,
            roots_total: roots.len(),
            roots_active: roots.iter().filter(|root| root.status == "active").count(),
            roots_problem: roots
                .iter()
                .filter(|root| root.status != "active" || !root.path.exists())
                .count(),
            remote: &remote,
            upload_total: upload_stats.total,
            upload_retrying: upload_stats.retrying,
            upload_delayed: upload_stats.delayed,
            upload_backpressure: upload_stats.has_backpressure(),
            encryption: config.security.encryption.as_str(),
            state_bytes: storage.state_bytes,
            object_bytes: storage.objects_bytes,
            queue_bytes: storage.queue_bytes,
            blob_bytes: db_stats.blob_bytes as u64,
            pack_bytes: db_stats.pack_bytes as u64,
            chunk_bytes: db_stats.chunk_bytes as u64,
        },
    );
    writeln!(output).expect("write status output");

    writeln!(output, "{}", ui.heading("Host")).expect("write status output");
    print_kv(&mut output, width, "Name", &config.host.name);
    print_kv(&mut output, width, "ID", &config.host.id);
    print_kv(
        &mut output,
        width,
        "Home",
        &paths.home.display().to_string(),
    );
    print_kv(
        &mut output,
        width,
        "Config",
        &paths.config.display().to_string(),
    );
    print_kv(
        &mut output,
        width,
        "Database",
        &paths.db.display().to_string(),
    );
    writeln!(output).expect("write status output");

    print_remote_section(&mut output, width, &ui, &remote);
    writeln!(output).expect("write status output");

    writeln!(output, "{}", ui.heading("Configuration")).expect("write status output");
    print_table(
        &mut output,
        width,
        &["AREA", "SETTING", "VALUE"],
        &[
            [
                "security",
                "encryption",
                config.security.encryption.as_str(),
            ],
            ["security", "hash", config.security.hash.as_str()],
            ["watch", "backend", config.watch.backend.as_str()],
            ["watch", "mode", config.watch.mode.as_str()],
            [
                "watch",
                "debounce",
                &format!("{} ms", config.watch.debounce),
            ],
            ["watch", "settle", &format!("{} ms", config.watch.settle)],
            ["large", "enabled", &config.large.enabled.to_string()],
            ["large", "min-size", &format_bytes(config.large.min_size)],
            [
                "large",
                "binary-min-size",
                &format_bytes(config.large.binary_min_size),
            ],
            ["large", "chunking", config.large.default_chunking.as_str()],
            [
                "large",
                "chunk-size",
                &format_bytes(config.large.chunk_size as u64),
            ],
            ["large", "multipart", &config.large.multipart.to_string()],
            [
                "pack",
                "small-target",
                &format_bytes(config.pack.small_pack_target),
            ],
            [
                "pack",
                "normal-target",
                &format_bytes(config.pack.normal_pack_target),
            ],
        ],
    );
    writeln!(output).expect("write status output");

    writeln!(output, "{}", ui.heading("Roots")).expect("write status output");
    if roots.is_empty() {
        writeln!(output, "  (none)").expect("write status output");
    } else {
        let (id_width, status_width) = root_table_widths(width);
        writeln!(
            output,
            "  {:<id_width$} {:<status_width$} PATH",
            "ID",
            "STATUS",
            id_width = id_width,
            status_width = status_width
        )
        .expect("write status output");
        writeln!(
            output,
            "  {:<id_width$} {:<status_width$} {}",
            "-".repeat(id_width),
            "-".repeat(status_width),
            "-".repeat(4),
            id_width = id_width,
            status_width = status_width
        )
        .expect("write status output");
    }
    for root in &roots {
        let state = if root.status == "active" && !root.path.exists() {
            "missing"
        } else {
            root.status.as_str()
        };
        print_root_row(
            &mut output,
            width,
            &root.id,
            state,
            &root.path.display().to_string(),
        );
    }
    writeln!(output).expect("write status output");

    writeln!(output, "{}", ui.heading("Metadata")).expect("write status output");
    print_table(
        &mut output,
        width,
        &["ITEM", "COUNT", "LOGICAL SIZE"],
        &[
            ["snapshots", &db_stats.snapshots.to_string(), "-"],
            ["operations", &db_stats.operations.to_string(), "-"],
            ["refs", &db_stats.refs.to_string(), "-"],
            [
                "blobs",
                &db_stats.blobs.to_string(),
                &format_bytes(db_stats.blob_bytes as u64),
            ],
            [
                "large objects",
                &db_stats.large_objects.to_string(),
                &format_bytes(db_stats.large_object_bytes as u64),
            ],
            [
                "chunks",
                &db_stats.chunks.to_string(),
                &format_bytes(db_stats.chunk_bytes as u64),
            ],
            [
                "packs",
                &db_stats.packs.to_string(),
                &format_bytes(db_stats.pack_bytes as u64),
            ],
            ["large pins", &db_stats.large_pins.to_string(), "-"],
            ["remote refs", &db_stats.remote_refs.to_string(), "-"],
        ],
    );
    writeln!(output).expect("write status output");

    writeln!(output, "{}", ui.heading("Storage")).expect("write status output");
    print_table(
        &mut output,
        width,
        &["SCOPE", "FILES", "SIZE"],
        &[
            [
                "state",
                &storage.state_files.to_string(),
                &format_bytes(storage.state_bytes),
            ],
            [
                "objects",
                &storage.objects_files.to_string(),
                &format_bytes(storage.objects_bytes),
            ],
            [
                "logs",
                &storage.logs_files.to_string(),
                &format_bytes(storage.logs_bytes),
            ],
            [
                "queue",
                &storage.queue_files.to_string(),
                &format_bytes(storage.queue_bytes),
            ],
        ],
    );
    writeln!(output).expect("write status output");

    writeln!(output, "{}", ui.heading("Queues")).expect("write status output");
    print_table(
        &mut output,
        width,
        &["QUEUE", "ITEMS", "DETAILS"],
        &[
            [
                "uploads",
                &upload_stats.total.to_string(),
                &format!(
                    "retrying={}, delayed={}, attempts={}, max_attempts={}, next_retry={}, backpressure={}",
                    upload_stats.retrying,
                    upload_stats.delayed,
                    upload_stats.attempts,
                    upload_stats.max_attempts,
                    upload_stats
                        .next_retry_after
                        .map(|ts| ts.to_string())
                        .unwrap_or_else(|| "(none)".into()),
                    upload_stats.has_backpressure()
                ),
            ],
            [
                "event journal",
                &event_count.to_string(),
                "pending local observations",
            ],
            [
                "restore jobs",
                &restore_queue_count.to_string(),
                "prepared restore jobs",
            ],
        ],
    );
    writeln!(output).expect("write status output");

    writeln!(output, "Machine").expect("write status output");
    writeln!(output, "current {current_label}").expect("write status output");
    emit_status_output(&output, height)?;
    Ok(())
}

struct StatusUi {
    color: bool,
}

impl StatusUi {
    fn new() -> Self {
        let color = std::env::var_os("NO_COLOR").is_none()
            && std::env::var("MJ_COLOR").as_deref() != Ok("never")
            && (std::env::var("MJ_COLOR").as_deref() == Ok("always")
                || (io::stdout().is_terminal() && std::env::var("TERM").as_deref() != Ok("dumb")));
        Self { color }
    }

    fn heading(&self, value: &str) -> String {
        self.paint(value, "1;36")
    }

    fn severity(&self, value: &str, severity: Severity) -> String {
        let code = match severity {
            Severity::Good => "1;32",
            Severity::Warn => "1;33",
            Severity::Bad => "1;31",
            Severity::Info => "1;34",
        };
        self.paint(value, code)
    }

    fn paint(&self, value: &str, code: &str) -> String {
        if self.color {
            format!("\x1b[{code}m{value}\x1b[0m")
        } else {
            value.to_string()
        }
    }
}

#[derive(Clone, Copy)]
enum Severity {
    Good,
    Warn,
    Bad,
    Info,
}

struct StatusOverview<'a> {
    current: &'a str,
    roots_total: usize,
    roots_active: usize,
    roots_problem: usize,
    remote: &'a RemoteStatus,
    upload_total: usize,
    upload_retrying: usize,
    upload_delayed: usize,
    upload_backpressure: bool,
    encryption: &'a str,
    state_bytes: u64,
    object_bytes: u64,
    queue_bytes: u64,
    blob_bytes: u64,
    pack_bytes: u64,
    chunk_bytes: u64,
}

struct StatusCard {
    title: String,
    state: String,
    detail: String,
    severity: Severity,
}

fn print_status_overview(
    out: &mut String,
    width: usize,
    ui: &StatusUi,
    overview: StatusOverview<'_>,
) {
    writeln!(out, "{}", ui.heading("Overview")).expect("write status output");
    let remote_severity = if !overview.remote.configured {
        Severity::Warn
    } else if overview.remote.open_error.is_some() {
        Severity::Bad
    } else {
        Severity::Good
    };
    let upload_severity = if overview.upload_backpressure {
        Severity::Bad
    } else if overview.upload_total > 0 {
        Severity::Warn
    } else {
        Severity::Good
    };
    let encryption_severity = if overview.encryption == "none" {
        Severity::Warn
    } else {
        Severity::Good
    };
    let root_severity = if overview.roots_problem > 0 {
        Severity::Warn
    } else {
        Severity::Good
    };
    let cards = [
        StatusCard {
            title: "snapshot".into(),
            state: shorten_middle(overview.current, 24),
            detail: "current ref".into(),
            severity: Severity::Info,
        },
        StatusCard {
            title: "roots".into(),
            state: format!("{}/{} active", overview.roots_active, overview.roots_total),
            detail: format!("problem={}", overview.roots_problem),
            severity: root_severity,
        },
        StatusCard {
            title: "remote".into(),
            state: overview.remote.summary().into(),
            detail: overview.remote.backend.clone(),
            severity: remote_severity,
        },
        StatusCard {
            title: "uploads".into(),
            state: if overview.upload_total == 0 {
                "clear".into()
            } else {
                format!("{} queued", overview.upload_total)
            },
            detail: format!(
                "retrying={} delayed={}",
                overview.upload_retrying, overview.upload_delayed
            ),
            severity: upload_severity,
        },
        StatusCard {
            title: "encryption".into(),
            state: overview.encryption.into(),
            detail: if overview.encryption == "none" {
                "unencrypted state".into()
            } else {
                "encrypted state".into()
            },
            severity: encryption_severity,
        },
        StatusCard {
            title: "state".into(),
            state: format_bytes_compact(overview.state_bytes),
            detail: format!("objects {}", format_bytes_compact(overview.object_bytes)),
            severity: Severity::Info,
        },
    ];
    print_card_grid(out, width, ui, &cards);
    print_usage_bars(
        out,
        width,
        &[
            ("state", overview.state_bytes),
            ("objects", overview.object_bytes),
            ("queue", overview.queue_bytes),
        ],
    );
    print_usage_bars(
        out,
        width,
        &[
            ("blobs", overview.blob_bytes),
            ("packs", overview.pack_bytes),
            ("chunks", overview.chunk_bytes),
        ],
    );
}

fn print_card_grid(out: &mut String, width: usize, ui: &StatusUi, cards: &[StatusCard]) {
    let columns = if width >= 108 {
        3
    } else if width >= 74 {
        2
    } else {
        1
    };
    let gap = if columns > 1 { 2 } else { 0 };
    let card_width = ((width.saturating_sub(2 + gap * (columns - 1))) / columns).max(28);
    for (row_index, row) in cards.chunks(columns).enumerate() {
        let first_line = if row_index == 0 { 0 } else { 1 };
        for line_index in first_line..4 {
            write!(out, "  ").expect("write status output");
            for (i, card) in row.iter().enumerate() {
                if i > 0 {
                    write!(out, "{:gap$}", "", gap = gap).expect("write status output");
                }
                let line = card_line(card, card_width, line_index);
                let rendered = if line_index == 1 {
                    color_card_state(ui, card, &line)
                } else {
                    line
                };
                write!(out, "{rendered}").expect("write status output");
            }
            writeln!(out).expect("write status output");
        }
    }
}

fn card_line(card: &StatusCard, width: usize, line_index: usize) -> String {
    let inner = width.saturating_sub(2).max(10);
    match line_index {
        0 => format!("+{}+", "-".repeat(inner)),
        1 => {
            let title = truncate_text(&card.title.to_uppercase(), 10);
            let state_space = inner.saturating_sub(title.len() + 1);
            let state = truncate_text(&card.state, state_space);
            format!("|{title} {state:<state_space$}|")
        }
        2 => {
            let detail = truncate_text(&card.detail, inner);
            format!("|{detail:<inner$}|")
        }
        _ => format!("+{}+", "-".repeat(inner)),
    }
}

fn color_card_state(ui: &StatusUi, card: &StatusCard, line: &str) -> String {
    if !ui.color {
        return line.to_string();
    }
    let trimmed_state = card.state.trim();
    if trimmed_state.is_empty() {
        return line.to_string();
    }
    line.replacen(trimmed_state, &ui.severity(trimmed_state, card.severity), 1)
}

fn print_usage_bars(out: &mut String, width: usize, values: &[(&str, u64)]) {
    let max_value = values
        .iter()
        .map(|(_, value)| *value)
        .max()
        .unwrap_or(0)
        .max(1);
    let label_width = values
        .iter()
        .map(|(label, _)| label.len())
        .max()
        .unwrap_or(6)
        .max(6);
    let value_width = values
        .iter()
        .map(|(_, value)| format_bytes_compact(*value).len())
        .max()
        .unwrap_or(5)
        .max(5);
    let bar_width = width
        .saturating_sub(2 + label_width + 2 + value_width + 3)
        .clamp(8, 32);
    for (label, value) in values {
        let filled = ((*value as f64 / max_value as f64) * bar_width as f64).round() as usize;
        let filled = filled.min(bar_width);
        writeln!(
            out,
            "  {label:<label_width$} [{}{}] {:>value_width$}",
            "#".repeat(filled),
            "-".repeat(bar_width - filled),
            format_bytes_compact(*value),
            label_width = label_width,
            value_width = value_width
        )
        .expect("write status output");
    }
}

fn truncate_text(value: &str, width: usize) -> String {
    if value.len() <= width {
        return value.to_string();
    }
    if width <= 1 {
        return value.chars().take(width).collect();
    }
    let mut out = value.chars().take(width - 1).collect::<String>();
    out.push('~');
    out
}

fn shorten_middle(value: &str, width: usize) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() <= width {
        return value.to_string();
    }
    if width <= 3 {
        return truncate_text(value, width);
    }
    let prefix = (width - 1) / 2;
    let suffix = width - 1 - prefix;
    let left = chars.iter().take(prefix).collect::<String>();
    let right = chars
        .iter()
        .skip(chars.len().saturating_sub(suffix))
        .collect::<String>();
    format!("{left}~{right}")
}

fn print_kv(out: &mut String, width: usize, key: &str, value: &str) {
    let prefix = format!("  {key:<18} ");
    print_wrapped(out, &prefix, value, width);
}

fn print_table<const N: usize>(
    out: &mut String,
    width: usize,
    headers: &[&str; N],
    rows: &[[&str; N]],
) {
    let mut widths = [0usize; N];
    for (i, column_width) in widths.iter_mut().enumerate() {
        *column_width = headers[i].len();
    }
    for row in rows {
        for (i, column_width) in widths.iter_mut().enumerate() {
            *column_width = (*column_width).max(row[i].len());
        }
    }
    if N > 1 {
        let fixed_width: usize = widths[..N - 1].iter().sum::<usize>() + ((N - 1) * 2) + 2;
        let max_last = width
            .saturating_sub(fixed_width)
            .max(headers[N - 1].len())
            .max(12);
        widths[N - 1] = widths[N - 1].min(max_last);
    }
    write!(out, "  ").expect("write status output");
    for (i, column_width) in widths.iter().enumerate() {
        if i > 0 {
            write!(out, "  ").expect("write status output");
        }
        write!(out, "{:<width$}", headers[i], width = *column_width).expect("write status output");
    }
    writeln!(out).expect("write status output");
    write!(out, "  ").expect("write status output");
    for (i, column_width) in widths.iter().enumerate() {
        if i > 0 {
            write!(out, "  ").expect("write status output");
        }
        write!(
            out,
            "{:<width$}",
            "-".repeat(*column_width),
            width = *column_width
        )
        .expect("write status output");
    }
    writeln!(out).expect("write status output");
    for row in rows {
        print_table_row(out, row, &widths, width);
    }
}

fn print_table_row<const N: usize>(
    out: &mut String,
    row: &[&str; N],
    widths: &[usize; N],
    terminal_width: usize,
) {
    let mut line_prefix = String::from("  ");
    for i in 0..N.saturating_sub(1) {
        if i > 0 {
            line_prefix.push_str("  ");
        }
        line_prefix.push_str(&format!("{:<width$}", row[i], width = widths[i]));
    }
    if N > 1 {
        line_prefix.push_str("  ");
        print_wrapped(out, &line_prefix, row[N - 1], terminal_width);
    } else if let Some(value) = row.first() {
        print_wrapped(out, &line_prefix, value, terminal_width);
    }
}

fn print_root_row(out: &mut String, width: usize, id: &str, state: &str, path: &str) {
    let (id_width, status_width) = root_table_widths(width);
    let prefix = format!(
        "  {id:<id_width$} {state:<status_width$} ",
        id_width = id_width,
        status_width = status_width
    );
    print_wrapped(out, &prefix, path, width);
}

fn root_table_widths(width: usize) -> (usize, usize) {
    if width < 60 {
        (18, 10)
    } else if width < 88 {
        (24, 18)
    } else {
        (32, 18)
    }
}

fn print_wrapped(out: &mut String, prefix: &str, value: &str, width: usize) {
    let available = width.saturating_sub(prefix.len()).max(16);
    let lines = wrap_text(value, available);
    if let Some((first, rest)) = lines.split_first() {
        writeln!(out, "{prefix}{first}").expect("write status output");
        let continuation = " ".repeat(prefix.len());
        for line in rest {
            writeln!(out, "{continuation}{line}").expect("write status output");
        }
    } else {
        writeln!(out, "{prefix}").expect("write status output");
    }
}

fn wrap_text(value: &str, width: usize) -> Vec<String> {
    if value.len() <= width {
        return vec![value.to_string()];
    }
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in value.split_whitespace() {
        if line.is_empty() {
            line.push_str(word);
        } else if line.len() + 1 + word.len() <= width {
            line.push(' ');
            line.push_str(word);
        } else {
            lines.push(line);
            line = word.to_string();
        }
        while line.len() > width {
            let rest = line.split_off(width);
            lines.push(line);
            line = rest;
        }
    }
    if !line.is_empty() {
        lines.push(line);
    }
    if lines.is_empty() {
        vec![String::new()]
    } else {
        lines
    }
}

fn terminal_width() -> usize {
    std::env::var("COLUMNS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|width| *width >= 40)
        .or_else(|| detect_terminal_size().map(|size| size.cols))
        .unwrap_or(100)
}

fn terminal_height() -> usize {
    std::env::var("LINES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|height| *height >= 5)
        .or_else(|| detect_terminal_size().map(|size| size.rows))
        .unwrap_or(24)
}

fn emit_status_output(output: &str, height: usize) -> Result<()> {
    if should_page_status(output, height) && write_to_pager(output).is_ok() {
        return Ok(());
    }
    print!("{output}");
    Ok(())
}

fn should_page_status(output: &str, height: usize) -> bool {
    io::stdout().is_terminal() && output.lines().count() > height
}

fn write_to_pager(output: &str) -> Result<()> {
    let pager = std::env::var("MJ_PAGER")
        .or_else(|_| std::env::var("PAGER"))
        .unwrap_or_else(|_| "less -R".into());
    let mut parts = pager.split_whitespace();
    let Some(program) = parts.next() else {
        bail!("pager command is empty");
    };
    let mut child = Command::new(program)
        .args(parts)
        .env(
            "LESS",
            std::env::var("LESS").unwrap_or_else(|_| "-R".into()),
        )
        .stdin(Stdio::piped())
        .spawn()
        .with_context(|| format!("start pager: {pager}"))?;
    if let Some(stdin) = child.stdin.as_mut() {
        std::io::Write::write_all(stdin, output.as_bytes()).context("write status to pager")?;
    }
    let status = child.wait().context("wait for pager")?;
    if !status.success() {
        bail!("pager exited with {status}");
    }
    Ok(())
}

struct TerminalSize {
    cols: usize,
    rows: usize,
}

#[cfg(unix)]
fn detect_terminal_size() -> Option<TerminalSize> {
    #[repr(C)]
    struct Winsize {
        ws_row: libc::c_ushort,
        ws_col: libc::c_ushort,
        ws_xpixel: libc::c_ushort,
        ws_ypixel: libc::c_ushort,
    }

    let mut winsize: Winsize = unsafe { mem::zeroed() };
    let result = unsafe {
        libc::ioctl(
            io::stdout().as_raw_fd(),
            libc::TIOCGWINSZ,
            &mut winsize as *mut Winsize,
        )
    };
    if result == 0 && winsize.ws_col >= 40 && winsize.ws_row >= 5 {
        Some(TerminalSize {
            cols: winsize.ws_col as usize,
            rows: winsize.ws_row as usize,
        })
    } else {
        None
    }
}

#[cfg(not(unix))]
fn detect_terminal_size() -> Option<TerminalSize> {
    None
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {} ({bytes} B)", UNITS[unit])
    }
}

fn format_bytes_compact(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else if value >= 10.0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[derive(Default)]
struct StatusDbStats {
    snapshots: i64,
    operations: i64,
    refs: i64,
    blobs: i64,
    blob_bytes: i64,
    large_objects: i64,
    large_object_bytes: i64,
    chunks: i64,
    chunk_bytes: i64,
    packs: i64,
    pack_bytes: i64,
    large_pins: i64,
    remote_refs: i64,
}

fn read_status_db_stats(conn: &Connection) -> Result<StatusDbStats> {
    Ok(StatusDbStats {
        snapshots: count_table(conn, "snapshots")?,
        operations: count_table(conn, "operations")?,
        refs: count_table(conn, "refs")?,
        blobs: count_table(conn, "blobs")?,
        blob_bytes: sum_i64(conn, "select coalesce(sum(size), 0) from blobs")?,
        large_objects: count_table(conn, "large_objects")?,
        large_object_bytes: sum_i64(conn, "select coalesce(sum(size), 0) from large_objects")?,
        chunks: count_table(conn, "chunks")?,
        chunk_bytes: sum_i64(conn, "select coalesce(sum(size), 0) from chunks")?,
        packs: count_table(conn, "packs")?,
        pack_bytes: sum_i64(conn, "select coalesce(sum(size), 0) from packs")?,
        large_pins: count_table(conn, "large_pins")?,
        remote_refs: count_table(conn, "remote_refs")?,
    })
}

fn count_table(conn: &Connection, table: &str) -> Result<i64> {
    let sql = format!("select count(*) from {table}");
    sum_i64(conn, &sql)
}

fn sum_i64(conn: &Connection, sql: &str) -> Result<i64> {
    conn.query_row(sql, [], |row| row.get(0))
        .map_err(Into::into)
}

struct StorageStats {
    state_files: u64,
    state_bytes: u64,
    objects_files: u64,
    objects_bytes: u64,
    logs_files: u64,
    logs_bytes: u64,
    queue_files: u64,
    queue_bytes: u64,
}

fn read_storage_stats(paths: &Paths) -> Result<StorageStats> {
    let state = dir_stats(&paths.home)?;
    let objects = dir_stats(&paths.home.join("objects"))?;
    let logs = dir_stats(&paths.logs)?;
    let queue = dir_stats(&paths.home.join("queue"))?;
    Ok(StorageStats {
        state_files: state.files,
        state_bytes: state.bytes,
        objects_files: objects.files,
        objects_bytes: objects.bytes,
        logs_files: logs.files,
        logs_bytes: logs.bytes,
        queue_files: queue.files,
        queue_bytes: queue.bytes,
    })
}

#[derive(Default)]
struct DirStats {
    files: u64,
    bytes: u64,
}

fn dir_stats(path: &Path) -> Result<DirStats> {
    if !path.exists() {
        return Ok(DirStats::default());
    }
    let mut stats = DirStats::default();
    for entry in WalkDir::new(path).follow_links(false) {
        let entry = entry?;
        if entry.file_type().is_file() {
            stats.files += 1;
            stats.bytes += entry.metadata()?.len();
        }
    }
    Ok(stats)
}

fn count_json_files(path: &Path) -> Result<usize> {
    if !path.exists() {
        return Ok(0);
    }
    let mut count = 0usize;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && entry.path().extension().and_then(|ext| ext.to_str()) == Some("json")
        {
            count += 1;
        }
    }
    Ok(count)
}

struct RemoteStatus {
    configured: bool,
    backend: String,
    url: Option<String>,
    resolved: Option<String>,
    open_error: Option<String>,
    lifecycle_rules: Option<bool>,
    object_tags: Option<bool>,
    storage_class_on_put: Option<bool>,
    restore_archived_object: Option<bool>,
    multipart_upload: Option<bool>,
    range_get: Option<bool>,
    conditional_put: Option<bool>,
}

impl RemoteStatus {
    fn summary(&self) -> &str {
        if !self.configured {
            return "not configured";
        }
        if self.open_error.is_some() {
            return "configured, unavailable";
        }
        "configured"
    }
}

fn read_remote_status(config: &Config) -> Result<RemoteStatus> {
    let Some(remote_config) = &config.remote else {
        return Ok(RemoteStatus {
            configured: false,
            backend: "none".into(),
            url: None,
            resolved: None,
            open_error: None,
            lifecycle_rules: None,
            object_tags: None,
            storage_class_on_put: None,
            restore_archived_object: None,
            multipart_upload: None,
            range_get: None,
            conditional_put: None,
        });
    };
    let remote_url = remote_config.url().context("resolve remote URL")?;
    let backend = if remote_url.starts_with("file://") {
        "file"
    } else if remote_url.starts_with("s3://") {
        "s3"
    } else {
        "unknown"
    };
    match open_remote(remote_config) {
        Ok(remote) => {
            let capabilities = remote.capabilities();
            Ok(RemoteStatus {
                configured: true,
                backend: backend.into(),
                url: Some(remote_url),
                resolved: Some(remote.describe()),
                open_error: None,
                lifecycle_rules: Some(capabilities.lifecycle_rules),
                object_tags: Some(capabilities.object_tags),
                storage_class_on_put: Some(capabilities.storage_class_on_put),
                restore_archived_object: Some(capabilities.restore_archived_object),
                multipart_upload: Some(capabilities.multipart_upload),
                range_get: Some(capabilities.range_get),
                conditional_put: Some(capabilities.conditional_put),
            })
        }
        Err(err) => Ok(RemoteStatus {
            configured: true,
            backend: backend.into(),
            url: Some(remote_url),
            resolved: None,
            open_error: Some(format!("{err:#}")),
            lifecycle_rules: None,
            object_tags: None,
            storage_class_on_put: None,
            restore_archived_object: None,
            multipart_upload: None,
            range_get: None,
            conditional_put: None,
        }),
    }
}

fn print_remote_section(out: &mut String, width: usize, ui: &StatusUi, remote: &RemoteStatus) {
    writeln!(out, "{}", ui.heading("Remote")).expect("write status output");
    print_kv(out, width, "Configured", &remote.configured.to_string());
    print_kv(out, width, "Backend", &remote.backend);
    if let Some(url) = &remote.url {
        print_kv(out, width, "URL", url);
    }
    if let Some(resolved) = &remote.resolved {
        print_kv(out, width, "Resolved", resolved);
    }
    if let Some(error) = &remote.open_error {
        print_kv(out, width, "Open error", error);
    }
    if remote.lifecycle_rules.is_some() {
        print_table(
            out,
            width,
            &["CAPABILITY", "SUPPORTED"],
            &[
                [
                    "lifecycle rules",
                    &display_option_bool(remote.lifecycle_rules),
                ],
                ["object tags", &display_option_bool(remote.object_tags)],
                [
                    "storage class on put",
                    &display_option_bool(remote.storage_class_on_put),
                ],
                [
                    "restore archived object",
                    &display_option_bool(remote.restore_archived_object),
                ],
                [
                    "multipart upload",
                    &display_option_bool(remote.multipart_upload),
                ],
                ["range get", &display_option_bool(remote.range_get)],
                [
                    "conditional put",
                    &display_option_bool(remote.conditional_put),
                ],
            ],
        );
    }
}

fn display_option_bool(value: Option<bool>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".into())
}

pub(crate) fn log_cmd(paths: &Paths, args: LogArgs) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    print_op_log(&conn, &args)
}

fn print_op_log(conn: &Connection, args: &LogArgs) -> Result<()> {
    let mut stmt = conn.prepare(
        "select id, kind, before_snapshot, after_snapshot, created_at, message, status, remote_sync_state
         from operations order by rowid desc",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, Option<String>>(7)?,
        ))
    })?;
    let mut printed = 0usize;
    for row in rows {
        let (id, kind, before, after, created, message, status, remote_sync_state) = row?;
        if let Some(root) = &args.root {
            let matches_root = message.as_deref() == Some(root)
                || before
                    .as_deref()
                    .and_then(|snapshot| snapshot_contains_root(conn, snapshot, root).ok())
                    .unwrap_or(false)
                || after
                    .as_deref()
                    .and_then(|snapshot| snapshot_contains_root(conn, snapshot, root).ok())
                    .unwrap_or(false);
            if !matches_root {
                continue;
            }
        }
        if printed >= args.limit {
            break;
        }
        println!(
            "{id}\t{created}\t{kind}\t{status}\t{}\t{} -> {}\t{}",
            remote_sync_state.unwrap_or_else(|| "-".into()),
            before.unwrap_or_default(),
            after.unwrap_or_default(),
            message.unwrap_or_default()
        );
        printed += 1;
    }
    Ok(())
}

pub(crate) fn op_cmd(paths: &Paths, command: OpCommand) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    match command {
        OpCommand::Log(args) => print_op_log(&conn, &args),
        OpCommand::Show { op_id } => {
            let op = query_operation(&conn, &op_id)?;
            println!("id {}", op.id);
            println!("parent {}", op.parent_op.unwrap_or_else(|| "(none)".into()));
            println!("kind {}", op.kind);
            println!("actor {}", op.actor);
            println!("status {}", op.status);
            println!(
                "before {}",
                op.before_snapshot.unwrap_or_else(|| "(none)".into())
            );
            println!(
                "after {}",
                op.after_snapshot.unwrap_or_else(|| "(none)".into())
            );
            println!("created_at {}", op.created_at);
            println!("message {}", op.message.unwrap_or_default());
            println!("error {}", op.error.unwrap_or_default());
            println!(
                "remote_sync_state {}",
                op.remote_sync_state.unwrap_or_default()
            );
            Ok(())
        }
        OpCommand::Restore { op_id } => {
            let op = query_operation(&conn, &op_id)?;
            let before = current_snapshot(&conn)?;
            let snapshot = op
                .before_snapshot
                .or(op.after_snapshot)
                .ok_or_else(|| anyhow!("operation has no snapshot to restore: {op_id}"))?;
            conn.execute(
                "insert into refs(name, value) values ('current', ?1)
                 on conflict(name) do update set value=excluded.value",
                params![snapshot],
            )?;
            record_op(
                &conn,
                "op-restore",
                before.as_deref(),
                Some(&snapshot),
                Some(&op_id),
            )?;
            println!("current {}", snapshot);
            Ok(())
        }
    }
}

pub(crate) fn diff_cmd(paths: &Paths, args: DiffArgs) -> Result<()> {
    crate::ensure_ready(paths)?;
    let conn = crate::open_db(paths)?;
    if args.at.is_some() && args.from.is_some() {
        bail!("use either a positional from snapshot or --at, not both");
    }
    let to_id = args
        .to
        .clone()
        .or_else(|| current_snapshot(&conn).ok().flatten())
        .ok_or_else(|| anyhow!("no target snapshot"))?;
    let to = load_snapshot_by_id(&conn, &to_id)?;
    let from_id = if let Some(at) = &args.at {
        Some(snapshot_id_at(&conn, at)?)
    } else {
        args.from.or_else(|| to.parent.clone())
    };
    let from = if let Some(from_id) = from_id {
        Some(load_snapshot_by_id(&conn, &from_id)?)
    } else {
        None
    };
    let from_files = from
        .as_ref()
        .map(snapshot_file_map)
        .transpose()?
        .unwrap_or_default();
    let to_files = snapshot_file_map(&to)?;
    let mut paths_all = from_files.keys().cloned().collect::<Vec<_>>();
    paths_all.extend(
        to_files
            .keys()
            .filter(|key| !from_files.contains_key(*key))
            .cloned(),
    );
    paths_all.sort();
    for key in paths_all {
        if let Some(root) = &args.root {
            if !key.starts_with(&format!("{root}/")) {
                continue;
            }
        }
        match (from_files.get(&key), to_files.get(&key)) {
            (None, Some(_)) => println!("A\t{key}"),
            (Some(_), None) => println!("D\t{key}"),
            (Some(a), Some(b)) if serde_json::to_value(a)? != serde_json::to_value(b)? => {
                println!("M\t{key}");
            }
            _ => {}
        }
    }
    Ok(())
}
