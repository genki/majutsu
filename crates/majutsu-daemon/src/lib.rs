use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaemonState {
    Running { pid: u32 },
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonStatus {
    pub state: DaemonState,
    pub socket_path: String,
}

pub fn render_daemon_service(
    provider: &str,
    exe: &Path,
    home: &Path,
    backend: &str,
    mode: &str,
    interval_secs: u64,
    debounce_ms: u64,
    settle_ms: u64,
    periodic_rescan_secs: u64,
) -> Result<String, String> {
    match provider {
        "systemd" => Ok(render_systemd_user_service(
            exe,
            home,
            backend,
            mode,
            interval_secs,
            debounce_ms,
            settle_ms,
            periodic_rescan_secs,
        )),
        "launchd" => Ok(render_launchd_plist(
            exe,
            home,
            backend,
            mode,
            interval_secs,
            debounce_ms,
            settle_ms,
            periodic_rescan_secs,
        )),
        other => Err(format!("unsupported daemon service provider: {other}")),
    }
}

fn daemon_watch_args(
    exe: &Path,
    home: &Path,
    backend: &str,
    mode: &str,
    interval_secs: u64,
    debounce_ms: u64,
    settle_ms: u64,
    periodic_rescan_secs: u64,
) -> Vec<String> {
    vec![
        exe.display().to_string(),
        "--home".into(),
        home.display().to_string(),
        "watch".into(),
        "--foreground".into(),
        "true".into(),
        "--backend".into(),
        backend.into(),
        "--mode".into(),
        mode.into(),
        "--interval-secs".into(),
        interval_secs.to_string(),
        "--debounce-ms".into(),
        debounce_ms.to_string(),
        "--settle-ms".into(),
        settle_ms.to_string(),
        "--periodic-rescan-secs".into(),
        periodic_rescan_secs.to_string(),
    ]
}

fn render_systemd_user_service(
    exe: &Path,
    home: &Path,
    backend: &str,
    mode: &str,
    interval_secs: u64,
    debounce_ms: u64,
    settle_ms: u64,
    periodic_rescan_secs: u64,
) -> String {
    let args = daemon_watch_args(
        exe,
        home,
        backend,
        mode,
        interval_secs,
        debounce_ms,
        settle_ms,
        periodic_rescan_secs,
    )
    .into_iter()
    .map(|arg| systemd_quote(&arg))
    .collect::<Vec<_>>()
    .join(" ");
    format!(
        "[Unit]\n\
         Description=Majutsu watch daemon\n\
         After=network-online.target\n\
         Wants=network-online.target\n\n\
         [Service]\n\
         Type=simple\n\
         ExecStart={args}\n\
         Restart=on-failure\n\
         RestartSec=10s\n\n\
         [Install]\n\
         WantedBy=default.target\n"
    )
}

fn systemd_quote(value: &str) -> String {
    let escaped = value.replace('%', "%%");
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || "/._:-=+".contains(ch))
    {
        escaped
    } else {
        format!("\"{}\"", escaped.replace('\\', "\\\\").replace('"', "\\\""))
    }
}

fn render_launchd_plist(
    exe: &Path,
    home: &Path,
    backend: &str,
    mode: &str,
    interval_secs: u64,
    debounce_ms: u64,
    settle_ms: u64,
    periodic_rescan_secs: u64,
) -> String {
    let args = daemon_watch_args(
        exe,
        home,
        backend,
        mode,
        interval_secs,
        debounce_ms,
        settle_ms,
        periodic_rescan_secs,
    )
    .into_iter()
    .map(|arg| format!("        <string>{}</string>", xml_escape(&arg)))
    .collect::<Vec<_>>()
    .join("\n");
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \
         \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\">\n\
         <dict>\n\
             <key>Label</key>\n\
             <string>dev.majutsu.watch</string>\n\
             <key>ProgramArguments</key>\n\
             <array>\n{args}\n\
             </array>\n\
             <key>KeepAlive</key>\n\
             <true/>\n\
             <key>RunAtLoad</key>\n\
             <true/>\n\
             <key>StandardOutPath</key>\n\
             <string>{}/logs/majutsu.log</string>\n\
             <key>StandardErrorPath</key>\n\
             <string>{}/logs/majutsu.log</string>\n\
         </dict>\n\
         </plist>\n",
        xml_escape(&home.display().to_string()),
        xml_escape(&home.display().to_string())
    )
}

fn xml_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn renders_systemd_service_with_quoted_paths() {
        let service = render_daemon_service(
            "systemd",
            Path::new("/opt/Majutsu Bin/mj"),
            Path::new("/home/alice/.majutsu%prod"),
            "inotify",
            "default",
            60,
            1500,
            500,
            3600,
        )
        .unwrap();

        assert!(service.contains("[Service]"));
        assert!(service.contains("ExecStart="));
        assert!(service.contains("\"/opt/Majutsu Bin/mj\""));
        assert!(service.contains("/home/alice/.majutsu%%prod"));
        assert!(service.contains("Restart=on-failure"));
    }

    #[test]
    fn renders_launchd_plist_with_escaped_paths() {
        let service = render_daemon_service(
            "launchd",
            Path::new("/opt/majutsu/mj"),
            Path::new("/Users/alice/.majutsu&prod"),
            "notify",
            "strict",
            30,
            25,
            15,
            0,
        )
        .unwrap();

        assert!(service.contains("<key>ProgramArguments</key>"));
        assert!(service.contains("<string>/Users/alice/.majutsu&amp;prod</string>"));
        assert!(service.contains("<string>strict</string>"));
        assert!(service.contains("<key>KeepAlive</key>"));
    }

    #[test]
    fn rejects_unknown_service_provider() {
        assert!(
            render_daemon_service(
                "cron",
                Path::new("/usr/bin/mj"),
                Path::new("/tmp/majutsu"),
                "poll",
                "default",
                60,
                1500,
                500,
                3600,
            )
            .is_err()
        );
    }
}
