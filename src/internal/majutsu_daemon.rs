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

#[derive(Debug, Clone, Copy)]
pub struct DaemonServiceConfig<'a> {
    pub provider: &'a str,
    pub scope: DaemonServiceScope,
    pub exe: &'a Path,
    pub home: &'a Path,
    pub backend: &'a str,
    pub mode: &'a str,
    pub interval_secs: u64,
    pub debounce_ms: u64,
    pub settle_ms: u64,
    pub buffer_max_ms: u64,
    pub buffer_max_events: usize,
    pub periodic_rescan_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DaemonServiceScope {
    User,
    System,
}

pub fn render_daemon_service(config: DaemonServiceConfig<'_>) -> Result<String, String> {
    let args = daemon_watch_args(&config);
    match config.provider {
        "systemd" => Ok(render_systemd_service(&config, args)),
        "launchd" => {
            if config.scope == DaemonServiceScope::System {
                Err("launchd system scope is not supported; use systemd or render a user launchd plist".into())
            } else {
                Ok(render_launchd_plist(&config, args))
            }
        }
        other => Err(format!("unsupported daemon service provider: {other}")),
    }
}

fn daemon_watch_args(config: &DaemonServiceConfig<'_>) -> Vec<String> {
    vec![
        config.exe.display().to_string(),
        "--home".into(),
        config.home.display().to_string(),
        "watch".into(),
        "--foreground".into(),
        "true".into(),
        "--backend".into(),
        config.backend.into(),
        "--mode".into(),
        config.mode.into(),
        "--interval-secs".into(),
        config.interval_secs.to_string(),
        "--debounce-ms".into(),
        config.debounce_ms.to_string(),
        "--settle-ms".into(),
        config.settle_ms.to_string(),
        "--buffer-max-ms".into(),
        config.buffer_max_ms.to_string(),
        "--buffer-max-events".into(),
        config.buffer_max_events.to_string(),
        "--periodic-rescan-secs".into(),
        config.periodic_rescan_secs.to_string(),
    ]
}

fn render_systemd_service(config: &DaemonServiceConfig<'_>, args: Vec<String>) -> String {
    let args = args
        .into_iter()
        .map(|arg| systemd_quote(&arg))
        .collect::<Vec<_>>()
        .join(" ");
    let daemon_env = format!(
        "-{}",
        systemd_quote(&format!("{}/daemon.env", config.home.display()))
    );
    let s3_env = format!(
        "-{}",
        systemd_quote(&format!("{}/s3.env", config.home.display()))
    );
    let wanted_by = match config.scope {
        DaemonServiceScope::User => "default.target",
        DaemonServiceScope::System => "multi-user.target",
    };
    let service_scope = match config.scope {
        DaemonServiceScope::User => "",
        DaemonServiceScope::System => "User=root\nUMask=0077\n",
    };
    format!(
        "[Unit]\n\
         Description=Majutsu watch daemon\n\
         After=network-online.target\n\
         Wants=network-online.target\n\n\
         [Service]\n\
         Type=simple\n\
         EnvironmentFile={daemon_env}\n\
         EnvironmentFile={s3_env}\n\
         ExecStart={args}\n\
         {service_scope}\
         Restart=on-failure\n\
         RestartSec=10s\n\n\
         [Install]\n\
         WantedBy={wanted_by}\n"
    )
}

fn systemd_quote(value: &str) -> String {
    let escaped = value.replace('%', "%%");
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || "/._:-=+-".contains(ch))
    {
        escaped
    } else {
        format!("\"{}\"", escaped.replace('\\', "\\\\").replace('"', "\\\""))
    }
}

fn render_launchd_plist(config: &DaemonServiceConfig<'_>, args: Vec<String>) -> String {
    let args = args
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
        xml_escape(&config.home.display().to_string()),
        xml_escape(&config.home.display().to_string())
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
        let service = render_daemon_service(DaemonServiceConfig {
            provider: "systemd",
            scope: DaemonServiceScope::User,
            exe: Path::new("/opt/Majutsu Bin/mj"),
            home: Path::new("/home/alice/.majutsu%prod"),
            backend: "inotify",
            mode: "default",
            interval_secs: 60,
            debounce_ms: 1500,
            settle_ms: 500,
            buffer_max_ms: 60000,
            buffer_max_events: 1000,
            periodic_rescan_secs: 3600,
        })
        .unwrap();

        assert!(service.contains("[Service]"));
        assert!(service.contains("ExecStart="));
        assert!(service.contains("EnvironmentFile=-"));
        assert!(service.contains("/home/alice/.majutsu%%prod/daemon.env"));
        assert!(service.contains("/home/alice/.majutsu%%prod/s3.env"));
        assert!(service.contains("\"/opt/Majutsu Bin/mj\""));
        assert!(service.contains("/home/alice/.majutsu%%prod"));
        assert!(service.contains("Restart=on-failure"));
        assert!(service.contains("WantedBy=default.target"));
        assert!(!service.contains("User=root"));
    }

    #[test]
    fn renders_systemd_system_service() {
        let service = render_daemon_service(DaemonServiceConfig {
            provider: "systemd",
            scope: DaemonServiceScope::System,
            exe: Path::new("/usr/local/bin/mj"),
            home: Path::new("/var/lib/majutsu"),
            backend: "inotify",
            mode: "default",
            interval_secs: 60,
            debounce_ms: 1500,
            settle_ms: 500,
            buffer_max_ms: 60000,
            buffer_max_events: 1000,
            periodic_rescan_secs: 3600,
        })
        .unwrap();

        assert!(service.contains("User=root"));
        assert!(service.contains("UMask=0077"));
        assert!(service.contains("WantedBy=multi-user.target"));
        assert!(service.contains("/var/lib/majutsu"));
    }

    #[test]
    fn renders_launchd_plist_with_escaped_paths() {
        let service = render_daemon_service(DaemonServiceConfig {
            provider: "launchd",
            scope: DaemonServiceScope::User,
            exe: Path::new("/opt/majutsu/mj"),
            home: Path::new("/Users/alice/.majutsu&prod"),
            backend: "notify",
            mode: "strict",
            interval_secs: 30,
            debounce_ms: 25,
            settle_ms: 15,
            buffer_max_ms: 1000,
            buffer_max_events: 20,
            periodic_rescan_secs: 0,
        })
        .unwrap();

        assert!(service.contains("<key>ProgramArguments</key>"));
        assert!(service.contains("<string>/Users/alice/.majutsu&amp;prod</string>"));
        assert!(service.contains("<string>strict</string>"));
        assert!(service.contains("<key>KeepAlive</key>"));
    }

    #[test]
    fn rejects_unknown_service_provider() {
        assert!(
            render_daemon_service(DaemonServiceConfig {
                provider: "cron",
                scope: DaemonServiceScope::User,
                exe: Path::new("/usr/bin/mj"),
                home: Path::new("/tmp/majutsu"),
                backend: "poll",
                mode: "default",
                interval_secs: 60,
                debounce_ms: 1500,
                settle_ms: 500,
                buffer_max_ms: 60000,
                buffer_max_events: 1000,
                periodic_rescan_secs: 3600,
            })
            .is_err()
        );
    }
}
