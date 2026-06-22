use std::process::Command;

fn mj_output(lang: &str, args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_mj"))
        .env_remove("LC_ALL")
        .env_remove("LC_MESSAGES")
        .env("LANG", lang)
        .args(args)
        .output()
        .expect("run mj");
    assert!(output.status.success(), "mj failed: {output:?}");
    String::from_utf8(output.stdout).expect("utf8 stdout")
}

#[test]
fn help_respects_major_languages_for_human_descriptions() {
    let cases = [
        (
            "C",
            "--help",
            "majutsu snapshots multiple directories on a development host",
        ),
        (
            "ja_JP.UTF-8",
            "--help",
            "ローカルデータ喪失から復旧できるように",
        ),
        ("zh_CN.UTF-8", "--help", "本地数据丢失后恢复"),
        ("es_ES.UTF-8", "--help", "pérdida local de datos"),
        ("fr_FR.UTF-8", "--help", "perte de données locale"),
    ];

    for (lang, arg, expected) in cases {
        let output = mj_output(lang, &[arg]);
        assert!(
            output.contains(expected),
            "LANG={lang} output did not contain {expected:?}:\n{output}"
        );
    }
}

#[test]
fn restore_help_localizes_command_and_option_descriptions() {
    let output = mj_output("ja_JP.UTF-8", &["restore", "--help"]);

    assert!(output.contains("復元の計画、適用、準備、再開を行います"));
    assert!(output.contains("このoperation idまたはprefixが作った状態を復元します"));
    assert!(output.contains("2hなどの相対時間以前の最新snapshotを復元します"));
}
