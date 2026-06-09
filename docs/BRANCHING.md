# majutsu branching

Majutsu の branch は host-level metadata database に保存される軽量な ref である。
Git branch ではなく、branch 操作だけではファイルは変更しない。branch は snapshot
を指し、branch switch は `refs/current` と `refs/current-branch` を更新する。
`--restore` を指定すると、その snapshot を configured roots または指定ディレクトリへ
実体化する。

## 基本フロー

```sh
mj snapshot --message 'main baseline'
mj branch create experiment --switch
# ファイルを編集する
mj snapshot --message 'experiment work'
mj branch switch main --restore --force
mj branch switch experiment --restore --force
```

## 過去時点からの分岐

```sh
mj branch create incident-review --at '2026-06-06 10:30:00' --switch --restore --force
```

既知の snapshot id から分岐する場合:

```sh
mj branch create before-upgrade --snapshot snap-... --switch
```

switch 後の次の `mj snapshot` は、その branch head を parent として作成される。
そのため、新しい snapshot は分岐した timeline になる。snapshot が成功すると、
active branch head は新しい snapshot へ進む。

## コマンド

```sh
mj branch list
mj branch current
mj branch create <name> [--snapshot <id> | --at <time>] [--switch] [--restore] [--force]
mj branch switch <name> [--restore] [--force] [--to <dir>]
mj branch set-head <name> [--snapshot <id> | --at <time>]
mj branch rename <old> <new> [--force]
mj branch delete <name> [--force]
```

## ref layout

branch は既存の refs table と metadata export に保存される。

```text
current                 active snapshot
current-branch          active branch name
branches/<name>         branch head snapshot
```

このため、branch state は他の host timeline metadata と同様に `mj sync` と
`mj clone` をまたいで復元できる。
