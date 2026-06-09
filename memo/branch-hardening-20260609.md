# branch hardening 2026-06-09

## 背景

`majutsu_branch_hardening_fix.zip` の提案を確認し、branch refs と restore の整合性を強化した。

## 取り込んだ内容

- `mj branch switch --restore` は、restore 成功後に `current-branch` と `current` を更新する。
  restore が失敗した場合、branch refs は旧状態のまま残る。
- `mj branch rename <old> <old>` を拒否する。
- active destination branch を `rename --force` で上書きした場合、`current` を新しいheadへ合わせる。
- `branch create --force` でactive branch headを移動した場合、`current` も同じsnapshotへ合わせる。
- branch create/rename のoperation logで、currentが変わる場合はbefore/after snapshotを記録する。
- branch headのadvance、restore switch、rename edge caseのE2Eを追加した。
- `docs/BRANCHING.md` に安全性と整合性の説明を追加した。

## 確認

```sh
cargo check --locked
cargo test --locked --test e2e_local branch
cargo test --locked --bin mj branch_runtime
cargo test --locked
```

全て成功。
