# 耐久ジャーナル先行同期

## 背景

watch が変更を検出してローカル snapshot を作成した後、通常の snapshot publish が完了するまで
`mj health` が `unprotected` になる事象があった。remote head の一致だけを保護条件にすると、
payload を遠隔へ保存済みでも、大量のメタデータ処理や root size 集計中は未保護に見える。

## 原因

- snapshot 時刻以前の未ackイベントを耐久ジャーナル転送から除外していた。
- payload と journal の転送が通常 snapshot オブジェクトの列挙・転送より後だった。
- snapshot compaction が remote ack 前のイベントを削除できた。
- journal オブジェクトの存在だけで ack し、参照 payload の存在を確認していなかった。

## 不変条件

1. 未ackの変更イベントは、ローカル snapshot に含まれていても遠隔耐久化対象とする。
2. 変更 payload と journal は通常 snapshot publish より先に転送する。
3. journal の ack は、通常 payload、large manifest、全chunkの遠隔存在確認後に行う。
4. remote ack 前のイベントは snapshot compaction で削除しない。
5. remote head が遅延中でも、同期が稼働し、今回の耐久ジャーナルが全てack済みで、
   upload retryのバックプレッシャーがなければ重大障害ではなく追随中として扱う。
6. `sync wait` の完了条件は変更せず、remote head 一致とqueue空を引き続き要求する。

## 検証

- `cargo test --locked`: unit 198件、CLI i18n 3件、local E2E 16件、remote file 305件が成功。
- snapshot 前に作成した未ackイベントが compaction 後も残り、次の sync で耐久化・ackされる
  回帰テストを追加した。
- `cargo clippy --locked --all-targets -- -D warnings` が成功。
