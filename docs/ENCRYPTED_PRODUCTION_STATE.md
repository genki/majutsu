# 暗号化 production state ガイド

`security.encryption = "none"` の state は、secret を含む root の継続運用には使わない。
production では encrypted state を作り、master key を host 外へ保管する。

## 新規 production state

```sh
mj --home ~/.majutsu-prod init --encrypt --remote s3://bucket/prefix
mj --home ~/.majutsu-prod key export > ~/majutsu-prod-master-key.txt
chmod 0600 ~/majutsu-prod-master-key.txt
```

master key は password manager / sealed secret / offline backup など、host とは別の場所に保管する。

## 暗号化復旧 drill

```sh
export MAJUTSU_MASTER_KEY="$(cat ~/majutsu-prod-master-key.txt)"
mj --home /tmp/majutsu-recovered clone --remote s3://bucket/prefix
mj --home /tmp/majutsu-recovered fsck
mj --home /tmp/majutsu-recovered restore plan --to /tmp/restore
mj --home /tmp/majutsu-recovered restore apply --to /tmp/restore
```

自動 smoke は次で実行できる。

```sh
MAJUTSU_ENCRYPTED_REMOTE=s3://bucket/prefix scripts/verify-encrypted-remote-recovery.sh
```

## 既存 unencrypted state からの移行

安全な移行は、新しい encrypted home を作り、root 設定を移して再 snapshot / sync する方法を推奨する。
既存 unencrypted remote に secret を含めた可能性がある場合、provider 側の object lifecycle / delete / bucket rotation も検討する。

1. 既存 state の root / exclude / large policy を確認する。
2. 新しい encrypted state を作る。
3. secret path が exclude されているか、または encrypted remote だけを使うことを確認する。
4. snapshot / sync / clone / restore drill を実行する。
5. 旧 unencrypted remote を必要に応じて rotate / delete する。
