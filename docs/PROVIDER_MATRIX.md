# S3 互換 provider matrix

`Status` は release 判定での扱いを示す。

- `local gate verified`: ローカル completion gate で毎回確認する。
- `verified YYYY-MM-DD`: 実 provider で検証済み。release ごとに再確認する。
- `experimental`: 実装上は動く可能性があるが、該当 release の supported provider には含めない。
- `provider-specific`: provider 側仕様差が大きいため、運用前に個別確認する。

| Provider | Put/Get | Range GET | Multipart | Conditional PUT | Tags | Storage class | Lifecycle apply | Archive restore | Status | Evidence |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| File remote | yes | yes | n/a | yes | n/a | n/a | n/a | n/a | local gate verified | `cargo test --test e2e_local` |
| MinIO via Podman | yes | yes | yes | yes | partial | partial | partial | n/a | local gate verified | `scripts/e2e-minio.sh` |
| GCS S3-compatible endpoint | yes | yes | yes | provider-specific | provider-specific | provider-specific | prefer native GCS lifecycle | experimental until provider drill | verified 2026-06-13 | `~/moon` root: sync, remote check, remote fsck quick, encrypted clone, path restore, native object alias repair |
| AWS S3 | yes | yes | yes | yes | yes | yes | yes | yes | experimental until release validation | `scripts/e2e-aws-archive-restore.sh` |
| Cloudflare R2 | yes | yes | yes | provider-specific | provider-specific | limited | provider-specific | no Glacier-style restore | experimental | manual validation required |

## release 判定方針

release は `Status` が `local gate verified` または当該 release candidate で
`verified <date>` になっている provider について complete と呼べる。
`experimental` の provider は、その release の supported provider set には含めない。

Archive / cold-tier restore は、`scripts/e2e-aws-archive-restore.sh` または同等の
provider 固有 drill の結果を release evidence に記録した provider だけ supported とする。

## GCS S3-compatible endpoint 検証証跡, 2026-06-09

`moon` root で観測した結果:

```text
root: moon -> /home/vagrant/moon
latest snapshot: snap-3d0df93a-535f-4a9c-8889-ae006380b25b
remote: s3://majutsu-vagrant-winvr-s21g-twpro-20260608/vagrant/c071a4f3-fa6e-4c54-b663-9c350bc77865
remote object count after cleanup: 70
local state usage after cleanup: 14 MiB
```

検証済み操作:

```sh
mj snapshot
mj sync
mj remote check
mj remote fsck
mj clone --remote <gcs-s3-compatible-remote>
mj restore apply --to <restore-dir>
```

moon 検証を通じて入った性能・scale 対応:

- S3 list pagination 対応
- 小さい blob が多い場合の sync 前 pack
- quick/deep remote fsck の分離
- packed payload による remote object request 削減

## GCS S3-compatible endpoint 検証証跡, 2026-06-13

現在の encrypted prefix で再検証した。

```text
root: configured roots in /home/vagrant/.majutsu
remote: s3://majutsu-vagrant-winvr-s21g-twpro-20260608/vagrant/c071a4f3-fa6e-4c54-b663-9c350bc77865-encrypted
current after sync: snap-d0a8dbbc-f939-4bee-998e-43ff289aebb7
clone target: /tmp/majutsu-gcs-recovery.D4NWhv/home
path restore target: /tmp/majutsu-gcs-recovery.D4NWhv/restore
restored file: moon/memo/tools/majutsu-status-review-20260613.md
```

検証済み操作:

```sh
mj remote check
mj remote fsck
mj sync --wait --timeout-secs 300
mj --home /tmp/majutsu-gcs-recovery.D4NWhv/home clone --remote <encrypted-gcs-remote>
mj --home /tmp/majutsu-gcs-recovery.D4NWhv/home restore apply \
  --root moon \
  --path memo/tools/majutsu-status-review-20260613.md \
  --to /tmp/majutsu-gcs-recovery.D4NWhv/restore
```

`remote fsck --objects` は全 object の逐次 probe で長時間かかるため途中停止したが、
その過程で legacy blob alias 欠落を検出した。ローカル state に残っていた 15 件の
loose blob を GCS canonical encrypted key に再アップロードし、native GCS listing で
local loose blob に対応する expected remote alias の欠落が 0 件であることを確認した。

```text
local_blob_expected=1565
remote_blob_keys=1720
missing_blob_keys=0
```

残る改善点:

- `remote fsck --objects` は S3/GCS の object probe を並列化し、進捗と timeout を持たせる。
- remote に存在せず local に存在する canonical object を再送する `remote repair` 相当の
  操作を CLI として用意する。
