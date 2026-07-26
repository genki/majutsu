# metadata-only snapshotの効率改善

## 症状

管理対象fileの差分がないwatch eventでもsnapshotが作られ、同期済みloose payloadを
約116 MiB再生成した直後にpruneする処理が観測された。prune後の`mj log`では、
file差分0件のoperationが`metadata-unavailable`警告になる場合もあった。

## 原因

1. directory mtimeをtree identityへ含めていた。excluded/transient fileの作成・削除でも
   親directory mtimeが変わるため、管理対象fileが同一でもtree IDが変化した。
2. scanでcontent hashを計算した後、remoteに同期済みの既知loose blobもローカルobjectへ
   再書き込みしていた。
3. `operation_changes`が0件の場合、完全な空要約と、旧データの要約欠落を区別できなかった。

## 修正

- directoryのmode、uid、gid、xattrs、種類が同一なら、mtimeは直前snapshotの値を継承する。
- file contentは従来どおり毎回hashし、同一size・mtimeへの置換も検出する。
- configured remoteのcurrentとparent snapshotが一致する場合だけ、DB上の既知blob・chunk
  keyを再利用し、remote-only payloadをローカルcacheへ再生成しない。
- remote current未確認時は従来どおりlocal objectを作る。pack内blobはremote状態に関係なく
  再利用する。
- file差分0件のoperationは`operation_changes`へ`N` markerを保存し、prune後も
  `metadata-unavailable`と誤表示しない。

## 不変条件

- directoryの権限、所有者、xattr変更はsnapshot対象のまま維持する。
- fileはmtime/sizeだけで同一判定せず、content hashを計算する。
- remote ackのないloose payloadはローカルへ必ず保存する。
- `sync wait`、remote fsck、restoreの完全性条件は変更しない。

## テスト

- transient fileの作成・削除によるdirectory mtime変化がno-op snapshotになる。
- directory metadataは直前snapshotと同一に保たれる。
- remote ack済みloose blobがscan後もlocalへ再生成されない。
- remote ack済みlarge/chunked payloadのchunkがscan後もlocalへ再生成されない。
- pack済みblobの既存再利用を維持する。
- directory metadataだけのsnapshotをpruneしてもlogに警告を出さない。
- 全527テストとClippy `-D warnings`が成功。

## 実環境評価

vagrant環境の18 roots、8,425 files、52 large filesを変更なしで走査した。

| 指標 | 修正前 | 修正後 | 改善 |
|---|---:|---:|---:|
| elapsed | 14.57秒 | 5.11秒 | 64.9%短縮 |
| filesystem outputs | 約120 MiB | 約6.3 MiB | 94.7%削減 |
| max RSS | 72.7 MiB | 71.6 MiB | 同等 |
| snapshot | unchanged | unchanged | 同等 |

file contentのhash計算はデータ置換検出のため維持しており、filesystem inputは残る。
書込6.3 MiBはtree/manifest一時object、journal、SQLite WALなどで、payload本体の再生成ではない。
