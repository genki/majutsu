# subtree reuse と restore index の設計メモ

## 目的

- 長期履歴で tree manifest が増え続ける問題を抑える。
- restore / diff / fsck の決定性を保つ。
- 小変更 sync で余分な小 object publish を増やさない。

## 提案する tree format v2

root tree を Merkle directory tree として表現する。

```text
root-tree-v2
  root_id
  root_node_key -> objects/trees/nodes/<hash>.cbor.zst.enc
```

各 directory node は sort 済み child entry を持つ。file entry は現在の `Payload` model を維持し、directory entry は child node key を指す。

変更のない subdirectory は snapshot 間で node key を再利用する。単一 file の編集では次だけを書き換える。

```text
changed file payload
leaf directory node
ancestor directory nodes up to root
snapshot manifest
head object
```

## diff への影響

diff はまず node key を比較する。node key が等しい subtree は同一とみなし、child entry を読み込まずに skip できる。

## restore への影響

restore は root node を解決し、指定された path filter に必要な node だけを辿る。path 指定 restore で flat root manifest 全体を読む必要がなくなる。

## fsck への影響

quick fsck は root node の到達性と基本 metadata を確認する。deep fsck は node と payload を辿って検査する。

## migration

- 現行の flat tree manifest は読み続ける。
- v2 の書き込みは `MAJUTSU_TREE_FORMAT=v2` または明示 migration command の後に限定する。
- metadata export には tree format と root node key の両方を含める。

## 2026-06-18 実装済みの移行準備

現行の `TreeManifest` に `root_node` と `subtree_nodes` を追加し、`MAJUTSU_TREE_SUBTREE_NODES=1` を指定した場合は一定数以上の entry を持つ root で
`objects/trees/nodes/` に content-addressed な `TreeNodeManifest` sidecar を書けるようにした。
未変更のトップレベルサブツリーは snapshot 間で同じ node key を再利用する。

この段階では復元互換性を優先し、flat `entries` はまだ root tree に残している。sidecar を既定有効にすると metadata が増えるため、既定では書き込まない。
次の変更で `entries` を optional / omitted にして node tree を source of truth に移すための remote encoding、sync、gc、root size の参照経路は通してある。

残る大きな変更:

- node をトップレベルだけでなく階層全体の Merkle directory tree にする。
- restore / diff / fsck を node traversal で動作させる。
- old flat tree と new node tree の混在 timeline を明示的に検証する。

## 2026-06-18 v2 opt-in 実装

`MAJUTSU_TREE_FORMAT=v2` を指定した snapshot / key rotation では、root tree manifest の `version` を 2 にし、flat `entries` を省略する。
entries は `root_node.node_key` が指す `TreeNodeManifest` から展開する。

対応済み:

- v1 の flat `entries` は引き続き読める。
- v2 tree は `entries` なしで local restore / clone restore できる。
- `snapshot_state` / compact snapshot hydrate / fsck payload scope / `root size` / root size summary / sync live key 計算は root node から entries を展開する。
- canonical remote encode-decode は tree node manifest を tree manifest と区別して扱う。
- clone 時の GC mark 検証では `objects/trees/nodes/` と `trees/nodes/` を tree metadata として扱う。

まだ v2 は opt-in のままにする。現時点の node は root node が flat entries 全体を保持するため、root tree object は小さくなるが、metadata 全体の根本削減は階層 Merkle node 化まで完了してから得られる。

## restore bundle 案

path 指定 restore 向けに、任意の restore index を書く案がある。

```text
hosts/<host>/restore-index/<snapshot>.cbor.zst.enc
```

これは `root/path-prefix -> required manifest/chunk keys` の mapping を持つ。`mj restore --root X --path Y` の小 object GET fanout を減らせる可能性がある。
