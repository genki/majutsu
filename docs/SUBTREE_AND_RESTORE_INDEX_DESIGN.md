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

## restore bundle 案

path 指定 restore 向けに、任意の restore index を書く案がある。

```text
hosts/<host>/restore-index/<snapshot>.cbor.zst.enc
```

これは `root/path-prefix -> required manifest/chunk keys` の mapping を持つ。`mj restore --root X --path Y` の小 object GET fanout を減らせる可能性がある。
