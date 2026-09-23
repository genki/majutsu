# mjのmaster／slave構成

## 保護モデル

`0.8.0`のcluster profileでは、`mba22`をデータ保持専用master、`vagrant`と
`win-vr`をデータ生成側slaveとして動かす。

通常のクラッシュ保全は、slaveのローカルデータ／永続queueとmasterのローカルstoreの
二重化で成立させる。S3/GCSはslaveとmasterを同時に失う場合に備えた二次バックアップであり、
slaveへのACKを返すための同期的な必須条件ではない。

```text
slave local／queue
    │ 認証付きbatch RPC
    ▼
master local store ── master_durable ACK ──▶ slave queue完了
    │
    └── 遅延・集約された非同期worker ──▶ S3/GCS secondary backup
```

masterは受信、永続保持、復元提供、S3二次転送だけを行う。cluster profileではmaster自身の
rootを監視せず、snapshotや通常syncも実行しない。

## standaloneとの境界

`MAJUTSU_CLUSTER_ROLE=master`を設定しない既存installationはstandalone profileであり、
従来どおりローカル監視、snapshot、S3直結sync、clone／restoreを行う。

cluster profileは設定で明示する。現在接続中のslaveが0台になってもstandaloneへ自動切替
しない。一時的な通信断でACKの意味やS3転送方針が変わることを防ぐためである。

cluster master roleで`mj watch`を起動すると、watch daemonの管理方式を保ったまま
データ保持serverだけを起動する。master roleでの`mj snapshot`と`mj sync`は誤更新防止の
ため拒否される。slaveを構成しない単体運用へ戻す場合は、S3 backlogをflushしてから
`MAJUTSU_CLUSTER_ROLE`を解除する。

## 永続化とACK

slaveからPUTを受信したmasterは、次の順序を完了してからprotocol v2の
`master_durable` ACKを返す。

1. key、host identity、frame上限を検証する。
2. `cluster/store`へpayloadをatomic writeし、fileと新規directory階層を同期する。
3. 同じkeyのS3転送予定を`cluster/backend-queue`へatomic writeする。
4. queue directoryを同期してからACKを返す。

応答だけを失った場合、slaveは同じobjectを再送する。content-addressed objectはdigestを
照合して冪等に扱い、同じkeyで内容が異なる場合はintegrity errorとする。mutable refは
key単位のqueue markerへcoalesceされ、S3には最新値が送られる。

既定pathは次のとおりである。

```text
~/.majutsu/cluster/store
~/.majutsu/cluster/backend-queue
~/.majutsu/runtime/cluster-master-status.json
```

`MAJUTSU_CLUSTER_STORE_DIR`と`MAJUTSU_CLUSTER_BACKEND_QUEUE_DIR`で変更できる。
`MAJUTSU_CLUSTER_STORE_MAX_BYTES`を設定した場合、書込み後の使用量が上限を超えるPUTには
ACKを返さない。OSの容量不足でも同様にslave queueが保持される。

## remote設定

masterは二次バックアップ先のS3/GCSを従来どおり設定する。

```toml
[remote]
url = "s3://majutsu-vagrant-winvr-s21g-twpro-20260608/majutsu-encrypted"
```

slaveはmaster endpointをremoteにする。

```toml
[remote]
type = "cluster"
endpoint = "mba22.local:4581"
```

slaveの`mj sync`、`mj remote check`、clone／restoreはmasterへ接続する。master-localに
objectがあればそこから読み、移行前の履歴などmaster-localにないobjectだけS3へfallbackする。
fallbackで取得した完全objectは容量に余裕があればmaster storeへhydrateされる。

## masterの起動

watch daemonとして運用する場合はmasterのenv fileへ次を設定する。

```text
MAJUTSU_CLUSTER_ROLE=master
MAJUTSU_CLUSTER_LISTEN=0.0.0.0:4581
MAJUTSU_CLUSTER_TOKEN_DIR=/Users/takiuchi/.majutsu/cluster-tokens
```

token directoryには`<host-id>.token`という名前で、slaveごとに異なるtokenを置く。例えば
`vagrant.token`と`winvr.token`を置き、Unixではdirectoryを`700`、各fileを`600`にする。
slave側には自身のhost idと対応するtokenだけを設定する。

```text
MAJUTSU_CLUSTER_HOST_ID=vagrant
MAJUTSU_CLUSTER_TOKEN_FILE=/home/vagrant/.majutsu/cluster.token
```

通常のdaemon起動手順を使う。

```sh
mj --home /Users/takiuchi/.majutsu daemon restart
```

foregroundで確認する場合は次を使う。このcommandはrole envの有無にかかわらず
データ保持serverとして動く。

```sh
mj --home /Users/takiuchi/.majutsu cluster serve --listen 0.0.0.0:4581
```

## S3二次転送とコスト

S3転送workerはリアルタイム性よりrequest削減とcoalesceを優先する。既定では次の条件で
flushを開始する。

- 最古queue itemが1時間に達したとき。
- 未送信payloadが1 GiBに達したとき。
- `mj cluster flush`で明示要求されたとき。

workerは1回に最大1024 itemをcontent、metadata、mutable refの順で送る。conditional PUTが
まとまっている場合はhost prefixを一度LISTし、S3に存在するimmutable objectをPUTせずに
queue完了とする。DELETEはS3 multi-object deleteへ最大1000 keyずつまとめ、providerが
対応しない場合だけ、そのprocessでは個別DELETEへfallbackする。S3互換XML APIが要求する
`Content-MD5`をbodyから計算し、署名対象headerとして送る。失敗時はjitter付き指数backoffを
記録し、master-local storeとbackend queueを保持したまま自律再送する。

clone／restoreなどの読取りはforeground、二次backup転送はbackgroundとして扱う。共通の
concurrency gateではforeground waiterを優先するため、backlog flush中も復旧処理が長時間
待たされない。

次のenvで運用値を調整できる。

```text
MAJUTSU_CLUSTER_BACKEND_FLUSH_INTERVAL_SECS=3600
MAJUTSU_CLUSTER_BACKEND_FLUSH_MAX_BYTES=1073741824
MAJUTSU_CLUSTER_BACKEND_FLUSH_MAX_OBJECTS=1024
MAJUTSU_CLUSTER_BACKEND_POLL_INTERVAL_SECS=30
MAJUTSU_CLUSTER_BACKEND_MAX_CONCURRENCY=4
MAJUTSU_CLUSTER_BACKEND_DEDUPE_LIST_MIN_OBJECTS=32
MAJUTSU_CLUSTER_BACKEND_DELETE_BATCH_MAX_OBJECTS=1000
```

RPC batchだけでは新規objectのS3 PUT回数自体は減らない。ただしmjの既存pack、
content-addressed dedupe、flush単位のLIST、mutable ref coalesce、長いLIST cacheにより、
同期critical pathからS3 APIを外しつつ再PUT、HEAD、DELETE requestを抑える。

cluster slaveのremote pruneも無効にはしない。slaveが24時間間隔でretention集合を計算し、
削除RPCをmasterへ送る。S3のLIST／DELETEと実行主体はmasterへ集約されるため、slaveがS3へ
直接接続することはない。

## master-local retention

slaveからDELETEを受けたkeyは、GET／Exists／Listからただちに隠す。一方、master-localの
payloadはS3側DELETEの成功後も既定7日間保持し、`cluster/local-gc`の永続tombstoneが期限を
迎えてから削除する。これにより、誤ったretention結果がslaveとmasterの双方から同時に実体を
消すことを避ける。

保持期間中に同じkeyが再PUTされた場合はtombstoneを取消し、S3への再送を必須にする。
local GCは、保持期限の経過、同じkeyのbackend queueがないこと、payload digestがDELETE時と
一致することをすべて確認してから実行する。

```text
MAJUTSU_CLUSTER_LOCAL_DELETE_GRACE_SECS=604800
```

## 状態確認と手動flush

masterでは次を確認する。

```sh
mj --home /Users/takiuchi/.majutsu cluster status
mj --home /Users/takiuchi/.majutsu cluster status --json
mj --home /Users/takiuchi/.majutsu cluster flush
mj --home /Users/takiuchi/.majutsu cluster flush --wait-secs 300
mj --home /Users/takiuchi/.majutsu daemon metrics
```

`store_bytes`はmaster-localで保護済みの容量、`backend_queue_*`はS3へ未転送の二次backup lagを
表す。`retained_deleted_*`は論理削除後の安全保持量を表す。`backend_queue_objects=0`なら、
その時点までに受信したqueueはS3へ追いついている。`daemon metrics`はhost別batch、受信量、
duplicate／queue再利用、同一key churn、backend PUT／skip／DELETE／失敗、quota拒否を
Prometheus text formatで出力する。slaveはupload retryとは独立したheartbeat schedulerから
既定60秒ごとに認証済みheartbeatを送る。master statusの`metrics.hosts.<host>.last_heartbeat_at`と、
`majutsu_cluster_host_heartbeat_age_seconds`で、通常のRPC受信時刻
`last_seen_at`とは分離したslave死活を監視できる。heartbeat未対応の旧masterには自動的に
通常動作を継続し、upgrade中の混在を許容する。間隔は
`MAJUTSU_CLUSTER_HEARTBEAT_INTERVAL_SECS`で変更でき、5秒未満または1時間超の値は制限する。
master profileの`mj daemon status`と`mj daemon metrics`もdata-holder状態を返す。

次を設定すると、masterの容量90%以上、S3 lag、retry、quota拒否と、slaveのmaster長時間停止を
外部通知commandへ渡せる。commandには`MAJUTSU_CLUSTER_NOTICE_ISSUES`などの環境変数が付く。

```text
MAJUTSU_CLUSTER_NOTICE_CMD=/path/to/notice-wrapper
MAJUTSU_CLUSTER_NOTICE_RATE_LIMIT_SECS=3600
MAJUTSU_CLUSTER_NOTICE_BACKEND_LAG_SECS=7200
MAJUTSU_CLUSTER_UNAVAILABLE_NOTICE_FAILURES=3
MAJUTSU_CLUSTER_UNAVAILABLE_NOTICE_AFTER_SECS=300
```

通知commandはshellで実行する。wrapperは通知を同期的に送信し、配信成功時だけexit 0を返すこと。
非同期送信して即座にexit 0するcommandでは、後続の配信失敗をmj側で再試行できない。

通知commandは起動前にattempt markerを永続化する。同じissueの通知が失敗した場合は、連続失敗数に
応じて60秒から最大3,600秒まで指数backoffし、成功後は`MAJUTSU_CLUSTER_NOTICE_RATE_LIMIT_SECS`に
従う。既存markerに新しいfieldがなくても読み込み可能である。

slaveでは従来のqueue状態も確認する。

```sh
mj --home ~/.majutsu sync status
mj --home ~/.majutsu health --deep --timeout-secs 30
```

slaveの送信失敗にはjitter付きbackoffが付き、watch daemonのretry schedulerが
未試行queue、またはremote ACK前のdurable journalを検出した時点で起床する。cluster slaveの
retryはlive rootを再走査しない単発のqueue-only syncとして実行し、進捗がなければ30秒
backoffする。1件のsourceがACLなどで読めなくても、他のjournal recordは先に送る。master復帰後の
手動syncは不要であり、恒久的に読めない1件がwatch本体を5分間待機させることもない。

standalone profileのwatch同期は、従来どおり`sync --wait --timeout-secs 300`でS3直結の追従を
待つ。待機を外す変更はcluster remoteだけに限定している。

## 障害時の復旧元

| 障害 | 復旧元／動作 |
| --- | --- |
| slave process／disk単独障害 | master-local store |
| master process単独障害 | 再起動後も残るmaster store／backend queueとslaveの冪等再送 |
| S3停止 | slave→masterの二重化を継続し、master queueから後送 |
| master disk単独障害 | 生存slave。過去履歴はS3 fallback |
| slaveとmasterの同時喪失 | 最後にflush済みのS3二次backup |

S3のRPOはflush intervalと容量thresholdで決まる。低コスト化の代わりに同時喪失時の最新分が
未転送となり得るため、`backend_queue_oldest_created_at`を監視する。

運用上の受入基準は、masterが返す`master_durable` ACK以降の一次保全RPOを0とし、二次S3
バックアップのRPO上限を既定の1時間または1 GiB到達までの短い方とする。master再起動後は
永続store／queueを自動再読込し、slave heartbeatの停止は既定5分の連続失敗後に通知する。
物理ディスク喪失を含むS3復旧のRTOは環境依存なので、リリースごとに隔離bucketでclone／restore
を実測して記録する。未実測値を本番の保証値とは扱わない。

## 認証、network、upgrade

masterでは`MAJUTSU_CLUSTER_TOKEN_DIR`によるhost別tokenを使用する。認証されたhostでも
PUT／DELETE／archive restoreは自身の`<host-id>/` prefixだけに制限され、lifecycle変更は
remote RPCから拒否される。clone／disaster recoveryのため、暗号化objectのread／listは
他host prefixにも許可する。旧共有tokenは移行用fallbackとして読めるが、定常運用には使わない。

接続時には通常operationより先にcapability handshakeを実行し、protocol version、request／
response frame上限、batch件数／payload上限、write prefix、対応operationを照合する。不一致時は
slave queueを保持したままfail-fastする。

tokenはログへ出さない。listenerはLAN／VPN内だけから到達可能にする。TLSは内蔵しないため
public networkへ公開しない。

ACK意味論の変更に伴いcluster protocolはv2である。v1とv2はfail-fastし、slave queueを保持する。
upgrade時はslave daemonを停止してqueueを保持し、masterを`0.8.1`へ更新してからslaveを更新する。
v2ではslaveのremote sync cacheを更新し、同期時の存在判定にはmaster-local専用LISTを使うため、
最初のfull syncで現在参照されるobjectがS3だけに存在していてもslaveからmasterへ再複製される。
全nodeの更新後、各slaveで`mj sync --wait`を完了させ、master-local ACK、S3 lag、
clone／restoreを確認してdaemonを通常運用へ戻す。
