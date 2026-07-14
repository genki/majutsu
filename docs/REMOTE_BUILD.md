# Remote build

Majutsuのコンパイル負荷と大きなCargo target directoryは、`machinaai`の
remote builderへ分離できる。SSHを認証・制御経路に使い、外部公開するbuild
APIは設けない。

## 構成

- macOS ARM64はmachinaai上でnative buildとtestを行う。
- macOS x86_64はRustのcross targetでbuildする。
- Linux ARM64/x86_64はmachinaai上のARM64 Lima/Colima環境から
  `cargo-zigbuild`でbuildする。Linux ARM64は同環境でtestも実行できる。
- Windowsのbuild/testはwinvrで行う。
- `sccache`は20 GiBを上限とし、Cargo incremental compilationは無効にする。
- ソースはtargetごとの安定したpathへ同期し、Rustのsccache keyを再利用可能にする。
- Cargo targetは一時directoryとして扱い、成果物回収後に削除する。
- 同じtargetのjobは排他実行し、host負荷の無制限な増加を防ぐ。

## 利用方法

初回またはworker更新時の準備:

```sh
scripts/setup-remote-builder.sh
```

macOS ARM64 release build:

```sh
scripts/remote-build.sh aarch64-apple-darwin release
```

Linux x86_64 release build:

```sh
scripts/remote-build.sh x86_64-unknown-linux-gnu release
```

macOS上でtest:

```sh
scripts/remote-build.sh aarch64-apple-darwin test
```

macOSまたはLinux ARM64上でclippy:

```sh
scripts/remote-build.sh aarch64-apple-darwin clippy
scripts/remote-build.sh aarch64-unknown-linux-gnu clippy
```

成果物は既定で`target/remote-dist/<job-id>/`へ回収される。保存先は
`MAJUTSU_REMOTE_BUILD_OUT`、SSH host aliasは`MAJUTSU_BUILD_REMOTE`で変更できる。

回収したLinux binaryをローカルで再コンパイルせずrelease archiveにする場合:

```sh
MAJUTSU_REMOTE_BUILD_OUT=/tmp/majutsu-release \
  scripts/remote-build.sh x86_64-unknown-linux-gnu release
MAJUTSU_PREBUILT_BIN=/tmp/majutsu-release/mj \
MAJUTSU_PACKAGE_PLATFORM=linux-x86_64 \
  scripts/package-release.sh
```

## 制約

cross buildが成功しても対象OSでの動作を証明するものではない。LinuxはColima内と
このLinux環境、Windowsはwinvr、macOSはmachinaaiまたはmba22でsmoke testと
E2Eを実施する。
