# Ethernet上で独自L3とUDP/IPv4を比べる

Ethernetは両方式が使うL2なので、比較対象は同じEthernet上のLinux UDP/IPv4とする。独自L3の優先制御・期限・送信枠・時計同期を含めた実装全体の比較。ヘッダの違いだけを測るものではない。

実測値は[2026-09-28の結果](results-2026-09-28.md)を参照。

この比較は`--delivery deadline`を明示し、以前と同じ配送条件を維持する。既定の[信頼性配送](reliable-delivery.md)とTCPの比較ではない。

## 準備する

Ubuntu 24.04の場合、ターミナルで次を実行する。Rustが入っている環境ではRustの導入は省略する。

```bash
sudo apt-get update
sudo apt-get install -y build-essential curl docker.io python3
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/amitoki-rustup.sh
sh /tmp/amitoki-rustup.sh -y
. "$HOME/.cargo/env"
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"
```

`docker`グループを追加した場合はログインし直す。リポジトリのルートで実行する。

```bash
cargo test -p amitoki-l3-lab --locked
bash scripts/compare-l3.sh
```

スクリプトはDockerの内部ネットワークと2つのコンテナを作り、終了時に削除する。両端は別のnetwork namespaceなので、UDPはLinuxのIP・UDPスタックと仮想Ethernetを通る。localhost内の折り返し通信ではない。Dockerイメージだけは再利用する。

標準は4条件×2方式×3回、各回の負荷生成は3秒。順番による偏りを減らすため、反復ごとにUDPとL3の実行順を逆転する。短い確認とCIでは次を使う。

```bash
L3_REPETITIONS=1 L3_DURATION_MS=1000 bash scripts/compare-l3.sh artifacts/l3-compare/quick
```

出力は`artifacts/l3-compare/<JST日付>/<時刻>/`。`report.md`、全計測値の`measurements.json`、成否・カーネル・実行ファイルSHA256の`verification.json`、各条件の設定とログを保存する。既存の出力先は上書きしない。

## 条件と数値を読む

| 条件 | 短文 | bulk | 送信側の帯域制限 |
|---|---|---|---|
| `idle` | 128B × 100件/秒 | なし | L3は100MB/s、UDPはなし |
| `short_10k` | 64B × 10,000件/秒 | なし | 同上 |
| `bulk_10k` | なし | 1400B × 10,000件/秒 | 同上 |
| `mixed_1mbps` | 128B × 100件/秒 | 1200B × 500件/秒 | 両方式1Mbps |

両端ともRustで、同じ本文サイズ・予定到着率・期限を使う。短文20ms、bulk200ms。本文を受けたら8バイトのfingerprintをACKで返す。再送と複製は無効。UDP側は32バイトの識別ヘッダ、L3側は64バイトのヘッダを使う。両方式の本文に同じ意味のACKを返すが、UDP側にはcreditや時計交換がない。

1Mbps条件のL3は既存の優先キューとbyte token bucket、UDPはLinux TBFと192件のFIFOを使う。L3は期限切れを途中で捨て、UDPは期限後もキューを排出する。L3の総キュー容量も192件だがクラス別に枠を分ける。**帯域制限の実装と配送仕様が違うため、結果を「IPヘッダを外すだけの効果」とは解釈しない。** UDPにも優先キュー・期限制御を実装した比較は別の検証になる。

期限内ACK率の分母は送信予定数。予定時刻からACKまでをRTTとして、送信待ちも含める。p99は成功したものだけなので、ACK率が低い条件のp99だけを比べない。有効MbpsとACK ppsの分母は生成開始から終了までの計測時間で、最後の最大200msのACK待ちも含む。同期とcreditの開始処理は計測開始前に完了する。

この試験の10,000件/秒は固定負荷。無損失の最大ppsを探索する試験ではない。L3のGRANTは最大256件×32枠で、使用済みの記録を期限まで保持するため、高ppsでは空き枠が制約になる。`no_credit`と受信側の`throttled`なども読む。

物理NICの線速度、CPUを固定した長時間の性能、AF_XDP、ハードウェアタイムスタンプは未検証。ホスト負荷で変わる性能値にCIの合否閾値は設けず、配送数・本文・期限・時計同期の整合性を検査する。

## 別カーネルのVMで確認する

x86_64 LinuxとKVMを使う。Dockerの試験とは別に、新しい2台のUbuntuゲストを作る。既存の`.vm-lab`、DB、プラグイン配備には接続しない。Ubuntu 24.04ホストでの準備は次のとおり。

```bash
sudo apt-get install -y qemu-system-x86 qemu-utils genisoimage openssh-client
sudo usermod -aG kvm "$USER"
```

`kvm`グループを追加した場合はログインし直す。既存のVMラボ用イメージがなければ取得する。日付付きの公式イメージを固定し、スクリプト内でもSHA256を照合する。

```bash
mkdir -p "$HOME/.cache/amitoki-vm"
curl -fL https://cloud-images.ubuntu.com/releases/noble/release-20260801/SHA256SUMS \
  -o "$HOME/.cache/amitoki-vm/SHA256SUMS"
curl -fL https://cloud-images.ubuntu.com/releases/noble/release-20260801/ubuntu-24.04-server-cloudimg-amd64.img \
  -o "$HOME/.cache/amitoki-vm/ubuntu-24.04-server-cloudimg-amd64.img"
cargo build --release -p amitoki-l3-lab --locked
python3 experiments/l3/comparison/verify_vm.py --directory artifacts/l3-vm/run1
```

各VMは2vCPU・1GiB RAM。専用のSSH鍵とディスクを指定ディレクトリ内に作り、ディレクトリは所有者だけが読める。起動時にパッケージを追加しない。QEMUの管理接続は127.0.0.1の空きportを使う。実験リンクはvirtio-netとQEMU socket backendで直結する。OS時計の変更や模擬offsetは使わない。

最初は実験リンクにIPを付けず、異なるboot IDの2VM間でL3同期と期限付き300件の送受信を確認する。同じリンクで信頼性配送の順序なし300件・順序あり300件を送り、本文と重複・順序も照合する。次に192.0.2.1/30・192.0.2.2/30を付けてUDPで測る。これは低負荷時の機能確認で、物理NICの性能比較ではない。

終了・失敗時はこの実行が起動したVMだけを停止する。結果と再調査用のディスクは残す。共有するのは`report.md`・`measurements.json`・`verification.json`だけにし、ディレクトリ全体にはSSH秘密鍵が含まれるため添付しない。VM試験はKVMが必要なので通常のCIには含めない。
