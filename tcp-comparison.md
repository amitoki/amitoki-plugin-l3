# 信頼性配送とTCPを比べる

独自L3の順序なし配送・channel内の順序あり配送と、Linux TCPの1接続・クラス別2接続を比較する。全方式で同じ本文、予定送信数、受信記録を使う。TCPにもメッセージ境界と受領確認を付け、届いた本文のサイズ・fingerprint・sequenceを照合する。

[2026-09-28の60試行の結果](tcp-results-2026-09-28.md)では、L3の混雑時と短文高負荷に未完了を確認した。既定の3秒比較はこの問題も結果として記録する。

## 実行する

Linux、Rust、Docker Engine、Python 3を使う。Ubuntuの導入コマンドは[比較環境の準備](comparison.md#準備する)を参照。リポジトリのルートで実行する。

```bash
bash scripts/compare-l3-tcp.sh
```

5条件×4方式×3回、各回の負荷生成は3秒。作成したDockerの内部ネットワークと2コンテナだけを使い、終了時に削除する。両端を別CPUへ固定し、方式の順番をseed付きで入れ替えて、順番に実行する。標準の出力先は`artifacts/l3-tcp/<JST日付>/<時刻>/`。既存ディレクトリは上書きしない。

```bash
L3_TCP_QUICK=1 L3_REPETITIONS=1 L3_DURATION_MS=1000 \
  bash scripts/compare-l3-tcp.sh artifacts/l3-tcp/quick
```

CIは上の短縮版でidle・mixed・lossを各4方式、計12試行する。速度には合否閾値を置かず、欠落・本文の破損・重複・順序違反・時計の不一致を失敗にする。全メッセージの受付完了を最大10秒追加で待つ。失敗した試行のログも残る。

配送timeoutは残りの条件の測定を続け、最後に未完了件数とともに非0で終了する。reportには配送数とACK確認数を分けて記録し、未完了の性能値を成功試行の中央値へ混ぜない。

## 負荷と方式

| 条件 | 短文 | bulk | A→Bのnetem |
|---|---|---|---|
| `idle` | 128B × 100件/秒 | なし | なし |
| `short_10k` | 64B × 10,000件/秒 | なし | なし |
| `bulk_10k` | なし | 1200B × 10,000件/秒 | なし |
| `mixed_10mbps` | 128B × 100件/秒 | 1200B × 1500件/秒 | 10Mbps、遅延1ms |
| `loss_1pct` | 128B × 100件/秒 | 1200B × 500件/秒 | 10Mbps、遅延1ms、ランダム損失1% |

両端のTSO/GSO/GROを無効化して確認する。netemのキュー上限は512パケット、逆向きには制限を付けない。損失位置は固定しない。iproute2 6.1にnetemのseed指定がないため、実行順のseedと損失の乱数は別扱いにする。各試行の`qdisc.json`に実際の送信・破棄数を残す。再送やフレーム数が違うため、損失が当たるメッセージも方式間で異なる。

- `l3_unordered`: short/bulkを別channelにし、両方で順序なし配送。
- `l3_ordered`: short/bulkを別channelにし、それぞれのchannel内で順序あり配送。
- `tcp_single`: short/bulkを1本のTCP接続へ載せる。
- `tcp_split`: short/bulkを別のTCP接続へ載せる。bulkの損失によるstream内の順序待ちを短文から分離する。

TCPは`TCP_NODELAY`を有効にし、Linux既定のsocket buffer・輻輳制御を使う。使用した輻輳制御名は送信側reportに記録する。アプリのフレームは16Bのヘッダと本文。受信アプリは本文fingerprintを含むACKを返す。TCP自体のACKとは別である。TCPの順序・streamの性質は[Linux tcp(7)](https://man7.org/linux/man-pages/man7/tcp.7.html)、損失・帯域設定は[tc-netem(8)](https://man7.org/linux/man-pages/man8/tc-netem.8.html)を参照。

L3は64Bの共通ヘッダと16Bの信頼性配送prefixを使う。送信上限100MB/s、受信creditは両クラス100,000件/秒、その他は現行の既定値。送信待ちは両方式とも各クラス256件だが、L3の受信窓は64件、TCPはLinuxの窓・bufferを使う。L3は固定20msを起点とした再送待ちと30,000B/秒の再送予算、TCPはLinuxの再送・輻輳制御を使う。buffer、ACKの位置、再送戦略、ヘッダを含む実装全体の比較になる。

## 数値の意味

主要な遅延は**予定生成時刻から受信アプリへ渡すまでの片道時間**。senderの開始時刻とsequenceから予定時刻を復元する。送信側の待ち、接続・L3時計同期の準備、受信側の順序待ちも含む。損失や混雑で送れなかったメッセージを分母から除かない。

ACKの所要時間を主要指標にはしない。L3のACKは順序待ちに入った段階でも返すため、TCPの受信アプリのACKと直接比べると意味が違う。受信記録`receipts.jsonl`を全件検証し、各方式の受信アプリへ渡った時刻からp50/p99/最大値と20ms以内の割合を出す。

送受信は同じホストの`CLOCK_BOOTTIME`を使う。boot ID・time namespace由来のclock domainが一致し、模擬offset/driftが0であることを検査する。この片道計測をそのまま別VM・別ホストへ流用しない。

時計同期が影響しているかを切り分ける場合は、ビルド後に以下を実行する。標準比較の結果とは別に保存する。同一ホストの時計を直接使う既存機能であり、別マシンへ適用する修正ではない。

```bash
python3 experiments/l3/comparison/reliable.py \
  --directory artifacts/l3-tcp/local-clock-diagnostic \
  --workload mixed_10mbps --mode l3_unordered --clock-mode local \
  --duration-ms 3000 --repetitions 3
```

有効Mbpsは全本文ビット数を最後のアプリ配送までの時間で割る。**固定の予定レートを処理した結果であり、無損失の最大速度・最大ppsではない。** CPUは送受信プロセスに計上されたuser+system時間を配送件数で割る。本文生成・照合・記録を含み、別のkernel thread等に計上された処理は含まない。

`summary.json`は各試行のp99等の中央値・最小・最大、`measurements.json`は全計測値と両端report、`verification.json`は成否・実行回数・CPU割当・カーネル・バイナリSHA256を記録する。全試行をまとめた一つのp99ではない。3秒×3回は探索用で、物理NICや長時間安定性の検証は別途必要。
