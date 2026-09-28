# 信頼性配送と通信グループの順序保証

`amitoki-l3 bench`は既定で信頼性のある順序なしメッセージ配送を使う。ACKまで本文を送信側に保持し、欠けたものだけを再送する。完成した別のメッセージは欠落を待たずに受信アプリへ渡す。

通信グループを`channel`と呼ぶ。順序が必要なchannelには`ordered`を指定する。同じchannelの中ではsequence順に渡し、別channelの配送は継続する。優先クラス（short/bulk）と順序指定は別の概念。ライブラリでは同じ優先クラスに複数のchannelを作れる。

## CLIで試す

[ネットワーク設定](README.md#ヘッダは64バイト経路は静的に指定する)の送信側を`a.json`、受信側を`b.json`として、ルーターを起動した環境で使う。raw socketを開く権限が必要。

```bash
# 受信側。delivery-logは任意。本文を含むので共有範囲に注意する。
target/release/amitoki-l3 receiver --config b.json \
  --duration-ms 30000 --receive-window 64 \
  --output receiver.json --ready receiver-ready.json --delivery-log deliveries.jsonl

# 送信側。shortは順序なし、bulkだけ順序あり。
target/release/amitoki-l3 bench --config a.json --peer 2 \
  --short-rate 100 --bulk-rate 100 --bulk-ordering ordered \
  --delivery-timeout-ms 10000 --pending-limit 256 --output sender.json

# 従来の期限付き配送。
target/release/amitoki-l3 bench --config a.json --peer 2 \
  --delivery deadline --short-deadline-us 20000 --output deadline.json
```

送信側・受信側は別ターミナルで実行する。CLIの負荷生成ではchannel 1がshort、channel 2がbulk。各channelの順序は`--short-ordering` / `--bulk-ordering`で`unordered`か`ordered`を選ぶ。本文はshort最大240B、bulk最大1384B。共通MTU内に16Bの配送情報を追加するため、deadlineモードより本文の上限が小さい。

`--delivery-timeout-ms`は送信キューへ受け付けてからの待ち時間で、既定10秒・最大60秒。信頼性モードは`--retries`を使わず、この時間まで再送する。`--replica-bytes-per-second`は全channelの再送が共有する帯域枠で、0は指定できない。2経路を指定すると再送先を交互に変える。deadlineモードの短文同時複製とは異なる。

## 受付と完了の意味

```mermaid
flowchart LR
    App[送信アプリ] -->|try_send| Send[有界送信キュー]
    Send -->|DATA・選択再送| Router[独自L3ルーター]
    Router --> Receive[グループ別の有界受信窓]
    Receive -->|受付ACK| Send
    Receive -->|take_delivery| Peer[受信アプリ]
```

ACKは**相手の受信キューへの受付**を表す。アプリの処理完了、ファイルへの永続化、再起動をまたぐ業務処理の一回性を意味しない。orderedでは後続のメッセージを受け付けてACKした後、先行する欠落を待つ場合がある。

timeout、受信プロセスの再起動、時計基準の世代変更では、そのchannelを終了し、未ACK分を`unconfirmed`として報告する。ACKだけが失われて受信済みの可能性もあるため、未配送とは断定しない。失敗後に同じ本文を新しいsessionへ自動で流し直さない。アプリが再実行する場合は、必要に応じて業務側のIDで重複を防ぐ。

一つでもtimeoutになったchannelを閉じるのは、orderedで抜けたsequenceを残したまま後続を追加し続けないため。別channelの状態は変更しない。

## 送信待ちと受信窓

送信キューはchannelごとに既定・最大256件。満杯なら`try_send`が`WouldBlock`を返し、本文を勝手に捨てず、sequenceも消費しない。呼び出し側が保持して後で再試行する。CLIの負荷生成も生成位置を進めずに待つ。

受信窓はchannelごとに既定64件、最大256件。`take_delivery`でアプリが読み取った分を解放し、連続して解放できたsequenceまで窓の先頭を進める。unorderedで先行する欠落があると、後続をアプリへ渡しても、その記録は欠落が埋まるまで窓内に残る。これにより履歴を無制限に増やさず、古い再送を再配送しない。

ACKは現在の窓の先頭も伝える。窓の更新が失われた場合や、ACK後にアプリが読み取った場合も、100msごとのOPEN/READY交換で空き枠を再取得できる。

受信側のchannel数は最大128。送信元・session・channelで識別し、満杯でも既存の履歴を追い出さない。30秒通信がないchannelは失効し、未読の本文数を`abandoned_messages`に記録する。失効後は新しい受信世代を割り当て、旧DATAにはRESETを返す。受信側の毎秒受付件数は既存の`--short-credits-per-second` / `--bulk-credits-per-second`で制限し、新規DATAだけがその枠を消費する。

各再送は20msから始めて待ち時間を倍増し、最大1秒に抑える。再送回数に固定上限はない。送信元で1回のパケットの滞留期限を200msに設定し、各ルーターではその期限を延ばさない。期限切れのパケットを捨てても、送信側に保持した論理メッセージは次の再送で送れる。同期断の間も保持を続け、同じ時計世代で復帰すれば再送する。同期断の時間も配送timeoutに含む。

固定の窓・受信レート・再送帯域枠・指数backoffを使う実験実装。TCPのような適応的な輻輳制御を実装したわけではなく、TCPより速いという測定結果もない。

## ワイヤ形式

共通ヘッダは64B、versionは1のまま。kindを追加した。旧実行ファイルは未知のkindを拒否するため、利用には全ノードの更新が必要。

| kind | 値 | message | credit | 16Bの情報に続く本文 |
|---|---:|---|---|---|
| RELIABLE_OPEN | 7 | 1 | 0 | なし |
| RELIABLE_READY | 8 | 受信窓の先頭 | 窓の大きさ | なし |
| RELIABLE_DATA | 9 | channel内のsequence | 0 | アプリの本文 |
| RELIABLE_ACK | 10 | 対象sequence | 受信窓の先頭 | 本文のfingerprint、8B |
| RELIABLE_RESET | 11 | 要求から引き継ぐ | 0 | なし |

本文先頭の16Bは`channel:u32 / ordering:u8 / reserved:3B / epoch:u64`。整数はnetwork byte order、orderingは0がunordered・1がordered。epochは初回OPENだけ0、READYで受信側が発行し、以後は同じ値を使う。受信プロセスの起動ごとに乱数を使い、channelの再作成でも世代を更新する。classとorderingはchannel作成後に変更できない。

checksum・fingerprint・世代番号は送信元認証ではない。信頼できるprivate networkの実験用であり、暗号化や攻撃者に対する配送保証は含まない。

## Rustから利用する

実験crateの`delivery`モジュールを使う。本体のRelay/Stage SDKへの統合はまだ行っていない。

1. `ChannelOptions::new(source, destination, session)`を作り、channel・class・orderingを設定する。sessionは起動ごとに新しい非ゼロ値を使う。
2. `Channel::try_send(&payload, now)`でsequenceを取得する。`WouldBlock`なら本文を保持する。
3. `Channel::poll(now, reading, &mut retry_budget)`が返すPacketをNetworkへ渡す。時計・hop・経路・有界転送キューは既存のNetworkが検証する。
4. 検証済み応答を`Channel::receive`へ渡す。正しいACKの場合だけ受付を確認できたsequenceが返る。
5. 受信側は検証済みPacketを`Receiver::receive`へ渡し、`take_delivery`で本文を読む。同じイベント内で読み取った分は`refresh_ack`でACKへ反映してから送信する。

送信側は`state()`と`metrics.unconfirmed`も確認する。任意に中断するときは`abort()`で未ACK分を未確認として終了する。受信側の`prune(now)`は通信が途切れている間も定期的に呼ぶ。時刻・Packetのネットワーク検証を含む呼び出し例は`src/runtime/reliable.rs`と`src/runtime/node.rs`にある。

## 検証

```bash
cargo test -p amitoki-l3-lab --locked
L3_SUITE=reliable bash scripts/test-l3.sh artifacts/l3/reliable
```

外部非接続の4プロセス・2経路で、先頭DATAの欠落（ordered/unordered）、別channelの継続、ACK消失、受信速度制限と小さい窓、主経路断、channel単独timeout、受信プロセス再起動、異なる時計・drift、同期断からの復帰を検証する。本文・sequence・重複・順序・有界キュー・明示失敗を照合する。失敗が期待される条件は終了コードが非ゼロであることも検査する。

`test-l3.sh`の既定とCIはdeadlineの18条件と、この10条件を実行する。`L3_SUITE=deadline`なら従来条件だけ。別カーネルの確認は`python3 experiments/l3/comparison/verify_vm.py --directory artifacts/l3-vm/reliable`で、時計を模擬しない2台のKVMゲスト間に順序なし300件・順序あり300件を送る。

CLIレポートの`acknowledged`は受信受付数、`delivered`は受信側がアプリへ渡した数。`unsubmitted`は生成予定のうち送信キューへ入れられなかった数で、成功率の分母から除かない。ACK時間は送信キューへの受付から測り、その前の生成待ちは含めない。CLI全体の`elapsed_us`には生成待ち・接続準備・最後の再送待ちを含む。
