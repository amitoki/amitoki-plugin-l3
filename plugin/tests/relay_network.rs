use amitoki_plugin_l3::manifest::manifest;
use amitoki_plugin_sdk::{
    relay::{Frame, Relay, RelayContext},
    ProcessRelay,
};
use bytes::Bytes;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::{Child, Command},
    time::Duration,
};

struct Broker(Child);
impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn connect(directory: &Path, index: usize) -> Result<ProcessRelay, amitoki_plugin_sdk::relay::RelayError> {
    ProcessRelay::connect(
        Path::new(env!("CARGO_BIN_EXE_amitoki-plugin-l3")),
        &manifest(),
        (
            RelayContext {
                node_id: format!("test-{index}"),
                channel: "integration".into(),
            },
            serde_json::json!({"socket_path":directory.join(format!("{index}.sock"))}),
        ),
    )
    .await
}

fn assert_plugins_have_no_capabilities() {
    let parent = format!("PPid:\t{}", std::process::id());
    let mut count = 0;
    for entry in std::fs::read_dir("/proc").unwrap().flatten() {
        let Ok(status) = std::fs::read_to_string(entry.path().join("status")) else { continue };
        if !status.lines().any(|line| line == parent) {
            continue;
        }
        let Ok(arguments) = std::fs::read(entry.path().join("cmdline")) else { continue };
        if arguments.split(|byte| *byte == 0).next() != Some(env!("CARGO_BIN_EXE_amitoki-plugin-l3").as_bytes()) {
            continue;
        }
        assert!(status.lines().any(|line| line == "CapEff:\t0000000000000000"), "{status}");
        count += 1;
    }
    assert_eq!(count, 3);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "scripts/test-plugin.shでIPなしの専用veth内に実行する"]
async fn three_unprivileged_plugins_relay_large_frames_and_deduplicate_retries() {
    let directory = PathBuf::from(std::env::var("L3_TEST_DIRECTORY").expect("専用ネットワークの設定ディレクトリ"));
    let mut brokers = Vec::new();
    for index in 1..=3 {
        let child = Command::new(env!("CARGO_BIN_EXE_amitoki-l3-broker"))
            .args([
                "--config",
                directory.join(format!("{index}.json")).to_str().unwrap(),
                "--socket",
                directory.join(format!("{index}.sock")).to_str().unwrap(),
            ])
            .spawn()
            .unwrap();
        brokers.push(Broker(child));
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !(1..=3).all(|index| directory.join(format!("{index}.sock")).exists()) {
        assert!(tokio::time::Instant::now() < deadline, "補助プロセスが起動しませんでした");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let a = connect(&directory, 1).await.unwrap();
    let b = connect(&directory, 2).await.unwrap();
    let c = connect(&directory, 3).await.unwrap();
    assert_plugins_have_no_capabilities();
    assert!(connect(&directory, 1).await.is_err(), "同じノードを同時に占有できてはいけません");
    let relays = [&a, &b, &c];
    let frames: Vec<Vec<Frame>> = (0..3)
        .map(|source| [14, 64, 1514, 65535].into_iter().map(|size| Frame::new(Bytes::from((0..size).map(|offset| (offset + source) as u8).collect::<Vec<_>>())).unwrap()).collect())
        .collect();
    let (first, second, third) = tokio::join!(a.publish(&frames[0]), b.publish(&frames[1]), c.publish(&frames[2]));
    first.unwrap();
    second.unwrap();
    third.unwrap();
    for (index, relay) in relays.iter().enumerate() {
        let expected: HashMap<_, _> =
            frames.iter().enumerate().filter(|(source, _)| *source != index).flat_map(|(_, batch)| batch.iter()).map(|frame| (frame.id, frame.clone())).collect();
        let received = relay.receive(128).await.unwrap();
        assert_eq!(received.len(), expected.len());
        for delivery in &received {
            assert_eq!(expected.get(&delivery.frame.id), Some(&delivery.frame));
        }
        assert_eq!(relay.receive(128).await.unwrap().len(), expected.len());
        let receipts: Vec<_> = received.iter().map(|delivery| delivery.receipt.clone()).collect();
        relay.acknowledge(&receipts).await.unwrap();
        relay.acknowledge(&receipts).await.unwrap();
        assert!(relay.receive(128).await.unwrap().is_empty());
    }
    let (first, second, third) = tokio::join!(a.publish(&frames[0]), b.publish(&frames[1]), c.publish(&frames[2]));
    first.unwrap();
    second.unwrap();
    third.unwrap();
    for relay in relays {
        assert!(relay.receive(128).await.unwrap().is_empty());
    }
    // Dropで子プロセスのIPCを閉じ、補助プロセスのleaseを解放する。
    drop((a, b, c));
    println!("3 nodes: 12 published / 24 delivered; 14..65535 B; retries and repeated ACKs idempotent; plugin CapEff=0");
    drop(brokers);
}
