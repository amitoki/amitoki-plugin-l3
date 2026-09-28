pub mod broker;
mod codec;
mod config;
mod inbox;
pub mod manifest;
mod session;
mod socket_client;
mod worker;
pub use socket_client::L3Plugin;

use amitoki_l3_lab::runtime::Endpoint;
use amitoki_plugin_sdk::{
    relay::{Delivery, Frame, Receipt, Relay, RelayContext, RelayError, RelayPlugin},
    wire::MAX_BATCH,
};
use async_trait::async_trait;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
};

pub(crate) struct RawL3Plugin;

struct L3Relay {
    commands: mpsc::SyncSender<worker::Publish>,
    inbox: Arc<Mutex<inbox::Inbox>>,
    stopped: Arc<AtomicBool>,
    failure: Arc<Mutex<Option<String>>>,
    thread: Option<thread::JoinHandle<()>>,
}

#[async_trait]
impl RelayPlugin for RawL3Plugin {
    fn name(&self) -> &'static str {
        "l3"
    }

    async fn connect(&self, context: RelayContext, options: serde_json::Value) -> Result<Arc<dyn Relay>, RelayError> {
        context.validate()?;
        manifest::network_manifest().validate_options(&options)?;
        let options: config::Options = serde_json::from_value(options).map_err(|_| RelayError::permanent("L3設定を解釈できません"))?;
        options.validate()?;
        let lease = session::claim_node(&context)?;
        let network_lease = session::claim_network(options.network.node)?;
        let endpoint = Endpoint::open(options.network, &options.peers).map_err(|error| RelayError::permanent(format!("L3リンクを開けません: {error}")))?;
        let channel = codec::digest(context.channel.as_bytes());
        let inbox = Arc::new(Mutex::new(inbox::Inbox::new(channel, options.queue_capacity)));
        let stopped = Arc::new(AtomicBool::new(false));
        let failure = Arc::new(Mutex::new(None));
        let (commands, receiver) = mpsc::sync_channel(1);
        let worker = worker::Worker {
            endpoint,
            peers: options.peers,
            channel,
            inbox: inbox.clone(),
            stopped: stopped.clone(),
            commands: receiver,
            failure: failure.clone(),
        };
        let thread = thread::Builder::new()
            .name("amitoki-l3".into())
            .spawn(move || {
                let _leases = (lease, network_lease);
                worker.run();
            })
            .map_err(|error| RelayError::permanent(error.to_string()))?;
        Ok(Arc::new(L3Relay {
            commands,
            inbox,
            stopped,
            failure,
            thread: Some(thread),
        }))
    }
}

impl L3Relay {
    fn check_running(&self) -> Result<(), RelayError> {
        let failure = self.failure.lock().map_err(|_| RelayError::permanent("L3実行状態を読めません"))?;
        if let Some(message) = &*failure {
            return Err(RelayError::permanent(message));
        }
        Ok(())
    }
}

#[async_trait]
impl Relay for L3Relay {
    async fn publish(&self, frames: &[Frame]) -> Result<(), RelayError> {
        self.check_running()?;
        if frames.len() > MAX_BATCH {
            return Err(RelayError::permanent("1回のpublishは128フレーム以内です"));
        }
        for frame in frames {
            frame.validate()?;
            // 転送用NICを誤ってキャプチャした場合のトンネル再帰を止める。
            if frame.bytes[12..14] == amitoki_l3_lab::packet::ETHER_TYPE.to_be_bytes() && frame.bytes.get(14..18) == Some(b"AMTK") {
                return Err(RelayError::permanent("L3自身の通信を中継できません。キャプチャ用NICとL3用NICを分けてください"));
            }
        }
        if frames.is_empty() {
            return Ok(());
        }
        let (reply, response) = tokio::sync::oneshot::channel();
        self.commands.try_send(worker::Publish { frames: frames.to_vec(), reply }).map_err(|_| RelayError::retryable("L3送信キューが満杯か、停止しています"))?;
        response.await.map_err(|_| RelayError::retryable("L3送信処理が終了しました"))?
    }

    async fn receive(&self, limit: usize) -> Result<Vec<Delivery>, RelayError> {
        self.check_running()?;
        Ok(self.inbox.lock().map_err(|_| RelayError::permanent("受信キューを読めません"))?.receive(limit.min(MAX_BATCH)))
    }

    async fn acknowledge(&self, receipts: &[Receipt]) -> Result<(), RelayError> {
        self.inbox.lock().map_err(|_| RelayError::permanent("受信キューを更新できません"))?.acknowledge(receipts);
        Ok(())
    }
}

impl Drop for L3Relay {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests;
