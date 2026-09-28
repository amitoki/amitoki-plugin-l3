//! raw socketの処理を専用threadに置き、stdio要求の待ち時間にも受信を継続する。
use crate::{codec, inbox::Inbox};
use amitoki_l3_lab::{
    delivery::SubmitError,
    runtime::{Endpoint, Submission},
};
use amitoki_plugin_sdk::relay::{Frame, RelayError};
use std::{
    collections::{HashSet, VecDeque},
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

// 本体の既定操作期限10秒より前に、未確認を明示して再試行へ戻す。
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(8);
const SUBMIT_BURST: usize = 32;

pub struct Publish {
    pub frames: Vec<Frame>,
    pub reply: oneshot::Sender<Result<(), RelayError>>,
}

struct Outgoing {
    peer: u32,
    payload: Vec<u8>,
}

struct ActivePublish {
    remaining: VecDeque<Outgoing>,
    pending: HashSet<Submission>,
    deadline: Instant,
    reply: oneshot::Sender<Result<(), RelayError>>,
}

pub struct Worker {
    pub endpoint: Endpoint,
    pub peers: Vec<u32>,
    pub channel: codec::DigestBytes,
    pub inbox: Arc<Mutex<Inbox>>,
    pub stopped: Arc<AtomicBool>,
    pub commands: mpsc::Receiver<Publish>,
    pub failure: Arc<Mutex<Option<String>>>,
}

impl Worker {
    pub fn run(mut self) {
        let mut active: Option<ActivePublish> = None;
        let outcome = self.drive(&mut active);
        let message = outcome.err().map(|error| error.to_string()).unwrap_or_else(|| "L3プラグインを停止しました".into());
        if let Some(active) = active {
            let _ = active.reply.send(Err(RelayError::retryable(&message)));
        }
        if let Ok(mut failure) = self.failure.lock() {
            *failure = Some(message);
        }
    }

    fn drive(&mut self, active: &mut Option<ActivePublish>) -> io::Result<()> {
        while !self.stopped.load(Ordering::Acquire) {
            if active.is_none() {
                match self.commands.try_recv() {
                    Ok(request) => *active = Some(self.start(request)),
                    Err(mpsc::TryRecvError::Disconnected) => break,
                    Err(mpsc::TryRecvError::Empty) => {},
                }
            }
            let confirmed = {
                let mut inbox = self.inbox.lock().map_err(|_| io::Error::other("受信キューの状態が不正です"))?;
                self.endpoint.poll(&mut *inbox)?
            };
            if let Some(batch) = active {
                for confirmation in confirmed {
                    batch.pending.remove(&confirmation);
                }
                let failed = self.endpoint.failed();
                if failed.is_some() || Instant::now() >= batch.deadline {
                    let batch = active.take().expect("進行中のpublish");
                    let _ = batch.reply.send(Err(RelayError::retryable(format!("L3配送が未確認です: {failed:?}"))));
                    self.endpoint.reset_senders();
                    continue;
                }
                // 混雑したpeerを飛ばし、別peerの送信を進める。
                for _ in 0..SUBMIT_BURST.min(batch.remaining.len()) {
                    let Some(outgoing) = batch.remaining.pop_front() else { break };
                    match self.endpoint.submit(outgoing.peer, &outgoing.payload) {
                        Ok(submission) => {
                            batch.pending.insert(submission);
                        },
                        Err(SubmitError::WouldBlock) => batch.remaining.push_back(outgoing),
                        Err(error) => return Err(io::Error::other(error)),
                    }
                }
                if batch.remaining.is_empty() && batch.pending.is_empty() {
                    let batch = active.take().expect("完了したpublish");
                    let _ = batch.reply.send(Ok(()));
                }
            }
            self.endpoint.wait()?;
        }
        Ok(())
    }

    fn start(&mut self, request: Publish) -> ActivePublish {
        if self.endpoint.failed().is_some() {
            self.endpoint.reset_senders();
        }
        let mut remaining = VecDeque::new();
        for frame in &request.frames {
            for payload in codec::encode(frame, &self.channel) {
                for peer in &self.peers {
                    remaining.push_back(Outgoing {
                        peer: *peer,
                        payload: payload.clone(),
                    });
                }
            }
        }
        ActivePublish {
            remaining,
            pending: HashSet::new(),
            deadline: Instant::now() + PUBLISH_TIMEOUT,
            reply: request.reply,
        }
    }
}
