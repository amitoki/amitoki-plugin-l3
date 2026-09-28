//! 本体SDKが落とした権限を取り戻さず、独立起動したL3補助プロセスを使う。
use amitoki_plugin_sdk::{
    relay::{Delivery, Frame, Receipt, Relay, RelayContext, RelayError, RelayPlugin},
    wire::{read_message, write_message, Request, Response},
    PROTOCOL_VERSION,
};
use async_trait::async_trait;
use serde::Deserialize;
use std::{path::Path, sync::Arc, time::Duration};
use tokio::{net::UnixStream, sync::Mutex};

// 本体SDKの30秒より前に失敗を返す。timeout後の応答は別要求へ流用しない。
const REQUEST_TIMEOUT: Duration = Duration::from_secs(25);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    socket_path: String,
}

pub struct L3Plugin;
struct SocketRelay {
    stream: Mutex<Option<UnixStream>>,
}

#[async_trait]
impl RelayPlugin for L3Plugin {
    fn name(&self) -> &'static str {
        "l3"
    }
    async fn connect(&self, context: RelayContext, options: serde_json::Value) -> Result<Arc<dyn Relay>, RelayError> {
        context.validate()?;
        crate::manifest::manifest().validate_options(&options)?;
        let options: Options = serde_json::from_value(options).map_err(|_| RelayError::permanent("socket_pathが不正です"))?;
        if !Path::new(&options.socket_path).is_absolute() {
            return Err(RelayError::permanent("socket_pathは絶対パスで指定してください"));
        }
        let stream = UnixStream::connect(&options.socket_path).await.map_err(|error| RelayError::retryable(format!("L3補助プロセスに接続できません: {error}")))?;
        let peer = stream.peer_cred().map_err(|error| RelayError::permanent(error.to_string()))?;
        let user = unsafe { libc::geteuid() };
        if peer.uid() != 0 && peer.uid() != user {
            return Err(RelayError::permanent("L3補助プロセスは同じユーザまたはrootで起動してください"));
        }
        let relay = SocketRelay { stream: Mutex::new(Some(stream)) };
        relay
            .success(Request::Connect {
                protocol_version: PROTOCOL_VERSION,
                context,
                options: serde_json::json!({}),
            })
            .await?;
        Ok(Arc::new(relay))
    }
}

impl SocketRelay {
    async fn request(&self, request: Request) -> Result<Response, RelayError> {
        let mut connection = self.stream.lock().await;
        let stream = connection.as_mut().ok_or_else(|| RelayError::permanent("L3補助プロセスとの接続が終了しています。中継を再起動してください"))?;
        let response = tokio::time::timeout(REQUEST_TIMEOUT, async {
            write_message(stream, &request).await?;
            read_message::<Response>(stream).await
        })
        .await;
        match response {
            Ok(Ok(Response::Error { message, retryable })) => Err(if retryable { RelayError::retryable(message) } else { RelayError::permanent(message) }),
            Ok(Ok(response)) => Ok(response),
            _ => {
                *connection = None;
                Err(RelayError::permanent("L3補助プロセスとの通信が失敗しました。中継を再起動してください"))
            },
        }
    }
    async fn success(&self, request: Request) -> Result<(), RelayError> {
        match self.request(request).await? {
            Response::Success => Ok(()),
            _ => Err(RelayError::permanent("L3補助プロセスの応答が不正です")),
        }
    }
}

#[async_trait]
impl Relay for SocketRelay {
    async fn publish(&self, frames: &[Frame]) -> Result<(), RelayError> {
        self.success(Request::Publish { frames: frames.to_vec() }).await
    }
    async fn receive(&self, limit: usize) -> Result<Vec<Delivery>, RelayError> {
        match self.request(Request::Receive { limit }).await? {
            Response::Deliveries(deliveries) => Ok(deliveries),
            _ => Err(RelayError::permanent("L3補助プロセスの応答が不正です")),
        }
    }
    async fn acknowledge(&self, receipts: &[Receipt]) -> Result<(), RelayError> {
        self.success(Request::Acknowledge { receipts: receipts.to_vec() }).await
    }
}
