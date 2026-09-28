//! 明示された設定のraw socketだけを開く。接続元はsocket所有者のUIDに制限する。
use crate::RawL3Plugin;
use amitoki_plugin_sdk::{
    relay::{Relay, RelayError, RelayPlugin},
    wire::{read_message, write_message, Request, Response, MAX_BATCH},
    PROTOCOL_VERSION,
};
use std::{
    io,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
    sync::Arc,
};
use tokio::{
    net::{UnixListener, UnixStream},
    sync::Semaphore,
};

// 切断待ちのクライアントによるthread/受信バッファの無制限な増加を防ぐ。
const MAX_CLIENTS: usize = 8;
const MAX_CONFIG_BYTES: u64 = 65_536;

pub async fn serve(config_path: &Path, socket_path: &Path, user: u32) -> io::Result<()> {
    if std::fs::metadata(config_path)?.len() > MAX_CONFIG_BYTES {
        return Err(io::Error::other("設定ファイルが大きすぎます"));
    }
    let options: serde_json::Value = serde_json::from_slice(&std::fs::read(config_path)?)?;
    let parsed: crate::config::Options = serde_json::from_value(options.clone())?;
    parsed.validate().map_err(io::Error::other)?;
    let parent = socket_path.parent().ok_or_else(|| io::Error::other("ソケットの親ディレクトリが必要です"))?;
    let metadata = std::fs::symlink_metadata(parent)?;
    if !socket_path.is_absolute() || !metadata.is_dir() || ![0, user].contains(&metadata.uid()) || metadata.mode() & 0o022 != 0 {
        return Err(io::Error::other("ソケットの親はrootまたは指定UIDの、他ユーザが書き込めないディレクトリにしてください"));
    }
    if unsafe { libc::geteuid() } == 0 {
        // rootでpathへchmod/chownする場合、途中の親も差し替え不能にする。
        for ancestor in parent.ancestors() {
            let metadata = std::fs::symlink_metadata(ancestor)?;
            if !metadata.is_dir() || metadata.uid() != 0 || metadata.mode() & 0o022 != 0 {
                return Err(io::Error::other(
                    "root起動時のsocketは、全ての親をrootが所有する書き換え不能なパス（/run/amitoki-l3等）に置いてください",
                ));
            }
        }
    }
    // bind直後から指定UID以外には使わせない。既存socketを勝手に削除しない。
    unsafe {
        libc::umask(0o077);
    }
    let listener = UnixListener::bind(socket_path)?;
    let _socket = SocketFile {
        path: socket_path.to_path_buf(),
        inode: std::fs::symlink_metadata(socket_path)?.ino(),
    };
    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(0o600))?;
    std::os::unix::fs::chown(socket_path, Some(user), None)?;
    let permits = Arc::new(Semaphore::new(MAX_CLIENTS));
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        let (stream, _) = tokio::select! {
            accepted = listener.accept() => accepted?,
            _ = terminate.recv() => return Ok(()),
            interrupted = tokio::signal::ctrl_c() => { interrupted?; return Ok(()); },
        };
        if stream.peer_cred()?.uid() != user {
            continue;
        }
        let Ok(permit) = permits.clone().try_acquire_owned() else { continue };
        let options = options.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(error) = session(stream, options).await {
                if error.kind() != io::ErrorKind::UnexpectedEof {
                    eprintln!("L3クライアントを終了: {error}");
                }
            }
        });
    }
}

async fn session(mut stream: UnixStream, options: serde_json::Value) -> io::Result<()> {
    let mut relay: Option<Arc<dyn Relay>> = None;
    loop {
        let request: Request = read_message(&mut stream).await?;
        let response = dispatch(request, &mut relay, &options).await.unwrap_or_else(Response::from);
        write_message(&mut stream, &response).await?;
    }
}

async fn dispatch(request: Request, relay: &mut Option<Arc<dyn Relay>>, options: &serde_json::Value) -> Result<Response, RelayError> {
    match request {
        Request::Describe => Ok(Response::Manifest(crate::manifest::manifest())),
        Request::Connect {
            protocol_version,
            context,
            options: client_options,
        } => {
            if relay.is_some() || protocol_version != PROTOCOL_VERSION || client_options != serde_json::json!({}) {
                return Err(RelayError::permanent("接続状態または通信仕様が不正です"));
            }
            *relay = Some(RawL3Plugin.connect(context, options.clone()).await?);
            Ok(Response::Success)
        },
        Request::Publish { frames } => {
            connected(relay)?.publish(&frames).await?;
            Ok(Response::Success)
        },
        Request::Receive { limit } => Ok(Response::Deliveries(connected(relay)?.receive(limit.min(MAX_BATCH)).await?)),
        Request::Acknowledge { receipts } => {
            if receipts.len() > MAX_BATCH {
                return Err(RelayError::permanent("ACKは128件以内です"));
            }
            connected(relay)?.acknowledge(&receipts).await?;
            Ok(Response::Success)
        },
        _ => Err(RelayError::permanent("L3では未対応の要求です")),
    }
}

fn connected(relay: &Option<Arc<dyn Relay>>) -> Result<&Arc<dyn Relay>, RelayError> {
    relay.as_ref().ok_or_else(|| RelayError::permanent("先にconnectしてください"))
}

struct SocketFile {
    path: std::path::PathBuf,
    inode: u64,
}
impl Drop for SocketFile {
    fn drop(&mut self) {
        // 別プロセスが差し替えたパスを消さない。
        if std::fs::symlink_metadata(&self.path).is_ok_and(|metadata| metadata.ino() == self.inode) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
