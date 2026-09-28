use amitoki_plugin_sdk::relay::{RelayContext, RelayError};
use fs2::FileExt;
use sha2::{Digest, Sha256};
use std::{
    fs::{DirBuilder, File, OpenOptions},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
};

pub fn claim_node(context: &RelayContext) -> Result<File, RelayError> {
    let identity = serde_json::to_vec(&["context", &context.channel, &context.node_id]).expect("識別子のJSON");
    claim(&identity)
}

pub fn claim_network(node: u32) -> Result<File, RelayError> {
    let namespace = std::fs::metadata("/proc/self/ns/net").map_err(|_| RelayError::permanent("network namespaceを読めません"))?;
    let identity = format!("network:{}:{}:{node}", namespace.dev(), namespace.ino());
    claim(identity.as_bytes())
}

fn claim(identity: &[u8]) -> Result<File, RelayError> {
    let user = unsafe { libc::geteuid() };
    // プラグインの複数プロセスで同じノードIDを占有させない。
    let directory = std::env::temp_dir().join(format!("amitoki-l3-{user}"));
    if let Err(error) = DirBuilder::new().mode(0o700).create(&directory) {
        if error.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(RelayError::permanent("ノード占有ディレクトリを作成できません"));
        }
    }
    let metadata = directory.symlink_metadata().map_err(|_| RelayError::permanent("ノード占有ディレクトリを読めません"))?;
    if !metadata.is_dir() || metadata.uid() != user || metadata.mode() & 0o777 != 0o700 {
        return Err(RelayError::permanent("ノード占有ディレクトリの所有者または権限が不正です"));
    }
    let path = directory.join(format!("{:x}.lock", Sha256::digest(identity)));
    // ロックファイルを削除すると、別inode上で同じIDを多重起動できるため残す。
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| RelayError::permanent("ノード占有ファイルを開けません"))?;
    file.try_lock_exclusive().map_err(|_| RelayError::permanent("このノードIDは同じユーザの別プロセスが使用中です"))?;
    Ok(file)
}
