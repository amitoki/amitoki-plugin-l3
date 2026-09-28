//! ACKは受信キューへの受付を表す。アプリ処理・永続化は保証しない。
mod receiver;
mod retry;
mod sender;
pub(crate) mod wire;

pub use receiver::{DeliveredMessage, Receiver, ReceiverOptions};
pub use sender::{Channel, ChannelOptions, ChannelState, SendTick, SubmitError};
pub use wire::{Ordering, PREFIX_SIZE};

// パケットの滞留期限と論理メッセージの再送期限を分離する。
pub const ATTEMPT_LIFETIME_US: u64 = 200_000;
// 小さい窓を維持し、欠落があっても受信メモリを有限にする。
pub const DEFAULT_WINDOW: usize = 64;
pub const MAX_WINDOW: usize = 256;
pub const DEFAULT_PENDING: usize = 256;
pub const DEFAULT_TIMEOUT_US: u64 = 10_000_000;
