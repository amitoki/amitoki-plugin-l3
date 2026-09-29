//! CSIG・適応的な経路分散を参考にした実験機能。標準プロトコルとの互換性は持たない。
mod controller;
mod settings;
mod signal;
pub use controller::{Acknowledgement, Controller, Transmission};
pub use settings::Settings;
pub use signal::Signal;

// 設定と送信状態の両方で同じ経路数上限を使う。
pub const MAX_PATHS: usize = 8;

mod trimming;
pub use trimming::trim;
