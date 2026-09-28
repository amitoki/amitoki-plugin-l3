use std::{io, os::unix::fs::MetadataExt};

#[derive(Clone, Copy)]
pub struct Clock {
    pub domain: u64,
}

impl Clock {
    pub fn local() -> io::Result<Self> {
        let mut identity = std::fs::read("/proc/sys/kernel/random/boot_id")?;
        // 同じbootでもtime namespaceが違えばMONOTONICのオフセットが異なり得る。
        identity.extend_from_slice(&std::fs::metadata("/proc/self/ns/time")?.ino().to_be_bytes());
        Ok(Self {
            domain: crate::packet::fingerprint(&identity),
        })
    }

    pub fn now(self) -> u64 {
        let mut value = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        // CLOCK_MONOTONICは同じカーネルのネットワーク名前空間で共有される。
        let status = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut value) };
        assert_eq!(status, 0, "CLOCK_MONOTONICを取得できません");
        value.tv_sec as u64 * 1_000_000 + value.tv_nsec as u64 / 1_000
    }
}
