use serde::{Deserialize, Serialize};
use std::{io, os::unix::fs::MetadataExt};

// 試験用の時計はプロセス内だけでずらし、ホストの時計には触れない。
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Simulation {
    pub offset_us: i64,
    pub drift_ppm: i64,
}

impl Simulation {
    pub fn validate(self) -> Result<(), &'static str> {
        if self.offset_us.unsigned_abs() > 3_600_000_000 || self.drift_ppm.unsigned_abs() > 500 {
            return Err("試験用時計はoffset±1時間・drift±500ppmまでです");
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct Clock {
    pub domain: u64,
    origin: u64,
    simulation: Simulation,
}

impl Clock {
    pub fn local() -> io::Result<Self> {
        Self::with_simulation(Simulation::default())
    }

    pub fn with_simulation(simulation: Simulation) -> io::Result<Self> {
        simulation.validate().map_err(io::Error::other)?;
        let origin = boottime();
        if i128::from(origin) + i128::from(simulation.offset_us) < 0 {
            return Err(io::Error::other("試験用時計が負になります"));
        }
        let mut identity = std::fs::read("/proc/sys/kernel/random/boot_id")?;
        // 同じbootでもtime namespaceが違えばMONOTONICのオフセットが異なり得る。
        identity.extend_from_slice(&std::fs::metadata("/proc/self/ns/time")?.ino().to_be_bytes());
        identity.extend_from_slice(&simulation.offset_us.to_be_bytes());
        identity.extend_from_slice(&simulation.drift_ppm.to_be_bytes());
        if simulation.drift_ppm != 0 {
            identity.extend_from_slice(&origin.to_be_bytes());
        }
        Ok(Self {
            domain: crate::packet::fingerprint(&identity).max(1),
            origin,
            simulation,
        })
    }

    pub fn now(self) -> u64 {
        let elapsed = boottime() - self.origin;
        let adjusted = i128::from(elapsed) * i128::from(1_000_000 + self.simulation.drift_ppm) / 1_000_000;
        (i128::from(self.origin) + i128::from(self.simulation.offset_us) + adjusted) as u64
    }

    pub fn simulation(self) -> Simulation {
        self.simulation
    }
}

fn boottime() -> u64 {
    let mut value = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // suspend中も進む時計にし、復帰直後に古い同期を有効と判定しない。
    let status = unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut value) };
    assert_eq!(status, 0, "CLOCK_BOOTTIMEを取得できません");
    value.tv_sec as u64 * 1_000_000 + value.tv_nsec as u64 / 1_000
}
