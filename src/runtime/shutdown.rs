use std::sync::atomic::{AtomicBool, Ordering};

static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn request_stop(_: libc::c_int) {
    STOP.store(true, Ordering::Relaxed);
}

pub fn install_shutdown() -> std::io::Result<()> {
    for signal in [libc::SIGINT, libc::SIGTERM] {
        // ハンドラ内ではlock-freeなboolだけを変更し、I/Oと後始末は通常ループで行う。
        if unsafe { libc::signal(signal, request_stop as *const () as libc::sighandler_t) } == libc::SIG_ERR {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

pub(super) fn requested() -> bool {
    STOP.load(Ordering::Relaxed)
}
