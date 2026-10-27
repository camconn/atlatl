//! Kill behaviour (03 §9): the first SIGINT/SIGTERM cancels all work and the process exits within
//! about a second, after a best-effort report. A second signal exits at once.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use atlatl_core::obs::report::{KillReason, ReportHandle};
use tokio_util::sync::CancellationToken;

/// How long work gets to unwind after a signal before the watchdog writes the report and exits.
pub const GRACE: Duration = Duration::from_millis(800);
const REPORT_BUDGET: Duration = Duration::from_millis(300);

#[derive(Clone)]
pub struct Kill {
    pub token: CancellationToken,
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    reason: Mutex<Option<KillReason>>,
    report: Mutex<Option<(ReportHandle, Option<PathBuf>)>>,
    finished: AtomicBool,
}

pub fn exit_code(r: KillReason) -> u8 {
    match r {
        KillReason::Sigint => 130,
        KillReason::Sigterm => 143,
        KillReason::Timeout => 1,
    }
}

impl Kill {
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            inner: Arc::default(),
        }
    }

    /// Listens for SIGINT/SIGTERM on the current tokio runtime.
    pub fn install(&self) -> std::io::Result<()> {
        use tokio::signal::unix::{SignalKind, signal};
        let mut int = signal(SignalKind::interrupt())?;
        let mut term = signal(SignalKind::terminate())?;
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                let r = tokio::select! {
                    _ = int.recv() => KillReason::Sigint,
                    _ = term.recv() => KillReason::Sigterm,
                };
                me.trigger(r);
            }
        });
        Ok(())
    }

    /// Registers the report the watchdog writes if the process hasn't finished by then.
    pub fn set_report(&self, report: ReportHandle, path: Option<PathBuf>) {
        *self
            .inner
            .report
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((report, path));
    }

    pub fn reason(&self) -> Option<KillReason> {
        *self
            .inner
            .reason
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Normal shutdown finished (report written); the watchdog must not exit the process.
    pub fn finished(&self) {
        self.inner.finished.store(true, Ordering::SeqCst);
    }

    pub fn trigger(&self, r: KillReason) {
        let first = {
            let mut g = self
                .inner
                .reason
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let first = g.is_none();
            if first {
                *g = Some(r);
            }
            first
        };
        if !first {
            std::process::exit(i32::from(exit_code(r)));
        }
        tracing::warn!(reason = ?r, "signal received: cancelling all work");
        self.token.cancel();
        let inner = self.inner.clone();
        std::thread::spawn(move || {
            std::thread::sleep(GRACE);
            if inner.finished.load(Ordering::SeqCst) {
                return;
            }
            let report = inner
                .report
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            let code = match report {
                Some((h, path)) => h.write_on_kill(path.as_deref(), r, REPORT_BUDGET),
                None => exit_code(r),
            };
            std::process::exit(i32::from(code));
        });
    }
}

impl Default for Kill {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes() {
        assert_eq!(exit_code(KillReason::Sigint), 130);
        assert_eq!(exit_code(KillReason::Sigterm), 143);
    }

    #[test]
    fn first_signal_cancels() {
        let k = Kill::new();
        k.finished();
        assert!(!k.token.is_cancelled());
        k.trigger(KillReason::Sigterm);
        assert!(k.token.is_cancelled());
        assert_eq!(k.reason(), Some(KillReason::Sigterm));
    }
}
