//! A live status line under the logs on a terminal's stderr (10 §2), redrawn in place like
//! cargo's. Log lines clear it, print and redraw it, so the two never interleave. Off unless the
//! CLI enables it (stderr is a TTY and logs are text).

use std::io::{self, Write};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

static ENABLED: AtomicBool = AtomicBool::new(false);
static LINE: Mutex<String> = Mutex::new(String::new());

pub fn enable() {
    ENABLED.store(true, Ordering::Relaxed);
}

pub fn is_enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// stderr's width in columns: the terminal's own, else `COLUMNS`, else 80.
fn width() -> usize {
    #[cfg(unix)]
    {
        let mut ws = libc::winsize {
            ws_row: 0,
            ws_col: 0,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        // SAFETY: TIOCGWINSZ writes a `winsize` into the struct we own.
        if unsafe { libc::ioctl(libc::STDERR_FILENO, libc::TIOCGWINSZ, &mut ws) } == 0
            && ws.ws_col > 0
        {
            return ws.ws_col as usize;
        }
    }
    std::env::var("COLUMNS")
        .ok()
        .and_then(|c| c.parse().ok())
        .filter(|&c| c > 0)
        .unwrap_or(80)
}

/// Replaces the line; empty removes it. Cut to one column short of the terminal's width, read
/// on every draw, so it never wraps: `\r\x1b[K` only clears the last physical row.
pub fn set(line: &str) {
    if !is_enabled() {
        return;
    }
    let line = fit(line);
    let mut cur = LINE.lock().unwrap_or_else(|e| e.into_inner());
    if *cur == line {
        return;
    }
    let mut err = io::stderr().lock();
    let _ = write!(err, "\r\x1b[K{line}");
    let _ = err.flush();
    *cur = line;
}

fn fit(line: &str) -> String {
    line.chars().take(width().saturating_sub(1)).collect()
}

pub fn clear() {
    set("");
}

/// Runs `f` (which writes to stderr) with the line cleared first and redrawn after.
pub fn around<R>(f: impl FnOnce() -> R) -> R {
    if !is_enabled() {
        return f();
    }
    let cur = LINE.lock().unwrap_or_else(|e| e.into_inner());
    if cur.is_empty() {
        return f();
    }
    let _ = write!(io::stderr().lock(), "\r\x1b[K");
    let r = f();
    let mut err = io::stderr().lock();
    let _ = write!(err, "{}", fit(&cur));
    let _ = err.flush();
    r
}
