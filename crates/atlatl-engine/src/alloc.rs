//! Returns memory DuckDB has freed to the OS (spike S3 §6.2). Buffer-managed blocks come from
//! libduckdb's bundled jemalloc, which decays dirty pages only on activity in the same arena;
//! everything else (`new`) comes from glibc, whose heap DuckLake deletes leave fragmented. Both
//! build up outside `memory_limit` across chunks.
//!
//! Symbols are looked up at runtime (Linux/glibc only); whatever is missing is skipped.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use serde::Serialize;

static RELEASES: AtomicU64 = AtomicU64::new(0);
static ENABLED: AtomicBool = AtomicBool::new(true);

/// What [`release`] could reach.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Released {
    pub jemalloc: bool,
    pub glibc: bool,
}

/// Purges every jemalloc arena's dirty and muzzy pages and runs `malloc_trim(0)`. Safe to call
/// concurrently with queries; takes 10–100 ms after a large merge.
pub fn release() -> Released {
    if !ENABLED.load(Ordering::Relaxed) {
        return Released::default();
    }
    RELEASES.fetch_add(1, Ordering::Relaxed);
    imp::release()
}

/// Releases done so far in this process.
pub fn releases() -> u64 {
    RELEASES.load(Ordering::Relaxed)
}

/// Turns [`release`] into a no-op process-wide (for measuring without it).
pub fn set_enabled(on: bool) {
    ENABLED.store(on, Ordering::Relaxed);
}

/// Allocator counters in bytes; `None` where the allocator isn't reachable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Stats {
    /// Live jemalloc allocations (DuckDB's buffer-managed blocks and its other `Allocator` use).
    pub jemalloc_allocated: Option<u64>,
    /// Physically resident jemalloc pages, including dirty ones not yet returned.
    pub jemalloc_resident: Option<u64>,
    /// Dirty pages across all arenas: freed, still resident, reusable only by their arena.
    pub jemalloc_dirty: Option<u64>,
    /// Muzzy pages: decayed with `MADV_FREE`, so in RSS (as `LazyFree`) until the kernel needs
    /// them, and not in `jemalloc_resident`.
    pub jemalloc_muzzy: Option<u64>,
    /// glibc `mallinfo2`: bytes in use (including mmapped chunks).
    pub glibc_in_use: Option<u64>,
    /// glibc `mallinfo2`: free bytes held in arenas (resident unless trimmed).
    pub glibc_free: Option<u64>,
}

pub fn stats() -> Stats {
    imp::stats()
}

/// jemalloc's `opt.dirty_decay_ms`, `opt.muzzy_decay_ms`, `opt.narenas` and `background_thread`.
pub fn jemalloc_config() -> Option<(i64, i64, u32, bool)> {
    imp::jemalloc_config()
}

/// libduckdb's bundled jemalloc `version` string.
pub fn jemalloc_version() -> Option<String> {
    imp::jemalloc_version()
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
mod imp {
    use std::ffi::{CStr, c_char, c_int, c_void};
    use std::sync::OnceLock;

    use super::{Released, Stats};

    type Mallctl =
        unsafe extern "C" fn(*const c_char, *mut c_void, *mut usize, *mut c_void, usize) -> c_int;
    type MallocTrim = unsafe extern "C" fn(usize) -> c_int;
    type Mallinfo2 = unsafe extern "C" fn() -> Mallinfo2Out;

    /// glibc's `struct mallinfo2`.
    #[repr(C)]
    #[allow(dead_code)]
    struct Mallinfo2Out {
        arena: usize,
        ordblks: usize,
        smblks: usize,
        hblks: usize,
        hblkhd: usize,
        usmblks: usize,
        fsmblks: usize,
        uordblks: usize,
        fordblks: usize,
        keepcost: usize,
    }

    /// 4096 is jemalloc's `MALLCTL_ARENAS_ALL`.
    const PURGE_ALL: &CStr = c"arena.4096.purge";

    unsafe extern "C" {
        fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    }

    /// `dlsym(RTLD_DEFAULT, name)`.
    pub(super) fn symbol(name: &CStr) -> Option<*mut c_void> {
        let p = unsafe { dlsym(std::ptr::null_mut(), name.as_ptr()) };
        (!p.is_null()).then_some(p)
    }

    struct Fns {
        mallctl: Option<Mallctl>,
        trim: Option<MallocTrim>,
        mallinfo2: Option<Mallinfo2>,
    }

    fn fns() -> &'static Fns {
        static F: OnceLock<Fns> = OnceLock::new();
        F.get_or_init(|| {
            // SAFETY: the symbols have these C signatures (jemalloc 5 `mallctl`, glibc).
            // mallinfo2 is glibc 2.33+; the release builds target 2.28.
            unsafe {
                Fns {
                    mallctl: symbol(c"duckdb_je_mallctl")
                        .map(|p| std::mem::transmute::<*mut c_void, Mallctl>(p)),
                    trim: symbol(c"malloc_trim")
                        .map(|p| std::mem::transmute::<*mut c_void, MallocTrim>(p)),
                    mallinfo2: symbol(c"mallinfo2")
                        .map(|p| std::mem::transmute::<*mut c_void, Mallinfo2>(p)),
                }
            }
        })
    }

    fn ctl(f: Mallctl, name: &CStr) -> bool {
        let n = std::ptr::null_mut();
        unsafe { f(name.as_ptr(), n, n.cast(), n, 0) == 0 }
    }

    fn get<T: Copy + Default>(f: Mallctl, name: &CStr) -> Option<T> {
        let mut v = T::default();
        let mut len = size_of::<T>();
        let rc = unsafe {
            f(
                name.as_ptr(),
                (&raw mut v).cast(),
                &raw mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        (rc == 0 && len == size_of::<T>()).then_some(v)
    }

    pub(super) fn release() -> Released {
        let f = fns();
        let jemalloc = f.mallctl.is_some_and(|m| ctl(m, PURGE_ALL));
        let glibc = f.trim.is_some_and(|t| {
            unsafe { t(0) };
            true
        });
        Released { jemalloc, glibc }
    }

    pub(super) fn stats() -> Stats {
        let f = fns();
        let mut s = Stats::default();
        if let Some(m) = f.mallctl {
            let mut epoch = 1u64;
            unsafe {
                m(
                    c"epoch".as_ptr(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    (&raw mut epoch).cast(),
                    size_of::<u64>(),
                )
            };
            let page = get::<usize>(m, c"arenas.page").unwrap_or(4096) as u64;
            s.jemalloc_allocated = get::<usize>(m, c"stats.allocated").map(|v| v as u64);
            s.jemalloc_resident = get::<usize>(m, c"stats.resident").map(|v| v as u64);
            s.jemalloc_dirty =
                get::<usize>(m, c"stats.arenas.4096.pdirty").map(|v| v as u64 * page);
            s.jemalloc_muzzy =
                get::<usize>(m, c"stats.arenas.4096.pmuzzy").map(|v| v as u64 * page);
        }
        if let Some(mi) = f.mallinfo2 {
            let m = unsafe { mi() };
            s.glibc_in_use = Some((m.uordblks + m.hblkhd) as u64);
            s.glibc_free = Some(m.fordblks as u64);
        }
        s
    }

    pub(super) fn jemalloc_config() -> Option<(i64, i64, u32, bool)> {
        let m = fns().mallctl?;
        Some((
            get::<isize>(m, c"opt.dirty_decay_ms")? as i64,
            get::<isize>(m, c"opt.muzzy_decay_ms")? as i64,
            get::<u32>(m, c"opt.narenas")?,
            get::<bool>(m, c"background_thread")?,
        ))
    }

    pub(super) fn jemalloc_version() -> Option<String> {
        let m = fns().mallctl?;
        let mut p: *const c_char = std::ptr::null();
        let mut len = size_of::<*const c_char>();
        let rc = unsafe {
            m(
                c"version".as_ptr(),
                (&raw mut p).cast(),
                &raw mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        // SAFETY: jemalloc's `version` is a static NUL-terminated string.
        (rc == 0 && !p.is_null())
            .then(|| unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned())
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
mod imp {
    use super::{Released, Stats};

    pub(super) fn release() -> Released {
        Released::default()
    }

    pub(super) fn stats() -> Stats {
        Stats::default()
    }

    pub(super) fn jemalloc_config() -> Option<(i64, i64, u32, bool)> {
        None
    }

    pub(super) fn jemalloc_version() -> Option<String> {
        None
    }
}

#[cfg(all(test, target_os = "linux", target_env = "gnu"))]
mod tests {
    use super::*;

    #[test]
    fn missing_symbols_are_skipped() {
        assert!(imp::symbol(c"duckdb_je_no_such_symbol").is_none());
        assert!(imp::symbol(c"malloc_trim").is_some());
    }
}
