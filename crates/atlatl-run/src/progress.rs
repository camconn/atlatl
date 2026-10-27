//! Progress while REST streams extract (10 §2, §3): a status line on a terminal, else an INFO
//! line every `interval`, and `stream.progress` events either way.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use atlatl_core::obs::events::{EventBody, EventSink, HttpProgress, Phase, StreamProgress};
use atlatl_core::obs::status;
use atlatl_http::{Live, LiveSnapshot};

pub const INTERVAL_ENV: &str = "ATLATL_PROGRESS_INTERVAL";
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(30);
const TICK: Duration = Duration::from_secs(1);

struct Entry {
    id: u64,
    replication: String,
    stream_id: String,
    member: String,
    live: Arc<Live>,
    started: Instant,
}

/// The REST extracts in flight.
#[derive(Default)]
pub struct Board {
    entries: Mutex<(u64, Vec<Entry>)>,
}

/// Removes its entry when the extract ends, however it ends.
pub struct Tracked {
    board: Arc<Board>,
    id: u64,
    pub live: Arc<Live>,
}

impl Drop for Tracked {
    fn drop(&mut self) {
        let mut e = self.board.entries.lock().unwrap_or_else(|e| e.into_inner());
        e.1.retain(|x| x.id != self.id);
        if e.1.is_empty() {
            status::clear();
        }
    }
}

impl Board {
    pub fn track(self: &Arc<Self>, replication: &str, stream_id: &str, member: &str) -> Tracked {
        let live = Arc::new(Live::default());
        let mut e = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        e.0 += 1;
        let id = e.0;
        e.1.push(Entry {
            id,
            replication: replication.to_owned(),
            stream_id: stream_id.to_owned(),
            member: member.to_owned(),
            live: live.clone(),
            started: Instant::now(),
        });
        Tracked {
            board: self.clone(),
            id,
            live,
        }
    }

    fn snapshot(&self) -> Vec<(String, String, String, LiveSnapshot, Duration)> {
        let e = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        e.1.iter()
            .map(|x| {
                (
                    x.replication.clone(),
                    x.stream_id.clone(),
                    x.member.clone(),
                    x.live.snapshot(),
                    x.started.elapsed(),
                )
            })
            .collect()
    }
}

pub fn http_progress(s: &LiveSnapshot, elapsed: Duration) -> HttpProgress {
    let mins = elapsed.as_secs_f64() / 60.0;
    let eta_s = match s.items_total {
        Some(t) if s.items_done > 0 && t > s.items_done => Some(
            (elapsed.as_secs_f64() * (t - s.items_done) as f64 / s.items_done as f64).round()
                as u64,
        ),
        _ => None,
    };
    HttpProgress {
        items_done: s.items_done,
        items_total: s.items_total,
        requests: s.requests,
        requests_per_min: if mins > 0.0 {
            (s.requests as f64 / mins * 10.0).round() / 10.0
        } else {
            0.0
        },
        retries: s.retries,
        throttle_wait_ms: s.throttle_wait_ms,
        status_counts: s.status_counts.clone(),
        eta_s,
    }
}

fn thousands(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

fn duration(s: u64) -> String {
    match s {
        0..60 => format!("{s}s"),
        60..3600 => format!("{}m", s.div_ceil(60)),
        _ => format!("{}h{:02}m", s / 3600, (s % 3600) / 60),
    }
}

/// `vendor/symbols  8,412/21,976 items  792 req/min  0 retries  0 429s  eta 17m`
pub fn line(stream_id: &str, h: &HttpProgress) -> String {
    let items = match h.items_total {
        Some(t) => format!("{}/{} items", thousands(h.items_done), thousands(t)),
        None => format!("{} items", thousands(h.items_done)),
    };
    let throttled = h.status_counts.get("429").copied().unwrap_or(0);
    let mut s = format!(
        "{stream_id}  {items}  {} req/min  {} retries  {} 429s",
        thousands(h.requests_per_min.round() as u64),
        thousands(h.retries),
        thousands(throttled)
    );
    if let Some(e) = h.eta_s {
        s.push_str(&format!("  eta {}", duration(e)));
    }
    s
}

/// Reports until dropped. A terminal's status line redraws every second; events and (off a
/// terminal) log lines go out when an extract starts and then every `interval`. `None` (an
/// interval of `0`) reports nothing.
pub struct Ticker(Option<tokio::task::JoinHandle<()>>);

impl Drop for Ticker {
    fn drop(&mut self) {
        if let Some(h) = self.0.take() {
            h.abort();
            status::clear();
        }
    }
}

pub fn start(board: Arc<Board>, events: EventSink, interval: Option<Duration>) -> Ticker {
    let Some(interval) = interval else {
        return Ticker(None);
    };
    Ticker(Some(tokio::spawn(async move {
        let tty = status::is_enabled();
        let mut last: Option<Instant> = None;
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let snap = board.snapshot();
            let due = !snap.is_empty() && last.is_none_or(|l| l.elapsed() >= interval);
            let mut lines = Vec::new();
            for (replication, stream_id, member, s, elapsed) in snap {
                let h = http_progress(&s, elapsed);
                let l = line(&stream_id, &h);
                if due && !tty {
                    tracing::info!(
                        replication = %replication,
                        stream_id = %stream_id,
                        items_done = h.items_done,
                        items_total = h.items_total,
                        requests = h.requests,
                        requests_per_min = h.requests_per_min,
                        retries = h.retries,
                        throttle_wait_ms = h.throttle_wait_ms,
                        eta_s = h.eta_s,
                        "progress: {l}"
                    );
                }
                if due {
                    events.emit(EventBody::StreamProgress(StreamProgress {
                        replication,
                        stream_id,
                        member: Some(member),
                        phase: Phase::Extract,
                        rows_so_far: s.records,
                        bytes_so_far: 0,
                        chunk: s.items_done,
                        http: Some(h),
                    }));
                }
                lines.push(l);
            }
            if due {
                last = Some(Instant::now());
            }
            if tty {
                status::set(&lines.join("  │  "));
            }
        }
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_and_rates() {
        let s = LiveSnapshot {
            items_done: 8_412,
            items_total: Some(21_976),
            requests: 15_840,
            retries: 2,
            status_counts: [("200".to_owned(), 15_838), ("429".to_owned(), 2)].into(),
            ..Default::default()
        };
        let h = http_progress(&s, Duration::from_secs(20 * 60));
        assert_eq!(h.requests_per_min, 792.0);
        assert_eq!(h.eta_s, Some(1935));
        assert_eq!(
            line("vendor/symbols", &h),
            "vendor/symbols  8,412/21,976 items  792 req/min  2 retries  2 429s  eta 33m"
        );
        let h = http_progress(
            &LiveSnapshot {
                items_done: 3,
                ..Default::default()
            },
            Duration::ZERO,
        );
        assert_eq!(
            line("a/b", &h),
            "a/b  3 items  0 req/min  0 retries  0 429s",
            "no total, no eta, no rate before any time passes"
        );
    }

    #[derive(Clone, Default)]
    struct Buf(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Buf {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .extend_from_slice(b);
            Ok(b.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn ticker_emits_progress_events_while_tracked() {
        let buf = Buf::default();
        let board = Arc::new(Board::default());
        let t = board.track("r", "api/symbols", "symbols");
        t.live.set_items_total(10);
        t.live
            .items_done
            .fetch_add(4, std::sync::atomic::Ordering::Relaxed);
        t.live
            .requests
            .fetch_add(4, std::sync::atomic::Ordering::Relaxed);
        let ticker = start(
            board.clone(),
            EventSink::to_writer(buf.clone(), "run"),
            Some(Duration::from_secs(30)),
        );
        tokio::time::sleep(Duration::from_millis(1500)).await;
        drop(t);
        tokio::time::sleep(Duration::from_millis(1500)).await;
        drop(ticker);
        let out = String::from_utf8(buf.0.lock().expect("buf").clone()).expect("utf8");
        let events: Vec<serde_json::Value> = out
            .lines()
            .map(|l| serde_json::from_str(l).expect("json"))
            .collect();
        assert_eq!(events.len(), 1, "one at the start, then every 30s: {out}");
        let e = &events[0];
        assert_eq!(e["type"], "stream.progress");
        assert_eq!(e["stream_id"], "api/symbols");
        assert_eq!(e["http"]["items_done"], 4);
        assert_eq!(e["http"]["items_total"], 10);
        assert_eq!(e["http"]["requests"], 4);
        assert_eq!(e["chunk"], 4);
        let last = events.len();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(
            String::from_utf8(buf.0.lock().expect("buf").clone())
                .expect("utf8")
                .lines()
                .count(),
            last,
            "nothing after the extract ends and the ticker stops"
        );
    }

    #[test]
    fn board_entries_end_with_their_guard() {
        let b = Arc::new(Board::default());
        let t = b.track("r", "a/b", "m");
        t.live.set_items_total(4);
        assert_eq!(b.snapshot().len(), 1);
        drop(t);
        assert!(b.snapshot().is_empty());
        assert_eq!(thousands(1_234_567), "1,234,567");
        assert_eq!(duration(4000), "1h06m");
    }
}
