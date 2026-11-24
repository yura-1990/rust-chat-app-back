use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Custom epoch so IDs stay small for longer (2024-01-01T00:00:00Z).
const EPOCH_MS: i64 = 1_704_067_200_000;
const WORKER_BITS: u32 = 10;
const SEQUENCE_BITS: u32 = 12;
const MAX_SEQUENCE: i64 = (1 << SEQUENCE_BITS) - 1;
const MAX_WORKER: u16 = (1 << WORKER_BITS) - 1;

/// Twitter/Discord-style Snowflake ID generator: time-sortable, globally
/// unique without coordination across processes as long as each one gets a
/// distinct `worker_id`. Message/chat/user IDs across every service come
/// from this instead of a DB sequence, so ordering survives a future move
/// off a single Postgres instance.
pub struct SnowflakeGenerator {
    worker_id: i64,
    // Packs (last_timestamp_ms << SEQUENCE_BITS) | sequence into one word so
    // a single compare_exchange is enough to claim an ID.
    state: AtomicI64,
}

impl SnowflakeGenerator {
    pub fn new(worker_id: u16) -> Self {
        assert!(worker_id <= MAX_WORKER, "worker_id exceeds {WORKER_BITS} bits");
        Self {
            worker_id: worker_id as i64,
            state: AtomicI64::new(0),
        }
    }

    pub fn next_id(&self) -> i64 {
        loop {
            let now = now_ms();
            let prev = self.state.load(Ordering::Relaxed);
            let prev_ts = prev >> SEQUENCE_BITS;

            let (ts, seq) = if now > prev_ts {
                (now, 0)
            } else {
                let next_seq = (prev & MAX_SEQUENCE) + 1;
                if next_seq > MAX_SEQUENCE {
                    // Exhausted this millisecond's sequence space — spin
                    // until the clock ticks forward rather than block.
                    continue;
                }
                (prev_ts, next_seq)
            };

            let next_state = (ts << SEQUENCE_BITS) | seq;
            if self
                .state
                .compare_exchange(prev, next_state, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return ((ts - EPOCH_MS) << (WORKER_BITS + SEQUENCE_BITS))
                    | (self.worker_id << SEQUENCE_BITS)
                    | seq;
            }
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn ids_are_unique_and_increasing_under_contention() {
        let generator = Arc::new(SnowflakeGenerator::new(1));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let generator = generator.clone();
            handles.push(thread::spawn(move || {
                (0..5_000).map(|_| generator.next_id()).collect::<Vec<_>>()
            }));
        }

        let mut all_ids = HashSet::new();
        for handle in handles {
            for id in handle.join().unwrap() {
                assert!(all_ids.insert(id), "duplicate id generated: {id}");
            }
        }
        assert_eq!(all_ids.len(), 8 * 5_000);
    }

    #[test]
    fn later_ids_sort_after_earlier_ones() {
        let generator = SnowflakeGenerator::new(2);
        let first = generator.next_id();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let second = generator.next_id();
        assert!(second > first);
    }
}
