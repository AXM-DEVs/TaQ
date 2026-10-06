use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy)]
pub struct DeduplicatorConfig {
    pub capacity: usize,
    pub retention: Duration,
}

#[derive(Debug)]
pub struct Deduplicator {
    entries: HashMap<String, (Instant, u64)>,
    insertion_order: VecDeque<(String, Instant, u64)>,
    next_sequence: u64,
    config: DeduplicatorConfig,
}

impl Deduplicator {
    pub fn new(config: DeduplicatorConfig) -> Self {
        Self {
            entries: HashMap::new(),
            insertion_order: VecDeque::new(),
            next_sequence: 0,
            config,
        }
    }

    pub fn check_and_insert(&mut self, id: &str, now: Instant) -> bool {
        self.expire(now);
        if self.entries.contains_key(id) {
            return false;
        }
        if self.config.capacity == 0 {
            return true;
        }
        if self.entries.len() >= self.config.capacity {
            self.evict_oldest();
        }
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        self.entries.insert(id.to_owned(), (now, sequence));
        self.insertion_order
            .push_back((id.to_owned(), now, sequence));
        true
    }

    fn expire(&mut self, now: Instant) {
        while let Some((id, seen_at, sequence)) = self.insertion_order.front() {
            if now.saturating_duration_since(*seen_at) < self.config.retention {
                break;
            }
            let id = id.clone();
            let sequence = *sequence;
            self.insertion_order.pop_front();
            if self
                .entries
                .get(&id)
                .is_some_and(|(_, current_sequence)| *current_sequence == sequence)
            {
                self.entries.remove(&id);
            }
        }
    }

    fn evict_oldest(&mut self) {
        while let Some((id, _, sequence)) = self.insertion_order.pop_front() {
            if self
                .entries
                .get(&id)
                .is_some_and(|(_, current_sequence)| *current_sequence == sequence)
            {
                self.entries.remove(&id);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Deduplicator, DeduplicatorConfig};
    use std::time::{Duration, Instant};

    fn dedup(capacity: usize, retention: Duration) -> Deduplicator {
        Deduplicator::new(DeduplicatorConfig {
            capacity,
            retention,
        })
    }

    #[test]
    fn rejects_exact_duplicate_within_retention() {
        let now = Instant::now();
        let mut dedup = dedup(10, Duration::from_secs(60));

        assert!(dedup.check_and_insert("event-1", now));
        assert!(!dedup.check_and_insert("event-1", now + Duration::from_secs(1)));
    }

    #[test]
    fn accepts_event_after_retention_expires() {
        let now = Instant::now();
        let mut dedup = dedup(10, Duration::from_secs(2));

        assert!(dedup.check_and_insert("event-1", now));
        assert!(dedup.check_and_insert("event-1", now + Duration::from_secs(2)));
    }

    #[test]
    fn evicts_oldest_when_capacity_is_reached() {
        let now = Instant::now();
        let mut dedup = dedup(1, Duration::from_secs(60));

        assert!(dedup.check_and_insert("event-1", now));
        assert!(dedup.check_and_insert("event-2", now + Duration::from_secs(1)));
        assert!(dedup.check_and_insert("event-1", now + Duration::from_secs(2)));
    }
}
