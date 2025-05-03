use std::collections::{BTreeMap, HashMap, VecDeque};

#[derive(Default)]
pub enum EvictionPolicy {
    Fifo,
    #[default]
    LruK,
}

pub(super) trait Replacer {
    fn record_access(&mut self, key: usize);
    fn evict(&mut self) -> Option<usize>;
    fn set_evictable(&mut self, key: usize, is_evictable: bool);
    fn available(&self) -> usize;
}

impl From<EvictionPolicy> for Box<dyn Replacer> {
    fn from(policy: EvictionPolicy) -> Self {
        match policy {
            EvictionPolicy::Fifo => Box::new(Fifo::default()),
            EvictionPolicy::LruK => Box::new(LruK::default()),
        }
    }
}

#[derive(Default)]
struct Fifo {
    store: BTreeMap<usize, bool>,
    available: usize,
}

impl Replacer for Fifo {
    fn record_access(&mut self, key: usize) {
        self.store.insert(key, false);
    }

    fn evict(&mut self) -> Option<usize> {
        let key = self
            .store
            .iter()
            .find(|&(_, evictable)| *evictable)
            .map(|(k, _)| *k);

        if let Some(k) = key {
            self.available -= 1;
            self.store.remove(&k);
        }

        key
    }

    fn set_evictable(&mut self, key: usize, is_evictable: bool) {
        self.store.entry(key).and_modify(|e| {
            if is_evictable && !*e {
                self.available += 1;
            }
            *e = is_evictable
        });
    }

    fn available(&self) -> usize {
        self.available
    }
}

#[derive(Default)]
struct LruKNode {
    is_evictable: bool,
    history: VecDeque<usize>,
}

impl LruKNode {
    fn backward_k_distance(&self, current_ts: usize, k: usize) -> usize {
        if self.history.len() < k {
            usize::MAX
        } else {
            current_ts - *self.history.front().unwrap()
        }
    }

    fn least_recent_timestamp(&self) -> usize {
        *self.history.front().unwrap()
    }
}

/// The LRU-K algorithm evicts a frame whose backward k-distance is maximum of all frames in the replacer.
/// Backward k-distance is computed as the difference in time between current timestamp and the timestamp of kth previous access.
/// A frame with less than k historical accesses is given +inf as its backward k-distance.
/// When multipe frames have +inf backward k-distance, the replacer evicts the frame with the earliest timestamp.
struct LruK {
    /// BufferId -> History
    ///
    /// History contains the logical timestamp recorded on each access.
    /// Latest timestamp is stored in the back.
    store: HashMap<usize, LruKNode>,
    k: usize,
    current_ts: usize,
    available: usize,
}

impl Default for LruK {
    fn default() -> Self {
        Self {
            store: HashMap::new(),
            k: 2,
            current_ts: 0,
            available: 0,
        }
    }
}

impl Replacer for LruK {
    fn record_access(&mut self, key: usize) {
        self.current_ts += 1;
        let entry = self.store.entry(key).or_default();
        entry.is_evictable = false;
        entry.history.push_back(self.current_ts);
        if entry.history.len() > self.k {
            entry.history.pop_front();
        }
    }

    fn evict(&mut self) -> Option<usize> {
        let mut max_dist = 0;
        let mut earliest_ts = usize::MAX;
        let mut key = None;

        for (k, node) in &self.store {
            if node.is_evictable {
                let dist = node.backward_k_distance(self.current_ts, self.k);
                if dist < max_dist {
                    continue;
                }
                let ts = node.least_recent_timestamp();
                if dist > max_dist || (dist == max_dist && ts < earliest_ts) {
                    max_dist = dist;
                    earliest_ts = ts;
                    key = Some(*k);
                }
            }
        }

        if let Some(k) = key {
            self.store.remove(&k);
        }

        key
    }

    fn set_evictable(&mut self, key: usize, is_evictable: bool) {
        self.store.entry(key).and_modify(|e| {
            if is_evictable && !e.is_evictable {
                self.available += 1;
            }
            e.is_evictable = is_evictable
        });
    }

    fn available(&self) -> usize {
        self.available
    }
}
