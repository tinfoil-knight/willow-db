#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::{Condvar, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use crate::file::BlockId;

const MAX_TIME: Duration = Duration::from_secs(10);

enum Lock {
    /// Exclusive lock
    XLock,
    /// Shared lock with lock count
    SLock(usize),
}

struct LockTable {
    locks: Mutex<HashMap<BlockId, Lock>>,
    cvar: Condvar,
}

type LockGuard<'a> = MutexGuard<'a, HashMap<BlockId, Lock>>;

impl LockTable {
    /// Tries to acquire a shared lock on the specified block.
    /// If return value is `true` then lock was acquired.
    pub fn s_lock(&self, block: &BlockId) -> bool {
        let mut map = self.locks.lock().unwrap();
        let start = Instant::now();

        while Self::has_x_lock(&map, block) && !Self::waiting_too_long(start) {
            let (guard, _) = self.cvar.wait_timeout(map, MAX_TIME).unwrap();
            map = guard;
        }

        if Self::has_x_lock(&map, block) {
            return false;
        }

        let new_val = match map.get(block) {
            Some(Lock::SLock(n)) => Lock::SLock(n + 1),
            _ => Lock::SLock(1),
        };
        map.insert(block.to_owned(), new_val);

        true
    }

    /// Tries to acquire an exclusive lock on the specified block.
    /// If return value is `true` then lock was acquired.
    pub fn x_lock(&self, block: &BlockId) -> bool {
        let mut map = self.locks.lock().unwrap();
        let start = Instant::now();

        while Self::has_other_s_locks(&map, block) && !Self::waiting_too_long(start) {
            let (guard, _) = self.cvar.wait_timeout(map, MAX_TIME).unwrap();
            map = guard;
        }

        if Self::has_other_s_locks(&map, block) {
            return false;
        }

        map.insert(block.to_owned(), Lock::XLock);

        true
    }

    /// Releases a lock on the specified block.
    pub fn unlock(&self, block: &BlockId) {
        let mut map = self.locks.lock().unwrap();
        match map.get(block) {
            Some(Lock::SLock(n)) if *n > 1 => {
                let new_val = Lock::SLock(*n - 1);
                map.insert(block.to_owned(), new_val);
            }
            _ => {
                map.remove(block);
                self.cvar.notify_all();
            }
        }
    }

    fn has_x_lock(map: &LockGuard, block: &BlockId) -> bool {
        matches!(map.get(block), Some(Lock::XLock))
    }

    fn has_other_s_locks(map: &LockGuard, block: &BlockId) -> bool {
        matches!(map.get(block), Some(Lock::SLock(n)) if *n > 1)
    }

    fn waiting_too_long(start: Instant) -> bool {
        start.elapsed() >= MAX_TIME
    }
}
