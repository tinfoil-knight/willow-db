#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::{Condvar, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use crate::file::BlockId;

use super::transaction::TxNum;

const MAX_TIME: Duration = Duration::from_secs(10);

enum Lock {
    /// Exclusive lock
    XLock,
    /// Shared lock with lock count
    SLock(usize),
}

pub(super) struct LockTable {
    locks: Mutex<HashMap<TxNum, HashMap<BlockId, Lock>>>,
    cvar: Condvar,
}

type LockGuard<'a> = MutexGuard<'a, HashMap<TxNum, HashMap<BlockId, Lock>>>;

impl LockTable {
    pub fn new() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
            cvar: Condvar::new(),
        }
    }

    /// Tries to acquire a shared lock on the specified block.
    /// If return value is `true` then lock was acquired.
    pub fn s_lock(&self, txn_num: TxNum, block: &BlockId) -> Result<(), &str> {
        let mut map = self.locks.lock().unwrap();
        let start = Instant::now();

        while Self::has_x_lock(&map, txn_num, block) && !Self::waiting_too_long(start) {
            let (guard, _) = self.cvar.wait_timeout(map, MAX_TIME).unwrap();
            map = guard;
        }

        if Self::has_x_lock(&map, txn_num, block) {
            return Err("lock aborted");
        }

        let new_val = match map.get(&txn_num).and_then(|x| x.get(block)) {
            Some(Lock::SLock(n)) => Lock::SLock(n + 1),
            _ => Lock::SLock(1),
        };
        map.entry(txn_num)
            .or_default()
            .insert(block.to_owned(), new_val);

        Ok(())
    }

    /// Tries to acquire an exclusive lock on the specified block.
    /// If return value is `true` then lock was acquired.
    ///
    /// This method assumes that a shared lock has already been acquired for the block.
    pub fn x_lock(&self, txn_num: TxNum, block: &BlockId) -> Result<(), &str> {
        let mut map = self.locks.lock().unwrap();
        let start = Instant::now();

        while Self::has_other_s_locks(&map, txn_num, block) && !Self::waiting_too_long(start) {
            let (guard, _) = self.cvar.wait_timeout(map, MAX_TIME).unwrap();
            map = guard;
        }

        if Self::has_other_s_locks(&map, txn_num, block) {
            return Err("lock aborted");
        }

        map.entry(txn_num)
            .or_default()
            .insert(block.to_owned(), Lock::XLock);

        Ok(())
    }

    /// Releases a lock on the specified block.
    pub fn unlock(&self, txn_num: TxNum, block: &BlockId) {
        let mut map = self.locks.lock().unwrap();
        map.entry(txn_num).and_modify(|x| match x.get(block) {
            Some(Lock::SLock(n)) if *n > 1 => {
                let new_val = Lock::SLock(*n - 1);
                x.insert(block.to_owned(), new_val);
            }
            _ => {
                x.remove(block);
                self.cvar.notify_all();
            }
        });
    }

    fn has_x_lock(map: &LockGuard, txn_num: TxNum, block: &BlockId) -> bool {
        map.get(&txn_num)
            .is_some_and(|x| matches!(x.get(block), Some(Lock::XLock)))
    }

    fn has_other_s_locks(map: &LockGuard, txn_num: TxNum, block: &BlockId) -> bool {
        map.get(&txn_num)
            .is_some_and(|x| matches!(x.get(block), Some(Lock::SLock(n)) if *n > 1))
    }

    fn waiting_too_long(start: Instant) -> bool {
        start.elapsed() >= MAX_TIME
    }
}
