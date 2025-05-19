#![allow(dead_code)]

use std::{collections::HashMap, sync::Arc};

use super::lock_table::LockTable;
use crate::file::BlockId;

enum LockType {
    X,
    S,
}

struct ConcurrencyManager {
    lock_tbl: Arc<LockTable>,
    locks: HashMap<BlockId, LockType>,
}

impl ConcurrencyManager {
    pub fn new(lock_tbl: Arc<LockTable>) -> Self {
        Self {
            lock_tbl,
            locks: HashMap::new(),
        }
    }

    /// Acquires a shared lock on the block if no lock is already present.
    pub fn s_lock(&mut self, block: &BlockId) {
        if !self.locks.contains_key(block) {
            self.lock_tbl.s_lock(block).expect("slock to be acquired");
            self.locks.insert(block.to_owned(), LockType::S);
        }
    }

    /// Acquires an exclusive lock on the block if no exclusive lock is already present.
    pub fn x_lock(&mut self, block: &BlockId) {
        if !self.has_x_lock(block) {
            self.s_lock(block);
            self.lock_tbl.x_lock(block).expect("xlock to be acquired");
            self.locks.insert(block.to_owned(), LockType::X);
        }
    }

    /// Releases all locks held by the transaction.
    pub fn release(&mut self) {
        for block in self.locks.keys() {
            self.lock_tbl.unlock(block);
        }
        self.locks.clear();
    }

    fn has_x_lock(&self, block: &BlockId) -> bool {
        matches!(self.locks.get(block), Some(LockType::X))
    }
}
