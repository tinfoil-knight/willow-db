#![allow(dead_code)]

use std::collections::HashMap;

use super::{lock_table::LockTable, transaction::TxNum};
use crate::file::BlockId;

enum LockType {
    X,
    S,
}

pub(super) struct ConcurrencyManager {
    lock_tbl: LockTable,
    locks: HashMap<TxNum, HashMap<BlockId, LockType>>,
}

impl ConcurrencyManager {
    pub fn new() -> Self {
        Self {
            lock_tbl: LockTable::new(),
            locks: HashMap::new(),
        }
    }

    /// Acquires a shared lock on the block if no lock is already present.
    pub fn s_lock(&mut self, txn_num: TxNum, block: &BlockId) {
        let entry = self.locks.entry(txn_num).or_default();
        if !entry.contains_key(block) {
            self.lock_tbl
                .s_lock(txn_num, block)
                .expect("slock to be acquired");
            entry.insert(block.to_owned(), LockType::S);
        }
    }

    /// Acquires an exclusive lock on the block if no exclusive lock is already present.
    pub fn x_lock(&mut self, txn_num: TxNum, block: &BlockId) {
        if !self.has_x_lock(txn_num, block) {
            self.s_lock(txn_num, block);
            self.lock_tbl
                .x_lock(txn_num, block)
                .expect("xlock to be acquired");
            self.locks
                .entry(txn_num)
                .or_default()
                .insert(block.to_owned(), LockType::X);
        };
    }

    /// Releases all locks held by the transaction.
    pub fn release(&mut self, txn_num: TxNum) {
        if let Some(map) = self.locks.get(&txn_num) {
            for block in map.keys() {
                self.lock_tbl.unlock(txn_num, block);
            }
        }
        self.locks.remove(&txn_num);
    }

    fn has_x_lock(&self, txn_num: TxNum, block: &BlockId) -> bool {
        matches!(
            self.locks.get(&txn_num).and_then(|m| m.get(block)),
            Some(LockType::X)
        )
    }
}
