#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{
    file::{BlockId, FileManager, Page},
    log::{LogManager, Lsn},
};

struct Buffer {
    fm: Arc<FileManager>,
    lm: Arc<LogManager>,
    contents: Page,
    block: Option<BlockId>,
    pins: usize,
    /// Used to check if the page is modified. Some(T) indicates modified.
    txn_num: Option<usize>,
    lsn: Option<Lsn>,
}

impl Buffer {
    fn new(fm: Arc<FileManager>, lm: Arc<LogManager>) -> Self {
        let contents = Page::new(fm.block_size());
        Self {
            fm,
            lm,
            contents,
            block: None,
            pins: 0,
            txn_num: None,
            lsn: None,
        }
    }

    fn contents(&mut self) -> &mut Page {
        &mut self.contents
    }

    fn block(&self) -> Option<&BlockId> {
        self.block.as_ref()
    }

    fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    fn modifying_txn(&self) -> Option<usize> {
        self.txn_num
    }

    fn set_modified(&mut self, txn_num: usize, lsn: Option<Lsn>) {
        self.txn_num = Some(txn_num);
        if lsn.is_some() {
            // Lsn won't be present in case no log record is generated for an update.
            self.lsn = lsn;
        }
    }

    fn assign_to_block(&mut self, block: &BlockId) {
        self.flush();
        self.fm.read(block, &mut self.contents);
        self.pins = 0;
    }

    fn flush(&mut self) {
        if self.txn_num.is_some() {
            self.lm.flush(self.lsn.unwrap());
            self.txn_num = None
        }
    }

    fn pin(&mut self) {
        self.pins += 1
    }

    fn unpin(&mut self) {
        self.pins = self.pins.saturating_sub(1);
    }
}

/// index in the buffer pool
type BufferId = usize;

struct BufferManagerInner {
    buf_table: HashMap<BlockId, BufferId>,
    free_list: Vec<BufferId>,
    pool: Box<[Arc<RwLock<Buffer>>]>,
}

impl BufferManagerInner {
    fn new(fm: Arc<FileManager>, lm: Arc<LogManager>, capacity: usize) -> Self {
        let mut v = Vec::new();
        v.resize_with(capacity, || {
            Arc::new(RwLock::new(Buffer::new(Arc::clone(&fm), Arc::clone(&lm))))
        });

        Self {
            buf_table: HashMap::new(),
            pool: v.into_boxed_slice(),
            free_list: (0..capacity).collect(),
        }
    }

    fn pin(&mut self, block: &BlockId) -> Option<Arc<RwLock<Buffer>>> {
        // find existing buffer or choose an unpinned buffer
        let existing = self.buf_table.get(block).copied();
        let pos = existing.or_else(|| self.free_list.pop())?;

        let lock = self.pool.get(pos)?;
        let mut buf = lock.write().unwrap();
        buf.assign_to_block(block);
        buf.pin();

        if existing.is_none() {
            self.buf_table.insert(block.clone(), pos);
        }

        Some(Arc::clone(lock))
    }

    fn unpin(&mut self, block: &BlockId) {
        if let Some(&idx) = self.buf_table.get(block) {
            let mut buf = self.pool.get(idx).unwrap().write().unwrap();
            buf.unpin();
            if !buf.is_pinned() {
                self.free_list.push(idx);
            }
        }
    }

    fn available(&self) -> usize {
        self.free_list.len()
    }

    fn flush_all(&self, txn_num: usize) {
        for &buffer_id in self.buf_table.values() {
            let matches = {
                let buf = self.pool.get(buffer_id).unwrap().read().unwrap();
                buf.modifying_txn().is_some_and(|x| x == txn_num)
            };
            if matches {
                let mut buf = self.pool.get(buffer_id).unwrap().write().unwrap();
                buf.flush();
            }
        }
    }
}

pub struct BufferManager {
    state: RwLock<BufferManagerInner>,
}

impl BufferManager {
    pub fn new(fm: Arc<FileManager>, lm: Arc<LogManager>, capacity: usize) -> Self {
        Self {
            state: RwLock::new(BufferManagerInner::new(fm, lm, capacity)),
        }
    }

    fn pin(&self, block: &BlockId) -> Result<Arc<RwLock<Buffer>>, &str> {
        let mut state = self.state.write().unwrap();
        state.pin(block).ok_or("buffer abort")
    }

    fn unpin(&self, block: &BlockId) {
        let mut state = self.state.write().unwrap();
        state.unpin(block);
    }

    fn available(&self) -> usize {
        let state = self.state.read().unwrap();
        state.free_list.len()
    }

    fn flush_all(&self, txn_num: usize) {
        let state = self.state.read().unwrap();
        state.flush_all(txn_num);
    }
}
