#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::{Arc, RwLock, RwLockWriteGuard},
};

use crate::{
    file::{BlockId, FileManager, Page},
    log::{LogManager, Lsn},
    txn::TxNum,
};

use super::replacer::{EvictionPolicy, Replacer};

pub struct Buffer {
    fm: Arc<FileManager>,
    lm: Arc<LogManager>,
    contents: Page,
    block: Option<BlockId>,
    /// Some(t) indicates that the page is modified where
    /// t is the txn_num that made the change.
    txn_num: Option<TxNum>,
    /// If page is modified then this holds the LSN of the most recent log record.
    /// None indicates that no log record was generated for the update.
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
            txn_num: None,
            lsn: None,
        }
    }

    pub fn contents(&self) -> &Page {
        &self.contents
    }

    pub fn contents_mut(&mut self) -> &mut Page {
        &mut self.contents
    }

    pub fn block(&self) -> Option<&BlockId> {
        self.block.as_ref()
    }

    fn modifying_txn(&self) -> Option<TxNum> {
        self.txn_num
    }

    pub fn set_modified(&mut self, txn_num: usize, lsn: Option<Lsn>) {
        self.txn_num = Some(txn_num);
        if lsn.is_some() {
            // Lsn won't be present in case no log record is generated for an update.
            self.lsn = lsn;
        }
    }

    fn assign_to_block(&mut self, block: &BlockId) {
        self.flush();
        self.block = Some(block.clone());
        self.fm.read(block, &mut self.contents);
    }

    fn flush(&mut self) {
        if self.txn_num.is_some() {
            self.lm.flush(self.lsn.unwrap());
            self.fm.write(self.block().unwrap(), &self.contents);
            self.txn_num = None
        }
    }
}

/// index in the buffer pool
type BufferId = usize;

#[derive(Copy, Clone)]
struct BufferMeta {
    pos: BufferId,
    pins: usize,
}

struct BufferManagerInner {
    buf_table: HashMap<BlockId, BufferMeta>,
    free_list: Vec<BufferId>,
    pool: Box<[Arc<RwLock<Buffer>>]>,
    replacer: Box<dyn Replacer>,
}

impl BufferManagerInner {
    fn new(
        fm: Arc<FileManager>,
        lm: Arc<LogManager>,
        capacity: usize,
        eviction_policy: EvictionPolicy,
    ) -> Self {
        let mut v = Vec::new();
        v.resize_with(capacity, || {
            Arc::new(RwLock::new(Buffer::new(Arc::clone(&fm), Arc::clone(&lm))))
        });

        Self {
            buf_table: HashMap::new(),
            free_list: (0..capacity).collect(),
            pool: v.into_boxed_slice(),
            replacer: eviction_policy.into(),
        }
    }

    fn pin(&mut self, block: &BlockId) -> Option<Arc<RwLock<Buffer>>> {
        // find existing buffer or choose an un-pinned buffer
        let existing = self.buf_table.get(block).copied().map(|e| e.pos);
        let pos = existing
            .or_else(|| self.free_list.pop())
            .or_else(|| self.replacer.evict())?;

        let buf_lock = self.pool.get_mut(pos)?;
        if existing.is_none() {
            buf_lock.write().unwrap().assign_to_block(block);
        }

        self.buf_table
            .entry(block.to_owned())
            .and_modify(|e| {
                e.pins += 1;
            })
            .or_insert(BufferMeta { pos, pins: 1 });

        self.replacer.record_access(pos);

        Some(Arc::clone(buf_lock))
    }

    fn unpin(&mut self, buf: RwLockWriteGuard<Buffer>) {
        let block = buf.block().unwrap();
        if let Some(e) = self.buf_table.get_mut(block) {
            e.pins = e.pins.saturating_sub(1);
            let is_pinned = e.pins > 0;
            if !is_pinned {
                let meta = self.buf_table.remove(block).unwrap();
                self.replacer.set_evictable(meta.pos, true);
            }
        };
    }

    fn available(&self) -> usize {
        self.free_list.len() + self.replacer.available()
    }

    fn flush_all(&mut self, txn_num: TxNum) {
        for meta in self.buf_table.values() {
            let matches = {
                let buf = self.pool.get(meta.pos).unwrap().read().unwrap();
                buf.modifying_txn().is_some_and(|x| x == txn_num)
            };
            if matches {
                let mut buf = self.pool.get(meta.pos).unwrap().write().unwrap();
                buf.flush();
            }
        }
    }
}

pub struct BufferManager {
    state: RwLock<BufferManagerInner>,
}

impl BufferManager {
    pub fn new(
        fm: Arc<FileManager>,
        lm: Arc<LogManager>,
        capacity: usize,
        eviction_policy: EvictionPolicy,
    ) -> Self {
        Self {
            state: RwLock::new(BufferManagerInner::new(fm, lm, capacity, eviction_policy)),
        }
    }

    pub fn pin(&self, block: &BlockId) -> Option<Arc<RwLock<Buffer>>> {
        let mut state = self.state.write().unwrap();
        state.pin(block)
    }

    pub fn unpin(&self, buf: RwLockWriteGuard<Buffer>) {
        let mut state = self.state.write().unwrap();
        state.unpin(buf);
    }

    fn available(&self) -> usize {
        let state = self.state.read().unwrap();
        state.free_list.len() + state.replacer.available()
    }

    pub fn flush_all(&self, txn_num: TxNum) {
        let mut state = self.state.write().unwrap();
        state.flush_all(txn_num);
    }
}

#[cfg(test)]
mod tests {

    use std::{
        env,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    fn setup(
        prefix: &str,
        block_size: usize,
        capacity: usize,
    ) -> (Arc<FileManager>, BufferManager) {
        let dirname = format!(
            "{}_{}",
            prefix,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        let dir_path = env::temp_dir().join(env!("CARGO_PKG_NAME")).join(dirname);
        let fm = Arc::new(FileManager::new(&dir_path, block_size));
        let lm = Arc::new(LogManager::new(Arc::clone(&fm), "db.log"));
        (
            Arc::clone(&fm),
            BufferManager::new(fm, lm, capacity, EvictionPolicy::default()),
        )
    }

    #[test]
    fn test_buffer() {
        let (fm, bm) = setup("buffertest", 400, 3);
        let fname = "testfile";

        assert_eq!(bm.available(), 3);

        let (bid1, bid2) = (BlockId::new(fname, 1), BlockId::new(fname, 2));

        let buf1_lock = bm.pin(&bid1).unwrap();
        let mut buf1 = buf1_lock.write().unwrap();
        let p = buf1.contents_mut();

        let n = p.get_int(80);
        assert_eq!(n, 0);

        // this modification will get written to disk
        p.set_int(80, n + 1);
        buf1.set_modified(1, Some(0));
        bm.unpin(buf1);

        assert_eq!(bm.available(), 3);

        let buf2_lock = bm.pin(&BlockId::new(fname, 2)).unwrap();
        let buf2 = buf2_lock.write().unwrap();

        bm.pin(&BlockId::new(fname, 3)).unwrap();
        bm.pin(&BlockId::new(fname, 4)).unwrap();

        // ^one of these pins should've flushed block1 to disk
        bm.unpin(buf2);
        assert_eq!(bm.available(), 1);

        // verify that block1 was written to disk

        let mut p1 = Page::new(fm.block_size());
        fm.read(&bid1, &mut p1);

        assert_eq!(p1.get_int(80), 1);

        let buf2_lock = bm.pin(&bid2).unwrap();
        let mut buf2 = buf2_lock.write().unwrap();
        let p2 = buf2.contents_mut();

        // this modification won't get written to disk
        p2.set_int(80, 9999);
        buf2.set_modified(1, Some(0));
        bm.unpin(buf2);

        // verify that block2 wasn't written to disk

        let mut p2 = Page::new(fm.block_size());
        fm.read(&bid2, &mut p2);

        assert_eq!(p2.get_int(80), 0);
    }

    #[test]
    fn test_buffer_manager() {
        let (_fm, bm) = setup("buffermgrtest", 400, 3);
        let fname = "testfile";

        let mut bufv = Vec::new();
        bufv.resize_with(6, || None);

        let (bid0, bid1, bid2, bid3) = (
            BlockId::new(fname, 0),
            BlockId::new(fname, 1),
            BlockId::new(fname, 2),
            BlockId::new(fname, 3),
        );

        bufv[0] = bm.pin(&bid0);
        bufv[1] = bm.pin(&bid1);
        bufv[2] = bm.pin(&bid2);

        bm.unpin(bufv[1].as_mut().unwrap().write().unwrap());
        bufv[1] = None;

        bufv[3] = bm.pin(&bid0);
        bufv[4] = bm.pin(&bid1);

        assert_eq!(bm.available(), 0);
        bufv[5] = bm.pin(&bid3);
        assert!(bufv[5].is_none());

        bm.unpin(bufv[2].as_mut().unwrap().write().unwrap());
        bufv[2] = None;

        bufv[5] = bm.pin(&bid3);
        assert!(bufv[5].is_some());
    }
}
