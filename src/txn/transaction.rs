#![allow(dead_code)]

use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
};

use crate::{
    buffer::{Buffer, BufferManager},
    file::{BlockId, FileManager},
    log::{LogManager, Lsn},
};

use super::{
    concurrency::ConcurrencyManager,
    recovery::{RecoveryManager, UpdateValue},
};

/// Transaction Number
pub type TxNum = usize;

struct BufferList {
    buffers: HashMap<BlockId, Arc<RwLock<Buffer>>>,
    pins: HashSet<BlockId>,
    bm: Arc<BufferManager>,
}

impl BufferList {
    fn new(bm: Arc<BufferManager>) -> Self {
        Self {
            buffers: HashMap::new(),
            pins: HashSet::new(),
            bm,
        }
    }

    fn get(&self, block: &BlockId) -> Option<&Arc<RwLock<Buffer>>> {
        self.buffers.get(block)
    }

    fn pin(&mut self, block: &BlockId) {
        let lock = self.bm.pin(block).unwrap();
        self.buffers.insert(block.to_owned(), lock);
        self.pins.insert(block.to_owned());
    }

    fn unpin(&mut self, block: &BlockId) {
        if let Some(buf) = self.buffers.get(block) {
            self.bm.unpin(buf.write().unwrap());
            self.pins.remove(block);
            if !self.pins.contains(block) {
                self.buffers.remove(block);
            }
        }
    }

    fn unpin_all(&mut self) {
        for block in &self.pins {
            if let Some(buf) = self.buffers.get(block) {
                self.bm.unpin(buf.write().unwrap());
            };
        }
        self.buffers.clear();
        self.pins.clear();
    }
}

pub struct Transaction {
    fm: Arc<FileManager>,
    lm: Arc<LogManager>,
    bm: Arc<BufferManager>,
    cm: Arc<Mutex<ConcurrencyManager>>,

    buffers: BufferList,
    txn_num: TxNum,
}

impl Transaction {
    fn new(
        txn_num: usize,
        fm: Arc<FileManager>,
        lm: Arc<LogManager>,
        bm: Arc<BufferManager>,
        cm: Arc<Mutex<ConcurrencyManager>>,
    ) -> Self {
        RecoveryManager::start(&lm, txn_num);
        let buffers = BufferList::new(Arc::clone(&bm));
        Self {
            fm,
            lm,
            bm,
            cm,
            txn_num,
            buffers,
        }
    }

    fn commit(&mut self) {
        RecoveryManager::commit(&self.bm, &self.lm, self.txn_num);
        self.cm.lock().unwrap().release(self.txn_num);
        self.buffers.unpin_all();
        println!("txn {} committed", self.txn_num)
    }

    fn rollback(&mut self) {
        let (bm, lm, txn_num) = (&self.bm.clone(), &self.lm.clone(), self.txn_num);
        RecoveryManager::rollback(bm, lm, txn_num, self);
        self.cm.lock().unwrap().release(self.txn_num);
        self.buffers.unpin_all();
        println!("txn {} rolled back", self.txn_num)
    }

    fn recover(&mut self) {
        self.bm.flush_all(self.txn_num);
        let (bm, lm, txn_num) = (&self.bm.clone(), &self.lm.clone(), self.txn_num);
        RecoveryManager::recover(bm, lm, txn_num, self);
    }

    pub fn pin(&mut self, block: &BlockId) {
        self.buffers.pin(block);
    }

    pub fn unpin(&mut self, block: &BlockId) {
        self.buffers.unpin(block);
    }

    pub fn set_value(&mut self, block: &BlockId, offset: usize, v: &UpdateValue, ok_to_log: bool) {
        self.cm.lock().unwrap().x_lock(self.txn_num, block);
        let buf_lock = self.buffers.get(block).unwrap();

        let lsn: Option<Lsn> = ok_to_log.then_some(RecoveryManager::set_update(
            &self.lm,
            self.txn_num,
            buf_lock.read().unwrap(),
            offset,
            v.clone(),
        ));

        let mut buf = buf_lock.write().unwrap();
        let p = buf.contents_mut();
        match v {
            UpdateValue::INT(n) => p.set_int(offset, *n),
            UpdateValue::STRING(s) => p.set_string(offset, s),
        }

        buf.set_modified(self.txn_num, lsn);
    }

    fn get_string(&self, block: &BlockId, offset: usize) -> String {
        self.cm.lock().unwrap().s_lock(self.txn_num, block);
        let buf_lock = self.buffers.get(block).unwrap();
        let buf = buf_lock.write().unwrap();

        let p = buf.contents();
        p.get_string(offset).into()
    }

    fn get_int(&self, block: &BlockId, offset: usize) -> i32 {
        self.cm.lock().unwrap().s_lock(self.txn_num, block);
        let buf_lock = self.buffers.get(block).unwrap();
        let buf = buf_lock.write().unwrap();

        let p = buf.contents();
        p.get_int(offset)
    }
}

struct TransactionManager {
    fm: Arc<FileManager>,
    lm: Arc<LogManager>,
    bm: Arc<BufferManager>,

    concurrency_mgr: Arc<Mutex<ConcurrencyManager>>,
    next_txn_num: AtomicUsize,
}

impl TransactionManager {
    fn new(fm: Arc<FileManager>, lm: Arc<LogManager>, bm: Arc<BufferManager>) -> Self {
        Self {
            fm,
            lm,
            bm,
            concurrency_mgr: Arc::new(Mutex::new(ConcurrencyManager::new())),
            next_txn_num: AtomicUsize::new(0),
        }
    }

    fn create_txn(&self) -> Transaction {
        let txn_num = self.next_txn_num.fetch_add(1, Ordering::SeqCst);
        Transaction::new(
            txn_num,
            self.fm.clone(),
            self.lm.clone(),
            self.bm.clone(),
            self.concurrency_mgr.clone(),
        )
    }
}
