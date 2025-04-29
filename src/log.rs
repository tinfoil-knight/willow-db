#![allow(dead_code)]

use std::sync::{Arc, RwLock};

use crate::{
    constants::SIZE_OF_INT,
    file::{BlockId, FileManager, Page},
};

/// Log Sequence Number
pub type Lsn = u32;

struct LogManagerInner {
    fm: Arc<FileManager>,
    logfile: String,
    logpage: Page,
    current_block: BlockId,
    latest_lsn: Lsn,
    last_saved_lsn: Lsn,
}

impl LogManagerInner {
    fn new(fm: Arc<FileManager>, logfile: &str) -> Self {
        let mut logpage = Page::new(fm.block_size());
        let logsize = fm.length(logfile);
        let current_block = if logsize == 0 {
            let block = fm.append(logfile);
            logpage.set_int(0, fm.block_size() as i32);
            fm.write(&block, &mut logpage);
            block
        } else {
            let block = BlockId::new(logfile, logsize as usize - 1);
            fm.read(&block, &mut logpage);
            block
        };

        Self {
            fm,
            logfile: logfile.to_owned(),
            logpage,
            current_block,
            latest_lsn: 0,
            last_saved_lsn: 0,
        }
    }

    fn append(&mut self, record: Box<[u8]>) -> Lsn {
        let mut boundary = self.logpage.get_int(0);
        let record_size = record.len();
        let bytes_needed = record_size + SIZE_OF_INT;

        assert!(bytes_needed + SIZE_OF_INT <= self.fm.block_size());

        if boundary - (bytes_needed as i32) < SIZE_OF_INT as i32 {
            // doesn't fit so move to the next block
            self.flush();
            self.current_block = self.append_new_block();
            boundary = self.logpage.get_int(0);
        }

        // records are placed right -> left
        // boundary value is written to the start of the page
        // this allow the log itr. to read records in reverse order (i.e. left -> right)
        //
        // Page: [ boundary | gap | record n | ... | record1 ]
        // gap -> optional, in case everything doesn't fit exactly
        // 1..n -> order in which the log was written (record1 was written first and so on..)

        let record_pos = boundary as usize - bytes_needed;
        self.logpage.set_bytes(record_pos, &record);
        self.logpage.set_int(0, record_pos as i32);

        self.latest_lsn += 1;
        self.latest_lsn
    }

    fn append_new_block(&mut self) -> BlockId {
        let block = self.fm.append(&self.logfile);
        self.logpage.set_int(0, self.fm.block_size() as i32);
        self.fm.write(&block, &mut self.logpage);
        block
    }

    fn flush(&mut self) {
        self.fm.write(&self.current_block, &mut self.logpage);
        self.last_saved_lsn = self.latest_lsn;
    }
}

pub struct LogManager {
    inner: RwLock<LogManagerInner>,
}

impl LogManager {
    pub fn new(fm: Arc<FileManager>, logfile: &str) -> Self {
        Self {
            inner: RwLock::new(LogManagerInner::new(fm, logfile)),
        }
    }

    fn append(&self, record: Box<[u8]>) -> Lsn {
        let mut state = self.inner.write().unwrap();
        state.append(record)
    }

    /// Ensures that the content of the log are flushed at least till `lsn`.
    pub fn flush(&self, lsn: Lsn) {
        let last_saved_lsn = {
            let state = self.inner.read().unwrap();
            state.last_saved_lsn
        };

        if lsn > last_saved_lsn {
            let mut state = self.inner.write().unwrap();
            state.flush();
        }
    }

    /// Starts at the first (latest) record in the last block and iterates from the latest -> oldest record.
    fn iterator(&self) -> impl Iterator<Item = Box<[u8]>> {
        let (fm, block) = {
            let mut state = self.inner.write().unwrap();
            state.flush();
            (Arc::clone(&state.fm), state.current_block.clone())
        };

        LogIterator::new(fm, block)
    }
}

struct LogIterator {
    fm: Arc<FileManager>,
    block: BlockId,
    page: Page,
    current_pos: usize,
    boundary: usize,
}

impl LogIterator {
    fn new(fm: Arc<FileManager>, block: BlockId) -> Self {
        let page = Page::new(fm.block_size());
        let mut itr = Self {
            fm,
            block: block.clone(),
            page,
            current_pos: 0,
            boundary: 0,
        };
        itr.move_to_block(&block);
        itr
    }

    fn move_to_block(&mut self, block: &BlockId) {
        self.fm.read(block, &mut self.page);
        self.boundary = self.page.get_int(0) as usize;
        self.current_pos = self.boundary;
    }
}

impl Iterator for LogIterator {
    type Item = Box<[u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos < self.fm.block_size() || self.block.number() > 0 {
            if self.current_pos == self.fm.block_size() {
                let block = BlockId::new(self.block.filename(), self.block.number() - 1);
                self.move_to_block(&block);
                self.block = block;
            }
            let record = self.page.get_bytes(self.current_pos);
            self.current_pos += SIZE_OF_INT + record.len();
            return Some(record.into());
        }
        None
    }
}

#[cfg(test)]
mod tests {

    use std::{
        env,
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::file::Page;

    use super::*;

    impl LogManager {
        fn create_records(&mut self, start: i32, end: i32) {
            for i in start..=end {
                let record = Self::create_log_record(&format!("record{}", i), i + 100);
                self.append(record);
            }
        }

        fn create_log_record(s: &str, n: i32) -> Box<[u8]> {
            let npos = Page::str_size(s);
            let mut p = Page::new(npos + SIZE_OF_INT);
            p.set_string(0, s);
            p.set_int(npos, n);
            p.contents().into()
        }

        fn get_flushed_records(&self) -> Vec<Box<[u8]>> {
            self.iterator().collect()
        }
    }

    fn setup(block_size: usize) -> LogManager {
        let dirname = format!(
            "logtest_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        let dir_path = env::temp_dir().join(env!("CARGO_PKG_NAME")).join(dirname);
        let fm = Arc::new(FileManager::new(&dir_path, block_size));
        LogManager::new(fm, "db.log")
    }

    #[test]
    fn test_log_manager() {
        let mut lm = setup(400);

        lm.create_records(1, 35);

        let records = lm.get_flushed_records();
        assert_eq!(records.len(), 20);

        lm.create_records(36, 70);
        lm.flush(65);

        let records = lm.get_flushed_records();
        assert_eq!(records.len(), 70);

        let expected: Vec<String> = (1..=70).rev().map(|i| format!("record{}", i)).collect();

        for (idx, (rec, exp)) in records.iter().zip(expected.iter()).enumerate() {
            let p: Page = rec.clone().into();
            let actual = p.get_string(0);
            assert_eq!(
                actual, *exp,
                "mismatch at record {}: expected {}, got {}",
                idx, exp, actual
            );
        }
    }
}
