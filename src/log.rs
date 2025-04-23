#![allow(dead_code)]

use crate::{
    constants::SIZE_OF_INT,
    file::{BlockId, FileManager, Page},
};

/// Log Sequence Number
type Lsn = u32;

struct LogManager {
    fm: FileManager,
    logfile: String,
    logpage: Page,
    current_block: BlockId,
    latest_lsn: Lsn,
    last_saved_lsn: Lsn,
}

impl LogManager {
    fn new(fm: FileManager, logfile: &str) -> Self {
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
        // todo: make this thread-safe

        let mut boundary = self.logpage.get_int(0);
        let record_size = record.len();
        let bytes_needed = record_size + SIZE_OF_INT;

        if boundary as usize - bytes_needed < SIZE_OF_INT {
            // doesn't fit so move to the next block

            self.flush();
            self.current_block = self.append_new_block();
            boundary = self.logpage.get_int(0);
        }

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

    fn flush_till(&mut self, lsn: Lsn) {
        if lsn > self.last_saved_lsn {
            self.flush();
        }
    }

    fn flush(&mut self) {
        self.fm.write(&self.current_block, &mut self.logpage);
        self.last_saved_lsn = self.latest_lsn;
    }
}

#[cfg(test)]
mod tests {

    use std::env;

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

        fn get_flushed_records(&self) -> Vec<&[u8]> {
            // could return the itr instead from here
            todo!()
        }
    }

    #[test]
    fn test_log_manager() {
        let dir_path = env::temp_dir().join(env!("CARGO_PKG_NAME"));
        let fm = FileManager::new(&dir_path, 400);
        let mut lm = LogManager::new(fm, "logtest");

        lm.create_records(1, 35);
        assert_eq!(lm.get_flushed_records().len(), 20);
        // todo: verify contents and reverse order

        lm.create_records(36, 70);
        lm.flush_till(65);
        assert_eq!(lm.get_flushed_records().len(), 70);
    }
}
