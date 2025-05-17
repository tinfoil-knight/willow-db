#![allow(dead_code)]

use std::{fmt, sync::Arc};

use crate::{
    buffer::BufferManager,
    constants::SIZE_OF_INT,
    file::{BlockId, Page},
    log::{LogManager, Lsn},
};

struct RecoveryManager {
    lm: Arc<LogManager>,
    bm: Arc<BufferManager>,
}

impl RecoveryManager {
    fn commit() {}

    fn rollback() {}

    fn recover() {}
}

enum RecordType {
    Checkpoint = 0,
    Start = 1,
    Commit = 2,
    Rollback = 3,
    Update = 4,
}

impl TryFrom<i32> for RecordType {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Checkpoint),
            1 => Ok(Self::Start),
            2 => Ok(Self::Commit),
            3 => Ok(Self::Rollback),
            4 => Ok(Self::Update),
            _ => Err(()),
        }
    }
}

#[allow(clippy::upper_case_acronyms)]
enum UpdateValue {
    INT(i32),
    STRING(String),
}

impl UpdateValue {
    fn size(&self) -> usize {
        match &self {
            UpdateValue::INT(_) => SIZE_OF_INT,
            UpdateValue::STRING(s) => Page::str_size(s),
        }
    }
}

impl fmt::Display for UpdateValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match &self {
            UpdateValue::STRING(v) => format!("STRING {}", v),
            UpdateValue::INT(v) => format!("INT {}", v),
        };
        write!(f, "{s}")
    }
}

enum LogRecord {
    Checkpoint {},
    Start {
        txn_num: usize,
    },
    Commit {
        txn_num: usize,
    },
    Rollback {
        txn_num: usize,
    },
    Update {
        value: UpdateValue,
        txn_num: usize,
        offset: usize,
        block: BlockId,
    },
}

impl fmt::Display for LogRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s: String = match &self {
            LogRecord::Checkpoint {} => "<CHECKPOINT>".to_owned(),
            LogRecord::Start { txn_num } => format!("<START {}>", txn_num),
            LogRecord::Commit { txn_num } => format!("<COMMIT {}>", txn_num),
            LogRecord::Rollback { txn_num } => format!("<ROLLBACK {}>", txn_num),
            LogRecord::Update {
                value,
                txn_num,
                offset,
                block,
            } => format!("<UPDATE {} {} {} {}>", txn_num, block, offset, value),
        };
        write!(f, "{s}")
    }
}

impl LogRecord {
    fn new(bytes: Box<[u8]>) -> Option<Self> {
        let p: Page = bytes.into();

        if let Ok(record_type) = RecordType::try_from(p.get_int(0)) {
            match record_type {
                RecordType::Checkpoint => Some(Self::Checkpoint {}),
                RecordType::Start => Some(Self::Commit {
                    txn_num: p.get_int(SIZE_OF_INT) as usize,
                }),
                RecordType::Commit => Some(Self::Commit {
                    txn_num: p.get_int(SIZE_OF_INT) as usize,
                }),
                RecordType::Rollback => Some(Self::Commit {
                    txn_num: p.get_int(SIZE_OF_INT) as usize,
                }),
                RecordType::Update => todo!(),
            }
        } else {
            None
        }
    }

    fn operation(&self) -> RecordType {
        match &self {
            LogRecord::Checkpoint { .. } => RecordType::Checkpoint,
            LogRecord::Start { .. } => RecordType::Start,
            LogRecord::Commit { .. } => RecordType::Commit,
            LogRecord::Rollback { .. } => RecordType::Rollback,
            LogRecord::Update { .. } => RecordType::Update,
        }
    }

    fn txn_num(&self) -> Option<usize> {
        match &self {
            LogRecord::Checkpoint {} => None,
            LogRecord::Start { txn_num }
            | LogRecord::Commit { txn_num }
            | LogRecord::Rollback { txn_num }
            | LogRecord::Update { txn_num, .. } => Some(*txn_num),
        }
    }

    fn undo(&self, txn_num: usize) { // ? usize or Txn object
        match &self {
            LogRecord::Checkpoint {}
            | LogRecord::Start { .. }
            | LogRecord::Commit { .. }
            | LogRecord::Rollback { .. } => {}
            LogRecord::Update {
                value,
                txn_num: log_txn,
                offset,
                block,
            } => todo!(),
        }
    }

    fn write_to_log(&self, lm: Arc<LogManager>) -> Lsn {
        let op = self.operation();

        match &self {
            LogRecord::Checkpoint {} => {
                let mut p = Page::new(SIZE_OF_INT);
                p.set_int(0, op as i32);
                lm.append(p.contents())
            }
            LogRecord::Start { txn_num }
            | LogRecord::Commit { txn_num }
            | LogRecord::Rollback { txn_num } => {
                let mut p = Page::new(SIZE_OF_INT * 2);
                p.set_int(0, op as i32);
                p.set_int(SIZE_OF_INT, *txn_num as i32);
                lm.append(p.contents())
            }
            LogRecord::Update {
                value,
                txn_num,
                offset,
                block,
            } => {
                // op | txn_num | blk_filename | blk_number | offset | prev int value at offset

                let tpos = SIZE_OF_INT;
                let fpos = tpos + SIZE_OF_INT;
                let bpos = fpos + Page::str_size(block.filename());
                let opos = bpos + SIZE_OF_INT;
                let vpos = opos + SIZE_OF_INT;

                let val_size = value.size();

                let mut p = Page::new(vpos + val_size);
                p.set_int(0, op as i32);
                p.set_int(tpos, *txn_num as i32);
                p.set_string(fpos, block.filename());
                p.set_int(bpos, block.number() as i32);
                p.set_int(opos, *offset as i32);

                match value {
                    UpdateValue::INT(n) => {
                        p.set_int(vpos, *n);
                    }
                    UpdateValue::STRING(s) => {
                        p.set_string(vpos, s);
                    }
                };

                lm.append(p.contents())
            }
        }
    }
}
