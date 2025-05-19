#![allow(dead_code)]

use std::{fmt, sync::Arc};

use crate::{
    buffer::{Buffer, BufferManager},
    constants::SIZE_OF_INT,
    file::{BlockId, Page},
    log::{LogManager, Lsn},
};

use super::transaction::{Transaction, TxNum};

pub(super) struct RecoveryManager {
    lm: Arc<LogManager>,
    bm: Arc<BufferManager>,
}

impl RecoveryManager {
    pub fn new(lm: Arc<LogManager>, bm: Arc<BufferManager>) -> Self {
        Self { lm, bm }
    }

    pub fn start(&self, txn_num: TxNum) {
        LogRecord::Start { txn_num }.write_to_log(&self.lm);
    }

    pub fn commit(&self, txn_num: TxNum) {
        self.bm.flush_all(txn_num);
        let lsn = LogRecord::Commit { txn_num }.write_to_log(&self.lm);
        self.lm.flush(lsn);
    }

    pub fn rollback(&self, txn_num: TxNum, txn: &mut Transaction) {
        self.do_rollback(txn_num, txn);

        self.bm.flush_all(txn_num);
        let lsn = LogRecord::Rollback { txn_num }.write_to_log(&self.lm);
        self.lm.flush(lsn);
    }

    pub fn recover(&self, txn_num: TxNum, txn: &mut Transaction) {
        self.do_recover(txn);

        self.bm.flush_all(txn_num);
        let lsn = LogRecord::Checkpoint {}.write_to_log(&self.lm);
        self.lm.flush(lsn);
    }

    pub fn set_update(&self, txn_num: TxNum, buf: &Buffer, offset: usize, new_val: UpdateValue) -> Lsn {
        let old_val = match new_val {
            UpdateValue::INT(_) => UpdateValue::INT(buf.contents().get_int(offset)),
            UpdateValue::STRING(_) => {
                UpdateValue::STRING(buf.contents().get_string(offset).into_owned())
            }
        };
        let block = buf.block().unwrap().clone();
        LogRecord::Update {
            value: old_val,
            txn_num,
            offset,
            block,
        }
        .write_to_log(&self.lm)
    }

    fn do_rollback(&self, txn_num: TxNum, txn: &mut Transaction) {
        let itr = self.lm.iterator();
        for bytes in itr {
            let record = LogRecord::new(bytes).expect("valid record");
            if record.txn_num().is_some_and(|x| x == txn_num) {
                if record.operation() == RecordType::Start {
                    return;
                }
                record.undo(txn);
            }
        }
    }

    fn do_recover(&self, txn: &mut Transaction) {
        let itr = self.lm.iterator();
        let mut finished_txns = Vec::new();

        for bytes in itr {
            let record = LogRecord::new(bytes).expect("valid record");
            match record.operation() {
                RecordType::Checkpoint => return,
                RecordType::Commit | RecordType::Rollback => {
                    finished_txns.push(record.txn_num().unwrap());
                }
                _ => {
                    if !finished_txns.contains(&record.txn_num().unwrap()) {
                        record.undo(txn);
                    }
                }
            }
        }
    }
}

#[derive(PartialEq)]
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
enum UpdateValueType {
    INT = 0,
    STRING = 1,
}

impl TryFrom<i32> for UpdateValueType {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::INT),
            1 => Ok(Self::STRING),
            _ => Err(()),
        }
    }
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone)]
pub enum UpdateValue {
    INT(i32),
    STRING(String),
}

impl UpdateValue {
    fn data_type(&self) -> UpdateValueType {
        match &self {
            UpdateValue::INT(_) => UpdateValueType::INT,
            UpdateValue::STRING(_) => UpdateValueType::STRING,
        }
    }

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
        txn_num: usize,
        value: UpdateValue,
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
            let record = match record_type {
                RecordType::Checkpoint => Self::Checkpoint {},
                RecordType::Start => Self::Start {
                    txn_num: p.get_int(SIZE_OF_INT) as usize,
                },
                RecordType::Commit => Self::Commit {
                    txn_num: p.get_int(SIZE_OF_INT) as usize,
                },
                RecordType::Rollback => Self::Rollback {
                    txn_num: p.get_int(SIZE_OF_INT) as usize,
                },
                RecordType::Update => {
                    let tpos = SIZE_OF_INT;
                    let txn_num = p.get_int(tpos) as usize;

                    let fpos = tpos + SIZE_OF_INT;
                    let filename = p.get_string(fpos);

                    let bpos = fpos + Page::str_size(&filename);
                    let block_num = p.get_int(bpos);
                    let block = BlockId::new(&filename, block_num as usize);

                    let dtpos = bpos + SIZE_OF_INT;
                    let data_type =
                        UpdateValueType::try_from(p.get_int(dtpos)).expect("valid data type");

                    let opos = dtpos + SIZE_OF_INT;
                    let offset = p.get_int(opos) as usize;

                    let vpos = opos + SIZE_OF_INT;
                    let value = match data_type {
                        UpdateValueType::INT => UpdateValue::INT(p.get_int(vpos)),
                        UpdateValueType::STRING => {
                            UpdateValue::STRING(p.get_string(vpos).into_owned())
                        }
                    };

                    Self::Update {
                        txn_num,
                        value,
                        offset,
                        block,
                    }
                }
            };
            Some(record)
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

    fn undo(&self, txn: &mut Transaction) {
        match &self {
            LogRecord::Checkpoint {}
            | LogRecord::Start { .. }
            | LogRecord::Commit { .. }
            | LogRecord::Rollback { .. } => {}
            LogRecord::Update {
                value,
                offset,
                block,
                ..
            } => {
                txn.pin(block);
                txn.set_value(block, *offset, value, false);
                txn.unpin(block);
            }
        }
    }

    fn write_to_log(&self, lm: &Arc<LogManager>) -> Lsn {
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
                txn_num,
                value,
                offset,
                block,
            } => {
                // Physical Repr:
                // op | txn_num | blk_filename | blk_number | data type | offset | value

                let tpos = SIZE_OF_INT;
                let fpos = tpos + SIZE_OF_INT;
                let bpos = fpos + Page::str_size(block.filename());
                let dtpos = bpos + SIZE_OF_INT;
                let opos = dtpos + SIZE_OF_INT;
                let vpos = opos + SIZE_OF_INT;

                let val_size = value.size();

                let mut p = Page::new(vpos + val_size);
                p.set_int(0, op as i32);
                p.set_int(tpos, *txn_num as i32);
                p.set_string(fpos, block.filename());
                p.set_int(bpos, block.number() as i32);
                p.set_int(dtpos, value.data_type() as i32);
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
