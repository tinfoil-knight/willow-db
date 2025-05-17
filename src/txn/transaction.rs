#![allow(dead_code)]

use std::sync::Arc;

use crate::{buffer::BufferManager, file::FileManager, log::LogManager};

pub struct Transaction {
    fm: Arc<FileManager>,
    lm: Arc<LogManager>,
    bm: Arc<BufferManager>,
}

impl Transaction {
    fn commit() {}
    fn rollback() {}
    fn recover() {}
}
