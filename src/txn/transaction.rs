#![allow(dead_code)]

use std::sync::Arc;

use crate::{
    buffer::BufferManager,
    file::{BlockId, FileManager},
    log::LogManager,
};

use super::recovery::UpdateValue;
/// Transaction Number
pub type TxNum = usize;

pub struct Transaction {
    fm: Arc<FileManager>,
    lm: Arc<LogManager>,
    bm: Arc<BufferManager>,
}

impl Transaction {
    fn commit(&self) {}
    fn rollback(&self) {}
    fn recover(&self) {}
    pub fn pin(&self, block: &BlockId) {}
    pub fn unpin(&self, block: &BlockId) {}
    pub fn set_value(&self, block: &BlockId, offset: usize, v: &UpdateValue, ok_to_log: bool) {}
    fn get_value(&self, block: &BlockId, offset: usize) -> UpdateValue {
        todo!()
    }
}
