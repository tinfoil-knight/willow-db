use std::{path::Path, sync::Arc};

use buffer::{BufferManager, EvictionPolicy};
use file::FileManager;
use log::LogManager;

mod buffer;
mod constants;
mod file;
mod log;

fn main() {
    let fm = Arc::new(FileManager::new(Path::new("testdb"), 1000));
    let lm = Arc::new(LogManager::new(Arc::clone(&fm), "willowdb.log"));
    let _bm = BufferManager::new(Arc::clone(&fm), lm, 400, EvictionPolicy::default());
}
