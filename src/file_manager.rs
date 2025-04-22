#![allow(dead_code)]

use std::{
    borrow::Cow,
    collections::HashMap,
    fmt,
    fs::{self, File, OpenOptions},
    hash::{DefaultHasher, Hash, Hasher},
    mem,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
};

#[derive(Clone, PartialEq, Hash)]
struct BlockId {
    filename: String,
    block_num: usize,
}

impl BlockId {
    fn new(filename: &str, block_num: usize) -> Self {
        BlockId {
            filename: filename.to_owned(),
            block_num,
        }
    }

    fn hash_code(&self) -> u64 {
        let mut hasher = DefaultHasher::default();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[file {} block {}]", self.filename, self.block_num)
    }
}

const SIZE_OF_I32: usize = mem::size_of::<i32>();

struct Page {
    byte_buf: Box<[u8]>,
}

impl Page {
    fn with_blocksize(block_size: usize) -> Self {
        Self {
            byte_buf: vec![0; block_size].into_boxed_slice(),
        }
    }

    fn with_bytes(b: &[u8]) -> Self {
        Self { byte_buf: b.into() }
    }

    fn get_int(&self, offset: usize) -> i32 {
        let bytes = self
            .byte_buf
            .get(offset..offset + SIZE_OF_I32)
            .expect("in bound");
        i32::from_le_bytes(bytes.try_into().unwrap())
    }

    fn set_int(&mut self, offset: usize, n: i32) {
        self.byte_buf[offset..offset + SIZE_OF_I32].copy_from_slice(&n.to_le_bytes());
    }

    fn get_bytes(&self, offset: usize) -> &[u8] {
        let len = self.get_int(offset);
        let start = offset + SIZE_OF_I32;

        self.byte_buf
            .get(start..start + len as usize)
            .expect("range to be in bound")
    }

    fn set_bytes(&mut self, offset: usize, bytes: &[u8]) {
        let len = bytes.len();
        self.set_int(offset, len as i32);

        let start = offset + SIZE_OF_I32;
        self.byte_buf[start..start + len].copy_from_slice(bytes);
    }

    fn get_string(&self, offset: usize) -> Cow<'_, str> {
        String::from_utf8_lossy(self.get_bytes(offset))
    }

    fn set_string(&mut self, offset: usize, s: &str) {
        self.set_bytes(offset, s.as_bytes());
    }

    fn str_size(s: &str) -> usize {
        SIZE_OF_I32 + s.len()
    }
}

struct FileManager {
    db_directory: PathBuf,
    block_size: usize,
    is_new: bool,
    open_files: Arc<RwLock<HashMap<String, Arc<Mutex<File>>>>>,
}

impl FileManager {
    fn new(db_directory: &Path, block_size: usize) -> Self {
        let path_exists = match db_directory.try_exists() {
            Ok(v) => v,
            Err(e) => panic!("failed to check db_directory path: {}", e),
        };
        if path_exists && !db_directory.is_dir() {
            panic!("specifed path is not a directory")
        }
        if !path_exists {
            println!("creating dir: {}", db_directory.to_string_lossy());
            fs::create_dir_all(db_directory).unwrap();
        }
        Self {
            db_directory: db_directory.to_owned(),
            block_size,
            is_new: !path_exists,
            open_files: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn read(&self, block: &BlockId, p: &mut Page) {
        let f_ptr = self.get_file(&block.filename);
        let f = f_ptr.lock().unwrap();
        let offset = block.block_num * self.block_size;

        f.read_exact_at(&mut p.byte_buf, offset as u64)
            .expect("failed to read page from file");
    }

    fn write(&self, block: &BlockId, p: &mut Page) {
        let f_ptr = self.get_file(&block.filename);
        let f = f_ptr.lock().unwrap();
        let offset = block.block_num * self.block_size;

        f.write_all_at(&p.byte_buf, offset as u64)
            .expect("failed to write page to file");
        f.sync_all().expect("failed to sync data to disk")
    }

    fn append(&self, filename: &str) -> BlockId {
        let block = BlockId::new(filename, self.length(filename) as usize);
        let bytes = vec![0; self.block_size].into_boxed_slice();

        let f_ptr = self.get_file(filename);
        let f = f_ptr.lock().unwrap();
        let offset = block.block_num * self.block_size;

        f.write_all_at(&bytes, offset as u64)
            .expect("failed to append to file");

        block
    }

    fn length(&self, filename: &str) -> u64 {
        let f_ptr = self.get_file(filename);
        let f = f_ptr.lock().unwrap();

        f.metadata()
            .expect("failed to get number of blocks in file")
            .len()
            / (self.block_size as u64)
    }

    fn get_file(&self, filename: &str) -> Arc<Mutex<File>> {
        if let Some(f) = self.open_files.read().unwrap().get(filename) {
            return Arc::clone(f);
        }
        let mut map = self.open_files.write().unwrap();

        let table_path = self.db_directory.join(filename);
        let table = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(table_path)
            .expect("failed to create file");

        map.insert(filename.to_owned(), Arc::new(Mutex::new(table)));

        Arc::clone(map.get(filename).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    #[test]
    fn test_file_manager() {
        let dir_path = env::temp_dir().join(env!("CARGO_PKG_NAME"));
        let fm = FileManager::new(&dir_path, 400);
        let fname = format!(
            "testfile_{}.tmp",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        let block = BlockId::new(&fname, 2);
        let mut p1 = Page::with_blocksize(fm.block_size);

        let pos1 = 88;
        let test_str = "abcdefg";
        p1.set_string(pos1, test_str);

        let size = Page::str_size(test_str);
        let pos2 = pos1 + size;
        let test_int = 345;
        p1.set_int(pos2, test_int);

        fm.write(&block, &mut p1);

        let mut p2 = Page::with_blocksize(fm.block_size);
        fm.read(&block, &mut p2);

        assert_eq!(p2.get_int(pos2), test_int);
        assert_eq!(p2.get_string(pos1), test_str);

        assert_eq!(fm.length(&fname), 3); // page was added at start offset of block 2; (0, 1, 2) => 3 blocks so far

        let appended_block = fm.append(&fname);
        assert_eq!(appended_block.block_num, 3);
        assert_eq!(fm.length(&fname), 4);
    }
}
