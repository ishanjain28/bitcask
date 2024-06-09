mod record;
pub(crate) use record::*;
use sha2::digest::HashMarker;

use std::{
    collections::HashMap,
    env,
    fs::{self, read_dir, File},
    io::{Error as IoError, Write},
    path::Path,
    time,
};

pub struct BitCask {
    dir_name: String,
    active_file: File,
    keydir: HashMap<Vec<u8>, IndexRecord>,
}

struct IndexRecord {
    file_id: String,
    value_size: u64,
    value_offset: u64,
    timestamp: u128,
}

impl BitCask {
    pub fn open(dir_name: &str) -> Result<Self, IoError> {
        // TODO: Assume we open it to read/write
        // Create directory if it doesn't exist already
        let cwd = env::current_dir().expect("failed to read current directory");
        let db_path = Path::new(&cwd).join(dir_name);

        fs::create_dir_all(dir_name).expect("error in creating directory for database");

        // Create a lockfile in this directory, quit if a lockfile exists already
        let lock_file_path = Path::new(&db_path).join("db.lock");
        if let Err(e) = File::create_new(lock_file_path) {
            eprintln!("error in creating lockfile: {}", e);
        }

        // TODO:
        // Read all the files and construct in-memory index
        let active_file_path = Path::new(&db_path).join("000001.cask");

        let mut out = Self {
            dir_name: dir_name.to_owned(),
            active_file: File::create(active_file_path)?,
            keydir: HashMap::new(),
        };

        out.read_all_and_seed_keydir();

        Ok(out)
    }

    fn read_all_and_seed_keydir(&mut self) {
        let entries = fs::read_dir(&self.dir_name).expect("error in reading db directory");

        let mut files = vec![];

        for entry in entries {
            let entry = entry.expect("error in reading entry");
            let metadata = entry.metadata().expect("error in reading entry metadata");

            if metadata.is_dir() {
                continue;
            }

            if metadata.is_file() {
                if let Some(e) = entry.path().extension() {
                    if e != "cask" {
                        continue;
                    }
                } else {
                    continue;
                }
                files.push(entry.path());
            }
        }

        files.sort_unstable();

        println!("{:?}", files);
    }

    pub fn close(mut self) {
        let cwd = env::current_dir().expect("failed to read current directory");
        let db_path = Path::new(&cwd).join(&self.dir_name);

        // TODO: Flush all writes
        self.flush().expect("error in flushing data");

        let lock_file_path = Path::new(&db_path).join("db.lock");
        fs::remove_file(lock_file_path).expect("error in removing lockfile");
    }

    pub fn put(
        &mut self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<(), IoError> {
        let timestamp = time::UNIX_EPOCH.elapsed().unwrap().as_nanos();
        let key = key.into();
        let value = value.into();

        let record = Record {
            timestamp,
            key_size: key.len() as u32,
            value_size: value.len() as u32,
            key,
            value,
        };

        let data = record.marshal();

        self.write(&data)
    }
}

impl BitCask {
    fn write(&mut self, data: &[u8]) -> Result<(), IoError> {
        self.active_file.write_all(data)?;

        self.flush()?;

        Ok(())
    }

    fn flush(&mut self) -> Result<(), IoError> {
        self.active_file.flush()
    }
}
