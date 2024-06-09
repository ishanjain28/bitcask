mod record;
pub(crate) use record::*;
use sha2::digest::HashMarker;

use std::{
    collections::HashMap,
    env,
    ffi::OsString,
    fs::{self, read_dir, File},
    io::{Error as IoError, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    time,
};

pub struct BitCask {
    dir_name: String,
    active_file: File,
    keydir: HashMap<Vec<u8>, IndexRecord>,
}

#[derive(Debug)]
struct IndexRecord {
    file_id: PathBuf,
    value_size: u32,
    value_offset: usize,
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

        let out = Self {
            dir_name: dir_name.to_owned(),
            active_file: File::create(active_file_path)?,
            keydir: HashMap::new(),
        };

        Ok(out)
    }

    pub fn read_all_and_seed_keydir(&mut self) {
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

        for file in files {
            // TODO:
            // Read all to memory
            // Read the whole file 1 record at a time and keep updating keydir
            let mut file_handle = File::open(&file).expect("error in opening file");
            // TODO: try_reserve_exact from file metadata
            // or read in smaller chunks
            let mut contents = vec![];
            file_handle
                .read_to_end(&mut contents)
                .expect("error in reading file");

            let mut offset = 0;
            while offset < contents.len() {
                let content = &contents[offset..];

                let record = Record::unmarshal(content);

                self.keydir.insert(
                    record.key.clone(),
                    IndexRecord {
                        file_id: file.clone(),
                        timestamp: record.timestamp,
                        value_size: record.value_size,
                        value_offset: offset + record.length() - record.value_size as usize,
                    },
                );

                offset += record.length()
            }
        }
    }

    pub fn close(mut self) {
        let cwd = env::current_dir().expect("failed to read current directory");
        let db_path = Path::new(&cwd).join(&self.dir_name);

        // TODO: Flush all writes
        self.flush().expect("error in flushing data");

        let lock_file_path = Path::new(&db_path).join("db.lock");
        fs::remove_file(lock_file_path).expect("error in removing lockfile");
    }

    pub fn get(&mut self, key: impl Into<Vec<u8>>) -> Result<Record, String> {
        let key = key.into();

        if let Some(index_record) = self.keydir.get(&key) {
            let mut file_handle = File::open(&index_record.file_id).expect("error in opening file");
            // TODO: try_reserve_exact from file metadata
            // or read in smaller chunks
            let mut contents = vec![];
            file_handle
                .read_to_end(&mut contents)
                .expect("error in reading file");

            let value = &contents[index_record.value_offset
                ..index_record.value_offset + index_record.value_size as usize];

            let record = Record {
                timestamp: index_record.timestamp,
                key_size: key.len() as u32,
                value_size: index_record.value_size,
                key: key.clone(),
                value: value.to_vec(),
            };

            Ok(record)
        } else {
            Err("could not find key".to_string())
        }
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
