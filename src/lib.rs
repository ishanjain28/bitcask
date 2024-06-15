mod record;
pub(crate) use record::*;

use std::{
    collections::HashMap,
    env,
    fs::{self, File},
    io::{Error as IoError, Read, Write},
    path::{Path, PathBuf},
    time,
};

pub struct BitCask {
    dir_name: String,
    active_file: ActiveFile,
    keydir: HashMap<Vec<u8>, IndexRecord>,
}

struct ActiveFile {
    file_id: u32,
    handle: File,
}

#[derive(Debug)]
struct IndexRecord {
    file_id: u32,
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

        let mut out = Self {
            dir_name: dir_name.to_owned(),
            active_file: ActiveFile {
                handle: File::create(active_file_path)?,
                file_id: 0,
            },
            keydir: HashMap::new(),
        };

        out.init();

        Ok(out)
    }

    fn derive_filepath_from_fileid(&self, file_id: u32) -> PathBuf {
        Path::new(&self.dir_name).join(format!("{:#06}.cask", file_id))
    }

    pub fn init(&mut self) {
        let mut file_id = 1;

        loop {
            let filename = self.derive_filepath_from_fileid(file_id);
            let mut file_handle = match File::open(&filename) {
                Ok(v) => v,
                Err(_) => {
                    // TODO: Add a debug logger marking the end here
                    break;
                }
            };
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
                        file_id,
                        timestamp: record.timestamp,
                        value_size: record.value_size,
                        value_offset: offset + record.length() - record.value_size as usize,
                    },
                );

                offset += record.length()
            }
            file_id += 1;
        }

        self.active_file.file_id = file_id;
        self.active_file.handle =
            File::create(Path::new(&self.dir_name).join(format!("{:#06}.cask", file_id)))
                .expect("error in creating new segment");
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
            let mut file_handle =
                File::open(&self.derive_filepath_from_fileid(index_record.file_id))
                    .expect("error in opening file");

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

        // TODO: Update keydir on writes
        // TODO: Create and use a new segment if it's over the size limit
        self.write(&data)?;

        self.keydir.insert(
            key.clone(),
            IndexRecord {
                file_id: self.active_file.file_id,
                timestamp: timestamp,
            },
        );

        Ok(())
    }
}

impl BitCask {
    fn write(&mut self, data: &[u8]) -> Result<(), IoError> {
        self.active_file.handle.write_all(data)?;

        self.flush()?;

        Ok(())
    }

    fn flush(&mut self) -> Result<(), IoError> {
        self.active_file.handle.flush()
    }
}
