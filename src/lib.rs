mod keydir;
mod record;
use keydir::KeyDir;
pub(crate) use record::*;

use std::{
    collections::HashMap,
    env,
    fs::{self, File},
    io::{Error as IoError, Read, Write},
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    time,
};

pub struct BitCask {
    options: BitCaskOptions,
    keydir: KeyDir,
    active_file_id: u32,
    active_file: Option<File>,
}

pub struct BitCaskOptions {
    pub dir_name: String,
    pub segment_size_limit: u64,
}

impl Default for BitCaskOptions {
    fn default() -> Self {
        Self {
            dir_name: "db".to_string(),
            segment_size_limit: 100 * 1024 * 1024, // 100MiB
        }
    }
}

impl BitCask {
    pub fn open(options: BitCaskOptions) -> Result<Self, IoError> {
        // TODO: Assume we open it to read/write
        // Create directory if it doesn't exist already
        let cwd = env::current_dir().expect("failed to read current directory");
        let db_path = Path::new(&cwd).join(&options.dir_name);

        fs::create_dir_all(&db_path).expect("error in creating directory for database");

        // Create a lockfile in this directory, quit if a lockfile exists already
        let lock_file_path = Path::new(&db_path).join("db.lock");
        File::create_new(lock_file_path).expect("error in creating lockfile");

        // TODO:
        // Read all the files and construct in-memory index
        let mut out = Self {
            options: options,
            active_file_id: 0,
            active_file: None,
            keydir: KeyDir::new(),
        };

        out.init()?;

        Ok(out)
    }

    fn create_and_use_segment(&mut self, file_id: u32) -> Result<(), IoError> {
        self.active_file_id = file_id;

        self.active_file = Some(File::create(derive_filepath_from_fileid(
            &self.options.dir_name,
            self.active_file_id,
        ))?);

        Ok(())
    }

    pub fn init(&mut self) -> Result<(), IoError> {
        let mut file_id = 1;

        loop {
            let filename = derive_filepath_from_fileid(&self.options.dir_name, file_id);
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

            let mut offset = 0u64;
            while offset < contents.len() as u64 {
                let content = &contents[offset as usize..];

                let record = Record::unmarshal(content);

                self.keydir.set(
                    &record.key,
                    file_id,
                    record.value_size,
                    offset + record.length() - record.value_size as u64,
                    record.timestamp,
                );

                offset += record.length();
            }

            file_id += 1;
        }

        self.create_and_use_segment(file_id)?;

        Ok(())
    }

    pub fn close(mut self) {
        let cwd = env::current_dir().expect("failed to read current directory");
        let db_path = Path::new(&cwd).join(&self.options.dir_name);

        // TODO: Flush all writes
        self.flush().expect("error in flushing data");

        let lock_file_path = Path::new(&db_path).join("db.lock");
        fs::remove_file(lock_file_path).expect("error in removing lockfile");
    }

    pub fn get(&mut self, key: impl Into<Vec<u8>>) -> Result<Record, String> {
        let key = key.into();
        let index_record = self.keydir.get(&key)?;

        let mut file = File::open(derive_filepath_from_fileid(
            &self.options.dir_name,
            index_record.file_id,
        ))
        .expect("error in opening file");

        // TODO: try_reserve_exact from file metadata
        // or read in smaller chunks
        let mut contents = vec![];
        file.read_to_end(&mut contents)
            .expect("error in reading file");

        println!("{:?}", contents);
        let value = &contents[index_record.value_offset as usize
            ..index_record.value_offset as usize + index_record.value_size as usize];

        let record = Record {
            timestamp: index_record.timestamp,
            key_size: key.len() as u32,
            value_size: index_record.value_size,
            key: key.clone(),
            value: value.to_vec(),
        };

        Ok(record)
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
            key: key.clone(),
            value: value.clone(),
        };

        let data = record.clone().marshal();

        if self.active_file.as_mut().unwrap().metadata()?.size() >= self.options.segment_size_limit
        {
            self.create_and_use_segment(self.active_file_id + 1)?;
        }

        // TODO: Update keydir on writes
        // TODO: Create and use a new segment if it's over the size limit
        let offset = self.write(&data)?;

        self.keydir.set(
            &key,
            self.active_file_id,
            value.len() as u32,
            offset + record.length() - value.len() as u64,
            record.timestamp,
        );

        Ok(())
    }
}

impl BitCask {
    fn write(&mut self, data: &[u8]) -> Result<u64, IoError> {
        let offset = self.active_file.as_ref().unwrap().metadata()?.size();

        self.active_file.as_mut().unwrap().write_at(data, offset)?;

        self.flush()?;

        Ok(offset)
    }

    fn flush(&mut self) -> Result<(), IoError> {
        self.active_file.as_mut().unwrap().flush()
    }
}

fn derive_filepath_from_fileid(dir_name: &str, file_id: u32) -> PathBuf {
    Path::new(&dir_name).join(format!("{:#08}.cask", file_id))
}
