use sha2::{Digest, Sha256};
use std::time;
use std::{
    env,
    fs::{self, File},
    path::Path,
};

pub struct BitCask {
    dir_name: String,
}

#[derive(Debug)]
struct Record {
    timestamp: u128,
    key_size: u32,
    value_size: u32,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Record {
    fn marshal(self) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(self.timestamp.to_be_bytes());
        hasher.update(self.key_size.to_be_bytes());
        hasher.update(self.value_size.to_be_bytes());
        hasher.update(&self.key);
        hasher.update(&self.value);

        let result = hasher.finalize();

        let mut output = Vec::with_capacity(56);
        output.extend(result);
        output.extend(self.timestamp.to_be_bytes());
        output.extend(self.key_size.to_be_bytes());
        output.extend(self.value_size.to_be_bytes());
        output.reserve((self.key_size + self.value_size) as usize);
        output.extend(self.key);
        output.extend(self.value);

        output
    }
    fn unmarshal(data: &[u8]) -> Record {
        let hash = &data[..32];
        let timestamp = u128::from_be_bytes([
            data[32], data[33], data[34], data[35], data[36], data[37], data[38], data[39],
            data[40], data[41], data[42], data[43], data[44], data[45], data[46], data[47],
        ]);
        let key_size = u32::from_be_bytes([data[48], data[49], data[50], data[51]]);
        let value_size = u32::from_be_bytes([data[52], data[53], data[54], data[55]]);

        let data = &data[56..];
        let key = &data[..key_size as usize];
        let value = &data[key_size as usize..key_size as usize + value_size as usize];

        let mut hasher = Sha256::new();
        hasher.update(timestamp.to_be_bytes());
        hasher.update(key_size.to_be_bytes());
        hasher.update(value_size.to_be_bytes());
        hasher.update(key);
        hasher.update(value);

        let result = hasher.finalize();

        assert_eq!(&result[..], hash);

        Record {
            timestamp,
            key_size,
            value_size,
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }
}

impl BitCask {
    pub fn open(dir_name: &str) -> Self {
        // TODO: Assume we open it to read/write
        // Create a lockfile in this directory, quit if a lockfile exists already

        // Create directory if it doesn't exist already
        let cwd = env::current_dir().expect("failed to read current directory");
        let db_path = Path::new(&cwd).join(dir_name);

        fs::create_dir_all(dir_name).expect("error in creating directory for database");

        let lock_file_path = Path::new(&db_path).join("db.lock");
        match File::create_new(lock_file_path) {
            Ok(_) => (),
            Err(e) => {
                eprintln!("error in creating lockfile: {}", e);
            }
        }

        Self {
            dir_name: dir_name.to_owned(),
        }
    }

    pub fn close(self) {
        let cwd = env::current_dir().expect("failed to read current directory");
        let db_path = Path::new(&cwd).join(self.dir_name);

        // TODO: Flush all writes

        let lock_file_path = Path::new(&db_path).join("db.lock");
        fs::remove_file(lock_file_path).expect("error in removing lockfile");
    }

    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
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

        let result = record.marshal();
        let record = Record::unmarshal(&result);

        println!("{:?}", record);
    }
}
