use std::fmt::{Display, Formatter, Result as FmtResult};

use sha2::{Digest, Sha256};

#[derive(Debug)]
pub struct Record {
    pub timestamp: u128,
    pub key_size: u32,
    pub value_size: u32,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Record {
    pub fn marshal(self) -> Vec<u8> {
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
    pub fn unmarshal(data: &[u8]) -> Record {
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

    pub fn length(&self) -> usize {
        // Hash + timestamp + key_size + value_size + key + value
        32 + 16 + 4 + 4 + self.key.len() + self.value.len()
    }
}

impl Display for Record {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_fmt(format_args!(
            "key = {} value = {} timestamp = {}",
            // TODO: Keys may not always be encoded in utf8!
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.value),
            self.timestamp,
        ))
    }
}
