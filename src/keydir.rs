use std::collections::HashMap;

pub struct KeyDir {
    entries: HashMap<Vec<u8>, KeyDirRecord>,
}

#[derive(Copy, Clone)]
pub struct KeyDirRecord {
    pub timestamp: u128,
    pub file_id: u32,
    pub value_size: u32,
    pub value_offset: u64,
}

impl KeyDir {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn set(
        &mut self,
        key: &[u8],
        file_id: u32,
        value_size: u32,
        value_offset: u64,
        timestamp: u128,
    ) {
        self.entries.insert(
            key.into(),
            KeyDirRecord {
                timestamp: timestamp,
                file_id: file_id,
                value_offset: value_offset,
                value_size: value_size,
            },
        );
    }

    pub fn get(&self, key: &[u8]) -> Result<KeyDirRecord, String> {
        if let Some(val) = self.entries.get(key) {
            Ok(val.clone())
        } else {
            Err("key not found".to_string())
        }
    }
}
