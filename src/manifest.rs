use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, bail};
use bytes::Buf;
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)?,
            )),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut records = Vec::new();
        let mut buf = buf.as_slice();
        while !buf.is_empty() {
            let len = buf.get_u64();
            let slice = &buf[..len as usize];
            let json = serde_json::from_slice::<ManifestRecord>(slice)?;
            buf.advance(len as usize);
            let checksum = buf.get_u32();
            if checksum != crc32fast::hash(slice) {
                bail!("checksum mismatched!");
            }
            records.push(json);
        }
        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();
        let json = serde_json::to_vec(&record)?;
        let len = json.len() as u64;
        let checksum = crc32fast::hash(&json);
        file.write_all(&len.to_be_bytes())?;
        file.write_all(&json)?;
        file.write_all(&checksum.to_be_bytes())?;
        file.sync_all()?;
        Ok(())
    }
}
