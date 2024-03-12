#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{bloom::Bloom, BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    // These are temporary holders for the first and last keys of the block currently under
    // construction.
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));

        if self.builder.add(key, value) {
            self.last_key.set_from_slice(key);
            return;
        }

        self.store_block_and_meta_to_memory();

        assert!(self.builder.add(key, value));
        self.last_key.set_from_slice(key);
        self.first_key.set_from_slice(key);
    }

    fn store_block_and_meta_to_memory(&mut self) {
        let meta = BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        };
        self.meta.push(meta);

        let block = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        self.data.extend(block.build().encode());
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.store_block_and_meta_to_memory();

        // Meta section follows the data section followed by meta offset
        let block_meta_offset = self.data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);
        self.data.put_u32(block_meta_offset as u32);

        // ... followed by bloom filter and its ofset
        let bloom = Bloom::build_from_key_hashes(
            self.key_hashes.as_slice(),
            Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01),
        );
        let bloom_offset = self.data.len();
        bloom.encode(&mut self.data);
        self.data.put_u32(bloom_offset as u32);

        let file = FileObject::create(path.as_ref(), self.data)?;
        Ok(SsTable {
            file,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
