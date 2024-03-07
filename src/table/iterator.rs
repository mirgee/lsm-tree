#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        Ok(Self {
            table: table.clone(),
            blk_iter: BlockIterator::create_and_seek_to_first(table.read_block(0)?),
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let first_key = self.table.first_key.clone();
        self.seek_to_key(first_key.as_key_slice())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let (blk_idx, blk_iter) = SsTableIterator::seek_to_key_in_table(table.clone(), key)?;
        Ok(Self {
            table,
            blk_iter,
            blk_idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (blk_idx, blk_iter) = SsTableIterator::seek_to_key_in_table(self.table.clone(), key)?;
        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;
        Ok(())
    }

    fn seek_to_key_in_table(table: Arc<SsTable>, key: KeySlice) -> Result<(usize, BlockIterator)> {
        let mut idx = table.find_block_idx(key);
        let mut iter = BlockIterator::create_and_seek_to_key(table.read_block(idx).unwrap(), key);
        if !iter.is_valid() {
            SsTableIterator::search_to_next_block(&mut idx, &mut iter, table.clone())?;
        }
        Ok((idx, iter))
    }

    fn search_to_next_block(idx: &mut usize, iter: &mut BlockIterator, table: Arc<SsTable>) -> Result<()> {
        *idx += 1;
        if *idx < table.num_of_blocks() {
            *iter = BlockIterator::create_and_seek_to_first(table.read_block(*idx)?);
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    // TODO: Why are we allowing such a strange behavior if the iterator is invalid?
    // This is not a rubust API and may result in bugs if misused
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.is_valid() {
            // At the end of the last block, we bump the index, but do not read the next block,
            // the current block stays invalid
            Self::search_to_next_block(&mut self.blk_idx, &mut self.blk_iter, self.table.clone())?;
        }
        Ok(())
    }
}
