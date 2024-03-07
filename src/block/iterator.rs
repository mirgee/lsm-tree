#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use bytes::Buf;

use crate::{
    constants::SIZEOF_U16,
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut new = Self::new(block);
        new.seek_to_first();
        new
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut new = Self::new(block);
        new.seek_to_key(key);
        new
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        self.block
            .data
            .get(self.value_range.0..self.value_range.1)
            .unwrap()
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0);
    }

    /// Modifies idx, self.key and self.value_range
    fn seek_to_idx(&mut self, idx: usize) {
        // As per the book: "If we reach the end of the block, we can set key to empty and return false from is_valid,
        // so that the caller can switch to another block if possible."
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }
        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset_bytes(offset);
        self.idx = idx;
    }

    /// Assumes that there is a valid entry after offset_bytes
    /// Modifies self.key and self.value_range
    fn seek_to_offset_bytes(&mut self, offset_bytes: usize) {
        let mut current_entry = self.block.data[offset_bytes..].as_ref();

        let key_len = current_entry.get_u16() as usize;
        let key = current_entry[..key_len].as_ref();
        current_entry.advance(key_len);

        let value_len = current_entry.get_u16() as usize;
        let value = current_entry[..value_len].as_ref();
        current_entry.advance(value_len);

        self.key.clear();
        self.key.append(key);

        let value_range_begin = offset_bytes + SIZEOF_U16 + key_len + SIZEOF_U16;
        let value_range_end = value_range_begin + value_len;
        self.value_range = (value_range_begin, value_range_end);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx = self.idx + 1;
        self.seek_to_idx(self.idx);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut left = 0;
        let mut right = self.block.offsets.len();
        while left < right {
            let middle = left + (right - left) / 2;
            self.seek_to_idx(middle);
            if self.key() > key {
                right = middle;
            } else if self.key() < key {
                left = middle + 1;
            } else {
                return;
            }
        }
        self.seek_to_idx(left);
    }
}
