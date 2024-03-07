#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod
mod builder;
mod iterator;

use crate::constants::SIZEOF_U16;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut data = self.data.clone();
        for offset in self.offsets.iter() {
            data.put_u16(*offset);
        }
        data.put_u16(self.offsets.len() as u16);
        data.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_offsets_start = data.len() - SIZEOF_U16;
        let num_offsets = data[num_offsets_start..].as_ref().get_u16() as usize;
        let offsets_start = num_offsets_start - num_offsets * SIZEOF_U16;
        let offsets = data[offsets_start..num_offsets_start]
            .as_ref()
            .chunks(SIZEOF_U16)
            .map(|mut c| c.get_u16())
            .collect();
        Self {
            data: data[0..offsets_start].to_vec(),
            offsets,
        }
    }
}
