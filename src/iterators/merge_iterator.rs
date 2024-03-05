#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iters = BinaryHeap::from_iter(
            iters
                .into_iter()
                .filter(|it| it.is_valid())
                .enumerate()
                .map(|(i, it)| HeapWrapper(i, it)),
        );
        let current = iters.pop();
        Self { iters, current }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current
            .as_ref()
            .map(|current| KeySlice::from_slice(current.1.key().raw_ref()))
            .unwrap_or_default()
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .map(|current| current.1.value())
            .unwrap_or_default()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        let current_key = self.key().to_key_vec();

        // Remove values with the same key from all iterators
        while let Some(mut top_heap_iter) = self.iters.peek_mut() {
            if top_heap_iter.1.key().to_key_vec() == current_key {
                if let Err(error) = top_heap_iter.1.next() {
                    // We must drop the iterator on error, as next may have modified it
                    // and PeekMut destructor will call partial_cmp to make sure the heap
                    // invariant is not violated.
                    PeekMut::<'_, HeapWrapper<I>>::pop(top_heap_iter);
                    return Err(error);
                }
                if !top_heap_iter.1.is_valid() {
                    PeekMut::<'_, HeapWrapper<I>>::pop(top_heap_iter);
                }
            } else {
                break;
            }
        }

        // Must not call next if is_valid is false
        let mut current_iter = self.current.take().unwrap();

        current_iter.1.next()?;

        if !current_iter.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                self.current = Some(iter);
            }
            return Ok(());
        }

        // Push the current iterator back to the heap
        self.iters.push(current_iter);
        self.current = self.iters.pop();

        Ok(())
    }
}
