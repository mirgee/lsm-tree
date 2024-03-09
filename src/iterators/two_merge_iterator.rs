#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    use_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    fn use_a(a: &A, b: &B) -> bool {
        if !a.is_valid() {
            return false;
        }
        if !b.is_valid() {
            return true;
        }
        a.key() <= b.key()
    }

    // Similar to MergeIterator, if the same key is found in both iterators, a takes the precedence. So, we only ever need to skip b.
    fn skip_b(a: &A, b: &mut B) -> Result<()> {
        // Accessing the key may give incrorrect result if the iterator is invalid
        if a.is_valid() && b.is_valid() && a.key() == b.key() {
            b.next()?;
        };
        Ok(())
    }

    pub fn create(a: A, b: B) -> Result<Self> {
        let mut new = Self {
            use_a: false,
            a,
            b,
        };
        Self::skip_b(&new.a, &mut new.b)?;
        new.use_a = Self::use_a(&new.a, &new.b);
        Ok(new)
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.use_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.use_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.use_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.use_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        Self::skip_b(&self.a, &mut self.b)?;
        self.use_a = Self::use_a(&self.a, &self.b);
        Ok(())
    }
}
