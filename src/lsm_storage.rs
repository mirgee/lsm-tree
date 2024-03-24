#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{Key, KeySlice};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(()).ok();
        self.compaction_notifier.send(()).ok();

        // The following should not be necessary, but just to make sure the thread exits
        let mut flush_thread = self.flush_thread.lock();
        if let Some(handle) = flush_thread.take() {
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("flush thread panicked"))?;
        }

        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(handle) = compaction_thread.take() {
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("compaction thread panicked"))?;
        }

        if !self.inner.options.enable_wal {
            // Freeze the current memtable
            if !self.inner.state.read().memtable.is_empty() {
                self.inner
                    .force_freeze_memtable(&self.inner.state_lock.lock())?;
            };
            // Flush all memtables to disk
            while !self.inner.state.read().imm_memtables.is_empty() {
                self.inner.force_flush_next_imm_memtable()?;
            }
        };
        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let mut state = LsmStorageState::create(&options);
        let mut last_sst_id = 0;
        let block_cache = Arc::new(BlockCache::new(1024));
        let mut memtables = BTreeSet::new();

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let manifest_path = path.join("MANIFEST");
        let manifest = if !manifest_path.exists() {
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?);
            }
            let manifest = Manifest::create(manifest_path)?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            manifest
        } else {
            let (manifest, records) = Manifest::recover(manifest_path)?;
            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        assert!(memtables.remove(&sst_id));
                        state.l0_sstables.insert(0, sst_id);
                        last_sst_id = last_sst_id.max(sst_id);
                    }
                    ManifestRecord::NewMemtable(memtable_id) => {
                        last_sst_id = last_sst_id.max(memtable_id);
                        memtables.insert(memtable_id);
                    }
                    ManifestRecord::Compaction(task, new_sst_ids) => {
                        // No need to strictly delete sstables, they have probably been deleted
                        let (new_state, _ssts_to_delete) = compaction_controller
                            .apply_compaction_result(&state, &task, &new_sst_ids);
                        state = new_state;
                        last_sst_id = last_sst_id.max(new_sst_ids.iter().max().copied().unwrap());
                    }
                }
            }
            manifest
        };

        let mut sst_cnt = 0;
        for sst_id in state
            .l0_sstables
            .iter()
            .chain(state.levels.iter().flat_map(|(_, sst_id)| sst_id))
        {
            let sst_path = Self::path_of_sst_static(path, *sst_id);
            let sst = SsTable::open(
                *sst_id,
                Some(block_cache.clone()),
                FileObject::open(&sst_path)?,
            )?;
            state.sstables.insert(*sst_id, Arc::new(sst));
            sst_cnt += 1;
        }

        last_sst_id = last_sst_id + 1;
        if !options.enable_wal {
            state.memtable = Arc::new(MemTable::create(last_sst_id));
        } else {
            for memtable_id in memtables.iter() {
                let memtable = MemTable::recover_from_wal(
                    *memtable_id,
                    Self::path_of_wal_static(path, *memtable_id),
                )?;
                if !memtable.is_empty() {
                    state.imm_memtables.insert(0, memtable.into())
                }
            }
            state.memtable = MemTable::create_with_wal(
                last_sst_id,
                Self::path_of_wal_static(path, last_sst_id),
            )?.into();
        }
        manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(last_sst_id + 1),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        storage.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let state_snapshot = Arc::clone(&self.state.read());

        if let Some(value) = state_snapshot.memtable.get(key) {
            if !value.is_empty() {
                return Ok(Some(value));
            } else {
                return Ok(None);
            }
        }

        for memtable in state_snapshot.imm_memtables.iter() {
            if let Some(value) = memtable.get(key) {
                if !value.is_empty() {
                    return Ok(Some(value));
                } else {
                    return Ok(None);
                }
            }
        }

        let keep_table = |key: &[u8], table: &SsTable| -> bool {
            if key_within(
                key,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                if let Some(bloom) = &table.bloom {
                    if bloom.may_contain(farmhash::fingerprint32(key)) {
                        return true;
                    }
                } else {
                    return true;
                }
            }
            return false;
        };

        for l0_idx in state_snapshot.l0_sstables.iter() {
            let table = state_snapshot.sstables.get(l0_idx).unwrap().to_owned();
            if keep_table(key, &table) {
                let iter = SsTableIterator::create_and_seek_to_key(table, Key::from_slice(key))?;
                if iter.is_valid() {
                    if iter.key().raw_ref() == key {
                        if iter.value().is_empty() {
                            return Ok(None);
                        } else {
                            return Ok(Some(Bytes::copy_from_slice(iter.value())));
                        }
                    }
                }
            }
        }

        for (_level, lx_idxs) in &state_snapshot.levels {
            for idx in lx_idxs {
                let table = state_snapshot.sstables.get(idx).unwrap().to_owned();
                if keep_table(key, &table) {
                    let iter =
                        SsTableIterator::create_and_seek_to_key(table, Key::from_slice(key))?;
                    if iter.is_valid() {
                        if iter.key().raw_ref() == key {
                            if iter.value().is_empty() {
                                return Ok(None);
                            } else {
                                return Ok(Some(Bytes::copy_from_slice(iter.value())));
                            }
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        for record in batch {
            match record {
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    assert!(!key.is_empty());
                    assert!(!value.is_empty());
                    let size;
                    {
                        let guard = self.state.read();
                        guard.memtable.put(key, value)?;
                        size = guard.memtable.approximate_size();
                    }
                    self.maybe_freeze(size)?;
                }
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    assert!(!key.is_empty());
                    let size;
                    {
                        let guard = self.state.read();
                        guard.memtable.put(key, b"")?;
                        size = guard.memtable.approximate_size();
                    };
                    self.maybe_freeze(size)?;
                }
            }
        }
        Ok(())
    }

    pub fn maybe_freeze(&self, size: usize) -> Result<()> {
        if size >= self.options.target_sst_size {
            let lock = self.state_lock.lock();
            let size = self.state.read().memtable.approximate_size();
            if size >= self.options.target_sst_size {
                self.force_freeze_memtable(&lock)?;
            }
        }
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let mut state_guard = self.state.write();
        let mut state_snapshot = state_guard.as_ref().to_owned();
        state_snapshot.memtable.sync_wal()?;
        state_snapshot
            .imm_memtables
            .insert(0, state_snapshot.memtable);
        let new_sst_id = self.next_sst_id();
        state_snapshot.memtable = if self.options.enable_wal {
            MemTable::create_with_wal(new_sst_id, self.path_of_wal(new_sst_id))?.into()
        } else {
            MemTable::create(new_sst_id).into()
        };
        *state_guard = state_snapshot.into();

        self.manifest.as_ref().unwrap().add_record(
            _state_lock_observer,
            ManifestRecord::NewMemtable(new_sst_id),
        )?;

        self.sync_dir()?;

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    /// We will worry about compaction later
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // Ensure only one thread is flushing at a time, without blocking the state unnecessarily
        let state_lock = self.state_lock.lock();

        // Obtain the read guard to the state and snapshot the imm memtable to flush
        let memtable_to_flush = {
            let guard = self.state.read();
            // Can't pop here, we still want the memtable to be readable
            guard.imm_memtables.last().unwrap().to_owned()
        };

        // Using SST builder, flush the memtable into SST
        let sst_id = memtable_to_flush.id();
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        memtable_to_flush.flush(&mut sst_builder)?;
        let sst = sst_builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;

        // Add the flushed SST to l0
        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            snapshot.imm_memtables.pop();
            snapshot.l0_sstables.insert(0, sst_id);
            // TODO: Here we are copying the SSTable to heap and potentially resizing sstables while
            // holding the lock on the entire state, can we be more granular?
            snapshot.sstables.insert(sst_id, Arc::new(sst));
            *guard = Arc::new(snapshot);
        }

        self.sync_dir()?;

        // Add manifest record
        self.manifest
            .as_ref()
            .unwrap()
            .add_record(&state_lock, ManifestRecord::Flush(sst_id))?;

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let memtable_iters = {
            let current_memtable_range = snapshot.memtable.scan(lower, upper);
            let frozen_memtables_range = snapshot
                .imm_memtables
                .iter()
                .map(|memtable| memtable.scan(lower, upper))
                .collect::<Vec<_>>();
            let mut all_memtables = vec![Box::new(current_memtable_range)];
            for frozen_memtable_range in frozen_memtables_range.into_iter() {
                all_memtables.push(Box::new(frozen_memtable_range));
            }
            MergeIterator::create(all_memtables)
        };

        let l0_iters = {
            let mut l0_iters = vec![];
            for l0_idx in &snapshot.l0_sstables {
                let table = snapshot.sstables.get(l0_idx).unwrap().to_owned();
                if range_overlap(
                    lower,
                    upper,
                    table.first_key().as_key_slice(),
                    table.last_key().as_key_slice(),
                ) {
                    match lower {
                        Bound::Included(key) => {
                            l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                                table,
                                Key::from_slice(key),
                            )?));
                        }
                        Bound::Excluded(key) => {
                            let mut iter = SsTableIterator::create_and_seek_to_key(
                                table,
                                Key::from_slice(key),
                            )?;
                            if iter.is_valid() {
                                iter.next()?;
                            }
                            l0_iters.push(Box::new(iter));
                        }
                        Bound::Unbounded => {
                            l0_iters
                                .push(Box::new(SsTableIterator::create_and_seek_to_first(table)?));
                        }
                    }
                }
            }
            MergeIterator::create(l0_iters)
        };

        let lx_iters = {
            let mut lx_iters = vec![];
            for (_, lx_idxs) in &snapshot.levels {
                let mut lx_tables = vec![];
                for idx in lx_idxs {
                    let table = snapshot.sstables.get(&idx).unwrap();
                    if range_overlap(
                        lower,
                        upper,
                        table.first_key().as_key_slice(),
                        table.last_key().as_key_slice(),
                    ) {
                        lx_tables.push(table.to_owned());
                    }
                }

                let level_iter = match lower {
                    Bound::Included(key) => {
                        SstConcatIterator::create_and_seek_to_key(lx_tables, Key::from_slice(key))?
                    }
                    Bound::Excluded(key) => {
                        let mut iter = SstConcatIterator::create_and_seek_to_key(
                            lx_tables,
                            Key::from_slice(key),
                        )?;
                        if iter.is_valid() {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(lx_tables)?,
                };

                lx_iters.push(Box::new(level_iter));
            }
            lx_iters
        };

        let two_merge_iterator =
            TwoMergeIterator::create(l0_iters, MergeIterator::create(lx_iters))?;
        let two_merge_iterator = TwoMergeIterator::create(memtable_iters, two_merge_iterator)?;

        Ok(FusedIterator::new(LsmIterator::new(
            two_merge_iterator,
            map_bound(upper),
        )?))
    }
}

fn range_overlap(
    user_begin: Bound<&[u8]>,
    user_end: Bound<&[u8]>,
    table_begin: KeySlice,
    table_end: KeySlice,
) -> bool {
    match user_end {
        Bound::Excluded(key) if key <= table_begin.raw_ref() => {
            return false;
        }
        Bound::Included(key) if key < table_begin.raw_ref() => {
            return false;
        }
        _ => {}
    }
    match user_begin {
        Bound::Excluded(key) if key >= table_end.raw_ref() => {
            return false;
        }
        Bound::Included(key) if key > table_end.raw_ref() => {
            return false;
        }
        _ => {}
    }
    true
}

fn key_within(key: &[u8], table_begin: KeySlice, table_end: KeySlice) -> bool {
    key >= table_begin.raw_ref() && key <= table_end.raw_ref()
}
