#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn sstables_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut res = vec![];
        let mut builder: Option<SsTableBuilder> = None;

        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            let builder_inner = builder.as_mut().unwrap();
            // No need to repeat tombstones in compacted SSTs
            if !iter.value().is_empty() {
                builder_inner.add(iter.key(), iter.value());
            }
            iter.next()?;

            if builder_inner.estimated_size() >= self.options.target_sst_size {
                let id = self.next_sst_id();
                let builder = builder.take().unwrap();
                res.push(Arc::new(builder.build(
                    id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(id),
                )?));
            }
        }

        if let Some(builder) = builder {
            let id = self.next_sst_id();
            res.push(Arc::new(builder.build(
                id,
                Some(self.block_cache.clone()),
                self.path_of_sst(id),
            )?));
        }

        Ok(res)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::Leveled(_) => todo!(),
            CompactionTask::Tiered(_) => todo!(),
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            }) => {
                match upper_level {
                    None => {
                        let state_snapshot = {
                            let state = self.state.read();
                            state.clone()
                        };
                        let mut lower_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                        for id in lower_level_sst_ids {
                            lower_ssts.push(state_snapshot.sstables.get(id).unwrap().to_owned());
                        }
                        let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                        let mut upper_ssts = Vec::with_capacity(upper_level_sst_ids.len());
                        for id in upper_level_sst_ids {
                            upper_ssts.push(Box::new(SsTableIterator::create_and_seek_to_first(
                                state_snapshot.sstables.get(id).unwrap().to_owned(),
                            )?));
                        }
                        // Upper level is 0, we can't use SstConcatIterator
                        let upper_iter = MergeIterator::create(upper_ssts);
                        self.sstables_from_iter(TwoMergeIterator::create(upper_iter, lower_iter)?)
                    }
                    Some(_upper_level) => {
                        let state_snapshot = {
                            let state = self.state.read();
                            state.clone()
                        };
                        let mut upper_ssts = Vec::with_capacity(upper_level_sst_ids.len());
                        for id in upper_level_sst_ids {
                            upper_ssts.push(state_snapshot.sstables.get(id).unwrap().to_owned());
                        }
                        let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                        let mut lower_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                        for id in lower_level_sst_ids {
                            lower_ssts.push(state_snapshot.sstables.get(id).unwrap().to_owned());
                        }
                        let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                        let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                        self.sstables_from_iter(iter)
                    }
                }
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let state_snapshot = {
                    let state = self.state.read();
                    state.clone()
                };
                let mut l0_iters = Vec::with_capacity(l0_sstables.len());
                for id in l0_sstables {
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        state_snapshot.sstables.get(id).unwrap().to_owned(),
                    )?));
                }
                let mut l1_tables = Vec::with_capacity(l1_sstables.len());
                for id in l1_sstables {
                    l1_tables.push(state_snapshot.sstables.get(id).unwrap().to_owned());
                }
                let iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_iters),
                    SstConcatIterator::create_and_seek_to_first(l1_tables)?,
                )?;
                self.sstables_from_iter(iter)
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let state_snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let l0_sstables = state_snapshot.l0_sstables.clone();
        let l1_sstables = state_snapshot.levels[0].1.clone();

        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };

        let new_ssts = self.compact(&compaction_task)?;

        {
            let state_lock = self.state_lock.lock();
            let mut state_snapshot = self.state.read().as_ref().clone();

            // No SSTables should have added or remobed from L1
            assert_eq!(l1_sstables, state_snapshot.levels[0].1);

            // Remove the compacted SSTables
            for old_sst in l0_sstables.iter().chain(l1_sstables.iter()) {
                state_snapshot.sstables.remove(old_sst);
                std::fs::remove_file(self.path_of_sst(*old_sst))?;
            }

            // Insert new SSTables while colelcting their IDs
            let mut new_sst_ids = Vec::new();
            for new_sst in &new_ssts {
                state_snapshot
                    .sstables
                    .insert(new_sst.sst_id(), new_sst.clone());
                new_sst_ids.push(new_sst.sst_id());
            }

            // Update L0 and L1 indeces; newly created moved to L1
            state_snapshot.levels[0].1 = new_sst_ids.clone();

            // In L0, remove all except those which are not in l0_sstables
            // those were added after the compaction was triggered
            state_snapshot.l0_sstables = state_snapshot
                .l0_sstables
                .into_iter()
                .filter(|x| !l0_sstables.contains(x))
                .collect();

            *self.state.write() = Arc::new(state_snapshot);

            self.manifest.as_ref().unwrap().add_record(
                &state_lock,
                ManifestRecord::Compaction(compaction_task, new_sst_ids.clone()),
            )?;

            self.sync_dir()?;
        };
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let state_snapshot = {
            let state = self.state.read();
            state.clone()
        };

        let task = self
            .compaction_controller
            .generate_compaction_task(&state_snapshot);
        let Some(task) = task else {
            return Ok(());
        };
        // self.dump_structure();
        // println!("running compaction task: {:?}", task);

        let new_ssts = self.compact(&task)?;

        {
            let state_lock = self.state_lock.lock();
            let mut state_snapshot = self.state.read().as_ref().clone();
            let mut sst_ids = Vec::new();

            for new_sst in new_ssts {
                sst_ids.push(new_sst.sst_id());
                assert!(state_snapshot
                    .sstables
                    .insert(new_sst.sst_id(), new_sst)
                    .is_none());
            }

            let (mut snapshot, ssts_to_delete) = self
                .compaction_controller
                .apply_compaction_result(&state_snapshot, &task, &sst_ids);

            for sst_to_delete in &ssts_to_delete {
                let removed_sst = snapshot.sstables.remove(sst_to_delete).unwrap();
                std::fs::remove_file(self.path_of_sst(removed_sst.sst_id()))?;
            }

            *self.state.write() = Arc::new(snapshot);

            self.manifest.as_ref().unwrap().add_record(
                &state_lock,
                ManifestRecord::Compaction(task, sst_ids.clone()),
            )?;

            self.sync_dir()?;

            println!(
                "compaction finished: {} files removed, {} files added, output={:?}",
                ssts_to_delete.len(),
                sst_ids.len(),
                sst_ids
            );
        };
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let num_memtables = {
            // We should drop here to avoid deadlock as force_flush_next_imm_memtable needs the
            // write lock
            let state = self.state.read();
            state.imm_memtables.len()
        };
        if num_memtables >= self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
