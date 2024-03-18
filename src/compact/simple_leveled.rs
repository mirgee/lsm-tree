use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    // Excludes L0
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        // Implement just the L0 trigger for now
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: false,
            });
        } else {
            let mut level_sizes = vec![];
            level_sizes.push(snapshot.l0_sstables.len());
            for (_, ssts) in &snapshot.levels {
                level_sizes.push(ssts.len());
            }
            // TODO: What levels should be compacted in case only max_levels triggers?
            assert!(level_sizes.len() - 1 <= self.options.max_levels);
            for upper_level in 0..self.options.max_levels {
                let upper_level_size = if upper_level == 0 {
                    snapshot.l0_sstables.len()
                } else {
                    snapshot.levels[upper_level - 1].1.len()
                };
                let lower_level = upper_level + 1;
                let lower_level_size = snapshot.levels[lower_level - 1].1.len();
                let size_ratio_percent = lower_level_size as f64 / upper_level_size as f64 * 100.0;
                if size_ratio_percent < self.options.size_ratio_percent as f64 {
                    return Some(SimpleLeveledCompactionTask {
                        upper_level: if upper_level == 0 {
                            None
                        } else {
                            Some(upper_level)
                        },
                        upper_level_sst_ids: if upper_level == 0 {
                            snapshot.l0_sstables.clone()
                        } else {
                            snapshot.levels[upper_level - 1].1.clone()
                        },
                        lower_level,
                        lower_level_sst_ids: snapshot.levels[lower_level - 1].1.clone(),
                        is_lower_level_bottom_level: lower_level == self.options.max_levels,
                    });
                }
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState, // TODO: While not take by value?
        task: &SimpleLeveledCompactionTask,
        output: &[usize], // TODO: Why not take vec
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut to_remove: Vec<usize> = vec![];
        if let Some(upper_level) = task.upper_level {
            assert_eq!(task.upper_level_sst_ids, snapshot.levels[upper_level - 1].1);
            to_remove.extend(&task.upper_level_sst_ids);
            snapshot.levels[upper_level - 1].1.clear();
        } else {
            to_remove.extend(&task.upper_level_sst_ids);
            let mut l0_ssts_to_be_compacted = task
                .upper_level_sst_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>();
            let l0_ssts_newly_added = snapshot
                .l0_sstables
                .iter()
                .copied()
                .filter(|sst| !l0_ssts_to_be_compacted.remove(sst))
                .collect();
            snapshot.l0_sstables = l0_ssts_newly_added;
        }
        assert_eq!(
            task.lower_level_sst_ids,
            snapshot.levels[task.lower_level - 1].1
        );
        to_remove.extend(&task.lower_level_sst_ids);
        snapshot.levels[task.lower_level - 1].1 = output.to_vec();
        (snapshot, to_remove)
    }
}
