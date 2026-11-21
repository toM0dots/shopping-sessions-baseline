//! Bounded batches for releasing already-read, file-backed input pages.
//!
//! Every queue item must call `complete` exactly once, including empty items
//! and items read with pread. Each incomplete batch contains at least one
//! in-flight item, so at most worker_count incomplete batches can retain data.
//! At batch size K, the extra completed-but-retained input is bounded by
//! worker_count * (K - 1) * maximum_chunk_bytes (448 MiB for eight workers,
//! K = 8, and the default 8 MiB maximum chunk). Nothing here releases parsed
//! records or changes the input mappings; the caller supplies MADV_DONTNEED.
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Batches {
    chunks: usize,
    size: usize,
    completed: Vec<AtomicUsize>,
}

impl Batches {
    pub fn new(chunks: usize, size: usize) -> Self {
        assert!(size > 0);
        Self {
            chunks,
            size,
            completed: (0..chunks.div_ceil(size)).map(|_| AtomicUsize::new(0)).collect(),
        }
    }

    /// Only the last finisher receives the fully completed batch's range.
    pub fn complete(&self, index: usize) -> Option<Range<usize>> {
        assert!(index < self.chunks);
        let batch = index / self.size;
        let start = batch * self.size;
        let end = start.saturating_add(self.size).min(self.chunks);
        let completed = self.completed[batch].fetch_add(1, Ordering::AcqRel) + 1;
        debug_assert!(completed <= end - start, "queue item completed twice");
        (completed == end - start).then_some(start..end)
    }
}

#[derive(Clone, Copy)]
pub struct InputRange {
    pub file: u32,
    pub start: usize,
    pub end: usize,
}

/// Merge only adjacent ranges in the same file, then emit whole input pages.
/// The callback receives (offset from input mapping base, length in bytes).
pub fn whole_pages(ranges: impl IntoIterator<Item = InputRange>, mut release: impl FnMut(usize, usize)) {
    const PAGE: usize = 4096; // Linux amd64, matching the input mapper.
    let mut emit = |range: InputRange| {
        let Some(start) = range.start.checked_add(PAGE - 1) else { return };
        let start = start & !(PAGE - 1);
        let end = range.end & !(PAGE - 1);
        if end > start {
            release(start, end - start);
        }
    };
    let mut pending: Option<InputRange> = None;
    for range in ranges {
        if range.start >= range.end {
            continue;
        }
        if let Some(previous) = pending.as_mut() {
            if previous.file == range.file && previous.end == range.start {
                previous.end = range.end;
                continue;
            }
            emit(*previous);
        }
        pending = Some(range);
    }
    if let Some(range) = pending {
        emit(range);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn incomplete_earlier_batch_does_not_block_completed_later_batch() {
        let batches = Batches::new(10, 4);
        assert_eq!(batches.complete(2), None);
        assert_eq!(batches.complete(7), None);
        assert_eq!(batches.complete(4), None);
        assert_eq!(batches.complete(6), None);
        assert_eq!(batches.complete(5), Some(4..8));
        assert_eq!(batches.complete(9), None);
        assert_eq!(batches.complete(8), Some(8..10));
        assert_eq!(batches.complete(0), None);
        assert_eq!(batches.complete(3), None);
        assert_eq!(batches.complete(1), Some(0..4));
    }

    #[test]
    fn single_chunk_batches_and_empty_queue() {
        let empty = Batches::new(0, 8);
        assert!(empty.completed.is_empty());
        let batches = Batches::new(3, 1);
        assert_eq!(batches.complete(2), Some(2..3));
        assert_eq!(batches.complete(0), Some(0..1));
        assert_eq!(batches.complete(1), Some(1..2));
    }

    #[test]
    fn concurrent_last_finisher_observes_all_completions_and_releases_once() {
        use std::sync::atomic::AtomicBool;
        const CHUNKS: usize = 1027;
        const SIZE: usize = 8;
        const WORKERS: usize = 16;
        let batches = Arc::new(Batches::new(CHUNKS, SIZE));
        let done = Arc::new((0..CHUNKS).map(|_| AtomicBool::new(false)).collect::<Vec<_>>());
        let released = Arc::new((0..CHUNKS.div_ceil(SIZE)).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let barrier = Arc::new(Barrier::new(WORKERS));
        std::thread::scope(|scope| {
            for tid in 0..WORKERS {
                let (batches, done, released, barrier) =
                    (batches.clone(), done.clone(), released.clone(), barrier.clone());
                scope.spawn(move || {
                    barrier.wait();
                    for index in (tid..CHUNKS).step_by(WORKERS).rev() {
                        done[index].store(true, Ordering::Relaxed);
                        if let Some(range) = batches.complete(index) {
                            assert!(range.clone().all(|i| done[i].load(Ordering::Relaxed)));
                            released[range.start / SIZE].fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert!(released.iter().all(|n| n.load(Ordering::Relaxed) == 1));
    }

    #[test]
    fn merges_adjacent_same_file_ranges_before_page_alignment() {
        let ranges = [
            InputRange { file: 1, start: 1, end: 4097 },
            InputRange { file: 1, start: 4097, end: 8193 },
            InputRange { file: 1, start: 8193, end: 12289 },
        ];
        let mut released = Vec::new();
        whole_pages(ranges, |start, len| released.push((start, len)));
        assert_eq!(released, [(4096, 8192)]);
    }

    #[test]
    fn never_merges_files_gaps_or_overlapping_ranges() {
        let ranges = [
            InputRange { file: 1, start: 0, end: 4096 },
            InputRange { file: 2, start: 4096, end: 8192 },
            InputRange { file: 2, start: 12288, end: 16384 },
            InputRange { file: 2, start: 12288, end: 16384 },
        ];
        let mut released = Vec::new();
        whole_pages(ranges, |start, len| released.push((start, len)));
        assert_eq!(released, [(0, 4096), (4096, 4096), (12288, 4096), (12288, 4096)]);
    }

    #[test]
    fn partial_empty_and_overflowing_ranges_release_no_pages() {
        let ranges = [
            InputRange { file: 1, start: 1, end: 4097 },
            InputRange { file: 2, start: 8192, end: 8192 },
            InputRange { file: 3, start: 9000, end: 8500 },
            InputRange { file: 4, start: usize::MAX - 5, end: usize::MAX },
        ];
        whole_pages(ranges, |_, _| panic!("must not release incomplete pages"));
    }
}
