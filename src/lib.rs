use kernel::{
    BlockTreeEntry, ChainstateManager,
    core::{TransactionExt, TxInExt, TxOutPointExt, TxidExt},
};
use rayon::slice::ParallelSliceMut;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct OutPoint {
    txid: [u8; 32],
    index: u32,
}

impl OutPoint {
    pub const fn new(txid: [u8; 32], index: u32) -> Self {
        Self { txid, index }
    }
}

impl PartialEq for OutPoint {
    fn eq(&self, other: &Self) -> bool {
        self.txid == other.txid && self.index == other.index
    }
}

impl Eq for OutPoint {}

impl std::cmp::PartialOrd for OutPoint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for OutPoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.txid
            .cmp(&other.txid)
            .then(self.index.cmp(&other.index))
    }
}

#[derive(Debug, Clone)]
pub struct OutPointTaggedBlockPos {
    outpoint: OutPoint,
    block_pos: Option<(u32, u16)>,
}

impl OutPointTaggedBlockPos {
    pub fn new_input(outpoint: OutPoint) -> Self {
        Self {
            outpoint,
            block_pos: None,
        }
    }

    pub fn new_output(outpoint: OutPoint, block: u32, index: u16) -> Self {
        Self {
            outpoint,
            block_pos: Some((block, index)),
        }
    }
}

#[derive(Debug)]
pub struct OutPointList {
    list: Vec<OutPointTaggedBlockPos>,
}

impl OutPointList {
    /// Outpoint + height + position
    const MEMBER_SIZE: usize = 42;

    pub fn new(bytes: usize) -> Self {
        let size = bytes / Self::MEMBER_SIZE;
        Self {
            list: Vec::with_capacity(size),
        }
    }

    pub fn size(&self) -> usize {
        self.list.len() * Self::MEMBER_SIZE
    }

    pub fn add(&mut self, more: &[OutPointTaggedBlockPos]) {
        self.list.extend_from_slice(more);
    }

    pub fn sort_and_dedup(&mut self) {
        let time = std::time::Instant::now();
        let prev_count = self.list.len();
        self.list.par_sort_unstable_by(|a, b| {
            a.outpoint
                .cmp(&b.outpoint)
                .then_with(|| match (a.block_pos, b.block_pos) {
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    _ => std::cmp::Ordering::Equal,
                })
        });
        let mut prev = None;
        self.list.retain(|item| {
            if prev == Some(item.outpoint.clone()) {
                false
            } else {
                prev = Some(item.outpoint.clone());
                true
            }
        });
        log::info!(
            "Sorted {} OutPoints in {} seconds",
            prev_count,
            time.elapsed().as_secs()
        );
        log::info!("Total OutPoints remaining {}", self.list.len());
    }

    pub fn merge(&mut self, other: Self) {
        self.list.extend(other.list);
        self.sort_and_dedup();
    }
}

fn task(
    entries: &[BlockTreeEntry],
    chainman: Arc<ChainstateManager>,
    mem_size: usize,
) -> OutPointList {
    let mut outpoint_list = OutPointList::new(mem_size);
    for entry in entries {
        let block = chainman.read_block_data(entry).unwrap();
        let height = entry.height() as u32;
        let mut outpoints = Vec::new();
        let mut curr = 0;
        for (index, transaction) in block.transactions().enumerate() {
            if index != 0 {
                for input in transaction.inputs() {
                    let txid = input.outpoint().txid().to_bytes();
                    let index = input.outpoint().index();
                    let outpoint = OutPointTaggedBlockPos::new_input(OutPoint::new(txid, index));
                    outpoints.push(outpoint);
                }
            }
            let txid = transaction.txid().to_bytes();
            for (index, _) in transaction.outputs().enumerate() {
                let outpoint = OutPointTaggedBlockPos::new_output(
                    OutPoint::new(txid, index as u32),
                    height,
                    curr,
                );
                outpoints.push(outpoint);
                curr += 1;
            }
        }
        outpoint_list.add(&outpoints);
        outpoints.clear();
        if outpoint_list.size() > mem_size - 10_000 {
            outpoint_list.sort_and_dedup();
        }
    }
    outpoint_list
}

pub fn worker(
    id: usize,
    jobs: Arc<Mutex<Vec<Vec<BlockTreeEntry>>>>,
    chainman: Arc<ChainstateManager>,
    mem_size: usize,
) -> OutPointList {
    let mut outpoints = OutPointList::new(mem_size);
    loop {
        let job = {
            let mut job_list = jobs.lock().unwrap();
            job_list.pop()
        };
        match job {
            Some(job) => {
                log::info!("Worker {id} finished a job");
                let outpoint_list = task(&job, chainman.clone(), mem_size);
                outpoints.merge(outpoint_list);
            }
            None => return outpoints,
        }
    }
}

pub fn log_progress(jobs: Arc<Mutex<Vec<Vec<BlockTreeEntry>>>>) {
    loop {
        std::thread::sleep(Duration::from_secs(10));
        let (num_jobs, num_blocks) = {
            let jobs = jobs.lock().unwrap();
            (jobs.len(), jobs.iter().map(|v| v.len()).sum::<usize>())
        };
        log::info!("Total remaining jobs: {num_jobs}; number of blocks {num_blocks}");
    }
}
