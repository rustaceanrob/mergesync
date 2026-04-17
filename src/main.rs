use kernel::{BlockTreeEntry, ChainType, ChainstateManager, ContextBuilder};
use mergesync::{log_progress, worker, OutPointList};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

const LIST_SIZE: usize = 50 * 1_000_000_000;
const TASKS: usize = 16;

fn main() {
    let mut builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"));
    builder.init();
    let bitcoin_dir = std::env::var("BITCOIN_DIR").unwrap();
    log::info!("Using directory {bitcoin_dir}");
    let data_dir = bitcoin_dir.parse::<PathBuf>().unwrap();
    let blocks_dir = data_dir.join("blocks");
    let context = ContextBuilder::new()
        .chain_type(ChainType::Mainnet)
        .build()
        .unwrap();
    log::info!("Initializing chainstate");
    let chainman = ChainstateManager::new(
        &context,
        data_dir.to_str().unwrap(),
        blocks_dir.to_str().unwrap(),
    )
    .unwrap();
    log::info!("Importing blocks");
    let chainman = Arc::new(chainman);
    chainman.import_blocks().unwrap();
    let chain = chainman.active_chain();
    let entries: Vec<BlockTreeEntry<'static>> = unsafe {
        std::mem::transmute(
            chain
                .iter()
                .map(|entry| entry.to_owned())
                .collect::<Vec<_>>(),
        )
    };
    let jobs = entries
        .chunks(10_000)
        .map(|window| window.to_vec())
        .collect();
    let jobs = Arc::new(Mutex::new(jobs));
    log::info!("Allocating OutPoint vector");
    let mut final_list = OutPointList::new(LIST_SIZE);
    let mem_size = LIST_SIZE / TASKS;
    let mut task_handles = Vec::with_capacity(TASKS);
    for id in 0..TASKS {
        log::info!("Spawning worker {id}");
        let jobs = jobs.clone();
        let chainman = chainman.clone();
        let handle =
            std::thread::spawn(move || worker(id, jobs.clone(), chainman.clone(), mem_size));
        task_handles.push(handle);
    }
    std::thread::spawn(|| log_progress(jobs));
    for handle in task_handles {
        let outpoints = handle.join().unwrap();
        final_list.merge(outpoints);
    }
}
