use hintsfile::HintsfileBuilder;
use hintsgen::{task, OutPointMap};
use kernel::{ChainType, ChainstateManager, ContextBuilder};
use std::{fs::File, path::PathBuf, sync::Arc, time::Instant};

const TOTAL_MEMORY_BUDGET: usize = 4 * 1_000_000_000;

configure_me::include_config!();

fn main() {
    let (config, _) = Config::including_optional_config_files::<&[&str]>(&[]).unwrap_or_exit();
    let network = config.network;
    let network = match network.as_str() {
        "bitcoin" => ChainType::Mainnet,
        "signet" => ChainType::Signet,
        _ => panic!("unsupported network"),
    };
    let bitcoin_dir = config.datadir;
    let mut builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"));
    builder.init();
    log::info!("Using directory {bitcoin_dir}");
    let data_dir = bitcoin_dir.parse::<PathBuf>().unwrap();
    let blocks_dir = data_dir.join("blocks");
    let context = ContextBuilder::new().chain_type(network).build().unwrap();
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
    let active_chain = chainman.active_chain();
    let then = Instant::now();
    let path = PathBuf::from(config.hintsfile);
    log::info!("Allocating OutPoint vector");
    let mut curr = OutPointMap::new(TOTAL_MEMORY_BUDGET);
    for entry in active_chain.iter() {
        if entry.height() == 0 {
            continue;
        }
        task(&mut curr, &entry, chainman.clone());
        log::info!("block {}:{}", entry.height(), entry.block_hash());
        log::info!(
            "outpoint list size: {}mb, num txos {}k",
            curr.size() / 1_000_000,
            curr.len() / 1_000
        );
        if config.stop == entry.height() as u32 {
            break;
        }
    }
    let file = File::create(path).unwrap();
    let mut hintsfile = HintsfileBuilder::new(file).initialize(config.stop).unwrap();
    for (height, hints) in curr.into_vec() {
        log::info!("block {}: num hints: {}", height, hints.len());
        hintsfile
            .append(hintsfile::EliasFano::compress(&hints))
            .unwrap();
    }
    let now = then.elapsed();
    log::info!("Total time {}secs", now.as_secs());
}
