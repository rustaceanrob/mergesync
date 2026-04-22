use bitcoin_hashes::sha256;
use kernel::{
    BlockTreeEntry, ChainstateManager, TxOutRef,
    core::{ScriptPubkeyExt, TransactionExt, TxInExt, TxOutExt, TxOutPointExt, TxidExt},
};
use rayon::prelude::*;
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Clone)]
pub struct OutPoint {
    txid: [u8; 32],
    index: u32,
}

impl OutPoint {
    pub const fn new(txid: [u8; 32], index: u32) -> Self {
        Self { txid, index }
    }

    #[inline]
    pub fn tag(self) -> [u8; 16] {
        let mut preimage = [0u8; 36];
        preimage[0..32].copy_from_slice(&self.txid);
        preimage[32..].copy_from_slice(&self.index.to_le_bytes());
        let hash = sha256::hash(&preimage).to_byte_array();
        hash[..16].try_into().unwrap()
    }
}

#[derive(Debug, Clone)]
pub struct OutPointTaggedBlockPos {
    fingerprint: [u8; 16],
    block_pos: (u32, u32),
}

impl OutPointTaggedBlockPos {
    const SIZE: usize = 16 + 4 + 4;

    pub fn new_output(outpoint: OutPoint, block: u32, index: u32) -> Self {
        Self {
            fingerprint: outpoint.tag(),
            block_pos: (block, index),
        }
    }
}

#[derive(Debug)]
pub struct OutPointMap {
    map: HashMap<[u8; 16], (u32, u32)>,
}

impl OutPointMap {
    pub fn new(bytes: usize) -> Self {
        let size = bytes / OutPointTaggedBlockPos::SIZE;
        Self {
            map: HashMap::with_capacity(size),
        }
    }

    pub fn size(&self) -> usize {
        self.map.len() * OutPointTaggedBlockPos::SIZE
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn add(&mut self, el: OutPointTaggedBlockPos) {
        self.map.insert(el.fingerprint, el.block_pos);
    }

    pub fn remove(&mut self, input: OutPoint) {
        self.map
            .remove(&input.tag())
            .expect("removed outpoint before adding");
    }

    pub fn into_vec(self) -> Vec<(u32, Vec<u32>)> {
        group_by_u32(self.map)
    }
}

pub fn task(
    outpoint_list: &mut OutPointMap,
    entry: &BlockTreeEntry,
    chainman: Arc<ChainstateManager>,
) {
    let block = chainman.read_block_data(entry).unwrap();
    let height = entry.height() as u32;
    let mut curr = 0;
    for (index, transaction) in block.transactions().enumerate() {
        let txid = transaction.txid().to_bytes();
        for (vout, txout) in transaction.outputs().enumerate() {
            if txout.is_unspendable() || index == 0 && entry.is_bip30_unspendable() {
                continue;
            }
            let outpoint =
                OutPointTaggedBlockPos::new_output(OutPoint::new(txid, vout as u32), height, curr);
            outpoint_list.add(outpoint);
            curr += 1;
        }
        if index != 0 {
            for input in transaction.inputs() {
                let txid = input.outpoint().txid().to_bytes();
                let index = input.outpoint().index();
                let outpoint = OutPoint::new(txid, index);
                outpoint_list.remove(outpoint);
            }
        }
    }
}

trait Bip30UnspendableExt {
    fn is_bip30_unspendable(&self) -> bool;
}

impl Bip30UnspendableExt for BlockTreeEntry<'_> {
    fn is_bip30_unspendable(&self) -> bool {
        self.height() == 91722
            && "00000000000271a2dc26e7667f8419f2e15416dc6955e5a6c6cdf3f2574dd08e"
                .eq(&self.block_hash().to_string())
            || self.height() == 91812
                && "00000000000af0aed4792b1acee3d966af36cf5def14935db8de83d6f9306f2f"
                    .eq(&self.block_hash().to_string())
    }
}

trait IsUnspendable {
    fn is_unspendable(&self) -> bool;
}

impl IsUnspendable for TxOutRef<'_> {
    fn is_unspendable(&self) -> bool {
        let spk = self.script_pubkey().to_bytes();
        !spk.is_empty() && spk[0] == 0x6a || spk.len() > 10_000
    }
}

fn group_by_u32(input: HashMap<[u8; 16], (u32, u32)>) -> Vec<(u32, Vec<u32>)> {
    // Step 1: Parallel fold into per-thread HashMaps, then reduce them together.
    let grouped: HashMap<u32, Vec<u32>> = input
        .par_iter()
        .fold(
            HashMap::new,
            |mut acc: HashMap<u32, Vec<u32>>, (_key, &(group, val))| {
                acc.entry(group).or_default().push(val);
                acc
            },
        )
        .reduce_with(|mut m1, m2| {
            for (k, mut v) in m2 {
                m1.entry(k).or_default().append(&mut v);
            }
            m1
        })
        .unwrap_or_default();

    // Step 2: Find min/max keys in parallel to build the contiguous range.
    let max_key = grouped
        .par_iter()
        .map(|(&k, _)| k)
        .reduce_with(|a, b| a.max(b))
        .unwrap();

    // Step 3: Build a contiguous Vec over min..=max, filling gaps with empty Vecs.
    let mut result: Vec<(u32, Vec<u32>)> = (1..=max_key)
        .into_par_iter()
        .map(|k| {
            let mut v = grouped.get(&k).cloned().unwrap_or_default();
            v.par_sort_unstable();
            (k, v)
        })
        .collect();

    // Step 4: The parallel iterator over a range preserves order on collect,
    // but we sort explicitly to be safe and correct.
    result.par_sort_unstable_by_key(|(k, _)| *k);
    let mut last = 0;
    for (height, hints) in &result {
        assert!(*height == last + 1);
        last = *height;
        assert!(hints.is_sorted());
    }
    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    #[test]
    fn group_by() {
        let mut map: HashMap<[u8; 16], (u32, u32)> = HashMap::new();
        map.insert([0u8; 16], (3, 100));
        map.insert([1u8; 16], (1, 50));
        map.insert([2u8; 16], (1, 20));
        map.insert([3u8; 16], (3, 10));
        map.insert([4u8; 16], (5, 75));

        let result = super::group_by_u32(map);
        for (group, vals) in &result {
            println!("group {group}: {vals:?}");
        }
    }
}
