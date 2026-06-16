use std::collections::hash_map::Entry;

use itertools::Itertools;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Deserialize;
use substreams_ethereum::pb::eth::v2 as eth;
use tiny_keccak::{Hasher, Keccak};
use tycho_substreams::pb::tycho::evm::v1::{
    AccountBalanceChange, Block, BlockChanges, ChangeType, ContractChange, Transaction,
    TransactionChanges,
};

use hex_literal::hex;

const TRANSFER_TOPIC: [u8; 32] =
    hex!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
const BLOOM_BYTES: usize = 256;
const BLOOM_BITS: usize = 2048;
const DEFAULT_MAX_MAPPING_SLOT: u16 = 64;
const MAX_MAPPING_SLOT_CAP: u16 = 1024;

#[derive(Debug, Deserialize)]
pub struct Params {
    #[serde(alias = "holder_address")]
    pub address: [u8; 20],
    #[serde(default = "default_max_mapping_slot")]
    pub max_mapping_slot: u16,
}

impl Params {
    pub fn parse(raw: &str) -> Self {
        let mut params: RawParams =
            serde_qs::from_str(raw).expect("Unable to deserialize balance params");
        params.max_mapping_slot = params
            .max_mapping_slot
            .min(MAX_MAPPING_SLOT_CAP);
        Params {
            address: decode_address(&params.address),
            max_mapping_slot: params.max_mapping_slot,
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawParams {
    #[serde(alias = "holder_address")]
    address: String,
    #[serde(default = "default_max_mapping_slot")]
    max_mapping_slot: u16,
}

fn default_max_mapping_slot() -> u16 {
    DEFAULT_MAX_MAPPING_SLOT
}

#[derive(Clone)]
pub struct BalanceUpdate {
    tx: Transaction,
    token: [u8; 20],
    balance: Vec<u8>,
    tx_index: u64,
    pub first_storage_ordinal: u64,
}

#[substreams::handlers::map]
pub fn map_balance_changes(
    params: String,
    block: eth::Block,
) -> Result<BlockChanges, substreams::errors::Error> {
    let params = Params::parse(&params);
    let updates = balance_updates(&block, &params);

    Ok(balance_changes_from_updates(&block, &params.address, updates))
}

pub fn balance_updates(block: &eth::Block, params: &Params) -> Vec<BalanceUpdate> {
    if !block_bloom_may_contain_transfer_for_holder(block, &params.address) {
        return Vec::new();
    }

    let balance_keys = balance_mapping_keys(&params.address, params.max_mapping_slot);
    let mut updates = Vec::new();

    for tx in block
        .transaction_traces
        .iter()
        .filter(|tx| tx.status == i32::from(eth::TransactionTraceStatus::Succeeded))
    {
        let candidate_tokens = candidate_transfer_tokens(tx, &params.address);
        if candidate_tokens.is_empty() {
            continue;
        }

        updates.extend(storage_balance_updates(tx, &candidate_tokens, &balance_keys));
    }

    updates.sort_unstable_by_key(|update| (update.tx_index, update.first_storage_ordinal));
    updates
}

fn balance_changes_from_updates(
    block: &eth::Block,
    holder: &[u8; 20],
    updates: Vec<BalanceUpdate>,
) -> BlockChanges {
    let mut transaction_changes: FxHashMap<u64, (Transaction, Vec<AccountBalanceChange>)> =
        FxHashMap::default();

    for update in updates {
        let (_, token_balances) = transaction_changes
            .entry(update.tx_index)
            .or_insert_with(|| (update.tx.clone(), Vec::new()));
        token_balances.push(AccountBalanceChange {
            token: update.token.to_vec(),
            balance: normalized_balance(&update.balance),
        });
    }

    BlockChanges {
        block: Some(block_from_eth(block)),
        changes: transaction_changes
            .drain()
            .sorted_unstable_by_key(|(index, _)| *index)
            .map(|(_, (tx, token_balances))| TransactionChanges {
                tx: Some(tx),
                contract_changes: vec![ContractChange {
                    address: holder.to_vec(),
                    balance: Vec::new(),
                    code: Vec::new(),
                    slots: Vec::new(),
                    change: ChangeType::Update.into(),
                    token_balances,
                }],
                entity_changes: Vec::new(),
                component_changes: Vec::new(),
                balance_changes: Vec::new(),
                entrypoints: Vec::new(),
                entrypoint_params: Vec::new(),
            })
            .collect(),
        storage_changes: Vec::new(),
    }
}

fn block_from_eth(block: &eth::Block) -> Block {
    Block {
        number: block.number,
        hash: block.hash.clone(),
        parent_hash: block
            .header
            .as_ref()
            .expect("Block header not present")
            .parent_hash
            .clone(),
        ts: block.timestamp_seconds(),
    }
}

fn transaction_from_eth(tx: &eth::TransactionTrace) -> Transaction {
    Transaction {
        hash: tx.hash.clone(),
        from: tx.from.clone(),
        to: tx.to.clone(),
        index: tx.index.into(),
    }
}

fn storage_balance_updates(
    tx: &eth::TransactionTrace,
    candidate_tokens: &[[u8; 20]],
    balance_keys: &FxHashSet<[u8; 32]>,
) -> Vec<BalanceUpdate> {
    let mut latest_by_token: FxHashMap<[u8; 20], BalanceUpdate> = FxHashMap::default();
    let tycho_tx = transaction_from_eth(tx);

    for call in &tx.calls {
        if call.state_reverted || call.storage_changes.is_empty() {
            continue;
        }

        for storage_change in &call.storage_changes {
            if storage_change.old_value == storage_change.new_value {
                continue;
            }
            let Some(token) = address_key(&storage_change.address) else {
                continue;
            };
            if !candidate_tokens.contains(&token) {
                continue;
            }
            let Some(slot) = slot_key(&storage_change.key) else {
                continue;
            };
            if !balance_keys.contains(&slot) {
                continue;
            }

            let update = BalanceUpdate {
                tx: tycho_tx.clone(),
                token,
                balance: storage_change.new_value.clone(),
                tx_index: tx.index.into(),
                first_storage_ordinal: storage_change.ordinal,
            };

            match latest_by_token.entry(token) {
                Entry::Occupied(mut entry) => {
                    if update.first_storage_ordinal > entry.get().first_storage_ordinal {
                        entry.insert(update);
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(update);
                }
            }
        }
    }

    latest_by_token.into_values().collect()
}

fn candidate_transfer_tokens(tx: &eth::TransactionTrace, holder: &[u8; 20]) -> Vec<[u8; 20]> {
    let Some(receipt) = tx.receipt.as_ref() else {
        return Vec::new();
    };

    let mut tokens = Vec::new();
    for log in &receipt.logs {
        if !is_watched_transfer(log, holder) {
            continue;
        }
        let Some(token) = address_key(&log.address) else {
            continue;
        };
        if !tokens.contains(&token) {
            tokens.push(token);
        }
    }

    tokens
}

fn is_watched_transfer(log: &eth::Log, holder: &[u8; 20]) -> bool {
    log.topics.len() == 3
        && log.data.len() == 32
        && log.topics[0].as_slice() == TRANSFER_TOPIC.as_slice()
        && (topic_address_matches(&log.topics[1], holder)
            || topic_address_matches(&log.topics[2], holder))
}

fn topic_address_matches(topic: &[u8], address: &[u8; 20]) -> bool {
    topic.len() == 32 && topic[12..32] == address[..]
}

fn block_bloom_may_contain_transfer_for_holder(block: &eth::Block, holder: &[u8; 20]) -> bool {
    let Some(header) = block.header.as_ref() else {
        return true;
    };
    if header.logs_bloom.len() != BLOOM_BYTES {
        return true;
    }

    bloom_contains(&header.logs_bloom, &TRANSFER_TOPIC)
        && bloom_contains(&header.logs_bloom, &padded_address(holder))
}

fn bloom_contains(bloom: &[u8], value: &[u8]) -> bool {
    bloom_bit_indexes(value)
        .iter()
        .all(|bit| bloom[BLOOM_BYTES - 1 - (bit / 8)] & (1 << (bit % 8)) != 0)
}

fn bloom_bit_indexes(value: &[u8]) -> [usize; 3] {
    let mut hash = [0u8; 32];
    let mut hasher = Keccak::v256();
    hasher.update(value);
    hasher.finalize(&mut hash);

    [
        (((hash[0] as usize) << 8) | hash[1] as usize) & (BLOOM_BITS - 1),
        (((hash[2] as usize) << 8) | hash[3] as usize) & (BLOOM_BITS - 1),
        (((hash[4] as usize) << 8) | hash[5] as usize) & (BLOOM_BITS - 1),
    ]
}

fn balance_mapping_keys(holder: &[u8; 20], max_mapping_slot: u16) -> FxHashSet<[u8; 32]> {
    let mut keys = FxHashSet::default();
    keys.reserve(max_mapping_slot as usize + 1);
    for slot in 0..=max_mapping_slot {
        keys.insert(balance_mapping_key(holder, slot));
    }
    keys
}

fn balance_mapping_key(holder: &[u8; 20], slot: u16) -> [u8; 32] {
    let mut encoded = [0u8; 64];
    encoded[12..32].copy_from_slice(holder);
    encoded[62..64].copy_from_slice(&slot.to_be_bytes());

    let mut key = [0u8; 32];
    let mut hasher = Keccak::v256();
    hasher.update(&encoded);
    hasher.finalize(&mut key);
    key
}

fn padded_address(address: &[u8; 20]) -> [u8; 32] {
    let mut padded = [0u8; 32];
    padded[12..32].copy_from_slice(address);
    padded
}

fn normalized_balance(balance: &[u8]) -> Vec<u8> {
    if balance.is_empty() {
        vec![0]
    } else {
        balance.to_vec()
    }
}

fn slot_key(value: &[u8]) -> Option<[u8; 32]> {
    if value.len() != 32 {
        return None;
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(value);
    Some(key)
}

fn address_key(value: &[u8]) -> Option<[u8; 20]> {
    if value.len() != 20 {
        return None;
    }
    let mut key = [0u8; 20];
    key.copy_from_slice(value);
    Some(key)
}

fn decode_address(address: &str) -> [u8; 20] {
    let address = hex::decode(address.trim_start_matches("0x")).expect("invalid address");
    assert_eq!(address.len(), 20, "address must be 20 bytes");

    let mut decoded = [0u8; 20];
    decoded.copy_from_slice(&address);
    decoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use substreams::scalar::BigInt;

    #[test]
    fn computes_solidity_balance_mapping_key() {
        let holder = address(0x11);
        let key = balance_mapping_key(&holder, 0);

        let mut encoded = [0u8; 64];
        encoded[12..32].copy_from_slice(&holder);
        let mut expected = [0u8; 32];
        let mut hasher = Keccak::v256();
        hasher.update(&encoded);
        hasher.finalize(&mut expected);

        assert_eq!(key, expected);
    }

    #[test]
    fn skips_block_when_bloom_cannot_match() {
        let params = Params { address: address(1), max_mapping_slot: 1 };
        let block = eth::Block {
            header: Some(eth::BlockHeader {
                logs_bloom: vec![0u8; BLOOM_BYTES],
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(balance_updates(&block, &params).is_empty());
    }

    #[test]
    fn emits_latest_storage_balance_for_watched_transfer() {
        let holder = address(1);
        let token = address(2);
        let slot = balance_mapping_key(&holder, 0);
        let block = eth::Block {
            transaction_traces: vec![eth::TransactionTrace {
                status: i32::from(eth::TransactionTraceStatus::Succeeded),
                index: 7,
                hash: vec![7],
                receipt: Some(eth::TransactionReceipt {
                    logs: vec![transfer_log(token.to_vec(), holder, address(3))],
                    ..Default::default()
                }),
                calls: vec![eth::Call {
                    address: token.to_vec(),
                    storage_changes: vec![
                        storage_change(token, slot, 10, 41, 100),
                        storage_change(token, slot, 41, 42, 101),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let params = Params { address: holder, max_mapping_slot: 1 };

        let updates = balance_updates(&block, &params);

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].token, token);
        assert_eq!(BigInt::from_signed_bytes_be(&updates[0].balance), BigInt::from(42));
        assert_eq!(updates[0].tx_index, 7);
    }

    #[test]
    fn ignores_matching_slot_without_watched_transfer() {
        let holder = address(1);
        let token = address(2);
        let slot = balance_mapping_key(&holder, 0);
        let block = eth::Block {
            transaction_traces: vec![eth::TransactionTrace {
                status: i32::from(eth::TransactionTraceStatus::Succeeded),
                index: 7,
                receipt: Some(eth::TransactionReceipt {
                    logs: vec![transfer_log(token.to_vec(), address(4), address(5))],
                    ..Default::default()
                }),
                calls: vec![eth::Call {
                    address: token.to_vec(),
                    storage_changes: vec![storage_change(token, slot, 10, 42, 100)],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let params = Params { address: holder, max_mapping_slot: 1 };

        assert!(balance_updates(&block, &params).is_empty());
    }

    fn transfer_log(token: Vec<u8>, from: [u8; 20], to: [u8; 20]) -> eth::Log {
        eth::Log {
            address: token,
            topics: vec![
                TRANSFER_TOPIC.to_vec(),
                padded_address(&from).to_vec(),
                padded_address(&to).to_vec(),
            ],
            data: word(1),
            ..Default::default()
        }
    }

    fn storage_change(
        token: [u8; 20],
        key: [u8; 32],
        old: i64,
        new: i64,
        ordinal: u64,
    ) -> eth::StorageChange {
        eth::StorageChange {
            address: token.to_vec(),
            key: key.to_vec(),
            old_value: BigInt::from(old).to_signed_bytes_be(),
            new_value: BigInt::from(new).to_signed_bytes_be(),
            ordinal,
        }
    }

    fn address(byte: u8) -> [u8; 20] {
        [byte; 20]
    }

    fn word(value: i64) -> Vec<u8> {
        let value = BigInt::from(value).to_signed_bytes_be();
        let mut word = vec![0u8; 32 - value.len()];
        word.extend(value);
        word
    }
}
