use crate::{
    modules::{
        map_events::{address_key, classify_v3_pool_event_log, log_to_event, PoolLookupCache},
        pool_key,
    },
    pb::uniswap::v3::{
        BlockMetadata, BlockPoolData, Events, Pool, ProtocolFeeChange, ProtocolFeeToken,
    },
    storage::{
        changed_protocol_fee_bytes, PROTOCOL_FEES_SLOT, PROTOCOL_FEE_TOKEN0_OFFSET,
        PROTOCOL_FEE_TOKEN1_OFFSET,
    },
};
use arrayvec::ArrayVec;
use itertools::Itertools;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::hash_map::Entry;
use substreams::scalar::BigInt;
use substreams::store::{StoreGet, StoreGetProto};
use substreams_ethereum::pb::eth::v2::{self as eth, TransactionTrace};

const BASE_PROTOCOL_FEES_INITIAL_BLOCK: u64 = 43_005_492;
const INLINE_POOL_LIMIT: usize = 4;

#[substreams::handlers::map]
pub fn map_pool_data(
    block: eth::Block,
    pools_store: StoreGetProto<Pool>,
) -> Result<BlockPoolData, anyhow::Error> {
    collect_pool_data(block, |address| pools_store.get_last(pool_key(address)))
}

fn collect_pool_data<F>(
    block: eth::Block,
    mut lookup_pool: F,
) -> Result<BlockPoolData, anyhow::Error>
where
    F: FnMut(&[u8]) -> Option<Pool>,
{
    let block_number = block.number;
    let block_metadata = block_metadata(&block);
    let mut pool_events = Vec::new();
    let mut last_pool_event_ordinal = None;
    let mut pool_events_are_sorted = true;
    let mut pool_cache = PoolLookupCache::default();
    let mut protocol_fee_changes = FxHashMap::default();

    for tx in block
        .transaction_traces
        .into_iter()
        .filter(|tx| tx.status == 1)
    {
        let mut fee_candidate_pools = CandidatePools::default();
        let receipt = tx
            .receipt
            .as_ref()
            .expect("all transaction traces have a receipt");

        for log in &receipt.logs {
            let Some(event_kind) = classify_v3_pool_event_log(log) else {
                continue;
            };
            let Some(pool) = pool_cache.get_or_lookup(&log.address, &mut lookup_pool) else {
                continue;
            };
            let Some(event) = log_to_event(log, event_kind, pool, &tx) else {
                continue;
            };

            if event_kind.can_change_protocol_fees() {
                if let Some(pool) = address_key(&event.pool_address) {
                    fee_candidate_pools.insert(pool);
                }
            }

            if let Some(last_ordinal) = last_pool_event_ordinal {
                pool_events_are_sorted &= last_ordinal <= event.log_ordinal;
            }
            last_pool_event_ordinal = Some(event.log_ordinal);
            pool_events.push(event);
        }

        if block_number >= BASE_PROTOCOL_FEES_INITIAL_BLOCK && !fee_candidate_pools.is_empty() {
            add_tx_protocol_fee_changes(&tx, &fee_candidate_pools, &mut protocol_fee_changes);
        }
    }

    if !pool_events_are_sorted {
        pool_events.sort_unstable_by_key(|e| e.log_ordinal);
    }

    Ok(BlockPoolData {
        block: Some(block_metadata),
        events: Some(Events { pool_events }),
        protocol_fee_changes: protocol_fee_changes
            .into_values()
            .map(PendingProtocolFeeChange::into_protocol_fee_change)
            .sorted_unstable_by_key(|change| (change.ordinal, change.token))
            .collect(),
    })
}

fn block_metadata(block: &eth::Block) -> BlockMetadata {
    let header = block
        .header
        .as_ref()
        .expect("all blocks have a header");
    let timestamp = header
        .timestamp
        .as_ref()
        .expect("all block headers have a timestamp");

    BlockMetadata {
        hash: block.hash.clone(),
        parent_hash: header.parent_hash.clone(),
        number: block.number,
        timestamp: timestamp.seconds as u64,
    }
}

enum CandidatePools {
    Small(ArrayVec<[u8; 20], INLINE_POOL_LIMIT>),
    Large(FxHashSet<[u8; 20]>),
}

impl Default for CandidatePools {
    fn default() -> Self {
        Self::Small(ArrayVec::new())
    }
}

impl CandidatePools {
    fn insert(&mut self, pool: [u8; 20]) {
        match self {
            Self::Small(pools) => {
                if pools.contains(&pool) {
                    return;
                }

                if pools.len() < INLINE_POOL_LIMIT {
                    pools.push(pool);
                    return;
                }

                let mut large = FxHashSet::default();
                large.reserve(pools.len() + 1);
                for pool in pools.drain(..) {
                    large.insert(pool);
                }
                large.insert(pool);
                *self = Self::Large(large);
            }
            Self::Large(pools) => {
                pools.insert(pool);
            }
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Small(pools) => pools.is_empty(),
            Self::Large(pools) => pools.is_empty(),
        }
    }

    fn get(&self, address: &[u8]) -> Option<[u8; 20]> {
        match self {
            Self::Small(pools) => pools
                .iter()
                .copied()
                .find(|pool| pool.as_slice() == address),
            Self::Large(pools) => {
                let pool = address_key(address)?;
                pools.contains(&pool).then_some(pool)
            }
        }
    }
}

fn add_tx_protocol_fee_changes(
    tx: &TransactionTrace,
    event_pools: &CandidatePools,
    latest_changes: &mut FxHashMap<PendingProtocolFeeKey, PendingProtocolFeeChange>,
) {
    let mut tycho_tx = None;

    for call in &tx.calls {
        if call.state_reverted || call.storage_changes.is_empty() {
            continue;
        }

        let mut call_pool = None;
        let mut call_pool_checked = false;

        for storage_change in &call.storage_changes {
            if storage_change.key.as_slice() != PROTOCOL_FEES_SLOT.as_slice() {
                continue;
            }

            if !call_pool_checked {
                call_pool = event_pools.get(&call.address);
                call_pool_checked = true;
            }

            let Some(call_pool) = call_pool else {
                break;
            };

            let pool = if storage_change.address.as_slice() == call.address.as_slice() {
                call_pool
            } else {
                let Some(pool) = event_pools.get(&storage_change.address) else {
                    continue;
                };
                pool
            };

            upsert_protocol_fee_change(
                latest_changes,
                tx,
                &mut tycho_tx,
                pool,
                storage_change,
                ProtocolFeeToken::Token0,
            );
            upsert_protocol_fee_change(
                latest_changes,
                tx,
                &mut tycho_tx,
                pool,
                storage_change,
                ProtocolFeeToken::Token1,
            );
        }
    }
}

fn upsert_protocol_fee_change(
    latest_changes: &mut FxHashMap<PendingProtocolFeeKey, PendingProtocolFeeChange>,
    tx: &TransactionTrace,
    tycho_tx: &mut Option<crate::pb::uniswap::v3::Transaction>,
    pool: [u8; 20],
    change: &eth::StorageChange,
    token: ProtocolFeeToken,
) {
    let Some(new_value) = changed_protocol_fee_bytes(
        &change.old_value,
        &change.new_value,
        protocol_fee_offset(token),
    ) else {
        return;
    };

    let key = PendingProtocolFeeKey { pool, token };
    let order = (tx.index as u64, change.ordinal);

    match latest_changes.entry(key) {
        Entry::Occupied(mut entry) => {
            if order > entry.get().order {
                entry.insert(PendingProtocolFeeChange {
                    tx: tycho_tx
                        .get_or_insert_with(|| tx.into())
                        .clone(),
                    pool,
                    token,
                    value: BigInt::from_unsigned_bytes_be(new_value).to_signed_bytes_be(),
                    ordinal: change.ordinal,
                    order,
                });
            }
        }
        Entry::Vacant(entry) => {
            entry.insert(PendingProtocolFeeChange {
                tx: tycho_tx
                    .get_or_insert_with(|| tx.into())
                    .clone(),
                pool,
                token,
                value: BigInt::from_unsigned_bytes_be(new_value).to_signed_bytes_be(),
                ordinal: change.ordinal,
                order,
            });
        }
    }
}

fn protocol_fee_offset(token: ProtocolFeeToken) -> usize {
    match token {
        ProtocolFeeToken::Token0 => PROTOCOL_FEE_TOKEN0_OFFSET,
        ProtocolFeeToken::Token1 => PROTOCOL_FEE_TOKEN1_OFFSET,
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct PendingProtocolFeeKey {
    pool: [u8; 20],
    token: ProtocolFeeToken,
}

struct PendingProtocolFeeChange {
    tx: crate::pb::uniswap::v3::Transaction,
    pool: [u8; 20],
    token: ProtocolFeeToken,
    value: Vec<u8>,
    ordinal: u64,
    order: (u64, u64),
}

impl PendingProtocolFeeChange {
    fn into_protocol_fee_change(self) -> ProtocolFeeChange {
        ProtocolFeeChange {
            pool_address: self.pool.to_vec(),
            token: self.token.into(),
            value: self.value,
            ordinal: self.ordinal,
            transaction: Some(self.tx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;
    use substreams_ethereum::pb::eth::v2::Log;

    const SWAP_TOPIC: [u8; 32] =
        hex!("c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67");
    const MINT_TOPIC: [u8; 32] =
        hex!("7a53080ba414158be7ec69b987b5fb7d07dee101fe85548f0853ae16239d0bde");

    #[test]
    fn extracts_events_block_metadata_and_protocol_fee_changes_once() {
        let pool = pool_address(1);
        let data = collect_pool_data(
            eth::Block {
                hash: vec![9; 32],
                number: BASE_PROTOCOL_FEES_INITIAL_BLOCK,
                header: Some(eth::BlockHeader {
                    parent_hash: vec![8; 32],
                    timestamp: Some(prost_types::Timestamp { seconds: 1_700_000_000, nanos: 0 }),
                    ..Default::default()
                }),
                transaction_traces: vec![TransactionTrace {
                    index: 7,
                    status: 1,
                    hash: vec![7; 32],
                    from: pool_address(7),
                    to: pool_address(8),
                    receipt: Some(eth::TransactionReceipt {
                        logs: vec![swap_log(pool.clone(), 11)],
                        ..Default::default()
                    }),
                    calls: vec![eth::Call {
                        address: pool.clone(),
                        storage_changes: vec![
                            protocol_fee_storage_change(pool.clone(), 12, 1, 2, 3, 4),
                            protocol_fee_storage_change(pool.clone(), 13, 3, 4, 5, 6),
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            |address| {
                if address == pool.as_slice() {
                    Some(pool_from_address(pool.clone()))
                } else {
                    None
                }
            },
        )
        .unwrap();

        let block = data.block.unwrap();
        assert_eq!(block.number, BASE_PROTOCOL_FEES_INITIAL_BLOCK);
        assert_eq!(block.hash, vec![9; 32]);
        assert_eq!(block.parent_hash, vec![8; 32]);
        assert_eq!(block.timestamp, 1_700_000_000);

        let events = data.events.unwrap();
        assert_eq!(events.pool_events.len(), 1);
        assert_eq!(events.pool_events[0].pool_address, pool);

        assert_eq!(data.protocol_fee_changes.len(), 2);
        assert_eq!(data.protocol_fee_changes[0].pool_address, pool_address(1));
        assert_eq!(data.protocol_fee_changes[0].token, i32::from(ProtocolFeeToken::Token0));
        assert_eq!(
            BigInt::from_signed_bytes_be(&data.protocol_fee_changes[0].value),
            BigInt::from(5)
        );
        assert_eq!(data.protocol_fee_changes[1].token, i32::from(ProtocolFeeToken::Token1));
        assert_eq!(
            BigInt::from_signed_bytes_be(&data.protocol_fee_changes[1].value),
            BigInt::from(6)
        );
    }

    #[test]
    fn ignores_fee_storage_without_matching_pool_event() {
        let pool = pool_address(1);
        let data = collect_with_pools(
            block_with_transactions(vec![tx_with_logs_and_calls(
                1,
                Vec::new(),
                vec![call_with_storage(
                    pool.clone(),
                    vec![protocol_fee_storage_change(pool.clone(), 10, 1, 2, 3, 4)],
                )],
            )]),
            &[pool],
        );

        assert!(data.protocol_fee_changes.is_empty());
    }

    #[test]
    fn ignores_protocol_fee_storage_for_non_fee_affecting_pool_events() {
        let pool = pool_address(1);
        let data = collect_with_pools(
            block_with_transactions(vec![tx_with_logs_and_calls(
                1,
                vec![mint_log(pool.clone(), 9)],
                vec![call_with_storage(
                    pool.clone(),
                    vec![protocol_fee_storage_change(pool.clone(), 10, 1, 2, 3, 4)],
                )],
            )]),
            &[pool],
        );

        assert!(data.protocol_fee_changes.is_empty());
    }

    #[test]
    fn skips_protocol_fee_storage_changes_outside_event_pool_calls() {
        let pool = pool_address(1);
        let data = collect_with_pools(
            block_with_transactions(vec![tx_with_logs_and_calls(
                1,
                vec![swap_log(pool.clone(), 9)],
                vec![
                    call_with_storage(
                        pool_address(9),
                        vec![protocol_fee_storage_change(pool.clone(), 10, 1, 2, 3, 4)],
                    ),
                    call_with_storage(
                        pool.clone(),
                        vec![protocol_fee_storage_change(pool.clone(), 11, 3, 4, 5, 6)],
                    ),
                ],
            )]),
            &[pool.clone()],
        );

        assert_eq!(protocol_fee_value(&data, &pool, ProtocolFeeToken::Token0), BigInt::from(5));
        assert_eq!(protocol_fee_value(&data, &pool, ProtocolFeeToken::Token1), BigInt::from(6));
    }

    #[test]
    fn handles_multiple_fee_candidate_pools_in_one_transaction() {
        let pool1 = pool_address(1);
        let pool2 = pool_address(2);
        let data = collect_with_pools(
            block_with_transactions(vec![tx_with_logs_and_calls(
                1,
                vec![
                    swap_log(pool1.clone(), 9),
                    swap_log(pool1.clone(), 10),
                    swap_log(pool2.clone(), 11),
                ],
                vec![
                    call_with_storage(
                        pool1.clone(),
                        vec![protocol_fee_storage_change(pool1.clone(), 12, 1, 2, 3, 4)],
                    ),
                    call_with_storage(
                        pool2.clone(),
                        vec![protocol_fee_storage_change(pool2.clone(), 13, 5, 6, 7, 8)],
                    ),
                ],
            )]),
            &[pool1.clone(), pool2.clone()],
        );

        assert_eq!(protocol_fee_value(&data, &pool1, ProtocolFeeToken::Token0), BigInt::from(3));
        assert_eq!(protocol_fee_value(&data, &pool1, ProtocolFeeToken::Token1), BigInt::from(4));
        assert_eq!(protocol_fee_value(&data, &pool2, ProtocolFeeToken::Token0), BigInt::from(7));
        assert_eq!(protocol_fee_value(&data, &pool2, ProtocolFeeToken::Token1), BigInt::from(8));
    }

    #[test]
    fn preserves_ordinal_event_order_when_block_logs_are_out_of_order() {
        let pool = pool_address(1);
        let data = collect_with_pools(
            block_with_transactions(vec![tx_with_logs_and_calls(
                1,
                vec![swap_log(pool.clone(), 20), swap_log(pool.clone(), 10)],
                Vec::new(),
            )]),
            &[pool],
        );

        let events = data.events.unwrap();
        assert_eq!(events.pool_events[0].log_ordinal, 10);
        assert_eq!(events.pool_events[1].log_ordinal, 20);
    }

    #[test]
    fn skips_protocol_fee_storage_before_fee_tracking_start_block() {
        let pool = pool_address(1);
        let data = collect_pool_data(
            block_with_number_and_transactions(
                BASE_PROTOCOL_FEES_INITIAL_BLOCK - 1,
                vec![tx_with_logs_and_calls(
                    1,
                    vec![swap_log(pool.clone(), 9)],
                    vec![call_with_storage(
                        pool.clone(),
                        vec![protocol_fee_storage_change(pool.clone(), 10, 1, 2, 3, 4)],
                    )],
                )],
            ),
            |address| {
                if address == pool.as_slice() {
                    Some(pool_from_address(pool.clone()))
                } else {
                    None
                }
            },
        )
        .unwrap();

        assert!(data.protocol_fee_changes.is_empty());
    }

    fn swap_log(address: Vec<u8>, ordinal: u64) -> Log {
        Log {
            address,
            topics: vec![SWAP_TOPIC.to_vec(), address_topic(2), address_topic(3)],
            data: vec![0; 160],
            ordinal,
            ..Default::default()
        }
    }

    fn mint_log(address: Vec<u8>, ordinal: u64) -> Log {
        Log {
            address,
            topics: vec![MINT_TOPIC.to_vec(), address_topic(2), vec![0; 32], vec![0; 32]],
            data: vec![0; 128],
            ordinal,
            ..Default::default()
        }
    }

    fn pool_from_address(address: Vec<u8>) -> Pool {
        Pool {
            address,
            token0: pool_address(11),
            token1: pool_address(21),
            created_tx_hash: vec![1; 32],
        }
    }

    fn block_with_transactions(transactions: Vec<TransactionTrace>) -> eth::Block {
        block_with_number_and_transactions(BASE_PROTOCOL_FEES_INITIAL_BLOCK, transactions)
    }

    fn block_with_number_and_transactions(
        number: u64,
        transactions: Vec<TransactionTrace>,
    ) -> eth::Block {
        eth::Block {
            hash: vec![9; 32],
            number,
            header: Some(eth::BlockHeader {
                parent_hash: vec![8; 32],
                timestamp: Some(prost_types::Timestamp { seconds: 1_700_000_000, nanos: 0 }),
                ..Default::default()
            }),
            transaction_traces: transactions,
            ..Default::default()
        }
    }

    fn tx_with_logs_and_calls(
        index: u32,
        logs: Vec<Log>,
        calls: Vec<eth::Call>,
    ) -> TransactionTrace {
        TransactionTrace {
            index,
            status: 1,
            hash: vec![index as u8; 32],
            from: pool_address(7),
            to: pool_address(8),
            receipt: Some(eth::TransactionReceipt { logs, ..Default::default() }),
            calls,
            ..Default::default()
        }
    }

    fn call_with_storage(address: Vec<u8>, storage_changes: Vec<eth::StorageChange>) -> eth::Call {
        eth::Call { address, storage_changes, ..Default::default() }
    }

    fn collect_with_pools(block: eth::Block, pools: &[Vec<u8>]) -> BlockPoolData {
        collect_pool_data(block, |address| {
            pools
                .iter()
                .find(|pool| pool.as_slice() == address)
                .map(|pool| pool_from_address(pool.clone()))
        })
        .unwrap()
    }

    fn protocol_fee_value(data: &BlockPoolData, pool: &[u8], token: ProtocolFeeToken) -> BigInt {
        let change = data
            .protocol_fee_changes
            .iter()
            .find(|change| change.pool_address == pool && change.token == i32::from(token))
            .unwrap_or_else(|| panic!("missing protocol fee change for token {token:?}"));
        BigInt::from_signed_bytes_be(&change.value)
    }

    fn pool_address(seed: u8) -> Vec<u8> {
        vec![seed; 20]
    }

    fn address_topic(seed: u8) -> Vec<u8> {
        let mut topic = vec![0; 12];
        topic.extend(pool_address(seed));
        topic
    }

    fn protocol_fee_storage_change(
        address: Vec<u8>,
        ordinal: u64,
        old_token0: u128,
        old_token1: u128,
        new_token0: u128,
        new_token1: u128,
    ) -> eth::StorageChange {
        eth::StorageChange {
            address,
            key: PROTOCOL_FEES_SLOT.to_vec(),
            old_value: protocol_fee_slot_value(old_token0, old_token1),
            new_value: protocol_fee_slot_value(new_token0, new_token1),
            ordinal,
        }
    }

    fn protocol_fee_slot_value(token0: u128, token1: u128) -> Vec<u8> {
        let mut value = token1.to_be_bytes().to_vec();
        value.extend(token0.to_be_bytes());
        value
    }
}
