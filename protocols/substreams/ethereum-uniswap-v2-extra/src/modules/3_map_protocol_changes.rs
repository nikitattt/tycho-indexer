use arrayvec::ArrayVec;
use itertools::Itertools;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Deserialize;
use std::collections::hash_map::Entry;
use substreams::store::{StoreGet, StoreGetProto};
use substreams_ethereum::pb::eth::v2::{self as eth};
use tiny_keccak::{Hasher, Keccak};

use crate::{
    abi::pool::events::Sync,
    storage::{v2_extra_attribute_kind, V2ExtraAttribute},
    store_key::StoreKey,
};
use hex_literal::hex;
use tycho_substreams::prelude::*;

const SYNC_TOPIC: [u8; 32] =
    hex!("1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1");
const MINT_TOPIC: [u8; 32] =
    hex!("4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f");
const BURN_TOPIC: [u8; 32] =
    hex!("dccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496");
const BLOOM_BYTES: usize = 256;
const BLOOM_BITS: usize = 2048;
const INLINE_POOL_LIMIT: usize = 4;

#[derive(Debug, Deserialize, Default)]
struct Params {
    #[serde(default)]
    use_bloom_gate: bool,
}

#[substreams::handlers::map]
pub fn map_protocol_changes(
    params: String,
    block: eth::Block,
    created_pools: BlockChanges,
    pools_store: StoreGetProto<ProtocolComponent>,
) -> Result<BlockChanges, substreams::errors::Error> {
    let params = parse_params(&params);

    Ok(collect_protocol_changes(&block, created_pools, &params, |address| {
        pools_store.get_last(pool_store_key(address))
    }))
}

fn parse_params(params: &str) -> Params {
    if params.trim().is_empty() {
        return Params::default();
    }

    serde_qs::from_str(params).expect("Unable to deserialize params")
}

fn collect_protocol_changes<F>(
    block: &eth::Block,
    created_pools: BlockChanges,
    params: &Params,
    mut lookup_pool: F,
) -> BlockChanges
where
    F: FnMut(&[u8]) -> Option<ProtocolComponent>,
{
    let block_metadata = created_pools.block;
    let mut transaction_changes = FxHashMap::default();

    for change in created_pools.changes {
        add_existing_transaction_change(&mut transaction_changes, change);
    }

    let mut pool_cache = PoolLookupCache::default();
    let mut latest_extra_attributes = FxHashMap::default();

    if !params.use_bloom_gate || block_bloom_may_contain_pair_events(block) {
        for tx in block
            .transaction_traces
            .iter()
            .filter(|tx| tx.status == i32::from(eth::TransactionTraceStatus::Succeeded))
        {
            let extra_candidate_pools =
                add_log_changes(tx, &mut pool_cache, &mut lookup_pool, &mut transaction_changes);

            if !extra_candidate_pools.is_empty() {
                add_extra_changes(
                    tx,
                    &extra_candidate_pools,
                    &mut pool_cache,
                    &mut lookup_pool,
                    &mut latest_extra_attributes,
                );
            }
        }
    }

    for pending in latest_extra_attributes
        .into_values()
        .sorted_unstable_by_key(|pending| pending.order)
    {
        let builder = transaction_builder(&mut transaction_changes, &pending.tx);
        builder.add_entity_change(&EntityChanges {
            component_id: pending.component_id,
            attributes: vec![pending.attribute],
        });
    }

    BlockChanges {
        block: block_metadata,
        changes: transaction_changes
            .drain()
            .sorted_unstable_by_key(|(index, _)| *index)
            .filter_map(|(_, builder)| builder.build())
            .collect(),
    }
}

fn block_bloom_may_contain_pair_events(block: &eth::Block) -> bool {
    let Some(header) = block.header.as_ref() else {
        return true;
    };

    if header.logs_bloom.len() != BLOOM_BYTES {
        return true;
    }

    bloom_contains(&header.logs_bloom, &SYNC_TOPIC)
        || bloom_contains(&header.logs_bloom, &MINT_TOPIC)
        || bloom_contains(&header.logs_bloom, &BURN_TOPIC)
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

fn add_log_changes<F>(
    tx: &eth::TransactionTrace,
    pool_cache: &mut PoolLookupCache,
    lookup_pool: &mut F,
    transaction_changes: &mut FxHashMap<u64, TransactionChangesBuilder>,
) -> CandidatePools
where
    F: FnMut(&[u8]) -> Option<ProtocolComponent>,
{
    let mut extra_candidate_pools = CandidatePools::default();

    let Some(receipt) = tx.receipt.as_ref() else {
        return extra_candidate_pools;
    };

    for log in &receipt.logs {
        let Some(kind) = classify_pair_log(log) else {
            continue;
        };

        let Some(pool) = pool_cache.get_or_lookup(&log.address, lookup_pool) else {
            continue;
        };

        match kind {
            PairLogKind::Sync => {
                let Ok(sync) = Sync::decode(log) else {
                    continue;
                };

                let component_id = pool.id.clone();
                let reserve0 = sync.reserve0.to_signed_bytes_be();
                let reserve1 = sync.reserve1.to_signed_bytes_be();
                let builder = transaction_builder_for_trace(transaction_changes, tx);

                builder.add_entity_change(&EntityChanges {
                    component_id: component_id.clone(),
                    attributes: vec![
                        Attribute {
                            name: "reserve0".to_string(),
                            value: reserve0.clone(),
                            change: ChangeType::Update.into(),
                        },
                        Attribute {
                            name: "reserve1".to_string(),
                            value: reserve1.clone(),
                            change: ChangeType::Update.into(),
                        },
                    ],
                });

                if let Some(token0) = pool.tokens.first() {
                    builder.add_balance_change(&BalanceChange {
                        token: token0.clone(),
                        balance: reserve0,
                        component_id: component_id.as_bytes().to_vec(),
                    });
                }
                if let Some(token1) = pool.tokens.get(1) {
                    builder.add_balance_change(&BalanceChange {
                        token: token1.clone(),
                        balance: reserve1,
                        component_id: component_id.as_bytes().to_vec(),
                    });
                }
            }
            PairLogKind::Mint | PairLogKind::Burn => {
                if let Some(pool_key) = address_key(&log.address) {
                    extra_candidate_pools.insert(pool_key);
                }
            }
        }
    }

    extra_candidate_pools
}

fn add_extra_changes<F>(
    tx: &eth::TransactionTrace,
    extra_candidate_pools: &CandidatePools,
    pool_cache: &mut PoolLookupCache,
    lookup_pool: &mut F,
    latest_extra_attributes: &mut FxHashMap<PendingAttributeKey, PendingAttribute>,
) where
    F: FnMut(&[u8]) -> Option<ProtocolComponent>,
{
    let mut tycho_tx: Option<Transaction> = None;

    for call in &tx.calls {
        if call.state_reverted || call.storage_changes.is_empty() {
            continue;
        }

        for storage_change in &call.storage_changes {
            let Some(kind) = v2_extra_attribute_kind(storage_change) else {
                continue;
            };
            let Some(pool_key) = extra_candidate_pools.get(&storage_change.address) else {
                continue;
            };
            let Some(pool) = pool_cache.get_or_lookup(&storage_change.address, lookup_pool) else {
                continue;
            };

            let order = (tx.index as u64, storage_change.ordinal);
            let key = PendingAttributeKey { pool: pool_key, kind };
            let pending = PendingAttribute {
                tx: tycho_tx
                    .get_or_insert_with(|| tx.into())
                    .clone(),
                component_id: pool.id.clone(),
                attribute: kind.attribute(&storage_change.new_value),
                order,
            };

            match latest_extra_attributes.entry(key) {
                Entry::Occupied(mut entry) => {
                    if order > entry.get().order {
                        entry.insert(pending);
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(pending);
                }
            }
        }
    }
}

fn add_existing_transaction_change(
    transaction_changes: &mut FxHashMap<u64, TransactionChangesBuilder>,
    change: TransactionChanges,
) {
    let Some(tx) = change.tx.as_ref() else {
        return;
    };

    let builder = transaction_builder(transaction_changes, tx);

    for entity_change in &change.entity_changes {
        builder.add_entity_change(entity_change);
    }
    for component in &change.component_changes {
        builder.add_protocol_component(component);
    }
    for balance_change in &change.balance_changes {
        builder.add_balance_change(balance_change);
    }
}

fn transaction_builder_for_trace<'a>(
    transaction_changes: &'a mut FxHashMap<u64, TransactionChangesBuilder>,
    tx: &eth::TransactionTrace,
) -> &'a mut TransactionChangesBuilder {
    transaction_changes
        .entry(tx.index.into())
        .or_insert_with(|| TransactionChangesBuilder::new(&tx.into()))
}

fn transaction_builder<'a>(
    transaction_changes: &'a mut FxHashMap<u64, TransactionChangesBuilder>,
    tx: &Transaction,
) -> &'a mut TransactionChangesBuilder {
    transaction_changes
        .entry(tx.index)
        .or_insert_with(|| TransactionChangesBuilder::new(tx))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PairLogKind {
    Sync,
    Mint,
    Burn,
}

fn classify_pair_log(log: &eth::Log) -> Option<PairLogKind> {
    let topic0 = log.topics.first()?.as_slice();

    match (log.topics.len(), log.data.len()) {
        (1, 64) if topic0 == SYNC_TOPIC.as_slice() => Some(PairLogKind::Sync),
        (2, 64) if topic0 == MINT_TOPIC.as_slice() => Some(PairLogKind::Mint),
        (3, 64) if topic0 == BURN_TOPIC.as_slice() => Some(PairLogKind::Burn),
        _ => None,
    }
}

fn pool_store_key(address: &[u8]) -> String {
    StoreKey::Pool.get_unique_pool_key(&hex_address(address))
}

fn hex_address(address: &[u8]) -> String {
    format!("0x{}", hex::encode(address))
}

fn address_key(address: &[u8]) -> Option<[u8; 20]> {
    if address.len() != 20 {
        return None;
    }

    let mut key = [0u8; 20];
    key.copy_from_slice(address);
    Some(key)
}

#[derive(Default)]
struct PoolLookupCache {
    pools: FxHashMap<[u8; 20], Option<ProtocolComponent>>,
}

impl PoolLookupCache {
    fn get_or_lookup<F>(
        &mut self,
        address: &[u8],
        lookup_pool: &mut F,
    ) -> Option<&ProtocolComponent>
    where
        F: FnMut(&[u8]) -> Option<ProtocolComponent>,
    {
        let key = address_key(address)?;

        match self.pools.entry(key) {
            Entry::Occupied(entry) => entry.into_mut().as_ref(),
            Entry::Vacant(entry) => entry
                .insert(lookup_pool(address))
                .as_ref(),
        }
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

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct PendingAttributeKey {
    pool: [u8; 20],
    kind: V2ExtraAttribute,
}

struct PendingAttribute {
    tx: Transaction,
    component_id: String,
    attribute: Attribute,
    order: (u64, u64),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abi::pool::events::{Burn, Mint};
    use crate::storage::{K_LAST_ATTRIBUTE, K_LAST_SLOT, TOTAL_SUPPLY_SLOT};
    use substreams::scalar::BigInt;

    const TRANSFER_TOPIC: [u8; 32] =
        hex!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");

    #[test]
    fn skips_non_sync_logs_before_pool_lookup() {
        let pool = address(1);
        let block = block_with_transactions(vec![transaction(
            0,
            vec![eth::Log {
                address: pool,
                topics: vec![TRANSFER_TOPIC.to_vec(), word(2), word(3)],
                data: word(1),
                ordinal: 10,
                ..Default::default()
            }],
            Vec::new(),
        )]);
        let mut lookups = 0;

        let changes =
            collect_protocol_changes(&block, BlockChanges::default(), &Params::default(), |_| {
                lookups += 1;
                None
            });

        assert_eq!(lookups, 0);
        assert!(changes.changes.is_empty());
    }

    #[test]
    fn caches_pool_lookup_hits_and_keeps_latest_sync_per_transaction() {
        let pool = address(1);
        let expected_topic = event_topic(b"Sync(uint112,uint112)");
        let log = sync_log(pool.clone(), 1, 2, 10);
        let block = block_with_transactions(vec![transaction(
            0,
            vec![log.clone(), sync_log(pool.clone(), 3, 4, 11)],
            Vec::new(),
        )]);
        let mut lookups = 0;

        assert_eq!(SYNC_TOPIC, expected_topic);
        assert_eq!(log.topics[0].as_slice(), expected_topic.as_slice());
        assert!(Sync::match_log(&log));

        let changes = collect_protocol_changes(
            &block,
            BlockChanges::default(),
            &Params::default(),
            |address| {
                lookups += 1;
                (address == pool.as_slice()).then(|| protocol_component(&pool))
            },
        );

        assert_eq!(lookups, 1);
        assert_eq!(attribute_value(&changes, &pool, "reserve0"), BigInt::from(3));
        assert_eq!(attribute_value(&changes, &pool, "reserve1"), BigInt::from(4));
        assert_eq!(balance_value(&changes, &pool, &token0()), BigInt::from(3));
        assert_eq!(balance_value(&changes, &pool, &token1()), BigInt::from(4));
    }

    #[test]
    fn caches_pool_lookup_misses_for_sync_shaped_logs() {
        let pool = address(1);
        let block = block_with_transactions(vec![transaction(
            0,
            vec![sync_log(pool.clone(), 1, 2, 10), sync_log(pool.clone(), 3, 4, 11)],
            Vec::new(),
        )]);
        let mut lookups = 0;

        let changes =
            collect_protocol_changes(&block, BlockChanges::default(), &Params::default(), |_| {
                lookups += 1;
                None
            });

        assert_eq!(lookups, 1);
        assert!(changes.changes.is_empty());
    }

    #[test]
    fn repeated_extra_updates_keep_latest_by_ordinal() {
        let pool = address(1);
        let block = block_with_transactions(vec![transaction(
            0,
            vec![mint_log(pool.clone(), 9)],
            vec![call(vec![
                storage_change(&pool, K_LAST_SLOT, 1, 2, 10),
                storage_change(&pool, K_LAST_SLOT, 2, 3, 20),
            ])],
        )]);
        let mut lookups = 0;

        let changes = collect_protocol_changes(
            &block,
            BlockChanges::default(),
            &Params::default(),
            |address| {
                lookups += 1;
                (address == pool.as_slice()).then(|| protocol_component(&pool))
            },
        );

        assert_eq!(lookups, 1);
        assert_eq!(attribute_value(&changes, &pool, K_LAST_ATTRIBUTE), BigInt::from(3));
    }

    #[test]
    fn skips_extra_storage_without_mint_or_burn_candidate() {
        let pool = address(1);
        let block = block_with_transactions(vec![transaction(
            0,
            vec![sync_log(pool.clone(), 1, 2, 10)],
            vec![call(vec![storage_change(&pool, K_LAST_SLOT, 1, 2, 11)])],
        )]);
        let mut lookups = 0;

        let changes = collect_protocol_changes(
            &block,
            BlockChanges::default(),
            &Params::default(),
            |address| {
                lookups += 1;
                (address == pool.as_slice()).then(|| protocol_component(&pool))
            },
        );

        assert_eq!(lookups, 1);
        assert_eq!(attribute_value(&changes, &pool, "reserve0"), BigInt::from(1));
        assert!(maybe_attribute_value(&changes, &pool, K_LAST_ATTRIBUTE).is_none());
    }

    #[test]
    fn limits_extra_storage_to_mint_or_burn_candidate_pools() {
        let pool = address(1);
        let other_pool = address(2);
        let block = block_with_transactions(vec![transaction(
            0,
            vec![burn_log(pool.clone(), 9)],
            vec![call(vec![
                storage_change(&pool, K_LAST_SLOT, 1, 2, 10),
                storage_change(&other_pool, K_LAST_SLOT, 1, 7, 11),
            ])],
        )]);
        let mut lookups = 0;

        let changes = collect_protocol_changes(
            &block,
            BlockChanges::default(),
            &Params::default(),
            |address| {
                lookups += 1;
                (address == pool.as_slice() || address == other_pool.as_slice())
                    .then(|| protocol_component(address))
            },
        );

        assert_eq!(lookups, 1);
        assert_eq!(attribute_value(&changes, &pool, K_LAST_ATTRIBUTE), BigInt::from(2));
        assert!(maybe_attribute_value(&changes, &other_pool, K_LAST_ATTRIBUTE).is_none());
    }

    #[test]
    fn bloom_gate_skips_log_and_storage_scan_when_pair_topics_are_absent() {
        let pool = address(1);
        let block = block_with_bloom(
            vec![transaction(
                0,
                vec![sync_log(pool.clone(), 1, 2, 10), mint_log(pool.clone(), 11)],
                vec![call(vec![storage_change(&pool, K_LAST_SLOT, 1, 2, 12)])],
            )],
            vec![0; BLOOM_BYTES],
        );
        let mut lookups = 0;

        let changes = collect_protocol_changes(
            &block,
            BlockChanges::default(),
            &Params { use_bloom_gate: true },
            |_| {
                lookups += 1;
                Some(protocol_component(&pool))
            },
        );

        assert_eq!(lookups, 0);
        assert!(changes.changes.is_empty());
    }

    #[test]
    fn bloom_gate_scans_when_pair_topic_may_be_present() {
        let pool = address(1);
        let block = block_with_bloom(
            vec![transaction(0, vec![sync_log(pool.clone(), 1, 2, 10)], Vec::new())],
            bloom_for(&SYNC_TOPIC),
        );
        let mut lookups = 0;

        let changes = collect_protocol_changes(
            &block,
            BlockChanges::default(),
            &Params { use_bloom_gate: true },
            |address| {
                lookups += 1;
                (address == pool.as_slice()).then(|| protocol_component(&pool))
            },
        );

        assert_eq!(lookups, 1);
        assert_eq!(attribute_value(&changes, &pool, "reserve0"), BigInt::from(1));
    }

    #[test]
    fn recognizes_pair_log_topic_shapes() {
        let pool = address(1);
        let sync = sync_log(pool.clone(), 1, 2, 10);
        let mint = mint_log(pool.clone(), 11);
        let burn = burn_log(pool, 12);

        assert_eq!(SYNC_TOPIC, event_topic(b"Sync(uint112,uint112)"));
        assert_eq!(classify_pair_log(&sync), Some(PairLogKind::Sync));
        assert_eq!(sync.topics.len(), 1);
        assert_eq!(sync.data.len(), 64);
        assert!(Sync::match_log(&sync));

        assert_eq!(MINT_TOPIC, event_topic(b"Mint(address,uint256,uint256)"));
        assert_eq!(classify_pair_log(&mint), Some(PairLogKind::Mint));
        assert_eq!(mint.topics.len(), 2);
        assert_eq!(mint.data.len(), 64);
        assert!(Mint::match_log(&mint));

        assert_eq!(BURN_TOPIC, event_topic(b"Burn(address,uint256,uint256,address)"));
        assert_eq!(classify_pair_log(&burn), Some(PairLogKind::Burn));
        assert_eq!(burn.topics.len(), 3);
        assert_eq!(burn.data.len(), 64);
        assert!(Burn::match_log(&burn));
    }

    #[test]
    fn skips_failed_transactions_and_reverted_calls() {
        let pool = address(1);
        let failed = eth::TransactionTrace {
            status: i32::from(eth::TransactionTraceStatus::Failed),
            calls: vec![call(vec![storage_change(&pool, K_LAST_SLOT, 1, 2, 10)])],
            receipt: Some(eth::TransactionReceipt {
                logs: vec![sync_log(pool.clone(), 1, 2, 10)],
                ..Default::default()
            }),
            ..transaction(0, Vec::new(), Vec::new())
        };
        let reverted_call = eth::Call {
            state_reverted: true,
            storage_changes: vec![storage_change(&pool, TOTAL_SUPPLY_SLOT, 1, 2, 10)],
            ..Default::default()
        };
        let block =
            block_with_transactions(vec![failed, transaction(1, Vec::new(), vec![reverted_call])]);
        let mut lookups = 0;

        let changes =
            collect_protocol_changes(&block, BlockChanges::default(), &Params::default(), |_| {
                lookups += 1;
                Some(protocol_component(&pool))
            });

        assert_eq!(lookups, 0);
        assert!(changes.changes.is_empty());
    }

    fn block_with_transactions(transactions: Vec<eth::TransactionTrace>) -> eth::Block {
        eth::Block { transaction_traces: transactions, ..Default::default() }
    }

    fn block_with_bloom(
        transactions: Vec<eth::TransactionTrace>,
        logs_bloom: Vec<u8>,
    ) -> eth::Block {
        eth::Block {
            header: Some(eth::BlockHeader { logs_bloom, ..Default::default() }),
            ..block_with_transactions(transactions)
        }
    }

    fn transaction(
        index: u32,
        logs: Vec<eth::Log>,
        calls: Vec<eth::Call>,
    ) -> eth::TransactionTrace {
        eth::TransactionTrace {
            index,
            status: i32::from(eth::TransactionTraceStatus::Succeeded),
            hash: vec![index as u8; 32],
            from: address(7),
            to: address(8),
            receipt: Some(eth::TransactionReceipt { logs, ..Default::default() }),
            calls,
            ..Default::default()
        }
    }

    fn call(storage_changes: Vec<eth::StorageChange>) -> eth::Call {
        eth::Call { storage_changes, ..Default::default() }
    }

    fn sync_log(address: Vec<u8>, reserve0: u64, reserve1: u64, ordinal: u64) -> eth::Log {
        let mut data = word(reserve0);
        data.extend(word(reserve1));

        eth::Log { address, topics: vec![SYNC_TOPIC.to_vec()], data, ordinal, ..Default::default() }
    }

    fn mint_log(address: Vec<u8>, ordinal: u64) -> eth::Log {
        let mut data = word(1);
        data.extend(word(2));

        eth::Log {
            address,
            topics: vec![MINT_TOPIC.to_vec(), word(7)],
            data,
            ordinal,
            ..Default::default()
        }
    }

    fn burn_log(address: Vec<u8>, ordinal: u64) -> eth::Log {
        let mut data = word(1);
        data.extend(word(2));

        eth::Log {
            address,
            topics: vec![BURN_TOPIC.to_vec(), word(7), word(8)],
            data,
            ordinal,
            ..Default::default()
        }
    }

    fn storage_change(
        address: &[u8],
        key: [u8; 32],
        old_value: u64,
        new_value: u64,
        ordinal: u64,
    ) -> eth::StorageChange {
        eth::StorageChange {
            address: address.to_vec(),
            key: key.to_vec(),
            old_value: word(old_value),
            new_value: word(new_value),
            ordinal,
        }
    }

    fn protocol_component(pool: &[u8]) -> ProtocolComponent {
        ProtocolComponent {
            id: hex_address(pool),
            tokens: vec![token0(), token1()],
            ..Default::default()
        }
    }

    fn attribute_value(changes: &BlockChanges, pool: &[u8], name: &str) -> BigInt {
        maybe_attribute_value(changes, pool, name)
            .unwrap_or_else(|| panic!("missing attribute {name}"))
    }

    fn maybe_attribute_value(changes: &BlockChanges, pool: &[u8], name: &str) -> Option<BigInt> {
        let component_id = hex_address(pool);
        changes
            .changes
            .iter()
            .flat_map(|tx| tx.entity_changes.iter())
            .find(|entity| entity.component_id == component_id)
            .and_then(|entity| {
                entity
                    .attributes
                    .iter()
                    .find(|attr| attr.name == name)
            })
            .map(|attribute| BigInt::from_signed_bytes_be(&attribute.value))
    }

    fn balance_value(changes: &BlockChanges, pool: &[u8], token: &[u8]) -> BigInt {
        let component_id = hex_address(pool).as_bytes().to_vec();
        let balance = changes
            .changes
            .iter()
            .flat_map(|tx| tx.balance_changes.iter())
            .find(|balance| balance.component_id == component_id && balance.token == token)
            .unwrap_or_else(|| panic!("missing balance change"));

        BigInt::from_signed_bytes_be(&balance.balance)
    }

    fn address(seed: u8) -> Vec<u8> {
        vec![seed; 20]
    }

    fn token0() -> Vec<u8> {
        address(10)
    }

    fn token1() -> Vec<u8> {
        address(11)
    }

    fn word(value: u64) -> Vec<u8> {
        let mut word = vec![0; 32];
        word[24..32].copy_from_slice(&value.to_be_bytes());
        word
    }

    fn event_topic(signature: &[u8]) -> [u8; 32] {
        let mut topic = [0u8; 32];
        let mut hasher = Keccak::v256();
        hasher.update(signature);
        hasher.finalize(&mut topic);
        topic
    }

    fn bloom_for(value: &[u8]) -> Vec<u8> {
        let mut bloom = vec![0; BLOOM_BYTES];
        for bit in bloom_bit_indexes(value) {
            bloom[BLOOM_BYTES - 1 - (bit / 8)] |= 1 << (bit % 8);
        }
        bloom
    }
}
