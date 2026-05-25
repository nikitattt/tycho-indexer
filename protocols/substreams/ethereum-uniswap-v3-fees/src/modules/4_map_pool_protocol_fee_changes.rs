use crate::{
    pb::uniswap::v3::{
        events::{pool_event, PoolEvent},
        Events,
    },
    storage::{
        changed_protocol_fee_bytes, PROTOCOL_FEES_SLOT, PROTOCOL_FEE_TOKEN0_OFFSET,
        PROTOCOL_FEE_TOKEN1_OFFSET,
    },
};
use itertools::Itertools;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::hash_map::Entry;
use substreams::scalar::BigInt;
use substreams_ethereum::pb::eth::v2 as eth;
use substreams_helper::hex::Hexable;
use tycho_substreams::prelude::*;

const BASE_PROTOCOL_FEES_INITIAL_BLOCK: u64 = 43_005_492;
const INLINE_POOL_LIMIT: usize = 4;

#[substreams::handlers::map]
pub fn map_pool_protocol_fee_changes(
    block: eth::Block,
    events: Events,
) -> Result<BlockEntityChanges, substreams::errors::Error> {
    Ok(collect_pool_protocol_fee_changes(&block, events))
}

fn collect_pool_protocol_fee_changes(block: &eth::Block, events: Events) -> BlockEntityChanges {
    let mut transaction_changes: FxHashMap<u64, TransactionChangesBuilder> = FxHashMap::default();

    if block.number >= BASE_PROTOCOL_FEES_INITIAL_BLOCK {
        let event_pools_by_tx = event_pools_by_tx(&events);
        add_protocol_fee_changes(block, &event_pools_by_tx, &mut transaction_changes);
    }

    BlockEntityChanges {
        block: None,
        changes: transaction_changes
            .drain()
            .sorted_unstable_by_key(|(index, _)| *index)
            .filter_map(|(_, builder)| builder.build())
            .map(|changes| TransactionEntityChanges {
                tx: changes.tx,
                entity_changes: changes.entity_changes,
                component_changes: changes.component_changes,
                balance_changes: changes.balance_changes,
            })
            .collect(),
    }
}

fn event_pools_by_tx(events: &Events) -> FxHashMap<u64, CandidatePools> {
    let mut pools_by_tx = FxHashMap::default();

    for event in &events.pool_events {
        let Some(tx) = &event.transaction else {
            continue;
        };

        if !can_change_protocol_fees(event) {
            continue;
        }

        let Some(pool) = PoolAddress::from_slice(&event.pool_address) else {
            continue;
        };

        pools_by_tx
            .entry(tx.index)
            .or_insert_with(CandidatePools::default)
            .insert(pool);
    }

    pools_by_tx
}

fn can_change_protocol_fees(event: &PoolEvent) -> bool {
    matches!(
        event.r#type.as_ref(),
        Some(pool_event::Type::Swap(_))
            | Some(pool_event::Type::Flash(_))
            | Some(pool_event::Type::CollectProtocol(_))
    )
}

fn add_protocol_fee_changes(
    block: &eth::Block,
    event_pools_by_tx: &FxHashMap<u64, CandidatePools>,
    transaction_changes: &mut FxHashMap<u64, TransactionChangesBuilder>,
) {
    let mut latest_attributes: FxHashMap<PendingAttributeKey, PendingAttribute> =
        FxHashMap::default();

    for tx in block.transactions() {
        if tx.status != 1 {
            continue;
        }

        let Some(event_pools) = event_pools_by_tx.get(&(tx.index as u64)) else {
            continue;
        };

        let mut tycho_tx = None;

        for call in &tx.calls {
            if call.state_reverted || !event_pools.contains(&call.address) {
                continue;
            }

            for storage_change in &call.storage_changes {
                if storage_change.key != PROTOCOL_FEES_SLOT {
                    continue;
                }

                let Some(pool) = event_pools.get(&storage_change.address) else {
                    continue;
                };

                upsert_protocol_fee_attribute(
                    &mut latest_attributes,
                    tx,
                    &mut tycho_tx,
                    pool,
                    storage_change,
                    ProtocolFeeToken::Token0,
                );
                upsert_protocol_fee_attribute(
                    &mut latest_attributes,
                    tx,
                    &mut tycho_tx,
                    pool,
                    storage_change,
                    ProtocolFeeToken::Token1,
                );
            }
        }
    }

    for pending in latest_attributes
        .into_values()
        .sorted_unstable_by_key(|pending| pending.order)
    {
        let PendingAttribute { tx, pool, token, value, .. } = pending;
        add_pool_attribute(
            transaction_changes,
            &tx,
            pool.as_slice(),
            Attribute { name: token.name().to_string(), value, change: ChangeType::Update.into() },
        );
    }
}

fn upsert_protocol_fee_attribute(
    latest_attributes: &mut FxHashMap<PendingAttributeKey, PendingAttribute>,
    tx: &eth::TransactionTrace,
    tycho_tx: &mut Option<Transaction>,
    pool: PoolAddress,
    change: &eth::StorageChange,
    token: ProtocolFeeToken,
) {
    let Some(new_value) =
        changed_protocol_fee_bytes(&change.old_value, &change.new_value, token.offset())
    else {
        return;
    };

    let key = PendingAttributeKey { pool, token };
    let order = (tx.index as u64, change.ordinal);

    match latest_attributes.entry(key) {
        Entry::Occupied(mut entry) => {
            if order > entry.get().order {
                entry.insert(PendingAttribute {
                    tx: tycho_tx
                        .get_or_insert_with(|| transaction_from_trace(tx))
                        .clone(),
                    pool,
                    token,
                    value: BigInt::from_unsigned_bytes_be(new_value).to_signed_bytes_be(),
                    order,
                });
            }
        }
        Entry::Vacant(entry) => {
            entry.insert(PendingAttribute {
                tx: tycho_tx
                    .get_or_insert_with(|| transaction_from_trace(tx))
                    .clone(),
                pool,
                token,
                value: BigInt::from_unsigned_bytes_be(new_value).to_signed_bytes_be(),
                order,
            });
        }
    }
}

fn add_pool_attribute(
    transaction_changes: &mut FxHashMap<u64, TransactionChangesBuilder>,
    tx: &Transaction,
    pool_address: &[u8],
    attribute: Attribute,
) {
    let builder = transaction_changes
        .entry(tx.index)
        .or_insert_with(|| TransactionChangesBuilder::new(tx));

    builder.add_entity_change(&EntityChanges {
        component_id: pool_address.to_hex(),
        attributes: vec![attribute],
    });
}

fn transaction_from_trace(tx: &eth::TransactionTrace) -> Transaction {
    Transaction {
        hash: tx.hash.clone(),
        from: tx.from.clone(),
        to: tx.to.clone(),
        index: tx.index.into(),
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct PoolAddress([u8; 20]);

impl PoolAddress {
    fn from_slice(address: &[u8]) -> Option<Self> {
        if address.len() != 20 {
            return None;
        }

        let mut pool = [0u8; 20];
        pool.copy_from_slice(address);
        Some(Self(pool))
    }

    fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

enum CandidatePools {
    Small(Vec<PoolAddress>),
    Large(FxHashSet<PoolAddress>),
}

impl Default for CandidatePools {
    fn default() -> Self {
        Self::Small(Vec::new())
    }
}

impl CandidatePools {
    fn insert(&mut self, pool: PoolAddress) {
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

    fn contains(&self, address: &[u8]) -> bool {
        match self {
            Self::Small(pools) => pools
                .iter()
                .any(|pool| pool.as_slice() == address),
            Self::Large(pools) => PoolAddress::from_slice(address)
                .map(|pool| pools.contains(&pool))
                .unwrap_or(false),
        }
    }

    fn get(&self, address: &[u8]) -> Option<PoolAddress> {
        match self {
            Self::Small(pools) => pools
                .iter()
                .copied()
                .find(|pool| pool.as_slice() == address),
            Self::Large(pools) => {
                let pool = PoolAddress::from_slice(address)?;
                pools.contains(&pool).then_some(pool)
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum ProtocolFeeToken {
    Token0,
    Token1,
}

impl ProtocolFeeToken {
    fn name(self) -> &'static str {
        match self {
            Self::Token0 => "protocol_fees/token0",
            Self::Token1 => "protocol_fees/token1",
        }
    }

    fn offset(self) -> usize {
        match self {
            Self::Token0 => PROTOCOL_FEE_TOKEN0_OFFSET,
            Self::Token1 => PROTOCOL_FEE_TOKEN1_OFFSET,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct PendingAttributeKey {
    pool: PoolAddress,
    token: ProtocolFeeToken,
}

struct PendingAttribute {
    tx: Transaction,
    pool: PoolAddress,
    token: ProtocolFeeToken,
    value: Vec<u8>,
    order: (u64, u64),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        pb::uniswap::v3::{
            events::{
                pool_event::{self, Type},
                PoolEvent,
            },
            Transaction,
        },
        storage::PROTOCOL_FEES_SLOT,
    };
    use substreams::scalar::BigInt;
    use substreams_ethereum::pb::eth::v2 as eth;

    #[test]
    fn emits_protocol_fee_storage_attributes_for_event_known_pool_without_store_lookup() {
        let pool = pool_bytes(1);
        let storage_change = protocol_fee_storage_change(pool_bytes(1), 10, 1, 2, 3, 4);
        let events = Events { pool_events: vec![swap_event(pool.clone(), 1)] };

        let changes = collect_pool_protocol_fee_changes(
            &block(43_005_492, vec![tx_trace(1, vec![storage_change])]),
            events,
        );
        let attrs = attributes_for_pool(&changes, &pool);

        assert_eq!(attr_value(&attrs, "protocol_fees/token0"), BigInt::from(3));
        assert_eq!(attr_value(&attrs, "protocol_fees/token1"), BigInt::from(4));
    }

    #[test]
    fn ignores_fee_storage_without_matching_pool_event() {
        let unknown_pool = pool_bytes(9);
        let storage_change = protocol_fee_storage_change(unknown_pool.clone(), 10, 1, 2, 3, 4);
        let events = Events { pool_events: vec![] };

        let changes = collect_pool_protocol_fee_changes(
            &block(43_005_492, vec![tx_trace(1, vec![storage_change.clone(), storage_change])]),
            events,
        );

        assert!(changes.changes.is_empty());
    }

    #[test]
    fn keeps_latest_protocol_fee_attribute_by_transaction_and_ordinal() {
        let pool = pool_bytes(1);
        let events = Events { pool_events: vec![swap_event(pool.clone(), 1)] };

        let changes = collect_pool_protocol_fee_changes(
            &block(
                43_005_492,
                vec![tx_trace(
                    1,
                    vec![
                        protocol_fee_storage_change(pool_bytes(1), 12, 3, 4, 7, 8),
                        protocol_fee_storage_change(pool_bytes(1), 10, 1, 2, 3, 4),
                    ],
                )],
            ),
            events,
        );
        let attrs = attributes_for_pool(&changes, &pool);

        assert_eq!(attr_value(&attrs, "protocol_fees/token0"), BigInt::from(7));
        assert_eq!(attr_value(&attrs, "protocol_fees/token1"), BigInt::from(8));
    }

    #[test]
    fn skips_protocol_fee_storage_changes_outside_event_pool_calls() {
        let pool = pool_bytes(1);
        let events = Events { pool_events: vec![swap_event(pool.clone(), 1)] };

        let changes = collect_pool_protocol_fee_changes(
            &block(
                43_005_492,
                vec![eth::TransactionTrace {
                    index: 1,
                    status: 1,
                    hash: vec![1; 32],
                    from: pool_bytes(7),
                    to: pool_bytes(8),
                    calls: vec![
                        eth::Call {
                            address: pool_bytes(9),
                            storage_changes: vec![protocol_fee_storage_change(
                                pool_bytes(1),
                                10,
                                1,
                                2,
                                3,
                                4,
                            )],
                            ..Default::default()
                        },
                        eth::Call {
                            address: pool_bytes(1),
                            storage_changes: vec![protocol_fee_storage_change(
                                pool_bytes(1),
                                11,
                                3,
                                4,
                                5,
                                6,
                            )],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
            ),
            events,
        );
        let attrs = attributes_for_pool(&changes, &pool);

        assert_eq!(attr_value(&attrs, "protocol_fees/token0"), BigInt::from(5));
        assert_eq!(attr_value(&attrs, "protocol_fees/token1"), BigInt::from(6));
    }

    #[test]
    fn ignores_protocol_fee_storage_for_non_fee_affecting_pool_events() {
        let pool = pool_bytes(1);
        let storage_change = protocol_fee_storage_change(pool_bytes(1), 10, 1, 2, 3, 4);
        let events = Events { pool_events: vec![mint_event(pool, 1)] };

        let changes = collect_pool_protocol_fee_changes(
            &block(43_005_492, vec![tx_trace(1, vec![storage_change])]),
            events,
        );

        assert!(changes.changes.is_empty());
    }

    #[test]
    fn handles_multiple_fee_candidate_pools_in_one_transaction() {
        let pool1 = pool_bytes(1);
        let pool2 = pool_bytes(2);
        let events = Events {
            pool_events: vec![
                swap_event_in_tx(pool1.clone(), 1, 1),
                swap_event_in_tx(pool1.clone(), 2, 1),
                swap_event_in_tx(pool2.clone(), 3, 1),
            ],
        };

        let changes = collect_pool_protocol_fee_changes(
            &block(
                43_005_492,
                vec![eth::TransactionTrace {
                    index: 1,
                    status: 1,
                    hash: vec![1; 32],
                    from: pool_bytes(7),
                    to: pool_bytes(8),
                    calls: vec![
                        eth::Call {
                            address: pool1.clone(),
                            storage_changes: vec![protocol_fee_storage_change(
                                pool1.clone(),
                                10,
                                1,
                                2,
                                3,
                                4,
                            )],
                            ..Default::default()
                        },
                        eth::Call {
                            address: pool2.clone(),
                            storage_changes: vec![protocol_fee_storage_change(
                                pool2.clone(),
                                11,
                                5,
                                6,
                                7,
                                8,
                            )],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
            ),
            events,
        );

        let pool1_attrs = attributes_for_pool(&changes, &pool1);
        let pool2_attrs = attributes_for_pool(&changes, &pool2);

        assert_eq!(attr_value(&pool1_attrs, "protocol_fees/token0"), BigInt::from(3));
        assert_eq!(attr_value(&pool1_attrs, "protocol_fees/token1"), BigInt::from(4));
        assert_eq!(attr_value(&pool2_attrs, "protocol_fees/token0"), BigInt::from(7));
        assert_eq!(attr_value(&pool2_attrs, "protocol_fees/token1"), BigInt::from(8));
    }

    fn attributes_for_pool(
        changes: &tycho_substreams::prelude::BlockEntityChanges,
        pool: &[u8],
    ) -> Vec<tycho_substreams::prelude::Attribute> {
        let component_id = pool.to_hex();
        changes
            .changes
            .iter()
            .flat_map(|tx| tx.entity_changes.iter())
            .filter(|entity| entity.component_id == component_id)
            .flat_map(|entity| entity.attributes.iter().cloned())
            .collect()
    }

    fn attr_value(attrs: &[tycho_substreams::prelude::Attribute], name: &str) -> BigInt {
        let attr = attrs
            .iter()
            .find(|attr| attr.name == name)
            .unwrap_or_else(|| panic!("missing attribute {name}"));
        BigInt::from_signed_bytes_be(&attr.value)
    }

    fn block(number: u64, transactions: Vec<eth::TransactionTrace>) -> eth::Block {
        eth::Block { number, transaction_traces: transactions, ..Default::default() }
    }

    fn tx_trace(index: u32, storage_changes: Vec<eth::StorageChange>) -> eth::TransactionTrace {
        eth::TransactionTrace {
            index,
            status: 1,
            hash: vec![index as u8; 32],
            from: pool_bytes(7),
            to: pool_bytes(8),
            calls: vec![eth::Call {
                address: storage_changes
                    .first()
                    .map(|change| change.address.clone())
                    .unwrap_or_default(),
                storage_changes,
                ..Default::default()
            }],
            ..Default::default()
        }
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

    fn swap_event(pool_address: Vec<u8>, ordinal: u64) -> PoolEvent {
        swap_event_in_tx(pool_address, ordinal, ordinal)
    }

    fn swap_event_in_tx(pool_address: Vec<u8>, ordinal: u64, tx_index: u64) -> PoolEvent {
        PoolEvent {
            pool_address,
            log_ordinal: ordinal,
            transaction: Some(tx(tx_index)),
            r#type: Some(Type::Swap(pool_event::Swap {
                sqrt_price: amount(0),
                tick: 0,
                liquidity: amount(0),
                amount_0: amount(0),
                amount_1: amount(0),
                sender: Vec::new(),
                recipient: Vec::new(),
            })),
            ..Default::default()
        }
    }

    fn mint_event(pool_address: Vec<u8>, ordinal: u64) -> PoolEvent {
        PoolEvent {
            pool_address,
            log_ordinal: ordinal,
            transaction: Some(tx(ordinal)),
            r#type: Some(Type::Mint(pool_event::Mint {
                sender: Vec::new(),
                owner: Vec::new(),
                tick_lower: -1,
                tick_upper: 1,
                amount: amount(1),
                amount_0: amount(0),
                amount_1: amount(0),
            })),
            ..Default::default()
        }
    }

    fn tx(index: u64) -> Transaction {
        Transaction { index, hash: vec![index as u8; 32], ..Default::default() }
    }

    fn amount(value: i64) -> Vec<u8> {
        BigInt::from(value).to_signed_bytes_be()
    }

    fn pool_bytes(seed: u8) -> Vec<u8> {
        vec![seed; 20]
    }
}
