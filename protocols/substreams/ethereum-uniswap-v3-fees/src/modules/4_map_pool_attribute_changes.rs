use crate::{
    modules::pool_key,
    pb::uniswap::v3::{
        events::{pool_event, PoolEvent},
        Events, Pool,
    },
    storage::{protocol_fee_attributes, PROTOCOL_FEES_SLOT},
};
use itertools::Itertools;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};
use substreams::{
    scalar::BigInt,
    store::{StoreGet, StoreGetProto},
};
use substreams_ethereum::pb::eth::v2 as eth;
use substreams_helper::hex::Hexable;
use tycho_substreams::prelude::*;

type PoolAddress = Vec<u8>;

const BASE_PROTOCOL_FEES_INITIAL_BLOCK: u64 = 43_005_492;

#[substreams::handlers::map]
pub fn map_pool_attribute_changes(
    block: eth::Block,
    events: Events,
    pools_store: StoreGetProto<Pool>,
) -> Result<BlockEntityChanges, substreams::errors::Error> {
    Ok(collect_pool_attribute_changes(&block, events, |address| {
        pools_store.get_last(pool_key(address))
    }))
}

fn collect_pool_attribute_changes<F>(
    block: &eth::Block,
    events: Events,
    mut lookup_pool: F,
) -> BlockEntityChanges
where
    F: FnMut(&[u8]) -> Option<Pool>,
{
    let event_known_pools = events
        .pool_events
        .iter()
        .filter_map(|event| hex::decode(&event.pool_address).ok())
        .collect::<HashSet<_>>();
    let mut transaction_changes: HashMap<u64, TransactionChangesBuilder> = HashMap::new();

    add_event_attribute_changes(events, &mut transaction_changes);

    if block.number >= BASE_PROTOCOL_FEES_INITIAL_BLOCK {
        add_protocol_fee_changes(
            block,
            &event_known_pools,
            &mut lookup_pool,
            &mut transaction_changes,
        );
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

fn add_event_attribute_changes(
    events: Events,
    transaction_changes: &mut HashMap<u64, TransactionChangesBuilder>,
) {
    events
        .pool_events
        .into_iter()
        .flat_map(event_to_attribute_updates)
        .for_each(|(tx, pool_address, attr)| {
            add_pool_attribute(transaction_changes, &tx, &pool_address, attr);
        });
}

fn add_protocol_fee_changes<F>(
    block: &eth::Block,
    event_known_pools: &HashSet<Vec<u8>>,
    lookup_pool: &mut F,
    transaction_changes: &mut HashMap<u64, TransactionChangesBuilder>,
) where
    F: FnMut(&[u8]) -> Option<Pool>,
{
    let mut latest_attributes: HashMap<(Vec<u8>, String), PendingAttribute> = HashMap::new();
    let mut known_pools: HashMap<Vec<u8>, bool> = HashMap::new();

    for tx in block.transactions() {
        if tx.status != 1 {
            continue;
        }

        let mut protocol_fee_storage_changes = tx
            .calls()
            .filter(|call_view| !call_view.call.state_reverted)
            .flat_map(|call_view| call_view.call.storage_changes.iter())
            .filter(|change| change.key == PROTOCOL_FEES_SLOT)
            .collect::<Vec<_>>();

        if protocol_fee_storage_changes.is_empty() {
            continue;
        }

        protocol_fee_storage_changes.sort_unstable_by_key(|change| change.ordinal);

        let tycho_tx = Transaction {
            hash: tx.hash.clone(),
            from: tx.from.clone(),
            to: tx.to.clone(),
            index: tx.index.into(),
        };

        for storage_change in protocol_fee_storage_changes {
            let pool = &storage_change.address;
            let attributes = protocol_fee_attributes(storage_change);

            if attributes.is_empty()
                || !is_known_pool(pool, event_known_pools, lookup_pool, &mut known_pools)
            {
                continue;
            }

            for attribute in attributes {
                let key = (pool.clone(), attribute.name.clone());
                latest_attributes.insert(
                    key,
                    PendingAttribute {
                        tx: tycho_tx.clone(),
                        pool: pool.clone(),
                        attribute,
                        order: (tycho_tx.index, storage_change.ordinal),
                    },
                );
            }
        }
    }

    for pending in latest_attributes
        .into_values()
        .sorted_unstable_by_key(|pending| pending.order)
    {
        add_pool_attribute(transaction_changes, &pending.tx, &pending.pool, pending.attribute);
    }
}

fn is_known_pool<F>(
    address: &[u8],
    event_known_pools: &HashSet<Vec<u8>>,
    lookup_pool: &mut F,
    known_pools: &mut HashMap<Vec<u8>, bool>,
) -> bool
where
    F: FnMut(&[u8]) -> Option<Pool>,
{
    if event_known_pools.contains(address) {
        return true;
    }

    if let Some(is_known) = known_pools.get(address) {
        return *is_known;
    }

    let is_known = lookup_pool(address).is_some();
    known_pools.insert(address.to_vec(), is_known);
    is_known
}

fn add_pool_attribute(
    transaction_changes: &mut HashMap<u64, TransactionChangesBuilder>,
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

struct PendingAttribute {
    tx: Transaction,
    pool: Vec<u8>,
    attribute: Attribute,
    order: (u64, u64),
}

fn event_to_attribute_updates(event: PoolEvent) -> Vec<(Transaction, PoolAddress, Attribute)> {
    match event.r#type.as_ref().unwrap() {
        pool_event::Type::Initialize(initialize) => {
            vec![
                (
                    event
                        .transaction
                        .as_ref()
                        .unwrap()
                        .into(),
                    hex::decode(&event.pool_address).unwrap(),
                    Attribute {
                        name: "sqrt_price_x96".to_string(),
                        value: BigInt::from_str(&initialize.sqrt_price)
                            .unwrap()
                            .to_signed_bytes_be(),
                        change: ChangeType::Update.into(),
                    },
                ),
                (
                    event.transaction.unwrap().into(),
                    hex::decode(event.pool_address).unwrap(),
                    Attribute {
                        name: "tick".to_string(),
                        value: BigInt::from(initialize.tick).to_signed_bytes_be(),
                        change: ChangeType::Update.into(),
                    },
                ),
            ]
        }
        pool_event::Type::Swap(swap) => vec![
            (
                event
                    .transaction
                    .as_ref()
                    .unwrap()
                    .into(),
                hex::decode(&event.pool_address).unwrap(),
                Attribute {
                    name: "sqrt_price_x96".to_string(),
                    value: BigInt::from_str(&swap.sqrt_price)
                        .unwrap()
                        .to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            ),
            (
                event.transaction.unwrap().into(),
                hex::decode(event.pool_address).unwrap(),
                Attribute {
                    name: "tick".to_string(),
                    value: BigInt::from(swap.tick).to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            ),
        ],
        pool_event::Type::SetFeeProtocol(sfp) => vec![
            (
                event
                    .transaction
                    .as_ref()
                    .unwrap()
                    .into(),
                hex::decode(&event.pool_address).unwrap(),
                Attribute {
                    name: "fee_protocol/token0".to_string(),
                    value: BigInt::from(sfp.fee_protocol_0_new).to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            ),
            (
                event.transaction.unwrap().into(),
                hex::decode(event.pool_address).unwrap(),
                Attribute {
                    name: "fee_protocol/token1".to_string(),
                    value: BigInt::from(sfp.fee_protocol_1_new).to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            ),
        ],
        _ => vec![],
    }
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
            Events, Pool, Transaction,
        },
        storage::PROTOCOL_FEES_SLOT,
    };
    use substreams::scalar::BigInt;
    use substreams_ethereum::pb::eth::v2 as eth;
    use tycho_substreams::prelude::ChangeType;

    #[test]
    fn emits_event_driven_pool_attributes() {
        let pool = pool_address(1);
        let events = Events {
            pool_events: vec![
                initialize_event(pool.clone(), 10, 123, -7),
                swap_event(pool.clone(), 11, 456, 8),
                set_fee_protocol_event(pool.clone(), 12, 3, 4),
            ],
        };

        let changes = collect_pool_attribute_changes(&block(43_005_491, vec![]), events, |_| None);
        let attrs = attributes_for_pool(&changes, &pool);

        assert_eq!(attr_value(&attrs, "sqrt_price_x96"), BigInt::from(456));
        assert_eq!(attr_value(&attrs, "tick"), BigInt::from(8));
        assert_eq!(attr_value(&attrs, "fee_protocol/token0"), BigInt::from(3));
        assert_eq!(attr_value(&attrs, "fee_protocol/token1"), BigInt::from(4));
    }

    #[test]
    fn emits_protocol_fee_storage_attributes_for_event_known_pool_without_store_lookup() {
        let pool = pool_address(1);
        let storage_change = protocol_fee_storage_change(pool_bytes(1), 10, 1, 2, 3, 4);
        let events = Events { pool_events: vec![swap_event(pool.clone(), 9, 0, 0)] };
        let mut lookups = 0;

        let changes = collect_pool_attribute_changes(
            &block(43_005_492, vec![tx_trace(1, vec![storage_change])]),
            events,
            |_| {
                lookups += 1;
                None
            },
        );
        let attrs = attributes_for_pool(&changes, &pool);

        assert_eq!(lookups, 0);
        assert_eq!(attr_value(&attrs, "protocol_fees/token0"), BigInt::from(3));
        assert_eq!(attr_value(&attrs, "protocol_fees/token1"), BigInt::from(4));
    }

    #[test]
    fn uses_store_lookup_for_fee_storage_without_matching_event_and_caches_misses() {
        let unknown_pool = pool_bytes(9);
        let storage_change = protocol_fee_storage_change(unknown_pool.clone(), 10, 1, 2, 3, 4);
        let events = Events { pool_events: vec![] };
        let mut lookups = 0;

        let changes = collect_pool_attribute_changes(
            &block(43_005_492, vec![tx_trace(1, vec![storage_change.clone(), storage_change])]),
            events,
            |_| {
                lookups += 1;
                None::<Pool>
            },
        );

        assert_eq!(lookups, 1);
        assert!(changes.changes.is_empty());
    }

    fn attributes_for_pool(
        changes: &tycho_substreams::prelude::BlockEntityChanges,
        pool: &str,
    ) -> Vec<tycho_substreams::prelude::Attribute> {
        let component_id = format!("0x{pool}");
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
            .filter(|attr| attr.name == name)
            .last()
            .unwrap_or_else(|| panic!("missing attribute {name}"));
        assert_eq!(attr.change, i32::from(ChangeType::Update));
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
            calls: vec![eth::Call { storage_changes, ..Default::default() }],
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

    fn initialize_event(
        pool_address: String,
        ordinal: u64,
        sqrt_price: u64,
        tick: i32,
    ) -> PoolEvent {
        PoolEvent {
            pool_address,
            log_ordinal: ordinal,
            transaction: Some(tx(ordinal)),
            r#type: Some(Type::Initialize(pool_event::Initialize {
                sqrt_price: sqrt_price.to_string(),
                tick,
            })),
            ..Default::default()
        }
    }

    fn swap_event(pool_address: String, ordinal: u64, sqrt_price: u64, tick: i32) -> PoolEvent {
        PoolEvent {
            pool_address,
            log_ordinal: ordinal,
            transaction: Some(tx(ordinal)),
            r#type: Some(Type::Swap(pool_event::Swap {
                sqrt_price: sqrt_price.to_string(),
                tick,
                liquidity: "0".to_string(),
                amount_0: "0".to_string(),
                amount_1: "0".to_string(),
                sender: String::new(),
                recipient: String::new(),
            })),
            ..Default::default()
        }
    }

    fn set_fee_protocol_event(
        pool_address: String,
        ordinal: u64,
        token0_fee: u64,
        token1_fee: u64,
    ) -> PoolEvent {
        PoolEvent {
            pool_address,
            log_ordinal: ordinal,
            transaction: Some(tx(ordinal)),
            r#type: Some(Type::SetFeeProtocol(pool_event::SetFeeProtocol {
                fee_protocol_0_new: token0_fee,
                fee_protocol_1_new: token1_fee,
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    fn tx(index: u64) -> Transaction {
        Transaction { index, hash: vec![index as u8; 32], ..Default::default() }
    }

    fn pool_address(seed: u8) -> String {
        hex::encode(pool_bytes(seed))
    }

    fn pool_bytes(seed: u8) -> Vec<u8> {
        vec![seed; 20]
    }
}
