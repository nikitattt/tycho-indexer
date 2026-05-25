use crate::{
    pb::uniswap::v3::Events,
    storage::{protocol_fee_attributes, PROTOCOL_FEES_SLOT},
};
use itertools::Itertools;
use std::collections::{hash_map::Entry, HashMap, HashSet};
use substreams_ethereum::pb::eth::v2 as eth;
use substreams_helper::hex::Hexable;
use tycho_substreams::prelude::*;

const BASE_PROTOCOL_FEES_INITIAL_BLOCK: u64 = 43_005_492;

#[substreams::handlers::map]
pub fn map_pool_protocol_fee_changes(
    block: eth::Block,
    events: Events,
) -> Result<BlockEntityChanges, substreams::errors::Error> {
    Ok(collect_pool_protocol_fee_changes(&block, events))
}

fn collect_pool_protocol_fee_changes(block: &eth::Block, events: Events) -> BlockEntityChanges {
    let mut transaction_changes: HashMap<u64, TransactionChangesBuilder> = HashMap::new();

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

fn event_pools_by_tx(events: &Events) -> HashMap<u64, HashSet<Vec<u8>>> {
    let mut pools_by_tx = HashMap::new();

    for event in &events.pool_events {
        let Some(tx) = &event.transaction else {
            continue;
        };

        pools_by_tx
            .entry(tx.index)
            .or_insert_with(HashSet::new)
            .insert(event.pool_address.clone());
    }

    pools_by_tx
}

fn add_protocol_fee_changes(
    block: &eth::Block,
    event_pools_by_tx: &HashMap<u64, HashSet<Vec<u8>>>,
    transaction_changes: &mut HashMap<u64, TransactionChangesBuilder>,
) {
    let mut latest_attributes: HashMap<(Vec<u8>, String), PendingAttribute> = HashMap::new();

    for tx in block.transactions() {
        if tx.status != 1 {
            continue;
        }

        let Some(event_pools) = event_pools_by_tx.get(&(tx.index as u64)) else {
            continue;
        };

        let tycho_tx = Transaction {
            hash: tx.hash.clone(),
            from: tx.from.clone(),
            to: tx.to.clone(),
            index: tx.index.into(),
        };

        for storage_change in tx
            .calls()
            .filter(|call_view| {
                !call_view.call.state_reverted
                    && event_pools.contains(call_view.call.address.as_slice())
            })
            .flat_map(|call_view| call_view.call.storage_changes.iter())
            .filter(|change| {
                change.key == PROTOCOL_FEES_SLOT && event_pools.contains(change.address.as_slice())
            })
        {
            let pool = &storage_change.address;
            let attributes = protocol_fee_attributes(storage_change);

            if attributes.is_empty() {
                continue;
            }

            for attribute in attributes {
                upsert_latest_attribute(
                    &mut latest_attributes,
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

fn upsert_latest_attribute(
    latest_attributes: &mut HashMap<(Vec<u8>, String), PendingAttribute>,
    pending: PendingAttribute,
) {
    let key = (pending.pool.clone(), pending.attribute.name.clone());

    match latest_attributes.entry(key) {
        Entry::Occupied(mut entry) => {
            if pending.order > entry.get().order {
                entry.insert(pending);
            }
        }
        Entry::Vacant(entry) => {
            entry.insert(pending);
        }
    }
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
        PoolEvent {
            pool_address,
            log_ordinal: ordinal,
            transaction: Some(tx(ordinal)),
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
