use crate::pb::uniswap::v3::{
    events::{pool_event, PoolEvent},
    Events,
};
use itertools::Itertools;
use std::{collections::HashMap, str::FromStr};
use substreams::scalar::BigInt;
use substreams_helper::hex::Hexable;
use tycho_substreams::prelude::*;

#[substreams::handlers::map]
pub fn map_pool_event_attribute_changes(
    events: Events,
) -> Result<BlockEntityChanges, substreams::errors::Error> {
    Ok(collect_pool_event_attribute_changes(events))
}

fn collect_pool_event_attribute_changes(events: Events) -> BlockEntityChanges {
    let mut pool_events = events.pool_events;
    let mut transaction_changes: HashMap<u64, TransactionChangesBuilder> = HashMap::new();

    pool_events.sort_unstable_by_key(|event| event.log_ordinal);
    for event in pool_events {
        add_event_attribute_changes(event, &mut transaction_changes);
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
    event: PoolEvent,
    transaction_changes: &mut HashMap<u64, TransactionChangesBuilder>,
) {
    let Some(event_type) = event.r#type.as_ref() else {
        return;
    };

    match event_type {
        pool_event::Type::Initialize(initialize) => {
            let tx = event
                .transaction
                .as_ref()
                .unwrap()
                .into();
            let pool_address = hex::decode(&event.pool_address).unwrap();

            add_pool_attribute(
                transaction_changes,
                &tx,
                &pool_address,
                Attribute {
                    name: "sqrt_price_x96".to_string(),
                    value: BigInt::from_str(&initialize.sqrt_price)
                        .unwrap()
                        .to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            );
            add_pool_attribute(
                transaction_changes,
                &tx,
                &pool_address,
                Attribute {
                    name: "tick".to_string(),
                    value: BigInt::from(initialize.tick).to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            );
        }
        pool_event::Type::Swap(swap) => {
            let tx = event
                .transaction
                .as_ref()
                .unwrap()
                .into();
            let pool_address = hex::decode(&event.pool_address).unwrap();

            add_pool_attribute(
                transaction_changes,
                &tx,
                &pool_address,
                Attribute {
                    name: "sqrt_price_x96".to_string(),
                    value: BigInt::from_str(&swap.sqrt_price)
                        .unwrap()
                        .to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            );
            add_pool_attribute(
                transaction_changes,
                &tx,
                &pool_address,
                Attribute {
                    name: "tick".to_string(),
                    value: BigInt::from(swap.tick).to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            );
        }
        pool_event::Type::SetFeeProtocol(sfp) => {
            let tx = event
                .transaction
                .as_ref()
                .unwrap()
                .into();
            let pool_address = hex::decode(&event.pool_address).unwrap();

            add_pool_attribute(
                transaction_changes,
                &tx,
                &pool_address,
                Attribute {
                    name: "fee_protocol/token0".to_string(),
                    value: BigInt::from(sfp.fee_protocol_0_new).to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            );
            add_pool_attribute(
                transaction_changes,
                &tx,
                &pool_address,
                Attribute {
                    name: "fee_protocol/token1".to_string(),
                    value: BigInt::from(sfp.fee_protocol_1_new).to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                },
            );
        }
        _ => {}
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::uniswap::v3::{
        events::{
            pool_event::{self, Type},
            PoolEvent,
        },
        Transaction,
    };

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

        let changes = collect_pool_event_attribute_changes(events);
        let attrs = attributes_for_pool(&changes, &pool);

        assert_eq!(attr_value(&attrs, "sqrt_price_x96"), BigInt::from(456));
        assert_eq!(attr_value(&attrs, "tick"), BigInt::from(8));
        assert_eq!(attr_value(&attrs, "fee_protocol/token0"), BigInt::from(3));
        assert_eq!(attr_value(&attrs, "fee_protocol/token1"), BigInt::from(4));
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
        hex::encode(vec![seed; 20])
    }
}
