use crate::pb::uniswap::v3::{
    events::{pool_event, PoolEvent},
    Events, LiquidityChanges, Pool, TickDeltas,
};
use itertools::Itertools;
use std::{collections::HashMap, str::FromStr, vec};
use substreams::{
    pb::substreams::StoreDeltas,
    scalar::BigInt,
    store::{StoreGet, StoreGetProto},
};
use substreams_ethereum::pb::eth::v2::{self as eth};
use substreams_helper::hex::Hexable;
use tycho_substreams::{balances::aggregate_balances_changes, prelude::*};

use crate::storage::{protocol_fee_attributes, PROTOCOL_FEES_SLOT};

type PoolAddress = Vec<u8>;

const BASE_PROTOCOL_FEES_INITIAL_BLOCK: u64 = 43_005_492;

#[substreams::handlers::map]
pub fn map_protocol_changes(
    block: eth::Block,
    created_pools: BlockEntityChanges,
    events: Events,
    balances_map_deltas: BlockBalanceDeltas,
    balances_store_deltas: StoreDeltas,
    ticks_map_deltas: TickDeltas,
    ticks_store_deltas: StoreDeltas,
    pool_liquidity_changes: LiquidityChanges,
    pool_liquidity_store_deltas: StoreDeltas,
    pools_store: StoreGetProto<Pool>,
) -> Result<BlockChanges, substreams::errors::Error> {
    // We merge contract changes by transaction (identified by transaction index) making it easy to
    //  sort them at the very end.
    let mut transaction_changes: HashMap<_, TransactionChangesBuilder> = HashMap::new();

    // Add created pools to the tx_changes_map
    for change in created_pools.changes.into_iter() {
        let tx = change.tx.as_ref().unwrap();
        let builder = transaction_changes
            .entry(tx.index)
            .or_insert_with(|| TransactionChangesBuilder::new(tx));
        change
            .component_changes
            .iter()
            .for_each(|c| {
                builder.add_protocol_component(c);
            });
        change
            .entity_changes
            .iter()
            .for_each(|ec| {
                builder.add_entity_change(ec);
            });
        change
            .balance_changes
            .iter()
            .for_each(|bc| {
                builder.add_balance_change(bc);
            });
    }

    // Balance changes are gathered by the `StoreDelta` based on `PoolBalanceChanged` creating
    //  `BlockBalanceDeltas`. We essentially just process the changes that occurred to the `store`
    // this  block. Then, these balance changes are merged onto the existing map of tx contract
    // changes,  inserting a new one if it doesn't exist.
    aggregate_balances_changes(balances_store_deltas, balances_map_deltas)
        .into_iter()
        .for_each(|(_, (tx, balances))| {
            let builder = transaction_changes
                .entry(tx.index)
                .or_insert_with(|| TransactionChangesBuilder::new(&tx));
            balances
                .values()
                .for_each(|token_bc_map| {
                    token_bc_map
                        .values()
                        .for_each(|bc| builder.add_balance_change(bc))
                });
        });

    // Insert ticks net-liquidity changes
    ticks_store_deltas
        .deltas
        .into_iter()
        .zip(ticks_map_deltas.deltas)
        .for_each(|(store_delta, tick_delta)| {
            let new_value_bigint =
                BigInt::from_str(&String::from_utf8(store_delta.new_value).unwrap()).unwrap();

            // If old value is empty or the int value is 0, it's considered as a creation.
            let is_creation = store_delta.old_value.is_empty()
                || BigInt::from_str(&String::from_utf8(store_delta.old_value).unwrap())
                    .unwrap()
                    .is_zero();
            let attribute_name = format!("ticks/{}/net-liquidity", tick_delta.tick_index);
            let attribute = Attribute {
                name: attribute_name,
                value: new_value_bigint.to_signed_bytes_be(),
                change: if is_creation {
                    ChangeType::Creation.into()
                } else if new_value_bigint.is_zero() {
                    ChangeType::Deletion.into()
                } else {
                    ChangeType::Update.into()
                },
            };
            let tx = tick_delta.transaction.unwrap();
            let builder = transaction_changes
                .entry(tx.index)
                .or_insert_with(|| TransactionChangesBuilder::new(&tx.into()));

            builder.add_entity_change(&EntityChanges {
                component_id: tick_delta.pool_address.to_hex(),
                attributes: vec![attribute],
            });
        });

    // Insert liquidity changes
    pool_liquidity_store_deltas
        .deltas
        .into_iter()
        .zip(pool_liquidity_changes.changes)
        .for_each(|(store_delta, change)| {
            let new_value_bigint = BigInt::from_str(
                String::from_utf8(store_delta.new_value)
                    .unwrap()
                    .split(':')
                    .nth(1)
                    .unwrap(),
            )
            .unwrap();
            let tx = change.transaction.unwrap();
            let builder = transaction_changes
                .entry(tx.index)
                .or_insert_with(|| TransactionChangesBuilder::new(&tx.into()));

            builder.add_entity_change(&EntityChanges {
                component_id: change.pool_address.to_hex(),
                attributes: vec![Attribute {
                    name: "liquidity".to_string(),
                    value: new_value_bigint.to_signed_bytes_be(),
                    change: ChangeType::Update.into(),
                }],
            });
        });

    // Insert accrued protocol fee changes. This preserves the old wrapper substream behavior:
    // fee attributes were produced by a separate Base-only map that started at this block.
    if block.number >= BASE_PROTOCOL_FEES_INITIAL_BLOCK {
        add_protocol_fee_changes(&block, &pools_store, &mut transaction_changes);
    }

    // Insert others changes
    events
        .pool_events
        .into_iter()
        .flat_map(event_to_attributes_updates)
        .for_each(|(tx, pool_address, attr)| {
            let builder = transaction_changes
                .entry(tx.index)
                .or_insert_with(|| TransactionChangesBuilder::new(&tx));
            builder.add_entity_change(&EntityChanges {
                component_id: pool_address.to_hex(),
                attributes: vec![attr],
            });
        });

    Ok(BlockChanges {
        block: Some((&block).into()),
        changes: transaction_changes
            .drain()
            .sorted_unstable_by_key(|(index, _)| *index)
            .filter_map(|(_, builder)| builder.build())
            .collect::<Vec<_>>(),
        ..Default::default()
    })
}

fn add_protocol_fee_changes(
    block: &eth::Block,
    pools_store: &StoreGetProto<Pool>,
    transaction_changes: &mut HashMap<u64, TransactionChangesBuilder>,
) {
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

            if attributes.is_empty() || !is_known_pool(pool, pools_store, &mut known_pools) {
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
        let builder = transaction_changes
            .entry(pending.tx.index)
            .or_insert_with(|| TransactionChangesBuilder::new(&pending.tx));

        builder.add_entity_change(&EntityChanges {
            component_id: pending.pool.to_hex(),
            attributes: vec![pending.attribute],
        });
    }
}

fn is_known_pool(
    address: &[u8],
    pools_store: &StoreGetProto<Pool>,
    known_pools: &mut HashMap<Vec<u8>, bool>,
) -> bool {
    if let Some(is_known) = known_pools.get(address) {
        return *is_known;
    }

    let is_known = pools_store
        .get_last(pool_key(address))
        .is_some();
    known_pools.insert(address.to_vec(), is_known);
    is_known
}

fn pool_key(pool: &[u8]) -> String {
    format!("Pool:{}", pool.to_hex())
}

struct PendingAttribute {
    tx: Transaction,
    pool: Vec<u8>,
    attribute: Attribute,
    order: (u64, u64),
}

fn event_to_attributes_updates(event: PoolEvent) -> Vec<(Transaction, PoolAddress, Attribute)> {
    match event.r#type.as_ref().unwrap() {
        pool_event::Type::Initialize(initalize) => {
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
                        value: BigInt::from_str(&initalize.sqrt_price)
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
                        value: BigInt::from(initalize.tick).to_signed_bytes_be(),
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
