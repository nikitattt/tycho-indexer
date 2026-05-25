use crate::pb::uniswap::v3::{BlockMetadata, BlockPoolData, LiquidityChanges, TickDeltas};
use itertools::Itertools;
use rustc_hash::FxHashMap;
use std::str::{self, FromStr};
use substreams::{pb::substreams::StoreDeltas, scalar::BigInt};
use substreams_helper::hex::Hexable;
use tycho_substreams::{balances::aggregate_balances_changes, prelude::*};

#[substreams::handlers::map]
pub fn map_protocol_changes(
    pool_data: BlockPoolData,
    created_pools: BlockEntityChanges,
    pool_event_attribute_changes: BlockEntityChanges,
    pool_protocol_fee_changes: BlockEntityChanges,
    balances_map_deltas: BlockBalanceDeltas,
    balances_store_deltas: StoreDeltas,
    ticks_map_deltas: TickDeltas,
    ticks_store_deltas: StoreDeltas,
    pool_liquidity_changes: LiquidityChanges,
    pool_liquidity_store_deltas: StoreDeltas,
) -> Result<BlockChanges, substreams::errors::Error> {
    // We merge contract changes by transaction (identified by transaction index) making it easy to
    //  sort them at the very end.
    let mut transaction_changes: FxHashMap<u64, TransactionChangesBuilder> = FxHashMap::default();

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

    add_pool_attribute_changes(pool_event_attribute_changes, &mut transaction_changes);
    add_pool_attribute_changes(pool_protocol_fee_changes, &mut transaction_changes);

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
            let new_value_bigint = bigint_from_store_value(&store_delta.new_value);

            // If old value is empty or the int value is 0, it's considered as a creation.
            let is_creation = store_delta.old_value.is_empty()
                || bigint_from_store_value(&store_delta.old_value).is_zero();
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
            let new_value_bigint = liquidity_bigint_from_store_value(&store_delta.new_value);
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

    Ok(BlockChanges {
        block: pool_data.block.map(block_from_metadata),
        changes: transaction_changes
            .drain()
            .sorted_unstable_by_key(|(index, _)| *index)
            .filter_map(|(_, builder)| builder.build())
            .collect::<Vec<_>>(),
        ..Default::default()
    })
}

fn add_pool_attribute_changes(
    pool_attribute_changes: BlockEntityChanges,
    transaction_changes: &mut FxHashMap<u64, TransactionChangesBuilder>,
) {
    for change in pool_attribute_changes.changes {
        let tx = change.tx.as_ref().unwrap();
        let builder = transaction_changes
            .entry(tx.index)
            .or_insert_with(|| TransactionChangesBuilder::new(tx));
        change
            .entity_changes
            .iter()
            .for_each(|ec| {
                builder.add_entity_change(ec);
            });
    }
}

fn block_from_metadata(block: BlockMetadata) -> Block {
    Block {
        hash: block.hash,
        parent_hash: block.parent_hash,
        number: block.number,
        ts: block.timestamp,
    }
}

fn bigint_from_store_value(value: &[u8]) -> BigInt {
    BigInt::from_str(str::from_utf8(value).unwrap()).unwrap()
}

fn liquidity_bigint_from_store_value(value: &[u8]) -> BigInt {
    BigInt::from_str(
        str::from_utf8(value)
            .unwrap()
            .split(':')
            .nth(1)
            .unwrap(),
    )
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_store_delta_bigints_without_string_allocation() {
        assert_eq!(bigint_from_store_value(b"-17"), BigInt::from(-17));
        assert_eq!(liquidity_bigint_from_store_value(b"set:12345"), BigInt::from(12_345));
    }
}
