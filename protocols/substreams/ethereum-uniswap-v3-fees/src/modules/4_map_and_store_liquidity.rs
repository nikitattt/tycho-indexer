use std::collections::{hash_map::Entry, HashMap};

use substreams::store::{
    StoreGet, StoreGetInt64, StoreSet, StoreSetInt64, StoreSetSum, StoreSetSumBigInt,
};

use crate::pb::uniswap::v3::{
    events::{pool_event, PoolEvent},
    Events, LiquidityChange, LiquidityChangeType, LiquidityChanges,
};

use substreams::{scalar::BigInt, store::StoreNew};

use anyhow::Ok;

#[substreams::handlers::store]
pub fn store_pool_current_tick(events: Events, store: StoreSetInt64) {
    events
        .pool_events
        .into_iter()
        .filter_map(event_to_current_tick)
        .for_each(|(pool, ordinal, new_tick_index)| {
            store.set(ordinal, format!("pool:{}", hex::encode(pool)), &new_tick_index.into())
        });
}

#[substreams::handlers::map]
pub fn map_liquidity_changes(
    events: Events,
    pools_current_tick_store: StoreGetInt64,
) -> Result<LiquidityChanges, anyhow::Error> {
    Ok(liquidity_changes_from_events(events, |ordinal, pool| {
        pools_current_tick_store.get_at(ordinal, format!("pool:{}", hex::encode(pool)))
    }))
}

#[substreams::handlers::store]
pub fn store_liquidity(ticks_deltas: LiquidityChanges, store: StoreSetSumBigInt) {
    ticks_deltas
        .changes
        .iter()
        .for_each(|changes| match changes.change_type() {
            LiquidityChangeType::Delta => {
                store.sum(
                    changes.ordinal,
                    format!("pool:{0}", hex::encode(&changes.pool_address)),
                    BigInt::from_signed_bytes_be(&changes.value),
                );
            }
            LiquidityChangeType::Absolute => {
                store.set(
                    changes.ordinal,
                    format!("pool:{0}", hex::encode(&changes.pool_address)),
                    BigInt::from_signed_bytes_be(&changes.value),
                );
            }
        });
}

fn event_to_liquidity_deltas(current_tick: i64, event: PoolEvent) -> Option<LiquidityChange> {
    match event.r#type.unwrap() {
        pool_event::Type::Mint(mint) => {
            if current_tick >= mint.tick_lower.into() && current_tick < mint.tick_upper.into() {
                Some(LiquidityChange {
                    pool_address: event.pool_address,
                    value: mint.amount,
                    change_type: LiquidityChangeType::Delta.into(),
                    ordinal: event.log_ordinal,
                    transaction: Some(event.transaction.unwrap()),
                })
            } else {
                None
            }
        }
        pool_event::Type::Burn(burn) => {
            if current_tick >= burn.tick_lower.into() && current_tick < burn.tick_upper.into() {
                Some(LiquidityChange {
                    pool_address: event.pool_address,
                    value: BigInt::from_signed_bytes_be(&burn.amount)
                        .neg()
                        .to_signed_bytes_be(),
                    change_type: LiquidityChangeType::Delta.into(),
                    ordinal: event.log_ordinal,
                    transaction: Some(event.transaction.unwrap()),
                })
            } else {
                None
            }
        }
        pool_event::Type::Swap(swap) => Some(LiquidityChange {
            pool_address: event.pool_address,
            value: swap.liquidity,
            change_type: LiquidityChangeType::Absolute.into(),
            ordinal: event.log_ordinal,
            transaction: Some(event.transaction.unwrap()),
        }),
        _ => None,
    }
}

fn event_to_current_tick(event: PoolEvent) -> Option<(Vec<u8>, u64, i32)> {
    match event.r#type.unwrap() {
        pool_event::Type::Initialize(initialize) => {
            Some((event.pool_address, event.log_ordinal, initialize.tick))
        }
        pool_event::Type::Swap(swap) => Some((event.pool_address, event.log_ordinal, swap.tick)),
        _ => None,
    }
}

fn liquidity_changes_from_events<F>(events: Events, mut current_tick_at: F) -> LiquidityChanges
where
    F: FnMut(u64, &[u8]) -> Option<i64>,
{
    let mut pool_events = events.pool_events;
    let mut current_ticks = HashMap::new();
    let mut changes = Vec::new();

    pool_events.sort_unstable_by_key(|event| event.log_ordinal);

    for event in pool_events {
        if let Some(tick) = event_current_tick(&event) {
            current_ticks.insert(event.pool_address.clone(), tick);
        }

        match event.r#type.as_ref().unwrap() {
            pool_event::Type::Swap(_) => {
                if let Some(change) = event_to_liquidity_deltas(0, event) {
                    changes.push(change);
                }
            }
            pool_event::Type::Mint(_) | pool_event::Type::Burn(_) => {
                let pool = event.pool_address.clone();
                let current_tick = match current_ticks.entry(pool.clone()) {
                    Entry::Occupied(entry) => *entry.get(),
                    Entry::Vacant(entry) => {
                        *entry.insert(current_tick_at(event.log_ordinal, &pool).unwrap_or(0))
                    }
                };

                if let Some(change) = event_to_liquidity_deltas(current_tick, event) {
                    changes.push(change);
                }
            }
            _ => {}
        }
    }

    changes.sort_unstable_by_key(|change| change.ordinal);
    LiquidityChanges { changes }
}

fn event_current_tick(event: &PoolEvent) -> Option<i64> {
    match event.r#type.as_ref().unwrap() {
        pool_event::Type::Initialize(initialize) => Some(initialize.tick.into()),
        pool_event::Type::Swap(swap) => Some(swap.tick.into()),
        _ => None,
    }
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
    fn swap_liquidity_change_does_not_read_current_tick_store() {
        let events = Events { pool_events: vec![swap_event(pool_address(1), 10, 5, 100)] };
        let mut tick_reads = 0;

        let changes = liquidity_changes_from_events(events, |_, _| {
            tick_reads += 1;
            Some(5)
        });

        assert_eq!(tick_reads, 0);
        assert_eq!(changes.changes.len(), 1);
        assert_eq!(changes.changes[0].change_type(), LiquidityChangeType::Absolute);
    }

    #[test]
    fn mint_reuses_tick_from_prior_swap_without_store_read() {
        let pool = pool_address(1);
        let events = Events {
            pool_events: vec![
                swap_event(pool.clone(), 10, 5, 100),
                mint_event(pool, 11, 0, 10, 25),
            ],
        };
        let mut tick_reads = 0;

        let changes = liquidity_changes_from_events(events, |_, _| {
            tick_reads += 1;
            Some(5)
        });

        assert_eq!(tick_reads, 0);
        assert_eq!(changes.changes.len(), 2);
        assert_eq!(changes.changes[1].change_type(), LiquidityChangeType::Delta);
    }

    #[test]
    fn mint_reads_current_tick_once_per_pool_when_not_cached() {
        let pool = pool_address(1);
        let events = Events {
            pool_events: vec![
                mint_event(pool.clone(), 10, 0, 10, 25),
                mint_event(pool, 11, 0, 10, 30),
            ],
        };
        let mut tick_reads = 0;

        let changes = liquidity_changes_from_events(events, |_, _| {
            tick_reads += 1;
            Some(5)
        });

        assert_eq!(tick_reads, 1);
        assert_eq!(changes.changes.len(), 2);
    }

    fn swap_event(pool_address: Vec<u8>, ordinal: u64, tick: i32, liquidity: i64) -> PoolEvent {
        PoolEvent {
            pool_address,
            log_ordinal: ordinal,
            transaction: Some(tx(ordinal)),
            r#type: Some(Type::Swap(pool_event::Swap {
                tick,
                liquidity: amount(liquidity),
                amount_0: amount(0),
                amount_1: amount(0),
                sqrt_price: amount(0),
                sender: Vec::new(),
                recipient: Vec::new(),
            })),
            ..Default::default()
        }
    }

    fn mint_event(
        pool_address: Vec<u8>,
        ordinal: u64,
        tick_lower: i32,
        tick_upper: i32,
        value: i64,
    ) -> PoolEvent {
        PoolEvent {
            pool_address,
            log_ordinal: ordinal,
            transaction: Some(tx(ordinal)),
            r#type: Some(Type::Mint(pool_event::Mint {
                tick_lower,
                tick_upper,
                amount: amount(value),
                amount_0: amount(0),
                amount_1: amount(0),
                sender: Vec::new(),
                owner: Vec::new(),
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

    fn pool_address(seed: u8) -> Vec<u8> {
        vec![seed; 20]
    }
}
