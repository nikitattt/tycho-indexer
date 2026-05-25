use substreams::store::StoreAddBigInt;

use crate::pb::uniswap::v3::{
    events::{pool_event, PoolEvent},
    Events, TickDelta, TickDeltas,
};

use substreams::{
    scalar::BigInt,
    store::{StoreAdd, StoreNew},
};

use anyhow::Ok;

#[substreams::handlers::map]
pub fn map_ticks_changes(events: Events) -> Result<TickDeltas, anyhow::Error> {
    Ok(TickDeltas { deltas: tick_deltas_from_events(events) })
}

#[substreams::handlers::store]
pub fn store_ticks_liquidity(ticks_deltas: TickDeltas, store: StoreAddBigInt) {
    let mut deltas = ticks_deltas.deltas;

    deltas.sort_unstable_by_key(|delta| delta.ordinal);

    deltas.iter().for_each(|delta| {
        store.add(
            delta.ordinal,
            format!("pool:{0}:tick:{1}", hex::encode(&delta.pool_address), delta.tick_index,),
            BigInt::from_signed_bytes_be(&delta.liquidity_net_delta),
        );
    });
}

fn tick_deltas_from_events(events: Events) -> Vec<TickDelta> {
    let mut deltas = Vec::new();

    for event in events.pool_events {
        push_tick_deltas(event, &mut deltas);
    }

    deltas
}

fn push_tick_deltas(event: PoolEvent, deltas: &mut Vec<TickDelta>) {
    let Some(event_type) = event.r#type else {
        return;
    };

    match event_type {
        pool_event::Type::Mint(mint) => {
            let negative_amount = neg(&mint.amount);
            push_tick_delta(
                deltas,
                event.pool_address.clone(),
                mint.tick_lower,
                mint.amount,
                event.log_ordinal,
                event.transaction.clone(),
            );
            push_tick_delta(
                deltas,
                event.pool_address,
                mint.tick_upper,
                negative_amount,
                event.log_ordinal,
                event.transaction,
            );
        }
        pool_event::Type::Burn(burn) => {
            let negative_amount = neg(&burn.amount);
            push_tick_delta(
                deltas,
                event.pool_address.clone(),
                burn.tick_lower,
                negative_amount,
                event.log_ordinal,
                event.transaction.clone(),
            );
            push_tick_delta(
                deltas,
                event.pool_address,
                burn.tick_upper,
                burn.amount,
                event.log_ordinal,
                event.transaction,
            );
        }
        _ => {}
    }
}

fn push_tick_delta(
    deltas: &mut Vec<TickDelta>,
    pool_address: Vec<u8>,
    tick_index: i32,
    liquidity_net_delta: Vec<u8>,
    ordinal: u64,
    transaction: Option<crate::pb::uniswap::v3::Transaction>,
) {
    deltas.push(TickDelta { pool_address, tick_index, liquidity_net_delta, ordinal, transaction });
}

fn neg(value: &[u8]) -> Vec<u8> {
    BigInt::from_signed_bytes_be(value)
        .neg()
        .to_signed_bytes_be()
}

#[cfg(test)]
fn event_to_ticks_deltas(event: PoolEvent) -> Vec<TickDelta> {
    let mut deltas = Vec::new();
    push_tick_deltas(event, &mut deltas);
    deltas
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
    fn emits_tick_deltas_from_byte_event_payloads() {
        let pool = address(1);
        let deltas = event_to_ticks_deltas(PoolEvent {
            pool_address: pool.clone(),
            log_ordinal: 10,
            transaction: Some(tx(7)),
            r#type: Some(Type::Mint(pool_event::Mint {
                sender: address(2),
                owner: address(3),
                tick_lower: -10,
                tick_upper: 10,
                amount: amount(25),
                amount_0: amount(0),
                amount_1: amount(0),
            })),
            ..Default::default()
        });

        assert_eq!(deltas.len(), 2);
        assert_eq!(deltas[0].pool_address, pool);
        assert_eq!(deltas[0].tick_index, -10);
        assert_eq!(BigInt::from_signed_bytes_be(&deltas[0].liquidity_net_delta), BigInt::from(25));
        assert_eq!(deltas[1].tick_index, 10);
        assert_eq!(BigInt::from_signed_bytes_be(&deltas[1].liquidity_net_delta), BigInt::from(-25));
    }

    #[test]
    fn emits_burn_tick_deltas_and_ignores_non_tick_events() {
        let pool = address(1);
        let burn_deltas = event_to_ticks_deltas(PoolEvent {
            pool_address: pool.clone(),
            log_ordinal: 10,
            transaction: Some(tx(7)),
            r#type: Some(Type::Burn(pool_event::Burn {
                owner: address(2),
                tick_lower: -20,
                tick_upper: 20,
                amount: amount(30),
                amount_0: amount(0),
                amount_1: amount(0),
            })),
            ..Default::default()
        });
        let swap_deltas = event_to_ticks_deltas(PoolEvent {
            pool_address: pool,
            log_ordinal: 11,
            transaction: Some(tx(7)),
            r#type: Some(Type::Swap(pool_event::Swap {
                sender: address(2),
                recipient: address(3),
                amount_0: amount(1),
                amount_1: amount(-1),
                sqrt_price: amount(100),
                liquidity: amount(50),
                tick: 0,
            })),
            ..Default::default()
        });

        assert_eq!(burn_deltas.len(), 2);
        assert!(swap_deltas.is_empty());
        assert_eq!(burn_deltas[0].tick_index, -20);
        assert_eq!(
            BigInt::from_signed_bytes_be(&burn_deltas[0].liquidity_net_delta),
            BigInt::from(-30)
        );
        assert_eq!(burn_deltas[1].tick_index, 20);
        assert_eq!(
            BigInt::from_signed_bytes_be(&burn_deltas[1].liquidity_net_delta),
            BigInt::from(30)
        );
    }

    fn tx(index: u64) -> Transaction {
        Transaction { index, hash: vec![index as u8; 32], ..Default::default() }
    }

    fn amount(value: i64) -> Vec<u8> {
        BigInt::from(value).to_signed_bytes_be()
    }

    fn address(seed: u8) -> Vec<u8> {
        vec![seed; 20]
    }
}
