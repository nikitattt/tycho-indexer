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
    let ticks_deltas = events
        .pool_events
        .into_iter()
        .flat_map(event_to_ticks_deltas)
        .collect();

    Ok(TickDeltas { deltas: ticks_deltas })
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

fn event_to_ticks_deltas(event: PoolEvent) -> Vec<TickDelta> {
    match event.r#type.unwrap() {
        pool_event::Type::Mint(mint) => {
            vec![
                TickDelta {
                    pool_address: event.pool_address.clone(),
                    tick_index: mint.tick_lower,
                    liquidity_net_delta: mint.amount.clone(),
                    ordinal: event.log_ordinal,
                    transaction: event.transaction.clone(),
                },
                TickDelta {
                    pool_address: event.pool_address,
                    tick_index: mint.tick_upper,
                    liquidity_net_delta: BigInt::from_signed_bytes_be(&mint.amount)
                        .neg()
                        .to_signed_bytes_be(),
                    ordinal: event.log_ordinal,
                    transaction: event.transaction,
                },
            ]
        }
        pool_event::Type::Burn(burn) => vec![
            TickDelta {
                pool_address: event.pool_address.clone(),
                tick_index: burn.tick_lower,
                liquidity_net_delta: BigInt::from_signed_bytes_be(&burn.amount)
                    .neg()
                    .to_signed_bytes_be(),
                ordinal: event.log_ordinal,
                transaction: event.transaction.clone(),
            },
            TickDelta {
                pool_address: event.pool_address,
                tick_index: burn.tick_upper,
                liquidity_net_delta: burn.amount,
                ordinal: event.log_ordinal,
                transaction: event.transaction,
            },
        ],
        _ => vec![],
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
