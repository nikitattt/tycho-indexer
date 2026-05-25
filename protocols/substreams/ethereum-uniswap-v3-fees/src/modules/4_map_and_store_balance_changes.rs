use anyhow::Ok;
use substreams_helper::hex::Hexable;
use tycho_substreams::models::{BalanceDelta, BlockBalanceDeltas};

use crate::pb::uniswap::v3::{
    events::{pool_event, PoolEvent},
    Events,
};
use substreams::{
    scalar::BigInt,
    store::{StoreAddBigInt, StoreNew},
};

#[substreams::handlers::map]
pub fn map_balance_changes(events: Events) -> Result<BlockBalanceDeltas, anyhow::Error> {
    Ok(BlockBalanceDeltas { balance_deltas: balance_deltas_from_events(events) })
}

#[substreams::handlers::store]
pub fn store_pools_balances(balances_deltas: BlockBalanceDeltas, store: StoreAddBigInt) {
    tycho_substreams::balances::store_balance_changes(balances_deltas, store);
}

fn balance_deltas_from_events(events: Events) -> Vec<BalanceDelta> {
    let mut balance_deltas = Vec::new();

    for event in events.pool_events {
        push_balance_deltas(event, &mut balance_deltas);
    }

    balance_deltas
}

fn push_balance_deltas(event: PoolEvent, balance_deltas: &mut Vec<BalanceDelta>) {
    let Some(event_type) = event.r#type else {
        return;
    };

    match event_type {
        pool_event::Type::Mint(e) => push_pair_balance_deltas(
            balance_deltas,
            event.pool_address,
            event.token0,
            event.token1,
            e.amount_0,
            e.amount_1,
            event.log_ordinal,
            event.transaction,
        ),
        pool_event::Type::Collect(e) => push_pair_balance_deltas(
            balance_deltas,
            event.pool_address,
            event.token0,
            event.token1,
            neg(&e.amount_0),
            neg(&e.amount_1),
            event.log_ordinal,
            event.transaction,
        ),
        // Burn balance changes are accounted for in the Collect event.
        pool_event::Type::Burn(_) => {}
        pool_event::Type::Swap(e) => push_pair_balance_deltas(
            balance_deltas,
            event.pool_address,
            event.token0,
            event.token1,
            e.amount_0,
            e.amount_1,
            event.log_ordinal,
            event.transaction,
        ),
        pool_event::Type::Flash(e) => push_pair_balance_deltas(
            balance_deltas,
            event.pool_address,
            event.token0,
            event.token1,
            e.paid_0,
            e.paid_1,
            event.log_ordinal,
            event.transaction,
        ),
        pool_event::Type::CollectProtocol(e) => push_pair_balance_deltas(
            balance_deltas,
            event.pool_address,
            event.token0,
            event.token1,
            neg(&e.amount_0),
            neg(&e.amount_1),
            event.log_ordinal,
            event.transaction,
        ),
        _ => {}
    }
}

fn push_pair_balance_deltas(
    balance_deltas: &mut Vec<BalanceDelta>,
    pool_address: Vec<u8>,
    token0: Vec<u8>,
    token1: Vec<u8>,
    amount0: Vec<u8>,
    amount1: Vec<u8>,
    ordinal: u64,
    transaction: Option<crate::pb::uniswap::v3::Transaction>,
) {
    let component_id = pool_address.to_hex().into_bytes();
    let tx = transaction.map(Into::into);

    balance_deltas.push(BalanceDelta {
        token: token0,
        delta: amount0,
        component_id: component_id.clone(),
        ord: ordinal,
        tx: tx.clone(),
    });
    balance_deltas.push(BalanceDelta {
        token: token1,
        delta: amount1,
        component_id,
        ord: ordinal,
        tx,
    });
}

fn neg(value: &[u8]) -> Vec<u8> {
    BigInt::from_signed_bytes_be(value)
        .neg()
        .to_signed_bytes_be()
}

#[cfg(test)]
fn event_to_balance_deltas(event: PoolEvent) -> Vec<BalanceDelta> {
    let mut balance_deltas = Vec::new();
    push_balance_deltas(event, &mut balance_deltas);
    balance_deltas
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
    fn emits_balance_deltas_from_byte_event_payloads() {
        let token0 = address(2);
        let token1 = address(3);

        let deltas = event_to_balance_deltas(PoolEvent {
            pool_address: address(1),
            token0: token0.clone(),
            token1: token1.clone(),
            log_ordinal: 10,
            transaction: Some(tx(7)),
            r#type: Some(Type::Swap(pool_event::Swap {
                sender: address(4),
                recipient: address(5),
                amount_0: amount(-5),
                amount_1: amount(7),
                sqrt_price: amount(0),
                liquidity: amount(0),
                tick: 0,
            })),
        });

        assert_eq!(deltas.len(), 2);
        assert_eq!(deltas[0].token, token0);
        assert_eq!(BigInt::from_signed_bytes_be(&deltas[0].delta), BigInt::from(-5));
        assert_eq!(deltas[1].token, token1);
        assert_eq!(BigInt::from_signed_bytes_be(&deltas[1].delta), BigInt::from(7));
    }

    #[test]
    fn emits_negative_collection_deltas_and_ignores_burns() {
        let pool = address(1);
        let token0 = address(2);
        let token1 = address(3);
        let collect_protocol_deltas = event_to_balance_deltas(PoolEvent {
            pool_address: pool.clone(),
            token0: token0.clone(),
            token1: token1.clone(),
            log_ordinal: 10,
            transaction: Some(tx(7)),
            r#type: Some(Type::CollectProtocol(pool_event::CollectProtocol {
                sender: address(4),
                recipient: address(5),
                amount_0: amount(11),
                amount_1: amount(13),
            })),
        });
        let burn_deltas = event_to_balance_deltas(PoolEvent {
            pool_address: pool,
            token0: token0.clone(),
            token1: token1.clone(),
            log_ordinal: 11,
            transaction: Some(tx(7)),
            r#type: Some(Type::Burn(pool_event::Burn {
                owner: address(4),
                tick_lower: -10,
                tick_upper: 10,
                amount: amount(17),
                amount_0: amount(19),
                amount_1: amount(23),
            })),
        });

        assert_eq!(collect_protocol_deltas.len(), 2);
        assert!(burn_deltas.is_empty());
        assert_eq!(collect_protocol_deltas[0].token, token0);
        assert_eq!(
            BigInt::from_signed_bytes_be(&collect_protocol_deltas[0].delta),
            BigInt::from(-11)
        );
        assert_eq!(collect_protocol_deltas[1].token, token1);
        assert_eq!(
            BigInt::from_signed_bytes_be(&collect_protocol_deltas[1].delta),
            BigInt::from(-13)
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
