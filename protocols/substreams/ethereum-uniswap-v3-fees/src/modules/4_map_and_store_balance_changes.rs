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
    let balance_deltas = events
        .pool_events
        .into_iter()
        .flat_map(event_to_balance_deltas)
        .collect();

    Ok(BlockBalanceDeltas { balance_deltas })
}

#[substreams::handlers::store]
pub fn store_pools_balances(balances_deltas: BlockBalanceDeltas, store: StoreAddBigInt) {
    tycho_substreams::balances::store_balance_changes(balances_deltas, store);
}

fn event_to_balance_deltas(event: PoolEvent) -> Vec<BalanceDelta> {
    let address = event
        .pool_address
        .to_hex()
        .as_bytes()
        .to_vec();
    match event.r#type.unwrap() {
        pool_event::Type::Mint(e) => vec![
            BalanceDelta {
                token: event.token0.clone(),
                delta: e.amount_0,
                component_id: address.clone(),
                ord: event.log_ordinal,
                tx: event
                    .transaction
                    .as_ref()
                    .map(Into::into),
            },
            BalanceDelta {
                token: event.token1,
                delta: e.amount_1,
                component_id: address,
                ord: event.log_ordinal,
                tx: event.transaction.map(Into::into),
            },
        ],
        pool_event::Type::Collect(e) => vec![
            BalanceDelta {
                token: event.token0.clone(),
                delta: BigInt::from_signed_bytes_be(&e.amount_0)
                    .neg()
                    .to_signed_bytes_be(),
                component_id: address.clone(),
                ord: event.log_ordinal,
                tx: event
                    .transaction
                    .as_ref()
                    .map(Into::into),
            },
            BalanceDelta {
                token: event.token1,
                delta: BigInt::from_signed_bytes_be(&e.amount_1)
                    .neg()
                    .to_signed_bytes_be(),
                component_id: address,
                ord: event.log_ordinal,
                tx: event.transaction.map(Into::into),
            },
        ],
        //Burn balance changes are accounted for in the Collect event.
        pool_event::Type::Burn(_) => vec![],
        pool_event::Type::Swap(e) => {
            vec![
                BalanceDelta {
                    token: event.token0.clone(),
                    delta: e.amount_0,
                    component_id: address.clone(),
                    ord: event.log_ordinal,
                    tx: event
                        .transaction
                        .as_ref()
                        .map(Into::into),
                },
                BalanceDelta {
                    token: event.token1,
                    delta: e.amount_1,
                    component_id: address,
                    ord: event.log_ordinal,
                    tx: event.transaction.map(Into::into),
                },
            ]
        }
        pool_event::Type::Flash(e) => vec![
            BalanceDelta {
                token: event.token0.clone(),
                delta: e.paid_0,
                component_id: address.clone(),
                ord: event.log_ordinal,
                tx: event
                    .transaction
                    .as_ref()
                    .map(Into::into),
            },
            BalanceDelta {
                token: event.token1,
                delta: e.paid_1,
                component_id: address,
                ord: event.log_ordinal,
                tx: event.transaction.map(Into::into),
            },
        ],
        pool_event::Type::CollectProtocol(e) => {
            vec![
                BalanceDelta {
                    token: event.token0.clone(),
                    delta: BigInt::from_signed_bytes_be(&e.amount_0)
                        .neg()
                        .to_signed_bytes_be(),
                    component_id: address.clone(),
                    ord: event.log_ordinal,
                    tx: event
                        .transaction
                        .as_ref()
                        .map(Into::into),
                },
                BalanceDelta {
                    token: event.token1,
                    delta: BigInt::from_signed_bytes_be(&e.amount_1)
                        .neg()
                        .to_signed_bytes_be(),
                    component_id: address,
                    ord: event.log_ordinal,
                    tx: event.transaction.map(Into::into),
                },
            ]
        }
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
