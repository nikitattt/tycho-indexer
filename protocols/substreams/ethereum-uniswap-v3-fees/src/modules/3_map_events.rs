use crate::{
    abi::pool::events::{
        Burn, Collect, CollectProtocol, Flash, Initialize, Mint, SetFeeProtocol, Swap,
    },
    pb::uniswap::v3::{
        events::{
            pool_event::{self, Type},
            PoolEvent,
        },
        BlockPoolData, Events, Pool,
    },
};
use rustc_hash::FxHashMap;
use std::collections::hash_map::Entry;
use substreams_ethereum::pb::eth::v2::{Log, TransactionTrace};

#[substreams::handlers::map]
pub fn map_events(pool_data: BlockPoolData) -> Result<Events, anyhow::Error> {
    Ok(pool_data.events.unwrap_or_default())
}

#[derive(Default)]
pub(super) struct PoolLookupCache {
    pools: FxHashMap<[u8; 20], Option<Pool>>,
}

impl PoolLookupCache {
    fn get_or_lookup<F>(&mut self, address: &[u8], mut lookup_pool: F) -> Option<&Pool>
    where
        F: FnMut(&[u8]) -> Option<Pool>,
    {
        let Some(key) = address_key(address) else {
            return None;
        };

        match self.pools.entry(key) {
            Entry::Occupied(entry) => entry.into_mut().as_ref(),
            Entry::Vacant(entry) => entry
                .insert(lookup_pool(address))
                .as_ref(),
        }
    }
}

pub(super) fn address_key(address: &[u8]) -> Option<[u8; 20]> {
    if address.len() != 20 {
        return None;
    }

    let mut key = [0u8; 20];
    key.copy_from_slice(address);
    Some(key)
}

pub(super) fn event_from_known_pool_log<F>(
    log: &Log,
    tx: &TransactionTrace,
    pool_cache: &mut PoolLookupCache,
    lookup_pool: F,
) -> Option<PoolEvent>
where
    F: FnMut(&[u8]) -> Option<Pool>,
{
    let event_kind = classify_v3_pool_event_log(log)?;

    let pool = pool_cache.get_or_lookup(&log.address, lookup_pool)?;
    log_to_event(log, event_kind, pool, tx)
}

#[derive(Clone, Copy)]
enum EventKind {
    Initialize,
    Swap,
    Flash,
    Mint,
    Burn,
    Collect,
    SetFeeProtocol,
    CollectProtocol,
}

fn classify_v3_pool_event_log(log: &Log) -> Option<EventKind> {
    if Initialize::match_log(log) {
        Some(EventKind::Initialize)
    } else if Swap::match_log(log) {
        Some(EventKind::Swap)
    } else if Flash::match_log(log) {
        Some(EventKind::Flash)
    } else if Mint::match_log(log) {
        Some(EventKind::Mint)
    } else if Burn::match_log(log) {
        Some(EventKind::Burn)
    } else if Collect::match_log(log) {
        Some(EventKind::Collect)
    } else if SetFeeProtocol::match_log(log) {
        Some(EventKind::SetFeeProtocol)
    } else if CollectProtocol::match_log(log) {
        Some(EventKind::CollectProtocol)
    } else {
        None
    }
}

fn log_to_event(
    event: &Log,
    event_kind: EventKind,
    pool: &Pool,
    tx: &TransactionTrace,
) -> Option<PoolEvent> {
    match event_kind {
        EventKind::Initialize => {
            let init = Initialize::decode(event).ok()?;
            Some(pool_event(
                event,
                pool,
                tx,
                Type::Initialize(pool_event::Initialize {
                    sqrt_price: init.sqrt_price_x96.to_signed_bytes_be(),
                    tick: init.tick.into(),
                }),
            ))
        }
        EventKind::Swap => {
            let swap = Swap::decode(event).ok()?;
            Some(pool_event(
                event,
                pool,
                tx,
                Type::Swap(pool_event::Swap {
                    sender: swap.sender,
                    recipient: swap.recipient,
                    amount_0: swap.amount0.to_signed_bytes_be(),
                    amount_1: swap.amount1.to_signed_bytes_be(),
                    sqrt_price: swap.sqrt_price_x96.to_signed_bytes_be(),
                    liquidity: swap.liquidity.to_signed_bytes_be(),
                    tick: swap.tick.into(),
                }),
            ))
        }
        EventKind::Flash => {
            let flash = Flash::decode(event).ok()?;
            Some(pool_event(
                event,
                pool,
                tx,
                Type::Flash(pool_event::Flash {
                    sender: flash.sender,
                    recipient: flash.recipient,
                    amount_0: flash.amount0.to_signed_bytes_be(),
                    amount_1: flash.amount1.to_signed_bytes_be(),
                    paid_0: flash.paid0.to_signed_bytes_be(),
                    paid_1: flash.paid1.to_signed_bytes_be(),
                }),
            ))
        }
        EventKind::Mint => {
            let mint = Mint::decode(event).ok()?;
            Some(pool_event(
                event,
                pool,
                tx,
                Type::Mint(pool_event::Mint {
                    sender: mint.sender,
                    owner: mint.owner,
                    tick_lower: mint.tick_lower.into(),
                    tick_upper: mint.tick_upper.into(),
                    amount: mint.amount.to_signed_bytes_be(),
                    amount_0: mint.amount0.to_signed_bytes_be(),
                    amount_1: mint.amount1.to_signed_bytes_be(),
                }),
            ))
        }
        EventKind::Burn => {
            let burn = Burn::decode(event).ok()?;
            Some(pool_event(
                event,
                pool,
                tx,
                Type::Burn(pool_event::Burn {
                    owner: burn.owner,
                    tick_lower: burn.tick_lower.into(),
                    tick_upper: burn.tick_upper.into(),
                    amount: burn.amount.to_signed_bytes_be(),
                    amount_0: burn.amount0.to_signed_bytes_be(),
                    amount_1: burn.amount1.to_signed_bytes_be(),
                }),
            ))
        }
        EventKind::Collect => {
            let collect = Collect::decode(event).ok()?;
            Some(pool_event(
                event,
                pool,
                tx,
                Type::Collect(pool_event::Collect {
                    owner: collect.owner,
                    recipient: collect.recipient,
                    tick_lower: collect.tick_lower.into(),
                    tick_upper: collect.tick_upper.into(),
                    amount_0: collect.amount0.to_signed_bytes_be(),
                    amount_1: collect.amount1.to_signed_bytes_be(),
                }),
            ))
        }
        EventKind::SetFeeProtocol => {
            let set_fp = SetFeeProtocol::decode(event).ok()?;
            Some(pool_event(
                event,
                pool,
                tx,
                Type::SetFeeProtocol(pool_event::SetFeeProtocol {
                    fee_protocol_0_old: set_fp.fee_protocol0_old.to_u64(),
                    fee_protocol_1_old: set_fp.fee_protocol1_old.to_u64(),
                    fee_protocol_0_new: set_fp.fee_protocol0_new.to_u64(),
                    fee_protocol_1_new: set_fp.fee_protocol1_new.to_u64(),
                }),
            ))
        }
        EventKind::CollectProtocol => {
            let cp = CollectProtocol::decode(event).ok()?;
            Some(pool_event(
                event,
                pool,
                tx,
                Type::CollectProtocol(pool_event::CollectProtocol {
                    sender: cp.sender,
                    recipient: cp.recipient,
                    amount_0: cp.amount0.to_signed_bytes_be(),
                    amount_1: cp.amount1.to_signed_bytes_be(),
                }),
            ))
        }
    }
}

fn pool_event(event: &Log, pool: &Pool, tx: &TransactionTrace, r#type: Type) -> PoolEvent {
    PoolEvent {
        log_ordinal: event.ordinal,
        pool_address: pool.address.clone(),
        token0: pool.token0.clone(),
        token1: pool.token1.clone(),
        transaction: Some(tx.into()),
        r#type: Some(r#type),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;
    use substreams::scalar::BigInt;

    const SWAP_TOPIC: [u8; 32] =
        hex!("c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67");
    const TRANSFER_TOPIC: [u8; 32] =
        hex!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");

    #[test]
    fn skips_non_v3_logs_before_pool_lookup() {
        let tx = tx_trace(7);
        let log = Log {
            address: pool_address(1),
            topics: vec![TRANSFER_TOPIC.to_vec(), address_topic(2), address_topic(3)],
            data: vec![0; 32],
            ordinal: 10,
            ..Default::default()
        };
        let mut cache = PoolLookupCache::default();
        let mut lookups = 0;

        let event = event_from_known_pool_log(&log, &tx, &mut cache, |_| {
            lookups += 1;
            Some(pool(1))
        });

        assert!(event.is_none());
        assert_eq!(lookups, 0);
    }

    #[test]
    fn caches_pool_lookup_hits_and_misses_per_block() {
        let tx = tx_trace(8);
        let known_log = swap_log(pool_address(1), 11);
        let unknown_log = swap_log(pool_address(9), 12);
        let mut cache = PoolLookupCache::default();
        let mut lookups = 0;

        for _ in 0..2 {
            assert!(event_from_known_pool_log(&known_log, &tx, &mut cache, |address| {
                lookups += 1;
                if address == pool_address(1).as_slice() {
                    Some(pool(1))
                } else {
                    None
                }
            })
            .is_some());
        }

        for _ in 0..2 {
            assert!(event_from_known_pool_log(&unknown_log, &tx, &mut cache, |address| {
                lookups += 1;
                if address == pool_address(1).as_slice() {
                    Some(pool(1))
                } else {
                    None
                }
            })
            .is_none());
        }

        assert_eq!(lookups, 2);
    }

    #[test]
    fn recognizes_only_supported_v3_pool_event_shapes() {
        assert!(classify_v3_pool_event_log(&swap_log(pool_address(1), 1)).is_some());

        let malformed_swap = Log {
            address: pool_address(1),
            topics: vec![SWAP_TOPIC.to_vec()],
            data: vec![0; 160],
            ordinal: 1,
            ..Default::default()
        };
        assert!(classify_v3_pool_event_log(&malformed_swap).is_none());
    }

    #[test]
    fn emits_pool_event_metadata_and_amounts_as_bytes() {
        let tx = tx_trace(8);
        let log = swap_log(pool_address(1), 11);
        let mut cache = PoolLookupCache::default();

        let event = event_from_known_pool_log(&log, &tx, &mut cache, |_| Some(pool(1))).unwrap();

        assert_eq!(event.pool_address, pool_address(1));
        assert_eq!(event.token0, pool_address(11));
        assert_eq!(event.token1, pool_address(21));

        let Type::Swap(swap) = event.r#type.unwrap() else {
            panic!("expected swap event");
        };
        assert_eq!(BigInt::from_signed_bytes_be(&swap.amount_0), BigInt::from(0));
        assert_eq!(BigInt::from_signed_bytes_be(&swap.liquidity), BigInt::from(0));
    }

    fn swap_log(address: Vec<u8>, ordinal: u64) -> Log {
        Log {
            address,
            topics: vec![SWAP_TOPIC.to_vec(), address_topic(2), address_topic(3)],
            data: vec![0; 160],
            ordinal,
            ..Default::default()
        }
    }

    fn pool(seed: u8) -> Pool {
        Pool {
            address: pool_address(seed),
            token0: pool_address(seed + 10),
            token1: pool_address(seed + 20),
            created_tx_hash: vec![seed; 32],
        }
    }

    fn tx_trace(index: u32) -> TransactionTrace {
        TransactionTrace {
            index,
            hash: vec![index as u8; 32],
            from: pool_address(7),
            to: pool_address(8),
            ..Default::default()
        }
    }

    fn pool_address(seed: u8) -> Vec<u8> {
        vec![seed; 20]
    }

    fn address_topic(seed: u8) -> Vec<u8> {
        let mut topic = vec![0; 12];
        topic.extend(pool_address(seed));
        topic
    }
}
