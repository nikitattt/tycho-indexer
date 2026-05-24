use crate::{
    abi::pool::events::{
        Burn, Collect, CollectProtocol, Flash, Initialize, Mint, SetFeeProtocol, Swap,
    },
    modules::pool_key,
    pb::uniswap::v3::{
        events::{
            pool_event::{self, Type},
            PoolEvent,
        },
        Events, Pool,
    },
};
use anyhow::Ok;
use std::collections::{hash_map::Entry, HashMap};
use substreams::{
    store::{StoreGet, StoreGetProto},
    Hex,
};
use substreams_ethereum::{
    pb::eth::v2::{self as eth, Log, TransactionTrace},
    Event,
};

#[substreams::handlers::map]
pub fn map_events(
    block: eth::Block,
    pools_store: StoreGetProto<Pool>,
) -> Result<Events, anyhow::Error> {
    let mut pool_events = Vec::new();
    let mut pool_cache = PoolLookupCache::default();

    for tx in block
        .transaction_traces
        .into_iter()
        .filter(|tx| tx.status == 1)
    {
        let receipt = tx
            .receipt
            .as_ref()
            .expect("all transaction traces have a receipt");

        for log in &receipt.logs {
            if let Some(event) = event_from_known_pool_log(log, &tx, &mut pool_cache, |address| {
                pools_store.get_last(pool_key(address))
            }) {
                pool_events.push(event);
            }
        }
    }

    pool_events.sort_unstable_by_key(|e| e.log_ordinal);

    Ok(Events { pool_events })
}

#[derive(Default)]
struct PoolLookupCache {
    pools: HashMap<Vec<u8>, Option<Pool>>,
}

impl PoolLookupCache {
    fn get_or_lookup<F>(&mut self, address: &[u8], mut lookup_pool: F) -> Option<&Pool>
    where
        F: FnMut(&[u8]) -> Option<Pool>,
    {
        match self.pools.entry(address.to_vec()) {
            Entry::Occupied(entry) => entry.into_mut().as_ref(),
            Entry::Vacant(entry) => entry
                .insert(lookup_pool(address))
                .as_ref(),
        }
    }
}

fn event_from_known_pool_log<F>(
    log: &Log,
    tx: &TransactionTrace,
    pool_cache: &mut PoolLookupCache,
    lookup_pool: F,
) -> Option<PoolEvent>
where
    F: FnMut(&[u8]) -> Option<Pool>,
{
    if !is_v3_pool_event_log(log) {
        return None;
    }

    let pool = pool_cache.get_or_lookup(&log.address, lookup_pool)?;
    log_to_event(log, pool, tx)
}

fn is_v3_pool_event_log(log: &Log) -> bool {
    Initialize::match_log(log)
        || Swap::match_log(log)
        || Mint::match_log(log)
        || Burn::match_log(log)
        || Collect::match_log(log)
        || Flash::match_log(log)
        || SetFeeProtocol::match_log(log)
        || CollectProtocol::match_log(log)
}

fn log_to_event(event: &Log, pool: &Pool, tx: &TransactionTrace) -> Option<PoolEvent> {
    if let Some(init) = Initialize::match_and_decode(event) {
        Some(pool_event(
            event,
            pool,
            tx,
            Type::Initialize(pool_event::Initialize {
                sqrt_price: init.sqrt_price_x96.to_string(),
                tick: init.tick.into(),
            }),
        ))
    } else if let Some(swap) = Swap::match_and_decode(event) {
        Some(pool_event(
            event,
            pool,
            tx,
            Type::Swap(pool_event::Swap {
                sender: Hex(swap.sender).to_string(),
                recipient: Hex(swap.recipient).to_string(),
                amount_0: swap.amount0.to_string(),
                amount_1: swap.amount1.to_string(),
                sqrt_price: swap.sqrt_price_x96.to_string(),
                liquidity: swap.liquidity.to_string(),
                tick: swap.tick.into(),
            }),
        ))
    } else if let Some(flash) = Flash::match_and_decode(event) {
        Some(pool_event(
            event,
            pool,
            tx,
            Type::Flash(pool_event::Flash {
                sender: Hex(flash.sender).to_string(),
                recipient: Hex(flash.recipient).to_string(),
                amount_0: flash.amount0.to_string(),
                amount_1: flash.amount1.to_string(),
                paid_0: flash.paid0.to_string(),
                paid_1: flash.paid1.to_string(),
            }),
        ))
    } else if let Some(mint) = Mint::match_and_decode(event) {
        Some(pool_event(
            event,
            pool,
            tx,
            Type::Mint(pool_event::Mint {
                sender: Hex(mint.sender).to_string(),
                owner: Hex(mint.owner).to_string(),
                tick_lower: mint.tick_lower.into(),
                tick_upper: mint.tick_upper.into(),
                amount: mint.amount.to_string(),
                amount_0: mint.amount0.to_string(),
                amount_1: mint.amount1.to_string(),
            }),
        ))
    } else if let Some(burn) = Burn::match_and_decode(event) {
        Some(pool_event(
            event,
            pool,
            tx,
            Type::Burn(pool_event::Burn {
                owner: Hex(burn.owner).to_string(),
                tick_lower: burn.tick_lower.into(),
                tick_upper: burn.tick_upper.into(),
                amount: burn.amount.to_string(),
                amount_0: burn.amount0.to_string(),
                amount_1: burn.amount1.to_string(),
            }),
        ))
    } else if let Some(collect) = Collect::match_and_decode(event) {
        Some(pool_event(
            event,
            pool,
            tx,
            Type::Collect(pool_event::Collect {
                owner: Hex(collect.owner).to_string(),
                recipient: Hex(collect.recipient).to_string(),
                tick_lower: collect.tick_lower.into(),
                tick_upper: collect.tick_upper.into(),
                amount_0: collect.amount0.to_string(),
                amount_1: collect.amount1.to_string(),
            }),
        ))
    } else if let Some(set_fp) = SetFeeProtocol::match_and_decode(event) {
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
    } else if let Some(cp) = CollectProtocol::match_and_decode(event) {
        Some(pool_event(
            event,
            pool,
            tx,
            Type::CollectProtocol(pool_event::CollectProtocol {
                sender: Hex(cp.sender).to_string(),
                recipient: Hex(cp.recipient).to_string(),
                amount_0: cp.amount0.to_string(),
                amount_1: cp.amount1.to_string(),
            }),
        ))
    } else {
        None
    }
}

fn pool_event(event: &Log, pool: &Pool, tx: &TransactionTrace, r#type: Type) -> PoolEvent {
    PoolEvent {
        log_ordinal: event.ordinal,
        pool_address: Hex(pool.address.clone()).to_string(),
        token0: Hex(pool.token0.clone()).to_string(),
        token1: Hex(pool.token1.clone()).to_string(),
        transaction: Some(tx.into()),
        r#type: Some(r#type),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

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
        assert!(is_v3_pool_event_log(&swap_log(pool_address(1), 1)));

        let malformed_swap = Log {
            address: pool_address(1),
            topics: vec![SWAP_TOPIC.to_vec()],
            data: vec![0; 160],
            ordinal: 1,
            ..Default::default()
        };
        assert!(!is_v3_pool_event_log(&malformed_swap));
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
