use std::collections::{HashMap, HashSet};

use substreams_ethereum::{pb::eth::v2 as eth, Event};

use crate::{abi::pool::events::SetFeeProtocol, pb::fees::ProtocolFeePoolCandidates};

#[substreams::handlers::map]
pub fn map_protocol_fee_pool_candidates(
    block: eth::Block,
) -> Result<ProtocolFeePoolCandidates, substreams::errors::Error> {
    let mut pool_addresses = find_set_fee_protocol_pools(&block)
        .into_iter()
        .collect::<Vec<_>>();
    pool_addresses.sort_unstable();

    Ok(ProtocolFeePoolCandidates { pool_addresses })
}

pub(crate) fn find_set_fee_protocol_pools(block: &eth::Block) -> HashSet<Vec<u8>> {
    find_set_fee_protocol_pool_ordinals(block)
        .into_keys()
        .collect()
}

pub(crate) fn find_set_fee_protocol_pool_ordinals(block: &eth::Block) -> HashMap<Vec<u8>, u64> {
    let mut ordinals = HashMap::new();

    for tx in block.transactions() {
        if tx.status != 1 {
            continue;
        }

        for (log, call_view) in tx.logs_with_calls() {
            if call_view.call.state_reverted {
                continue;
            }
            if SetFeeProtocol::match_and_decode(log).is_some() {
                ordinals
                    .entry(log.address.clone())
                    .and_modify(|ordinal| {
                        if log.ordinal < *ordinal {
                            *ordinal = log.ordinal;
                        }
                    })
                    .or_insert(log.ordinal);
            }
        }
    }

    ordinals
}
