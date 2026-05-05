use substreams::store::{StoreNew, StoreSetIfNotExists, StoreSetIfNotExistsRaw};

use crate::{modules::pool_key, pb::fees::ProtocolFeePoolCandidates};

#[substreams::handlers::store]
pub fn store_protocol_fee_pools(
    candidates: ProtocolFeePoolCandidates,
    store: StoreSetIfNotExistsRaw,
) {
    for pool in candidates.pool_addresses {
        store.set_if_not_exists(0, pool_key(&pool), &pool);
    }
}
