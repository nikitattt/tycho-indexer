pub use map_protocol_fee_changes::map_protocol_fee_changes;
pub use map_protocol_fee_pool_candidates::map_protocol_fee_pool_candidates;
pub use store_protocol_fee_pools::store_protocol_fee_pools;

#[path = "1_map_protocol_fee_pool_candidates.rs"]
mod map_protocol_fee_pool_candidates;

#[path = "2_store_protocol_fee_pools.rs"]
mod store_protocol_fee_pools;

#[path = "3_map_protocol_fee_changes.rs"]
mod map_protocol_fee_changes;

fn pool_key(pool: &[u8]) -> String {
    format!("Pool:0x{}", hex::encode(pool))
}

fn fee_component_id(pool: &[u8]) -> String {
    format!("v3-fees:0x{}", hex::encode(pool))
}
