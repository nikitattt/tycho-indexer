pub use map_pool_events_with_extra::map_pool_events_with_extra;
pub use map_v2_extra_changes::map_v2_extra_changes;

#[path = "1_map_v2_extra_changes.rs"]
mod map_v2_extra_changes;

#[path = "2_map_pool_events_with_extra.rs"]
mod map_pool_events_with_extra;

fn pool_key(pool: &[u8]) -> String {
    format!("Pool:0x{}", hex::encode(pool))
}
