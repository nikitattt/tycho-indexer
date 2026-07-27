pub use map_protocol_changes_with_fees::map_protocol_changes_with_fees;
pub use map_protocol_fee_changes::map_protocol_fee_changes;

#[path = "1_map_protocol_fee_changes.rs"]
mod map_protocol_fee_changes;

#[path = "2_map_protocol_changes_with_fees.rs"]
mod map_protocol_changes_with_fees;

fn pool_key(pool: &[u8]) -> String {
    format!("Pool:0x{}", hex::encode(pool))
}
