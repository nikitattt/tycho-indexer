use hex_literal::hex;
use substreams::scalar::BigInt;
use substreams_ethereum::pb::eth::v2::StorageChange;
use tycho_substreams::prelude::{Attribute, ChangeType};

pub const TOTAL_SUPPLY_SLOT: [u8; 32] =
    hex!("0000000000000000000000000000000000000000000000000000000000000000");
pub const K_LAST_SLOT: [u8; 32] =
    hex!("000000000000000000000000000000000000000000000000000000000000000b");

pub const TOTAL_SUPPLY_ATTRIBUTE: &str = "total_supply";
pub const K_LAST_ATTRIBUTE: &str = "k_last";

pub fn v2_extra_attribute(change: &StorageChange) -> Option<Attribute> {
    let name = if change.key == TOTAL_SUPPLY_SLOT {
        TOTAL_SUPPLY_ATTRIBUTE
    } else if change.key == K_LAST_SLOT {
        K_LAST_ATTRIBUTE
    } else {
        return None;
    };

    if change.old_value == change.new_value {
        return None;
    }

    Some(Attribute {
        name: name.to_string(),
        value: BigInt::from_unsigned_bytes_be(&change.new_value).to_signed_bytes_be(),
        change: ChangeType::Update.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_slot_0_into_total_supply() {
        let attribute = v2_extra_attribute(&storage_change(TOTAL_SUPPLY_SLOT, 1, 42)).unwrap();

        assert_eq!(attribute.name, TOTAL_SUPPLY_ATTRIBUTE);
        assert_eq!(BigInt::from_signed_bytes_be(&attribute.value), BigInt::from(42));
        assert_eq!(attribute.change, i32::from(ChangeType::Update));
    }

    #[test]
    fn decodes_slot_11_into_k_last() {
        let attribute = v2_extra_attribute(&storage_change(K_LAST_SLOT, 1, 99)).unwrap();

        assert_eq!(attribute.name, K_LAST_ATTRIBUTE);
        assert_eq!(BigInt::from_signed_bytes_be(&attribute.value), BigInt::from(99));
        assert_eq!(attribute.change, i32::from(ChangeType::Update));
    }

    #[test]
    fn skips_unchanged_slot_values() {
        assert!(v2_extra_attribute(&storage_change(K_LAST_SLOT, 7, 7)).is_none());
    }

    fn storage_change(slot: [u8; 32], old_value: u64, new_value: u64) -> StorageChange {
        StorageChange {
            key: slot.to_vec(),
            old_value: BigInt::from(old_value).to_signed_bytes_be(),
            new_value: BigInt::from(new_value).to_signed_bytes_be(),
            ..Default::default()
        }
    }
}
