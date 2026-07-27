use hex_literal::hex;
use substreams::scalar::BigInt;
use substreams_ethereum::pb::eth::v2::StorageChange;
use tycho_substreams::prelude::{Attribute, ChangeType};

pub const PROTOCOL_FEES_SLOT: [u8; 32] =
    hex!("0000000000000000000000000000000000000000000000000000000000000003");

const TOKEN0_OFFSET: usize = 0;
const TOKEN1_OFFSET: usize = 16;
const PROTOCOL_FEE_BYTES: usize = 16;

pub fn protocol_fee_attributes(change: &StorageChange) -> Vec<Attribute> {
    if change.key.as_slice() != PROTOCOL_FEES_SLOT.as_slice() {
        return Vec::new();
    }

    let mut attributes = Vec::with_capacity(2);
    push_protocol_fee_attribute(
        &mut attributes,
        "protocol_fees/token0",
        &change.old_value,
        &change.new_value,
        TOKEN0_OFFSET,
    );
    push_protocol_fee_attribute(
        &mut attributes,
        "protocol_fees/token1",
        &change.old_value,
        &change.new_value,
        TOKEN1_OFFSET,
    );
    attributes
}

fn push_protocol_fee_attribute(
    attributes: &mut Vec<Attribute>,
    name: &str,
    old_value: &[u8],
    new_value: &[u8],
    offset: usize,
) {
    let old_data = read_slot_bytes(old_value, offset, PROTOCOL_FEE_BYTES);
    let new_data = read_slot_bytes(new_value, offset, PROTOCOL_FEE_BYTES);

    if old_data == new_data {
        return;
    }

    attributes.push(Attribute {
        name: name.to_string(),
        value: BigInt::from_unsigned_bytes_be(new_data).to_signed_bytes_be(),
        change: ChangeType::Update.into(),
    });
}

fn read_slot_bytes(buf: &[u8], offset: usize, number_of_bytes: usize) -> &[u8] {
    let buf_length = buf.len();
    if buf_length < number_of_bytes {
        panic!("attempting to read {number_of_bytes} bytes in buffer size {buf_length}");
    }
    if offset >= buf_length {
        panic!("offset {offset} exceeds buffer size {buf_length}");
    }

    let end = buf_length - offset;
    let start = end.checked_sub(number_of_bytes).unwrap_or_else(|| {
        panic!(
            "number of bytes {number_of_bytes} with offset {offset} exceeds buffer size {buf_length}"
        )
    });
    &buf[start..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_both_protocol_fee_values_from_slot_three() {
        let attributes = protocol_fee_attributes(&storage_change(1, 2, 3, 4));

        assert_eq!(attributes.len(), 2);
        assert_eq!(attributes[0].name, "protocol_fees/token0");
        assert_eq!(BigInt::from_signed_bytes_be(&attributes[0].value), BigInt::from(3));
        assert_eq!(attributes[1].name, "protocol_fees/token1");
        assert_eq!(BigInt::from_signed_bytes_be(&attributes[1].value), BigInt::from(4));
    }

    #[test]
    fn emits_only_the_packed_value_that_changed() {
        let attributes = protocol_fee_attributes(&storage_change(1, 2, 3, 2));

        assert_eq!(attributes.len(), 1);
        assert_eq!(attributes[0].name, "protocol_fees/token0");
        assert_eq!(BigInt::from_signed_bytes_be(&attributes[0].value), BigInt::from(3));
    }

    #[test]
    fn ignores_other_storage_slots() {
        let mut change = storage_change(1, 2, 3, 4);
        change.key = vec![0; 32];

        assert!(protocol_fee_attributes(&change).is_empty());
    }

    fn storage_change(
        old_token0: u128,
        old_token1: u128,
        new_token0: u128,
        new_token1: u128,
    ) -> StorageChange {
        StorageChange {
            key: PROTOCOL_FEES_SLOT.to_vec(),
            old_value: slot_value(old_token0, old_token1),
            new_value: slot_value(new_token0, new_token1),
            ..Default::default()
        }
    }

    fn slot_value(token0: u128, token1: u128) -> Vec<u8> {
        let mut value = Vec::with_capacity(32);
        value.extend_from_slice(&token1.to_be_bytes());
        value.extend_from_slice(&token0.to_be_bytes());
        value
    }
}
