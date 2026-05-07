use hex_literal::hex;
use substreams::scalar::BigInt;
use substreams_ethereum::pb::eth::v2::StorageChange;
use tycho_substreams::prelude::{Attribute, ChangeType};

pub const PROTOCOL_FEES_SLOT: [u8; 32] =
    hex!("0000000000000000000000000000000000000000000000000000000000000003");

const TOKEN0_OFFSET: usize = 0;
const TOKEN1_OFFSET: usize = 16;
const PROTOCOL_FEE_BYTES: usize = 16;

pub fn protocol_fee_attributes(storage_changes: &[StorageChange], pool: &[u8]) -> Vec<Attribute> {
    let mut attributes = Vec::new();

    for change in storage_changes {
        if change.address != pool || change.key != PROTOCOL_FEES_SLOT {
            continue;
        }

        push_protocol_fee_attribute(
            &mut attributes,
            "protocol_fees_accrued/token0",
            &change.old_value,
            &change.new_value,
            TOKEN0_OFFSET,
        );
        push_protocol_fee_attribute(
            &mut attributes,
            "protocol_fees_accrued/token1",
            &change.old_value,
            &change.new_value,
            TOKEN1_OFFSET,
        );
    }

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

pub fn read_slot_bytes(buf: &[u8], offset: usize, number_of_bytes: usize) -> &[u8] {
    let buf_length = buf.len();
    if buf_length < number_of_bytes {
        panic!(
            "attempting to read {number_of_bytes} bytes in buffer size {buf_size}",
            buf_size = buf.len()
        )
    }

    if offset > (buf_length - 1) {
        panic!("offset {offset} exceeds buffer size {buf_size}", buf_size = buf.len())
    }

    let end = buf_length - 1 - offset;
    let start = (end + 1)
        .checked_sub(number_of_bytes)
        .unwrap_or_else(|| {
            panic!(
                "number of bytes {number_of_bytes} with offset {offset} exceeds buffer size {buf_size}",
                buf_size = buf.len()
            )
        });

    &buf[start..=end]
}

#[cfg(test)]
mod tests {
    use super::*;
    use substreams_ethereum::pb::eth::v2::StorageChange;

    #[test]
    fn decodes_protocol_fees_from_slot_3() {
        let pool = hex!("1111111111111111111111111111111111111111").to_vec();
        let old_value =
            hex!("0000000000000000000000000000000200000000000000000000000000000001").to_vec();
        let new_value =
            hex!("0000000000000000000000000000000400000000000000000000000000000003").to_vec();

        let attributes = protocol_fee_attributes(
            &[StorageChange {
                address: pool.clone(),
                key: PROTOCOL_FEES_SLOT.to_vec(),
                old_value,
                new_value,
                ..Default::default()
            }],
            &pool,
        );

        assert_eq!(attributes.len(), 2);
        assert_eq!(attributes[0].name, "protocol_fees_accrued/token0");
        assert_eq!(BigInt::from_signed_bytes_be(&attributes[0].value), BigInt::from(3));
        assert_eq!(attributes[1].name, "protocol_fees_accrued/token1");
        assert_eq!(BigInt::from_signed_bytes_be(&attributes[1].value), BigInt::from(4));
    }

    #[test]
    fn skips_unchanged_packed_values() {
        let pool = hex!("1111111111111111111111111111111111111111").to_vec();
        let old_value =
            hex!("0000000000000000000000000000000200000000000000000000000000000001").to_vec();
        let new_value =
            hex!("0000000000000000000000000000000200000000000000000000000000000003").to_vec();

        let attributes = protocol_fee_attributes(
            &[StorageChange {
                address: pool.clone(),
                key: PROTOCOL_FEES_SLOT.to_vec(),
                old_value,
                new_value,
                ..Default::default()
            }],
            &pool,
        );

        assert_eq!(attributes.len(), 1);
        assert_eq!(attributes[0].name, "protocol_fees_accrued/token0");
        assert_eq!(BigInt::from_signed_bytes_be(&attributes[0].value), BigInt::from(3));
    }
}
