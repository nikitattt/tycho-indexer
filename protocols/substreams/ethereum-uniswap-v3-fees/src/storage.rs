use hex_literal::hex;

pub const PROTOCOL_FEES_SLOT: [u8; 32] =
    hex!("0000000000000000000000000000000000000000000000000000000000000003");

pub const PROTOCOL_FEE_TOKEN0_OFFSET: usize = 0;
pub const PROTOCOL_FEE_TOKEN1_OFFSET: usize = 16;
pub const PROTOCOL_FEE_BYTES: usize = 16;

pub fn changed_protocol_fee_bytes<'a>(
    old_value: &[u8],
    new_value: &'a [u8],
    offset: usize,
) -> Option<&'a [u8]> {
    let old_data = read_slot_bytes(old_value, offset, PROTOCOL_FEE_BYTES);
    let new_data = read_slot_bytes(new_value, offset, PROTOCOL_FEE_BYTES);

    if old_data == new_data {
        None
    } else {
        Some(new_data)
    }
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
    use substreams::scalar::BigInt;

    #[test]
    fn decodes_protocol_fees_from_slot_3() {
        let old_value =
            hex!("0000000000000000000000000000000200000000000000000000000000000001").to_vec();
        let new_value =
            hex!("0000000000000000000000000000000400000000000000000000000000000003").to_vec();

        let token0 =
            changed_protocol_fee_bytes(&old_value, &new_value, PROTOCOL_FEE_TOKEN0_OFFSET).unwrap();
        let token1 =
            changed_protocol_fee_bytes(&old_value, &new_value, PROTOCOL_FEE_TOKEN1_OFFSET).unwrap();

        assert_eq!(BigInt::from_unsigned_bytes_be(token0), BigInt::from(3));
        assert_eq!(BigInt::from_unsigned_bytes_be(token1), BigInt::from(4));
    }

    #[test]
    fn skips_unchanged_packed_values() {
        let old_value =
            hex!("0000000000000000000000000000000200000000000000000000000000000001").to_vec();
        let new_value =
            hex!("0000000000000000000000000000000200000000000000000000000000000003").to_vec();

        let token0 =
            changed_protocol_fee_bytes(&old_value, &new_value, PROTOCOL_FEE_TOKEN0_OFFSET).unwrap();
        let token1 = changed_protocol_fee_bytes(&old_value, &new_value, PROTOCOL_FEE_TOKEN1_OFFSET);

        assert_eq!(BigInt::from_unsigned_bytes_be(token0), BigInt::from(3));
        assert!(token1.is_none());
    }
}
