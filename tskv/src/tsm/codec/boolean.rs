use std::{cmp, convert::TryInto, error::Error};

use crate::tsm::codec::Encoding;
use integer_encoding::VarInt;
// note: encode/decode adapted from influxdb_iox
// https://github.com/influxdata/influxdb_iox/tree/main/influxdb_tsm/src/encoders

/// The header consists of one byte indicating the compression type.
const HEADER_LEN: usize = 1;
/// A bit packed format using 1 bit per boolean. This is the only available
/// boolean compression format at this time.
const BOOLEAN_COMPRESSED_BIT_PACKED: u8 = 1;

/// Encodes a slice of booleans into `dst`.
///
/// Boolean encoding uses 1 bit per value. Each compressed byte slice contains a
/// 1 byte header indicating the compression type, followed by a variable byte
/// encoded length indicating how many booleans are packed in the slice. The
/// remaining bytes contain 1 byte for every 8 boolean values encoded.
pub fn bool_bitpack_encode(
    src: &[bool],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    dst.clear();
    if src.is_empty() {
        return Ok(());
    }

    let size = HEADER_LEN + 8 + ((src.len() + 7) / 8); // Header + Num bools + bool data.
    dst.resize(size, 0);

    // Store the encoding type in the 4 high bits of the first byte
    dst[0] = BOOLEAN_COMPRESSED_BIT_PACKED << 4;

    let mut n = 8u64; // Current bit in current byte.

    // Encode the number of booleans written.
    let len_u64: u64 = src.len().try_into()?;
    let i = len_u64.encode_var(&mut dst[1..]);
    let step: u64 = (i * 8).try_into()?;
    n += step;

    for &v in src {
        let index: usize = (n >> 3).try_into()?;
        if v {
            dst[index] |= 128 >> (n & 7); // Set current bit on current byte.
        } else {
            dst[index] &= !(128 >> (n & 7)); // Clear current bit on current
                                             // byte.
        }
        n += 1;
    }

    let mut length = n >> 3;
    if n & 7 > 0 {
        length += 1; // Add an extra byte to capture overflowing bits.
    }
    let length: usize = length.try_into()?;

    dst.truncate(length);
    dst.insert(0, Encoding::BitPack as u8);

    Ok(())
}

pub fn bool_without_compress_encode(
    src: &[bool],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    dst.push(Encoding::Null as u8);
    for i in src {
        if *i {
            dst.push(1);
        } else {
            dst.push(0);
        }
    }
    Ok(())
}

/// Decodes a slice of bytes into a destination vector of `bool`s.
pub fn bool_bitpack_decode(
    src: &[u8],
    dst: &mut Vec<bool>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(());
    }

    let src = &src[1..];

    // First byte stores the encoding type, only have the bit packed format
    // currently so ignore for now.
    assert_eq!(src[0], BOOLEAN_COMPRESSED_BIT_PACKED << 4);
    let src = &src[HEADER_LEN..];

    let (count, num_bytes_read) = u64::decode_var(src).ok_or("boolean decoder: invalid count")?;

    let mut count: usize = count.try_into()?;
    let src = &src[num_bytes_read..];

    let min = src.len() * 8;

    // Shouldn't happen - TSM file was truncated/corrupted. This is what the Go code
    // does
    count = cmp::min(min, count);

    if dst.capacity() < count {
        dst.reserve_exact(count - dst.capacity());
    }

    let mut j = 0;
    for &v in src {
        let mut i = 128;
        while i > 0 && j < count {
            dst.push(v & i != 0);
            i >>= 1;
            j += 1;
        }
    }
    Ok(())
}

pub fn bool_without_compress_decode(
    src: &[u8],
    dst: &mut Vec<bool>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(());
    }

    let src = &src[1..];

    for i in src {
        if *i == 1 {
            dst.push(true);
        } else {
            dst.push(false);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_no_values() {
        let src: Vec<bool> = vec![];
        let mut dst = vec![];

        // check for error
        bool_bitpack_encode(&src, &mut dst).expect("failed to encode src");

        // verify encoded no values.
        assert_eq!(dst.len(), 0);
    }

    #[test]
    fn encode_single_true() {
        let src = vec![true];
        let mut dst = vec![];

        bool_bitpack_encode(&src, &mut dst).expect("failed to encode src");
        assert_eq!(dst[1..], vec![16, 1, 128]);
    }

    #[test]
    fn encode_single_false() {
        let src = vec![false];
        let mut dst = vec![];

        bool_bitpack_encode(&src, &mut dst).expect("failed to encode src");
        assert_eq!(dst[1..], vec![16, 1, 0]);
    }

    #[test]
    fn encode_multi_compressed() {
        let src: Vec<_> = (0..10).map(|i| i % 2 == 0).collect();
        let mut dst = vec![];

        bool_bitpack_encode(&src, &mut dst).expect("failed to encode src");
        assert_eq!(dst[1..], vec![16, 10, 170, 128]);
    }

    #[test]
    fn decode_no_values() {
        let src: Vec<u8> = vec![];
        let mut dst = vec![];

        // check for error
        bool_bitpack_decode(&src, &mut dst).expect("failed to decode src");

        // verify decoded no values.
        assert_eq!(dst.len(), 0);
    }

    #[test]
    fn decode_single_true() {
        let src = vec![0, 16, 1, 128];
        let mut dst = vec![];

        bool_bitpack_decode(&src, &mut dst).expect("failed to decode src");
        assert_eq!(dst, vec![true]);
    }

    #[test]
    fn decode_single_false() {
        let src = vec![0, 16, 1, 0];
        let mut dst = vec![];

        bool_bitpack_decode(&src, &mut dst).expect("failed to decode src");
        assert_eq!(dst, vec![false]);
    }

    #[test]
    fn decode_multi_compressed() {
        let src = vec![0, 16, 10, 170, 128];
        let mut dst = vec![];

        bool_bitpack_decode(&src, &mut dst).expect("failed to decode src");

        let expected: Vec<_> = (0..10).map(|i| i % 2 == 0).collect();
        assert_eq!(dst, expected);
    }

    #[test]
    fn test_bool_encode_decode() {
        let mut data = vec![];
        for _ in 0..100 {
            data.push(rand::random::<bool>());
        }

        let mut dst = vec![];
        let mut got = vec![];

        bool_bitpack_encode(&data, &mut dst).unwrap();
        bool_bitpack_decode(&dst, &mut got).unwrap();

        assert_eq!(data, got);
        dst.clear();
        got.clear();

        bool_without_compress_encode(&data, &mut dst).unwrap();
        bool_without_compress_decode(&dst, &mut got).unwrap();

        assert_eq!(data, got);
    }
}
