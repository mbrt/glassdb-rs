//! Custom base64 codec matching the Go `internal/data/paths` encoding.
//!
//! The alphabet is chosen so the encoded form preserves the lexicographic
//! ordering of the input bytes, which is required for object-store listing to
//! return objects in key order. Because the alphabet contains `=` (a byte the
//! standard `base64` crate reserves for padding), we cannot use that crate and
//! implement the (unpadded) codec by hand.

const ALPHABET: &[u8; 64] = b"0123456789=ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";

const fn build_rev() -> [i16; 256] {
    let mut rev = [-1i16; 256];
    let mut i = 0;
    while i < 64 {
        rev[ALPHABET[i] as usize] = i as i16;
        i += 1;
    }
    rev
}

const REV: [i16; 256] = build_rev();

/// Error returned when decoding malformed input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// A byte was not part of the encoding alphabet.
    InvalidByte(u8),
    /// The encoded length is not a valid (unpadded) base64 length.
    InvalidLength,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::InvalidByte(b) => write!(f, "invalid base64 byte: {b:#x}"),
            DecodeError::InvalidLength => write!(f, "invalid base64 length"),
        }
    }
}

impl std::error::Error for DecodeError {}

fn decode_symbol(symbol: u8) -> Result<u32, DecodeError> {
    let value = REV[symbol as usize];
    if value < 0 {
        Err(DecodeError::InvalidByte(symbol))
    } else {
        Ok(value as u32)
    }
}

/// Encodes a 12-bit value as exactly two symbols.
pub(crate) fn encode_u12(value: usize) -> [u8; 2] {
    assert!(value < 1 << 12, "base64 u12 value out of range");
    [ALPHABET[value >> 6], ALPHABET[value & 63]]
}

/// Decodes exactly two symbols into their 12-bit value.
pub(crate) fn decode_u12(encoded: &str) -> Result<usize, DecodeError> {
    let bytes = encoded.as_bytes();
    if bytes.len() != 2 {
        return Err(DecodeError::InvalidLength);
    }
    Ok(((decode_symbol(bytes[0])? << 6) | decode_symbol(bytes[1])?) as usize)
}

/// Encodes `input` into the custom, order-preserving, unpadded base64 form.
pub fn encode(input: &[u8]) -> String {
    let mut out: Vec<u8> = Vec::with_capacity(input.len().div_ceil(3) * 4);
    let mut i = 0;
    while i + 3 <= input.len() {
        let n = ((input[i] as u32) << 16) | ((input[i + 1] as u32) << 8) | (input[i + 2] as u32);
        out.push(ALPHABET[((n >> 18) & 63) as usize]);
        out.push(ALPHABET[((n >> 12) & 63) as usize]);
        out.push(ALPHABET[((n >> 6) & 63) as usize]);
        out.push(ALPHABET[(n & 63) as usize]);
        i += 3;
    }
    match input.len() - i {
        1 => {
            let n = (input[i] as u32) << 16;
            out.push(ALPHABET[((n >> 18) & 63) as usize]);
            out.push(ALPHABET[((n >> 12) & 63) as usize]);
        }
        2 => {
            let n = ((input[i] as u32) << 16) | ((input[i + 1] as u32) << 8);
            out.push(ALPHABET[((n >> 18) & 63) as usize]);
            out.push(ALPHABET[((n >> 12) & 63) as usize]);
            out.push(ALPHABET[((n >> 6) & 63) as usize]);
        }
        _ => {}
    }
    // SAFETY: every pushed byte comes from ALPHABET, which is ASCII.
    String::from_utf8(out).expect("alphabet is ascii")
}

/// Decodes a string produced by [`encode`].
pub fn decode(input: &str) -> Result<Vec<u8>, DecodeError> {
    let bytes = input.as_bytes();

    let mut out = Vec::with_capacity(bytes.len() / 4 * 3 + 2);
    let mut i = 0;
    while i + 4 <= bytes.len() {
        let n = (decode_symbol(bytes[i])? << 18)
            | (decode_symbol(bytes[i + 1])? << 12)
            | (decode_symbol(bytes[i + 2])? << 6)
            | decode_symbol(bytes[i + 3])?;
        out.push((n >> 16) as u8);
        out.push((n >> 8) as u8);
        out.push(n as u8);
        i += 4;
    }
    match bytes.len() - i {
        0 => {}
        1 => return Err(DecodeError::InvalidLength),
        2 => {
            let n = (decode_symbol(bytes[i])? << 18) | (decode_symbol(bytes[i + 1])? << 12);
            out.push((n >> 16) as u8);
        }
        3 => {
            let n = (decode_symbol(bytes[i])? << 18)
                | (decode_symbol(bytes[i + 1])? << 12)
                | (decode_symbol(bytes[i + 2])? << 6);
            out.push((n >> 16) as u8);
            out.push((n >> 8) as u8);
        }
        _ => unreachable!(),
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        for case in [
            &b""[..],
            b"a",
            b"ab",
            b"abc",
            b"Hello",
            b"\x00\x01\x02\x03\x04",
            b"the quick brown fox",
        ] {
            let enc = encode(case);
            let dec = decode(&enc).unwrap();
            assert_eq!(dec, case, "round trip failed for {case:?}");
        }
    }

    #[test]
    fn order_preserving() {
        // Encoded strings must sort the same way as the raw bytes.
        let mut inputs: Vec<Vec<u8>> = vec![
            vec![0x00],
            vec![0x01],
            vec![0x10],
            vec![0xff],
            vec![0x00, 0x00],
            vec![0x00, 0x01],
        ];
        let mut encoded: Vec<String> = inputs.iter().map(|i| encode(i)).collect();
        inputs.sort();
        encoded.sort();
        let from_sorted: Vec<String> = inputs.iter().map(|i| encode(i)).collect();
        assert_eq!(encoded, from_sorted);
    }

    #[test]
    fn u12_round_trip() {
        for value in 0..1 << 12 {
            let encoded = encode_u12(value);
            let encoded = std::str::from_utf8(&encoded).unwrap();
            assert_eq!(decode_u12(encoded).unwrap(), value);
        }
        assert!(matches!(decode_u12("0"), Err(DecodeError::InvalidLength)));
        assert!(matches!(
            decode_u12("!0"),
            Err(DecodeError::InvalidByte(b'!'))
        ));
    }
}
