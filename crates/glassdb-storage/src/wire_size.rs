//! Protobuf field-size arithmetic shared by the storage codecs.

use prost::encoding::{encoded_len_varint, key_len};

/// Encoded size of one present length-delimited field.
pub(crate) const fn length_delimited_field(tag: u32, payload_len: usize) -> usize {
    key_len(tag) + encoded_len_varint(payload_len as u64) + payload_len
}

/// Encoded size of a proto3 bytes/string field, which omits an empty value.
pub(crate) const fn nonempty_length_delimited_field(tag: u32, payload_len: usize) -> usize {
    if payload_len == 0 {
        0
    } else {
        length_delimited_field(tag, payload_len)
    }
}

/// Encoded size of a present varint field, including a oneof's default value.
pub(crate) const fn present_varint_field(tag: u32, value: u64) -> usize {
    key_len(tag) + encoded_len_varint(value)
}

/// Encoded size of a proto3 scalar varint, which omits zero.
pub(crate) const fn nonzero_varint_field(tag: u32, value: u64) -> usize {
    if value == 0 {
        0
    } else {
        present_varint_field(tag, value)
    }
}
