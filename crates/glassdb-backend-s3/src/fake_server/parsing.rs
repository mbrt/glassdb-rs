//! Header, query, continuation-token, and XML codecs used by fake S3 routing.

use std::collections::HashMap;

pub(super) fn header_str(headers: &hyper::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

pub(super) fn query_params(query: &str) -> HashMap<String, String> {
    query
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (pct_decode(key), pct_decode(value))
        })
        .collect()
}

pub(super) fn encode_page_token(prefix: &str, last: &str) -> String {
    format!("{}:{prefix}{last}", prefix.len())
}

pub(super) fn decode_page_token<'a>(prefix: &str, token: &'a str) -> Option<&'a str> {
    let (prefix_len, body) = token.split_once(':')?;
    let prefix_len = prefix_len.parse::<usize>().ok()?;
    let stored_prefix = body.get(..prefix_len)?;
    let last = body.get(prefix_len..)?;
    (stored_prefix == prefix && last.starts_with(prefix)).then_some(last)
}

pub(super) fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn pct_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let Ok(hex) = u8::from_str_radix(&value[index + 1..index + 3], 16)
        {
            out.push(hex);
            index += 3;
            continue;
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}
