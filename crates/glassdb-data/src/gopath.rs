//! A faithful port of the subset of Go's `path` package used by GlassDB
//! (`Clean` and `Join`), so storage paths are byte-identical to the Go
//! implementation.

/// Equivalent of Go's `path.Clean`.
pub fn clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }
    let bytes = path.as_bytes();
    let rooted = bytes[0] == b'/';
    let n = bytes.len();
    let mut out: Vec<u8> = Vec::with_capacity(n);
    let mut r = 0usize;
    let mut dotdot = 0usize;
    if rooted {
        out.push(b'/');
        r = 1;
        dotdot = 1;
    }
    while r < n {
        if bytes[r] == b'/' || (bytes[r] == b'.' && (r + 1 == n || bytes[r + 1] == b'/')) {
            // Empty path element ('/') or '.' element: skip it.
            r += 1;
        } else if bytes[r] == b'.'
            && r + 1 < n
            && bytes[r + 1] == b'.'
            && (r + 2 == n || bytes[r + 2] == b'/')
        {
            r += 2;
            if out.len() > dotdot {
                let mut w = out.len() - 1;
                while w > dotdot && out[w] != b'/' {
                    w -= 1;
                }
                out.truncate(w);
            } else if !rooted {
                if !out.is_empty() {
                    out.push(b'/');
                }
                out.push(b'.');
                out.push(b'.');
                dotdot = out.len();
            }
        } else {
            if (rooted && out.len() != 1) || (!rooted && !out.is_empty()) {
                out.push(b'/');
            }
            while r < n && bytes[r] != b'/' {
                out.push(bytes[r]);
                r += 1;
            }
        }
    }
    if out.is_empty() {
        return ".".to_string();
    }
    String::from_utf8(out).expect("input was valid utf8")
}

/// Equivalent of Go's `path.Join`: joins non-empty elements with `/` and runs
/// the result through [`clean`].
pub fn join(parts: &[&str]) -> String {
    let mut buf = String::new();
    for p in parts {
        if p.is_empty() {
            continue;
        }
        if !buf.is_empty() {
            buf.push('/');
        }
        buf.push_str(p);
    }
    if buf.is_empty() {
        return String::new();
    }
    clean(&buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_basics() {
        assert_eq!(clean("a/b/../c"), "a/c");
        assert_eq!(clean("a//b"), "a/b");
        assert_eq!(clean("a/./b"), "a/b");
        assert_eq!(clean(""), ".");
        assert_eq!(clean("/a/b"), "/a/b");
    }

    #[test]
    fn join_basics() {
        assert_eq!(join(&["foo/bar", "_i"]), "foo/bar/_i");
        assert_eq!(join(&["_k", "SGVsbG8"]), "_k/SGVsbG8");
        assert_eq!(join(&["", "_i"]), "_i");
    }
}
