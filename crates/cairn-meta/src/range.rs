//! Byte/character helpers that make listing a half-open range seek on the indexed key
//! column (ARCH 11.4, F-11) rather than a scan. Their correctness is the correctness of
//! listing and pagination, so they are unit-tested against empty, maximal, and multibyte
//! inputs.
//!
//! SQLite's default `BINARY` collation orders TEXT by UTF-8 bytes, and UTF-8 preserves code
//! point order in byte order, so incrementing the last code point yields a valid UTF-8
//! string that is a correct exclusive upper bound under byte ordering.

/// The smallest key strictly greater than `key`: `key` with a NUL appended. Used to turn an
/// inclusive lower-bound cursor into an exclusive resume point after a consumed key.
#[must_use]
pub fn successor(key: &str) -> String {
    let mut s = String::with_capacity(key.len() + 1);
    s.push_str(key);
    s.push('\u{0}');
    s
}

/// The exclusive upper bound of all keys beginning with `prefix`: the smallest string that
/// is greater than every string with that prefix. Returns `None` when no finite bound exists
/// (an empty prefix, or a prefix that is all `char::MAX`), meaning "no upper bound".
#[must_use]
pub fn prefix_upper_bound(prefix: &str) -> Option<String> {
    if prefix.is_empty() {
        return None;
    }
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(&last) = chars.last() {
        if let Some(next) = char::from_u32(last as u32 + 1) {
            // skip the surrogate gap automatically handled by char::from_u32 returning None
            chars.pop();
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
        // last is char::MAX (or just below the surrogate range edge); drop it and carry.
        chars.pop();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successor_is_minimal_greater() {
        assert!(successor("a").as_str() > "a");
        assert!(successor("a").as_str() < "a\u{1}");
        assert!(successor("").as_str() > "");
        // nothing sorts strictly between key and successor(key)
        assert_eq!(successor("foo"), "foo\u{0}");
    }

    #[test]
    fn prefix_upper_bound_bounds_the_family() {
        let ub = prefix_upper_bound("ab").unwrap();
        assert!("ab" < ub.as_str());
        assert!("abzzzz" < ub.as_str());
        assert!("ab\u{10FFFF}" < ub.as_str());
        assert!("ac" >= ub.as_str()); // ac is at or past the bound
        assert_eq!(ub, "ac");
    }

    #[test]
    fn prefix_upper_bound_edges() {
        assert_eq!(prefix_upper_bound(""), None);
        // a multibyte last char increments by code point, staying valid UTF-8.
        let ub = prefix_upper_bound("é").unwrap();
        assert!("é" < ub.as_str());
        assert!(ub.chars().all(|c| c as u32 != 0));
        // char::MAX as last char carries to the previous position.
        let s = format!("a{}", char::MAX);
        let ub = prefix_upper_bound(&s).unwrap();
        assert_eq!(ub, "b");
        // all-max yields no bound.
        let allmax: String = std::iter::repeat_n(char::MAX, 2).collect();
        assert_eq!(prefix_upper_bound(&allmax), None);
    }

    #[test]
    fn prefix_upper_bound_excludes_everything_under_prefix() {
        // For a delimiter-style common prefix ending in '/'.
        let ub = prefix_upper_bound("photos/").unwrap();
        assert!("photos/2026/a.jpg" < ub.as_str());
        assert!("photos/zzz" < ub.as_str());
        assert!("photos0" >= ub.as_str());
    }
}
