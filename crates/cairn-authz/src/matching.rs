//! ARN/resource matching and `StringLike`-style `*`/`?` wildcard matching.

use cairn_types::Resource;

/// The ARN prefix for every S3 resource (`arn:aws:s3:::`).
const S3_ARN_PREFIX: &str = "arn:aws:s3:::";

/// Render a [`Resource`] as the ARN-like string a policy resource pattern matches against.
///
/// A bucket becomes `arn:aws:s3:::bucket`; an object becomes `arn:aws:s3:::bucket/key`.
#[must_use]
pub fn resource_arn(resource: &Resource) -> String {
    match resource {
        Resource::Bucket(b) => format!("{S3_ARN_PREFIX}{}", b.as_str()),
        Resource::Object { bucket, key } => {
            format!("{S3_ARN_PREFIX}{}/{}", bucket.as_str(), key.as_str())
        }
    }
}

/// Whether a policy `Resource` pattern (which may contain `*`/`?` wildcards and is itself an
/// ARN-like string) matches the concrete request resource.
#[must_use]
pub fn resource_matches(pattern: &str, resource: &Resource) -> bool {
    wildcard_match(pattern, &resource_arn(resource))
}

/// Glob-style match supporting `*` (any run, including empty) and `?` (exactly one char),
/// matching over Unicode scalar values. This is the semantics S3 uses for both `StringLike`
/// conditions and resource ARNs.
#[must_use]
pub fn wildcard_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    matches_from(&p, &t)
}

/// Iterative wildcard matcher with backtracking on the last `*` seen. Linear in practice and
/// never recurses, so it cannot blow the stack on adversarial patterns.
fn matches_from(p: &[char], t: &[char]) -> bool {
    let mut pi = 0usize;
    let mut ti = 0usize;
    // The position in `p` just after the most recent `*`, and the `t` position to resume from.
    let mut star_pi: Option<usize> = None;
    let mut star_ti = 0usize;

    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star_pi = Some(pi);
            star_ti = ti;
            pi += 1;
        } else if let Some(s) = star_pi {
            // Backtrack: let the last `*` absorb one more character.
            pi = s + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    // Consume any trailing `*`s in the pattern.
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::{BucketName, ObjectKey};

    fn bucket(name: &str) -> Resource {
        Resource::Bucket(BucketName::parse(name).unwrap())
    }
    fn object(b: &str, k: &str) -> Resource {
        Resource::Object {
            bucket: BucketName::parse(b).unwrap(),
            key: ObjectKey::parse(k).unwrap(),
        }
    }

    #[test]
    fn exact_and_star() {
        assert!(wildcard_match("abc", "abc"));
        assert!(!wildcard_match("abc", "abd"));
        assert!(wildcard_match("*", ""));
        assert!(wildcard_match("*", "anything"));
        assert!(wildcard_match("a*c", "abbbc"));
        assert!(wildcard_match("a*c", "ac"));
        assert!(!wildcard_match("a*c", "ab"));
    }

    #[test]
    fn question_mark() {
        assert!(wildcard_match("a?c", "abc"));
        assert!(!wildcard_match("a?c", "ac"));
        assert!(!wildcard_match("a?c", "abbc"));
    }

    #[test]
    fn multiple_stars_backtrack() {
        assert!(wildcard_match("*a*b*", "xxaxxbxx"));
        assert!(!wildcard_match("*a*b*c", "xxaxxbxx"));
        assert!(wildcard_match("a*b*c", "axbxc"));
    }

    #[test]
    fn arn_rendering() {
        assert_eq!(resource_arn(&bucket("my-bucket")), "arn:aws:s3:::my-bucket");
        assert_eq!(
            resource_arn(&object("my-bucket", "photos/a.jpg")),
            "arn:aws:s3:::my-bucket/photos/a.jpg"
        );
    }

    #[test]
    fn resource_pattern_matching() {
        let obj = object("my-bucket", "photos/2026/a.jpg");
        assert!(resource_matches("arn:aws:s3:::my-bucket/*", &obj));
        assert!(resource_matches("arn:aws:s3:::my-bucket/photos/*", &obj));
        assert!(!resource_matches("arn:aws:s3:::my-bucket/docs/*", &obj));
        assert!(!resource_matches("arn:aws:s3:::my-bucket", &obj));

        let buck = bucket("my-bucket");
        assert!(resource_matches("arn:aws:s3:::my-bucket", &buck));
        assert!(resource_matches("arn:aws:s3:::*", &buck));
        // A bucket-only ARN must not match an object pattern.
        assert!(!resource_matches("arn:aws:s3:::my-bucket/*", &buck));
    }
}
