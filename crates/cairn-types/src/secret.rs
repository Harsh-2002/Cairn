//! Secret-bearing value types shared across crate boundaries.
//!
//! These wrappers make accidental disclosure through `Debug` impossible and scrub their owned
//! buffers on drop. Callers must opt in to plaintext access through the explicitly named
//! [`SecretString::expose_secret`] and [`SecretKey32::expose_secret`] methods.

use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::ops::Deref;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// An owned UTF-8 secret that redacts `Debug` output and zeroizes its allocation on drop.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(transparent)]
pub struct SecretString(String);

impl SecretString {
    /// Wrap an owned secret.
    #[must_use]
    pub fn new(secret: String) -> Self {
        Self(secret)
    }

    /// Expose the secret text at the narrow point where plaintext is required.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }

    /// Whether this secret contains no bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SecretString(<redacted>)")
    }
}

impl From<String> for SecretString {
    fn from(secret: String) -> Self {
        Self::new(secret)
    }
}

impl From<&str> for SecretString {
    fn from(secret: &str) -> Self {
        Self::new(secret.to_owned())
    }
}

impl std::str::FromStr for SecretString {
    type Err = std::convert::Infallible;

    fn from_str(secret: &str) -> Result<Self, Self::Err> {
        Ok(Self::from(secret))
    }
}

impl Deref for SecretString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.expose_secret()
    }
}

impl AsRef<str> for SecretString {
    fn as_ref(&self) -> &str {
        self.expose_secret()
    }
}

impl Borrow<str> for SecretString {
    fn borrow(&self) -> &str {
        self.expose_secret()
    }
}

impl PartialEq<str> for SecretString {
    fn eq(&self, other: &str) -> bool {
        self.expose_secret() == other
    }
}

impl PartialEq<&str> for SecretString {
    fn eq(&self, other: &&str) -> bool {
        self.expose_secret() == *other
    }
}

/// An owned 256-bit secret key.
///
/// The type deliberately does not implement `Copy`; cloning a key is therefore visible at call
/// sites, and every owned clone is scrubbed on drop.
///
/// ```compile_fail
/// use cairn_types::SecretKey32;
///
/// fn requires_copy<T: Copy>() {}
/// requires_copy::<SecretKey32>();
/// ```
#[derive(Clone, PartialEq, Eq, Zeroize, ZeroizeOnDrop)]
pub struct SecretKey32([u8; 32]);

impl SecretKey32 {
    /// Wrap exactly 32 secret bytes.
    #[must_use]
    pub fn new(secret: [u8; 32]) -> Self {
        Self(secret)
    }

    /// Copy a validated 32-byte slice into a zeroizing key owner.
    ///
    /// Returns `None` when `secret` is not exactly 32 bytes.
    #[must_use]
    pub fn from_slice(secret: &[u8]) -> Option<Self> {
        secret.try_into().ok().map(Self::new)
    }

    /// Expose the key bytes at the narrow point where a cryptographic primitive needs them.
    #[must_use]
    pub fn expose_secret(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for SecretKey32 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SecretKey32(<redacted>)")
    }
}

impl From<[u8; 32]> for SecretKey32 {
    fn from(secret: [u8; 32]) -> Self {
        Self::new(secret)
    }
}

impl AsRef<[u8]> for SecretKey32 {
    fn as_ref(&self) -> &[u8] {
        self.expose_secret()
    }
}

#[cfg(test)]
mod tests {
    use super::{SecretKey32, SecretString};
    use zeroize::{Zeroize, ZeroizeOnDrop};

    fn assert_zeroize_on_drop<T: ZeroizeOnDrop>() {}

    #[test]
    fn debug_output_redacts_secret_sentinels() {
        let text = SecretString::from("sentinel-secret");
        let key = SecretKey32::new([0xa5; 32]);

        let text_debug = format!("{text:?}");
        let key_debug = format!("{key:?}");

        assert!(!text_debug.contains("sentinel-secret"));
        assert!(!key_debug.contains("165"));
        assert!(text_debug.contains("<redacted>"));
        assert!(key_debug.contains("<redacted>"));
    }

    #[test]
    fn wrappers_implement_zeroize_on_drop_and_explicit_zeroize() {
        assert_zeroize_on_drop::<SecretString>();
        assert_zeroize_on_drop::<SecretKey32>();

        let mut text = SecretString::from("sentinel-secret");
        let mut key = SecretKey32::new([0xa5; 32]);
        text.zeroize();
        key.zeroize();

        assert!(text.is_empty());
        assert_eq!(key.expose_secret(), &[0; 32]);
    }
}
