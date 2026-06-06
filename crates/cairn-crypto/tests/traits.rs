//! Integration tests: the production implementations are usable through the frozen trait
//! objects, and behave compatibly with the in-memory doubles in `cairn-types::testing` where
//! the contracts overlap (round-trip, constant-time compare, expiry semantics).

use cairn_crypto::{HmacPublicUrl, SystemClock, SystemCrypto};
use cairn_types::testing::{StubCrypto, StubPublicUrl, TestClock};
use cairn_types::traits::{Clock, Crypto, PublicUrl};
use cairn_types::{Nonce, Signature, Timestamp};

#[test]
fn crypto_is_object_safe_and_round_trips_via_dyn() {
    let crypto: Box<dyn Crypto> = Box::new(SystemCrypto::new([42u8; 32]));
    let secret = b"AKIA-style-secret-value";
    let sealed = crypto.seal(secret).expect("seal");
    let opened = crypto
        .open(&sealed.ciphertext, &sealed.nonce)
        .expect("open");
    assert_eq!(opened, secret);
}

#[test]
fn production_and_stub_crypto_agree_on_ct_eq_contract() {
    let prod: &dyn Crypto = &SystemCrypto::new([0u8; 32]);
    let stub: &dyn Crypto = &StubCrypto;
    for (a, b) in [
        (&b"abc"[..], &b"abc"[..]),
        (&b"abc"[..], &b"abd"[..]),
        (&b"abc"[..], &b"abcd"[..]),
        (&b""[..], &b""[..]),
    ] {
        assert_eq!(
            prod.ct_eq(a, b),
            stub.ct_eq(a, b),
            "ct_eq disagreement on {a:?} vs {b:?}"
        );
    }
}

#[test]
fn production_crypto_rejects_what_stub_nonce_cannot_open() {
    // A stub-shaped all-zero nonce of the right length still fails AEAD auth against the real
    // cipher when paired with foreign ciphertext.
    let prod = SystemCrypto::new([9u8; 32]);
    let foreign = StubCrypto.seal(b"hello").expect("stub seal");
    let err = prod
        .open(&foreign.ciphertext, &Nonce(vec![0u8; 12]))
        .expect_err("real cipher must reject stub ciphertext");
    assert!(matches!(err, cairn_types::CryptoError::Decrypt));
}

#[test]
fn clock_is_object_safe_and_after_the_test_double_default() {
    let sys: Box<dyn Clock> = Box::new(SystemClock::new());
    let test: Box<dyn Clock> = Box::new(TestClock::default());
    assert!(
        sys.now() > test.now(),
        "the wall clock should be ahead of the fixed test default"
    );
}

#[test]
fn public_url_object_safe_sign_verify_round_trip() {
    let pu: Box<dyn PublicUrl> = Box::new(HmacPublicUrl::new(b"signing-secret".to_vec()));
    let expiry = Timestamp(2_000_000_000_000);
    let now = Timestamp(1_999_999_000_000);
    let sig = pu.sign("GET", "/b/k", expiry);
    assert!(pu.verify("GET", "/b/k", expiry, &sig, now));
    // Expired now must be rejected.
    let later = Timestamp(expiry.as_millis() + 1);
    assert!(!pu.verify("GET", "/b/k", expiry, &sig, later));
}

#[test]
fn production_signatures_are_not_interchangeable_with_the_stub() {
    let prod = HmacPublicUrl::new(b"k".to_vec());
    let stub = StubPublicUrl;
    let expiry = Timestamp(2_000_000_000_000);
    let now = Timestamp(1_000_000_000_000);

    let stub_sig: Signature = stub.sign("GET", "/p", expiry);
    // The HMAC verifier must reject the stub's plaintext-shaped signature.
    assert!(!prod.verify("GET", "/p", expiry, &stub_sig, now));

    let prod_sig = prod.sign("GET", "/p", expiry);
    // And the stub verifier rejects the HMAC hex signature.
    assert!(!stub.verify("GET", "/p", expiry, &prod_sig, now));
}
