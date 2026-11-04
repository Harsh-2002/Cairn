//! Multipart-completion assembly micro-benchmark, isolating `LocalBlobStore::assemble` from the
//! HTTP, auth, metadata and part-upload layers. It stages a fixed part set once (10 x 5 MiB by
//! default, the strict Warp multipart shape), then assembles it repeatedly with a fresh
//! admission permit per run, exactly as CompleteMultipartUpload does.
//!
//! Each run is reported individually plus mean/median/min/max, together with the process CPU
//! (utime + stime) consumed while assembling, so a wall-time change can be attributed. The
//! drained `MultipartStage` samples split assembly from durability. One run at the end reads the
//! assembled object back and compares it byte-for-byte, and checks the internal BLAKE3 baseline and
//! the v3 sidecar, so an arm that is faster because it is wrong cannot be reported as a win.
//!
//! Run: `cargo run --release --example bench_assembly -p cairn-blob`
//!
//! Knobs (env):
//! - `BENCH_LABEL` printed in the header (arm name).
//! - `BENCH_PARTS` (default 10), `BENCH_PART_MIB` (default 5).
//! - `BENCH_WARMUP` (default 1), `BENCH_RUNS` (default 8).
//! - `BENCH_ENCRYPT=1` stores and assembles AES-256-GCM block containers.
//! - `BENCH_COMPRESS=1` applies the default zstd bucket policy.
//! - `BENCH_CRC64=1` requests the CRC64NVME checksum on the completed object.
//! - `BENCH_DIR` data directory (default: a temp dir under the current directory, i.e. real disk).
//! - `BENCH_URING=1` prefers the io_uring staging backend when the feature is compiled in.
//! - `BENCH_NO_VERIFY=1` skips the final read-back validation (timing runs only).
//! - `BENCH_KEEP=1` keeps the assembled files instead of unlinking each after its timing.

use bytes::Bytes;
use cairn_blob::{LocalBlobStore, MultipartStage};
use cairn_types::testing::FixtureBlobStore;
use cairn_types::*;
use futures_util::StreamExt;
use std::time::{Duration, Instant};

fn body(data: Vec<u8>) -> BodyStream {
    Box::pin(futures_util::stream::once(
        async move { Ok(Bytes::from(data)) },
    ))
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_flag(key: &str) -> bool {
    matches!(
        std::env::var(key).ok().as_deref(),
        Some("1" | "true" | "yes")
    )
}

/// Deterministic, incompressible-ish payload so compression arms do real work and repeated runs
/// hash the same bytes.
fn payload(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// Process CPU time in seconds from `/proc/self/stat` (utime + stime, all threads).
fn process_cpu() -> Option<Duration> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let close = stat.rfind(')')?;
    let fields: Vec<&str> = stat[close + 1..].split_whitespace().collect();
    // Field 14 (utime) and 15 (stime) in proc(5) are 11 and 12 after the comm field.
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    let ticks = utime + stime;
    let hz = 100.0;
    Some(Duration::from_secs_f64(ticks as f64 / hz))
}

fn median(xs: &mut [f64]) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    xs[xs.len() / 2]
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let label = std::env::var("BENCH_LABEL").unwrap_or_else(|_| "assembly".to_owned());
    let parts = env_usize("BENCH_PARTS", 10);
    let part_mib = env_usize("BENCH_PART_MIB", 5);
    let warmup = env_usize("BENCH_WARMUP", 1);
    let runs = env_usize("BENCH_RUNS", 8);
    let encrypted = env_flag("BENCH_ENCRYPT");
    let compressed = env_flag("BENCH_COMPRESS");
    let crc64 = env_flag("BENCH_CRC64");
    let use_uring = env_flag("BENCH_URING");
    let verify = !env_flag("BENCH_NO_VERIFY");
    let keep = env_flag("BENCH_KEEP");
    let part_len = part_mib * 1024 * 1024;

    let owned_dir;
    let dir = match std::env::var("BENCH_DIR") {
        Ok(path) => {
            std::fs::create_dir_all(&path).unwrap();
            std::path::PathBuf::from(path)
        }
        Err(_) => {
            owned_dir = tempfile::tempdir_in(".").unwrap();
            owned_dir.path().to_path_buf()
        }
    };
    let store = LocalBlobStore::open(&dir, cairn_types::testing::fixture_storage_io())
        .await
        .unwrap()
        .with_io_uring(use_uring);

    let upload = UploadId::generate();
    let part_checksums = ChecksumSet::none();
    let dek = encrypted.then(|| SecretKey32::from_slice(&[0x42u8; 32]).unwrap());
    let mut staged_parts = Vec::with_capacity(parts);
    for n in 1..=parts {
        let staged = store
            .stage_part_fixture(
                &upload,
                u16::try_from(n).unwrap(),
                &format!("attempt-{n}"),
                body(payload(part_len, n as u64 * 0x9e37_79b9)),
                part_checksums.clone(),
                8 * 1024 * 1024 * 1024,
                dek.clone(),
            )
            .await
            .unwrap();
        staged_parts.push(staged);
    }
    let composite = {
        // The S3 composite ETag: md5 of the concatenated binary part MD5s, then "-<part count>".
        use md5::{Digest, Md5};
        let mut ctx = Md5::new();
        for part in &staged_parts {
            ctx.update(hex::decode(&part.md5_hex).unwrap());
        }
        format!("{}-{}", hex::encode(ctx.finalize()), parts)
    };
    let parts_ref: Vec<PartRef> = staged_parts
        .iter()
        .enumerate()
        .map(|(index, staged)| PartRef {
            part_number: u16::try_from(index + 1).unwrap(),
            storage_path: staged.storage_path.clone(),
            size: staged.size,
            cipher: match dek.clone() {
                Some(dek) => BlobCipher::AuthenticatedV3(dek),
                None => BlobCipher::KnownPlaintext,
            },
        })
        .collect();
    let assembled_len = parts_ref.iter().map(|p| p.size).sum::<u64>();
    let bucket = BucketName::parse("bench-assembly").unwrap();

    let seed = || cairn_types::storage::ObjectMetadataSeed {
        bucket: bucket.clone(),
        key: ObjectKey::parse("assembled.bin").unwrap(),
        version_id: VersionId::null(),
        etag: Some(ETag::from_string(composite.clone())),
        row_id: cairn_types::storage::StorageToken::generate()
            .as_str()
            .to_owned(),
        is_delete_marker: false,
        multipart_parts: Some(
            (1..=parts)
                .map(|n| cairn_types::object::MultipartPartRecord {
                    part_number: u16::try_from(n).unwrap(),
                    size: part_len as u64,
                })
                .collect(),
        ),
        content_type: "application/octet-stream".to_owned(),
        content_encoding: None,
        cache_control: None,
        content_disposition: None,
        content_language: None,
        expires: None,
        expire_at: None,
        storage_class: StorageClass::Standard,
        owner_id: UserId("owner".to_owned()),
        user_metadata: Vec::new(),
        acl: None,
        stored_checksums: if crc64 {
            ChecksumSet(vec![ChecksumAlgorithm::Crc64Nvme])
        } else {
            ChecksumSet::none()
        },
        checksums: None,
        sse_descriptor: None,
        created_at: Timestamp(10),
        updated_at: Timestamp(11),
        tags: Vec::new(),
        lock_intent: ExplicitObjectLockIntent::default(),
    };
    let options = || StageOptions {
        compression: compressed.then(CompressionPolicy::default),
        extra_checksums: if crc64 {
            ChecksumSet(vec![ChecksumAlgorithm::Crc64Nvme])
        } else {
            ChecksumSet::none()
        },
        size_ceiling: 8 * 1024 * 1024 * 1024,
        content_type: "application/octet-stream".to_owned(),
        encryption: dek.clone(),
        content_length: None,
        metadata_seed: Some(seed()),
    };

    println!(
        "assembly: {label} | {parts} x {part_mib} MiB = {} MiB | encrypt={encrypted} compress={compressed} crc64={crc64} uring={use_uring} | {warmup} warm-up + {runs} timed | dir={}",
        assembled_len / (1024 * 1024),
        dir.display()
    );

    let mut wall = Vec::with_capacity(runs);
    let mut cpu_total = Duration::ZERO;
    let mut assembly_stage = Duration::ZERO;
    let mut durability_stage = Duration::ZERO;
    let mut assembled_paths: Vec<(StoragePath, String, CompressionDescriptor)> = Vec::new();
    for run in 0..(warmup + runs) {
        drop(store.drain_multipart_timings());
        let cpu_before = process_cpu();
        let started = Instant::now();
        let assembled = store
            .assemble_fixture(&bucket, &parts_ref, options())
            .await
            .unwrap();
        let elapsed = started.elapsed();
        let cpu_after = process_cpu();
        let stages = store.drain_multipart_timings();
        let assembly = stages
            .iter()
            .find(|s| s.stage == MultipartStage::Assembly)
            .map(|s| s.elapsed)
            .unwrap_or_default();
        let durability = stages
            .iter()
            .find(|s| s.stage == MultipartStage::Durability)
            .map(|s| s.elapsed)
            .unwrap_or_default();
        if run >= warmup {
            println!(
                "  run {:>2}: {:>8.2} ms  assembly {:>8.2}  durability {:>6.2}  cpu {:>7.2} ms",
                run - warmup + 1,
                elapsed.as_secs_f64() * 1e3,
                assembly.as_secs_f64() * 1e3,
                durability.as_secs_f64() * 1e3,
                cpu_after
                    .zip(cpu_before)
                    .map(|(a, b)| (a - b).as_secs_f64() * 1e3)
                    .unwrap_or(f64::NAN),
            );
            wall.push(elapsed.as_secs_f64() * 1e3);
            cpu_total += cpu_after
                .zip(cpu_before)
                .map(|(a, b)| a - b)
                .unwrap_or_default();
            assembly_stage += assembly;
            durability_stage += durability;
        }
        assembled_paths.push((
            assembled.storage_path.clone(),
            assembled.internal_blake3.clone(),
            assembled.compression.clone(),
        ));
        if !keep {
            // Unlink the completed object so repeated runs do not accumulate 50 MiB files or leave
            // the page cache full of never-read data. The parts stay staged for every run.
            let _ = std::fs::remove_file(dir.join(assembled.storage_path.as_str()));
            if let Some(meta) = cairn_types::storage::object_metadata_path(&assembled.storage_path)
            {
                let _ = std::fs::remove_file(dir.join(meta.as_str()));
            }
        }
    }

    let (mean, med, min, max) = {
        let mut sorted = wall.clone();
        let median_value = median(&mut sorted);
        (
            wall.iter().sum::<f64>() / wall.len() as f64,
            median_value,
            wall.iter().copied().fold(f64::INFINITY, f64::min),
            wall.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        )
    };
    let n = wall.len() as f64;
    println!(
        "RESULT {label}: mean {mean:.2} ms  median {med:.2} ms  min-max {min:.2}-{max:.2} ms | assembly stage mean {:.2} ms | durability mean {:.2} ms | cpu mean {:.2} ms | {:.1} MiB/s",
        assembly_stage.as_secs_f64() * 1e3 / n,
        durability_stage.as_secs_f64() * 1e3 / n,
        cpu_total.as_secs_f64() * 1e3 / n,
        assembled_len as f64 / (1024.0 * 1024.0) / (mean / 1e3),
    );

    if verify {
        // The last successful run was unlinked above unless BENCH_KEEP; verify with one more
        // assembly that is kept for the read-back.
        let assembled = store
            .assemble_fixture(&bucket, &parts_ref, options())
            .await
            .unwrap();
        assert_eq!(assembled.size_logical, assembled_len);
        assert_eq!(assembled.md5_hex, None, "assembly must skip the object MD5");
        let mut expected = Vec::with_capacity(assembled_len as usize);
        for n in 1..=parts {
            expected.extend_from_slice(&payload(part_len, n as u64 * 0x9e37_79b9));
        }
        let handle = store
            .open_raw(
                &assembled.storage_path,
                None,
                dek.clone()
                    .map(BlobCipher::AuthenticatedV3)
                    .unwrap_or(BlobCipher::KnownPlaintext),
                &assembled.compression,
                assembled.size_logical,
            )
            .await
            .unwrap();
        let mut bytes = Vec::with_capacity(assembled_len as usize);
        let mut body = handle.body;
        while let Some(chunk) = body.next().await {
            bytes.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(bytes, expected, "assembled bytes must match the parts");
        let blake3_hex = {
            let mut hasher = blake3::Hasher::new();
            hasher.update(&bytes);
            hasher.finalize().to_hex().to_string()
        };
        assert_eq!(
            assembled.internal_blake3, blake3_hex,
            "internal BLAKE3 must cover the exact assembled plaintext"
        );
        let meta = cairn_types::storage::object_metadata_path(&assembled.storage_path).unwrap();
        let record = cairn_types::storage::ObjectMetadataRecord::decode(
            &std::fs::read(dir.join(meta.as_str())).unwrap(),
        )
        .unwrap();
        assert_eq!(record.format_version, 3);
        assert_eq!(record.multipart_parts.as_ref().map(Vec::len), Some(parts));
        assert_eq!(record.internal_blake3.as_deref(), Some(blake3_hex.as_str()));
        println!(
            "verify: {} bytes exact, internal blake3 ok, v3 sidecar ok ({})",
            bytes.len(),
            assembled.etag.as_str()
        );
    }
}
