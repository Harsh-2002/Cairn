//! Micro-benchmark for the streaming chunked-upload decoder (ARCH §29.6): confirms the de-framer
//! runs at many GiB/s and is not the bottleneck on the ingest path.

use cairn_s3::ChunkDecoder;
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};

fn build_unsigned_body(total: usize, chunk: usize) -> Vec<u8> {
    let mut body = Vec::with_capacity(total + total / chunk * 16);
    let mut remaining = total;
    let payload = vec![b'a'; chunk];
    while remaining > 0 {
        let n = remaining.min(chunk);
        body.extend_from_slice(format!("{n:x}\r\n").as_bytes());
        body.extend_from_slice(&payload[..n]);
        body.extend_from_slice(b"\r\n");
        remaining -= n;
    }
    body.extend_from_slice(b"0\r\n\r\n");
    body
}

fn bench_decode(c: &mut Criterion) {
    const TOTAL: usize = 8 * 1024 * 1024;
    let body = build_unsigned_body(TOTAL, 64 * 1024);
    let mut group = c.benchmark_group("chunked_decode");
    group.throughput(Throughput::Bytes(TOTAL as u64));
    group.bench_function("8MiB_64KiB_chunks", |b| {
        b.iter(|| {
            let mut d = ChunkDecoder::unsigned(u64::MAX);
            let mut out = Vec::new();
            d.push(black_box(&body), &mut out).unwrap();
            d.finish().unwrap();
            black_box(out.iter().map(bytes::Bytes::len).sum::<usize>())
        });
    });
    group.finish();
}

criterion_group!(benches, bench_decode);
criterion_main!(benches);
