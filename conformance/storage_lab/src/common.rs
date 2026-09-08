use serde_json::json;

pub(crate) fn payload(size: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..size)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 32) as u8
        })
        .collect()
}

pub(crate) fn emit(mut value: serde_json::Value) {
    value["unix_seconds"] = json!(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("laboratory clock predates the Unix epoch")
            .as_secs_f64()
    );
    println!("{value}");
}

pub(crate) fn distribution(values: &mut [f64]) -> serde_json::Value {
    values.sort_by(f64::total_cmp);
    if values.is_empty() {
        return json!({"count": 0});
    }
    json!({"count": values.len(), "sum_seconds": values.iter().sum::<f64>(),
        "p50_seconds": values[(values.len() - 1) / 2],
        "p99_seconds": (values.len() >= 10_000).then(|| values[(values.len() * 99).div_ceil(100) - 1]),
        "max_seconds": values.last()})
}
