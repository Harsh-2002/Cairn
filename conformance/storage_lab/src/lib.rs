//! Shared bounded workloads and isolated candidate adapters for the storage laboratory.
#[path = "metadata_fjall/mod.rs"]
pub mod metadata_fjall;
#[path = "metadata_capacity/metrics.rs"]
pub mod metadata_metrics;
pub mod metadata_run;
#[path = "metadata_capacity/workload.rs"]
pub mod metadata_workload;
