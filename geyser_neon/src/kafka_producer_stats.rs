use log::info;
use prometheus_client::metrics::counter::Counter;
use rdkafka::{ClientContext, Statistics};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

#[derive(Default)]
pub struct Stats {
    pub filtered_events: Counter<u64, AtomicU64>,
    pub kafka_update_account: Counter<u64, AtomicU64>,
    pub kafka_update_slot: Counter<u64, AtomicU64>,
    pub kafka_notify_transaction: Counter<u64, AtomicU64>,
    pub kafka_notify_block: Counter<u64, AtomicU64>,
    pub kafka_errors_update_account: Counter<u64, AtomicU64>,
    pub kafka_errors_update_slot: Counter<u64, AtomicU64>,
    pub kafka_errors_notify_transaction: Counter<u64, AtomicU64>,
    pub kafka_errors_notify_block: Counter<u64, AtomicU64>,
    pub kafka_errors_serialize: Counter<u64, AtomicU64>,
    pub kafka_bytes_tx: Counter<u64, AtomicU64>,
}

#[derive(Default, Clone)]
pub struct ContextWithStats {
    pub stats: Arc<Stats>,
}

impl ClientContext for ContextWithStats {
    fn stats(&self, stats: Statistics) {
        info!("{:?}", stats);
    }
}
