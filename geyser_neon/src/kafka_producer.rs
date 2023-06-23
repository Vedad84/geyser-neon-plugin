use kafka_common::message_type::MessageType;
use log::{error, info};
use rdkafka::message::DeliveryResult;
use rdkafka::producer::{Producer, ProducerContext, ThreadedProducer};
use rdkafka::{ClientConfig, ClientContext, Statistics};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::geyser_neon_config::GeyserPluginKafkaConfig;
use crate::stats::{Stats, TopicStats};
use rdkafka::config::RDKafkaLogLevel;

use rdkafka::util::Timeout;
use serde_derive::Deserialize;

#[derive(Deserialize)]
pub enum LogLevel {
    /// Higher priority than [`Level::Error`](log::Level::Error) from the log
    /// crate.
    Emerg = 0,
    /// Higher priority than [`Level::Error`](log::Level::Error) from the log
    /// crate.
    Alert = 1,
    /// Higher priority than [`Level::Error`](log::Level::Error) from the log
    /// crate.
    Critical = 2,
    /// Equivalent to [`Level::Error`](log::Level::Error) from the log crate.
    Error = 3,
    /// Equivalent to [`Level::Warn`](log::Level::Warn) from the log crate.
    Warning = 4,
    /// Higher priority than [`Level::Info`](log::Level::Info) from the log
    /// crate.
    Notice = 5,
    /// Equivalent to [`Level::Info`](log::Level::Info) from the log crate.
    Info = 6,
    /// Equivalent to [`Level::Debug`](log::Level::Debug) from the log crate.
    Debug = 7,
}

impl From<&LogLevel> for RDKafkaLogLevel {
    fn from(log_level: &LogLevel) -> Self {
        match log_level {
            LogLevel::Emerg => RDKafkaLogLevel::Emerg,
            LogLevel::Alert => RDKafkaLogLevel::Alert,
            LogLevel::Critical => RDKafkaLogLevel::Critical,
            LogLevel::Error => RDKafkaLogLevel::Error,
            LogLevel::Warning => RDKafkaLogLevel::Warning,
            LogLevel::Notice => RDKafkaLogLevel::Notice,
            LogLevel::Info => RDKafkaLogLevel::Info,
            LogLevel::Debug => RDKafkaLogLevel::Debug,
        }
    }
}

#[derive(Deserialize)]
pub struct KafkaProducerConfig {
    // Servers list in kafka format
    pub brokers_list: String,
    pub sasl_username: String,
    pub sasl_password: String,
    pub sasl_mechanism: String,
    pub security_protocol: String,
    // From 0 to 2147483647 (i32::MAX),
    pub producer_send_max_retries: String,
    pub producer_queue_max_messages: String,
    pub producer_queue_max_kbytes: String,
    pub producer_message_max_bytes: String,
    pub producer_request_timeout_ms: String,
    pub producer_retry_backoff_ms: String,
    pub producer_enable_idempotence: String,
    pub max_in_flight_requests_per_connection: String,
    pub compression_codec: String,
    pub compression_level: String,
    pub batch_size: String,
    pub batch_num_messages: String,
    pub linger_ms: String,
    pub acks: String,
    pub statistics_interval_ms: String,
    // This value is only enforced locally and limits the time a produced message waits for successful delivery.
    // A time of 0 is infinite.
    // This is the maximum time librdkafka may use to deliver a message (including retries)
    // From 0 to 2147483647 (i32::MAX)
    pub message_timeout_ms: String,
    pub kafka_log_level: LogLevel,
    pub fetch_metadata: bool,
    pub fetch_metadata_timeout_ms: u64,
    pub debug: String,
    pub flush_timeout_ms: u64,
}

fn client_config(config: &KafkaProducerConfig) -> ClientConfig {
    let rd_kafka_log_level = (&config.kafka_log_level).into();

    info!(
        "Rdkafka logging level will be set to {:?}",
        rd_kafka_log_level
    );

    let mut client_config = ClientConfig::new();
    client_config
        .set_log_level(rd_kafka_log_level)
        .set("bootstrap.servers", &config.brokers_list)
        .set("message.timeout.ms", &config.message_timeout_ms)
        .set("security.protocol", &config.security_protocol)
        .set("sasl.mechanism", &config.sasl_mechanism)
        .set("sasl.username", &config.sasl_username)
        .set("sasl.password", &config.sasl_password)
        .set(
            "message.send.max.retries",
            &config.producer_send_max_retries,
        )
        .set(
            "queue.buffering.max.messages",
            &config.producer_queue_max_messages,
        )
        .set(
            "queue.buffering.max.kbytes",
            &config.producer_queue_max_kbytes,
        )
        .set("message.max.bytes", &config.producer_message_max_bytes)
        .set("request.timeout.ms", &config.producer_request_timeout_ms)
        .set("retry.backoff.ms", &config.producer_retry_backoff_ms)
        .set("enable.idempotence", &config.producer_enable_idempotence)
        .set(
            "max.in.flight.requests.per.connection",
            &config.max_in_flight_requests_per_connection,
        )
        .set("compression.codec", &config.compression_codec)
        .set("compression.level", &config.compression_level)
        .set("batch.size", &config.batch_size)
        .set("batch.num.messages", &config.batch_num_messages)
        .set("linger.ms", &config.linger_ms)
        .set("acks", &config.acks)
        .set("statistics.interval.ms", &config.statistics_interval_ms)
        .set("debug", &config.debug);
    client_config
}

pub fn create_producer(
    config: Arc<GeyserPluginKafkaConfig>,
    stats: Arc<Stats>,
) -> anyhow::Result<ThreadedProducer<ContextWithStats>> {
    let kafka_producer_config = &config.kafka_producer_config;

    let kafka_producer: ThreadedProducer<_> = client_config(kafka_producer_config)
        .create_with_context(ContextWithStats::new(stats, config.clone()))?;

    if kafka_producer_config.fetch_metadata {
        let metadata = kafka_producer.client().fetch_metadata(
            None,
            Timeout::After(Duration::from_millis(
                kafka_producer_config.fetch_metadata_timeout_ms,
            )),
        )?;
        info!(
            "Kafka producer topics {:?}",
            metadata
                .topics()
                .iter()
                .map(|topic| topic.name())
                .collect::<Vec<_>>()
        )
    }

    Ok(kafka_producer)
}

pub struct ContextWithStats {
    stats: Arc<Stats>,
    config: Arc<GeyserPluginKafkaConfig>,
}

impl ContextWithStats {
    pub fn new(stats: Arc<Stats>, config: Arc<GeyserPluginKafkaConfig>) -> Self {
        ContextWithStats { stats, config }
    }
}

impl ClientContext for ContextWithStats {
    fn stats(&self, stats: Statistics) {
        self.stats.msg_cnt.set(stats.msg_cnt as i64);
        self.stats.msg_size.set(stats.msg_size as i64);
        self.stats
            .msg_max
            .inner()
            .store(stats.msg_max, Ordering::Relaxed);
        self.stats
            .msg_size_max
            .inner()
            .store(stats.msg_size_max, Ordering::Relaxed);
        self.stats
            .tx
            .inner()
            .store(stats.tx as u64, Ordering::Relaxed);
        self.stats
            .tx_bytes
            .inner()
            .store(stats.tx_bytes as u64, Ordering::Relaxed);
        self.stats
            .txmsgs
            .inner()
            .store(stats.txmsgs as u64, Ordering::Relaxed);
        self.stats
            .txmsg_bytes
            .inner()
            .store(stats.txmsg_bytes as u64, Ordering::Relaxed);
        self.stats.txerrs.inner().store(
            stats
                .brokers
                .values()
                .map(|broker| broker.txerrs)
                .sum::<u64>(),
            Ordering::Relaxed,
        );
        self.stats.num_brokers.set(stats.brokers.len() as i64);
        update_topic_stats_from_topic(
            &self.stats.update_account,
            &stats,
            &self.config.update_account_topic,
        );
        update_topic_stats_from_topic(
            &self.stats.update_slot,
            &stats,
            &self.config.update_slot_topic,
        );
        update_topic_stats_from_topic(
            &self.stats.notify_transaction,
            &stats,
            &self.config.notify_transaction_topic,
        );
        update_topic_stats_from_topic(
            &self.stats.notify_block,
            &stats,
            &self.config.notify_block_topic,
        );
        // TODO Maybe also add Partition::msgq_cnt, Partition::msgq_bytes
    }
}

fn update_topic_stats_from_topic(topic_stats: &TopicStats, stats: &Statistics, topic: &str) {
    if let Some(topic) = stats.topics.get(topic) {
        topic_stats.batch_size.p95.set(topic.batchsize.p95);
        topic_stats.batch_size.p99.set(topic.batchsize.p99);
        topic_stats.batch_size.p99_99.set(topic.batchsize.p99_99);
        topic_stats.batch_cnt.p95.set(topic.batchcnt.p95);
        topic_stats.batch_cnt.p99.set(topic.batchcnt.p99);
        topic_stats.batch_cnt.p99_99.set(topic.batchcnt.p99_99);
    };
}

impl ProducerContext for ContextWithStats {
    type DeliveryOpaque = Box<DeliveryOpaqueData>;

    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        delivery_opaque: Self::DeliveryOpaque,
    ) {
        match delivery_result {
            Ok(_) => delivery_opaque.inc_success_stats(),
            Err((error, message)) => {
                error!(
                    "Producer cannot send {}, slot {}, with size: {}, error: {}",
                    delivery_opaque.message_type,
                    delivery_opaque.slot,
                    message.payload_len(),
                    error
                );
                delivery_opaque.inc_error_stats();
            }
        }
    }
}

#[derive(Debug)]
pub struct DeliveryOpaqueData {
    message_type: MessageType,
    slot: u64,
    stats: Arc<Stats>,
}

impl DeliveryOpaqueData {
    pub fn new(message_type: MessageType, slot: u64, stats: Arc<Stats>) -> Self {
        DeliveryOpaqueData {
            message_type,
            slot,
            stats,
        }
    }

    fn inc_success_stats(&self) {
        self.stats
            .stats_for_message_type(&self.message_type)
            .sent
            .inc();
    }

    fn inc_error_stats(&self) {
        self.stats
            .stats_for_message_type(&self.message_type)
            .send_errors
            .inc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_threaded_producer_statistics_interval_ms_race_condition() -> anyhow::Result<()> {
        // fast_log::init(fast_log::Config::new().console())?;

        // statistics.interval.ms needs to be more than 100,
        // because of https://github.com/fede1024/rust-rdkafka/blob/master/src/producer/base_producer.rs#L538
        let _producer: ThreadedProducer<_> = ClientConfig::new()
            .set("statistics.interval.ms", "110")
            .create()?;
        Ok(())
    }
}
