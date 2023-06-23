use crate::kafka_producer::KafkaProducerConfig;
use log::LevelFilter;

use serde::Deserialize;

#[derive(Deserialize)]
pub enum GlobalLogLevel {
    /// A level lower than all log levels.
    Off,
    /// Corresponds to the `Error` log level.
    Error,
    /// Corresponds to the `Warn` log level.
    Warn,
    /// Corresponds to the `Info` log level.
    Info,
    /// Corresponds to the `Debug` log level.
    Debug,
    /// Corresponds to the `Trace` log level.
    Trace,
}

impl From<&GlobalLogLevel> for LevelFilter {
    fn from(log_level: &GlobalLogLevel) -> Self {
        match log_level {
            GlobalLogLevel::Off => LevelFilter::Off,
            GlobalLogLevel::Error => LevelFilter::Error,
            GlobalLogLevel::Warn => LevelFilter::Warn,
            GlobalLogLevel::Info => LevelFilter::Info,
            GlobalLogLevel::Debug => LevelFilter::Debug,
            GlobalLogLevel::Trace => LevelFilter::Trace,
        }
    }
}

#[derive(Deserialize)]
pub struct GeyserPluginKafkaConfig {
    pub kafka_producer_config: KafkaProducerConfig,

    pub update_account_topic: String,
    pub update_slot_topic: String,
    pub notify_transaction_topic: String,
    pub notify_block_topic: String,

    pub ignore_snapshot: bool,
    pub prometheus_port: u16,
    pub global_log_level: GlobalLogLevel,
    #[cfg(feature = "filter")]
    pub filter_config_path: String,
    pub log_path: String,
}

pub fn read_config(path: &str) -> anyhow::Result<GeyserPluginKafkaConfig> {
    Ok(serde_json::from_str(&std::fs::read_to_string(path)?)?)
}
