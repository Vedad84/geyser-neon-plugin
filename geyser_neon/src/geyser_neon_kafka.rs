use chrono::Utc;
use dashmap::DashMap;

use kafka_common::kafka_structs::{
    KafkaReplicaAccountInfoVersions, KafkaReplicaTransactionInfoVersions, NotifyBlockMetaData,
    NotifyTransaction, UpdateAccount, UpdateSlotStatus,
};

use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use solana_sdk::signature::Signature;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::Arc;
use tokio::{
    runtime::{self, Runtime},
    task::JoinHandle,
};

use {
    log::*,
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaTransactionInfoVersions, Result, SlotStatus,
    },
};

use ahash::AHasher;
use fast_log::{
    consts::LogSize,
    plugin::{file_split::RollingType, packer::LogPacker},
    Config, Logger,
};

use kafka_common::hash::GetHash;

use kafka_common::message_type::GetMessageType;

use rdkafka::producer::{BaseRecord, Producer, ThreadedProducer};

use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use serde::Serialize;
use std::hash::BuildHasherDefault;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;

use crate::filter::{Filter, FilterFactory, TheFilter};

use crate::geyser_neon_config::read_config;
use crate::kafka_producer::ContextWithStats;
use crate::kafka_producer::{create_producer, DeliveryOpaqueData};
use crate::prometheus::{build_registry, start_metrics_server};
use crate::{
    build_info::get_build_info, geyser_neon_config::GeyserPluginKafkaConfig, stats::Stats,
};

#[allow(clippy::large_enum_variant)]
pub enum GeyserPluginState {
    Unitialized,
    Initialized(GeyserPluginKafka),
}

impl Debug for GeyserPluginState {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl GeyserPlugin for GeyserPluginState {
    fn name(&self) -> &'static str {
        "GeyserPluginKafka"
    }

    fn on_load(&mut self, config_file: &str) -> Result<()> {
        match self {
            GeyserPluginState::Unitialized => {
                *self = GeyserPluginState::Initialized(
                    GeyserPluginKafka::new(config_file)
                        .map_err(|error| GeyserPluginError::Custom(error.into()))?,
                );
                Ok(())
            }
            GeyserPluginState::Initialized(_) => unreachable!(),
        }
    }

    fn on_unload(&mut self) {
        match self {
            GeyserPluginState::Unitialized => unreachable!(),
            GeyserPluginState::Initialized(_) => {
                info!("Unloading plugin: {}", self.name());
                *self = GeyserPluginState::Unitialized;
                info!("Unloaded plugin: {}", self.name());
            }
        }
    }

    fn update_account(
        &mut self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> Result<()> {
        self.run_if_initialized(|plugin| plugin.update_account(account, slot, is_startup))
    }

    fn notify_end_of_startup(&mut self) -> Result<()> {
        self.run_if_initialized(|plugin| plugin.notify_end_of_startup())
    }

    fn update_slot_status(
        &mut self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> Result<()> {
        self.run_if_initialized(|plugin| plugin.update_slot_status(slot, parent, status))
    }

    fn notify_transaction(
        &mut self,
        transaction: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> Result<()> {
        self.run_if_initialized(|plugin| plugin.notify_transaction(transaction, slot))
    }

    fn notify_block_metadata(&mut self, blockinfo: ReplicaBlockInfoVersions) -> Result<()> {
        self.run_if_initialized(|plugin| plugin.notify_block_metadata(blockinfo))
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }
}

impl GeyserPluginState {
    fn run_if_initialized<T, F: FnOnce(&mut GeyserPluginKafka) -> T>(&mut self, block: F) -> T {
        match self {
            GeyserPluginState::Unitialized => unreachable!(),
            GeyserPluginState::Initialized(plugin) => block(plugin),
        }
    }
}

#[derive(Hash, Eq, PartialEq)]
struct AccountOrderingKey {
    pub slot: u64,
    pub txn_signature: Signature,
}

impl AccountOrderingKey {
    pub fn new(slot: u64, txn_signature: Signature) -> Self {
        Self {
            slot,
            txn_signature,
        }
    }
}

pub struct GeyserPluginKafka {
    loaded_instant: Instant,
    internal_state: InternalState,
    last_ua_slot: u64,
    last_nt_slot: u64,
    ua_counter: u64,
    nt_counter: u64,
    account_ordering: DashMap<AccountOrderingKey, Vec<UpdateAccount>, BuildHasherDefault<AHasher>>,
    logger: &'static Logger,
    filter: TheFilter,
    kafka_producer: ThreadedProducer<ContextWithStats>,
    shutdown_tx: Option<Sender<()>>,
    prometheus_jhandle: Option<JoinHandle<()>>,
    runtime: Runtime,
    config: Arc<GeyserPluginKafkaConfig>,
    stats: Arc<Stats>,
}

enum InternalState {
    Initialized,
    Startup,
    Running,
}

impl Drop for GeyserPluginKafka {
    fn drop(&mut self) {
        info!("Drop: Dropping plugin");

        info!(
            "Drop: Kafka producer in_flight_count={}",
            self.kafka_producer.in_flight_count()
        );

        info!("Drop: Flushing Kafka producer");
        let start = Instant::now();
        match self.kafka_producer.flush(Duration::from_millis(
            self.config.kafka_producer_config.flush_timeout_ms,
        )) {
            Ok(()) => info!(
                "Drop: Flushed Kafka producer elapsed={}s",
                start.elapsed().as_secs()
            ),
            Err(e) => error!(
                "Drop: Flush Kafka producer error={:#?} elapsed={}s",
                e,
                start.elapsed().as_secs()
            ),
        }

        let _ = self
            .shutdown_tx
            .take()
            .expect("shutdown_tx should not be empty")
            .send(());
        info!("Drop: Sent shutdown token to metrics server");

        self.runtime
            .block_on(
                self.prometheus_jhandle
                    .take()
                    .expect("prometheus_jhandle should not be empty"),
            )
            .expect("Future should not fail");
        info!("Drop: Metrics server stopped");

        self.logger.flush();
        info!("Drop: Flushed logger");
    }
}

impl GeyserPluginKafka {
    fn new(config_file: &str) -> anyhow::Result<Self> {
        let config = Arc::new(read_config(config_file)?);

        let logger = fast_log::init(Config::new().console().file_split(
            &config.log_path,
            LogSize::KB(512),
            RollingType::All, // TODO: This will fill up disk space eventually
            LogPacker {},
        ))?;

        logger.set_level((&config.global_log_level).into());

        info!(
            "Global logging level is set to {:?}",
            Into::<LevelFilter>::into(&config.global_log_level)
        );

        info!("{}", get_build_info());

        let stats = Arc::new(Stats::default());

        let runtime = runtime::Builder::new_multi_thread().enable_all().build()?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        let plugin = GeyserPluginKafka {
            loaded_instant: Instant::now(),
            internal_state: InternalState::Initialized,
            last_ua_slot: 0,
            last_nt_slot: 0,
            ua_counter: 0,
            nt_counter: 0,
            account_ordering: DashMap::with_capacity_and_hasher(
                256,
                BuildHasherDefault::<AHasher>::default(),
            ),
            logger,
            filter: TheFilter::new(&config)?,
            kafka_producer: create_producer(config.clone(), stats.clone())?,
            shutdown_tx: Some(shutdown_tx),
            prometheus_jhandle: Some(runtime.spawn(start_metrics_server(
                build_registry(&stats, &config),
                config.prometheus_port,
                shutdown_rx,
            ))),
            runtime,
            config,
            stats,
        };

        info!("InternalState: Initialized GeyserPluginKafka");

        Ok(plugin)
    }

    fn update_account(
        &mut self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> Result<()> {
        self.update_internal_state(is_startup);

        if (self.config.ignore_snapshot && is_startup)
            || !self.filter.process_account_info(&account)
        {
            self.stats.filtered_events.inc();
            return Ok(());
        }

        if self.last_ua_slot != slot {
            info!(
                "Processed {} update account events is_startup={} for the slot {}",
                self.ua_counter, is_startup, self.last_ua_slot
            );
            self.last_ua_slot = slot;
            self.ua_counter = 1;
        } else {
            self.ua_counter += 1;
        }

        let account_v2 = match account {
            ReplicaAccountInfoVersions::V0_0_1(_) => unreachable!(),
            ReplicaAccountInfoVersions::V0_0_2(a) => a,
        };

        let update_account = UpdateAccount {
            account: KafkaReplicaAccountInfoVersions::from(account),
            slot,
            is_startup,
            retrieved_time: Utc::now().naive_utc(),
        };

        if let Some(signature) = account_v2.txn_signature {
            self.account_ordering
                .entry(AccountOrderingKey::new(slot, *signature))
                .or_insert_with(Vec::new)
                .push(update_account);
            self.stats.ordering_queue_len.inc();
            Ok(())
        } else {
            self.send(&self.config.clone().update_account_topic, update_account)
        }
    }

    fn update_internal_state(&mut self, is_startup: bool) {
        match self.internal_state {
            InternalState::Initialized => {
                if is_startup {
                    info!(
                        "InternalState: first update_account is_startup=true elapsed={}s",
                        self.loaded_instant.elapsed().as_secs()
                    );
                    self.internal_state = InternalState::Startup;
                } else {
                    error!("InternalState: Invalid state");
                }
            }
            InternalState::Startup => {
                if !is_startup {
                    info!(
                        "InternalState: finished startup elapsed={}s",
                        self.loaded_instant.elapsed().as_secs()
                    );
                    self.internal_state = InternalState::Running;
                }
            }
            InternalState::Running => {}
        }
    }

    fn update_slot_status(
        &mut self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> Result<()> {
        self.send(
            &self.config.clone().update_slot_topic,
            UpdateSlotStatus {
                slot,
                parent,
                status: status.into(),
                retrieved_time: Utc::now().naive_utc(),
            },
        )
    }

    fn notify_end_of_startup(&mut self) -> Result<()> {
        info!(
            "InternalState: notify_end_of_startup elapsed={}s",
            self.loaded_instant.elapsed().as_secs()
        );
        Ok(())
    }

    fn notify_transaction(
        &mut self,
        transaction_info: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> Result<()> {
        if !self.filter.process_transaction_info(&transaction_info) {
            self.stats.filtered_events.inc();
            return Ok(());
        }

        if self.last_nt_slot != slot {
            info!(
                "Processed {} notify transaction events for the slot {}",
                self.nt_counter, self.last_nt_slot
            );
            self.last_nt_slot = slot;
            self.nt_counter = 1;
        } else {
            self.nt_counter += 1;
        }

        let transaction_info_v2 = match transaction_info {
            ReplicaTransactionInfoVersions::V0_0_1(_) => unreachable!(),
            ReplicaTransactionInfoVersions::V0_0_2(info) => info,
        };

        let key = AccountOrderingKey::new(slot, *transaction_info_v2.signature);

        if let Some((_, update_account_vec)) = self.account_ordering.remove(&key) {
            for mut acc in update_account_vec.into_iter() {
                acc.set_write_version(transaction_info_v2.index as i64);
                self.stats.ordering_queue_len.dec();
                self.send(&self.config.clone().update_account_topic, acc)?
            }
        }

        self.send(
            &self.config.clone().notify_transaction_topic,
            NotifyTransaction {
                transaction_info: KafkaReplicaTransactionInfoVersions::from(transaction_info),
                slot,
                retrieved_time: Utc::now().naive_utc(),
            },
        )
    }

    fn notify_block_metadata(&mut self, block_info: ReplicaBlockInfoVersions) -> Result<()> {
        self.send(
            &self.config.clone().notify_block_topic,
            NotifyBlockMetaData {
                block_info: block_info.into(),
                retrieved_time: Utc::now().naive_utc(),
            },
        )
    }

    fn send<T: GetHash + Serialize + Debug + GetMessageType>(
        &mut self,
        topic: &str,
        message: T,
    ) -> Result<()> {
        let message_type = message.get_type();
        let key = message.get_hash();
        let payload = serde_json::to_string(&message).map_err(|e| {
            self.stats
                .stats_for_message_type(&message_type)
                .serialize_errors
                .inc();
            error!("Send: Failed to serialize {message:?} message, error {e}");
            GeyserPluginError::Custom(Box::new(e))
        })?;
        let mut base_record = BaseRecord::with_opaque_to(
            topic,
            Box::new(DeliveryOpaqueData::new(
                message.get_type(),
                message.get_slot(),
                self.stats.clone(),
            )),
        )
        .key(&key)
        .payload(&payload);

        loop {
            match self.kafka_producer.send(base_record) {
                Err((e, record))
                    if e == KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull) =>
                {
                    self.stats
                        .stats_for_message_type(&message_type)
                        .enqueue_retries
                        .inc();
                    base_record = record;
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err((e, record)) => {
                    self.stats
                        .stats_for_message_type(&message_type)
                        .enqueue_errors
                        .inc();
                    error!("Send: Failed to send record {record:?}, error {e}");

                    if let Some((error_code, error_message)) =
                        self.kafka_producer.client().fatal_error()
                    {
                        error!("Send: Recreating the producer because of fatal error: error_code={error_code} error_message={error_message}");

                        self.kafka_producer = create_producer(self.config.clone(), self.stats.clone()).map_err(|e| {
                            error!("Send: Failed to recreate the producer: error_code={error_code} error_message={error_message}");
                            self.stats.producer_recreation_errors.inc();
                            GeyserPluginError::Custom(e.into())
                        })?;

                        self.stats.producer_recreations.inc();

                        self.stats
                            .stats_for_message_type(&message_type)
                            .enqueue_retries
                            .inc();
                        base_record = record;
                    } else {
                        return Err(GeyserPluginError::Custom(Box::new(e)));
                    }
                }
                Ok(_) => {
                    self.stats
                        .stats_for_message_type(&message_type)
                        .enqueued
                        .inc();
                    return Ok(());
                }
            }
        }
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the GeyserPluginKafka pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    Box::into_raw(Box::new(GeyserPluginState::Unitialized))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_unload() -> anyhow::Result<()> {
        let mut plugin = GeyserPluginState::Unitialized;
        plugin.on_load("test-config.json")?;
        plugin.on_unload();
        info!("Unloaded plugin");
        std::fs::remove_file("logs/geyser.log")?;
        Ok(())
    }
}
