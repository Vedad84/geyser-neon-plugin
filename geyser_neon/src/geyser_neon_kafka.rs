use chrono::Utc;
use dashmap::DashMap;
use flume::Sender;
use kafka_common::kafka_structs::{
    KafkaReplicaAccountInfoVersions, KafkaReplicaTransactionInfoVersions, NotifyBlockMetaData,
    NotifyTransaction, UpdateAccount, UpdateSlotStatus,
};
use rdkafka::config::RDKafkaLogLevel;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use solana_sdk::signature::Signature;
use std::hash::Hash;
use std::{
    fs::File,
    io::Read,
    sync::{atomic::AtomicBool, Arc},
};
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
use flume::Receiver;
use std::hash::BuildHasherDefault;

use crate::filter::{Filter, FilterFactory, TheFilter};
use crate::{
    build_info::get_build_info,
    geyser_neon_config::{GeyserPluginKafkaConfig, DEFAULT_QUEUE_CAPACITY},
    kafka_producer_stats::{ContextWithStats, Stats},
    prometheus::start_prometheus,
    receivers::{
        notify_block_loop, notify_transaction_loop, update_account_loop, update_slot_status_loop,
    },
};

#[derive(Hash, Eq, PartialEq, Debug)]
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
    runtime: Arc<Runtime>,
    config: Arc<GeyserPluginKafkaConfig>,
    stats: Arc<Stats>,
    logger: &'static Logger,
    filter: TheFilter,
    account_tx: Option<Sender<UpdateAccount>>,
    slot_status_tx: Option<Sender<UpdateSlotStatus>>,
    transaction_tx: Option<Sender<NotifyTransaction>>,
    block_metadata_tx: Option<Sender<NotifyBlockMetaData>>,
    should_stop: Arc<AtomicBool>,
    prometheus_jhandle: Option<JoinHandle<()>>,
    update_account_jhandle: Option<JoinHandle<()>>,
    update_slot_status_jhandle: Option<JoinHandle<()>>,
    notify_transaction_jhandle: Option<JoinHandle<()>>,
    notify_block_jhandle: Option<JoinHandle<()>>,
    account_ordering: DashMap<AccountOrderingKey, Vec<UpdateAccount>, BuildHasherDefault<AHasher>>,
    last_ua_slot: u64,
    last_nt_slot: u64,
    ua_counter: u64,
    nt_counter: u64,
}

impl Default for GeyserPluginKafka {
    fn default() -> Self {
        Self::new()
    }
}

impl GeyserPluginKafka {
    pub fn new() -> Self {
        let runtime = Arc::new(
            runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to initialize Tokio runtime"),
        );

        let logger: &'static Logger = fast_log::init(Config::new().console().file_split(
            "/var/log/neon/geyser.log",
            LogSize::KB(512),
            RollingType::All,
            LogPacker {},
        ))
        .expect("Failed to initialize fast_log");

        let should_stop = Arc::new(AtomicBool::new(false));

        let hasher_builder = BuildHasherDefault::<AHasher>::default();

        let transaction_ordering: DashMap<
            AccountOrderingKey,
            Vec<UpdateAccount>,
            BuildHasherDefault<AHasher>,
        > = DashMap::with_capacity_and_hasher(256, hasher_builder);

        Self {
            runtime,
            config: Arc::new(GeyserPluginKafkaConfig::default()),
            stats: Arc::new(Stats::default()),
            logger,
            filter: TheFilter::default(),
            account_tx: None,
            slot_status_tx: None,
            transaction_tx: None,
            block_metadata_tx: None,
            should_stop,
            update_account_jhandle: None,
            update_slot_status_jhandle: None,
            notify_transaction_jhandle: None,
            notify_block_jhandle: None,
            prometheus_jhandle: None,
            account_ordering: transaction_ordering,
            last_ua_slot: 0,
            last_nt_slot: 0,
            ua_counter: 0,
            nt_counter: 0,
        }
    }

    fn run(
        &mut self,
        config: Arc<GeyserPluginKafkaConfig>,
        account_rx: Receiver<UpdateAccount>,
        slot_status_rx: Receiver<UpdateSlotStatus>,
        transaction_rx: Receiver<NotifyTransaction>,
        block_metadata_rx: Receiver<NotifyBlockMetaData>,
        should_stop: Arc<AtomicBool>,
    ) {
        info!(
            "Rdkafka logging level will be set to {:?}",
            Into::<RDKafkaLogLevel>::into(&config.kafka_log_level)
        );

        self.logger.set_level((&config.global_log_level).into());

        info!(
            "Global logging level is set to {:?}",
            Into::<LevelFilter>::into(&config.global_log_level)
        );

        info!("{}", get_build_info());

        let ctx_stats = ContextWithStats::default();

        self.stats = ctx_stats.stats.clone();

        let prometheus_port = config
            .prometheus_port
            .parse()
            .unwrap_or_else(|e| panic!("Wrong prometheus port number, error: {e}"));

        let prometheus_jhandle = Some(self.runtime.spawn(start_prometheus(
            ctx_stats.stats.clone(),
            config.clone(),
            prometheus_port,
        )));

        let update_account_jhandle = Some(self.runtime.spawn(update_account_loop(
            self.runtime.clone(),
            config.clone(),
            account_rx,
            ctx_stats.clone(),
            should_stop.clone(),
        )));

        let update_slot_status_jhandle = Some(self.runtime.spawn(update_slot_status_loop(
            self.runtime.clone(),
            config.clone(),
            slot_status_rx,
            ctx_stats.clone(),
            should_stop.clone(),
        )));

        let notify_transaction_jhandle = Some(self.runtime.spawn(notify_transaction_loop(
            self.runtime.clone(),
            config.clone(),
            transaction_rx,
            ctx_stats.clone(),
            should_stop.clone(),
        )));

        let notify_block_jhandle = Some(self.runtime.spawn(notify_block_loop(
            self.runtime.clone(),
            config,
            block_metadata_rx,
            ctx_stats,
            should_stop,
        )));

        self.prometheus_jhandle = prometheus_jhandle;
        self.update_account_jhandle = update_account_jhandle;
        self.update_slot_status_jhandle = update_slot_status_jhandle;
        self.notify_transaction_jhandle = notify_transaction_jhandle;
        self.notify_block_jhandle = notify_block_jhandle;
    }
}

impl std::fmt::Debug for GeyserPluginKafka {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl GeyserPlugin for GeyserPluginKafka {
    fn name(&self) -> &'static str {
        "GeyserPluginKafka"
    }

    fn on_load(&mut self, config_file: &str) -> Result<()> {
        let mut file = File::open(config_file)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let result: serde_json::Result<GeyserPluginKafkaConfig> = serde_json::from_str(&contents);
        match result {
            Err(err) => {
                return Err(GeyserPluginError::ConfigFileReadError {
                    msg: format!("The config file is not in the JSON format expected: {err:?}"),
                })
            }
            Ok(config) => {
                let config = Arc::new(config);
                self.config = config.clone();

                let update_account_queue_capacity = config
                    .update_account_queue_capacity
                    .parse::<usize>()
                    .unwrap_or(DEFAULT_QUEUE_CAPACITY);

                let update_slot_queue_capacity = config
                    .update_slot_queue_capacity
                    .parse::<usize>()
                    .unwrap_or(DEFAULT_QUEUE_CAPACITY);

                let notify_transaction_queue_capacity = config
                    .notify_transaction_queue_capacity
                    .parse::<usize>()
                    .unwrap_or(DEFAULT_QUEUE_CAPACITY);

                let notify_block_queue_capacity = config
                    .notify_block_queue_capacity
                    .parse::<usize>()
                    .unwrap_or(DEFAULT_QUEUE_CAPACITY);

                let (account_tx, account_rx) = flume::bounded(update_account_queue_capacity);
                let (slot_status_tx, slot_status_rx) = flume::bounded(update_slot_queue_capacity);
                let (transaction_tx, transaction_rx) =
                    flume::bounded(notify_transaction_queue_capacity);
                let (block_metadata_tx, block_metadata_rx) =
                    flume::bounded(notify_block_queue_capacity);

                self.account_tx = Some(account_tx);
                self.slot_status_tx = Some(slot_status_tx);
                self.transaction_tx = Some(transaction_tx);
                self.block_metadata_tx = Some(block_metadata_tx);

                self.run(
                    config,
                    account_rx,
                    slot_status_rx,
                    transaction_rx,
                    block_metadata_rx,
                    self.should_stop.clone(),
                );
            }
        }

        self.filter = TheFilter::new(&self.config).map_err(|e| {
            error!("Failed to initialize filter: {e:?}");
            GeyserPluginError::ConfigFileReadError {
                msg: format!("Failed to initialize filter: {e:?}"),
            }
        })?;

        Ok(())
    }

    fn on_unload(&mut self) {
        self.should_stop
            .store(true, std::sync::atomic::Ordering::SeqCst);
        info!("Unloading plugin: {}", self.name());
        let update_account_jhandle = self.update_account_jhandle.take();
        let update_slot_status_jhandle = self.update_slot_status_jhandle.take();
        let notify_transaction_jhandle = self.notify_transaction_jhandle.take();
        let notify_block_jhandle = self.notify_block_jhandle.take();
        let prometheus_handle = self.prometheus_jhandle.take();

        self.runtime.block_on(async move {
            if let Some(handle) = update_account_jhandle {
                let _ = handle.await;
            }

            if let Some(handle) = update_slot_status_jhandle {
                let _ = handle.await;
            }

            if let Some(handle) = notify_transaction_jhandle {
                let _ = handle.await;
            }

            if let Some(handle) = notify_block_jhandle {
                let _ = handle.await;
            }

            if let Some(handle) = prometheus_handle {
                let _ = handle.await;
            }
        });

        self.logger.flush();
    }

    fn update_account(
        &mut self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> Result<()> {
        if !(self.config.ignore_snapshot && is_startup)
            && self.filter.process_account_info(&account)
        {
            if self.last_ua_slot != slot {
                info!(
                    "Processed {} update account events for the slot {}",
                    self.ua_counter, self.last_ua_slot
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
                let account_key = AccountOrderingKey::new(slot, *signature);
                self.account_ordering
                    .entry(account_key)
                    .or_insert_with(Vec::new)
                    .push(update_account);
                self.stats.ordering_queue_len.inc();
            } else {
                self.account_tx
                    .as_ref()
                    .expect("Channel for UpdateAccount was not created!")
                    .send(update_account)
                    .map_err(|e| {
                        error!("Failed to send UpdateAccount, error: {}", e);
                        GeyserPluginError::AccountsUpdateError {
                            msg: format!("Failed to send UpdateAccount, error: {}", e),
                        }
                    })?;
            }
        } else {
            self.stats.filtered_events.inc();
        }

        Ok(())
    }

    fn update_slot_status(
        &mut self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> Result<()> {
        let slot_status_tx = self
            .slot_status_tx
            .as_ref()
            .expect("Channel for UpdateSlotStatus was not created!");

        self.stats
            .update_slot_queue_len
            .set(slot_status_tx.len() as f64);

        let update_slot_status = UpdateSlotStatus {
            slot,
            parent,
            status: status.into(),
            retrieved_time: Utc::now().naive_utc(),
        };

        slot_status_tx.send(update_slot_status).map_err(|e| {
            error!("Failed to send UpdateSlotStatus, error: {}", e);
            GeyserPluginError::SlotStatusUpdateError {
                msg: format!("Failed to send UpdateSlotStatus, error: {}", e),
            }
        })?;

        Ok(())
    }

    fn notify_end_of_startup(&mut self) -> Result<()> {
        info!("Notifying the end of startup for accounts notifications");

        Ok(())
    }

    fn notify_transaction(
        &mut self,
        transaction_info: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> Result<()> {
        if self.filter.process_transaction_info(&transaction_info) {
            let account_tx = self
                .account_tx
                .as_ref()
                .expect("Channel for UpdateAccount was not created!");

            let transaction_tx = self
                .transaction_tx
                .as_ref()
                .expect("Channel for NotifyTransaction was not created!");

            self.stats
                .notify_transaction_queue_len
                .set(transaction_tx.len() as f64);

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
                    account_tx
                        .send(acc)
                        .map_err(|e| GeyserPluginError::AccountsUpdateError {
                            msg: format!("Failed to send UpdateAccount, error: {}", e),
                        })?;
                }
            }

            let notify_transaction = NotifyTransaction {
                transaction_info: KafkaReplicaTransactionInfoVersions::from(transaction_info),
                slot,
                retrieved_time: Utc::now().naive_utc(),
            };

            transaction_tx.send(notify_transaction).map_err(|e| {
                GeyserPluginError::TransactionUpdateError {
                    msg: format!("Failed to send NotifyTransaction, error: {}", e),
                }
            })?;
        } else {
            self.stats.filtered_events.inc();
        }

        Ok(())
    }

    fn notify_block_metadata(&mut self, block_info: ReplicaBlockInfoVersions) -> Result<()> {
        let block_metadata_tx = self
            .block_metadata_tx
            .as_ref()
            .expect("Channel for NotifyBlockMetaData was not created!");

        self.stats
            .notify_block_queue_len
            .set(block_metadata_tx.len() as f64);

        let notify_block = NotifyBlockMetaData {
            block_info: block_info.into(),
            retrieved_time: Utc::now().naive_utc(),
        };

        if let Err(e) = block_metadata_tx.send(notify_block) {
            error!("Failed to send NotifyBlockMetaData, error: {}", e);
        }

        Ok(())
    }

    /// Check if the plugin is interested in account data
    /// Default is true -- if the plugin is not interested in
    /// account data, please return false.
    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    /// Check if the plugin is interested in transaction data
    fn transaction_notifications_enabled(&self) -> bool {
        true
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the GeyserPluginKafka pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = GeyserPluginKafka::new();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
