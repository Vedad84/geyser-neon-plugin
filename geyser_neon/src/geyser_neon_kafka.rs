use std::sync::{atomic::AtomicBool, Arc};

use flume::Sender;
use kafka_common::kafka_structs::{
    KafkaReplicaAccountInfoVersions, KafkaReplicaBlockInfoVersions,
    KafkaReplicaTransactionInfoVersions, KafkaSlotStatus, NotifyBlockMetaData, NotifyTransaction,
    UpdateAccount, UpdateSlotStatus,
};
use thiserror::Error;
use tokio::runtime::{self, Runtime};

/// Main entry for the Kafka plugin
use {
    log::*,
    serde_derive::{Deserialize, Serialize},
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaTransactionInfoVersions, Result, SlotStatus,
    },
};

use crate::receivers::{
    notify_block_loop, notify_transaction_loop, update_account_loop, update_slot_status_loop,
};

#[allow(dead_code)]
pub struct GeyserPluginKafka {
    runtime: Runtime,
    account_tx: Sender<UpdateAccount>,
    slot_status_tx: Sender<UpdateSlotStatus>,
    transaction_tx: Sender<NotifyTransaction>,
    block_metadata_tx: Sender<NotifyBlockMetaData>,
    should_stop: Arc<AtomicBool>,
}

impl Default for GeyserPluginKafka {
    fn default() -> Self {
        Self::new()
    }
}

impl GeyserPluginKafka {
    pub fn new() -> Self {
        let runtime = runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to initialize Tokio runtime");

        let should_stop = Arc::new(AtomicBool::new(false));

        let (account_tx, account_rx) = flume::unbounded();
        let (slot_status_tx, slot_status_rx) = flume::unbounded();
        let (transaction_tx, transaction_rx) = flume::unbounded();
        let (block_metadata_tx, block_metadata_rx) = flume::unbounded();

        tokio::spawn(update_account_loop(account_rx, should_stop.clone()));
        tokio::spawn(update_slot_status_loop(slot_status_rx, should_stop.clone()));
        tokio::spawn(notify_transaction_loop(transaction_rx, should_stop.clone()));
        tokio::spawn(notify_block_loop(block_metadata_rx, should_stop.clone()));

        Self {
            runtime,
            account_tx,
            slot_status_tx,
            transaction_tx,
            block_metadata_tx,
            should_stop,
        }
    }
}

/// The Configuration for the Kafka plugin
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GeyserPluginKafkaConfig {}

#[derive(Error, Debug)]
pub enum GeyserPluginKafkaError {}

impl std::fmt::Debug for GeyserPluginKafka {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl GeyserPlugin for GeyserPluginKafka {
    fn name(&self) -> &'static str {
        "GeyserPluginKafka"
    }

    fn on_load(&mut self, _config_file: &str) -> Result<()> {
        Ok(())
    }

    fn on_unload(&mut self) {
        self.should_stop
            .store(true, std::sync::atomic::Ordering::SeqCst);
        info!("Unloading plugin: {}", self.name());
    }

    fn update_account(
        &mut self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> Result<()> {
        let account: KafkaReplicaAccountInfoVersions = account.into();
        let account_tx = self.account_tx.clone();

        self.runtime.spawn(async move {
            let update_account = UpdateAccount {
                account,
                slot,
                is_startup,
            };
            match account_tx.send_async(update_account).await {
                Ok(_) => todo!(),
                Err(_) => todo!(),
            }
        });

        Ok(())
    }

    fn update_slot_status(
        &mut self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> Result<()> {
        info!("Updating slot {:?} at with status {:?}", slot, status);

        let status: KafkaSlotStatus = status.into();
        let slot_status_tx = self.slot_status_tx.clone();

        self.runtime.spawn(async move {
            let update_account = UpdateSlotStatus {
                slot,
                parent,
                status,
            };
            match slot_status_tx.send_async(update_account).await {
                Ok(_) => todo!(),
                Err(_) => todo!(),
            }
        });

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
        let transaction_info: KafkaReplicaTransactionInfoVersions = transaction_info.into();
        let transaction_tx = self.transaction_tx.clone();

        self.runtime.spawn(async move {
            let notify_transaction = NotifyTransaction {
                transaction_info,
                slot,
            };

            match transaction_tx.send_async(notify_transaction).await {
                Ok(_) => todo!(),
                Err(_) => todo!(),
            }
        });

        Ok(())
    }

    fn notify_block_metadata(&mut self, block_info: ReplicaBlockInfoVersions) -> Result<()> {
        let block_info: KafkaReplicaBlockInfoVersions = block_info.into();
        let block_metadata_tx = self.block_metadata_tx.clone();

        self.runtime.spawn(async move {
            let notify_block = NotifyBlockMetaData { block_info };

            match block_metadata_tx.send_async(notify_block).await {
                Ok(_) => todo!(),
                Err(_) => todo!(),
            }
        });

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
