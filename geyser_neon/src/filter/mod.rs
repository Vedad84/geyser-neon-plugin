use crate::geyser_neon_config::GeyserPluginKafkaConfig;
use solana_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaAccountInfoVersions, ReplicaTransactionInfoVersions,
};

#[cfg(not(feature = "filter"))]
pub mod dummy_filter;

#[cfg(not(feature = "filter"))]
pub type TheFilter = dummy_filter::DummyFilter;

#[cfg(feature = "filter")]
pub mod real_filter;

#[cfg(feature = "filter")]
pub type TheFilter = real_filter::RealFilter;

pub trait FilterFactory {
    fn new(config: &GeyserPluginKafkaConfig) -> anyhow::Result<Self>
    where
        Self: Sized;
}

pub trait Filter {
    fn process_transaction_info(
        &self,
        notify_transaction: &ReplicaTransactionInfoVersions<'_>,
    ) -> bool;

    fn process_account_info(&self, update_account: &ReplicaAccountInfoVersions<'_>) -> bool;
}
