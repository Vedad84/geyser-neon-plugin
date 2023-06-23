use crate::filter::{Filter, FilterFactory};
use crate::geyser_neon_config::GeyserPluginKafkaConfig;
use solana_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaAccountInfoVersions, ReplicaTransactionInfoVersions,
};

pub struct DummyFilter;

impl FilterFactory for DummyFilter {
    fn new(_config: &GeyserPluginKafkaConfig) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(DummyFilter {})
    }
}

impl Filter for DummyFilter {
    fn process_transaction_info(
        &self,
        _notify_transaction: &ReplicaTransactionInfoVersions<'_>,
    ) -> bool {
        true
    }

    fn process_account_info(&self, _update_account: &ReplicaAccountInfoVersions<'_>) -> bool {
        true
    }
}
