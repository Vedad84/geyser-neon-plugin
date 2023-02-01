use std::sync::Arc;

use log::trace;
use solana_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaAccountInfoVersions, ReplicaTransactionInfoVersions,
};

use crate::geyser_neon_config::GeyserPluginKafkaConfig;

#[inline(always)]
pub fn check_account<'a>(
    config: Arc<GeyserPluginKafkaConfig>,
    owner: Option<&'a [u8]>,
    pubkey: &'a [u8],
) -> bool {
    let empty: [u8; 0] = [];
    let owner = bs58::encode(owner.unwrap_or_else(|| empty.as_ref())).into_string();
    let pubkey = bs58::encode(pubkey).into_string();
    if config.filter_include_pubkeys.contains(&pubkey)
        || config.filter_include_owners.contains(&owner)
    {
        trace!(
            "Add update_account entry to db queue for pubkey {}, owner {}",
            pubkey,
            owner
        );
        return true;
    }
    false
}

#[inline(always)]
fn check_transaction(
    _config: Arc<GeyserPluginKafkaConfig>,
    _transaction_info: &ReplicaTransactionInfoVersions,
) -> bool {
    false
}

pub fn process_transaction_info(
    config: Arc<GeyserPluginKafkaConfig>,
    notify_transaction: &ReplicaTransactionInfoVersions,
) -> bool {
    match notify_transaction {
        ReplicaTransactionInfoVersions::V0_0_1(transaction_replica) => {
            if !transaction_replica.is_vote && check_transaction(config, notify_transaction) {
                return true;
            }
        }
        ReplicaTransactionInfoVersions::V0_0_2(transaction_replica) => {
            if !transaction_replica.is_vote && check_transaction(config, notify_transaction) {
                return true;
            }
        }
    }
    false
}

pub fn process_account_info(
    config: Arc<GeyserPluginKafkaConfig>,
    update_account: &ReplicaAccountInfoVersions,
) -> bool {
    match &update_account {
        // for 1.13.x or earlier
        ReplicaAccountInfoVersions::V0_0_1(account_info) => {
            if check_account(config, Some(account_info.owner), account_info.pubkey) {
                return true;
            }
        }
        ReplicaAccountInfoVersions::V0_0_2(account_info) => {
            if check_account(config, Some(account_info.owner), account_info.pubkey) {
                return true;
            }
        }
    }
    false
}
