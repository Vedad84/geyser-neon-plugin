use std::sync::Arc;

use solana_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaAccountInfoVersions, ReplicaTransactionInfoVersions,
};

use crate::filter_config::FilterConfig;

#[inline(always)]
fn check_account<'a>(config: Arc<FilterConfig>, owner: Option<&'a [u8]>, pubkey: &'a [u8]) -> bool {
    let owner = bs58::encode(owner.unwrap_or_else(|| [].as_ref())).into_string();
    let pubkey = bs58::encode(pubkey).into_string();
    if config.filter_include_pubkeys.contains(&pubkey)
        || config.filter_include_owners.contains(&owner)
    {
        return true;
    }
    false
}

#[inline(always)]
fn check_transaction(
    config: Arc<FilterConfig>,
    transaction_info: &ReplicaTransactionInfoVersions,
) -> bool {
    let (keys, loaded_addresses) = match transaction_info {
        ReplicaTransactionInfoVersions::V0_0_1(replica) => (
            replica.transaction.message().account_keys().iter(),
            replica.transaction.get_loaded_addresses(),
        ),
        ReplicaTransactionInfoVersions::V0_0_2(replica) => (
            replica.transaction.message().account_keys().iter(),
            replica.transaction.get_loaded_addresses(),
        ),
    };

    for i in keys {
        if check_account(config.clone(), None, &i.to_bytes()) {
            return true;
        }
    }

    let pubkey_iter = loaded_addresses
        .writable
        .iter()
        .chain(loaded_addresses.readonly.iter());

    for i in pubkey_iter {
        if check_account(config.clone(), None, &i.to_bytes()) {
            return true;
        }
    }

    false
}

pub fn process_transaction_info(
    config: Arc<FilterConfig>,
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
    config: Arc<FilterConfig>,
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
