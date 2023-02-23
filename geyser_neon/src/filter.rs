use std::sync::Arc;

use solana_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaAccountInfoVersions, ReplicaTransactionInfoVersions,
};
use tokio::{runtime::Runtime, sync::RwLock};

use crate::filter_config::FilterConfig;

#[inline(always)]
async fn check_account<'a>(
    config: Arc<RwLock<FilterConfig>>,
    owner: Option<&'a [u8]>,
    pubkey: &'a [u8],
) -> bool {
    let owner = bs58::encode(owner.unwrap_or_else(|| [].as_ref())).into_string();
    let pubkey = bs58::encode(pubkey).into_string();
    let read_guard = config.read().await;
    if read_guard.filter_include_pubkeys.contains(&pubkey)
        || read_guard.filter_include_owners.contains(&owner)
    {
        return true;
    }
    false
}

#[inline(always)]
async fn check_transaction(
    filter_config: Arc<RwLock<FilterConfig>>,
    transaction_info: &ReplicaTransactionInfoVersions<'_>,
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
        if check_account(filter_config.clone(), None, &i.to_bytes()).await {
            return true;
        }
    }

    let pubkey_iter = loaded_addresses
        .writable
        .iter()
        .chain(loaded_addresses.readonly.iter());

    for i in pubkey_iter {
        if check_account(filter_config.clone(), None, &i.to_bytes()).await {
            return true;
        }
    }

    false
}

pub fn process_transaction_info(
    runtime: Arc<Runtime>,
    filter_config: Arc<RwLock<FilterConfig>>,
    notify_transaction: &ReplicaTransactionInfoVersions<'_>,
) -> bool {
    match notify_transaction {
        ReplicaTransactionInfoVersions::V0_0_1(transaction_replica) => {
            !transaction_replica.is_vote
                && runtime.block_on(check_transaction(filter_config, notify_transaction))
        }
        ReplicaTransactionInfoVersions::V0_0_2(transaction_replica) => {
            !transaction_replica.is_vote
                && runtime.block_on(check_transaction(filter_config, notify_transaction))
        }
    }
}

pub fn process_account_info(
    runtime: Arc<Runtime>,
    config: Arc<RwLock<FilterConfig>>,
    update_account: &ReplicaAccountInfoVersions<'_>,
) -> bool {
    runtime.block_on(async move {
        match update_account {
            ReplicaAccountInfoVersions::V0_0_1(account_info) => {
                check_account(config, Some(account_info.owner), account_info.pubkey).await
            }
            ReplicaAccountInfoVersions::V0_0_2(account_info) => {
                check_account(config, Some(account_info.owner), account_info.pubkey).await
            }
        }
    })
}
