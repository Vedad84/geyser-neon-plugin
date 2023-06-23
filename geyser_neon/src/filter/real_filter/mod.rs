use std::sync::Arc;

use solana_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaAccountInfoVersions, ReplicaTransactionInfoVersions,
};
use solana_sdk::transaction::SanitizedTransaction;

use crate::filter::real_filter::filter_config::FilterConfigKeys;
use crate::filter::real_filter::filter_config_hot_reload::{watcher, WatcherHelper};
use crate::filter::{Filter, FilterFactory};
use crate::geyser_neon_config::GeyserPluginKafkaConfig;

mod filter_config;
mod filter_config_hot_reload;

pub struct RealFilter {
    watcher_helper: Arc<WatcherHelper>,
}

impl FilterFactory for RealFilter {
    fn new(config: &GeyserPluginKafkaConfig) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let watcher_helper = watcher(&config.filter_config_path)?;
        Ok(RealFilter { watcher_helper })
    }
}

impl Filter for RealFilter {
    fn process_transaction_info(
        &self,
        notify_transaction: &ReplicaTransactionInfoVersions<'_>,
    ) -> bool {
        self.watcher_helper
            .current_filter_config
            .read()
            .unwrap()
            .0
            .process_transaction_info(notify_transaction)
    }

    fn process_account_info(&self, update_account: &ReplicaAccountInfoVersions<'_>) -> bool {
        self.watcher_helper
            .current_filter_config
            .read()
            .unwrap()
            .0
            .process_account_info(update_account)
    }
}

impl FilterConfigKeys {
    #[inline(always)]
    fn check_account(&self, owner: &[u8], pubkey: &[u8]) -> bool {
        self.filter_include_pubkeys.contains(pubkey) || self.filter_include_owners.contains(owner)
    }

    #[inline(always)]
    fn check_transaction_account(&self, pubkey: &[u8]) -> bool {
        self.filter_include_pubkeys.contains(pubkey) || self.filter_include_owners.contains(pubkey)
    }

    #[inline(always)]
    fn check_transaction(&self, transaction: &SanitizedTransaction) -> bool {
        transaction
            .message()
            .account_keys()
            .iter()
            .any(|key| self.check_transaction_account(&key.to_bytes()))
    }
}

impl Filter for FilterConfigKeys {
    fn process_transaction_info(
        &self,
        notify_transaction: &ReplicaTransactionInfoVersions<'_>,
    ) -> bool {
        match notify_transaction {
            ReplicaTransactionInfoVersions::V0_0_1(transaction_replica) => {
                !transaction_replica.is_vote
                    && self.check_transaction(transaction_replica.transaction)
            }
            ReplicaTransactionInfoVersions::V0_0_2(transaction_replica) => {
                !transaction_replica.is_vote
                    && self.check_transaction(transaction_replica.transaction)
            }
        }
    }

    fn process_account_info(&self, update_account: &ReplicaAccountInfoVersions<'_>) -> bool {
        match update_account {
            ReplicaAccountInfoVersions::V0_0_1(account_info) => {
                self.check_account(account_info.owner, account_info.pubkey)
            }
            ReplicaAccountInfoVersions::V0_0_2(account_info) => {
                self.check_account(account_info.owner, account_info.pubkey)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use solana_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfo, ReplicaAccountInfoV2, ReplicaTransactionInfo, ReplicaTransactionInfoV2,
    };
    use solana_sdk::hash::Hash;
    use solana_sdk::message::{Message, MessageHeader};
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::{Keypair, Signature};
    use solana_sdk::signer::Signer;
    use solana_sdk::transaction::Transaction;
    use solana_transaction_status::TransactionStatusMeta;

    use crate::filter::real_filter::filter_config::FilterConfig;

    use super::*;

    impl FilterConfig {
        pub fn new(
            filter_include_owners: Vec<String>,
            filter_include_pubkeys: Vec<String>,
        ) -> FilterConfig {
            FilterConfig {
                filter_include_owners,
                filter_include_pubkeys,
            }
        }
    }

    #[test]
    fn test_bs58_encode_empty_slice() {
        assert_eq!(bs58::encode([]).into_string(), "");
    }

    #[test]
    fn test_process_account_info_v1_deny() {
        let filter_config = FilterConfig::new(
            vec![Pubkey::new_unique().to_string()],
            vec![Pubkey::new_unique().to_string()],
        )
        .to_filter_config_keys()
        .unwrap();

        assert!(
            !filter_config.process_account_info(&ReplicaAccountInfoVersions::V0_0_1(
                &ReplicaAccountInfo {
                    pubkey: &Pubkey::new_unique().to_bytes(),
                    lamports: 0,
                    owner: &Pubkey::new_unique().to_bytes(),
                    executable: false,
                    rent_epoch: 0,
                    data: &[],
                    write_version: 0
                }
            ))
        );
    }

    #[test]
    fn test_process_account_info_v2_deny() {
        let filter_config = FilterConfig::new(
            vec![Pubkey::new_unique().to_string()],
            vec![Pubkey::new_unique().to_string()],
        )
        .to_filter_config_keys()
        .unwrap();

        assert!(
            !filter_config.process_account_info(&ReplicaAccountInfoVersions::V0_0_2(
                &ReplicaAccountInfoV2 {
                    pubkey: &Pubkey::new_unique().to_bytes(),
                    lamports: 0,
                    owner: &Pubkey::new_unique().to_bytes(),
                    executable: false,
                    rent_epoch: 0,
                    data: &[],
                    write_version: 0,
                    txn_signature: None
                }
            ))
        );
    }

    #[test]
    fn test_process_account_info_v1_allow_pubkey() {
        let pubkey = Pubkey::new_unique();
        let filter_config = FilterConfig::new(
            vec![Pubkey::new_unique().to_string()],
            vec![pubkey.to_string()],
        )
        .to_filter_config_keys()
        .unwrap();

        assert!(
            filter_config.process_account_info(&ReplicaAccountInfoVersions::V0_0_1(
                &ReplicaAccountInfo {
                    pubkey: &pubkey.to_bytes(),
                    lamports: 0,
                    owner: &Pubkey::new_unique().to_bytes(),
                    executable: false,
                    rent_epoch: 0,
                    data: &[],
                    write_version: 0
                }
            ))
        );
    }

    #[test]
    fn test_process_account_info_v2_allow_pubkey() {
        let pubkey = Pubkey::new_unique();
        let filter_config = FilterConfig::new(
            vec![Pubkey::new_unique().to_string()],
            vec![pubkey.to_string()],
        )
        .to_filter_config_keys()
        .unwrap();

        assert!(
            filter_config.process_account_info(&ReplicaAccountInfoVersions::V0_0_2(
                &ReplicaAccountInfoV2 {
                    pubkey: &pubkey.to_bytes(),
                    lamports: 0,
                    owner: &Pubkey::new_unique().to_bytes(),
                    executable: false,
                    rent_epoch: 0,
                    data: &[],
                    write_version: 0,
                    txn_signature: None
                }
            ))
        );
    }

    #[test]
    fn test_process_account_info_v1_allow_owner() {
        let owner = Pubkey::new_unique();
        let filter_config = FilterConfig::new(
            vec![owner.to_string()],
            vec![Pubkey::new_unique().to_string()],
        )
        .to_filter_config_keys()
        .unwrap();

        assert!(
            filter_config.process_account_info(&ReplicaAccountInfoVersions::V0_0_1(
                &ReplicaAccountInfo {
                    pubkey: &Pubkey::new_unique().to_bytes(),
                    lamports: 0,
                    owner: &owner.to_bytes(),
                    executable: false,
                    rent_epoch: 0,
                    data: &[],
                    write_version: 0
                }
            ))
        );
    }

    #[test]
    fn test_process_account_info_v2_allow_owner() {
        let owner = Pubkey::new_unique();
        let filter_config = FilterConfig::new(
            vec![owner.to_string()],
            vec![Pubkey::new_unique().to_string()],
        )
        .to_filter_config_keys()
        .unwrap();

        assert!(
            filter_config.process_account_info(&ReplicaAccountInfoVersions::V0_0_2(
                &ReplicaAccountInfoV2 {
                    pubkey: &Pubkey::new_unique().to_bytes(),
                    lamports: 0,
                    owner: &owner.to_bytes(),
                    executable: false,
                    rent_epoch: 0,
                    data: &[],
                    write_version: 0,
                    txn_signature: None
                }
            ))
        );
    }

    #[test]
    fn test_process_transaction_info_v1_deny() {
        let filter_config = FilterConfig::new(
            vec![Pubkey::new_unique().to_string()],
            vec![Pubkey::new_unique().to_string()],
        )
        .to_filter_config_keys()
        .unwrap();

        let keypair = Keypair::new();
        let message = Message {
            header: MessageHeader {
                num_required_signatures: 1,
                ..MessageHeader::default()
            },
            account_keys: vec![keypair.pubkey(), keypair.pubkey()],
            ..Message::default()
        };

        assert!(
            !filter_config.process_transaction_info(&ReplicaTransactionInfoVersions::V0_0_1(
                &ReplicaTransactionInfo {
                    signature: &Signature::new_unique(),
                    is_vote: false,
                    transaction: &SanitizedTransaction::from_transaction_for_tests(
                        Transaction::new(&[&keypair], message, Hash::default())
                    ),
                    transaction_status_meta: &TransactionStatusMeta::default()
                }
            ))
        );
    }

    #[test]
    fn test_process_transaction_info_v2_deny() {
        let filter_config = FilterConfig::new(
            vec![Pubkey::new_unique().to_string()],
            vec![Pubkey::new_unique().to_string()],
        )
        .to_filter_config_keys()
        .unwrap();

        let keypair = Keypair::new();
        let message = Message {
            header: MessageHeader {
                num_required_signatures: 1,
                ..MessageHeader::default()
            },
            account_keys: vec![keypair.pubkey(), keypair.pubkey()],
            ..Message::default()
        };

        assert!(
            !filter_config.process_transaction_info(&ReplicaTransactionInfoVersions::V0_0_2(
                &ReplicaTransactionInfoV2 {
                    signature: &Signature::new_unique(),
                    is_vote: false,
                    transaction: &SanitizedTransaction::from_transaction_for_tests(
                        Transaction::new(&[&keypair], message, Hash::default())
                    ),
                    transaction_status_meta: &TransactionStatusMeta::default(),
                    index: 0
                }
            ))
        );
    }

    #[test]
    fn test_process_transaction_info_v1_allow_owner() {
        let keypair = Keypair::new();
        let filter_config = FilterConfig::new(
            vec![keypair.pubkey().to_string()],
            vec![Pubkey::new_unique().to_string()],
        )
        .to_filter_config_keys()
        .unwrap();

        let message = Message {
            header: MessageHeader {
                num_required_signatures: 1,
                ..MessageHeader::default()
            },
            account_keys: vec![keypair.pubkey(), keypair.pubkey()],
            ..Message::default()
        };

        assert!(
            filter_config.process_transaction_info(&ReplicaTransactionInfoVersions::V0_0_1(
                &ReplicaTransactionInfo {
                    signature: &Signature::new_unique(),
                    is_vote: false,
                    transaction: &SanitizedTransaction::from_transaction_for_tests(
                        Transaction::new(&[&keypair], message, Hash::default())
                    ),
                    transaction_status_meta: &TransactionStatusMeta::default()
                }
            ))
        );
    }

    #[test]
    fn test_process_transaction_info_v2_allow_owner() {
        let keypair = Keypair::new();
        let filter_config = FilterConfig::new(
            vec![keypair.pubkey().to_string()],
            vec![Pubkey::new_unique().to_string()],
        )
        .to_filter_config_keys()
        .unwrap();

        let message = Message {
            header: MessageHeader {
                num_required_signatures: 1,
                ..MessageHeader::default()
            },
            account_keys: vec![keypair.pubkey(), keypair.pubkey()],
            ..Message::default()
        };

        assert!(
            filter_config.process_transaction_info(&ReplicaTransactionInfoVersions::V0_0_2(
                &ReplicaTransactionInfoV2 {
                    signature: &Signature::new_unique(),
                    is_vote: false,
                    transaction: &SanitizedTransaction::from_transaction_for_tests(
                        Transaction::new(&[&keypair], message, Hash::default())
                    ),
                    transaction_status_meta: &TransactionStatusMeta::default(),
                    index: 0
                }
            ))
        );
    }

    #[test]
    fn test_process_transaction_info_v1_allow_pubkeys() {
        let keypair = Keypair::new();
        let filter_config = FilterConfig::new(
            vec![Pubkey::new_unique().to_string()],
            vec![keypair.pubkey().to_string()],
        )
        .to_filter_config_keys()
        .unwrap();

        let message = Message {
            header: MessageHeader {
                num_required_signatures: 1,
                ..MessageHeader::default()
            },
            account_keys: vec![keypair.pubkey(), keypair.pubkey()],
            ..Message::default()
        };

        assert!(
            filter_config.process_transaction_info(&ReplicaTransactionInfoVersions::V0_0_1(
                &ReplicaTransactionInfo {
                    signature: &Signature::new_unique(),
                    is_vote: false,
                    transaction: &SanitizedTransaction::from_transaction_for_tests(
                        Transaction::new(&[&keypair], message, Hash::default())
                    ),
                    transaction_status_meta: &TransactionStatusMeta::default()
                }
            ))
        );
    }

    #[test]
    fn test_process_transaction_info_v2_allow_pubkeys() {
        let keypair = Keypair::new();
        let filter_config = FilterConfig::new(
            vec![Pubkey::new_unique().to_string()],
            vec![keypair.pubkey().to_string()],
        )
        .to_filter_config_keys()
        .unwrap();

        let message = Message {
            header: MessageHeader {
                num_required_signatures: 1,
                ..MessageHeader::default()
            },
            account_keys: vec![keypair.pubkey(), keypair.pubkey()],
            ..Message::default()
        };

        assert!(
            filter_config.process_transaction_info(&ReplicaTransactionInfoVersions::V0_0_2(
                &ReplicaTransactionInfoV2 {
                    signature: &Signature::new_unique(),
                    is_vote: false,
                    transaction: &SanitizedTransaction::from_transaction_for_tests(
                        Transaction::new(&[&keypair], message, Hash::default())
                    ),
                    transaction_status_meta: &TransactionStatusMeta::default(),
                    index: 0
                }
            ))
        );
    }
}
