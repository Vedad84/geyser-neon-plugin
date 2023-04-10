use core::fmt;

use solana_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaAccountInfo, ReplicaAccountInfoV2, ReplicaAccountInfoVersions, ReplicaBlockInfo,
    ReplicaBlockInfoVersions, ReplicaTransactionInfo, ReplicaTransactionInfoV2,
    ReplicaTransactionInfoVersions, SlotStatus,
};
use solana_program::message::SanitizedMessage;
use solana_sdk::transaction::SanitizedTransaction;
use solana_transaction_status::{TransactionStatusMeta, TransactionTokenBalance};

use crate::kafka_structs::{
    KafkaLegacyMessage, KafkaLoadedMessage, KafkaReplicaAccountInfo, KafkaReplicaAccountInfoV2,
    KafkaReplicaAccountInfoVersions, KafkaReplicaBlockInfo, KafkaReplicaBlockInfoVersions,
    KafkaReplicaTransactionInfo, KafkaReplicaTransactionInfoV2,
    KafkaReplicaTransactionInfoVersions, KafkaSanitizedMessage, KafkaSanitizedTransaction,
    KafkaSlotStatus, KafkaTransactionStatusMeta, KafkaTransactionTokenBalance, UpdateAccount,
};

impl fmt::Display for KafkaSlotStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            KafkaSlotStatus::Processed => write!(f, "Processed"),
            KafkaSlotStatus::Rooted => write!(f, "Rooted"),
            KafkaSlotStatus::Confirmed => write!(f, "Confirmed"),
        }
    }
}

impl From<&SanitizedMessage> for KafkaSanitizedMessage {
    fn from(sanitized_message: &SanitizedMessage) -> Self {
        match sanitized_message {
            SanitizedMessage::Legacy(message) => {
                let lm = KafkaLegacyMessage {
                    message: message.to_owned().message.into_owned(),
                    is_writable_account_cache: message.is_writable_account_cache.clone(),
                };
                KafkaSanitizedMessage::Legacy(lm)
            }
            SanitizedMessage::V0(message) => {
                let lm = KafkaLoadedMessage {
                    message: message.to_owned().message.into_owned(),
                    loaded_addresses: message.to_owned().loaded_addresses.into_owned(),
                    is_writable_account_cache: message.is_writable_account_cache.clone(),
                };
                KafkaSanitizedMessage::V0(lm)
            }
        }
    }
}

impl From<&SanitizedTransaction> for KafkaSanitizedTransaction {
    fn from(sanitized_transaction: &SanitizedTransaction) -> Self {
        KafkaSanitizedTransaction {
            message: sanitized_transaction.message().into(),
            message_hash: *sanitized_transaction.message_hash(),
            is_simple_vote_tx: sanitized_transaction.is_simple_vote_transaction(),
            signatures: sanitized_transaction.signatures().to_vec(),
        }
    }
}

impl From<&TransactionTokenBalance> for KafkaTransactionTokenBalance {
    fn from(transaction_token_balance: &TransactionTokenBalance) -> Self {
        KafkaTransactionTokenBalance {
            account_index: transaction_token_balance.account_index,
            mint: transaction_token_balance.mint.clone(),
            ui_token_amount: transaction_token_balance.ui_token_amount.clone(),
            owner: transaction_token_balance.owner.clone(),
            program_id: transaction_token_balance.program_id.clone(),
        }
    }
}

impl From<&TransactionStatusMeta> for KafkaTransactionStatusMeta {
    fn from(transaction_status_meta: &TransactionStatusMeta) -> Self {
        let pre_token_balances = transaction_status_meta
            .pre_token_balances
            .as_ref()
            .map(|v| v.iter().map(|i| i.into()).collect());

        let post_token_balances = transaction_status_meta
            .post_token_balances
            .as_ref()
            .map(|v| v.iter().map(|i| i.into()).collect());

        KafkaTransactionStatusMeta {
            status: transaction_status_meta.status.clone(),
            fee: transaction_status_meta.fee,
            pre_balances: transaction_status_meta.pre_balances.clone(),
            post_balances: transaction_status_meta.post_balances.clone(),
            inner_instructions: transaction_status_meta.inner_instructions.clone(),
            log_messages: transaction_status_meta.log_messages.clone(),
            pre_token_balances,
            post_token_balances,
            rewards: transaction_status_meta.rewards.clone(),
            loaded_addresses: transaction_status_meta.loaded_addresses.clone(),
            return_data: transaction_status_meta.return_data.clone(),
            compute_units_consumed: transaction_status_meta.compute_units_consumed,
        }
    }
}

impl From<&ReplicaTransactionInfo<'_>> for KafkaReplicaTransactionInfo {
    fn from(replica_transaction_info: &ReplicaTransactionInfo<'_>) -> Self {
        KafkaReplicaTransactionInfo {
            transaction: replica_transaction_info.transaction.into(),
            signature: *replica_transaction_info.signature,
            is_vote: replica_transaction_info.is_vote,
            transaction_status_meta: replica_transaction_info.transaction_status_meta.into(),
        }
    }
}

impl From<&ReplicaTransactionInfoV2<'_>> for KafkaReplicaTransactionInfoV2 {
    fn from(replica_transaction_info: &ReplicaTransactionInfoV2<'_>) -> Self {
        KafkaReplicaTransactionInfoV2 {
            transaction: replica_transaction_info.transaction.into(),
            signature: *replica_transaction_info.signature,
            is_vote: replica_transaction_info.is_vote,
            transaction_status_meta: replica_transaction_info.transaction_status_meta.into(),
            index: replica_transaction_info.index,
        }
    }
}

impl UpdateAccount {
    pub fn set_write_version(&mut self, transaction_index: i64) {
        let mut account = match &mut self.account {
            KafkaReplicaAccountInfoVersions::V0_0_1(_) => unreachable!(),
            KafkaReplicaAccountInfoVersions::V0_0_2(account) => account,
        };

        account.write_version = transaction_index;
    }
}

impl From<ReplicaTransactionInfoVersions<'_>> for KafkaReplicaTransactionInfoVersions {
    fn from(replica_transaction_info: ReplicaTransactionInfoVersions<'_>) -> Self {
        match replica_transaction_info {
            ReplicaTransactionInfoVersions::V0_0_1(rti) => {
                KafkaReplicaTransactionInfoVersions::V0_0_1(rti.into())
            }
            ReplicaTransactionInfoVersions::V0_0_2(rti) => {
                KafkaReplicaTransactionInfoVersions::V0_0_2(rti.into())
            }
        }
    }
}

impl From<&ReplicaAccountInfo<'_>> for KafkaReplicaAccountInfo {
    fn from(replica_account_info: &ReplicaAccountInfo<'_>) -> Self {
        KafkaReplicaAccountInfo {
            owner: replica_account_info.owner.to_vec(),
            lamports: replica_account_info.lamports,
            executable: replica_account_info.executable,
            rent_epoch: replica_account_info.rent_epoch,
            data: replica_account_info.data.to_vec(),
            pubkey: replica_account_info.pubkey.to_vec(),
            write_version: replica_account_info.write_version,
        }
    }
}

impl From<&ReplicaAccountInfoV2<'_>> for KafkaReplicaAccountInfoV2 {
    fn from(replica_account_info: &ReplicaAccountInfoV2<'_>) -> Self {
        let write_version = if replica_account_info.txn_signature.is_none() {
            -1
        } else {
            0
        };
        KafkaReplicaAccountInfoV2 {
            owner: replica_account_info.owner.to_vec(),
            lamports: replica_account_info.lamports,
            executable: replica_account_info.executable,
            rent_epoch: replica_account_info.rent_epoch,
            data: replica_account_info.data.to_vec(),
            pubkey: replica_account_info.pubkey.to_vec(),
            write_version,
            txn_signature: replica_account_info.txn_signature.copied(),
        }
    }
}

impl From<ReplicaAccountInfoVersions<'_>> for KafkaReplicaAccountInfoVersions {
    fn from(account_info: ReplicaAccountInfoVersions) -> Self {
        match account_info {
            ReplicaAccountInfoVersions::V0_0_1(rai) => {
                KafkaReplicaAccountInfoVersions::V0_0_1(rai.into())
            }
            ReplicaAccountInfoVersions::V0_0_2(rai) => {
                KafkaReplicaAccountInfoVersions::V0_0_2(rai.into())
            }
        }
    }
}

impl From<&ReplicaBlockInfo<'_>> for KafkaReplicaBlockInfo {
    fn from(replica_block_info: &ReplicaBlockInfo) -> Self {
        KafkaReplicaBlockInfo {
            slot: replica_block_info.slot,
            blockhash: replica_block_info.blockhash.to_string(),
            rewards: replica_block_info.rewards.to_vec(),
            block_time: replica_block_info.block_time,
            block_height: replica_block_info.block_height,
        }
    }
}

impl From<ReplicaBlockInfoVersions<'_>> for KafkaReplicaBlockInfoVersions {
    fn from(replica_block_info: ReplicaBlockInfoVersions) -> Self {
        match replica_block_info {
            ReplicaBlockInfoVersions::V0_0_1(rbi) => {
                KafkaReplicaBlockInfoVersions::V0_0_1(rbi.into())
            }
        }
    }
}

impl From<SlotStatus> for KafkaSlotStatus {
    fn from(slot_status: SlotStatus) -> Self {
        match slot_status {
            SlotStatus::Processed => KafkaSlotStatus::Processed,
            SlotStatus::Rooted => KafkaSlotStatus::Rooted,
            SlotStatus::Confirmed => KafkaSlotStatus::Confirmed,
        }
    }
}
