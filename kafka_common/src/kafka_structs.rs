use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use solana_account_decoder::parse_token::UiTokenAmount;
use solana_program::hash::Hash;
use solana_program::message::legacy;
use solana_program::message::v0::{self, LoadedAddresses};
use solana_sdk::transaction::Result as TransactionResult;
use solana_sdk::transaction_context::TransactionReturnData;
use solana_sdk::{clock::UnixTimestamp, signature::Signature};
use solana_transaction_status::Rewards;
use solana_transaction_status::{InnerInstructions, Reward};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Information about an account being updated
pub struct KafkaReplicaAccountInfo {
    /// The Pubkey for the account
    pub pubkey: Vec<u8>,

    /// The lamports for the account
    pub lamports: u64,

    /// The Pubkey of the owner program account
    pub owner: Vec<u8>,

    /// This account's data contains a loaded program (and is now read-only)
    pub executable: bool,

    /// The epoch at which this account will next owe rent
    pub rent_epoch: u64,

    /// The data held in this account.
    pub data: Vec<u8>,

    /// A global monotonically increasing atomic number, which can be used
    /// to tell the order of the account update. For example, when an
    /// account is updated in the same slot multiple times, the update
    /// with higher write_version should supersede the one with lower
    /// write_version.
    pub write_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Information about an account being updated
/// (extended with transaction signature doing this update)
pub struct KafkaReplicaAccountInfoV2 {
    /// The Pubkey for the account
    pub pubkey: Vec<u8>,

    /// The lamports for the account
    pub lamports: u64,

    /// The Pubkey of the owner program account
    pub owner: Vec<u8>,

    /// This account's data contains a loaded program (and is now read-only)
    pub executable: bool,

    /// The epoch at which this account will next owe rent
    pub rent_epoch: u64,

    /// The data held in this account.
    pub data: Vec<u8>,

    /// A global monotonically increasing atomic number, which can be used
    /// to tell the order of the account update. For example, when an
    /// account is updated in the same slot multiple times, the update
    /// with higher write_version should supersede the one with lower
    /// write_version.
    pub write_version: i64,

    /// First signature of the transaction caused this account modification
    pub txn_signature: Option<Signature>,
}

/// A wrapper to future-proof ReplicaAccountInfo handling.
/// If there were a change to the structure of ReplicaAccountInfo,
/// there would be new enum entry for the newer version, forcing
/// plugin implementations to handle the change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum KafkaReplicaAccountInfoVersions {
    V0_0_1(KafkaReplicaAccountInfo),
    V0_0_2(KafkaReplicaAccountInfoV2),
}

/// Information about a transaction
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KafkaReplicaTransactionInfo {
    /// The first signature of the transaction, used for identifying the transaction.
    pub signature: Signature,

    /// Indicates if the transaction is a simple vote transaction.
    pub is_vote: bool,

    /// The sanitized transaction.
    pub transaction: KafkaSanitizedTransaction,

    /// Metadata of the transaction status.
    pub transaction_status_meta: KafkaTransactionStatusMeta,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KafkaLegacyMessage {
    /// Legacy message
    pub message: legacy::Message,
    /// List of boolean with same length as account_keys(), each boolean value indicates if
    /// corresponding account key is writable or not.
    pub is_writable_account_cache: Vec<bool>,
}

/// Combination of a version #0 message and its loaded addresses
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KafkaLoadedMessage {
    /// Message which loaded a collection of lookup table addresses
    pub message: v0::Message,
    /// Addresses loaded with on-chain address lookup tables
    pub loaded_addresses: LoadedAddresses,
    /// List of boolean with same length as account_keys(), each boolean value indicates if
    /// corresponding account key is writable or not.
    pub is_writable_account_cache: Vec<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum KafkaSanitizedMessage {
    /// Sanitized legacy message
    Legacy(KafkaLegacyMessage),
    /// Sanitized version #0 message with dynamically loaded addresses
    V0(KafkaLoadedMessage),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KafkaSanitizedTransaction {
    pub message: KafkaSanitizedMessage,
    pub message_hash: Hash,
    pub is_simple_vote_tx: bool,
    pub signatures: Vec<Signature>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KafkaTransactionTokenBalance {
    pub account_index: u8,
    pub mint: String,
    pub ui_token_amount: UiTokenAmount,
    pub owner: String,
    pub program_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KafkaTransactionStatusMeta {
    pub status: TransactionResult<()>,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub inner_instructions: Option<Vec<InnerInstructions>>,
    pub log_messages: Option<Vec<String>>,
    pub pre_token_balances: Option<Vec<KafkaTransactionTokenBalance>>,
    pub post_token_balances: Option<Vec<KafkaTransactionTokenBalance>>,
    pub rewards: Option<Rewards>,
    pub loaded_addresses: LoadedAddresses,
    pub return_data: Option<TransactionReturnData>,
    pub compute_units_consumed: Option<u64>,
}

/// Information about a transaction, including index in block
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KafkaReplicaTransactionInfoV2 {
    /// The first signature of the transaction, used for identifying the transaction.
    pub signature: Signature,

    /// Indicates if the transaction is a simple vote transaction.
    pub is_vote: bool,

    /// The sanitized transaction.
    pub transaction: KafkaSanitizedTransaction,

    /// Metadata of the transaction status.
    pub transaction_status_meta: KafkaTransactionStatusMeta,

    /// The transaction's index in the block
    pub index: usize,
}

/// A wrapper to future-proof ReplicaTransactionInfo handling.
/// If there were a change to the structure of ReplicaTransactionInfo,
/// there would be new enum entry for the newer version, forcing
/// plugin implementations to handle the change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum KafkaReplicaTransactionInfoVersions {
    V0_0_1(KafkaReplicaTransactionInfo),
    V0_0_2(KafkaReplicaTransactionInfoV2),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KafkaReplicaBlockInfo {
    pub slot: u64,
    pub blockhash: String,
    pub rewards: Vec<Reward>,
    pub block_time: Option<UnixTimestamp>,
    pub block_height: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum KafkaReplicaBlockInfoVersions {
    V0_0_1(KafkaReplicaBlockInfo),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotifyTransaction {
    #[serde(flatten)]
    pub transaction_info: KafkaReplicaTransactionInfoVersions,
    pub slot: u64,
    pub retrieved_time: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyBlockMetaData {
    #[serde(flatten)]
    pub block_info: KafkaReplicaBlockInfoVersions,
    pub retrieved_time: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateAccount {
    #[serde(flatten)]
    pub account: KafkaReplicaAccountInfoVersions,
    pub slot: u64,
    pub is_startup: bool,
    pub retrieved_time: NaiveDateTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KafkaSlotStatus {
    /// The highest slot of the heaviest fork processed by the node. Ledger state at this slot is
    /// not derived from a confirmed or finalized block, but if multiple forks are present, is from
    /// the fork the validator believes is most likely to finalize.
    Processed,

    /// The highest slot having reached max vote lockout.
    Rooted,

    /// The highest slot that has been voted on by supermajority of the cluster, ie. is confirmed.
    Confirmed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateSlotStatus {
    pub slot: u64,
    pub parent: Option<u64>,
    pub status: KafkaSlotStatus,
    pub retrieved_time: NaiveDateTime,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() -> anyhow::Result<()> {
        let update_account = UpdateAccount {
            account: KafkaReplicaAccountInfoVersions::V0_0_2(KafkaReplicaAccountInfoV2 {
                pubkey: b"123".to_vec(),
                lamports: 0,
                owner: vec![],
                executable: false,
                rent_epoch: 0,
                data: b"abcx".to_vec(),
                write_version: 0,
                txn_signature: None,
            }),
            slot: 0,
            is_startup: false,
            retrieved_time: NaiveDateTime::parse_from_str(
                "2023-06-16T12:07:36.517958",
                "%Y-%m-%dT%H:%M:%S%.f",
            )?,
        };

        assert_eq!(serde_json::to_string(&update_account)?, "{\"pubkey\":[49,50,51],\"lamports\":0,\"owner\":[],\"executable\":false,\"rent_epoch\":0,\"data\":[97,98,99,120],\"write_version\":0,\"txn_signature\":null,\"slot\":0,\"is_startup\":false,\"retrieved_time\":\"2023-06-16T12:07:36.517958\"}");

        Ok(())
    }
}
