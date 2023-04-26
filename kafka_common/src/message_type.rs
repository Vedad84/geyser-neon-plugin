use crate::kafka_structs::{
    NotifyBlockMetaData, NotifyTransaction, UpdateAccount, UpdateSlotStatus,
};
use std::fmt;

pub enum MessageType {
    UpdateAccount,
    UpdateSlot,
    NotifyTransaction,
    NotifyBlock,
}

impl fmt::Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MessageType::UpdateAccount => write!(f, "UpdateAccount"),
            MessageType::UpdateSlot => write!(f, "UpdateSlot"),
            MessageType::NotifyTransaction => write!(f, "NotifyTransaction"),
            MessageType::NotifyBlock => write!(f, "NotifyBlock"),
        }
    }
}

pub trait GetMessageType {
    fn get_type(&self) -> MessageType;
    fn get_slot(&self) -> u64;
}

impl GetMessageType for NotifyBlockMetaData {
    fn get_type(&self) -> MessageType {
        MessageType::NotifyBlock
    }

    fn get_slot(&self) -> u64 {
        match &self.block_info {
            crate::kafka_structs::KafkaReplicaBlockInfoVersions::V0_0_1(rbi) => rbi.slot,
        }
    }
}

impl GetMessageType for NotifyTransaction {
    fn get_type(&self) -> MessageType {
        MessageType::NotifyTransaction
    }

    fn get_slot(&self) -> u64 {
        self.slot
    }
}

impl GetMessageType for UpdateAccount {
    fn get_type(&self) -> MessageType {
        MessageType::UpdateAccount
    }

    fn get_slot(&self) -> u64 {
        self.slot
    }
}

impl GetMessageType for UpdateSlotStatus {
    fn get_type(&self) -> MessageType {
        MessageType::UpdateSlot
    }

    fn get_slot(&self) -> u64 {
        self.slot
    }
}

pub enum EventType<'a> {
    NotifyBlockMetaData(&'a NotifyBlockMetaData),
    NotifyTransaction(&'a NotifyTransaction),
    UpdateAccount(&'a UpdateAccount),
    UpdateSlotStatus(&'a UpdateSlotStatus),
}

pub trait GetEvent: Sized {
    fn as_ref(&self) -> EventType;
}

impl GetEvent for NotifyTransaction {
    fn as_ref(&self) -> EventType {
        EventType::NotifyTransaction(self)
    }
}

impl GetEvent for NotifyBlockMetaData {
    fn as_ref(&self) -> EventType {
        EventType::NotifyBlockMetaData(self)
    }
}

impl GetEvent for UpdateAccount {
    fn as_ref(&self) -> EventType {
        EventType::UpdateAccount(self)
    }
}

impl GetEvent for UpdateSlotStatus {
    fn as_ref(&self) -> EventType {
        EventType::UpdateSlotStatus(self)
    }
}
