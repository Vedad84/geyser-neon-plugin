use kafka_common::message_type::MessageType;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;

#[derive(Default, Debug)]
pub struct Stats {
    pub msg_cnt: Gauge,
    pub msg_size: Gauge,
    pub msg_max: Counter,
    pub msg_size_max: Counter,
    pub tx: Counter,
    pub tx_bytes: Counter,
    pub txmsgs: Counter,
    pub txmsg_bytes: Counter,
    pub txerrs: Counter,
    pub num_brokers: Gauge,

    pub ordering_queue_len: Gauge,
    pub filtered_events: Counter,
    pub producer_recreations: Counter,
    pub producer_recreation_errors: Counter,

    pub update_account_startup: TopicStats,
    pub update_account: TopicStats,
    pub update_slot: TopicStats,
    pub notify_transaction: TopicStats,
    pub notify_block: TopicStats,
}

#[derive(Default, Debug)]
pub struct TopicStats {
    pub serialize_errors: Counter,
    pub enqueued: Counter,
    pub enqueue_retries: Counter,
    pub enqueue_errors: Counter,
    pub sent: Counter,
    pub send_errors: Counter,
    pub batch_size: Window,
    pub batch_cnt: Window,
}

#[derive(Default, Debug)]
pub struct Window {
    pub p95: Gauge,
    pub p99: Gauge,
    pub p99_99: Gauge,
}

impl Stats {
    pub fn stats_for_message_type(&self, message_type: &MessageType) -> &TopicStats {
        match message_type {
            MessageType::UpdateAccountStartup => &self.update_account_startup,
            MessageType::UpdateAccount => &self.update_account,
            MessageType::UpdateSlot => &self.update_slot,
            MessageType::NotifyTransaction => &self.notify_transaction,
            MessageType::NotifyBlock => &self.notify_block,
        }
    }
}
