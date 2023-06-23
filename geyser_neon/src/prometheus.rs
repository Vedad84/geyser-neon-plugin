use std::{
    borrow::Cow,
    future::Future,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::Arc,
};

use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};
use log::info;
use prometheus_client::{encoding::text::encode, registry::Registry};

use tokio::sync::oneshot::Receiver;

use crate::stats::TopicStats;
use crate::{geyser_neon_config::GeyserPluginKafkaConfig, stats::Stats};

pub fn build_registry(stats: &Stats, config: &GeyserPluginKafkaConfig) -> Registry {
    let mut registry = Registry::with_prefix("neon_plugin");

    registry.register(
        "msg_cnt",
        "The current number of messages in producer queues.",
        stats.msg_cnt.clone(),
    );

    registry.register(
        "msg_size",
        "The current total size of messages in producer queues.",
        stats.msg_size.clone(),
    );

    registry.register(
        "msg_max",
        "The maximum number of messages allowed in the producer queues.",
        stats.msg_max.clone(),
    );

    registry.register(
        "msg_size_max",
        "The maximum total size of messages allowed in the producer queues.",
        stats.msg_size_max.clone(),
    );

    registry.register(
        "tx",
        "The total number of requests sent to brokers.",
        stats.tx.clone(),
    );

    registry.register(
        "tx_bytes",
        "The total number of bytes transmitted to brokers.",
        stats.tx_bytes.clone(),
    );

    registry.register(
        "txmsgs",
        "The total number of messages transmitted (produced) to brokers.",
        stats.txmsgs.clone(),
    );

    registry.register(
        "txmsgs_bytes",
        "The total number of bytes transmitted (produced) to brokers.",
        stats.txmsg_bytes.clone(),
    );

    registry.register(
        "txerrs",
        "The total number of transmission errors.",
        stats.txerrs.clone(),
    );

    registry.register(
        "num_brokers",
        "The number of brokers",
        stats.num_brokers.clone(),
    );

    registry.register(
        "ordering_queue_len",
        "How many accounts are pending in ordering queue",
        stats.ordering_queue_len.clone(),
    );

    registry.register(
        "filtered_events",
        "How many events were skipped by filter",
        stats.filtered_events.clone(),
    );

    registry.register(
        "producer_recreations",
        "How many times the producer has been recreated successfully",
        stats.producer_recreations.clone(),
    );

    registry.register(
        "producer_recreation_errors",
        "How many times the producer has been recreated with errors",
        stats.producer_recreation_errors.clone(),
    );

    add_metrics_for_topic(
        &mut registry,
        &stats.update_account_startup,
        &format!("{}_startup", config.update_account_topic),
    );
    add_metrics_for_topic(
        &mut registry,
        &stats.update_account,
        &config.update_account_topic,
    );
    add_metrics_for_topic(&mut registry, &stats.update_slot, &config.update_slot_topic);
    add_metrics_for_topic(
        &mut registry,
        &stats.notify_transaction,
        &config.notify_transaction_topic,
    );
    add_metrics_for_topic(
        &mut registry,
        &stats.notify_block,
        &config.notify_block_topic,
    );

    registry
}

fn add_metrics_for_topic(registry: &mut Registry, stats: &TopicStats, topic: &str) {
    let registry_with_label =
        registry.sub_registry_with_label((Cow::Borrowed("topic"), Cow::from(topic.to_string())));

    registry_with_label.register(
        "serialize_errors",
        format!("Count of {topic} messages serialize errors"),
        stats.serialize_errors.clone(),
    );

    registry_with_label.register(
        "kafka_messages_enqueued",
        format!("Count of {topic} messages successfully enqueued"),
        stats.enqueued.clone(),
    );

    registry_with_label.register(
        "kafka_messages_enqueue_retries",
        format!("Count of {topic} messages enqueue retries"),
        stats.enqueue_retries.clone(),
    );

    registry_with_label.register(
        "kafka_messages_enqueue_errors",
        format!("Count of {topic} messages enqueue errors"),
        stats.enqueue_errors.clone(),
    );

    registry_with_label.register(
        "kafka_messages_sent",
        format!("Count of {topic} messages successfully sent"),
        stats.sent.clone(),
    );

    registry_with_label.register(
        "kafka_messages_send_errors",
        format!("Count of {topic} messages send errors"),
        stats.send_errors.clone(),
    );

    registry_with_label.register(
        "kafka_messages_batch_size_p95",
        format!("Batch size p95 for {topic}"),
        stats.batch_size.p95.clone(),
    );

    registry_with_label.register(
        "kafka_messages_batch_cnt_p95",
        format!("Batch count p95 for {topic}"),
        stats.batch_cnt.p95.clone(),
    );
}

pub async fn start_metrics_server(registry: Registry, port: u16, shutdown_rx: Receiver<()>) {
    let metrics_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
    info!("Starting metrics server on {metrics_addr}");

    let registry = Arc::new(registry);
    Server::bind(&metrics_addr)
        .serve(make_service_fn(move |_conn| {
            let registry = registry.clone();
            async move { Ok::<_, io::Error>(service_fn(make_handler(registry))) }
        }))
        .with_graceful_shutdown(async move {
            shutdown_rx.await.ok();
            info!("Drop: Metrics server received shutdown token");
        })
        .await
        .expect("Failed to bind hyper server with graceful_shutdown");
}

fn make_handler(
    registry: Arc<Registry>,
) -> impl Fn(Request<Body>) -> Pin<Box<dyn Future<Output = io::Result<Response<Body>>> + Send>> {
    // This closure accepts a request and responds with the OpenMetrics encoding of our metrics.
    move |_req: Request<Body>| {
        let reg = registry.clone();
        Box::pin(async move {
            let mut buf = String::new();
            encode(&mut buf, &reg.clone())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                .map(|_| {
                    let body = Body::from(buf);
                    Response::builder()
                        .header(
                            hyper::header::CONTENT_TYPE,
                            "application/openmetrics-text; version=1.0.0; charset=utf-8",
                        )
                        .body(body)
                        .unwrap()
                })
        })
    }
}
