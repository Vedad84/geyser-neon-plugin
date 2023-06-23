## Geyser neon
The geyser_neon asynchronously sends data from the [geyser plugins interface](https://docs.solana.com/developing/plugins/geyser-plugins) to Kafka using [rdkafka](https://github.com/fede1024/rust-rdkafka).

### Requirements
1. Solana Validator 1.14.14
2. Kafka cluster
3. Geyser plugin must be compiled with the same version of Rust as the validator itself

### Configuration File Format
The plugin is configured using the input configuration file, read the [librdkafka documentation](https://docs.confluent.io/5.5.1/clients/librdkafka/md_CONFIGURATION.html) to set the optimal producer parameters.
\
In order to configure an SSL certificate, see the [librdkafka documentation](https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#ssl).
\
Path to the log file is **/var/log/neon/geyser.log**
\
An example configuration file looks like the following:
```json
{
  "libpath": "/home/user/libgeyser_neon.so",
  "kafka_producer_config": {
    "brokers_list": "167.235.75.213:9092,159.69.197.26:9092,167.235.151.85:9092",
    "sasl_username": "username",
    "sasl_password": "password",
    "sasl_mechanism": "SCRAM-SHA-512",
    "security_protocol": "SASL_SSL",
    "producer_send_max_retries": "100",
    "producer_queue_max_messages": "20000",
    "producer_message_max_bytes": "104857600",
    "producer_request_timeout_ms": "100000",
    "producer_retry_backoff_ms": "1000",
    "producer_enable_idempotence": "true",
    "max_in_flight_requests_per_connection": "5",
    "compression_codec": "lz4",
    "compression_level": "12",
    "batch_size": "1048576000",
    "batch_num_messages": "20000",
    "linger_ms": "200",
    "acks": "-1",
    "statistics_interval_ms" : "0",
    "message_timeout_ms": "100000",
    "kafka_log_level": "Info",
    "fetch_metadata": false,
    "fetch_metadata_timeout_ms": 30000
  },
  "update_account_topic": "update_account",
  "update_slot_topic": "update_slot",
  "notify_transaction_topic": "notify_transaction",
  "notify_block_topic": "notify_block",
  "ignore_snapshot": false,
  "prometheus_port": 9090,
  "global_log_level": "Info",
  "filter_config_path": "test-filter-config.json",
  "log_path": "logs/geyser.log"
}
```
If filtering of accounts and transactions *is not needed*, you need to remove filter_config_path from your config and build the project with:
```
cargo build --no-default-features
```
In order to load the plugin at the start of the Solana validator it is necessary to add the parameter
**--geyser-plugin-config** with the path to the config above.
\
\
To configure the logging level for librdkafka you should use:
```
RUST_LOG="librdkafka=trace,rdkafka::client=debug"
```
This will configure the logging level of librdkafka to trace, and the level of the client module of the Rust client to debug.
