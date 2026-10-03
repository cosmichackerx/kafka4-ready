# Oracle matrix

Result of `tests/oracle/run_oracle.py` on four Kafka releases (Scala 2.13 tarballs, Linux, Java 17), run 2026-10-03. `pass` = the behaviour claimed in `rules.py` was observed. Where a release differs, the harness adapts (the console producer takes `--command-config` only from 4.2, `kafka-producer-perf-test` takes `--bootstrap-server` only from 4.2) and the deprecation cases expect the warning only from the release that introduced it (`log.cleaner.enable`: 4.1, `group.coordinator.rebalance.protocols`: 4.3, the CLI options: 4.2): older releases must show the opposite, which is what `pass` means for them.

| Rule | Case | 4.3.1 | 4.2.2 | 4.1.2 | 4.0.1 |
|---|---|---|---|---|---|
| `zookeeper-mode` | `format-no-process-roles` | pass | pass | pass | pass |
| `invalid-config-value` | `format-copier-pool` | pass | pass | pass | pass |
| `invalid-config-value` | `format-expiration-pool` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.connect` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.session.timeout.ms` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.connection.timeout.ms` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.set.acl` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.max.in.flight.requests` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.metadata.migration.enable` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.clientCnxnSocket` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.ssl.client.enable` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.ssl.keystore.location` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.ssl.truststore.location` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.ssl.protocol` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.ssl.endpoint.identification.algorithm` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.sync.time.ms` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.ssl.keystore.password` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.ssl.keystore.type` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.ssl.truststore.password` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.ssl.truststore.type` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.ssl.cipher.suites` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.ssl.enabled.protocols` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.ssl.crl.enable` | pass | pass | pass | pass |
| `removed-broker-config` | `zookeeper.ssl.ocsp.enable` | pass | pass | pass | pass |
| `removed-broker-config` | `log.message.format.version` | pass | pass | pass | pass |
| `removed-broker-config` | `message.format.version` | pass | pass | pass | pass |
| `removed-broker-config` | `inter.broker.protocol.version` | pass | pass | pass | pass |
| `removed-broker-config` | `offsets.commit.required.acks` | pass | pass | pass | pass |
| `removed-broker-config` | `log.message.timestamp.difference.max.ms` | pass | pass | pass | pass |
| `removed-broker-config` | `delegation.token.master.key` | pass | pass | pass | pass |
| `removed-broker-config` | `metrics.jmx.blacklist` | pass | pass | pass | pass |
| `removed-broker-config` | `metrics.jmx.whitelist` | pass | pass | pass | pass |
| `removed-broker-config` | `auto.include.jmx.reporter` | pass | pass | pass | pass |
| `removed-broker-config` | `host.name` | pass | pass | pass | pass |
| `removed-broker-config` | `port` | pass | pass | pass | pass |
| `removed-broker-config` | `advertised.host.name` | pass | pass | pass | pass |
| `removed-broker-config` | `advertised.port` | pass | pass | pass | pass |
| `removed-broker-config` | `broker.id.generation.enable` | pass | pass | pass | pass |
| `deprecated-broker-config` | `log.cleaner.enable` | pass | pass | pass | pass |
| `deprecated-broker-config` | `group.coordinator.rebalance.protocols` | pass | pass | pass | pass |
| `removed-cli-option` | `kafka-topics --zookeeper` | pass | pass | pass | pass |
| `removed-cli-option` | `kafka-configs --zookeeper` | pass | pass | pass | pass |
| `removed-cli-option` | `kafka-reassign-partitions --zookeeper` | pass | pass | pass | pass |
| `removed-cli-option` | `kafka-consumer-groups --zookeeper` | pass | pass | pass | pass |
| `removed-cli-option` | `kafka-acls --authorizer` | pass | pass | pass | pass |
| `removed-cli-option` | `kafka-acls --authorizer-properties` | pass | pass | pass | pass |
| `removed-cli-option` | `kafka-acls --zk-tls-config-file` | pass | pass | pass | pass |
| `removed-cli-option` | `kafka-console-consumer --whitelist` | pass | pass | pass | pass |
| `removed-cli-option` | `kafka-replica-verification --topic-white-list` | pass | pass | pass | pass |
| `removed-cli-option` | `kafka-verifiable-consumer --broker-list` | pass | pass | pass | pass |
| `removed-cli-option` | `kafka-console-consumer --zookeeper` | pass | pass | pass | pass |
| `removed-cli-option` | `kafka-console-consumer --new-consumer` | pass | pass | pass | pass |
| `removed-cli-option` | `kafka-console-producer --broker-list` | pass | pass | pass | pass |
| `removed-cli-option` | `kafka-consumer-perf-test --broker-list` | pass | pass | pass | pass |
| `removed-cli-option` | `kafka-topics --delete-config` | pass | pass | pass | pass |
| `deprecated-cli-option` | `kafka-console-producer --producer.config` | pass | pass | pass | pass |
| `deprecated-cli-option` | `kafka-console-producer --producer-property` | pass | pass | pass | pass |
| `deprecated-cli-option` | `kafka-console-consumer --consumer.config` | pass | pass | pass | pass |
| `deprecated-cli-option` | `kafka-console-consumer --consumer-property` | pass | pass | pass | pass |
| `deprecated-cli-option` | `kafka-producer-perf-test --producer-props` | pass | pass | pass | pass |
| `deprecated-cli-option` | `kafka-consumer-perf-test --messages` | pass | pass | pass | pass |
| `bootstrap-server-format` | `kafka-topics` | pass | pass | pass | pass |
| `bootstrap-server-format` | `kafka-configs` | pass | pass | pass | pass |
| `bootstrap-server-format` | `kafka-console-consumer` | pass | pass | pass | pass |
| `removed-tool-class` | `kafka.admin.FeatureCommand` | pass | pass | pass | pass |
| `removed-tool-class` | `kafka.tools.ClusterTool` | pass | pass | pass | pass |
| `removed-tool-class` | `kafka.tools.EndToEndLatency` | pass | pass | pass | pass |
| `removed-tool-class` | `kafka.tools.MirrorMaker` | pass | pass | pass | pass |
| `removed-tool-class` | `kafka.tools.StateChangeLogMerger` | pass | pass | pass | pass |
| `removed-tool-class` | `kafka.tools.StreamsResetter` | pass | pass | pass | pass |
| `removed-tool-class` | `kafka.tools.JmxTool` | pass | pass | pass | pass |
| `zookeeper-script` | `zookeeper-server-start` | pass | pass | pass | pass |
| `zookeeper-script` | `zookeeper-server-stop` | pass | pass | pass | pass |
| `zookeeper-script` | `zookeeper-shell` | pass | pass | pass | pass |
| `zookeeper-script` | `zookeeper-security-migration` | pass | pass | pass | pass |
| `removed-tool-script` | `kafka-mirror-maker` | pass | pass | pass | pass |
| `removed-tool-script` | `kafka-preferred-replica-election` | pass | pass | pass | pass |
| `removed-tool-script` | `kafka-consumer-offset-checker` | pass | pass | pass | pass |
| `removed-broker-config` | `apache/kafka image: KAFKA_* variable names -> settings` | pass | pass | pass | pass |
| `kraft-config-path` | `config/kraft/server.properties` | pass | pass | pass | pass |
| `kraft-config-path` | `config/kraft/broker.properties` | pass | pass | pass | pass |
| `kraft-config-path` | `config/kraft/controller.properties` | pass | pass | pass | pass |
| `removed-partitioner` | `DefaultPartitioner` | pass | pass | pass | pass |
| `removed-partitioner` | `UniformStickyPartitioner` | pass | pass | pass | pass |
| `idempotence-in-flight` | `idempotence+6` | pass | pass | pass | pass |
| `idempotence-in-flight` | `default+6` | pass | pass | pass | pass |
| `idempotence-in-flight` | `idempotence off+6 (control)` | pass | pass | pass | pass |
| `removed-connector-config` | `topics.blacklist` | pass | pass | pass | pass |
| `removed-connector-config` | `groups.blacklist` | pass | pass | pass | pass |
| `removed-connector-config` | `config.properties.blacklist` | pass | pass | pass | pass |
| `removed-connector-config` | `use.incremental.alter.configs` | pass | pass | pass | pass |
| `removed-connector-config` | `add.source.alias.to.metrics` | pass | pass | pass | pass |
| `removed-connector-config` | `ReplaceField whitelist` | pass | pass | pass | pass |
| `removed-connector-config` | `ReplaceField blacklist` | pass | pass | pass | pass |
