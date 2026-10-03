"""Rule registry. Every rule is tied to the behaviour a real Kafka broker or tool showed in tests/oracle/run_oracle.py.

TESTED is the list of Kafka releases the oracle ran on (linux, Scala 2.13 tarball, SHA-512 checked); a rule is only claimed for those.
"""
from __future__ import annotations

from dataclasses import dataclass

REPO = "https://github.com/cosmichackerx/kafka4-ready"
TESTED = ["4.0.1", "4.1.2", "4.2.2", "4.3.1"]    # releases the oracle ran on (tests/oracle/kafka-sha512.txt)
UPGRADE_NOTES = "https://kafka.apache.org/43/getting-started/upgrade/"


@dataclass(frozen=True)
class Rule:
    id: str
    severity: str
    summary: str
    fix: str
    oracle: bool
    url: str = REPO + "#rules"


RULES = {r.id: r for r in [
    Rule("zookeeper-mode", "error", "A broker config without process.roles: ZooKeeper mode, which Kafka 4 no longer has", "Migrate the cluster to KRaft before upgrading (set process.roles, node.id, controller.quorum.bootstrap.servers, controller.listener.names).", True),
    Rule("removed-broker-config", "warning", "A broker setting that Kafka 4 does not know: the broker starts, ignores it and logs nothing", "Remove it, or use the replacement named in the message.", True),
    Rule("invalid-config-value", "error", "A broker setting whose value Kafka 4 rejects at startup", "Use a valid value (see the message).", True),
    Rule("deprecated-broker-config", "note", "A broker setting that Kafka 4 accepts but logs as deprecated", "Plan the change; the message names the Kafka 5.0 plan.", True),
    Rule("removed-cli-option", "error", "A command line option that a Kafka 4 tool rejects", "Use the replacement named in the message.", True),
    Rule("deprecated-cli-option", "note", "A command line option that a Kafka 4 tool still accepts but warns about", "Switch to the new option; the old one goes away in Kafka 5.0.", True),
    Rule("bootstrap-server-format", "error", "--bootstrap-server with space-separated brokers: Kafka 4 accepts only a comma-separated list", "Separate the brokers with commas.", True),
    Rule("removed-tool-class", "error", "kafka-run-class.sh with a tool class that Kafka 4 removed", "Use the dedicated tool named in the message.", True),
    Rule("zookeeper-script", "error", "A ZooKeeper script (zookeeper-server-start.sh, zookeeper-shell.sh, ...) that the Kafka 4 distribution no longer contains", "Remove the ZooKeeper step; use KRaft tools (kafka-metadata-quorum.sh, kafka-storage.sh).", True),
    Rule("removed-tool-script", "error", "A tool script (kafka-mirror-maker.sh, kafka-preferred-replica-election.sh, kafka-consumer-offset-checker.sh) that the Kafka 4 distribution no longer contains", "Use the replacement named in the message.", True),
    Rule("kraft-config-path", "error", "config/kraft/*.properties: Kafka 4 moved the KRaft files to config/", "Use config/server.properties, config/broker.properties or config/controller.properties.", True),
    Rule("removed-partitioner", "error", "partitioner.class set to a class that Kafka 4 removed", "Delete the line: the default partitioner is built in.", True),
    Rule("removed-connector-config", "warning", "A MirrorMaker 2 or ReplaceField setting that Kafka 4 Connect no longer defines: the worker accepts the connector and ignores the key", "Use the replacement named in the message.", True),
    Rule("idempotence-in-flight", "error", "A producer with idempotence and more than 5 in-flight requests: Kafka 4 fails instead of silently turning idempotence off", "Set max.in.flight.requests.per.connection to 5 or less, or enable.idempotence=false.", True),
]}

# ---- broker settings that the Kafka 4.3.1 broker does not know. `kafka-configs --describe --all` shows them as `=null sensitive=true`, a known setting shows its value.
ZK = "ZooKeeper mode is gone in Kafka 4; remove it (migrate to KRaft first)"
REMOVED_BROKER = {
    "zookeeper.connect": ZK, "zookeeper.session.timeout.ms": ZK, "zookeeper.connection.timeout.ms": ZK, "zookeeper.set.acl": ZK,
    "zookeeper.max.in.flight.requests": ZK, "zookeeper.metadata.migration.enable": ZK, "zookeeper.clientCnxnSocket": ZK,
    "zookeeper.ssl.client.enable": ZK, "zookeeper.ssl.keystore.location": ZK, "zookeeper.ssl.truststore.location": ZK,
    "zookeeper.ssl.protocol": ZK, "zookeeper.ssl.endpoint.identification.algorithm": ZK, "zookeeper.sync.time.ms": ZK,
    "zookeeper.ssl.keystore.password": ZK, "zookeeper.ssl.keystore.type": ZK, "zookeeper.ssl.truststore.password": ZK, "zookeeper.ssl.truststore.type": ZK,
    "zookeeper.ssl.cipher.suites": ZK, "zookeeper.ssl.enabled.protocols": ZK, "zookeeper.ssl.crl.enable": ZK, "zookeeper.ssl.ocsp.enable": ZK,
    "log.message.format.version": "removed; KRaft always uses record format v2, there is no replacement",
    "message.format.version": "removed; there is no replacement",
    "inter.broker.protocol.version": "removed; in KRaft choose the metadata version with kafka-storage.sh format --release-version or kafka-features.sh",
    "offsets.commit.required.acks": "removed (KIP-1041); there is no replacement",
    "log.message.timestamp.difference.max.ms": "removed; use log.message.timestamp.before.max.ms and log.message.timestamp.after.max.ms",
    "delegation.token.master.key": "removed; use delegation.token.secret.key",
    "metrics.jmx.blacklist": "removed; use metrics.jmx.exclude", "metrics.jmx.whitelist": "removed; use metrics.jmx.include",
    "auto.include.jmx.reporter": "removed; metric.reporters now defaults to JmxReporter",
    "host.name": "removed; use listeners", "port": "removed; use listeners", "advertised.host.name": "removed; use advertised.listeners", "advertised.port": "removed; use advertised.listeners",
    "broker.id.generation.enable": "not known to a KRaft broker; set node.id yourself",
}
# keys that mean something else outside a broker file (client or Connect configs): only flagged in broker-looking files
BROKER_ONLY = {"host.name", "port", "advertised.host.name", "advertised.port", "broker.id.generation.enable", "metrics.jmx.blacklist", "metrics.jmx.whitelist", "auto.include.jmx.reporter"}

# ---- values the broker rejects at startup: key -> (minimum)
INVALID_MIN = {"remote.log.manager.copier.thread.pool.size": 1, "remote.log.manager.expiration.thread.pool.size": 1}

# ---- accepted but logged as deprecated at startup: key -> (value that triggers it or None for any, message)
DEPRECATED_BROKER = {
    "log.cleaner.enable": ("false", "deprecated since Kafka 4.1; the log cleaner is always enabled in Kafka 5.0 and this setting is ignored"),
    "group.coordinator.rebalance.protocols": (None, "deprecated since Kafka 4.3; Kafka 5.0 always enables all protocols (controlled by feature versions)"),
}

# ---- tools: (tool, option, hint). Oracle: the tool prints "<option> is not a recognized option" / "unrecognized arguments" / "no longer supported".
REMOVED_CLI = [
    ("kafka-topics", "--zookeeper", "use --bootstrap-server"),
    ("kafka-configs", "--zookeeper", "use --bootstrap-server"),
    ("kafka-reassign-partitions", "--zookeeper", "use --bootstrap-server"),
    ("kafka-consumer-groups", "--zookeeper", "use --bootstrap-server"),
    ("kafka-acls", "--authorizer", "use --bootstrap-server (or --bootstrap-controller)"),
    ("kafka-acls", "--authorizer-properties", "use --bootstrap-server (or --bootstrap-controller)"),
    ("kafka-acls", "--zk-tls-config-file", "use --bootstrap-server (or --bootstrap-controller)"),
    ("kafka-console-consumer", "--whitelist", "use --include"),
    ("kafka-replica-verification", "--topic-white-list", "use --topics-include"),
    ("kafka-verifiable-consumer", "--broker-list", "use --bootstrap-server"),
    ("kafka-console-consumer", "--zookeeper", "use --bootstrap-server"),
    ("kafka-console-consumer", "--new-consumer", "the option is gone (the new consumer is the only one); delete it"),
    ("kafka-console-producer", "--broker-list", "use --bootstrap-server"),
    ("kafka-consumer-perf-test", "--broker-list", "use --bootstrap-server"),
    ("kafka-topics", "--delete-config", "the option is no longer supported; use kafka-configs.sh --alter --delete-config"),
]
# deprecated in 4.2: (tool, option, replacement)
DEPRECATED_CLI = [
    ("kafka-console-producer", "--producer.config", "--command-config"),
    ("kafka-console-producer", "--producer-property", "--command-property"),
    ("kafka-console-consumer", "--consumer.config", "--command-config"),
    ("kafka-console-consumer", "--consumer-property", "--command-property"),
    ("kafka-producer-perf-test", "--producer-props", "--command-property"),
    ("kafka-consumer-perf-test", "--messages", "--num-records"),
]
BOOTSTRAP_TOOLS = ("kafka-topics", "kafka-configs", "kafka-console-consumer")

# ---- tool classes the Kafka 4 distribution does not contain
REMOVED_CLASSES = {
    "kafka.admin.FeatureCommand": "kafka-features.sh", "kafka.tools.ClusterTool": "kafka-cluster.sh", "kafka.tools.EndToEndLatency": "kafka-e2e-latency.sh",
    "kafka.tools.MirrorMaker": "connect-mirror-maker.sh", "kafka.tools.StateChangeLogMerger": "(removed without replacement)", "kafka.tools.StreamsResetter": "kafka-streams-application-reset.sh", "kafka.tools.JmxTool": "kafka-jmx.sh",
}
REMOVED_SCRIPTS = {
    "kafka-mirror-maker": "MirrorMaker 1 was removed in Kafka 4.0; use connect-mirror-maker.sh (MirrorMaker 2)",
    "kafka-preferred-replica-election": "removed; use kafka-leader-election.sh --election-type preferred",
    "kafka-consumer-offset-checker": "removed; use kafka-consumer-groups.sh --describe",
}
ZOOKEEPER_SCRIPTS = ("zookeeper-server-start", "zookeeper-server-stop", "zookeeper-shell", "zookeeper-security-migration")
KRAFT_PATHS = ("config/kraft/server.properties", "config/kraft/broker.properties", "config/kraft/controller.properties")
REMOVED_PARTITIONERS = ("org.apache.kafka.clients.producer.internals.DefaultPartitioner", "org.apache.kafka.clients.producer.UniformStickyPartitioner")

# ---- Connect: settings the Kafka 4 Connect worker no longer defines. The REST validate endpoint lists every setting a connector knows (`configs[].definition.name`);
# these are absent there, their replacements present. key -> (connector class short name, replacement text)
REMOVED_CONNECTOR = {
    "topics.blacklist": ("MirrorSourceConnector", "use topics.exclude"),
    "groups.blacklist": ("MirrorSourceConnector", "use groups.exclude"),
    "config.properties.blacklist": ("MirrorSourceConnector", "use config.properties.exclude"),
    "use.incremental.alter.configs": ("MirrorSourceConnector", "removed; Kafka 4 always behaves like the old 'required' (target brokers must be 2.3.0 or newer)"),
    "add.source.alias.to.metrics": ("MirrorSourceConnector", "removed; the source cluster alias is always added to the metrics"),
}
REPLACEFIELD_TYPE = "org.apache.kafka.connect.transforms.ReplaceField"
REPLACEFIELD_KEYS = {"whitelist": "include", "blacklist": "exclude"}
