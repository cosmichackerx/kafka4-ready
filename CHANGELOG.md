# Changelog

## 0.4.0 - 2026-10-03

* **Strimzi `Kafka` resources**: the `spec.kafka.config` map of YAML documents with `apiVersion: kafka.strimzi.io/...` and `kind: Kafka` is checked with `removed-broker-config`, `invalid-config-value` and `deprecated-broker-config` (closes the gap the v0.2.0 study found: 83 lines). Not run against Strimzi itself; see the README.
* Unit tests: `tests/test_strimzi.py` (12 cases).

## 0.3.0 - 2026-10-03

* **`--fix` and `--diff`**: rewrite the mechanical spellings (`--whitelist` to `--include`, `--topic-white-list` to `--topics-include`, `--broker-list` to `--bootstrap-server`, `--new-consumer` deleted, spaces to commas in `--bootstrap-server`, `config/kraft/` to `config/`). Idempotent, only the option changes. 13 fix cases run on the real tools of Kafka 4.0.1 to 4.3.1 in CI (`tests/oracle/run_fix_oracle.py`).
* Fixed a miss found while writing the fixer: `--bootstrap-server='a:9092 b:9092'` (quoted value after `=`) was not reported.
* CI: the release gate and the weekly pin check call the reusable workflows of claims-check 0.3.0.

## 0.2.0 - 2026-10-03

* **Precision study** on 3,185 files of 2,446 public repositories (`docs/precision-study.md`): 378 findings labelled by hand. v0.1.0 was right on 186 of 216; the 30 false positives are fixed.
* False positives fixed: `zookeeper.*` keys in `consumer.properties` and client application configs (only broker files count now, `zookeeper.connect` alone no longer makes a broker file); vendored copies of Kafka's own `bin/kafka-*.sh` wrappers; a Dockerfile `COPY` of your own `zookeeper-*.sh`; `config/kraft/...` as a `COPY` or volume-mount destination.
* Recall: commands inside quoted strings (`bash -c "..."`, compose `command:`, `CMD ["sh", "-c", "..."]`, `echo "..." >> start.sh`) are scanned; Kubernetes `- name: KAFKA_...` env form; 8 more `zookeeper.ssl.*` settings.
* New rules: `removed-connector-config` (MirrorMaker 2 `*.blacklist` and friends, ReplaceField `whitelist`/`blacklist`; checked against a real Connect worker's REST validate endpoint) and `removed-tool-script` (`kafka-mirror-maker.sh`, `kafka-preferred-replica-election.sh`, `kafka-consumer-offset-checker.sh`). New removed options: `kafka-console-consumer --zookeeper/--new-consumer`, `kafka-console-producer` and `kafka-consumer-perf-test --broker-list`; class `kafka.tools.MirrorMaker`.
* `idempotence-in-flight` now also fires when idempotence is the default: a 4.3.1 producer rejects `max.in.flight.requests.per.connection` above 5 unless `enable.idempotence=false` is set.
* Oracle: 94 cases, 14 rules, on 4.0.1, 4.1.2, 4.2.2 and 4.3.1. New stage runs the apache/kafka image's `kafka.docker.KafkaDockerWrapper` from the tarball to check the `KAFKA_*` variable mapping (the image itself is not run).

## 0.1.0 - 2026-10-03

First release. Twelve rules, each run against a real Kafka KRaft broker and the real Kafka tools (`tests/oracle/run_oracle.py`, 68 cases, Kafka 4.0.1, 4.1.2, 4.2.2 and 4.3.1).

* Broker `.properties`: `zookeeper-mode`, `removed-broker-config` (27 settings that a Kafka 4 broker silently ignores), `invalid-config-value`, `deprecated-broker-config`.
* CLI in scripts, Dockerfiles, Makefiles, YAML and CI files: `removed-cli-option`, `deprecated-cli-option`, `bootstrap-server-format`, `removed-tool-class`, `zookeeper-script`, `kraft-config-path`.
* Client `.properties`: `removed-partitioner`, `idempotence-in-flight`.
* Text, markdown, JSON, GitHub annotation and SARIF 2.1.0 output; PR mode (`--base`) with a sticky comment; GitHub Action and pre-commit hook; `# kafka4-ready: ignore [rule]` comments.
