# Changelog

## 0.1.0 - 2026-10-03

First release. Twelve rules, each run against a real Kafka KRaft broker and the real Kafka tools (`tests/oracle/run_oracle.py`, 68 cases, Kafka 4.0.1, 4.1.2, 4.2.2 and 4.3.1).

* Broker `.properties`: `zookeeper-mode`, `removed-broker-config` (27 settings that a Kafka 4 broker silently ignores), `invalid-config-value`, `deprecated-broker-config`.
* CLI in scripts, Dockerfiles, Makefiles, YAML and CI files: `removed-cli-option`, `deprecated-cli-option`, `bootstrap-server-format`, `removed-tool-class`, `zookeeper-script`, `kraft-config-path`.
* Client `.properties`: `removed-partitioner`, `idempotence-in-flight`.
* Text, markdown, JSON, GitHub annotation and SARIF 2.1.0 output; PR mode (`--base`) with a sticky comment; GitHub Action and pre-commit hook; `# kafka4-ready: ignore [rule]` comments.
