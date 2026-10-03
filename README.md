# kafka4-ready

**Find the Kafka 3 settings and command lines that Kafka 4 rejects, or worse, silently ignores, before you upgrade.** A zero-dependency static scanner (Python 3.9+)
for broker `.properties` files, shell scripts, Makefiles, Dockerfiles, Docker Compose and Kubernetes YAML, Jenkinsfiles and CI files. Kafka 4 removed ZooKeeper, so a
broker file without `process.roles` cannot start, and **a removed setting such as `log.message.format.version`, `inter.broker.protocol.version` or any `zookeeper.*` key does not
make the broker fail: it starts, ignores the key and logs nothing**. The same goes for `kafka-topics.sh --zookeeper`, `kafka-acls.sh --authorizer`, `--bootstrap-server "a:9092 b:9092"`,
`kafka-run-class.sh kafka.tools.JmxTool` and `config/kraft/server.properties`. Every rule was run against **real Kafka 4.0.1, 4.1.2, 4.2.2 and 4.3.1** (a real KRaft broker plus the real tools,
see the [validation table](#validation--results)); nothing is mocked. It emits **SARIF** and GitHub annotations and ships as a **GitHub Action** and a **pre-commit** hook.

[![CI](https://github.com/cosmichackerx/kafka4-ready/actions/workflows/ci.yml/badge.svg)](https://github.com/cosmichackerx/kafka4-ready/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/cosmichackerx/kafka4-ready?sort=semver)](https://github.com/cosmichackerx/kafka4-ready/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Why a static scanner? The broker itself will not tell you: on Kafka 4.3.1 the 35 removed broker settings below were all accepted without a warning, and `kafka-configs.sh --describe --all`
is the only place that shows them as unknown (`zookeeper.connect=null sensitive=true`). The scanner lists every affected line at once, in files, before a rolling upgrade.
It does **not** connect to a cluster, does **not** migrate ZooKeeper metadata and does **not** replace the official [KRaft migration guide](https://kafka.apache.org/documentation/#kraft_zk_migration)
or the [upgrade notes](https://kafka.apache.org/43/getting-started/upgrade/). See [Related tools](#related-tools) for live-cluster checkers.

## At a glance

|  | Lite (try it in a minute) | Full (keep it in CI) |
|---|---|---|
| How | `pipx install git+https://github.com/cosmichackerx/kafka4-ready` then `kafka4-ready .` (read-only, no network) | the [GitHub Action](#github-action) (SARIF, job summary, PR comment), the [pre-commit](#pre-commit) hook and `--base origin/main` [PR mode](#pr-mode) |

## Validation / results

Every number below is from this repository's own tests or scripts. "Not proven" is as important as "Result".

| What is claimed | Checked against | Size | Result | Not proven |
|---|---|---|---|---|
| Each rule's claim about Kafka 4 (rejects / ignores / warns) is true | A **real KRaft broker** (formatted with `kafka-storage.sh`, started with `kafka-server-start.sh`), a **real Connect worker** (REST validate endpoint) and the real tools of **Kafka 4.3.1** (2026-09), run by [`tests/oracle/run_oracle.py`](tests/oracle/run_oracle.py) on every push; SHA-512 pinned tarballs | 94 cases for 14 rules: 35 removed broker settings, 3 format-time rejections, 2 deprecated settings, 15 removed and 6 deprecated CLI options, 3 bootstrap-server cases, 7 removed classes, 4 ZooKeeper and 3 other removed scripts, 3 KRaft paths, 2 partitioner and 3 idempotence cases, 7 Connect cases, 1 image variable mapping | 0 disagreements required for CI to pass; the agreement was 94/94 on 4.3.1 | Linux and Java 17 only, Scala 2.13 tarballs. A case passes on what the tool printed or what the broker described, so it shows the *message*, not that nothing else changed |
| The same on older releases | Kafka **4.0.1, 4.1.2, 4.2.2** (same cases, [matrix](docs/oracle-matrix.md)) | 94 x 3 | 94/94 on each, with the harness adapting two options that did not exist before 4.2; deprecations show up from the release that introduced them (4.1, 4.2, 4.3) and not before | Other 4.x patch releases and Kafka 3.x were not run. Behaviour can change between releases, so a rule is true **for the tested versions**, nothing else |
| "Removed settings are silently ignored" | `kafka-configs --describe --all` on the running broker: a known setting shows its value, an unknown one shows `=null sensitive=true`, with two known settings as controls | 35 | all 35 shown as unknown, the broker started without a log line about them | This is the broker's own report of what it knows, not a test that a changed value has no effect. All 35 were added at once to one broker |
| Detection works on real-world files | A [precision study](docs/precision-study.md): 3,185 files of 2,446 public repositories found by read-only code search, **378 findings labelled by hand** in three samples, plus a miss check with an independent loose regex; plus the unit tests (180+ unit tests) | 216 + 97 + 65 findings | v0.1.0: 186 of 216 correct (86%), **30 false positives, of which 20 were one pattern** (vendored `bin/kafka-features.sh` copies); fixed in v0.2.0; on a fresh sample 96 of 97 correct and 64 of 65 for the findings the quoted-string fix added | The sample is a convenience sample (code search relevance order, queries chosen for these patterns): it says nothing about how common the problems are. One labeller. Four rules had **no hit at all** in the corpus, so their precision in the wild is untested. Recall is only probed (a loose regex), not measured |
| Image environment variables (`KAFKA_ZOOKEEPER_CONNECT`, ...) map to the setting | The **apache/kafka image's own entrypoint code**: `kafka.docker.KafkaDockerWrapper`, which ships in the Kafka tarball, run with a variable for every removed setting (no container needed) | 35 + 2 escapes | all 35 variables became the same settings kafka4-ready maps (`.` = `_`, `_` = `__`, `-` = `___`) on 4.0.1 to 4.3.1 | **The image itself was not run** (no container runtime). The Bitnami `KAFKA_CFG_*` convention is not part of the Kafka distribution and is **not verified** |
| `--fix` rewrites are correct | The fixed commands run on the real tools of 4.0.1, 4.1.2, 4.2.2 and 4.3.1 against a real broker | 13 fix cases x 4 | the original is rejected, the fixed command is accepted and does its job (lists the topic, consumes), idempotent | Kafka 3 was not run, so "also works on Kafka 3" is not claimed; the `config/kraft` rewrite is checked by the target file existing, not by starting a broker with it |
| Kafka Connect, MirrorMaker 2, Streams | A real Connect worker; `PUT /connector-plugins/MirrorSourceConnector/config/validate` lists the settings a connector defines | 7 | `topics.blacklist`, `groups.blacklist`, `config.properties.blacklist`, `use.incremental.alter.configs`, `add.source.alias.to.metrics` and ReplaceField `whitelist` / `blacklist` are no longer defined | One connector class (MirrorSourceConnector) and ReplaceField only; other connectors and Kafka Streams are **not covered** |

Oracle outcomes, condensed (the full 94-row table per release is printed in the CI job summary):

| Case | Kafka 4 behaviour (observed) |
|---|---|
| no `process.roles` (ZooKeeper-mode file) | `kafka-storage.sh format` fails: `ConfigException: Missing required configuration "process.roles"` |
| 35 removed settings (`zookeeper.*` x 21, `log.message.format.version`, `message.format.version`, `inter.broker.protocol.version`, `offsets.commit.required.acks`, `log.message.timestamp.difference.max.ms`, `delegation.token.master.key`, `metrics.jmx.blacklist/whitelist`, `auto.include.jmx.reporter`, `host.name`, `port`, `advertised.host.name/port`, `broker.id.generation.enable`) | broker **starts**; `kafka-configs --describe --all` shows each as `<key>=null sensitive=true`; a known setting shows its value |
| `remote.log.manager.copier.thread.pool.size=-1` (also `expiration`) | refuses to start: `Value must be at least 1` |
| `log.cleaner.enable=false`, `group.coordinator.rebalance.protocols` | starts; the broker log says the setting is deprecated |
| `kafka-topics/-configs/-reassign-partitions/-consumer-groups --zookeeper`, `kafka-acls --authorizer*`, `--zk-tls-config-file`, `kafka-console-consumer --whitelist/--zookeeper/--new-consumer`, `kafka-console-producer` and `kafka-consumer-perf-test --broker-list`, `kafka-replica-verification --topic-white-list`, `kafka-verifiable-consumer --broker-list` | `... is not a recognized option` / `unrecognized arguments` |
| `kafka-run-class.sh kafka.tools.JmxTool` (and 6 more classes, `kafka.tools.MirrorMaker` among them) | `Could not find or load main class` |
| `kafka-mirror-maker.sh`, `kafka-preferred-replica-election.sh`, `kafka-consumer-offset-checker.sh` | no such file in the distribution |
| `topics.blacklist` (MirrorMaker 2), ReplaceField `whitelist` / `blacklist` | a real Connect worker's validate endpoint no longer lists the key (its replacement is listed) |
| `KAFKA_ZOOKEEPER_CONNECT` and 34 more variables | the image's `KafkaDockerWrapper` writes the same settings that kafka4-ready maps |
| `--bootstrap-server "a:9092 b:9092"` | `KafkaException: Failed to create new KafkaAdminClient` |
| `partitioner.class` = `DefaultPartitioner` or `UniformStickyPartitioner` | client `ConfigException: Invalid value ...` |
| `enable.idempotence=true` with `max.in.flight.requests.per.connection=6` | client `ConfigException` |

## Install and run

```
pipx install git+https://github.com/cosmichackerx/kafka4-ready      # or: pip install git+https://github.com/cosmichackerx/kafka4-ready
kafka4-ready .                          # scan the current repository
kafka4-ready . --base origin/main       # PR mode: only what this branch introduces
kafka4-ready . -f sarif -o kafka4.sarif --fail-on never
kafka4-ready --list-rules
```

From a checkout without installing: `PYTHONPATH=src python -m kafka4_ready .`

Exit code: 0 clean, 1 findings at or above `--fail-on` (default `error`), 2 usage error. Output formats: `text`, `markdown`, `json`, `github` (annotations), `sarif`.
Options: `--ignore GLOB`, `--disable RULE`, `--only RULE`. Suppress one finding with a comment on the same or the previous line:
`# kafka4-ready: ignore removed-cli-option` (any comment syntax; no rule id means all rules).

## Example output

```
ops.sh
      2:21   error   removed-cli-option       kafka-topics --zookeeper: Kafka 4.3.1 rejects it ("--zookeeper is not a recognized option" or its equivalent); use --bootstrap-server
      3:21   error   bootstrap-server-format  kafka-topics --bootstrap-server "b1:9092 b2:9092": the brokers are separated by spaces; Kafka 4.3.1 fails with "Failed to create new KafkaAdminClient". Use commas
      4:24   error   removed-tool-class       kafka-run-class kafka.tools.JmxTool: Kafka 4.3.1 fails with "Could not find or load main class"; use kafka-jmx.sh
      5:1    error   zookeeper-script         zookeeper-server-start.sh: the Kafka 4.3.1 distribution contains no ZooKeeper script (ZooKeeper support was removed in Kafka 4.0); Remove the ZooKeeper step; use KRaft tools (kafka-met…
      6:27   error   kraft-config-path        config/kraft/server.properties: Kafka 4.3.1 has no config/kraft directory (server.properties is KRaft by default); Use config/server.properties, config/broker.properties or config/cont…

server.properties
      4:1    warning removed-broker-config    zookeeper.connect: ZooKeeper mode is gone in Kafka 4; remove it (migrate to KRaft first). A Kafka 4.3.1 broker starts and ignores it without a log line (`kafka-configs --describe --all…
      4:1    error   zookeeper-mode           no process.roles: this is a ZooKeeper-mode broker file; Kafka 4.3.1 refuses to start ("Missing required configuration \"process.roles\""). Migrate the cluster to KRaft before upgrading…
      5:1    warning removed-broker-config    zookeeper.connection.timeout.ms: ZooKeeper mode is gone in Kafka 4; remove it (migrate to KRaft first). A Kafka 4.3.1 broker starts and ignores it without a log line (`kafka-configs --…
      6:1    warning removed-broker-config    log.message.format.version: removed; KRaft always uses record format v2, there is no replacement. A Kafka 4.3.1 broker starts and ignores it without a log line (`kafka-configs --descri…
      7:1    warning removed-broker-config    inter.broker.protocol.version: removed; in KRaft choose the metadata version with kafka-storage.sh format --release-version or kafka-features.sh. A Kafka 4.3.1 broker starts and ignore…
      8:1    note    deprecated-broker-config log.cleaner.enable=false: deprecated since Kafka 4.1; the log cleaner is always enabled in Kafka 5.0 and this setting is ignored

2 file(s) scanned, 5 Kafka tool command(s) read, checked against Kafka 4.3.1. 6 error, 4 warning, 1 note.
```

(That is `tests/fixtures/legacy-ci`; its migrated twin `tests/fixtures/clean-ci` reports nothing.)

## Rules (14)

| Rule | Default severity | What it finds | Tested on |
|---|---|---|---|
| `zookeeper-mode` | error | a broker file (named `*server*`, `*broker*`, `*controller*`, `kafka*.properties`, or with a broker-only key such as `log.dirs` or `broker.id`; **not** `consumer`/`producer`/`client` files) without `process.roles` | oracle: `kafka-storage format` fails |
| `removed-broker-config` | warning | the 35 removed settings above, in `.properties`, in `KAFKA_*` / `KAFKA_CFG_*` environment variables (compose, Dockerfile, k8s, including the `- name: KAFKA_...` form) and in `key=value` blocks of YAML. `host.name`, `port`, `advertised.*`, `broker.id.generation.enable`, `metrics.jmx.*` and `auto.include.jmx.reporter` count only in broker files (clients and Connect use some of them) | oracle: describe shows `=null`. The apache/kafka variable mapping was checked against the image's own `KafkaDockerWrapper`; `KAFKA_CFG_*` (Bitnami) is **not verified** |
| `invalid-config-value` | error | `remote.log.manager.copier|expiration.thread.pool.size` below 1 | oracle: format fails |
| `deprecated-broker-config` | note | `log.cleaner.enable=false` (deprecated in 4.1), `group.coordinator.rebalance.protocols` (4.3) | oracle: broker log, per release |
| `removed-cli-option` | error | the 15 (tool, option) pairs above | oracle: tool rejects it, a control run does not |
| `deprecated-cli-option` | note | `--producer.config`, `--consumer.config`, `--producer-property`, `--consumer-property` (console tools), `--producer-props` (producer perf test), `--messages` (consumer perf test): deprecated in 4.2, still accepted | oracle: warning text, per release |
| `bootstrap-server-format` | error | space-separated brokers in `--bootstrap-server` of `kafka-topics`, `kafka-configs`, `kafka-console-consumer` | oracle: client fails to construct |
| `removed-tool-class` | error | `kafka-run-class.sh` with `kafka.admin.FeatureCommand`, `kafka.tools.{ClusterTool,EndToEndLatency,StateChangeLogMerger,StreamsResetter,JmxTool,MirrorMaker}`; copies of Kafka's own `bin/kafka-*.sh` wrappers are skipped | oracle: `Could not find or load main class` |
| `zookeeper-script` | error | `zookeeper-server-start|stop`, `zookeeper-shell`, `zookeeper-security-migration` (a Dockerfile `COPY` of your own script is not a run) | oracle: no such file in the distribution |
| `removed-tool-script` | error | `kafka-mirror-maker`, `kafka-preferred-replica-election`, `kafka-consumer-offset-checker` | oracle: no such file in the distribution |
| `removed-connector-config` | warning | MirrorMaker 2 `topics.blacklist`, `groups.blacklist`, `config.properties.blacklist`, `use.incremental.alter.configs`, `add.source.alias.to.metrics`; ReplaceField `transforms.x.whitelist` / `blacklist` | oracle: a real Connect worker's validate endpoint |
| `kraft-config-path` | error | `config/kraft/{server,broker,controller}.properties` (not where your Dockerfile `COPY` or volume mount creates that path) | oracle: file does not exist |
| `removed-partitioner` | error | `partitioner.class` = `DefaultPartitioner` / `UniformStickyPartitioner` | oracle: client ConfigException |
| `idempotence-in-flight` | error | `enable.idempotence=true` with `max.in.flight.requests.per.connection` above 5 | oracle: client ConfigException |

## Fix the mechanical ones (`--fix`)

```
kafka4-ready . --diff      # show the changes as a unified diff, write nothing (exit 1 if there would be any)
kafka4-ready . --fix       # apply them in place
```

Only spellings with exactly one correct replacement are rewritten, in shell scripts, Dockerfiles, Makefiles, YAML (compose, Kubernetes, CI) and the like; `.properties` files are never touched.

| Before | After |
|---|---|
| `kafka-console-consumer --whitelist x` | `--include x` |
| `kafka-replica-verification --topic-white-list x` | `--topics-include x` |
| `--broker-list` of `kafka-console-producer`, `kafka-consumer-perf-test`, `kafka-verifiable-consumer` | `--bootstrap-server` |
| `kafka-console-consumer --new-consumer` | (option deleted) |
| `--bootstrap-server "a:9092 b:9092"` (`kafka-topics`, `kafka-configs`, `kafka-console-consumer`) | `"a:9092,b:9092"` |
| `config/kraft/{server,broker,controller}.properties` in a command | `config/{...}.properties` (not where a `COPY` or volume mount creates the path) |

Not rewritten, because they need a decision: `--zookeeper` (the replacement is another host), `kafka-acls --authorizer*`, ZooKeeper and MirrorMaker 1 scripts, removed settings, tool classes, `--producer.config` and friends (they only exist from Kafka 4.2).
Each rewrite is run on the real tools by [`tests/oracle/run_fix_oracle.py`](tests/oracle/run_fix_oracle.py) (13 fix cases on Kafka 4.0.1, 4.1.2, 4.2.2 and 4.3.1: the original command must be rejected, the fixed one must run), the fixer is idempotent (a second run changes nothing; unit-tested), and only the option itself changes (indentation, line endings, comments stay).
The new spellings are for Kafka 4: `config/server.properties` is a ZooKeeper-mode file in Kafka 3, so do not run `--fix` on a file that must keep working on both. **Not verified:** the same commands on Kafka 3 (no Kafka 3 binary was run), values given by variable (skipped), and anything that is not on the list above.

## GitHub Action

```yaml
- uses: actions/checkout@v7
  with:
    fetch-depth: 0          # only needed for pr-mode
- uses: cosmichackerx/kafka4-ready@v0.5.0
  with:
    path: .
    fail-on: error          # error | warning | never
    pr-mode: true           # on pull requests report only what the PR introduces
    comment: true           # one sticky comment, updated in place (needs pull-requests: write; skipped for forks)
    sarif-file: kafka4.sarif
```

Inputs: `path`, `fail-on`, `disable`, `ignore`, `summary` (job summary), `pr-mode`, `base`, `comment`, `github-token`, `sarif-file`. Upload the SARIF with
`github/codeql-action/upload-sarif`. The action runs the scanner from its own checkout with the runner's Python; it does not install anything and makes no network calls except the sticky comment.

## pre-commit

```yaml
repos:
  - repo: https://github.com/cosmichackerx/kafka4-ready
    rev: v0.5.0
    hooks:
      - id: kafka4-ready        # report; fails the commit on errors
```

The hook scans the whole repository (`pass_filenames: false`). CI checks it with `pre-commit try-repo`.

## PR mode

`kafka4-ready . --base origin/main` scans the merge base and the working tree and reports only findings that are new (matched by rule, file and the text of the line, so moving a line does not make it new).
In the Action, `pr-mode: true` does this on pull requests. Needs git history (`fetch-depth: 0`).

## How it is verified

* **Unit tests** (`pytest`, 100+ cases, [`tests/`](tests/)): positives and negatives per rule, `\` continuations, JSON-style `CMD [...]`, comments and prose, suppression, outputs, PR mode and the sticky comment.
  CI runs them on Ubuntu, Windows and macOS with Python 3.9, 3.11 and 3.13.
* **Oracle** ([`tests/oracle/run_oracle.py`](tests/oracle/run_oracle.py)): formats and starts one real KRaft broker per release (Java 17) with every removed setting added, and runs the real tools and a console producer.
  Also starts a Connect worker (heap 256 MB) and runs the image's `KafkaDockerWrapper`. Tarballs are SHA-512 checked against hashes pinned in the repository. Run it yourself: `python tests/oracle/fetch_kafka.py /tmp/kafka 4.3.1 && python tests/oracle/run_oracle.py --kafka /tmp/kafka/kafka_2.13-4.3.1`.
* **Precision study** ([`docs/precision-study.md`](docs/precision-study.md), scripts in [`study/`](study/)): read-only code search over public repositories, hand-labelled samples.
* **Action, packaging, pre-commit** self-tests in CI, plus a check of the Marketplace limits for `action.yml` (name, description of at most 125 characters, branding).
* CI also runs [dependabot-gaps](https://github.com/cosmichackerx/dependabot-gaps), [node24-ready](https://github.com/cosmichackerx/node24-ready) and [claims-check](https://github.com/cosmichackerx/claims-check) (the numbers in this README are checked against the code) on this repository.

## Related tools

Same author, same style (static, zero dependencies, SARIF, oracle-validated):
[helm4-ready](https://github.com/cosmichackerx/helm4-ready) (Helm 3 to 4), [pandas3-ready](https://github.com/cosmichackerx/pandas3-ready) (pandas 2 to 3), [node24-ready](https://github.com/cosmichackerx/node24-ready) (GitHub Actions on Node 24),
[dependabot-gaps](https://github.com/cosmichackerx/dependabot-gaps), [claims-check](https://github.com/cosmichackerx/claims-check) (keeps README numbers true),
[sha256-ready](https://github.com/cosmichackerx/sha256-ready), [gradle10-ready](https://github.com/cosmichackerx/gradle10-ready), [kotlin24-ready](https://github.com/cosmichackerx/kotlin24-ready).

Other tools for the Kafka 4 move, with what they do differently (read from their documentation on 2026-10-03; not run by me):

* [kraftpilot-cli](https://github.com/Vatsal-Chaudhary/kraftpilot-cli) connects to a live ZooKeeper ensemble and brokers and reports whether the cluster can migrate to KRaft (version and IBP floors, cluster IDs, log directories). It reads a running cluster; kafka4-ready reads your files.
* Confluent's [`kafka-migration-check`](https://docs.confluent.io/platform/current/tools/kraft-migration-tool.html) validates the controller configuration and ZooKeeper state of a Confluent Platform migration in progress.
* [KafkaGuard](https://github.com/KafkaGuard/kafkaguard-releases) is a live-cluster security and compliance scanner (55 controls, PCI-DSS / SOC 2 / ISO 27001 reports).

I found no other static scanner for Kafka 4 config files and scripts in my search; that is not proof there is none.

## Strimzi resources and Helm values

YAML documents with `apiVersion: kafka.strimzi.io/...` have the config map of these kinds read like a `.properties` file (line numbers point at the real YAML line; `# kafka4-ready: ignore <rule>` works):

| Kind | Path | Read as | Rules |
|---|---|---|---|
| `Kafka` | `spec.kafka.config` | broker | `removed-broker-config`, `invalid-config-value`, `deprecated-broker-config` (the v0.2.0 study had found 83 `log.message.format.version` / `inter.broker.protocol.version` lines there that nothing saw) |
| `KafkaConnect` | `spec.config` | client | `removed-broker-config` (client-valid keys only), `removed-partitioner`, `idempotence-in-flight` |
| `KafkaMirrorMaker2` | `spec.clusters[].config` | client | same as above |
| `KafkaMirrorMaker2`, `KafkaConnector` | connector `config` maps (`topics.blacklist`, `use.incremental.alter.configs`, ReplaceField `whitelist`/`blacklist`) | text rule, any file | `removed-connector-config` |
| `KafkaBridge` | `spec.producer.config`, `spec.consumer.config`, `spec.admin.config` | client | same as KafkaConnect |
| `KafkaNodePool` | (none) | has no config map: roles, storage, resources, replicas only | nothing to check |

Helm: in a file named `values*.yaml` that mentions `kafka` or `kraft`, the key/value maps `config`, `overrideConfiguration`, `controller.config`, `controller.overrideConfiguration`, `broker.config`, `broker.overrideConfiguration` (the layout of the Bitnami `kafka` chart 32.x; an outer `kafka:` for a dependency is accepted) and the text blocks `extraConfig: |` (older charts, `controller.`/`broker.` too) are read as broker settings. A `config:` anywhere else (for example under `other:`) is not read.

```yaml
config:
  log.message.format.version: "3.5"      # 3:1  warning  removed-broker-config
```

What this does **not** prove: the Kafka side (removed keys are ignored by a 4.x broker, invalid values are refused) comes from the oracle on real brokers. What Strimzi or a chart does with a key before it reaches the broker (it may reject, rewrite or manage some options itself, e.g. `process.roles`, listeners) was **not** run: there is no Kubernetes, operator, Helm or Docker in the test setup. The Bitnami layout comes from reading the chart's `values.yaml`, not from rendering it. The reader is plain indentation tracking, not a YAML parser: flow style (`config: {a: b}`), anchors and Helm template lines are skipped; Kustomize patches are not read. ZooKeeper sections (`spec.zookeeper`, `zookeeper.enabled`) are not reported (Strimzi 0.46+ dropped ZooKeeper per its release notes; not checked here).

## Limitations (read these)

* Text matching on logical lines, not a shell parser: a Kafka tool inside `eval`, a variable-built command line, `xargs` or a wrapper script is not seen. Option values given by variable are not inspected (`--bootstrap-server "$BROKERS"` is not flagged).
* Rules hold for Kafka 4.0.1, 4.1.2, 4.2.2 and 4.3.1 (Scala 2.13 tarballs on Linux, Java 17) only; the scanner's messages quote the newest. A rule is only claimed for what was run. Flag and setting lists are **not exhaustive**: only changes reproduced on a real broker or tool are rules.
* Broker settings that are **ignored** are verified through `kafka-configs --describe --all`, not by observing a behaviour change. Client settings that Kafka 4 ignores (for example `auto.include.jmx.reporter` in a producer) could not be told apart from valid ones through the tools, so there is no client rule for them.
* **Kafka Streams is not covered, Connect only for MirrorSourceConnector and ReplaceField, and no container image was run.** The `KAFKA_*` variable mapping was checked against the apache/kafka image's own wrapper code (not the image); Bitnami's `KAFKA_CFG_*` mapping is by name only.
* It does not render Helm charts and does not check ACLs, topics or data. Of operator resources and charts it reads only the config maps listed in "Strimzi resources and Helm values" below; Kustomize patches and other charts are not read.
* The [precision study](docs/precision-study.md) is a convenience sample labelled by one person; four rules never fired in it. A finding is true for the file as written, not proof that a deployment breaks: many hits are old Kafka 0.8 to 3.x fixtures, copies of the same course repository, or templates.

## Roadmap

See the [open issues](https://github.com/cosmichackerx/kafka4-ready/issues).

## License

MIT, see [LICENSE](LICENSE).
