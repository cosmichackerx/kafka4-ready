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

Why a static scanner? The broker itself will not tell you: on Kafka 4.3.1 the 27 removed broker settings below were all accepted without a warning, and `kafka-configs.sh --describe --all`
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
| Each rule's claim about Kafka 4 (rejects / ignores / warns) is true | A **real KRaft broker** (formatted with `kafka-storage.sh`, started with `kafka-server-start.sh`) and the real tools of **Kafka 4.3.1** (2026-09), run by [`tests/oracle/run_oracle.py`](tests/oracle/run_oracle.py) on every push; SHA-512 pinned tarballs | 68 cases for 12 rules: 27 removed broker settings, 3 format-time rejections, 2 deprecated settings, 11 removed and 6 deprecated CLI options, 3 bootstrap-server cases, 6 removed classes, 4 ZooKeeper scripts, 3 KRaft paths, 3 client cases | 0 disagreements required for CI to pass; the agreement was 68/68 on 4.3.1 | Linux and Java 17 only, Scala 2.13 tarballs. A case passes on what the tool printed or what the broker described, so it shows the *message*, not that nothing else changed |
| The same on older releases | Kafka **4.0.1, 4.1.2, 4.2.2** (same cases, [matrix](docs/oracle-matrix.md)) | 68 x 3 | 68/68 on each, with the harness adapting two options that did not exist before 4.2; deprecations show up from the release that introduced them (4.1, 4.2, 4.3) and not before | Other 4.x patch releases and Kafka 3.x were not run. Behaviour can change between releases, so a rule is true **for the tested versions**, nothing else |
| "Removed settings are silently ignored" | `kafka-configs --describe --all` on the running broker: a known setting shows its value, an unknown one shows `=null sensitive=true`, with two known settings as controls | 27 | all 27 shown as unknown, the broker started without a log line about them | This is the broker's own report of what it knows, not a test that a changed value has no effect. All 27 were added at once to one broker |
| Detection works on real-world files | Unit tests (100+ unit tests) and the two fixtures | see the CI run | pass on Ubuntu, Windows, macOS x Python 3.9, 3.11, 3.13 | **No precision study on real repositories has been run**; false positive and miss rates in the wild are unknown |
| Image environment variables (`KAFKA_ZOOKEEPER_CONNECT`, `KAFKA_CFG_*`) map to the setting | The apache/kafka and Bitnami image documentation (read, not run) | - | the mapping is applied by name | **Not run**: no container was started. The scanner labels such findings accordingly |
| Kafka Connect, MirrorMaker 2, Streams | - | 0 | **not covered** | no rule exists; see the roadmap |

Oracle outcomes, condensed (the full 68-row table per release is printed in the CI job summary):

| Case | Kafka 4 behaviour (observed) |
|---|---|
| no `process.roles` (ZooKeeper-mode file) | `kafka-storage.sh format` fails: `ConfigException: Missing required configuration "process.roles"` |
| 27 removed settings (`zookeeper.*` x 13, `log.message.format.version`, `message.format.version`, `inter.broker.protocol.version`, `offsets.commit.required.acks`, `log.message.timestamp.difference.max.ms`, `delegation.token.master.key`, `metrics.jmx.blacklist/whitelist`, `auto.include.jmx.reporter`, `host.name`, `port`, `advertised.host.name/port`, `broker.id.generation.enable`) | broker **starts**; `kafka-configs --describe --all` shows each as `<key>=null sensitive=true`; a known setting shows its value |
| `remote.log.manager.copier.thread.pool.size=-1` (also `expiration`) | refuses to start: `Value must be at least 1` |
| `log.cleaner.enable=false`, `group.coordinator.rebalance.protocols` | starts; the broker log says the setting is deprecated |
| `kafka-topics/-configs/-reassign-partitions/-consumer-groups --zookeeper`, `kafka-acls --authorizer*`, `--zk-tls-config-file`, `kafka-console-consumer --whitelist`, `kafka-replica-verification --topic-white-list`, `kafka-verifiable-consumer --broker-list` | `... is not a recognized option` / `unrecognized arguments` |
| `kafka-run-class.sh kafka.tools.JmxTool` (and 5 more classes) | `Could not find or load main class` |
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

## Rules (12)

| Rule | Default severity | What it finds | Tested on |
|---|---|---|---|
| `zookeeper-mode` | error | a broker file (`server|broker|controller*.properties`, or any file with `log.dirs`/`zookeeper.connect`) without `process.roles` | oracle: `kafka-storage format` fails |
| `removed-broker-config` | warning | the 27 removed settings above, in `.properties`, in `KAFKA_*` / `KAFKA_CFG_*` environment variables (compose, Dockerfile, k8s) and in `key=value` blocks of YAML. `host.name`, `port`, `advertised.*`, `broker.id.generation.enable`, `metrics.jmx.*` and `auto.include.jmx.reporter` count only in broker files (clients and Connect use some of them) | oracle: describe shows `=null`. The environment-variable mapping is the apache/kafka and Bitnami **image convention** and was **not run** |
| `invalid-config-value` | error | `remote.log.manager.copier|expiration.thread.pool.size` below 1 | oracle: format fails |
| `deprecated-broker-config` | note | `log.cleaner.enable=false` (deprecated in 4.1), `group.coordinator.rebalance.protocols` (4.3) | oracle: broker log, per release |
| `removed-cli-option` | error | the 11 (tool, option) pairs above | oracle: tool rejects it, a control run does not |
| `deprecated-cli-option` | note | `--producer.config`, `--consumer.config`, `--producer-property`, `--consumer-property` (console tools), `--producer-props` (producer perf test), `--messages` (consumer perf test): deprecated in 4.2, still accepted | oracle: warning text, per release |
| `bootstrap-server-format` | error | space-separated brokers in `--bootstrap-server` of `kafka-topics`, `kafka-configs`, `kafka-console-consumer` | oracle: client fails to construct |
| `removed-tool-class` | error | `kafka-run-class.sh` with `kafka.admin.FeatureCommand`, `kafka.tools.{ClusterTool,EndToEndLatency,StateChangeLogMerger,StreamsResetter,JmxTool}` | oracle: `Could not find or load main class` |
| `zookeeper-script` | error | `zookeeper-server-start|stop`, `zookeeper-shell`, `zookeeper-security-migration` | oracle: no such file in the distribution |
| `kraft-config-path` | error | `config/kraft/{server,broker,controller}.properties` | oracle: file does not exist |
| `removed-partitioner` | error | `partitioner.class` = `DefaultPartitioner` / `UniformStickyPartitioner` | oracle: client ConfigException |
| `idempotence-in-flight` | error | `enable.idempotence=true` with `max.in.flight.requests.per.connection` above 5 | oracle: client ConfigException |

## GitHub Action

```yaml
- uses: actions/checkout@v7
  with:
    fetch-depth: 0          # only needed for pr-mode
- uses: cosmichackerx/kafka4-ready@v0.1.0
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
    rev: v0.1.0
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
  Tarballs are SHA-512 checked against hashes pinned in the repository. Run it yourself: `python tests/oracle/fetch_kafka.py /tmp/kafka 4.3.1 && python tests/oracle/run_oracle.py --kafka /tmp/kafka/kafka_2.13-4.3.1`.
* **Action, packaging, pre-commit** self-tests in CI, plus a check of the Marketplace limits for `action.yml` (name, description of at most 125 characters, branding).
* CI also runs [dependabot-gaps](https://github.com/cosmichackerx/dependabot-gaps), [node24-ready](https://github.com/cosmichackerx/node24-ready) and [claims-check](https://github.com/cosmichackerx/claims-check) (the numbers in this README are checked against the code) on this repository.

## Related tools

Same author, same style (static, zero dependencies, SARIF, oracle-validated):
[helm4-ready](https://github.com/cosmichackerx/helm4-ready) (Helm 3 to 4), [node24-ready](https://github.com/cosmichackerx/node24-ready) (GitHub Actions on Node 24),
[dependabot-gaps](https://github.com/cosmichackerx/dependabot-gaps), [claims-check](https://github.com/cosmichackerx/claims-check) (keeps README numbers true),
[sha256-ready](https://github.com/cosmichackerx/sha256-ready), [gradle10-ready](https://github.com/cosmichackerx/gradle10-ready), [kotlin24-ready](https://github.com/cosmichackerx/kotlin24-ready).

Other tools for the Kafka 4 move, with what they do differently (read from their documentation on 2026-10-03; not run by me):

* [kraftpilot-cli](https://github.com/Vatsal-Chaudhary/kraftpilot-cli) connects to a live ZooKeeper ensemble and brokers and reports whether the cluster can migrate to KRaft (version and IBP floors, cluster IDs, log directories). It reads a running cluster; kafka4-ready reads your files.
* Confluent's [`kafka-migration-check`](https://docs.confluent.io/platform/current/tools/kraft-migration-tool.html) validates the controller configuration and ZooKeeper state of a Confluent Platform migration in progress.
* [KafkaGuard](https://github.com/KafkaGuard/kafkaguard-releases) is a live-cluster security and compliance scanner (55 controls, PCI-DSS / SOC 2 / ISO 27001 reports).

I found no other static scanner for Kafka 4 config files and scripts in my search; that is not proof there is none.

## Limitations (read these)

* Text matching on logical lines, not a shell parser: a Kafka tool inside `eval`, a variable-built command line, `xargs` or a wrapper script is not seen. Option values given by variable are not inspected (`--bootstrap-server "$BROKERS"` is not flagged).
* Rules hold for Kafka 4.0.1, 4.1.2, 4.2.2 and 4.3.1 (Scala 2.13 tarballs on Linux, Java 17) only; the scanner's messages quote the newest. A rule is only claimed for what was run. Flag and setting lists are **not exhaustive**: only changes reproduced on a real broker or tool are rules.
* Broker settings that are **ignored** are verified through `kafka-configs --describe --all`, not by observing a behaviour change. Client settings that Kafka 4 ignores (for example `auto.include.jmx.reporter` in a producer) could not be told apart from valid ones through the tools, so there is no client rule for them.
* **Kafka Connect, MirrorMaker 2, Kafka Streams and Docker image behaviour are not covered.** In particular the `KAFKA_*` environment variable mapping is mapped by name from the image documentation; the images were not run.
* It does not read Helm charts or operator custom resources (Strimzi `Kafka` resources) and does not check ACLs, topics or data.
* No precision study on real repositories has been run yet. The unit tests and the oracle prove the rules do what they say on the cases they contain, not how often they fire correctly in the wild.

## Roadmap

See the [open issues](https://github.com/cosmichackerx/kafka4-ready/issues).

## License

MIT, see [LICENSE](LICENSE).
