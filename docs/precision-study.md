# Precision study on public repositories

How often does each kafka4-ready rule fire on a line that really is a Kafka 3 construct that Kafka 4 breaks or ignores? This is a measurement of the **rules on real files**, done once for v0.2.0.
It does not say how common the problems are, and it is not a recall measurement. Everything here can be re-run with the scripts in [`study/`](../study).

## What was done

1. **Collect** (`study/collect.py`): 24 GitHub code search queries (read-only, `gh api search/code`, 3 pages each) for the constructs the rules know, for example `kafka-topics.sh --zookeeper`,
   `log.message.format.version`, `KAFKA_ZOOKEEPER_CONNECT filename:docker-compose.yml`, `config/kraft/server.properties`, `kafka.tools.JmxTool`. 3,482 hits in 2,446 repositories. Nothing was opened, starred, forked or commented on.
2. **Download** (`study/download.py`): the files at the commit code search returned, from raw.githubusercontent.com, at most 6 per repository. 3,185 files, 89 failed. The list is [`study/corpus.tsv`](../study/corpus.tsv) (repository, commit, path), so the corpus can be rebuilt. The files themselves are not committed.
3. **Scan** (`study/run_scan.py`) with the released scanner: 2,756 Kafka tool command lines in 807 repositories, 4,082 findings with v0.1.0.
4. **Label by hand.** I read each finding in a fixed-seed random sample **per rule** (`study/sample.py`) with two lines of context and decided: *true* = the line is the Kafka 3 construct the message names, in a place a user would run or load it; *false* = it is not (wrong kind of file, not a run, copy of Kafka's own file). The labels, with a reason for every false one, are in [`study/labels.json`](../study/labels.json), the samples in [`study/samples/`](../study/samples).
5. **Fix** the false positives (see below), re-scan, and label a **fresh** sample (B, a different seed) and, for the findings the recall fix added, **all** of them (C). Sample B and C were not used to design the fixes.
6. **Probe for misses** with an independent loose regex (`study/loose.py`), and read a random sample of what the scanner did not flag.

## Sample A: v0.1.0 (the released scanner), 216 findings

| Rule | Findings | Repos | Hand-checked | True | False | Precision (95% Wilson) |
|---|---:|---:|---:|---:|---:|---|
| `zookeeper-mode` | 457 | 332 | 30 | 26 | 4 | 87% (70% to 95%) |
| `removed-broker-config` | 2454 | 925 | 35 | 34 | 1 | 97% (85% to 99%) |
| `invalid-config-value` | 0 | 0 | 0 | 0 | 0 | no sample |
| `deprecated-broker-config` | 202 | 143 | 20 | 20 | 0 | 100% (84% to 100%) |
| `removed-cli-option` | 128 | 47 | 30 | 30 | 0 | 100% (89% to 100%) |
| `deprecated-cli-option` | 36 | 12 | 20 | 20 | 0 | 100% (84% to 100%) |
| `bootstrap-server-format` | 0 | 0 | 0 | 0 | 0 | no sample |
| `removed-tool-class` | 179 | 140 | 25 | 5 | 20 | 20% (9% to 39%) |
| `removed-tool-script` | 0 | 0 | 0 | 0 | 0 | no sample |
| `zookeeper-script` | 359 | 258 | 30 | 28 | 2 | 93% (79% to 98%) |
| `kraft-config-path` | 266 | 94 | 25 | 22 | 3 | 88% (70% to 96%) |
| `removed-partitioner` | 0 | 0 | 0 | 0 | 0 | no sample |
| `idempotence-in-flight` | 0 | 0 | 0 | 0 | 0 | no sample |
| `removed-connector-config` | 1 | 1 | 1 | 1 | 0 | 100% (21% to 100%) |
| all | 4082 | 1394 | 216 | 186 | 30 | 86% (81% to 90%) |

Read it as: **86% of the sampled findings were right, 14% were not, and one pattern caused two thirds of the errors.**
Wilson intervals are for the *sampled population* (a convenience sample), not for all repositories. Rules with 20 to 35 labels cannot tell 95% from 100%.

| False positive pattern | Rule | n in sample | Cause | Fix in v0.2.0 |
|---|---|---:|---|---|
| `bin/kafka-features.sh` and similar wrappers committed as part of a vendored Kafka 2.x/3.x tarball | `removed-tool-class` | 20 of 25 | The file is Kafka's own script (`exec kafka-run-class.sh kafka.admin.FeatureCommand`), not a use of it | Files named like a distribution wrapper that `exec` kafka-run-class are skipped |
| `consumer.properties` of old ZooKeeper consumers with `zookeeper.connect` | `zookeeper-mode` | 4 of 30 | `zookeeper.connect` alone made any file a broker file | A broker file needs a broker-looking name or a broker-only key; `consumer`/`producer`/`client` names never count |
| `zookeeper.connection.timeout.ms` in a client app's config | `removed-broker-config` | 1 of 35 | The `zookeeper.*` keys were flagged in any `.properties` | `zookeeper.*` keys count only in broker files |
| Dockerfile `COPY scripts/zookeeper-server-stop.sh ...` | `zookeeper-script` | 2 of 30 | Copying your own file is not running Kafka's | `COPY`/`ADD` lines are skipped for this rule |
| `COPY x /opt/kafka/config/kraft/server.properties`, volume mount onto that path | `kraft-config-path` | 3 of 25 | The repository creates the path itself, so it exists in a Kafka 4 image too | `COPY`/`ADD` destinations and the container side of a mount are skipped |

Re-scanning the same 216 findings with the fixed scanner: all 30 false positives are gone and all 186 true ones are still reported (this is **not** an independent check, the fixes were made from these labels; see samples B and C).

## Sample B: v0.2.0, fresh sample (seed 777, at most 12 per rule), 97 findings

| Rule | Findings | Repos | Hand-checked | True | False | Precision (95% Wilson) |
|---|---:|---:|---:|---:|---:|---|
| `zookeeper-mode` | 400 | 299 | 12 | 12 | 0 | 100% (76% to 100%) |
| `removed-broker-config` | 2405 | 958 | 12 | 12 | 0 | 100% (76% to 100%) |
| `invalid-config-value` | 0 | 0 | 0 | 0 | 0 | no sample |
| `deprecated-broker-config` | 205 | 146 | 12 | 12 | 0 | 100% (76% to 100%) |
| `removed-cli-option` | 167 | 58 | 12 | 12 | 0 | 100% (76% to 100%) |
| `deprecated-cli-option` | 37 | 13 | 12 | 12 | 0 | 100% (76% to 100%) |
| `bootstrap-server-format` | 0 | 0 | 0 | 0 | 0 | no sample |
| `removed-tool-class` | 41 | 16 | 12 | 12 | 0 | 100% (76% to 100%) |
| `removed-tool-script` | 1 | 1 | 0 | 0 | 0 | no sample |
| `zookeeper-script` | 398 | 304 | 12 | 12 | 0 | 100% (76% to 100%) |
| `kraft-config-path` | 247 | 92 | 12 | 11 | 1 | 92% (65% to 99%) |
| `removed-partitioner` | 0 | 0 | 0 | 0 | 0 | no sample |
| `idempotence-in-flight` | 0 | 0 | 0 | 0 | 0 | no sample |
| `removed-connector-config` | 1 | 1 | 1 | 1 | 0 | 100% (21% to 100%) |
| all | 3902 | 1338 | 97 | 96 | 1 | 99% (94% to 100%) |

One false positive: an entrypoint that appends to `kafka/config/kraft/server.properties` in an image whose Dockerfile `COPY`s that file in (`kraft-config-path`). It is a context the scanner cannot see across files.

## Sample C: findings added by scanning commands inside quoted strings, 65 findings (all of them)

v0.2.0 also scans `bash -c "..."`, compose `command: "..."`, `CMD ["sh", "-c", "..."]` and `echo "..." >> start.sh`. That added 65 findings in the same corpus; I labelled every one.

| Rule | Findings | Repos | Hand-checked | True | False | Precision (95% Wilson) |
|---|---:|---:|---:|---:|---:|---|
| `removed-cli-option` | 167 | 58 | 12 | 12 | 0 | 100% (76% to 100%) |
| `deprecated-cli-option` | 37 | 13 | 1 | 1 | 0 | 100% (21% to 100%) |
| `removed-tool-script` | 1 | 1 | 1 | 1 | 0 | 100% (21% to 100%) |
| `zookeeper-script` | 398 | 304 | 51 | 50 | 1 | 98% (90% to 100%) |
| all | 3902 | 1338 | 65 | 64 | 1 | 98% (92% to 100%) |

The one false positive is `echo "  zookeeper-server-start.sh"`, a help listing in an entrypoint. Several true ones are `echo "...zookeeper-server-start.sh..." >> start.sh` (writing the line that will run) or `echo "to start ZooKeeper run ..."` (user instructions): the line is the construct, whether it breaks a deployment depends on whether the script is used.

## What the sample does and does not say

* **Convenience sample.** Code search returns results in GitHub's relevance order, at most 1,000 per query, and the queries were the patterns themselves. Prevalence ("X% of repositories have this") **cannot** be read from the finding counts, and the repositories skew to tutorials, course material and fixtures.
* **Old fixtures and copies count as true.** Many true findings are Kafka 0.8 to 3.x test fixtures (for example `kafka-python` and `afkak` broker templates), a course repository copied into six accounts (`1-kafka-console-producer.sh`), or Confluent demo scripts. A finding means "this line would break or be ignored on Kafka 4", not "this project is broken today". Counting repositories instead of findings does not remove the duplicates (forks and copies are different repositories).
* **One labeller**, me, with the rule text in view. No second opinion, no inter-rater agreement.
* **Small n.** 12 to 35 labels per rule: a rule with 0 false positives in 12 can still be wrong 1 time in 4 (the Wilson lower bound is 76%).
* **Four rules never fired:** `invalid-config-value`, `bootstrap-server-format`, `removed-partitioner` and `idempotence-in-flight` had **no hit** in 3,185 files, and `removed-tool-script` and `removed-connector-config` had one each. Their precision in the wild is **untested**; they are only backed by the oracle against real Kafka.
* **Recall is not measured.** The loose cross-check (`study/loose.py`) lists lines that mention a known construct; of 3,657 such lines the scanner flagged 2,811 and left 846. Reading a random sample of those 846, they were mostly deliberate non-findings:
  copies of a command already flagged one line earlier (the same command on a continuation line), `zookeeper.*` keys in consumer or client files (`consumer.properties`, 246 lines), shell scripts that *read* `KAFKA_ZOOKEEPER_*` variables (the Bitnami `libkafka.sh`, 195 lines), the `org.apache.kafka.tools.JmxTool` class name of Kafka 3.5 to 3.9 (the loose regex matches inside it, 214 lines, not a miss), and mount destinations.
  **Real gaps found:** (1) Strimzi `Kafka` custom resources with `log.message.format.version: "3.x"` / `inter.broker.protocol.version` in a YAML `config:` block (83 lines): not covered (the roadmap item); (2) `kafka-run-class.sh ${KAFKA_CLASS} ... --zookeeper` with the class in a variable; (3) a `--zookeeper` flag on a continuation line of a command the scanner could not tie to a tool (20 lines).
* **Limits of "true".** The label says the line is what the rule names. It was not checked that the repository's *deployment* would run Kafka 4, only that the line is wrong for it.

## Reproduce

```
python study/collect.py files.json                       # needs gh and a token for the code search API; results change over time
python study/download.py files.json corpus/              # or rebuild from study/corpus.tsv (commits are pinned)
python study/run_scan.py corpus/ scan.json
python study/sample.py scan.json corpus/ sample.md --seed 777 --cap 12
python study/tabulate.py scan.json study/samples/B.json B
python study/loose.py corpus/ scan.json loose.json
```

The corpus is a snapshot of third-party files at pinned commits; it is not redistributed here.
