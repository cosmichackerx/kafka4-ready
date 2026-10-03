import os
import textwrap

from kafka4_ready.scan import scan, scan_text, strimzi_items

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "strimzi")


def run(text, name="kafka.yaml"):
    found, _ = scan_text(textwrap.dedent(text), name, "yaml", name)
    return found


HEAD = """\
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: c
spec:
  kafka:
    config:
"""


def test_legacy_fixture_findings_and_lines():
    r = scan(os.path.join(FIX, "kafka-legacy.yaml"))
    got = sorted((f.line, f.rule) for f in r.findings)
    assert got == [(11, "removed-broker-config"), (12, "removed-broker-config"), (14, "deprecated-broker-config"), (15, "invalid-config-value")]
    assert all(f.file == "kafka-legacy.yaml" for f in r.findings)


def test_clean_fixture_has_nothing():
    assert scan(os.path.join(FIX, "kafka-clean.yaml")).findings == []


def test_items_are_only_spec_kafka_config_of_kafka_documents():
    items = strimzi_items(open(os.path.join(FIX, "kafka-legacy.yaml")).read())
    assert [k for k, *_ in items] == ["offsets.topic.replication.factor", "log.message.format.version", "inter.broker.protocol.version", "num.recovery.threads.per.data.dir",
                                      "log.cleaner.enable", "remote.log.manager.copier.thread.pool.size", "min.insync.replicas"]
    assert dict((k, v) for k, v, *_ in items)["log.message.format.version"] == "3.5"


def test_not_a_kafka_resource_is_ignored():
    assert run("apiVersion: v1\nkind: ConfigMap\nspec:\n  kafka:\n    config:\n      log.message.format.version: '3.5'\n") == []
    assert run(HEAD.replace("kind: Kafka", "kind: KafkaConnect") + "      log.message.format.version: '3.5'\n") == []
    assert run(HEAD.replace("kafka.strimzi.io", "example.com") + "      log.message.format.version: '3.5'\n") == []


def test_other_places_are_ignored():
    assert run("apiVersion: kafka.strimzi.io/v1beta2\nkind: Kafka\nspec:\n  zookeeper:\n    config:\n      log.message.format.version: '3.5'\n") == []
    assert run("apiVersion: kafka.strimzi.io/v1beta2\nkind: Kafka\nspec:\n  kafka:\n    template:\n      log.message.format.version: '3.5'\n") == []


def test_key_after_config_block_not_read():
    t = HEAD + "      min.insync.replicas: 2\n    log.message.format.version: x\n"
    assert run(t) == []


def test_quoted_key_comment_and_line_number():
    f = run(HEAD + '      "log.message.format.version": "3.5"  # old\n')
    assert [(x.rule, x.line) for x in f] == [("removed-broker-config", 8)]


def test_multi_document_line_numbers():
    t = "kind: ConfigMap\n---\n" + HEAD + "      inter.broker.protocol.version: 3.9\n"
    assert [(x.rule, x.line) for x in run(t)] == [("removed-broker-config", 10)]


def test_suppression_comment():
    t = HEAD + "      log.message.format.version: '3.5'  # kafka4-ready: ignore removed-broker-config\n"
    assert run(t) == []


def test_helm_template_lines_skipped():
    t = HEAD + "      {{ .Values.key }}: 1\n      log.cleaner.enable: {{ .Values.x }}\n"
    assert [x.rule for x in run(t)] == []


def test_no_zookeeper_mode_finding_without_process_roles():
    t = HEAD + "      log.dirs: /data\n      broker.id: 1\n"
    assert all(x.rule != "zookeeper-mode" for x in run(t))


def test_deprecated_value_is_unquoted():
    assert [x.rule for x in run(HEAD + "      log.cleaner.enable: false\n")] == ["deprecated-broker-config"]
    assert run(HEAD + "      log.cleaner.enable: true\n") == []
