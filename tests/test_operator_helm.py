import os
import textwrap

from kafka4_ready.scan import config_entries, scan, scan_text

FIX = os.path.join(os.path.dirname(__file__), "fixtures")


def got(path):
    r = scan(path)
    return sorted((f.line, f.rule) for f in r.findings)


def test_connect_and_mm2_fixture():
    # KafkaConnect spec.config (host.name is broker-only: not reported on a client), MM2 clusters[].config client key, MM2 connector config (text rule), NodePool untouched
    assert got(os.path.join(FIX, "strimzi", "kafkaconnect-mm2.yaml")) == [(22, "removed-partitioner"), (32, "removed-connector-config")]


def test_helm_values_fixture_maps_and_block():
    r = got(os.path.join(FIX, "helm", "values-bitnami.yaml"))
    assert (4, "removed-broker-config") in r          # config: map
    assert (9, "removed-broker-config") in r          # controller.overrideConfiguration map
    assert any(rule == "deprecated-broker-config" for _, rule in r) and any(ln >= 13 for ln, _ in r)   # extraConfig text block (log.cleaner.enable=false, log.message.format.version=3.5)
    assert all(ln < 17 for ln, _ in r)               # `other.config` is not a Kafka path


def test_nodepool_has_no_entries():
    t = open(os.path.join(FIX, "strimzi", "kafkaconnect-mm2.yaml")).read().split("---")[-1]
    assert config_entries("apiVersion: kafka.strimzi.io/v1beta2" + t.split("apiVersion: kafka.strimzi.io/v1beta2", 1)[-1]) == []


def test_helm_only_in_values_files_that_mention_kafka():
    t = "config:\n  log.message.format.version: '3.5'\n"
    assert config_entries(t, "values.yaml") == []                       # no mention of kafka
    assert config_entries("# kafka\n" + t, "values.yaml")
    assert config_entries("# kafka\n" + t, "deployment.yaml") == []      # not a values file
    assert config_entries("# kafka\n" + t, "values-prod.yaml")


def test_helm_dependency_alias_and_template_lines():
    t = textwrap.dedent("""\
        kafka:
          controller:
            config:
              log.message.format.version: "3.5"
              "{{ .Values.k }}": x
              num.io.threads: {{ .Values.n }}
        """)
    e = config_entries(t, "values.yaml")
    assert [(k, ln) for _, k, _, ln, _ in e] == [("log.message.format.version", 4)]


def test_ignore_comment_in_helm_values():
    t = "# kafka\nconfig:\n  log.message.format.version: '3.5'  # kafka4-ready: ignore removed-broker-config\n"
    found, _ = scan_text(t, "values.yaml", "yaml", "values.yaml")
    assert found == []


def test_strimzi_kafka_still_broker_role():
    t = "apiVersion: kafka.strimzi.io/v1beta2\nkind: Kafka\nspec:\n  kafka:\n    config:\n      host.name: x\n      log.message.format.version: '3.5'\n"
    found, _ = scan_text(t, "k.yaml", "yaml", "k.yaml")
    assert sorted(f.line for f in found) == [6, 7]
