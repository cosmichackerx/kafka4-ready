import json
import textwrap

import pytest

from kafka4_ready import rules as R
from kafka4_ready.cli import main
from kafka4_ready.rules import RULES, TESTED
from kafka4_ready.scan import kind_of, scan_text, tool_name


def run(text, name="ci.sh", disabled=(), only=()):
    kind = kind_of(name.rsplit("/", 1)[-1], name)
    if kind is None:
        return []
    found, _ = scan_text(textwrap.dedent(text), name, kind, name.rsplit("/", 1)[-1])
    return [f for f in found if f.rule not in disabled and (not only or f.rule in only)]


def ids(findings):
    return sorted(f.rule for f in findings)


# ---------------------------------------------------------------- tool words
@pytest.mark.parametrize("tok,want", [("kafka-topics.sh", "kafka-topics"), ("/opt/kafka/bin/kafka-topics.sh", "kafka-topics"), ("\"kafka-acls\"", "kafka-acls"),
                                      ("bin\\kafka-configs.bat", "kafka-configs"), ("zookeeper-shell.sh", "zookeeper-shell"), ("kafka-ui", "kafka-ui"),
                                      ("kafka", None), ("kafkacat", None), ("my-kafka-topics.sh", None)])
def test_tool_name(tok, want):
    assert tool_name(tok) == want


# ---------------------------------------------------------------- CLI options
@pytest.mark.parametrize("tool,opt,_", R.REMOVED_CLI)
def test_removed_cli_every_entry(tool, opt, _):
    (f,) = run(f"{tool}.sh {opt} x --list")
    assert f.rule == "removed-cli-option" and f.severity == "error" and opt in f.message


def test_removed_cli_equals_form_and_path_prefix():
    assert ids(run("/opt/kafka/bin/kafka-topics.sh --zookeeper=zk:2181 --list")) == ["removed-cli-option"]


def test_removed_cli_only_for_that_tool():
    assert run("kafka-topics.sh --bootstrap-server b:9092 --whitelist x") == []
    assert run("kafka-console-consumer.sh --bootstrap-server b:9092 --include x") == []


def test_removed_cli_in_dockerfile_json_form_and_continuation():
    assert ids(run('CMD ["kafka-topics.sh", "--zookeeper", "zk:2181", "--list"]', "Dockerfile")) == ["removed-cli-option"]
    f = run("kafka-topics.sh \\\n  --create \\\n  --zookeeper zk:2181", "Makefile")
    assert [(x.rule, x.line) for x in f] == [("removed-cli-option", 3)]


def test_option_after_a_pipe_belongs_to_the_next_command():
    assert run("kafka-topics.sh --bootstrap-server b:9092 --list | grep --zookeeper") == []


@pytest.mark.parametrize("tool,opt,_", R.DEPRECATED_CLI)
def test_deprecated_cli_every_entry(tool, opt, _):
    (f,) = run(f"{tool}.sh {opt} x")
    assert f.rule == "deprecated-cli-option" and f.severity == "note"


def test_replacement_options_are_clean():
    assert run("kafka-console-producer.sh --bootstrap-server b:9092 --topic t --command-config c.properties --command-property linger.ms=1") == []


# ---------------------------------------------------------------- bootstrap server
def test_bootstrap_space_separated():
    (f,) = run('kafka-topics.sh --bootstrap-server "a:9092 b:9092" --list')
    assert f.rule == "bootstrap-server-format"
    assert ids(run("kafka-configs.sh --bootstrap-server 'a:1 b:2 c:3' --describe")) == ["bootstrap-server-format"]


@pytest.mark.parametrize("val", ["a:9092,b:9092", "a:9092", '"${BROKERS}"', '"$A $B"', '"a:9092, b:9092"'])
def test_bootstrap_ok_or_unknown(val):
    assert "bootstrap-server-format" not in ids(run(f"kafka-topics.sh --bootstrap-server {val} --list"))


def test_bootstrap_other_tools_not_checked():
    assert run('kafka-broker-api-versions.sh --bootstrap-server "a:1 b:2"') == []


# ---------------------------------------------------------------- scripts, classes, paths
@pytest.mark.parametrize("name", R.ZOOKEEPER_SCRIPTS)
def test_zookeeper_scripts(name):
    assert ids(run(f"bin/{name}.sh config/zookeeper.properties")) == ["zookeeper-script"]


def test_zookeeper_prose_is_not_a_script():
    assert run("# start zookeeper first\necho zookeeper is down") == []


@pytest.mark.parametrize("cls", R.REMOVED_CLASSES)
def test_removed_classes(cls):
    (f,) = run(f"kafka-run-class.sh {cls} --help")
    assert f.rule == "removed-tool-class" and R.REMOVED_CLASSES[cls] in f.message


def test_run_class_other_class_ok():
    assert run("kafka-run-class.sh org.apache.kafka.tools.TopicCommand --list") == []


@pytest.mark.parametrize("p", R.KRAFT_PATHS)
def test_kraft_paths(p):
    assert ids(run(f"kafka-server-start.sh {p}")) == ["kraft-config-path"]
    assert ids(run(f"      - ./{p}:/opt/kafka/config/server.properties", "docker-compose.yml")) == ["kraft-config-path"]


def test_new_paths_are_clean():
    assert run("kafka-server-start.sh config/server.properties config/broker.properties") == []


# ---------------------------------------------------------------- broker properties
def test_zookeeper_mode_file():
    f = run("broker.id=1\nlog.dirs=/d\nzookeeper.connect=zk:2181\n", "config/server.properties")
    assert ids(f) == ["removed-broker-config", "zookeeper-mode"]
    assert [x.line for x in f] == [3, 3]


def test_kraft_file_is_clean():
    text = "process.roles=broker,controller\nnode.id=1\nlisteners=PLAINTEXT://:9092,CONTROLLER://:9093\ncontroller.listener.names=CONTROLLER\nlog.dirs=/d\n"
    assert run(text, "server.properties") == []


def test_broker_id_alone_is_not_flagged():
    assert run("process.roles=broker\nbroker.id=1\n", "server.properties") == []


@pytest.mark.parametrize("key", [k for k in R.REMOVED_BROKER])
def test_removed_broker_keys_in_broker_file(key):
    (f,) = run(f"process.roles=broker\n{key}=x\n", "broker.properties")
    assert f.rule == "removed-broker-config" and f.line == 2 and key in f.message


@pytest.mark.parametrize("key", sorted(R.BROKER_ONLY))
def test_ambiguous_keys_only_in_broker_files(key):
    assert run(f"{key}=x\n", "app.properties") == []
    assert run(f"{key}=x\n", "client.properties") == []


def test_zookeeper_keys_only_in_broker_files():
    # study: old ZooKeeper consumers and client apps use zookeeper.* keys in files that are no broker config
    assert run("zookeeper.session.timeout.ms=18000\n", "app.properties") == []
    assert run("zookeeper.connect=zk:2181\ngroup.id=g\n", "consumer.properties") == []
    assert run("zookeeper.connect=zk:2181\nzookeeper.connection.timeout.ms=6000\n", "MsgRtrApi.properties") == []
    assert ids(run("zookeeper.connect=zk:2181\nzookeeper.connection.timeout.ms=6000\n", "server.properties")) == ["removed-broker-config", "removed-broker-config", "zookeeper-mode"]
    assert "zookeeper-mode" in ids(run("broker.id=0\nzookeeper.connect=zk:2181\n", "kafka.properties"))
    assert "zookeeper-mode" not in ids(run("broker.id=0\nzookeeper.connect=zk:2181\n", "consumer.properties"))
    assert ids(run("process.roles=broker\nzookeeper.ssl.truststore.type=null\n", "server.properties")) == ["removed-broker-config"]


def test_vendored_wrapper_scripts_are_skipped():
    wrapper = 'exec $(dirname $0)/kafka-run-class.sh kafka.admin.FeatureCommand "$@"\n'
    assert run(wrapper, "kafka-features.sh") == []
    assert ids(run(wrapper, "upgrade.sh")) == ["removed-tool-class"]


def test_copy_and_mount_are_not_uses():
    assert run("COPY scripts/zookeeper-server-stop.sh /opt/kafka/bin\n", "Dockerfile") == []
    assert ids(run("RUN /opt/kafka/bin/zookeeper-server-start.sh config/zookeeper.properties\n", "Dockerfile")) == ["zookeeper-script"]
    assert run("COPY server.properties /opt/kafka/config/kraft/server.properties\n", "Dockerfile") == []
    assert run("    volumes:\n      - ./server.properties:/kafka/config/kraft/server.properties\n", "docker-compose.yml") == []
    assert ids(run("COPY --from=k /opt/kafka/config/kraft/server.properties /conf/\n", "Dockerfile")) == ["kraft-config-path"]
    assert ids(run("bin/kafka-server-start.sh config/kraft/server.properties\n")) == ["kraft-config-path"]


def test_kubernetes_env_name_form():
    y = "env:\n  - name: KAFKA_ZOOKEEPER_CONNECT\n    value: zk:2181\n"
    assert ids(run(y, "kafka.yaml")) == ["removed-broker-config"]
    assert ids(run('{"name": "KAFKA_CFG_ZOOKEEPER_CONNECT",\n', "x.yaml")) == ["removed-broker-config"]


def test_more_removed_options_and_scripts():
    assert ids(run("kafka-console-consumer.sh --zookeeper zk:2181 --topic t\n")) == ["removed-cli-option"]
    assert ids(run("kafka-console-consumer.sh --bootstrap-server b:9092 --new-consumer --topic t\n")) == ["removed-cli-option"]
    assert ids(run("kafka-console-producer.sh --broker-list b:9092 --topic t\n")) == ["removed-cli-option"]
    assert ids(run("kafka-consumer-perf-test.sh --broker-list b:9092 --topic t\n")) == ["removed-cli-option"]
    assert ids(run("bin/kafka-mirror-maker.sh --consumer.config c --producer.config p --whitelist '.*'\n")) == ["removed-tool-script"]
    assert ids(run("kafka-preferred-replica-election.sh --zookeeper zk:2181\n")) == ["removed-tool-script"]
    assert ids(run("kafka-run-class.sh kafka.tools.MirrorMaker --whitelist x\n")) == ["removed-tool-class"]


def test_properties_comments_and_colon_and_spaces():
    assert run("# zookeeper.connect=zk\n! log.message.format.version=3\n", "server.properties") == []
    assert ids(run("process.roles=broker\nlog.message.format.version : 3.0\n", "server.properties")) == ["removed-broker-config"]
    assert ids(run("process.roles=broker\n  inter.broker.protocol.version = 3.9\n", "server.properties")) == ["removed-broker-config"]


def test_invalid_values():
    base = "process.roles=broker\n"
    for key in R.INVALID_MIN:
        (f,) = run(f"{base}{key}=-1\n", "server.properties")
        assert f.rule == "invalid-config-value" and f.severity == "error"
        assert run(f"{base}{key}=1\n", "server.properties") == []
        assert run(f"{base}{key}=${{POOL}}\n", "server.properties") == []


def test_deprecated_broker_configs():
    base = "process.roles=broker\n"
    assert ids(run(base + "log.cleaner.enable=false\n", "server.properties")) == ["deprecated-broker-config"]
    assert run(base + "log.cleaner.enable=true\n", "server.properties") == []
    assert ids(run(base + "group.coordinator.rebalance.protocols=classic,consumer\n", "server.properties")) == ["deprecated-broker-config"]


# ---------------------------------------------------------------- client properties
@pytest.mark.parametrize("cls", R.REMOVED_PARTITIONERS)
def test_removed_partitioners(cls):
    assert ids(run(f"partitioner.class={cls}\n", "producer.properties")) == ["removed-partitioner"]


def test_custom_partitioner_ok():
    assert run("partitioner.class=com.example.MyPartitioner\n", "producer.properties") == []


def test_idempotence_in_flight():
    assert ids(run("enable.idempotence=true\nmax.in.flight.requests.per.connection=6\n", "p.properties")) == ["idempotence-in-flight"]
    assert run("enable.idempotence=true\nmax.in.flight.requests.per.connection=5\n", "p.properties") == []
    assert run("enable.idempotence=false\nmax.in.flight.requests.per.connection=6\n", "p.properties") == []
    assert run("max.in.flight.requests.per.connection=1\n", "p.properties") == []


def test_idempotence_default_on_is_flagged_too():
    (f,) = run("bootstrap.servers=b:9092\nmax.in.flight.requests.per.connection=10\n", "producer.properties")
    assert f.rule == "idempotence-in-flight" and "the default" in f.message and f.line == 2


# ---------------------------------------------------------------- Connect
@pytest.mark.parametrize("key", list(R.REMOVED_CONNECTOR))
def test_removed_connector_keys_in_properties_json_and_yaml(key):
    assert ids(run(f"name=m\n{key}=x\n", "mm2.properties")) == ["removed-connector-config"]
    assert ids(run(f'{{\n  "{key}": "x"\n}}\n', "mirror.json")) == ["removed-connector-config"]
    assert ids(run(f"config:\n  {key}: x\n", "connector.yaml")) == ["removed-connector-config"]


def test_connector_replacements_are_clean():
    assert run("topics.exclude=a\ngroups.exclude=b\nconfig.properties.exclude=c\n", "mm2.properties") == []


def test_replacefield_needs_the_transform_in_the_file():
    text = "transforms=drop\ntransforms.drop.type=org.apache.kafka.connect.transforms.ReplaceField$Value\ntransforms.drop.blacklist=ssn\n"
    (f,) = run(text, "sink.properties")
    assert f.rule == "removed-connector-config" and "transforms.drop.exclude" in f.message and f.line == 3
    assert run("transforms.drop.whitelist=a\n", "other.properties") == []
    assert run(text.replace("blacklist", "exclude"), "sink.properties") == []


def test_connector_key_in_comment_is_ignored():
    assert run("# topics.blacklist=a\n", "mm2.properties") == []


# ---------------------------------------------------------------- environment variables (image convention)
def test_env_compose_and_k8s_and_dockerfile():
    f = run("""
        services:
          k:
            environment:
              KAFKA_ZOOKEEPER_CONNECT: zk:2181
              - KAFKA_CFG_LOG_MESSAGE_FORMAT_VERSION=3.0
              KAFKA_INTER_BROKER_PROTOCOL_VERSION: "3.9"
              KAFKA_ZOOKEEPER_SSL_CLIENT_ENABLE: "true"
    """, "docker-compose.yml")
    assert [x.rule for x in f] == ["removed-broker-config"] * 4
    assert ids(run("ENV KAFKA_ZOOKEEPER_CONNECT=zk:2181", "Dockerfile")) == ["removed-broker-config"]


def test_env_ambiguous_and_kraft_vars_are_clean():
    assert run("""
        environment:
          KAFKA_PORT: 9092
          KAFKA_HOST_NAME: x
          KAFKA_NODE_ID: 1
          KAFKA_PROCESS_ROLES: broker,controller
          KAFKA_CFG_LISTENERS: PLAINTEXT://:9092
    """, "compose.yaml") == []


def test_yaml_embedded_properties_block():
    f = run("data:\n  server.properties: |\n    zookeeper.connect=zk:2181\n    log.message.format.version=3.0\n    port=9092\n", "cm.yaml")
    assert [x.line for x in f] == [3, 4]


# ---------------------------------------------------------------- suppression and discovery
def test_ignore_comment_same_and_previous_line():
    assert run("kafka-topics.sh --zookeeper zk --list  # kafka4-ready: ignore\n") == []
    assert run("# kafka4-ready: ignore removed-cli-option\nkafka-topics.sh --zookeeper zk --list\n") == []
    assert ids(run("# kafka4-ready: ignore bootstrap-server-format\nkafka-topics.sh --zookeeper zk --list\n")) == ["removed-cli-option"]
    assert run("process.roles=broker\n# kafka4-ready: ignore\nlog.message.format.version=3.0\n", "server.properties") == []


def test_comments_are_not_scanned():
    assert run("# kafka-topics.sh --zookeeper zk:2181 --list\n") == []


def test_kinds():
    assert kind_of("server.properties") == "properties" and kind_of("server.properties.j2") == "properties"
    assert kind_of("a.yml") == "yaml" and kind_of("Dockerfile") == "dockerfile" and kind_of("Makefile") == "makefile" and kind_of("README.md") is None


# ---------------------------------------------------------------- registry consistency and CLI
def test_every_rule_has_a_detector_test_and_oracle_flag():
    assert set(RULES) == {"zookeeper-mode", "removed-broker-config", "invalid-config-value", "deprecated-broker-config", "removed-cli-option", "deprecated-cli-option",
                          "bootstrap-server-format", "removed-tool-class", "zookeeper-script", "kraft-config-path", "removed-partitioner", "idempotence-in-flight", "removed-connector-config", "removed-tool-script"}
    assert all(r.oracle for r in RULES.values()) and TESTED == sorted(TESTED, key=lambda v: [int(x) for x in v.split(".")])


def test_cli_formats_and_exit_codes(tmp_path, capsys):
    (tmp_path / "run.sh").write_text("kafka-topics.sh --zookeeper zk:2181 --list\nkafka-console-producer.sh --producer.config p\n")
    assert main([str(tmp_path)]) == 1
    assert "removed-cli-option" in capsys.readouterr().out
    assert main([str(tmp_path), "--fail-on", "never"]) == 0
    assert main([str(tmp_path), "--only", "deprecated-cli-option"]) == 0
    capsys.readouterr()
    assert main([str(tmp_path), "-f", "json", "--fail-on", "never"]) == 0
    d = json.loads(capsys.readouterr().out)
    assert d["summary"] == {"error": 1, "warning": 0, "note": 1} and d["testedAgainst"] == TESTED
    assert main([str(tmp_path), "-f", "sarif", "--fail-on", "never"]) == 0
    s = json.loads(capsys.readouterr().out)
    assert s["version"] == "2.1.0" and len(s["runs"][0]["tool"]["driver"]["rules"]) == len(RULES) and len(s["runs"][0]["results"]) == 2
    assert main([str(tmp_path), "-f", "github", "--fail-on", "never"]) == 0
    assert "::error file=run.sh,line=1" in capsys.readouterr().out
    assert main([str(tmp_path), "--disable", "nope"]) == 2


def test_list_rules(capsys):
    assert main(["--list-rules"]) == 0
    assert len(capsys.readouterr().out.strip().splitlines()) == len(RULES)


def test_commands_inside_quoted_strings():
    assert ids(run('command: "bin/zookeeper-server-start.sh config/zookeeper.properties"\n', "docker-compose.yml")) == ["zookeeper-script"]
    assert ids(run("command: \"bash -c 'kafka-topics --create --zookeeper zk:2181 --topic t'\"\n", "docker-compose.yml")) == ["removed-cli-option"]
    assert ids(run('CMD ["/bin/bash","-c","/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties"]\n', "Dockerfile")) == ["zookeeper-script"]
    assert run('echo "the kafka docs say hello there"\n') == []
    f = run("x: \"bash -c 'kafka-topics --zookeeper zk:2181'\"\n", "c.yml")[0]
    assert (f.line, f.col) == (1, 27)
