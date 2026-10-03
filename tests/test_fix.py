import os
import textwrap

import pytest

from kafka4_ready.cli import main
from kafka4_ready.fix import RENAMES, apply, fix_path, plan
from kafka4_ready.scan import scan_text

CASES = [
    ("kafka-console-consumer.sh --bootstrap-server b:9092 --whitelist 't.*'", "kafka-console-consumer.sh --bootstrap-server b:9092 --include 't.*'"),
    ("kafka-console-consumer --bootstrap-server b:9092 --whitelist=t.*", "kafka-console-consumer --bootstrap-server b:9092 --include=t.*"),
    ("kafka-replica-verification.sh --broker-list b:9092 --topic-white-list x", "kafka-replica-verification.sh --broker-list b:9092 --topics-include x"),
    ("kafka-console-producer.sh --broker-list b:9092 --topic t", "kafka-console-producer.sh --bootstrap-server b:9092 --topic t"),
    ("kafka-consumer-perf-test.sh --broker-list b:9092 --topic t", "kafka-consumer-perf-test.sh --bootstrap-server b:9092 --topic t"),
    ("kafka-verifiable-consumer.sh --broker-list b:9092 --topic t --group-id g", "kafka-verifiable-consumer.sh --bootstrap-server b:9092 --topic t --group-id g"),
    ("kafka-console-consumer.sh --bootstrap-server b:9092 --new-consumer --topic t", "kafka-console-consumer.sh --bootstrap-server b:9092 --topic t"),
    ('kafka-topics.sh --bootstrap-server "a:9092 b:9092" --list', 'kafka-topics.sh --bootstrap-server "a:9092,b:9092" --list'),
    ("kafka-configs.sh --bootstrap-server='a:9092  b:9092  c:9092' --describe", "kafka-configs.sh --bootstrap-server='a:9092,b:9092,c:9092' --describe"),
    ('bash -c "kafka-topics.sh --bootstrap-server \'a:1 b:2\' --list"', 'bash -c "kafka-topics.sh --bootstrap-server \'a:1,b:2\' --list"'),
    ("bin/kafka-server-start.sh config/kraft/server.properties", "bin/kafka-server-start.sh config/server.properties"),
    ("kafka-storage.sh format -t $ID -c /opt/kafka/config/kraft/controller.properties", "kafka-storage.sh format -t $ID -c /opt/kafka/config/controller.properties"),
]


def fixed(text, name="ci.sh"):
    e, s = plan(text, name)
    return apply(text, e), e, s


@pytest.mark.parametrize("before,after", CASES)
def test_rewrite_idempotent_and_finding_gone(before, after):
    out, edits, _ = fixed(before + "\n")
    assert out == after + "\n"
    assert plan(out, "ci.sh")[0] == []                        # idempotent
    assert apply(out, plan(out, "ci.sh")[0]) == out
    gone = {e.rule for e in edits}
    left = {f.rule for f in scan_text(out, "ci.sh", "shell")[0]}
    assert not (gone & left), (gone, left)                    # what was fixed is no longer reported
    assert {f.rule for f in scan_text(before + "\n", "ci.sh", "shell")[0]} >= gone


def test_every_rename_has_a_case():
    covered = {c[0].split()[0].split("/")[-1].replace(".sh", "") for c in CASES}
    assert {t for t, _ in RENAMES} <= covered


def test_not_mechanical_is_left_alone():
    for line in ["kafka-topics.sh --zookeeper zk:2181 --list", "kafka-acls.sh --authorizer kafka.security.authorizer.AclAuthorizer",
                 "zookeeper-server-start.sh config/zookeeper.properties", "kafka-run-class.sh kafka.tools.JmxTool --help",
                 'kafka-topics.sh --bootstrap-server "$BROKERS" --list', 'kafka-topics.sh --bootstrap-server "a:9092,b:9092" --list',
                 "kafka-console-producer.sh --producer.config p.properties --bootstrap-server b:9092"]:
        out, edits, _ = fixed(line + "\n")
        assert out == line + "\n" and edits == [], line


def test_only_the_option_changes_crlf_comments_indentation():
    text = "  kafka-console-consumer.sh --bootstrap-server b:9092 --whitelist x  # keep\r\n\tkafka-topics.sh --list\r\n"
    out, _, _ = fixed(text)
    assert out == "  kafka-console-consumer.sh --bootstrap-server b:9092 --include x  # keep\r\n\tkafka-topics.sh --list\r\n"


def test_comment_prose_copy_and_mount_are_not_touched():
    for text, name in [("# kafka-console-consumer.sh --whitelist x\n", "ci.sh"),
                       ("COPY server.properties /opt/kafka/config/kraft/server.properties\n", "Dockerfile"),
                       ("    volumes:\n      - ./s.properties:/kafka/config/kraft/server.properties\n", "docker-compose.yml")]:
        assert fixed(text, name)[1] == [], text


def test_suppression_comment_is_respected():
    text = "# kafka4-ready: ignore removed-cli-option\nkafka-console-producer.sh --broker-list b:9092 --topic t\n"
    assert fixed(text)[1] == []


def test_option_alone_on_its_line_is_skipped_not_deleted():
    text = "kafka-console-consumer.sh --bootstrap-server b:9092 \\\n  --new-consumer \\\n  --topic t\n"
    out, edits, skips = fixed(text)
    assert out == text and edits == [] and len(skips) == 1


def test_distribution_wrapper_is_skipped():
    text = 'exec $(dirname $0)/kafka-run-class.sh kafka.tools.MirrorMaker --whitelist x "$@"\n'
    assert fixed(text, "kafka-mirror-maker.sh")[1] == []


def test_dollar_and_multiline_values_are_skipped():
    out, edits, skips = fixed('kafka-topics.sh --bootstrap-server "$A:9092 $B:9092" --list\n')
    assert edits == []


def test_diff_and_fix_cli(tmp_path, capsys):
    f = tmp_path / "run.sh"
    f.write_text("kafka-console-producer.sh --broker-list b:9092 --topic t\n")
    assert main([str(tmp_path), "--diff"]) == 1
    cap = capsys.readouterr()
    assert "-kafka-console-producer.sh --broker-list" in cap.out and "+kafka-console-producer.sh --bootstrap-server" in cap.out
    assert "--broker-list" in f.read_text()                   # --diff writes nothing
    assert main([str(tmp_path), "--fix"]) == 0
    assert f.read_text() == "kafka-console-producer.sh --bootstrap-server b:9092 --topic t\n"
    assert main([str(tmp_path), "--diff"]) == 0               # idempotent: nothing left to change
    assert main([str(tmp_path), "--fix", "--diff"]) == 2
    assert main([str(tmp_path), "--fix", "--base", "HEAD"]) == 2


def test_properties_and_other_kinds_untouched(tmp_path):
    (tmp_path / "server.properties").write_text("zookeeper.connect=zk:2181\nlog.dirs=/x\n")
    (tmp_path / "notes.md").write_text("kafka-console-producer.sh --broker-list b:9092\n")
    edits, skips, diffs, changed = fix_path(str(tmp_path), write=True)
    assert edits == [] and changed == 0
    assert (tmp_path / "notes.md").read_text().startswith("kafka-console-producer.sh --broker-list")


def test_disable_and_only(tmp_path):
    f = tmp_path / "a.sh"
    f.write_text("kafka-console-producer.sh --broker-list b:9092 --topic t\nbin/kafka-server-start.sh config/kraft/server.properties\n")
    e, *_ = fix_path(str(tmp_path), only=["kraft-config-path"])
    assert [x.rule for x in e] == ["kraft-config-path"]
    e, *_ = fix_path(str(tmp_path), disable=["kraft-config-path"])
    assert [x.rule for x in e] == ["removed-cli-option"]
