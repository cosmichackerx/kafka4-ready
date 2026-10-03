#!/usr/bin/env python3
"""Oracle for `kafka4-ready --fix`: apply the fixer to a command, then run the BEFORE and the AFTER command on real Kafka tools.

    python tests/oracle/run_fix_oracle.py --kafka /path/to/kafka_2.13-4.3.1 [--extra-kafka DIR ...]
    python tests/oracle/run_fix_oracle.py --count        # number of cases, needs no Kafka

A case passes when the original command is rejected by the tool (so the case is meaningful), the fixed command is accepted (no "not a recognized option",
no "Failed to create new KafkaAdminClient", no ConfigException) and the fixer leaves the fixed command unchanged (idempotent). The `config/kraft` rewrite is
checked by the fixed path existing in the distribution and the old one not. One broker is started per release (heap 300 MB).
"""
from __future__ import annotations

import argparse
import os
import re
import shlex
import sys

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
from run_oracle import B, CID, REJECT, Kafka, parse_version  # noqa: E402

from kafka4_ready.fix import apply, plan  # noqa: E402
from kafka4_ready.rules import KRAFT_PATHS  # noqa: E402

BAD = re.compile(REJECT.pattern + r"|Failed to create new KafkaAdminClient|ConfigException|Invalid url", re.I)
# (name, command before the fix, stdin); the fixed command must also print EXPECT[name] when listed there (proof that it did its job, not only that it parsed)
EXPECT = {"kafka-topics --bootstrap-server spaces -> commas": "t1", "bash -c \"kafka-topics ... spaces\" (quoted command)": "t1",
          "console-consumer --whitelist -> --include": "Processed a total of", "console-consumer --new-consumer removed": "Processed a total of",
          "console-consumer --bootstrap-server spaces -> commas": "Processed a total of", "kafka-configs --bootstrap-server spaces -> commas": "Dynamic configs for broker"}
CASES = [
    ("console-consumer --whitelist -> --include", "kafka-console-consumer.sh --bootstrap-server localhost:9092 --whitelist 't1.*' --timeout-ms 3000 --max-messages 1", ""),
    ("console-consumer --new-consumer removed", "kafka-console-consumer.sh --bootstrap-server localhost:9092 --new-consumer --topic t1 --timeout-ms 3000 --max-messages 1", ""),
    ("console-producer --broker-list -> --bootstrap-server", "kafka-console-producer.sh --broker-list localhost:9092 --topic t1", "a\n"),
    ("consumer-perf-test --broker-list -> --bootstrap-server", "kafka-consumer-perf-test.sh --broker-list localhost:9092 --topic t1 --messages 1 --timeout 3000", ""),
    ("verifiable-consumer --broker-list -> --bootstrap-server", "kafka-verifiable-consumer.sh --broker-list localhost:9092 --topic t1 --group-id g --max-messages 1", ""),
    ("replica-verification --topic-white-list -> --topics-include", "kafka-replica-verification.sh --broker-list localhost:9092 --topic-white-list 't1.*'", ""),
    ("kafka-topics --bootstrap-server spaces -> commas", "kafka-topics.sh --bootstrap-server \"localhost:9092 localhost:9099\" --list", ""),
    ("kafka-configs --bootstrap-server spaces -> commas", "kafka-configs.sh --bootstrap-server 'localhost:9092 localhost:9099' --describe --entity-type brokers", ""),
    ("console-consumer --bootstrap-server spaces -> commas", "kafka-console-consumer.sh --bootstrap-server=\"localhost:9092 localhost:9099\" --topic t1 --timeout-ms 3000 --max-messages 1", ""),
    ("bash -c \"kafka-topics ... spaces\" (quoted command)", "bash -c \"kafka-topics.sh --bootstrap-server 'localhost:9092 localhost:9099' --list\"", ""),
] + [(f"{p} -> config/{p.rsplit('/', 1)[1]}", f"kafka-server-start.sh {p}", "") for p in KRAFT_PATHS]


def fixed(cmd: str) -> str:
    edits, _ = plan(cmd + "\n", "ci.sh")
    out = apply(cmd + "\n", edits).strip()
    again, _ = plan(out + "\n", "ci.sh")
    assert not again, ("not idempotent", cmd, out)
    return out


def run_cmd(k: Kafka, cmd: str, stdin: str):
    argv = shlex.split(cmd)
    if argv[0] == "bash":      # bash -c "<tool> ...": resolve the tool inside
        inner = shlex.split(argv[2])
        inner[0] = k.bin(inner[0].replace(".sh", ""))
        argv[2] = shlex.join(inner)
    else:
        argv[0] = k.bin(argv[0].replace(".sh", ""))
    return k.run(argv, stdin=stdin, t=30)


def run_release(home: str):
    k = Kafka(home)
    res = {}
    try:
        cfg = k.config({})
        rc, out = k.format(cfg)
        if rc != 0 or not k.start(cfg):
            return {name: (False, "broker did not start") for name, _, _ in CASES}
        k.run([k.bin("kafka-topics"), "--bootstrap-server", B, "--create", "--topic", "t1"])
        for name, before, stdin in CASES:
            after = fixed(before)
            if before.startswith("kafka-server-start.sh"):
                old, new = before.split()[1], after.split()[1]
                res[name] = (not os.path.exists(f"{home}/{old}") and os.path.exists(f"{home}/{new}") and old != new, f"{old}: missing; {new}: {'exists' if os.path.exists(home + '/' + new) else 'missing'}")
                continue
            _, ob = run_cmd(k, before, stdin)
            _, oa = run_cmd(k, after, stdin)
            rejected = BAD.search(ob) is not None
            accepted = BAD.search(oa) is None and EXPECT.get(name, "") in oa
            res[name] = (rejected and accepted, f"before: {'rejected' if rejected else 'NOT rejected'}; after: {'accepted' if accepted else ('REJECTED: ' + BAD.search(oa).group(0)) if BAD.search(oa) else 'expected output ' + repr(EXPECT.get(name)) + ' missing: ' + oa.strip()[-160:]}")
    finally:
        k.close()
    return res


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--kafka")
    ap.add_argument("--extra-kafka", action="append", default=[])
    ap.add_argument("--count", action="store_true")
    a = ap.parse_args()
    if a.count:
        print(f"{len(CASES)} fix cases")
        return 0
    if not a.kafka:
        ap.error("--kafka is required (or --count)")
    bad = 0
    for h in [a.kafka] + a.extra_kafka:
        v = ".".join(map(str, parse_version(h) or ("?",)))
        res = run_release(h)
        for name, (ok, why) in res.items():
            print(f"{'ok  ' if ok else 'FAIL'} [{v}] {name}: {why}")
            if not ok:
                bad += 1
    print(f"{len(CASES)} fix cases, {bad} failure(s)")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
