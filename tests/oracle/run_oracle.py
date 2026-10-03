#!/usr/bin/env python3
"""Oracle: check every claim of rules.py against real Kafka binaries (no mocks).

    python tests/oracle/run_oracle.py --kafka /path/to/kafka_2.13-4.3.1 [--extra-kafka /path/to/other ...] [--json out.json] [--markdown out.md]
    python tests/oracle/run_oracle.py --count        # number of cases, needs no Kafka (used by claims-check)

For each Kafka directory it formats and starts ONE real KRaft broker with all "removed" settings added and asks the broker what it knows
(`kafka-configs --describe --all`), runs format-only cases that must be rejected, runs the CLI tools with the removed and deprecated options,
and runs a console producer with client settings. A case passes when the observed behaviour equals the claim. Exit code 1 if any claim fails
on the primary (--kafka) release; on --extra-kafka releases a different outcome is reported in the matrix and counted as "differs".
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
from kafka4_ready import rules as R  # noqa: E402

B = "localhost:9092"
CID = "MkVLQ0hSQ0w1cE1PZmVpTw"
REJECT = re.compile(r"is not a recognized option|unrecognized arguments|no longer supported|Unrecognized option", re.I)

# arguments that make a tool parse its command line so that the option under test is the only problem
BASE = {
    "kafka-topics": ["--bootstrap-server", B, "--list"], "kafka-configs": ["--bootstrap-server", B, "--describe", "--entity-type", "brokers"],
    "kafka-reassign-partitions": ["--bootstrap-server", B, "--list"], "kafka-consumer-groups": ["--bootstrap-server", B, "--list"],
    "kafka-acls": ["--bootstrap-server", B, "--list"], "kafka-console-consumer": ["--bootstrap-server", B, "--timeout-ms", "3000"],
    "kafka-replica-verification": ["--broker-list", B], "kafka-verifiable-consumer": ["--bootstrap-server", B, "--topic", "t", "--group-id", "g"],
    "kafka-console-producer": ["--bootstrap-server", B, "--topic", "t1"], "kafka-producer-perf-test": ["--bootstrap-server", B, "--topic", "t1", "--num-records", "1", "--record-size", "10", "--throughput", "-1"],
    "kafka-consumer-perf-test": ["--bootstrap-server", B, "--topic", "t1", "--timeout", "3000"],
}
OPT_VALUE = {"--zookeeper": "localhost:2181", "--authorizer": "kafka.security.authorizer.AclAuthorizer", "--authorizer-properties": "zookeeper.connect=localhost:2181",
             "--zk-tls-config-file": "/tmp/none", "--whitelist": "x.*", "--topic-white-list": "x.*", "--broker-list": B, "--delete-config": "x",
             "--messages": "1", "--producer-props": "linger.ms=1", "--consumer-property": "fetch.min.bytes=1", "--producer-property": "linger.ms=1"}


def parse_version(d: str):
    m = re.search(r"kafka_[\d.]+-(\d+)\.(\d+)\.(\d+)", d)
    return tuple(int(x) for x in m.groups()) if m else None


# since: release from which the oracle expects the behaviour (default 4.0); older releases must show the opposite
SINCE = {"deprecated-broker-config:log.cleaner.enable": (4, 1), "deprecated-broker-config:group.coordinator.rebalance.protocols": (4, 3)}


def cases():
    out = [("zookeeper-mode", "format-no-process-roles", "format"), ("invalid-config-value", "format-copier-pool", "format"),
           ("invalid-config-value", "format-expiration-pool", "format")]
    out += [("removed-broker-config", k, "describe") for k in R.REMOVED_BROKER]
    out += [("deprecated-broker-config", k, "deprecated") for k in R.DEPRECATED_BROKER]
    out += [("removed-cli-option", f"{t} {o}", "cli-removed") for t, o, _ in R.REMOVED_CLI]
    out += [("deprecated-cli-option", f"{t} {o}", "cli-deprecated") for t, o, _ in R.DEPRECATED_CLI]
    out += [("bootstrap-server-format", t, "bootstrap") for t in R.BOOTSTRAP_TOOLS]
    out += [("removed-tool-class", c, "class") for c in R.REMOVED_CLASSES]
    out += [("zookeeper-script", s, "script") for s in R.ZOOKEEPER_SCRIPTS]
    out += [("removed-tool-script", s, "script") for s in R.REMOVED_SCRIPTS]
    out += [("removed-broker-config", IMAGE_CASE, "image")]
    out += [("kraft-config-path", p, "kraft-path") for p in R.KRAFT_PATHS]
    out += [("removed-partitioner", c.rsplit(".", 1)[1], "client") for c in R.REMOVED_PARTITIONERS]
    out += [("idempotence-in-flight", "idempotence+6", "client"), ("idempotence-in-flight", "default+6", "client"), ("idempotence-in-flight", "idempotence off+6 (control)", "client")]
    out += [("removed-connector-config", k, "connect") for k in R.REMOVED_CONNECTOR]
    out += [("removed-connector-config", f"ReplaceField {k}", "connect") for k in R.REPLACEFIELD_KEYS]
    return out


IMAGE_CASE = "apache/kafka image: KAFKA_* variable names -> settings"


def var_for(key: str) -> str:
    """The environment variable the apache/kafka image documents for a setting: `.` -> `_`, `_` -> `__`, `-` -> `___`, upper case, KAFKA_ prefix."""
    return "KAFKA_" + key.replace("_", "__").replace("-", "___").replace(".", "_").upper()


def image_env_case(k):
    """The docker image of Apache Kafka turns KAFKA_* variables into server.properties with kafka.docker.KafkaDockerWrapper, which ships in the tarball's kafka jar.
    Run that very class (no container needed) with a variable for every setting of the rules, and check that kafka4-ready maps the same names."""
    from kafka4_ready.scan import env_to_key
    d = tempfile.mkdtemp(prefix="k4img-", dir=k.tmp)
    for sub in ("default", "mounted", "final"):
        os.makedirs(f"{d}/{sub}")
    open(f"{d}/default/server.properties", "w").close()
    env = dict(k.env, CLUSTER_ID=CID, KAFKA_NODE_ID="1", KAFKA_PROCESS_ROLES="broker,controller", KAFKA_LISTENERS="PLAINTEXT://:9092,CONTROLLER://:9093",
               KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://localhost:9092", KAFKA_CONTROLLER_LISTENER_NAMES="CONTROLLER",
               KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT", KAFKA_CONTROLLER_QUORUM_VOTERS="1@localhost:9093",
               KAFKA_LOG_DIRS=f"{d}/data", KAFKA_HEAP_OPTS="-Xmx128m")
    extra = {"KAFKA_GROUP__INITIAL__REBALANCE__DELAY__MS": "group_initial_rebalance_delay_ms", "KAFKA_LOG___SEGMENT___BYTES": "log-segment-bytes"}   # the `__` and `___` escapes
    for key in R.REMOVED_BROKER:
        env[var_for(key)] = "x"
    env.update({v: "1" for v in extra})
    try:
        p = subprocess.run([f"{k.home}/bin/kafka-run-class.sh", "kafka.docker.KafkaDockerWrapper", "setup", "--default-configs-dir", f"{d}/default",
                            "--mounted-configs-dir", f"{d}/mounted", "--final-configs-dir", f"{d}/final"], capture_output=True, text=True, timeout=120, env=env)
        out = p.stderr + p.stdout
    except subprocess.TimeoutExpired:
        return {("removed-broker-config", IMAGE_CASE): (False, "wrapper timed out")}
    try:
        got = {l.split("=", 1)[0] for l in open(f"{d}/final/server.properties") if "=" in l}
    except OSError:
        return {("removed-broker-config", IMAGE_CASE): (False, "wrapper wrote no server.properties: " + first(out, "."))}
    bad = [key for key in R.REMOVED_BROKER if key.lower() not in got or env_to_key(var_for(key)) != key.lower() or env_to_key("KAFKA_CFG_" + var_for(key)[6:]) != key.lower()]
    bad += [v for v, want in extra.items() if want not in got or env_to_key(v) != want]
    return {("removed-broker-config", IMAGE_CASE): (not bad, f"{len(R.REMOVED_BROKER)} variables became settings with the same names kafka4-ready maps" if not bad else "mismatch: " + ", ".join(bad))}


class Kafka:
    def __init__(self, home: str):
        self.home, self.ver = home, parse_version(home)
        self.tmp = tempfile.mkdtemp(prefix="k4o-")
        self.env = dict(os.environ, KAFKA_HEAP_OPTS="-Xmx300m -Xms128m", LOG_DIR=self.tmp + "/logs", KAFKA_LOG4J_OPTS="")
        self.proc = None
        self.log: list = []

    def bin(self, name):
        return f"{self.home}/bin/{name}.sh"

    def run(self, args, stdin="", t=90):
        rc, out = self._run(args, stdin, t)
        if rc is None and not out.strip():      # a JVM that never got going on a loaded machine: one more try
            rc, out = self._run(args, stdin, t)
        return rc, out

    def _run(self, args, stdin, t):
        try:
            p = subprocess.run(args, capture_output=True, text=True, timeout=t, env=dict(self.env, KAFKA_HEAP_OPTS="-Xmx128m"), input=stdin)
            return p.returncode, p.stderr + p.stdout
        except subprocess.TimeoutExpired as e:
            def s(x):
                return x.decode(errors="replace") if isinstance(x, bytes) else (x or "")
            return None, s(e.stderr) + s(e.stdout)

    def config(self, edits: dict, name="server.properties") -> str:
        base, order = {}, []
        for line in open(f"{self.home}/config/server.properties"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                base[k] = v
        base["log.dirs"] = self.tmp + "/data-" + name
        for k, v in edits.items():
            if v is None:
                base.pop(k, None)
            else:
                base[k] = v
        path = f"{self.tmp}/{name}"
        with open(path, "w") as fh:
            fh.write("\n".join(f"{k}={v}" for k, v in base.items()) + "\n")
        return path

    def format(self, cfg):
        return self.run([self.bin("kafka-storage"), "format", "-t", CID, "-c", cfg, "--standalone"])

    def start(self, cfg) -> bool:
        self.proc = subprocess.Popen([self.bin("kafka-server-start"), cfg], env=self.env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True)
        threading.Thread(target=lambda: [self.log.append(x) for x in self.proc.stdout], daemon=True).start()
        t0 = time.time()
        while time.time() - t0 < 90:
            if "Kafka Server started" in "".join(self.log):
                time.sleep(1)
                return True
            if self.proc.poll() is not None:
                return False
            time.sleep(0.5)
        return False

    def stop(self):
        if self.proc and self.proc.poll() is None:
            try:
                os.killpg(self.proc.pid, signal.SIGKILL)
            except Exception:
                pass
            self.proc.wait()

    def close(self):
        self.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)


def run_release(home: str):
    """Return {(rule, case): (ok_as_claimed, observed_text)}"""
    k = Kafka(home)
    res = {}
    try:
        CFG = "--command-config" if k.ver is None or k.ver >= (4, 2) else "--producer.config"     # the console producer got --command-config in 4.2
        controls = {"controlled.shutdown.enable": "true", "replica.lag.time.max.ms": "30000"}
        edits = {key: "1" for key in R.REMOVED_BROKER}
        edits.update({"zookeeper.connect": "localhost:2181", "log.message.format.version": "3.0", "inter.broker.protocol.version": "3.9", "metrics.jmx.blacklist": ".*",
                      "metrics.jmx.whitelist": ".*", "auto.include.jmx.reporter": "false", "delegation.token.master.key": "secret", "host.name": "localhost", "advertised.host.name": "localhost",
                      "zookeeper.ssl.client.enable": "false", "zookeeper.ssl.protocol": "TLSv1.2", "zookeeper.ssl.endpoint.identification.algorithm": "HTTPS",
                      "zookeeper.ssl.keystore.location": "/tmp/k", "zookeeper.ssl.truststore.location": "/tmp/t", "zookeeper.clientCnxnSocket": "x", "zookeeper.set.acl": "false",
                      "zookeeper.metadata.migration.enable": "false", "broker.id.generation.enable": "false", "port": "9999", "advertised.port": "9999",
                      "log.cleaner.enable": "false", "group.coordinator.rebalance.protocols": "classic,consumer"})
        edits.update(controls)
        edits["offsets.commit.required.acks"] = "-1"
        edits["log.message.timestamp.difference.max.ms"] = "9223372036854775807"
        # --- format-only cases
        rc, out = k.format(k.config({"process.roles": None}, "noroles.properties"))
        res[("zookeeper-mode", "format-no-process-roles")] = (rc not in (0, None) and "process.roles" in out, first(out, "process.roles"))
        for key, name in (("remote.log.manager.copier.thread.pool.size", "format-copier-pool"), ("remote.log.manager.expiration.thread.pool.size", "format-expiration-pool")):
            rc, out = k.format(k.config({key: "-1"}, name + ".properties"))
            res[("invalid-config-value", name)] = (rc not in (0, None) and "Value must be at least 1" in out, first(out, "Value must be"))
        # --- one broker with everything removed
        cfg = k.config(edits, "main.properties")
        rc, out = k.format(cfg)
        started = rc == 0 and k.start(cfg)
        if not started:
            for key in R.REMOVED_BROKER:
                res[("removed-broker-config", key)] = (False, "broker did not start: " + first(out + "".join(k.log), "rror|xception"))
        else:
            rc, desc = k.run([k.bin("kafka-configs"), "--bootstrap-server", B, "--describe", "--all", "--entity-type", "brokers", "--entity-name", "1"])
            ctl_known = all(re.search(rf"^\s+{re.escape(c)}=\S+ sensitive=false", desc, re.M) for c in controls)
            for key in R.REMOVED_BROKER:
                ign = re.search(rf"^\s+{re.escape(key)}=null sensitive=true", desc, re.M) is not None
                res[("removed-broker-config", key)] = (ign and ctl_known, f"{key}=null sensitive=true in describe" if ign else f"describe does not show {key} as unknown")
            text = "".join(k.log)
            for key, (trig, _) in R.DEPRECATED_BROKER.items():
                m = re.search(rf"[^\n]*{re.escape(key)}[^\n]*deprecat[^\n]*|[^\n]*deprecat[^\n]*{re.escape(key)}[^\n]*", text)
                want = k.ver is None or k.ver >= SINCE.get(f"deprecated-broker-config:{key}", (4, 0))
                res[("deprecated-broker-config", key)] = ((m is not None) == want, (m.group(0).strip()[-150:] if m else "no deprecation line in the broker log"))
        # --- tools (broker is still running)
        for tool, opt, _ in R.REMOVED_CLI:
            args = [k.bin(tool)] + BASE[tool] + [opt] + ([OPT_VALUE[opt]] if opt in OPT_VALUE else [])
            if tool == "kafka-topics" and opt == "--delete-config":
                args = [k.bin(tool), "--bootstrap-server", B, "--alter", "--topic", "nope", "--delete-config", "x"]
            rc, out = k.run(args, t=90)
            _, ctl = k.run([k.bin(tool)] + BASE[tool], t=90) if tool != "kafka-topics" or opt != "--delete-config" else (0, "")
            hit = bool(REJECT.search(out)) and opt.lstrip("-") in out and not (REJECT.search(ctl) and opt.lstrip("-") in ctl)
            res[("removed-cli-option", f"{tool} {opt}")] = (hit, first(out, r"recognized|unrecognized|no longer"))
        for tool, opt, new in R.DEPRECATED_CLI:
            val = OPT_VALUE.get(opt, "client.properties")
            if opt.endswith(".config"):
                val = k.tmp + "/c.properties"
                open(val, "w").write("linger.ms=1\n" if "producer" in opt else "fetch.min.bytes=1\n")
            if opt == "--producer-props":
                val = "linger.ms=1"
            base = BASE[tool]
            if tool == "kafka-producer-perf-test" and k.ver and k.ver < (4, 2):    # --bootstrap-server was added to the perf tool in 4.2
                base = [x for x in base if x not in ("--bootstrap-server", B)]
                val = f"bootstrap.servers={B} linger.ms=1"
            a = [k.bin(tool)] + base + [opt, val]
            rc, out = k.run(a, stdin="a\n", t=90)
            want = k.ver is None or k.ver >= (4, 2)
            warned = re.search(rf"{re.escape(opt)}[^\n]*deprecat|deprecat[^\n]*{re.escape(opt)}", out, re.I) is not None
            res[("deprecated-cli-option", f"{tool} {opt}")] = (warned == want and not REJECT.search(out), first(out, "deprecat") if warned else "no deprecation line (option accepted silently)")
        for tool in R.BOOTSTRAP_TOOLS:
            a = [k.bin(tool), "--bootstrap-server", "localhost:9092 localhost:9093"]
            a += {"kafka-topics": ["--list"], "kafka-configs": ["--describe", "--entity-type", "brokers"], "kafka-console-consumer": ["--topic", "t1", "--timeout-ms", "3000"]}[tool]
            rc, out = k.run(a, t=90)
            b = [x if x != "localhost:9092 localhost:9093" else "localhost:9092,localhost:9093" for x in a]
            rc2, out2 = k.run(b, t=90)
            bad = re.search(r"Failed to create new KafkaAdminClient|Invalid url|Invalid port|ConfigException|KafkaException", out) is not None
            res[("bootstrap-server-format", tool)] = (bad and rc not in (0, None) or bad, first(out, r"Failed to create|Invalid|ConfigException|KafkaException"))
        for cls, new in R.REMOVED_CLASSES.items():
            rc, out = k.run([k.bin("kafka-run-class"), cls, "--help"], t=90)
            gone = f"Could not find or load main class {cls}" in out
            repl = os.path.exists(f"{home}/bin/{new}") if new.endswith(".sh") else True
            res[("removed-tool-class", cls)] = (gone and repl, f"Could not find or load main class {cls}" if gone else first(out, "."))
        for s, _why in R.REMOVED_SCRIPTS.items():
            here = glob.glob(f"{home}/bin/{s}*") + glob.glob(f"{home}/bin/windows/{s}*")
            res[("removed-tool-script", s)] = (not here, "no such file in the distribution" if not here else "present: " + here[0])
        for s in R.ZOOKEEPER_SCRIPTS:
            here = glob.glob(f"{home}/bin/{s}*") + glob.glob(f"{home}/bin/windows/{s}*")
            res[("zookeeper-script", s)] = (not here, "no such file in the distribution" if not here else "present: " + here[0])
        for p in R.KRAFT_PATHS:
            ex = os.path.exists(f"{home}/{p}")
            rc, out = k.run([k.bin("kafka-server-start"), f"{home}/{p}"], t=90)
            res[("kraft-config-path", p)] = (not ex and rc not in (0, None), "file does not exist; kafka-server-start fails" if not ex else "file exists")
        for cls in R.REMOVED_PARTITIONERS:
            name = cls.rsplit(".", 1)[1]
            c = k.tmp + "/part.properties"
            open(c, "w").write(f"partitioner.class={cls}\n")
            rc, out = k.run([k.bin("kafka-console-producer"), "--bootstrap-server", B, "--topic", "t1", CFG, c], stdin="a\n", t=90)
            res[("removed-partitioner", name)] = (bool(re.search(r"ConfigException|ClassNotFoundException|Class .* could not be found", out)), first(out, r"ConfigException|ClassNotFound|could not be found"))
        for case, props, want in (("idempotence+6", "enable.idempotence=true\nmax.in.flight.requests.per.connection=6\n", True),
                                  ("default+6", "max.in.flight.requests.per.connection=6\n", True),
                                  ("idempotence off+6 (control)", "enable.idempotence=false\nmax.in.flight.requests.per.connection=6\n", False)):
            c = k.tmp + "/idem.properties"
            open(c, "w").write(props)
            rc, out = k.run([k.bin("kafka-console-producer"), "--bootstrap-server", B, "--topic", "t1", CFG, c], stdin="a\n", t=90)
            rej = bool(re.search(r"ConfigException", out)) and "max.in.flight" in out
            res[("idempotence-in-flight", case)] = (rej == want, first(out, "ConfigException") if rej else "accepted (no ConfigException)")
        res.update(connect_cases(k))
        res.update(image_env_case(k))
    finally:
        k.close()
    return res


def connect_cases(k):
    """Start a standalone Connect worker (small heap) next to the broker and ask its REST validate endpoint which settings a connector defines."""
    import urllib.request
    res = {}
    w = k.tmp + "/worker.properties"
    with open(w, "w") as fh:
        fh.write(f"bootstrap.servers={B}\nkey.converter=org.apache.kafka.connect.json.JsonConverter\nvalue.converter=org.apache.kafka.connect.json.JsonConverter\n"
                 f"offset.storage.file.filename={k.tmp}/connect.offsets\nlisteners=HTTP://localhost:18083\n")
    log = []
    p = subprocess.Popen([k.bin("connect-standalone"), w], env=dict(k.env, KAFKA_HEAP_OPTS="-Xmx256m -Xms64m"), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True)
    threading.Thread(target=lambda: [log.append(x) for x in p.stdout], daemon=True).start()

    def req(path, body):
        r = urllib.request.Request("http://localhost:18083" + path, data=json.dumps(body).encode(), method="PUT", headers={"Content-Type": "application/json"})
        return json.load(urllib.request.urlopen(r, timeout=60))

    try:
        names = None
        base = {"connector.class": "org.apache.kafka.connect.mirror.MirrorSourceConnector", "name": "m", "source.cluster.alias": "a", "target.cluster.alias": "b"}
        for _ in range(60):
            try:
                names = {c["definition"]["name"] for c in req("/connector-plugins/MirrorSourceConnector/config/validate", base)["configs"]}
                break
            except Exception:
                time.sleep(2)
        rf = dict(base, **{"transforms": "x", "transforms.x.type": R.REPLACEFIELD_TYPE + "$Value"})
        rfnames = {c["definition"]["name"] for c in req("/connector-plugins/MirrorSourceConnector/config/validate", rf)["configs"]} if names else set()
        for key, (cls, _) in R.REMOVED_CONNECTOR.items():
            ok = bool(names) and key not in names and "topics.exclude" in names
            res[("removed-connector-config", key)] = (ok, f"{key} is not among the {len(names or [])} settings MirrorSourceConnector defines" if ok else f"defined, or worker did not start: {first(''.join(log), 'rror|xception')}")
        for key, new in R.REPLACEFIELD_KEYS.items():
            ok = bool(rfnames) and f"transforms.x.{key}" not in rfnames and f"transforms.x.{new}" in rfnames
            res[("removed-connector-config", f"ReplaceField {key}")] = (ok, f"transforms.x.{key} not defined, transforms.x.{new} is" if ok else "still defined, or worker did not start")
    finally:
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except Exception:
            pass
        p.wait()
    return res


def first(text: str, pat: str) -> str:
    for ln in text.splitlines():
        if re.search(pat, ln, re.I):
            return ln.strip()[:200]
    return text.strip().splitlines()[0][:200] if text.strip() else "(no output)"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--kafka", help="primary Kafka directory; its result must match every claim")
    ap.add_argument("--extra-kafka", action="append", default=[], help="more releases for the matrix (differences are reported, not fatal)")
    ap.add_argument("--json")
    ap.add_argument("--markdown")
    ap.add_argument("--count", action="store_true", help="print the number of cases and the number of rules and exit (no Kafka needed)")
    a = ap.parse_args(argv)
    cs = cases()
    if a.count:
        print(f"{len(cs)} cases, {len({c[0] for c in cs})} rules")
        return 0
    if not a.kafka:
        ap.error("--kafka is required (or use --count)")
    homes = [a.kafka] + a.extra_kafka
    results = {}
    for h in homes:
        v = ".".join(map(str, parse_version(h) or ("?",)))
        t0 = time.time()
        results[v] = run_release(h)
        print(f"[oracle] Kafka {v}: {sum(1 for x in results[v].values() if x[0])}/{len(results[v])} as claimed ({time.time() - t0:.0f}s)", file=sys.stderr)
    primary = list(results)[0]
    fails = 0
    lines = ["| Rule | Case | " + " | ".join(results) + " |", "|---|---|" + "---|" * len(results)]
    for rule, case, _ in cs:
        cells = []
        for v, r in results.items():
            ok, why = r.get((rule, case), (False, "not run"))
            cells.append("pass" if ok else "DIFFERS")
            if not ok and v == primary:
                fails += 1
                print(f"FAIL [{v}] {rule} / {case}: {why}", file=sys.stderr)
        lines.append(f"| `{rule}` | `{case}` | " + " | ".join(cells) + " |")
    if a.json:
        json.dump({v: {f"{k[0]} / {k[1]}": {"ok": x[0], "observed": x[1]} for k, x in r.items()} for v, r in results.items()}, open(a.json, "w"), indent=1)
    md = "\n".join(lines) + "\n"
    if a.markdown:
        open(a.markdown, "w").write(md)
    print(md)
    print(f"{len(cs)} cases, {fails} failed on Kafka {primary}", file=sys.stderr)
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
