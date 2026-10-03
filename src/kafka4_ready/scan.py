"""Find Kafka 3 settings and command lines that Kafka 4 rejects or ignores. Text matching only; nothing is executed.

Two readers: `.properties` files (broker and client settings) and everything else that holds command lines (shell, Makefile, Dockerfile, YAML,
Jenkinsfile) where a logical line (backslash continuations joined) is tokenised and every Kafka tool word starts an invocation that ends at `; & | ( ) \\``.
"""
from __future__ import annotations

import fnmatch
import os
import re
from dataclasses import dataclass, field

from .rules import (BOOTSTRAP_TOOLS, BROKER_ONLY, DEPRECATED_BROKER, DEPRECATED_CLI, INVALID_MIN, KRAFT_PATHS, REMOVED_BROKER, REMOVED_CLASSES,
                    REMOVED_CLI, REMOVED_PARTITIONERS, RULES, TESTED, ZOOKEEPER_SCRIPTS)

SKIP_DIRS = {".git", "node_modules", "vendor", "dist", "target", ".venv", "venv", "__pycache__", ".tox"}
YAML_EXT = (".yml", ".yaml")
SHELL_EXT = (".sh", ".bash", ".zsh", ".ksh", ".envrc")
MAX_BYTES = 1_000_000
V = TESTED[-1]


@dataclass
class Finding:
    rule: str
    severity: str
    file: str
    line: int
    col: int
    message: str
    snippet: str = ""

    @property
    def url(self) -> str:
        return RULES[self.rule].url


@dataclass
class Result:
    findings: list = field(default_factory=list)
    files_scanned: int = 0
    invocations: int = 0
    pr: dict | None = None


def kind_of(name: str, path: str = "") -> str | None:
    low = name.lower()
    if low.endswith(".properties") or ".properties." in low:
        return "properties"
    if low.endswith(YAML_EXT):
        return "yaml"
    if low.endswith(SHELL_EXT):
        return "shell"
    if low in ("makefile", "gnumakefile") or low.endswith(".mk"):
        return "makefile"
    if low == "dockerfile" or low.startswith("dockerfile.") or low.endswith(".dockerfile") or low.startswith("containerfile"):
        return "dockerfile"
    if low in ("jenkinsfile", "justfile", "taskfile"):
        return "shell"
    return None


def logical_lines(text: str):
    """Yield (joined_text, [(start_index_in_joined, lineno, col0)]) with backslash continuations joined."""
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        parts, segs, pos = [], [], 0
        j = i
        while j < len(lines):
            raw = lines[j]
            body = raw.rstrip()
            cont = body.endswith("\\") and not body.lstrip().startswith("#")
            if cont:
                body = body[:-1]
            lead = len(body) - len(body.lstrip()) if j > i else 0
            piece = body[lead:] if j > i else body
            segs.append((pos, j + 1, lead))
            parts.append(piece)
            pos += len(piece) + 1
            j += 1
            if not cont:
                break
        yield " ".join(parts), segs
        i = j


def locate(segs, idx):
    ln, col0, start = segs[0][1], segs[0][2], segs[0][0]
    for s, l, c in segs:
        if s <= idx:
            ln, col0, start = l, c, s
    return ln, idx - start + col0 + 1


def strip_comment(s: str) -> str:
    q = None
    for i, ch in enumerate(s):
        if q:
            if ch == q:
                q = None
        elif ch in "\"'":
            q = ch
        elif ch == "#" and (i == 0 or s[i - 1].isspace()):
            return s[:i]
    return s


TOKEN = re.compile(r"""(?:[^\s"';&|()`]+|"[^"]*"|'[^']*')+|[;&|()`]""")
SEP = {";", "&", "|", "(", ")", "`"}


def norm(tok: str) -> str:
    t = tok.strip(",[]")
    if len(t) >= 2 and t[0] == t[-1] and t[0] in "\"'":
        t = t[1:-1]
    return t


def tool_name(tok: str) -> str | None:
    """`/opt/kafka/bin/kafka-topics.sh` -> `kafka-topics`; None if it is no Kafka tool word."""
    t = norm(tok)
    base = t.replace("\\", "/").rsplit("/", 1)[-1]
    m = re.fullmatch(r"((?:kafka|zookeeper)-[a-z0-9-]+?)(?:\.sh|\.bat)?", base)
    return m.group(1) if m else None


IGNORE_RE = re.compile(r"kafka4-ready:\s*ignore(?:\s+([\w, -]+))?")


def suppressed(lines, line_no: int, rule: str) -> bool:
    for ln in (line_no, line_no - 1):
        if 1 <= ln <= len(lines):
            m = IGNORE_RE.search(lines[ln - 1])
            if m:
                listed = [x for x in re.split(r"[,\s]+", m.group(1) or "") if x]
                if not listed or rule in listed:
                    return True
    return False


# ---------------------------------------------------------------- command lines
def command_findings(text: str, file: str):
    out, n_inv = [], 0
    for joined, segs in logical_lines(text):
        code = strip_comment(joined)
        low = code.lower()
        if "kafka" not in low and "zookeeper" not in low:
            continue
        snippet = code.strip()[:160]
        toks = [(m.group(0), m.start()) for m in TOKEN.finditer(code)]
        k = 0
        while k < len(toks):
            t, pos = toks[k]
            name = tool_name(t) if t not in SEP else None
            if not name:
                k += 1
                continue
            j = k + 1
            while j < len(toks) and toks[j][0] not in SEP:
                j += 1
            args = toks[k + 1:j]
            n_inv += 1
            ln, col = locate(segs, pos)
            if name in ZOOKEEPER_SCRIPTS:
                out.append(Finding("zookeeper-script", "error", file, ln, col,
                                   f"{name}.sh: the Kafka {V} distribution contains no ZooKeeper script (ZooKeeper support was removed in Kafka 4.0); {RULES['zookeeper-script'].fix}", snippet))
            flags = []
            b = 0
            while b < len(args):
                a = norm(args[b][0])
                if a.startswith("--") and len(a) > 2:
                    fl, eq, val = a.partition("=")
                    if not eq and b + 1 < len(args) and not norm(args[b + 1][0]).startswith("-"):
                        val = norm(args[b + 1][0])
                    flags.append((fl, val if (eq or val) else None, args[b][1]))
                b += 1
            for fl, val, idx in flags:
                ln2, col2 = locate(segs, idx)
                for tool, opt, hint in REMOVED_CLI:
                    if name == tool and fl == opt:
                        out.append(Finding("removed-cli-option", "error", file, ln2, col2,
                                           f"{name} {opt}: Kafka {V} rejects it (\"{opt} is not a recognized option\" or its equivalent); {hint}", snippet))
                for tool, opt, new in DEPRECATED_CLI:
                    if name == tool and fl == opt:
                        out.append(Finding("deprecated-cli-option", "note", file, ln2, col2,
                                           f"{name} {opt}: deprecated since Kafka 4.2, still accepted in {V} with a warning; use {new}", snippet))
                if name in BOOTSTRAP_TOOLS and fl == "--bootstrap-server" and val and "${" not in val and "$(" not in val:
                    if re.fullmatch(r"[\w.\-]+:\d+(?:\s+[\w.\-]+:\d+)+", val.strip()):
                        out.append(Finding("bootstrap-server-format", "error", file, ln2, col2,
                                           f"{name} --bootstrap-server \"{val}\": the brokers are separated by spaces; Kafka {V} fails with \"Failed to create new KafkaAdminClient\". Use commas", snippet))
            if name == "kafka-run-class":
                for a, p in args:
                    a = norm(a)
                    if not a.startswith("-") and a in REMOVED_CLASSES:
                        ln2, col2 = locate(segs, p)
                        out.append(Finding("removed-tool-class", "error", file, ln2, col2,
                                           f"kafka-run-class {a}: Kafka {V} fails with \"Could not find or load main class\"; use {REMOVED_CLASSES[a]}", snippet))
                        break
            k = j
    for joined, segs in logical_lines(text):
        code = strip_comment(joined)
        for p in KRAFT_PATHS:
            i = code.find(p)
            if i >= 0:
                ln, col = locate(segs, i)
                out.append(Finding("kraft-config-path", "error", file, ln, col,
                                   f"{p}: Kafka {V} has no config/kraft directory (server.properties is KRaft by default); {RULES['kraft-config-path'].fix}", code.strip()[:160]))
    return out, n_inv


# ---------------------------------------------------------------- environment variables (image convention) and YAML-embedded key=value
ENV_RE = re.compile(r"\b(KAFKA_(?:CFG_)?[A-Z0-9_]+)\b(?=\s*[=:])")
BLOCK_RE = re.compile(r"^\s*(?:-\s*)?([a-z][A-Za-z0-9_.]*)\s*=\s*(.*)$")
LOWER_REMOVED = {k.lower(): k for k in REMOVED_BROKER}
IMAGE_NOTE = ("(variable-to-setting mapping is the apache/kafka and Bitnami image convention; mapped by name, the image was not run)")


def env_to_key(var: str) -> str:
    s = re.sub(r"^KAFKA_(?:CFG_)?", "", var)
    s = s.replace("___", "\0").replace("__", "\1").replace("_", ".")
    return s.replace("\0", "-").replace("\1", "_").lower()


def env_findings(text: str, file: str):
    out = []
    for i, line in enumerate(text.splitlines()):
        code = strip_comment(line)
        for m in ENV_RE.finditer(code):
            key = env_to_key(m.group(1))
            real = LOWER_REMOVED.get(key)
            if real and real not in BROKER_ONLY:
                out.append(Finding("removed-broker-config", "warning", file, i + 1, m.start() + 1,
                                   f"{m.group(1)} sets {real}: {REMOVED_BROKER[real]}. A Kafka {V} broker starts and ignores it without a log line {IMAGE_NOTE}", line.strip()[:160]))
        m = BLOCK_RE.match(code)
        if m and not ENV_RE.match(code.lstrip(" -")):
            real = LOWER_REMOVED.get(m.group(1).lower())
            if real and real not in BROKER_ONLY and real == m.group(1):
                out.append(Finding("removed-broker-config", "warning", file, i + 1, code.index(m.group(1)) + 1,
                                   f"{real}: {REMOVED_BROKER[real]}. A Kafka {V} broker starts and ignores it without a log line", line.strip()[:160]))
    return out


# ---------------------------------------------------------------- .properties
PROP_RE = re.compile(r"^\s*([^\s=:#!][^=:\s]*)\s*[=:]\s*(.*?)\s*$")


def parse_properties(text: str):
    items = []
    for i, line in enumerate(text.splitlines()):
        m = PROP_RE.match(line)
        if m:
            items.append((m.group(1), m.group(2), i + 1, line))
    return items


def is_broker_file(name: str, keys: set) -> bool:
    if re.fullmatch(r"(?:server|broker|controller)[\w.\-]*\.properties", name.lower()):
        return True
    return bool(keys & {"process.roles", "log.dirs", "zookeeper.connect", "controller.quorum.voters", "controller.listener.names", "inter.broker.listener.name"})


def properties_findings(text: str, file: str, name: str):
    out = []
    items = parse_properties(text)
    keys = {k for k, _, _, _ in items}
    broker = is_broker_file(name, keys)
    kv = {k: (v, ln) for k, v, ln, _ in items}
    for k, v, ln, raw in items:
        snippet = raw.strip()[:160]
        if k in REMOVED_BROKER and (broker or k not in BROKER_ONLY):
            out.append(Finding("removed-broker-config", "warning", file, ln, 1,
                               f"{k}: {REMOVED_BROKER[k]}. A Kafka {V} broker starts and ignores it without a log line (`kafka-configs --describe --all` shows {k}=null)", snippet))
        if broker and k in INVALID_MIN:
            try:
                bad = int(v) < INVALID_MIN[k]
            except ValueError:
                bad = False
            if bad:
                out.append(Finding("invalid-config-value", "error", file, ln, 1,
                                   f"{k}={v}: Kafka {V} refuses to start: ConfigException \"Invalid value {v} for configuration {k}: Value must be at least {INVALID_MIN[k]}\"", snippet))
        if broker and k in DEPRECATED_BROKER:
            trig, msg = DEPRECATED_BROKER[k]
            if trig is None or v.lower() == trig:
                out.append(Finding("deprecated-broker-config", "note", file, ln, 1, f"{k}={v}: {msg}", snippet))
        if k == "partitioner.class" and v in REMOVED_PARTITIONERS:
            out.append(Finding("removed-partitioner", "error", file, ln, 1,
                               f"partitioner.class={v}: Kafka {V} clients fail with ConfigException (class removed); {RULES['removed-partitioner'].fix}", snippet))
    mif, idem = kv.get("max.in.flight.requests.per.connection"), kv.get("enable.idempotence")
    if mif and idem and idem[0].lower() == "true" and mif[0].isdigit() and int(mif[0]) > 5:
        out.append(Finding("idempotence-in-flight", "error", file, mif[1], 1,
                           f"max.in.flight.requests.per.connection={mif[0]} with enable.idempotence=true: Kafka {V} clients fail with ConfigException; {RULES['idempotence-in-flight'].fix}", ""))
    if broker and "process.roles" not in keys and ({"zookeeper.connect", "log.dirs", "log.dir"} & keys):
        ln = kv["zookeeper.connect"][1] if "zookeeper.connect" in kv else 1
        out.append(Finding("zookeeper-mode", "error", file, ln, 1,
                           f"no process.roles: this is a ZooKeeper-mode broker file; Kafka {V} refuses to start (\"Missing required configuration \\\"process.roles\\\"\"). {RULES['zookeeper-mode'].fix}", ""))
    return out


def scan_text(text: str, file: str, kind: str, name: str = ""):
    if kind == "properties":
        findings, n_inv = properties_findings(text, file, name or os.path.basename(file)), 0
    else:
        findings, n_inv = command_findings(text, file)
        findings += env_findings(text, file)
    lines = text.splitlines()
    return [f for f in findings if not suppressed(lines, f.line, f.rule)], n_inv


def _walk(path: str):
    if os.path.isfile(path):
        yield path
        return
    for root, dirs, files in os.walk(path):
        dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS)
        for f in sorted(files):
            yield os.path.join(root, f)


def scan(path: str = ".", ignore=(), disabled=(), only=()) -> Result:
    base = path if os.path.isdir(path) else os.path.dirname(path) or "."
    res = Result()
    for full in _walk(path):
        rel = os.path.relpath(full, base).replace(os.sep, "/")
        kind = kind_of(os.path.basename(full), rel)
        if not kind or any(fnmatch.fnmatch(rel, g) or fnmatch.fnmatch(os.path.basename(rel), g) for g in ignore):
            continue
        try:
            if os.path.getsize(full) > MAX_BYTES:
                continue
            with open(full, encoding="utf-8", errors="replace") as fh:
                text = fh.read()
        except OSError:
            continue
        res.files_scanned += 1
        found, n = scan_text(text, rel, kind, os.path.basename(full))
        res.invocations += n
        res.findings += [f for f in found if f.rule not in disabled and (not only or f.rule in only)]
    res.findings.sort(key=lambda f: (f.file, f.line, f.col, f.rule))
    return res
