"""`--fix`: rewrite the mechanical Kafka 3 spellings in place. Nothing else in the file changes (indentation, line endings, comments).

Only edits that tests/oracle/run_fix_oracle.py ran on real Kafka tools (4.0.1 to 4.3.1) are made:

* renamed options: `kafka-console-consumer --whitelist` -> `--include`, `kafka-replica-verification --topic-white-list` -> `--topics-include`,
  `--broker-list` -> `--bootstrap-server` for `kafka-console-producer`, `kafka-consumer-perf-test`, `kafka-verifiable-consumer`;
* deleted option: `kafka-console-consumer --new-consumer` (the option is gone, the new consumer is the only one);
* `--bootstrap-server "a:9092 b:9092"` -> `"a:9092,b:9092"` (kafka-topics, kafka-configs, kafka-console-consumer);
* `config/kraft/{server,broker,controller}.properties` -> `config/{...}.properties` in commands (not where a COPY or volume mount creates that path).

Never rewritten, because they need a decision: `--zookeeper` (the replacement is another host), `kafka-acls --authorizer*`, ZooKeeper scripts, removed settings, tool classes.
The new spellings are meant for Kafka 4. `config/server.properties` is a ZooKeeper-mode file in Kafka 3, so a file that must still work on both should not be fixed blindly.
"""
from __future__ import annotations

import difflib
import fnmatch
import os
import re
from dataclasses import dataclass

from .rules import BOOTSTRAP_TOOLS, KRAFT_PATHS
from .scan import (MAX_BYTES, _walk, creates_path, flatten_tokens, is_distribution_wrapper, kind_of, locate, logical_lines, norm, strip_comment,
                   suppressed, tool_name)

RENAMES = {
    ("kafka-console-consumer", "--whitelist"): "--include",
    ("kafka-replica-verification", "--topic-white-list"): "--topics-include",
    ("kafka-console-producer", "--broker-list"): "--bootstrap-server",
    ("kafka-consumer-perf-test", "--broker-list"): "--bootstrap-server",
    ("kafka-verifiable-consumer", "--broker-list"): "--bootstrap-server",
}
DELETES = {("kafka-console-consumer", "--new-consumer")}
HOSTS = re.compile(r"[\w.\-]+:\d+(?:\s+[\w.\-]+:\d+)+")


@dataclass
class Edit:
    file: str
    line: int      # 1-based physical line
    start: int     # 0-based offset in that line
    end: int
    old: str
    new: str
    rule: str


@dataclass
class Skip:
    file: str
    line: int
    rule: str
    text: str
    reason: str


def _plan_line(joined, segs, file):
    code = strip_comment(joined)
    low = code.lower()
    if "kafka" not in low:
        return
    toks = flatten_tokens(code)
    SEP = {";", "&", "|", "(", ")", "`"}
    k = 0
    while k < len(toks):
        t, pos = toks[k]
        name = None if t in SEP else tool_name(t)
        if not name:
            k += 1
            continue
        j = k + 1
        while j < len(toks) and toks[j][0] not in SEP:
            j += 1
        args = toks[k + 1:j]
        for b, (raw, p) in enumerate(args):
            opt = raw.split("=", 1)[0]
            ln, col = locate(segs, p)
            if (name, opt) in RENAMES:
                if '"' in raw or "'" in raw:
                    continue
                yield "edit", Edit(file, ln, col - 1, col - 1 + len(opt), opt, RENAMES[(name, opt)], "removed-cli-option")
            elif (name, raw) in DELETES:
                yield "edit", Edit(file, ln, col - 1, col - 1 + len(raw), raw, "", "removed-cli-option")
            elif name in BOOTSTRAP_TOOLS and opt == "--bootstrap-server":
                if "=" in raw:
                    vraw, vpos = raw.split("=", 1)[1], p + len(opt) + 1
                elif b + 1 < len(args):
                    vraw, vpos = args[b + 1]
                else:
                    continue
                inner = norm(vraw)
                if vraw[:1] in "\"'" and vraw[-1:] == vraw[:1] and HOSTS.fullmatch(inner.strip()) and "${" not in vraw and "$(" not in vraw:
                    vl, vc = locate(segs, vpos)
                    if locate(segs, vpos + len(vraw) - 1)[0] != vl:
                        yield "skip", Skip(file, vl, "bootstrap-server-format", vraw, "the value spans several lines; fix it by hand")
                        continue
                    yield "edit", Edit(file, vl, vc - 1, vc - 1 + len(vraw), vraw, vraw[0] + re.sub(r"\s+", ",", inner.strip()) + vraw[0], "bootstrap-server-format")
        k = j
    for p in KRAFT_PATHS:
        i = code.find(p)
        if i >= 0 and not creates_path(code, i):
            ln, col = locate(segs, i)
            if locate(segs, i + len(p) - 1)[0] != ln:
                continue
            yield "edit", Edit(file, ln, col - 1 + len("config/"), col - 1 + len("config/kraft/"), "kraft/", "", "kraft-config-path")


def plan(text: str, file: str, name: str = ""):
    edits, skips = [], []
    if is_distribution_wrapper(name or os.path.basename(file), text):
        return edits, skips
    lines = text.splitlines()
    for joined, segs in logical_lines(text):
        for kind, item in _plan_line(joined, segs, file):
            if suppressed(lines, item.line, item.rule):
                continue
            (skips if kind == "skip" else edits).append(item)
    seen, out = set(), []
    for e in edits:      # nested quoting can reach one token twice
        key = (e.line, e.start, e.end)
        if key not in seen:
            seen.add(key)
            out.append(e)
    final = []
    for e in out:
        if e.new == "" and e.rule == "removed-cli-option":
            ln = lines[e.line - 1]
            if (ln[:e.start] + ln[e.end:]).strip() in ("", "\\"):
                skips.append(Skip(file, e.line, e.rule, e.old, "the option is alone on its line; remove it by hand"))
                continue
        final.append(e)
    return final, skips


def apply(text: str, edits) -> str:
    lines = text.splitlines(keepends=True)
    for e in sorted(edits, key=lambda e: (e.line, e.start), reverse=True):
        ln = lines[e.line - 1]
        body = ln.rstrip("\r\n")
        assert body[e.start:e.end] == e.old, (e, body)
        s, t = e.start, e.end
        if e.new == "" and e.rule == "removed-cli-option":     # delete the option with one adjacent space
            if s > 0 and body[s - 1] in " \t":
                s -= 1
            elif t < len(body) and body[t] in " \t":
                t += 1
        lines[e.line - 1] = body[:s] + e.new + body[t:] + ln[len(body):]
    return "".join(lines)


def unified(file: str, old: str, new: str) -> str:
    return "".join(difflib.unified_diff(old.splitlines(keepends=True), new.splitlines(keepends=True), f"a/{file}", f"b/{file}"))


def fix_path(path: str, ignore=(), disable=(), only=(), write: bool = False):
    """Plan (and with write=True apply) the fixes under `path`. Returns (edits, skips, diffs, files_changed)."""
    base = path if os.path.isdir(path) else os.path.dirname(path) or "."
    edits, skips, diffs, changed = [], [], [], 0
    for full in _walk(path):
        rel = os.path.relpath(full, base).replace(os.sep, "/")
        kind = kind_of(os.path.basename(full), rel)
        if not kind or kind in ("properties", "json") or any(fnmatch.fnmatch(rel, g) or fnmatch.fnmatch(os.path.basename(rel), g) for g in ignore):
            continue
        try:
            if os.path.getsize(full) > MAX_BYTES:
                continue
            with open(full, encoding="utf-8", newline="") as fh:     # newline="": keep CRLF
                text = fh.read()
        except (OSError, UnicodeDecodeError):
            continue
        e, s = plan(text, rel, os.path.basename(full))
        e = [x for x in e if x.rule not in disable and (not only or x.rule in only)]
        s = [x for x in s if x.rule not in disable and (not only or x.rule in only)]
        skips += s
        if not e:
            continue
        new = apply(text, e)
        edits += e
        diffs.append(unified(rel, text, new))
        changed += 1
        if write:
            with open(full, "w", encoding="utf-8", newline="") as fh:
                fh.write(new)
    return edits, skips, diffs, changed
