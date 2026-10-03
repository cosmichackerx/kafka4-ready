"""Independent cross-check for misses: a loose regex lists lines that mention a Kafka 3 construct the rules know; lines kafka4-ready did not flag are printed.

    python study/loose.py FILES_DIR SCAN.json OUT.json
"""
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from kafka4_ready.scan import kind_of  # noqa: E402

files, scan, out = sys.argv[1:4]
flagged = {(f["repo"], f["file"], f["line"]) for f in json.load(open(scan))["findings"]}
LOOSE = [
    ("cli --zookeeper", re.compile(r"kafka-[\w-]+(\.sh)?\b.*--zookeeper|^\s*--zookeeper\b")),
    ("zk script", re.compile(r"zookeeper-(server-start|server-stop|shell|security-migration)")),
    ("env zk", re.compile(r"KAFKA_(CFG_)?ZOOKEEPER_", re.I)),
    ("props zk", re.compile(r"^\s*zookeeper\.[a-zA-Z.]+\s*[=:]")),
    ("removed props", re.compile(r"^\s*(log\.message\.format\.version|inter\.broker\.protocol\.version|message\.format\.version)\s*[=:]")),
    ("whitelist", re.compile(r"--(topic-white-list|whitelist)\b")),
    ("tool class", re.compile(r"kafka\.(tools\.(JmxTool|StreamsResetter|ClusterTool|EndToEndLatency|StateChangeLogMerger)|admin\.FeatureCommand)")),
    ("kraft path", re.compile(r"config/kraft/(server|broker|controller)\.properties")),
    ("space bootstrap", re.compile(r"--bootstrap-server[ =][\"'][\w.\-]+:\d+\s+[\w.\-]+:\d+")),
]
miss = []
n = 0
for repo in sorted(os.listdir(files)):
    rd = os.path.join(files, repo)
    if not os.path.isdir(rd):
        continue
    for root, _, fs in os.walk(rd):
        for f in fs:
            p = os.path.join(root, f)
            rel = os.path.relpath(p, rd).replace(os.sep, "/")
            if not kind_of(f, rel):
                continue
            for i, line in enumerate(open(p, encoding="utf-8", errors="replace").read().splitlines()):
                if line.lstrip().startswith(("#", "!", "//")):
                    continue
                for name, rx in LOOSE:
                    if rx.search(line):
                        n += 1
                        if (repo.replace("__", "/", 1), rel, i + 1) not in flagged:
                            miss.append({"kind": name, "repo": repo.replace("__", "/", 1), "file": rel, "line": i + 1, "text": line.strip()[:160]})
                        break
json.dump(miss, open(out, "w"), indent=1)
import collections
print(n, "loose matches,", len(miss), "not flagged:", dict(collections.Counter(m["kind"] for m in miss)))
