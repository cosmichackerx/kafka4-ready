"""Per-rule table (findings, repos, hand-checked, TP, FP, Wilson 95% interval) from a scan json, the sample json and the labels.

    python study/tabulate.py SCAN.json SAMPLE.json A|B
"""
import json
import math
import os
import sys

scan, sample, which = sys.argv[1:4]
fp = {int(k) for k in json.load(open(os.path.join(os.path.dirname(__file__), "labels.json")))[which]["fp"]}
data, items = json.load(open(scan)), json.load(open(sample))


def wilson(k, n, z=1.96):
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0, c - h), min(1, c + h))


rules = ["zookeeper-mode", "removed-broker-config", "invalid-config-value", "deprecated-broker-config", "removed-cli-option", "deprecated-cli-option", "bootstrap-server-format",
         "removed-tool-class", "removed-tool-script", "zookeeper-script", "kraft-config-path", "removed-partitioner", "idempotence-in-flight", "removed-connector-config"]
print("| Rule | Findings | Repos | Hand-checked | True | False | Precision (95% Wilson) |\n|---|---:|---:|---:|---:|---:|---|")
tot = [0, 0, 0]
for r in rules:
    fs = [f for f in data["findings"] if f["rule"] == r]
    ids = [i for i, f in enumerate(items) if f["rule"] == r]
    n, bad = len(ids), sum(1 for i in ids if i in fp)
    lo, hi = wilson(n - bad, n)
    tot = [tot[0] + len(fs), tot[1] + n, tot[2] + bad]
    prec = f"{(n - bad) / n:.0%} ({lo:.0%} to {hi:.0%})" if n else "no sample"
    print(f"| `{r}` | {len(fs)} | {len({f['repo'] for f in fs})} | {n} | {n - bad} | {bad} | {prec} |")
lo, hi = wilson(tot[1] - tot[2], tot[1])
print(f"| all | {tot[0]} | {len({f['repo'] for f in data['findings']})} | {tot[1]} | {tot[1] - tot[2]} | {tot[2]} | {(tot[1] - tot[2]) / tot[1]:.0%} ({lo:.0%} to {hi:.0%}) |")
