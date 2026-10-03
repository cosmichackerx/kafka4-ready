#!/usr/bin/env python3
"""Download and unpack Kafka releases for the oracle. Every tarball is checked against the SHA-512 in kafka-sha512.txt (pinned in this repository,
taken from the .sha512 files that Apache publishes next to the artifacts). Usage: fetch_kafka.py DEST [VERSION ...] (default: all pinned)."""
import hashlib
import os
import sys
import tarfile
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
HOSTS = ["https://downloads.apache.org/kafka/{v}/{f}", "https://archive.apache.org/dist/kafka/{v}/{f}"]


def pinned():
    out = {}
    for line in open(os.path.join(HERE, "kafka-sha512.txt")):
        if line.strip():
            digest, name = line.split()
            out[name.split("-")[1].rsplit(".tgz", 1)[0]] = (digest, name)
    return out


def fetch(dest: str, version: str, digest: str, name: str) -> str:
    target = os.path.join(dest, name.replace(".tgz", ""))
    if os.path.isdir(target):
        return target
    os.makedirs(dest, exist_ok=True)
    tgz = os.path.join(dest, name)
    err = None
    for host in HOSTS:
        url = host.format(v=version, f=name)
        try:
            h = hashlib.sha512()
            with urllib.request.urlopen(url, timeout=120) as r, open(tgz, "wb") as fh:
                while chunk := r.read(1 << 20):
                    h.update(chunk)
                    fh.write(chunk)
            if h.hexdigest() != digest:
                raise SystemExit(f"SHA-512 mismatch for {url}")
            break
        except OSError as e:
            err = e
    else:
        raise SystemExit(f"could not download {name}: {err}")
    with tarfile.open(tgz) as t:
        t.extractall(dest)
    os.remove(tgz)
    return target


if __name__ == "__main__":
    dest = sys.argv[1]
    pins = pinned()
    for v in (sys.argv[2:] or list(pins)):
        print(fetch(dest, v, *pins[v]))
