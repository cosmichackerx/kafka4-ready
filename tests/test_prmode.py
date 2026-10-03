import os
import subprocess

import pytest

from kafka4_ready.cli import main

GIT = ["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid"]


def git(cwd, *a):
    subprocess.run([*GIT, *a], cwd=cwd, check=True, capture_output=True)


@pytest.fixture
def repo(tmp_path):
    git(tmp_path, "init", "-q", "-b", "main")
    (tmp_path / "Makefile").write_text("old:\n\tkafka-topics.sh --zookeeper zk:2181 --list\n")
    git(tmp_path, "add", "-A")
    git(tmp_path, "commit", "-q", "-m", "base")
    return tmp_path


def test_existing_findings_are_not_new(repo, capsys):
    assert main([str(repo), "--base", "HEAD"]) == 0
    assert "introduced since HEAD" in capsys.readouterr().out


def test_new_finding_fails_and_old_one_stays_hidden(repo, capsys):
    with open(repo / "Makefile", "a") as fh:
        fh.write("new:\n\tkafka-acls.sh --authorizer x --list\n")
    git(repo, "commit", "-q", "-am", "pr")
    rc = main([str(repo), "--base", "HEAD~1", "-f", "json"])
    out = capsys.readouterr().out
    assert rc == 1 and "--authorizer" in out and "--zookeeper" not in out


def test_moved_line_is_not_new(repo, capsys):
    (repo / "Makefile").write_text("\n\n# comment\nold:\n\tkafka-topics.sh --zookeeper zk:2181 --list\n")
    git(repo, "commit", "-q", "-am", "move")
    assert main([str(repo), "--base", "HEAD~1"]) == 0


def test_unknown_base_is_a_usage_error(repo, capsys):
    assert main([str(repo), "--base", "no-such-rev"]) == 2
