# Contributing

    python -m venv .venv && . .venv/bin/activate
    pip install -e . pytest
    pytest -q

* A new rule needs an entry in `src/kafka4_ready/rules.py`, a detector in `scan.py`, unit tests with positive and negative cases, and a case in `tests/oracle/run_oracle.py`.
* Every rule that claims "Kafka 4 rejects/ignores X" must be an oracle case run against real Kafka: `python tests/oracle/fetch_kafka.py /tmp/kafka 4.3.1 && python tests/oracle/run_oracle.py --kafka /tmp/kafka/kafka_2.13-4.3.1` (needs Java 17; starts a broker on localhost:9092). If a behaviour cannot be checked that way, say so in the README table instead of adding an oracle flag.
* When a new Kafka release changes a behaviour, add its SHA-512 to `tests/oracle/kafka-sha512.txt`, extend `TESTED` in `rules.py`, re-run the oracle and update `docs/oracle-matrix.md` and the README. Do not widen a claim to versions that were not run.
* Keep the project dependency-free (standard library only) and compatible with Python 3.9.
