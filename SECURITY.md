# Security

kafka4-ready only reads files under the directory you give it and writes a report to stdout or the file you name. It has no runtime dependencies, never runs `kafka-*` tools, a shell or any project code, and makes no network request, except the optional sticky pull request comment of the GitHub Action (GitHub API, only when you turn `comment` on).

The oracle in `tests/oracle/` downloads Kafka releases from the Apache mirrors in CI; each tarball is checked against the SHA-512 file published next to it.

Found a vulnerability (for example a path traversal through a crafted file name)? Please use GitHub's private vulnerability reporting for this repository ("Security" tab, "Report a vulnerability") instead of a public issue.
