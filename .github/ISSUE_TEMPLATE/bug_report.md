---
name: Bug report
about: Report a build failure, crash, correctness issue, or unexpected latency
title: "[bug] "
labels: bug
---

## Summary

A clear, concise description of what's wrong.

## Environment

- **OS:** (e.g. macOS 15.4 / Ubuntu 24.04)
- **CPU:** (e.g. Apple M3 Max / Intel i9-9900K) — this matters: the NEON and AVX2
  paths are selected by architecture
- **Rust:** output of `rustc --version`
- **Binary:** which one (`trading-engine`, `fake-exchange`, `market-simulator`,
  `bench-one-threaded`, `bench-multi-threaded`)

## Steps to reproduce

1. ...
2. ...

## Expected vs actual

What you expected to happen, and what actually happened.

## Output / logs

Paste the latency report, the relevant `logs/.../*.json`, and any compiler or
runtime error output. For a build failure, the full `cargo build --release`
output is the most useful thing you can include.
