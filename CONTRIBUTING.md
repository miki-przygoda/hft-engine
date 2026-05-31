# Contributing

Thanks for your interest in `rust-hft-software`. This is an educational/research
project with production-grade engineering standards — contributions are welcome,
whether that's a bug fix, a new platform path, or a step off the
[roadmap](CLAUDE.md#roadmap--what-isnt-here-yet).

Before diving in, please read [`CLAUDE.md`](CLAUDE.md) — it's the architecture
and design reference and explains *why* the code is shaped the way it is. In
particular, the [Invariants](CLAUDE.md#invariants--do-not-break-these) section
lists the load-bearing assumptions the lock-free design depends on; breaking one
introduces a data race or latency regression that a quick test won't catch.

## Getting started

```bash
git clone https://github.com/miki-przygoda/hft-engine
cd hft-engine
cargo build --release
cargo run --release --bin trading-engine
```

The project has **no external dependencies** and targets `edition = "2024"`.
Please keep it that way — hand-rolled stdlib-only solutions (the JSON writer, the
Gregorian date calculation) are a deliberate part of the project's character.

## Platform notes

- **macOS, Apple Silicon** is the primary target (NEON signal path, QOS P-core
  bias). This is where the headline latency numbers come from.
- **Linux, x86_64** is fully supported (AVX2 signal path, `SCHED_FIFO` +
  `sched_setaffinity`). Thread priority and pinning require `CAP_SYS_NICE` — run
  with `sudo` to exercise them; without it the engine still runs.
- If you change anything inside a `#[cfg(target_os = ...)]` or
  `#[cfg(target_arch = ...)]` block, build for **both** macOS and Linux before
  opening a PR. The CI matrix does this, but it's faster to catch locally — the
  original Linux build break was a `cfg`'d-out block that the macOS host never
  compiled.

## Before you open a PR

Please make sure the following pass on the platform(s) you touched:

```bash
cargo build --release                      # must compile clean (the hard gate)
cargo clippy --release --all-targets       # lints (informational)
cargo run --release --bin trading-engine   # smoke test: a full run should print a report and exit 0
```

A note on formatting: the codebase uses **deliberate manual column alignment**
(aligned colons in struct/const declarations). Please match the surrounding
style rather than running `cargo fmt`, which would collapse that alignment —
`fmt --check` is intentionally *not* a CI gate.

CI runs a **build + clippy matrix on macOS and Linux** (build is the hard gate,
clippy is informational). To conserve Actions minutes, include `[skip ci]` in a
commit message when a push is docs-only or you don't need a CI run (for example
the final commit before opening a PR, once you've already validated locally).

## Pull request guidelines

- Keep changes focused; one logical change per PR.
- If you touch a hot-path structure or a lock-free protocol, call out which
  invariant(s) you considered in the PR description.
- Update [`CLAUDE.md`](CLAUDE.md) and the file map if you add, move, or remove a
  source file (a repo hook will remind you).
- Latency claims should be backed by a run log (`logs/.../*.json`) or a
  reproducible benchmark, not eyeballed.

## Reporting bugs & requesting features

Use the issue templates under
[`.github/ISSUE_TEMPLATE`](.github/ISSUE_TEMPLATE). For bugs, please include your
platform (OS + CPU), the binary you ran, and the latency report / log output.
