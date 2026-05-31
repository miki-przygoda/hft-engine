//! SIMD throughput benchmarks (gated behind the `testing` Cargo feature).
//!
//! These establish the hardware ceiling the trading loop cannot exceed. Each
//! file is also a standalone binary (`bench-one-threaded`, `bench-multi-threaded`).

mod one_threaded;
mod multi_threaded;