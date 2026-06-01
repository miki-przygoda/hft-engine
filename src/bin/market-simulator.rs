//! `market-simulator` — standalone UDP feed generator for external testing.
//!
//! Sends `WARMUP_PACKETS` warmup ticks (no delay) followed by `REAL_PACKETS`
//! ticks at `PACKET_INTERVAL_MS` spacing to the engine's ingestor port. Used
//! with `fake-exchange` and `trading-engine` to measure the full kernel-path
//! round trip. The warmup count is shared via `rust_hft_software::config` so it
//! stays in lock-step with the engine (invariant #9).

use std::net::UdpSocket;
use std::thread;
use std::time::Duration;

use rust_hft_software::config::{WARMUP_PACKETS, REAL_PACKETS, INGESTOR_ADDR, PACKET_INTERVAL_MS};

fn main() {
    let socket = UdpSocket::bind("0.0.0.0:0").expect("Failed to create UDP socket");

    thread::sleep(Duration::from_secs(1));

    let price = 100.5_f32;
    let volume = 1000.0_f32;

    // Warmup phase: fire WARMUP_PACKETS with no inter-packet delay.
    // The engine processes these through the full hot path (NEON asm, elapsed(),
    // lock-free write) to warm all caches and train the branch predictor,
    // but does not record them in the trade log.
    for sequence in 1..=WARMUP_PACKETS {
        let mut packet = Vec::new();
        packet.extend_from_slice(&price.to_le_bytes());
        packet.extend_from_slice(&volume.to_le_bytes());
        packet.extend_from_slice(&sequence.to_le_bytes());
        socket.send_to(&packet, INGESTOR_ADDR).expect("Failed to send warmup packet");
    }

    // Brief gap so the engine drains all warmup ticks before real traffic starts.
    thread::sleep(Duration::from_millis(50));

    // Real trading phase: REAL_PACKETS at PACKET_INTERVAL_MS intervals.
    for i in 0..REAL_PACKETS {
        let sequence = WARMUP_PACKETS + 1 + i;
        let mut packet = Vec::new();
        packet.extend_from_slice(&price.to_le_bytes());
        packet.extend_from_slice(&volume.to_le_bytes());
        packet.extend_from_slice(&sequence.to_le_bytes());
        socket.send_to(&packet, INGESTOR_ADDR).expect("Failed to send packet");
        thread::sleep(Duration::from_millis(PACKET_INTERVAL_MS));
    }
}
