// Fake exchange — simulates the matching engine side of the round-trip.
//
// Design:
//   - Non-blocking spin-poll on the order socket (QOS_CLASS_USER_INTERACTIVE)
//     eliminates OS wakeup latency. Without this, every packet arrival costs a
//     full thread wakeup (~10–30 µs); with spin-poll the exchange detects the
//     packet on the very next recv attempt (~sub-µs).
//   - Heartbeat packets (amt < 24 bytes) are silently discarded. They exist only
//     to keep this process's socket and kernel networking state warm so real
//     order packets find a hot path when they arrive.
//   - Real order packets (amt == 24 bytes) are immediately echoed to port 34256.
//
// Packet layout (24 bytes, little-endian):
//   bytes  0– 7  sequence      u64
//   bytes  8–15  slot          u64
//   bytes 16–23  order_send_ns u64

use std::net::UdpSocket;

const LISTEN_ADDR:  &str = "127.0.0.1:34255";
const CONFIRM_ADDR: &str = "127.0.0.1:34256";

unsafe extern "C" {
    fn pthread_set_qos_class_self_np(qos: u32, relpri: i32) -> i32;
}

fn main() {
    // Raise to USER_INTERACTIVE so the OS schedules this on a P-core with the
    // same priority as the trading engine. Without this the exchange thread can
    // be starved behind lower-priority work, adding scheduling jitter.
    unsafe { pthread_set_qos_class_self_np(0x21, 0); }

    let socket = UdpSocket::bind(LISTEN_ADDR)
        .expect("fake-exchange: failed to bind on 34255");
    socket.set_nonblocking(true)
        .expect("fake-exchange: failed to set non-blocking");

    let mut buf = [0u8; 32];

    loop {
        match socket.recv_from(&mut buf) {
            Ok((amt, _)) if amt >= 24 => {
                // Real order — echo immediately to engine confirm socket.
                let _ = socket.send_to(&buf[..amt], CONFIRM_ADDR);
            }
            Ok(_) => {
                // Heartbeat packet — discard, path is already warm.
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::hint::spin_loop();
            }
            Err(_) => {}
        }
    }
}
