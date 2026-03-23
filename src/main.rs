mod models;
mod testing_scripts;

use std::arch::asm;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

unsafe extern "C" {
    fn pthread_set_qos_class_self_np(qos: u32, relpri: i32) -> i32;
}

fn main() {
    let buffer = Arc::new(models::RingBuffer {
        ticks: unsafe { std::mem::zeroed() },
        latest_idx: AtomicU64::new(0),
    });

    let buffer_clone = Arc::clone(&buffer);
    thread::spawn(move || {
        let mut seq = 1;
        loop {
            thread::sleep(Duration::from_millis(1000));
            buffer_clone.latest_idx.store(seq as u64, Ordering::Release);
            seq += 1;
        }
    });

    unsafe {
        pthread_set_qos_class_self_np(0x21, 0);
        println!("Strategy Live on Performance Core. Waiting for Ticks...");
        trading_strategy(&buffer);
    }
}

#[inline(always)]
unsafe fn trading_strategy(buffer: &models::RingBuffer) {
    unsafe {
        let mut last_processed_seq = 0;
        for _ in 0..10_000 {
            let mut _dummy: u64;
            asm!("fmul v0.4s, v0.4s, v0.4s", "fmov {res:w}, s0", res = out(reg) _dummy);
        }

        loop {
            let current_seq = buffer.latest_idx.load(Ordering::Acquire);

            if current_seq > last_processed_seq {
                let idx = (current_seq % models::BUFFER_SIZE as u64) as usize;
                let tick_ptr = &buffer.ticks[idx];

                let decision: u64;
                asm!(
                "ld1 {{v0.4s}}, [{ptr}]",
                "fmul v1.4s, v0.4s, v0.4s",
                "fmov {res:w}, s1",
                "and {res:w}, {res:w}, #1",
                ptr = in(reg) tick_ptr,
                res = out(reg) decision,
                options(nostack, nomem)
                );

                if decision == 1 {
                    println!("BUY TRIGGERED @ SEQ {}", current_seq);
                }

                last_processed_seq = current_seq;
            } else {
                std::hint::spin_loop();
            }
        }
    }
}