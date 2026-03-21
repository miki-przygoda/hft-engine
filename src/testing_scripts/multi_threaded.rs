use std::arch::asm;
use std::thread;
use std::time::{Duration, Instant};

const ITERATIONS_PER_THREAD: usize = 1_000_000_000;

fn main() {
    let num_cores = thread::available_parallelism().unwrap().get();
    println!("--- M3 Max 'The Kraken' Multi-Threaded Probe ---");
    println!("Detected Cores: {}", num_cores);

    println!("Phase 1: Progressive Thermal Ramp (10s)...");
    for i in 1..=num_cores {
        print!("\rActivating Core {}/{}...", i, num_cores);
        let mut handles = vec![];
        for _ in 0..i {
            handles.push(thread::spawn(|| {
                let start = Instant::now();
                while start.elapsed() < Duration::from_millis(1000) {
                    unsafe { asm!("fmul v0.4s, v0.4s, v0.4s", out("v0") _); }
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
    }
    println!("Phase 2: Executing 1 Billion Blocks per Core...");
    let start_time = Instant::now();
    let mut main_handles = vec![];

    for t_id in 0..num_cores {
        main_handles.push(thread::spawn(move || {
            unsafe {
                for _ in 0..(ITERATIONS_PER_THREAD / 128) {
                    asm!(
                    ".rept 128",
                    "fmul v0.4s, v0.4s, v0.4s", "fmul v1.4s, v1.4s, v1.4s",
                    "fmul v2.4s, v2.4s, v2.4s", "fmul v3.4s, v3.4s, v3.4s",
                    "fmul v4.4s, v4.4s, v4.4s", "fmul v5.4s, v5.4s, v5.4s",
                    "fmul v6.4s, v6.4s, v6.4s", "fmul v7.4s, v7.4s, v7.4s",
                    ".endr",
                    out("v0") _, out("v1") _, out("v2") _, out("v3") _,
                    out("v4") _, out("v5") _, out("v6") _, out("v7") _,
                    );
                }
            }
            t_id
        }));
    }

    for h in main_handles {
        h.join().unwrap();
    }

    let duration = start_time.elapsed();

    let total_ops = (num_cores * ITERATIONS_PER_THREAD) as f64 * 4.0;

    println!("-------------------------------------");
    println!("Total Wall Time:  {:?}", duration);
    println!("Total Operations: {} Billion", total_ops / 1e9);
    println!("Total Throughput: {:.2} Billion Ops/sec", (total_ops / 1e9) / duration.as_secs_f64());
    println!("1M Ops Latency:   {:.4} ns", (duration.as_nanos() as f64 / total_ops) * 1_000_000.0);
}