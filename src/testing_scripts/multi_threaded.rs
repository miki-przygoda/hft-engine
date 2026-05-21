use std::arch::asm;
use std::thread;
use std::time::{Duration, Instant};

const ITERATIONS_PER_THREAD: usize = 1_000_000_000;

fn main() {
    let num_cores = thread::available_parallelism().unwrap().get();

    #[cfg(target_arch = "aarch64")]
    println!("--- M3 Max 'The Kraken' Multi-Threaded Probe ---");
    #[cfg(target_arch = "x86_64")]
    println!("--- i9-9900K 'The Kraken' Multi-Threaded Probe (AVX2) ---");

    println!("Detected Cores: {}", num_cores);

    println!("Phase 1: Progressive Thermal Ramp (10s)...");
    for i in 1..=num_cores {
        print!("\rActivating Core {}/{}...", i, num_cores);
        let mut handles = vec![];
        for _ in 0..i {
            handles.push(thread::spawn(|| {
                let start = Instant::now();
                while start.elapsed() < Duration::from_millis(1000) {
                    unsafe {
                        #[cfg(target_arch = "aarch64")]
                        asm!("fmul v0.4s, v0.4s, v0.4s", out("v0") _);

                        // vmulps ymm0 operates on 8 f32 lanes — exercises the 256-bit
                        // FP multiply port (port 0 on Coffee Lake) matching the
                        // production hot path width.
                        #[cfg(target_arch = "x86_64")]
                        asm!("vmulps ymm0, ymm0, ymm0", out("ymm0") _);
                    }
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
    }

    println!("\nPhase 2: Executing 1 Billion Blocks per Core...");
    let start_time = Instant::now();
    let mut main_handles = vec![];

    for t_id in 0..num_cores {
        main_handles.push(thread::spawn(move || {
            unsafe {
                #[cfg(target_arch = "aarch64")]
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

                // AVX2: 8 independent vmulps ymm each iteration.
                // ymm0-ymm7 are 256-bit (8×f32); Coffee Lake has 2 FP multiply
                // execution units (ports 0 and 1), so interleaving 8 independent
                // chains keeps both ports saturated.
                #[cfg(target_arch = "x86_64")]
                for _ in 0..(ITERATIONS_PER_THREAD / 128) {
                    asm!(
                        ".rept 128",
                        "vmulps ymm0, ymm0, ymm0", "vmulps ymm1, ymm1, ymm1",
                        "vmulps ymm2, ymm2, ymm2", "vmulps ymm3, ymm3, ymm3",
                        "vmulps ymm4, ymm4, ymm4", "vmulps ymm5, ymm5, ymm5",
                        "vmulps ymm6, ymm6, ymm6", "vmulps ymm7, ymm7, ymm7",
                        ".endr",
                        out("ymm0") _, out("ymm1") _, out("ymm2") _, out("ymm3") _,
                        out("ymm4") _, out("ymm5") _, out("ymm6") _, out("ymm7") _,
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

    // AVX2: 8 f32 lanes per ymm × 8 registers per iteration.
    // NEON:  4 f32 lanes per v-reg  × 8 registers per iteration.
    #[cfg(target_arch = "x86_64")]
    let ops_per_iter = 8.0_f64;  // ymm = 8×f32
    #[cfg(target_arch = "aarch64")]
    let ops_per_iter = 4.0_f64;  // v-reg = 4×f32
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let ops_per_iter = 1.0_f64;

    let total_ops = (num_cores * ITERATIONS_PER_THREAD) as f64 * ops_per_iter;

    println!("-------------------------------------");
    println!("Total Wall Time:  {:?}", duration);
    println!("Total Operations: {} Billion", total_ops / 1e9);
    println!("Total Throughput: {:.2} Billion Ops/sec", (total_ops / 1e9) / duration.as_secs_f64());
    println!("1M Ops Latency:   {:.4} ns", (duration.as_nanos() as f64 / total_ops) * 1_000_000.0);
}
