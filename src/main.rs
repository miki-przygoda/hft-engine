use std::arch::asm;
use std::time::Instant;

const ITERATIONS: usize = 1_000_000_000;

fn main() {
    println!("--- Cross-Platform Hardware Bench ---");

    #[cfg(target_arch = "aarch64")]
    println!("Target: Apple Silicon (NEON 128-bit)");

    #[cfg(target_arch = "x86_64")]
    println!("Target: Intel/AMD (AVX-512 512-bit)");

    println!("Warming up core...");
    for _ in 0..100_000_000 {
        unsafe {
            #[cfg(target_arch = "aarch64")]
            asm!("fmul v0.4s, v0.4s, v0.4s", out("v0") _);
            #[cfg(target_arch = "x86_64")]
            asm!("vmulps xmm0, xmm0, xmm0", out("xmm0") _);
        }
    }

    println!("Executing {} billion blocks...", ITERATIONS / 1_000_000_000);
    let start = Instant::now();

    for _ in 0..(ITERATIONS / 128) {
        unsafe {
            run_compute_block();
        }
    }

    let duration = start.elapsed();
    let ops_per_instr = if cfg!(target_arch = "x86_64") { 16 } else { 4 };
    let total_ops = ITERATIONS as f64 * ops_per_instr as f64;

    println!("-------------------------------------");
    println!("Total Time:   {:?}", duration);
    println!("Thruput:      {:.2} Billion Ops/sec", (total_ops / 1e9) / duration.as_secs_f64());
    println!("1M Ops Time:  {:.4} ns", (duration.as_nanos() as f64 / total_ops) * 1_000_000.0);
}

#[inline(always)]
unsafe fn run_compute_block() {
    unsafe {
        #[cfg(target_arch = "aarch64")]
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

        #[cfg(target_arch = "x86_64")]
        asm!(
        ".rept 128",
        "vmulps zmm0, zmm0, zmm0", "vmulps zmm1, zmm1, zmm1",
        "vmulps zmm2, zmm2, zmm2", "vmulps zmm3, zmm3, zmm3",
        "vmulps zmm4, zmm4, zmm4", "vmulps zmm5, zmm5, zmm5",
        "vmulps zmm6, zmm6, zmm6", "vmulps zmm7, zmm7, zmm7",
        ".endr",
        out("zmm0") _, out("zmm1") _, out("zmm2") _, out("zmm3") _,
        out("zmm4") _, out("zmm5") _, out("zmm6") _, out("zmm7") _,
        );
    }
}