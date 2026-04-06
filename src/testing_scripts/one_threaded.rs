use std::arch::asm;
use std::time::Instant;

const ITERATIONS: usize = 1_000_000_000;

#[derive(Clone, Copy)]
enum ComputeMode {
    #[cfg(target_arch = "x86_64")]
    Avx512,
    #[cfg(target_arch = "x86_64")]
    Avx2,
    #[cfg(target_arch = "x86_64")]
    Sse,
    #[cfg(target_arch = "aarch64")]
    Neon,
    Scalar,
}

fn main() {
    println!("--- Cross-Platform Hardware Bench ---");
    if let ComputeMode::Scalar = ComputeMode::Scalar {
        println!("Warning: No SIMD support detected, running in scalar mode. Performance will be very low.");
    }
    let mode = select_mode();

    #[cfg(target_arch = "aarch64")]
    println!("Target: Apple Silicon (NEON 128-bit)");

    #[cfg(target_arch = "x86_64")]
    println!("Target: Intel/AMD ({})", mode_name(mode));

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    println!("Target: generic scalar");

    println!("Warming up core...");
    for _ in 0..100_000_000 {
        warmup_step(mode);
    }

    println!("Executing {} billion blocks...", ITERATIONS / 1_000_000_000);
    let start = Instant::now();

    for _ in 0..(ITERATIONS / 128) {
        run_compute_block(mode);
    }

    let duration = start.elapsed();
    let ops_per_instr = ops_per_instruction(mode);
    let total_ops = ITERATIONS as f64 * ops_per_instr as f64;

    println!("-------------------------------------");
    println!("Total Time:   {:?}", duration);
    println!("Throughput:      {:.2} Billion Ops/sec", (total_ops / 1e9) / duration.as_secs_f64());
    println!("1M Ops Time:  {:.4} ns", (duration.as_nanos() as f64 / total_ops) * 1_000_000.0);
}

#[cfg(target_arch = "x86_64")]
fn mode_name(mode: ComputeMode) -> &'static str {
    match mode {
        ComputeMode::Avx512 => "AVX-512 (512-bit, 16×f32)",
        ComputeMode::Avx2   => "AVX2 (256-bit, 8×f32)",
        ComputeMode::Sse    => "SSE (128-bit, 4×f32)",
        ComputeMode::Scalar => "Scalar (no SIMD)",
    }
}

#[inline(always)]
fn select_mode() -> ComputeMode {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512f") {
            return ComputeMode::Avx512;
        }
        if std::is_x86_feature_detected!("avx2") {
            return ComputeMode::Avx2;
        }
        if std::is_x86_feature_detected!("sse") {
            return ComputeMode::Sse;
        }
        return ComputeMode::Scalar;
    }

    #[cfg(target_arch = "aarch64")]
    {
        return ComputeMode::Neon;
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        ComputeMode::Scalar
    }
}

#[inline(always)]
fn ops_per_instruction(mode: ComputeMode) -> usize {
    match mode {
        #[cfg(target_arch = "x86_64")]
        ComputeMode::Avx512 => 16,
        #[cfg(target_arch = "x86_64")]
        ComputeMode::Avx2 => 8,
        #[cfg(target_arch = "x86_64")]
        ComputeMode::Sse => 4,
        #[cfg(target_arch = "aarch64")]
        ComputeMode::Neon => 4,
        ComputeMode::Scalar => 1,
    }
}

#[inline(always)]
fn warmup_step(mode: ComputeMode) {
    unsafe {
        match mode {
            #[cfg(target_arch = "x86_64")]
            ComputeMode::Avx512 => warmup_avx512(),
            #[cfg(target_arch = "x86_64")]
            ComputeMode::Avx2 => warmup_avx2(),
            #[cfg(target_arch = "x86_64")]
            ComputeMode::Sse => warmup_sse(),
            #[cfg(target_arch = "aarch64")]
            ComputeMode::Neon => warmup_neon(),
            ComputeMode::Scalar => warmup_scalar(),
        }
    }
}

#[inline(always)]
fn run_compute_block(mode: ComputeMode) {
    unsafe {
        match mode {
            #[cfg(target_arch = "x86_64")]
            ComputeMode::Avx512 => run_compute_block_avx512(),
            #[cfg(target_arch = "x86_64")]
            ComputeMode::Avx2 => run_compute_block_avx2(),
            #[cfg(target_arch = "x86_64")]
            ComputeMode::Sse => run_compute_block_sse(),
            #[cfg(target_arch = "aarch64")]
            ComputeMode::Neon => run_compute_block_neon(),
            ComputeMode::Scalar => run_compute_block_scalar(),
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn warmup_avx512() {
    unsafe {
        asm!("vmulps zmm0, zmm0, zmm0", out("zmm0") _);
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[allow(dead_code)]
unsafe fn warmup_avx512() {}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn warmup_avx2() {
    unsafe {
        asm!("vmulps ymm0, ymm0, ymm0", out("ymm0") _);
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[allow(dead_code)]
unsafe fn warmup_avx2() {}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse")]
unsafe fn warmup_sse() {
    unsafe {
        asm!("mulps xmm0, xmm0", out("xmm0") _);
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[allow(dead_code)]
unsafe fn warmup_sse() {}

#[cfg(target_arch = "aarch64")]
unsafe fn warmup_neon() {
    unsafe {
        asm!("fmul v0.4s, v0.4s, v0.4s", out("v0") _);
    }
}

#[cfg(not(target_arch = "aarch64"))]
#[allow(dead_code)]
unsafe fn warmup_neon() {}

unsafe fn warmup_scalar() {
    unsafe {
        asm!("", options(nomem, nostack, preserves_flags));
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn run_compute_block_avx512() {
    unsafe {
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

#[cfg(not(target_arch = "x86_64"))]
#[allow(dead_code)]
unsafe fn run_compute_block_avx512() {}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn run_compute_block_avx2() {
    unsafe {
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

#[cfg(not(target_arch = "x86_64"))]
#[allow(dead_code)]
unsafe fn run_compute_block_avx2() {}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse")]
unsafe fn run_compute_block_sse() {
    unsafe {
        asm!(
            ".rept 128",
            "mulps xmm0, xmm0", "mulps xmm1, xmm1",
            "mulps xmm2, xmm2", "mulps xmm3, xmm3",
            "mulps xmm4, xmm4", "mulps xmm5, xmm5",
            "mulps xmm6, xmm6", "mulps xmm7, xmm7",
            ".endr",
            out("xmm0") _, out("xmm1") _, out("xmm2") _, out("xmm3") _,
            out("xmm4") _, out("xmm5") _, out("xmm6") _, out("xmm7") _,
        );
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[allow(dead_code)]
unsafe fn run_compute_block_sse() {}

#[cfg(target_arch = "aarch64")]
unsafe fn run_compute_block_neon() {
    unsafe {
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

#[cfg(not(target_arch = "aarch64"))]
#[allow(dead_code)]
unsafe fn run_compute_block_neon() {}

unsafe fn run_compute_block_scalar() {
    unsafe {
        asm!("", options(nomem, nostack, preserves_flags));
    }
}