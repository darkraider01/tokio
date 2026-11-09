//! Benchmark harness for evaluating standard Tokio UDP vs Batched UDP (`sendmmsg`/`recvmmsg`).

#![cfg(target_os = "linux")]

use std::arch::x86_64::_rdtsc;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::udp_batch::{Datagram, InboundDatagram, UdpSocketBatchExt};
use tokio::net::UdpSocket;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Baseline,
    Batched(usize),
}

#[derive(Debug, Clone)]
pub struct BenchResult {
    pub mode: Mode,
    pub packet_size: usize,
    pub concurrency: usize,
    pub elapsed: Duration,
    pub packets_sent: u64,
    pub packets_recv: u64,
    pub bytes_recv: u64,
    pub syscalls_sent: u64,
    pub syscalls_recv: u64,
    pub pps: f64,
    pub mbps: f64,
    pub user_cpu_pct: f64,
    pub sys_cpu_pct: f64,
    pub total_cpu_pct: f64,
    pub cycles_per_pkt: f64,
    pub syscalls_per_pkt: f64,
    pub packet_loss_pct: f64,
    pub p50_rtt_us: f64,
    pub p95_rtt_us: f64,
    pub p99_rtt_us: f64,
    pub peak_rss_kb: i64,
    pub vol_cs: i64,
    pub invol_cs: i64,
}

fn get_rusage() -> libc::rusage {
    unsafe {
        let mut r = std::mem::MaybeUninit::<libc::rusage>::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, r.as_mut_ptr());
        r.assume_init()
    }
}

fn timeval_to_duration(tv: libc::timeval) -> Duration {
    Duration::from_secs(tv.tv_sec as u64) + Duration::from_micros(tv.tv_usec as u64)
}

/// Runs a high-throughput streaming benchmark for the specified configuration.
pub async fn run_throughput_bench(
    mode: Mode,
    packet_size: usize,
    concurrency: usize,
    duration: Duration,
) -> io::Result<BenchResult> {
    // Sockets setup: `concurrency` pairs of sockets.
    let mut receivers = Vec::with_capacity(concurrency);
    let mut receiver_addrs = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let r = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
        let addr = r.local_addr()?;
        receivers.push(r);
        receiver_addrs.push(addr);
    }

    let mut senders = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let s = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
        senders.push(s);
    }

    let running = Arc::new(AtomicBool::new(true));
    let total_sent = Arc::new(AtomicU64::new(0));
    let total_recv = Arc::new(AtomicU64::new(0));
    let total_send_syscalls = Arc::new(AtomicU64::new(0));
    let total_recv_syscalls = Arc::new(AtomicU64::new(0));

    // Warm-up phase (200ms)
    // Senders and receivers run briefly to prime page tables, socket buffers, and CPU caches.
    let warmup_running = Arc::new(AtomicBool::new(true));
    let mut warmup_handles = Vec::new();
    for i in 0..concurrency {
        let s = senders[i].clone();
        let target = receiver_addrs[i];
        let w_run = warmup_running.clone();
        warmup_handles.push(tokio::spawn(async move {
            let buf = vec![0xABu8; packet_size];
            while w_run.load(Ordering::Relaxed) {
                let _ = s.send_to(&buf, target).await;
                tokio::task::yield_now().await;
            }
        }));
        let r = receivers[i].clone();
        let w_run2 = warmup_running.clone();
        warmup_handles.push(tokio::spawn(async move {
            let mut buf = vec![0u8; packet_size + 64];
            while w_run2.load(Ordering::Relaxed) {
                let _ = r.recv_from(&mut buf).await;
            }
        }));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    warmup_running.store(false, Ordering::Relaxed);
    for h in warmup_handles {
        h.abort();
    }
    // Drain any remaining warmup packets
    for r in &receivers {
        let mut drain_buf = [0u8; 2048];
        while r.try_recv_from(&mut drain_buf).is_ok() {}
    }

    // Capture baseline system state before the measurement window
    let rusage_start = get_rusage();
    let tsc_start = unsafe { _rdtsc() };
    let start_time = Instant::now();

    // Spawn receiver tasks
    let mut recv_handles = Vec::with_capacity(concurrency);
    for i in 0..concurrency {
        let r = receivers[i].clone();
        let run = running.clone();
        let t_recv = total_recv.clone();
        let t_sys = total_recv_syscalls.clone();

        recv_handles.push(tokio::spawn(async move {
            let mut local_recv = 0u64;
            let mut local_sys = 0u64;

            match mode {
                Mode::Baseline => {
                    let mut buf = vec![0u8; packet_size + 64];
                    while run.load(Ordering::Relaxed) {
                        match r.recv_from(&mut buf).await {
                            Ok(_) => {
                                local_recv += 1;
                                local_sys += 1;
                            }
                            Err(_) => break,
                        }
                    }
                }
                Mode::Batched(batch_size) => {
                    let mut buffers = vec![vec![0u8; packet_size + 64]; batch_size];
                    while run.load(Ordering::Relaxed) {
                        let mut inbound: Vec<InboundDatagram<'_>> = buffers
                            .iter_mut()
                            .map(|b| InboundDatagram::new(b.as_mut_slice()))
                            .collect();
                        match r.recv_many(&mut inbound).await {
                            Ok(n) => {
                                local_recv += n as u64;
                                local_sys += 1;
                            }
                            Err(_) => break,
                        }
                    }
                }
            }

            t_recv.fetch_add(local_recv, Ordering::Relaxed);
            t_sys.fetch_add(local_sys, Ordering::Relaxed);
        }));
    }

    // Spawn sender tasks
    let mut send_handles = Vec::with_capacity(concurrency);
    for i in 0..concurrency {
        let s = senders[i].clone();
        let target = receiver_addrs[i];
        let run = running.clone();
        let t_sent = total_sent.clone();
        let t_sys = total_send_syscalls.clone();

        send_handles.push(tokio::spawn(async move {
            let mut local_sent = 0u64;
            let mut local_sys = 0u64;

            match mode {
                Mode::Baseline => {
                    let payload = vec![0x42u8; packet_size];
                    while run.load(Ordering::Relaxed) {
                        match s.send_to(&payload, target).await {
                            Ok(_) => {
                                local_sent += 1;
                                local_sys += 1;
                            }
                            Err(_) => break,
                        }
                        tokio::task::yield_now().await;
                    }
                }
                Mode::Batched(batch_size) => {
                    let payload = vec![0x42u8; packet_size];
                    let batch: Vec<Datagram<'_>> = (0..batch_size)
                        .map(|_| Datagram::to_addr(&payload, target))
                        .collect();
                    while run.load(Ordering::Relaxed) {
                        match s.send_many(&batch).await {
                            Ok(n) => {
                                local_sent += n as u64;
                                local_sys += 1;
                            }
                            Err(_) => break,
                        }
                        tokio::task::yield_now().await;
                    }
                }
            }

            t_sent.fetch_add(local_sent, Ordering::Relaxed);
            t_sys.fetch_add(local_sys, Ordering::Relaxed);
        }));
    }

    // Run measurement duration
    tokio::time::sleep(duration).await;
    running.store(false, Ordering::Relaxed);

    for h in send_handles {
        let _ = h.await;
    }
    // Give receivers a moment to receive in-flight packets, then abort
    tokio::time::sleep(Duration::from_millis(15)).await;
    for h in recv_handles {
        h.abort();
    }
    // Drain any remaining buffered packets
    for r in &receivers {
        let mut drain_buf = [0u8; 2048];
        while r.try_recv_from(&mut drain_buf).is_ok() {
            total_recv.fetch_add(1, Ordering::Relaxed);
        }
    }

    let elapsed = start_time.elapsed();
    let tsc_end = unsafe { _rdtsc() };
    let rusage_end = get_rusage();

    let sent = total_sent.load(Ordering::Relaxed);
    let recv = total_recv.load(Ordering::Relaxed);
    let send_syscalls = total_send_syscalls.load(Ordering::Relaxed);
    let recv_syscalls = total_recv_syscalls.load(Ordering::Relaxed);
    let total_syscalls = send_syscalls + recv_syscalls;

    let utime_diff = timeval_to_duration(rusage_end.ru_utime)
        .saturating_sub(timeval_to_duration(rusage_start.ru_utime));
    let stime_diff = timeval_to_duration(rusage_end.ru_stime)
        .saturating_sub(timeval_to_duration(rusage_start.ru_stime));
    let elapsed_sec = elapsed.as_secs_f64();

    let user_cpu_pct = (utime_diff.as_secs_f64() / elapsed_sec) * 100.0;
    let sys_cpu_pct = (stime_diff.as_secs_f64() / elapsed_sec) * 100.0;
    let total_cpu_pct = user_cpu_pct + sys_cpu_pct;

    let pps = (recv as f64) / elapsed_sec;
    let bytes_recv = recv * (packet_size as u64);
    let mbps = (bytes_recv as f64) / (1024.0 * 1024.0 * elapsed_sec);

    let total_cycles = tsc_end.saturating_sub(tsc_start);
    let cycles_per_pkt = if recv > 0 {
        (total_cycles as f64) / (recv as f64)
    } else {
        0.0
    };
    let syscalls_per_pkt = if recv > 0 {
        (total_syscalls as f64) / (recv as f64)
    } else {
        0.0
    };

    let packet_loss_pct = if sent > 0 {
        let lost = sent.saturating_sub(recv);
        (lost as f64) / (sent as f64) * 100.0
    } else {
        0.0
    };

    let vol_cs = rusage_end.ru_nvcsw - rusage_start.ru_nvcsw;
    let invol_cs = rusage_end.ru_nivcsw - rusage_start.ru_nivcsw;
    let peak_rss_kb = rusage_end.ru_maxrss;

    // Measure RTT latency percentiles using an isolated ping-pong sample
    let (p50, p95, p99) = measure_latency(mode, packet_size).await?;

    Ok(BenchResult {
        mode,
        packet_size,
        concurrency,
        elapsed,
        packets_sent: sent,
        packets_recv: recv,
        bytes_recv,
        syscalls_sent: send_syscalls,
        syscalls_recv: recv_syscalls,
        pps,
        mbps,
        user_cpu_pct,
        sys_cpu_pct,
        total_cpu_pct,
        cycles_per_pkt,
        syscalls_per_pkt,
        packet_loss_pct,
        p50_rtt_us: p50,
        p95_rtt_us: p95,
        p99_rtt_us: p99,
        peak_rss_kb,
        vol_cs,
        invol_cs,
    })
}

/// Measures round-trip time (RTT) latency percentiles for the configuration.
async fn measure_latency(mode: Mode, packet_size: usize) -> io::Result<(f64, f64, f64)> {
    let server = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    let server_addr = server.local_addr()?;
    let client = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);

    let stop_echo = Arc::new(AtomicBool::new(false));
    let stop_echo_clone = stop_echo.clone();

    // Spawn server echo loop
    let s_clone = server.clone();
    let echo_handle = tokio::spawn(async move {
        match mode {
            Mode::Baseline => {
                let mut buf = vec![0u8; packet_size + 64];
                while !stop_echo_clone.load(Ordering::Relaxed) {
                    if let Ok((len, src)) = s_clone.recv_from(&mut buf).await {
                        let _ = s_clone.send_to(&buf[..len], src).await;
                    }
                }
            }
            Mode::Batched(batch_size) => {
                let mut buffers = vec![vec![0u8; packet_size + 64]; batch_size];
                while !stop_echo_clone.load(Ordering::Relaxed) {
                    let mut inbound: Vec<InboundDatagram<'_>> = buffers
                        .iter_mut()
                        .map(|b| InboundDatagram::new(b.as_mut_slice()))
                        .collect();
                    if let Ok(n) = s_clone.recv_many(&mut inbound).await {
                        let outbound: Vec<Datagram<'_>> = inbound[..n]
                            .iter()
                            .filter_map(|d| d.source.map(|addr| Datagram::to_addr(d.payload(), addr)))
                            .collect();
                        let _ = s_clone.send_many(&outbound).await;
                    }
                }
            }
        }
    });

    let mut samples = Vec::with_capacity(300);
    let payload = vec![0x7Eu8; packet_size];
    let mut recv_buf = vec![0u8; packet_size + 64];

    // Measure 200 pings with timeout per ping
    for _ in 0..200 {
        let t0 = Instant::now();
        let ping_res = tokio::time::timeout(Duration::from_millis(50), async {
            match mode {
                Mode::Baseline => {
                    client.send_to(&payload, server_addr).await?;
                    let _ = client.recv_from(&mut recv_buf).await?;
                }
                Mode::Batched(_) => {
                    client.send_many(&[Datagram::to_addr(&payload, server_addr)]).await?;
                    let mut inbound = [InboundDatagram::new(&mut recv_buf)];
                    let _ = client.recv_many(&mut inbound).await?;
                }
            }
            Ok::<(), io::Error>(())
        })
        .await;

        if ping_res.is_ok() {
            let rtt = t0.elapsed();
            samples.push(rtt.as_secs_f64() * 1_000_000.0);
        }
    }

    echo_handle.abort();

    if samples.is_empty() {
        return Ok((0.0, 0.0, 0.0));
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p50 = samples[samples.len() * 50 / 100];
    let p95 = samples[samples.len() * 95 / 100];
    let p99 = samples[samples.len() * 99 / 100];

    Ok((p50, p95, p99))
}

fn print_header() {
    println!(
        "| Mode | PktSz | Conc | Pkts/sec | MByte/s | CPU% (U+S) | Cycles/Pkt | Syscalls/Pkt | Loss % | p50 (us) | p95 (us) | p99 (us) | Peak RSS |"
    );
    println!(
        "|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|"
    );
}

fn print_result(r: &BenchResult) {
    let mode_str = match r.mode {
        Mode::Baseline => "Baseline".to_string(),
        Mode::Batched(b) => format!("Batch-{}", b),
    };
    println!(
        "| {:<10} | {:>5} | {:>4} | {:>10.0} | {:>7.2} | {:>5.1}% ({:>4.1}+{:<4.1}) | {:>10.0} | {:>12.2} | {:>5.2}% | {:>8.1} | {:>8.1} | {:>8.1} | {:>7} KB |",
        mode_str,
        r.packet_size,
        r.concurrency,
        r.pps,
        r.mbps,
        r.total_cpu_pct,
        r.user_cpu_pct,
        r.sys_cpu_pct,
        r.cycles_per_pkt,
        r.syscalls_per_pkt,
        r.packet_loss_pct,
        r.p50_rtt_us,
        r.p95_rtt_us,
        r.p99_rtt_us,
        r.peak_rss_kb,
    );
}

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("=========================================================================================");
    println!("             TOKIO UDP BENCHMARK: BASELINE vs NATIVE LINUX BATCHED I/O                  ");
    println!("=========================================================================================");
    println!("Host: Linux 6.19 x86_64, 12 cores. Local loopback interface.");
    println!();

    let packet_sizes = [64, 512, 1200, 1472];
    let concurrencies = [1, 4, 16, 64];
    let batch_sizes = [1, 4, 8, 16, 32, 64];
    let duration = Duration::from_millis(600);

    // Section 1: Baseline Tokio across all Packet Sizes and Concurrencies
    println!("### PART 1: Baseline Tokio UDP Operations");
    print_header();
    for &pkt in &packet_sizes {
        for &conc in &concurrencies {
            let res = run_throughput_bench(Mode::Baseline, pkt, conc, duration).await?;
            print_result(&res);
        }
    }
    println!();

    // Section 2: Batch Size Evaluation (64-byte packets, 16x16 concurrency)
    println!("### PART 2: Batch Size Sweep (Packet Size: 64B, Concurrency: 16x16)");
    print_header();
    let b_res = run_throughput_bench(Mode::Baseline, 64, 16, duration).await?;
    print_result(&b_res);
    for &bs in &batch_sizes {
        let res = run_throughput_bench(Mode::Batched(bs), 64, 16, duration).await?;
        print_result(&res);
    }
    println!();

    // Section 3: Batch Size Evaluation (1472-byte packets, 16x16 concurrency)
    println!("### PART 3: Batch Size Sweep (Packet Size: 1472B, Concurrency: 16x16)");
    print_header();
    let b_res1472 = run_throughput_bench(Mode::Baseline, 1472, 16, duration).await?;
    print_result(&b_res1472);
    for &bs in &batch_sizes {
        let res = run_throughput_bench(Mode::Batched(bs), 1472, 16, duration).await?;
        print_result(&res);
    }
    println!();

    // Section 4: Concurrency Scaling with Batch-32 (64-byte packets)
    println!("### PART 4: Concurrency Scaling (Packet Size: 64B, Batch: 32 vs Baseline)");
    print_header();
    for &conc in &concurrencies {
        let b = run_throughput_bench(Mode::Baseline, 64, conc, duration).await?;
        print_result(&b);
        let batched = run_throughput_bench(Mode::Batched(32), 64, conc, duration).await?;
        print_result(&batched);
    }
    println!();

    // Section 5: Concurrency Scaling with Batch-32 (1472-byte packets)
    println!("### PART 5: Concurrency Scaling (Packet Size: 1472B, Batch: 32 vs Baseline)");
    print_header();
    for &conc in &concurrencies {
        let b = run_throughput_bench(Mode::Baseline, 1472, conc, duration).await?;
        print_result(&b);
        let batched = run_throughput_bench(Mode::Batched(32), 1472, conc, duration).await?;
        print_result(&batched);
    }
    println!();

    println!("Benchmark execution completed successfully.");
    Ok(())
}
