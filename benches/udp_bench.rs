//! Benchmark harness for evaluating standard Tokio UDP vs Grouped Nonblocking vs Batched UDP (`sendmmsg`/`recvmmsg`).

#![cfg(target_os = "linux")]

use std::io;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::udp_batch::{Datagram, InboundDatagram, UdpSocketBatchExt};
use tokio::net::UdpSocket;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Baseline,
    Grouped(usize),
    Batched(usize),
}

#[derive(Debug, Clone)]
pub struct BenchResult {
    pub mode: Mode,
    pub packet_size: usize,
    pub concurrency: usize,
    pub elapsed: Duration,
    pub packets_sent: u64,
    pub packets_recv_timed: u64,
    pub packets_drained: u64,
    pub total_recv: u64,
    pub bytes_recv: u64,
    /// Number of successful socket I/O operations (sendto or sendmmsg calls).
    pub successful_io_sends: u64,
    /// Number of successful socket I/O operations (recvfrom or recvmmsg calls).
    pub successful_io_recvs: u64,
    pub pps: f64,
    pub mbps: f64,
    pub user_cpu_pct: f64,
    pub sys_cpu_pct: f64,
    pub total_cpu_pct: f64,
    /// Aggregate CPU time (user + system) consumed per delivered packet, in nanoseconds.
    pub cpu_ns_per_pkt: f64,
    /// Proxy metric for system call efficiency: successful socket operations per delivered packet.
    pub io_ops_per_pkt: f64,
    pub packet_loss_pct: f64,
    pub p50_rtt_us: f64,
    pub p95_rtt_us: f64,
    pub p99_rtt_us: f64,
    pub latency_samples: usize,
    pub latency_timeouts: usize,
    pub latency_invalid: usize,
    pub peak_rss_kb: i64,
    pub vol_cs: i64,
    pub invol_cs: i64,
    pub effective_rcvbuf: usize,
    pub effective_sndbuf: usize,
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

fn configure_socket(sock: &UdpSocket) -> io::Result<(usize, usize)> {
    let buf_size: libc::c_int = 2 * 1024 * 1024;
    unsafe {
        let ret = libc::setsockopt(
            sock.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &buf_size as *const _ as *const libc::c_void,
            std::mem::size_of_val(&buf_size) as libc::socklen_t,
        );
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }

        let ret = libc::setsockopt(
            sock.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &buf_size as *const _ as *const libc::c_void,
            std::mem::size_of_val(&buf_size) as libc::socklen_t,
        );
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }

        let mut actual_rcv: libc::c_int = 0;
        let mut len = std::mem::size_of_val(&actual_rcv) as libc::socklen_t;
        let ret = libc::getsockopt(
            sock.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &mut actual_rcv as *mut _ as *mut libc::c_void,
            &mut len,
        );
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }

        let mut actual_snd: libc::c_int = 0;
        let mut len = std::mem::size_of_val(&actual_snd) as libc::socklen_t;
        let ret = libc::getsockopt(
            sock.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &mut actual_snd as *mut _ as *mut libc::c_void,
            &mut len,
        );
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }

        Ok((actual_rcv as usize, actual_snd as usize))
    }
}

/// Runs a high-throughput streaming benchmark comparing Baseline, Grouped, and Batched UDP.
pub async fn run_throughput_bench(
    mode: Mode,
    packet_size: usize,
    concurrency: usize,
    duration: Duration,
) -> io::Result<BenchResult> {
    // Sockets setup: `concurrency` pairs of sockets.
    let mut receivers = Vec::with_capacity(concurrency);
    let mut receiver_addrs = Vec::with_capacity(concurrency);
    let mut effective_rcvbuf = 0;
    let mut effective_sndbuf = 0;
    for _ in 0..concurrency {
        let r = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
        let (rcv, snd) = configure_socket(&r)?;
        effective_rcvbuf = rcv;
        effective_sndbuf = snd;
        let addr = r.local_addr()?;
        receivers.push(r);
        receiver_addrs.push(addr);
    }

    let mut senders = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let s = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
        let _ = configure_socket(&s)?;
        senders.push(s);
    }

    let running = Arc::new(AtomicBool::new(true));
    let (shutdown_tx, _) = tokio::sync::watch::channel(false);
    let start_barrier = Arc::new(tokio::sync::Barrier::new(concurrency * 2 + 2));

    // Warm-up phase (100ms): prime memory, kernel tables, and CPU caches.
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

    // Spawn receiver tasks with plain local integer counters
    let mut recv_handles = Vec::with_capacity(concurrency);
    for i in 0..concurrency {
        let r = receivers[i].clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let barrier = start_barrier.clone();

        recv_handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut local_packets = 0u64;
            let mut local_io_ops = 0u64;

            match mode {
                Mode::Baseline => {
                    let mut buf = vec![0u8; packet_size + 64];
                    loop {
                        tokio::select! {
                            biased;
                            _ = shutdown_rx.changed() => break,
                            res = r.recv_from(&mut buf) => match res {
                                Ok(_) => {
                                    local_packets += 1;
                                    local_io_ops += 1;
                                }
                                Err(_) => break,
                            }
                        }
                    }
                }
                Mode::Grouped(batch_size) => {
                    let mut buf = vec![0u8; packet_size + 64];
                    'outer: loop {
                        tokio::select! {
                            biased;
                            _ = shutdown_rx.changed() => break 'outer,
                            res = r.readable() => {
                                if res.is_err() {
                                    break 'outer;
                                }
                                let mut count = 0;
                                while count < batch_size {
                                    match r.try_recv_from(&mut buf) {
                                        Ok(_) => {
                                            count += 1;
                                            local_packets += 1;
                                            local_io_ops += 1;
                                        }
                                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                            break;
                                        }
                                        Err(_) => break 'outer,
                                    }
                                }
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                }
                Mode::Batched(batch_size) => {
                    let mut raw_buffers = vec![vec![0u8; packet_size + 64]; batch_size];
                    let mut inbound: Vec<InboundDatagram<'_>> = raw_buffers
                        .iter_mut()
                        .map(|b| InboundDatagram::new(b.as_mut_slice()))
                        .collect();
                    loop {
                        tokio::select! {
                            biased;
                            _ = shutdown_rx.changed() => break,
                            res = r.recv_many(&mut inbound) => match res {
                                Ok(n) => {
                                    local_packets += n as u64;
                                    local_io_ops += 1;
                                    tokio::task::yield_now().await;
                                }
                                Err(_) => break,
                            }
                        }
                    }
                }
            }
            (local_packets, local_io_ops)
        }));
    }

    // Spawn sender tasks with plain local integer counters
    let mut send_handles = Vec::with_capacity(concurrency);
    for i in 0..concurrency {
        let s = senders[i].clone();
        let target = receiver_addrs[i];
        let run = running.clone();
        let barrier = start_barrier.clone();

        send_handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut local_packets = 0u64;
            let mut local_io_ops = 0u64;

            match mode {
                Mode::Baseline => {
                    let payload = vec![0x42u8; packet_size];
                    while run.load(Ordering::Relaxed) {
                        match s.send_to(&payload, target).await {
                            Ok(_) => {
                                local_packets += 1;
                                local_io_ops += 1;
                            }
                            Err(_) => break,
                        }
                    }
                }
                Mode::Grouped(batch_size) => {
                    let payload = vec![0x42u8; packet_size];
                    while run.load(Ordering::Relaxed) {
                        if s.writable().await.is_err() {
                            break;
                        }
                        let mut sent = 0;
                        while sent < batch_size && run.load(Ordering::Relaxed) {
                            match s.try_send_to(&payload, target) {
                                Ok(_) => {
                                    sent += 1;
                                    local_packets += 1;
                                    local_io_ops += 1;
                                }
                                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                                    break;
                                }
                                Err(_) => break,
                            }
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
                                local_packets += n as u64;
                                local_io_ops += 1;
                            }
                            Err(_) => break,
                        }
                        tokio::task::yield_now().await;
                    }
                }
            }
            (local_packets, local_io_ops)
        }));
    }

    // Spawn concurrent latency probe under background load
    let probe_server = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    let probe_addr = probe_server.local_addr()?;
    let probe_client = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    let probe_run1 = running.clone();
    let s_probe_echo = probe_server.clone();
    let probe_echo_h = tokio::spawn(async move {
        let mut buf = [0u8; 512];
        while probe_run1.load(Ordering::Relaxed) {
            if let Ok((n, src)) = s_probe_echo.recv_from(&mut buf).await {
                let _ = s_probe_echo.send_to(&buf[..n], src).await;
            }
        }
    });

    let probe_run2 = running.clone();
    let probe_barrier = start_barrier.clone();
    let probe_h = tokio::spawn(async move {
        probe_barrier.wait().await;
        let mut samples = Vec::with_capacity(300);
        let mut timeouts = 0usize;
        let mut invalid_replies = 0usize;
        let mut buf = [0u8; 512];
        let mut ping_payload = [0x5Au8; 64];
        let mut seq = 0u64;

        while probe_run2.load(Ordering::Relaxed) {
            seq += 1;
            ping_payload[0..8].copy_from_slice(&seq.to_be_bytes());

            let t0 = Instant::now();
            let ping_res = tokio::time::timeout(Duration::from_millis(25), async {
                probe_client.send_to(&ping_payload, probe_addr).await?;
                loop {
                    let (n, _) = probe_client.recv_from(&mut buf).await?;
                    if n >= 8 {
                        let resp_seq = u64::from_be_bytes(buf[0..8].try_into().unwrap());
                        if resp_seq == seq {
                            return Ok::<Duration, io::Error>(t0.elapsed());
                        } else if resp_seq < seq {
                            // Stale reply from an earlier timed-out request; drop and continue reading
                            continue;
                        } else {
                            // Unexpected future sequence
                            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid probe seq"));
                        }
                    } else {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated probe reply"));
                    }
                }
            })
            .await;

            match ping_res {
                Ok(Ok(rtt)) => {
                    samples.push(rtt.as_secs_f64() * 1_000_000.0);
                }
                Ok(Err(_)) => {
                    invalid_replies += 1;
                }
                Err(_) => {
                    timeouts += 1;
                }
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        (samples, timeouts, invalid_replies)
    });

    // Synchronize workers so all tasks release simultaneously at the start of measurement
    let rusage_start = get_rusage();
    let start_time = Instant::now();
    start_barrier.wait().await;

    // Run measurement duration
    tokio::time::sleep(duration).await;
    running.store(false, Ordering::Relaxed);

    let mut sent = 0u64;
    let mut send_io_ops = 0u64;
    for h in send_handles {
        if let Ok((pkts, ops)) = h.await {
            sent += pkts;
            send_io_ops += ops;
        }
    }

    // Signal cooperative shutdown to all receivers so they exit without aborting
    let _ = shutdown_tx.send(true);

    let mut timed_recv = 0u64;
    let mut recv_io_ops = 0u64;
    for h in recv_handles {
        if let Ok((pkts, ops)) = h.await {
            timed_recv += pkts;
            recv_io_ops += ops;
        }
    }

    let (mut latency_samples, latency_timeouts, latency_invalid) =
        match tokio::time::timeout(Duration::from_millis(100), probe_h).await {
            Ok(Ok(stats)) => stats,
            _ => (Vec::new(), 0, 0),
        };
    probe_echo_h.abort();

    // Drain any remaining buffered packets from the kernel queues
    let mut drained_recv = 0u64;
    for r in &receivers {
        let mut drain_buf = [0u8; 2048];
        while r.try_recv_from(&mut drain_buf).is_ok() {
            drained_recv += 1;
        }
    }

    let elapsed = start_time.elapsed();
    let rusage_end = get_rusage();

    let total_recv = timed_recv + drained_recv;
    recv_io_ops += drained_recv;
    let total_io_ops = send_io_ops + recv_io_ops;

    let utime_diff = timeval_to_duration(rusage_end.ru_utime)
        .saturating_sub(timeval_to_duration(rusage_start.ru_utime));
    let stime_diff = timeval_to_duration(rusage_end.ru_stime)
        .saturating_sub(timeval_to_duration(rusage_start.ru_stime));
    let elapsed_sec = elapsed.as_secs_f64();

    let user_cpu_pct = (utime_diff.as_secs_f64() / elapsed_sec) * 100.0;
    let sys_cpu_pct = (stime_diff.as_secs_f64() / elapsed_sec) * 100.0;
    let total_cpu_pct = user_cpu_pct + sys_cpu_pct;

    let pps = (timed_recv as f64) / elapsed_sec;
    let bytes_recv = timed_recv * (packet_size as u64);
    let mbps = (bytes_recv as f64) / (1024.0 * 1024.0 * elapsed_sec);

    let total_cpu_time = utime_diff + stime_diff;
    let cpu_ns_per_pkt = if total_recv > 0 {
        (total_cpu_time.as_nanos() as f64) / (total_recv as f64)
    } else {
        0.0
    };
    let io_ops_per_pkt = if total_recv > 0 {
        (total_io_ops as f64) / (total_recv as f64)
    } else {
        0.0
    };

    let packet_loss_pct = if sent > 0 {
        let lost = sent.saturating_sub(total_recv);
        (lost as f64) / (sent as f64) * 100.0
    } else {
        0.0
    };

    let vol_cs = rusage_end.ru_nvcsw - rusage_start.ru_nvcsw;
    let invol_cs = rusage_end.ru_nivcsw - rusage_start.ru_nivcsw;
    let peak_rss_kb = rusage_end.ru_maxrss;

    let (p50, p95, p99) = if !latency_samples.is_empty() {
        latency_samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        (
            latency_samples[latency_samples.len() * 50 / 100],
            latency_samples[latency_samples.len() * 95 / 100],
            latency_samples[latency_samples.len() * 99 / 100],
        )
    } else {
        (0.0, 0.0, 0.0)
    };

    Ok(BenchResult {
        mode,
        packet_size,
        concurrency,
        elapsed,
        packets_sent: sent,
        packets_recv_timed: timed_recv,
        packets_drained: drained_recv,
        total_recv,
        bytes_recv,
        successful_io_sends: send_io_ops,
        successful_io_recvs: recv_io_ops,
        pps,
        mbps,
        user_cpu_pct,
        sys_cpu_pct,
        total_cpu_pct,
        cpu_ns_per_pkt,
        io_ops_per_pkt,
        packet_loss_pct,
        p50_rtt_us: p50,
        p95_rtt_us: p95,
        p99_rtt_us: p99,
        latency_samples: latency_samples.len(),
        latency_timeouts,
        latency_invalid,
        peak_rss_kb,
        vol_cs,
        invol_cs,
        effective_rcvbuf,
        effective_sndbuf,
    })
}

fn print_header() {
    println!(
        "| Mode | PktSz | Conc | Pkts/sec | MByte/s | CPU% (U+S) | CPU-ns/Pkt | IO-Ops/Pkt | Loss % | p50 (us) | p95 (us) | p99 (us) | Pings (TO) | Peak RSS |"
    );
    println!("|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|");
}

fn print_result(r: &BenchResult) {
    let mode_str = match r.mode {
        Mode::Baseline => "Baseline".to_string(),
        Mode::Grouped(b) => format!("Group-{}", b),
        Mode::Batched(b) => format!("Batch-{}", b),
    };
    let pings_str = format!("{}/{}", r.latency_samples, r.latency_timeouts);
    println!(
        "| {:<10} | {:>5} | {:>4} | {:>10.0} | {:>7.2} | {:>5.1}% ({:>4.1}+{:<4.1}) | {:>10.0} | {:>12.2} | {:>5.2}% | {:>8.1} | {:>8.1} | {:>8.1} | {:>10} | {:>7} KB |",
        mode_str,
        r.packet_size,
        r.concurrency,
        r.pps,
        r.mbps,
        r.total_cpu_pct,
        r.user_cpu_pct,
        r.sys_cpu_pct,
        r.cpu_ns_per_pkt,
        r.io_ops_per_pkt,
        r.packet_loss_pct,
        r.p50_rtt_us,
        r.p95_rtt_us,
        r.p99_rtt_us,
        pings_str,
        r.peak_rss_kb,
    );
}

#[tokio::main]
async fn main() -> io::Result<()> {
    println!(
        "========================================================================================="
    );
    println!(
        "        TOKIO UDP BENCHMARK: BASELINE vs GROUPED NONBLOCKING vs NATIVE LINUX BATCH       "
    );
    println!(
        "========================================================================================="
    );
    let test_sock = UdpSocket::bind("127.0.0.1:0").await?;
    let (eff_rcv, eff_snd) = configure_socket(&test_sock)?;
    println!(
        "Host: Linux x86_64, local loopback. Effective socket buffers: SO_RCVBUF = {} bytes ({} KB), SO_SNDBUF = {} bytes ({} KB).",
        eff_rcv,
        eff_rcv / 1024,
        eff_snd,
        eff_snd / 1024
    );
    println!();

    let packet_sizes = [64, 512, 1200, 1472];
    let concurrencies = [1, 4, 16, 64];
    let batch_sizes = [4, 8, 16, 32, 64];
    let duration = Duration::from_millis(400);

    // Section 1: Baseline Tokio across all Packet Sizes and Concurrencies
    println!("### PART 1: Baseline Tokio UDP Operations (Single-Packet async send_to/recv_from)");
    print_header();
    for &pkt in &packet_sizes {
        for &conc in &concurrencies {
            let res = run_throughput_bench(Mode::Baseline, pkt, conc, duration).await?;
            print_result(&res);
        }
    }
    println!();

    // Section 2: Three-Way Comparison: Baseline vs Grouped vs Batched (64B packets, 16x16 concurrency)
    println!("### PART 2: Three-Way Comparison (Packet Size: 64B, Concurrency: 16x16)");
    print_header();
    let b_res = run_throughput_bench(Mode::Baseline, 64, 16, duration).await?;
    print_result(&b_res);
    for &bs in &batch_sizes {
        let g_res = run_throughput_bench(Mode::Grouped(bs), 64, 16, duration).await?;
        print_result(&g_res);
        let res = run_throughput_bench(Mode::Batched(bs), 64, 16, duration).await?;
        print_result(&res);
    }
    println!();

    // Section 3: Three-Way Comparison: Baseline vs Grouped vs Batched (1472B packets, 16x16 concurrency)
    println!("### PART 3: Three-Way Comparison (Packet Size: 1472B, Concurrency: 16x16)");
    print_header();
    let b_res1472 = run_throughput_bench(Mode::Baseline, 1472, 16, duration).await?;
    print_result(&b_res1472);
    for &bs in &batch_sizes {
        let g_res = run_throughput_bench(Mode::Grouped(bs), 1472, 16, duration).await?;
        print_result(&g_res);
        let res = run_throughput_bench(Mode::Batched(bs), 1472, 16, duration).await?;
        print_result(&res);
    }
    println!();

    // Section 4: Concurrency Scaling (Packet Size: 64B): Baseline vs Group-32 vs Batch-32
    println!(
        "### PART 4: Concurrency Scaling (Packet Size: 64B: Baseline vs Group-32 vs Batch-32)"
    );
    print_header();
    for &conc in &concurrencies {
        let b = run_throughput_bench(Mode::Baseline, 64, conc, duration).await?;
        print_result(&b);
        let g = run_throughput_bench(Mode::Grouped(32), 64, conc, duration).await?;
        print_result(&g);
        let batched = run_throughput_bench(Mode::Batched(32), 64, conc, duration).await?;
        print_result(&batched);
    }
    println!();

    // Section 5: Concurrency Scaling (Packet Size: 1472B): Baseline vs Group-32 vs Batch-32
    println!(
        "### PART 5: Concurrency Scaling (Packet Size: 1472B: Baseline vs Group-32 vs Batch-32)"
    );
    print_header();
    for &conc in &concurrencies {
        let b = run_throughput_bench(Mode::Baseline, 1472, conc, duration).await?;
        print_result(&b);
        let g = run_throughput_bench(Mode::Grouped(32), 1472, conc, duration).await?;
        print_result(&g);
        let batched = run_throughput_bench(Mode::Batched(32), 1472, conc, duration).await?;
        print_result(&batched);
    }
    println!();

    println!("Benchmark execution completed successfully.");
    Ok(())
}
