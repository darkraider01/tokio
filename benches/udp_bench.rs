//! Benchmark harness for evaluating standard Tokio UDP vs Grouped Nonblocking vs Batched UDP (`sendmmsg`/`recvmmsg`).

#[cfg(target_os = "linux")]
mod linux {
    use std::io;
    use std::os::unix::io::AsRawFd;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::net::udp_batch::{Datagram, InboundDatagram, UdpSocketBatchExt};
    use tokio::net::UdpSocket;

    #[derive(Clone, Debug)]
    pub struct BenchConfig {
        pub duration: Duration,
        pub warmup_duration: Duration,
        pub probe_interval: Duration,
        pub probe_timeout: Duration,
        pub min_latency_samples: usize,
        pub trials: usize,
        pub seed: u64,
        pub csv_path: Option<String>,
        pub json_path: Option<String>,
    }

    impl Default for BenchConfig {
        fn default() -> Self {
            Self {
                duration: Duration::from_millis(400),
                warmup_duration: Duration::from_millis(100),
                probe_interval: Duration::from_millis(2),
                probe_timeout: Duration::from_millis(25),
                min_latency_samples: 10,
                trials: 1,
                seed: 42,
                csv_path: None,
                json_path: None,
            }
        }
    }

    impl BenchConfig {
        pub fn validate(&self) -> io::Result<()> {
            if self.duration == Duration::ZERO {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "UDP_BENCH_DURATION_MS must be greater than 0",
                ));
            }
            if self.duration > Duration::from_secs(3600) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "UDP_BENCH_DURATION_MS exceeds maximum allowed duration (3600s)",
                ));
            }
            if self.warmup_duration > Duration::from_secs(600) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "UDP_BENCH_WARMUP_MS exceeds maximum allowed duration (600s)",
                ));
            }
            if self.probe_interval == Duration::ZERO {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "UDP_BENCH_PROBE_INTERVAL_MS must be greater than 0",
                ));
            }
            if self.probe_timeout == Duration::ZERO {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "UDP_BENCH_PROBE_TIMEOUT_MS must be greater than 0",
                ));
            }
            if self.min_latency_samples == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "UDP_BENCH_MIN_SAMPLES must be greater than 0",
                ));
            }
            if self.trials == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "UDP_BENCH_TRIALS must be greater than 0",
                ));
            }
            if self.trials > 1000 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "UDP_BENCH_TRIALS exceeds maximum allowed trials (1000)",
                ));
            }
            Ok(())
        }

        pub fn from_env() -> io::Result<Self> {
            let mut cfg = Self::default();
            if let Ok(v) = std::env::var("UDP_BENCH_DURATION_MS") {
                let ms = v.parse::<u64>().map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Invalid UDP_BENCH_DURATION_MS '{v}': {e}"),
                    )
                })?;
                cfg.duration = Duration::from_millis(ms);
            }
            if let Ok(v) = std::env::var("UDP_BENCH_WARMUP_MS") {
                let ms = v.parse::<u64>().map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Invalid UDP_BENCH_WARMUP_MS '{v}': {e}"),
                    )
                })?;
                cfg.warmup_duration = Duration::from_millis(ms);
            }
            if let Ok(v) = std::env::var("UDP_BENCH_PROBE_INTERVAL_MS") {
                let ms = v.parse::<u64>().map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Invalid UDP_BENCH_PROBE_INTERVAL_MS '{v}': {e}"),
                    )
                })?;
                cfg.probe_interval = Duration::from_millis(ms);
            }
            if let Ok(v) = std::env::var("UDP_BENCH_PROBE_TIMEOUT_MS") {
                let ms = v.parse::<u64>().map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Invalid UDP_BENCH_PROBE_TIMEOUT_MS '{v}': {e}"),
                    )
                })?;
                cfg.probe_timeout = Duration::from_millis(ms);
            }
            if let Ok(v) = std::env::var("UDP_BENCH_MIN_SAMPLES") {
                let n = v.parse::<usize>().map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Invalid UDP_BENCH_MIN_SAMPLES '{v}': {e}"),
                    )
                })?;
                cfg.min_latency_samples = n;
            }
            if let Ok(v) = std::env::var("UDP_BENCH_TRIALS") {
                let n = v.parse::<usize>().map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Invalid UDP_BENCH_TRIALS '{v}': {e}"),
                    )
                })?;
                cfg.trials = n;
            }
            if let Ok(v) = std::env::var("UDP_BENCH_SEED") {
                let s = v.parse::<u64>().map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Invalid UDP_BENCH_SEED '{v}': {e}"),
                    )
                })?;
                cfg.seed = s;
            }
            if let Ok(path) = std::env::var("UDP_BENCH_CSV") {
                if path.trim().is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "UDP_BENCH_CSV path must not be empty",
                    ));
                }
                cfg.csv_path = Some(path);
            }
            if let Ok(path) = std::env::var("UDP_BENCH_JSON") {
                if path.trim().is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "UDP_BENCH_JSON path must not be empty",
                    ));
                }
                cfg.json_path = Some(path);
            }
            cfg.validate()?;
            Ok(cfg)
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Mode {
        Baseline,
        Grouped(usize),
        Batched(usize),
    }

    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct WorkerStats {
        pub packets: u64,
        pub io_ops: u64,
    }

    #[allow(dead_code)]
    #[derive(Debug, Clone)]
    pub struct TrialResult {
        pub trial_index: usize,
        pub mode: Mode,
        pub packet_size: usize,
        pub concurrency: usize,
        pub batch_size: usize,
        /// Configured nominal measurement duration.
        pub nominal_duration: Duration,
        /// Actual elapsed duration from start barrier release until all workers have stopped and joined.
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
        pub mib_per_sec: f64,
        pub user_cpu_pct: f64,
        pub sys_cpu_pct: f64,
        pub total_cpu_pct: f64,
        /// Aggregate CPU time (user + system) consumed per delivered packet, in nanoseconds.
        pub cpu_ns_per_pkt: f64,
        /// Proxy metric for system call efficiency: successful socket operations per delivered packet.
        pub io_ops_per_pkt: f64,
        pub packet_loss_pct: f64,
        pub p50_rtt_us: Option<f64>,
        pub p95_rtt_us: Option<f64>,
        pub p99_rtt_us: Option<f64>,
        pub latency_samples: usize,
        pub latency_timeouts: usize,
        pub latency_invalid: usize,
        pub peak_rss_kb: i64,
        pub vol_cs: i64,
        pub invol_cs: i64,
        pub effective_rcvbuf: usize,
        pub effective_sndbuf: usize,
    }

    impl TrialResult {
        pub fn csv_header() -> &'static str {
            "trial_index,mode,packet_size,concurrency,batch_size,nominal_duration_ms,elapsed_ms,packets_sent,packets_recv_timed,packets_drained,total_recv,bytes_recv,pps,mib_per_sec,cpu_user_pct,cpu_sys_pct,cpu_total_pct,cpu_ns_per_pkt,io_ops_per_pkt,packet_loss_pct,p50_rtt_us,p95_rtt_us,p99_rtt_us,latency_samples,latency_timeouts,latency_invalid,peak_rss_kb\n"
        }

        pub fn to_csv_row(&self) -> String {
            let mode_str = match self.mode {
                Mode::Baseline => "baseline".to_string(),
                Mode::Grouped(b) => format!("grouped_{b}"),
                Mode::Batched(b) => format!("batched_{b}"),
            };
            format!(
                "{},{},{},{},{},{:.3},{:.3},{},{},{},{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{},{},{},{},{},{},{}\n",
                self.trial_index,
                mode_str,
                self.packet_size,
                self.concurrency,
                self.batch_size,
                self.nominal_duration.as_secs_f64() * 1000.0,
                self.elapsed.as_secs_f64() * 1000.0,
                self.packets_sent,
                self.packets_recv_timed,
                self.packets_drained,
                self.total_recv,
                self.bytes_recv,
                self.pps,
                self.mib_per_sec,
                self.user_cpu_pct,
                self.sys_cpu_pct,
                self.total_cpu_pct,
                self.cpu_ns_per_pkt,
                self.io_ops_per_pkt,
                self.packet_loss_pct,
                self.p50_rtt_us.map(|v| format!("{v:.2}")).unwrap_or_default(),
                self.p95_rtt_us.map(|v| format!("{v:.2}")).unwrap_or_default(),
                self.p99_rtt_us.map(|v| format!("{v:.2}")).unwrap_or_default(),
                self.latency_samples,
                self.latency_timeouts,
                self.latency_invalid,
                self.peak_rss_kb,
            )
        }

        pub fn to_json(&self) -> String {
            let mode_str = match self.mode {
                Mode::Baseline => "baseline",
                Mode::Grouped(b) => &format!("grouped_{b}"),
                Mode::Batched(b) => &format!("batched_{b}"),
            };
            format!(
                "{{\"trial_index\":{},\"mode\":\"{}\",\"packet_size\":{},\"concurrency\":{},\"batch_size\":{},\"nominal_duration_ms\":{:.3},\"elapsed_ms\":{:.3},\"packets_sent\":{},\"packets_recv_timed\":{},\"packets_drained\":{},\"total_recv\":{},\"bytes_recv\":{},\"pps\":{:.2},\"mib_per_sec\":{:.2},\"cpu_user_pct\":{:.2},\"cpu_sys_pct\":{:.2},\"cpu_total_pct\":{:.2},\"cpu_ns_per_pkt\":{:.2},\"io_ops_per_pkt\":{:.2},\"packet_loss_pct\":{:.2},\"p50_rtt_us\":{},\"p95_rtt_us\":{},\"p99_rtt_us\":{},\"latency_samples\":{},\"latency_timeouts\":{},\"latency_invalid\":{},\"peak_rss_kb\":{}}}",
                self.trial_index,
                mode_str,
                self.packet_size,
                self.concurrency,
                self.batch_size,
                self.nominal_duration.as_secs_f64() * 1000.0,
                self.elapsed.as_secs_f64() * 1000.0,
                self.packets_sent,
                self.packets_recv_timed,
                self.packets_drained,
                self.total_recv,
                self.bytes_recv,
                self.pps,
                self.mib_per_sec,
                self.user_cpu_pct,
                self.sys_cpu_pct,
                self.total_cpu_pct,
                self.cpu_ns_per_pkt,
                self.io_ops_per_pkt,
                self.packet_loss_pct,
                self.p50_rtt_us.map(|v| format!("{v:.2}")).unwrap_or_else(|| "null".to_string()),
                self.p95_rtt_us.map(|v| format!("{v:.2}")).unwrap_or_else(|| "null".to_string()),
                self.p99_rtt_us.map(|v| format!("{v:.2}")).unwrap_or_else(|| "null".to_string()),
                self.latency_samples,
                self.latency_timeouts,
                self.latency_invalid,
                self.peak_rss_kb,
            )
        }
    }

    #[allow(dead_code)]
    #[derive(Debug, Clone)]
    pub struct BenchSummary {
        pub mode: Mode,
        pub packet_size: usize,
        pub concurrency: usize,
        pub trials: usize,
        pub median_pps: f64,
        pub mean_pps: f64,
        pub stddev_pps: f64,
        pub min_pps: f64,
        pub max_pps: f64,
        pub median_mib_per_sec: f64,
        pub mean_mib_per_sec: f64,
        pub median_cpu_pct: f64,
        pub mean_cpu_pct: f64,
        pub median_cpu_ns_per_pkt: f64,
        pub mean_cpu_ns_per_pkt: f64,
        pub median_io_ops_per_pkt: f64,
        pub mean_io_ops_per_pkt: f64,
        pub median_loss_pct: f64,
        pub mean_loss_pct: f64,
        pub median_p50_rtt_us: Option<f64>,
        pub median_p95_rtt_us: Option<f64>,
        pub median_p99_rtt_us: Option<f64>,
        pub representative: TrialResult,
        pub raw_trials: Vec<TrialResult>,
    }

    impl BenchSummary {
        pub fn from_trials(trials: Vec<TrialResult>) -> Self {
            assert!(!trials.is_empty(), "trials must not be empty");
            let n = trials.len();
            let mode = trials[0].mode;
            let packet_size = trials[0].packet_size;
            let concurrency = trials[0].concurrency;

            let mut sorted_by_pps = trials.clone();
            sorted_by_pps.sort_by(|a, b| {
                a.pps
                    .partial_cmp(&b.pps)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            let median_pps = if n % 2 == 1 {
                sorted_by_pps[n / 2].pps
            } else {
                (sorted_by_pps[n / 2 - 1].pps + sorted_by_pps[n / 2].pps) / 2.0
            };
            let min_pps = sorted_by_pps[0].pps;
            let max_pps = sorted_by_pps[n - 1].pps;

            let sum_pps: f64 = trials.iter().map(|t| t.pps).sum();
            let mean_pps = sum_pps / (n as f64);
            let var_pps: f64 = trials
                .iter()
                .map(|t| (t.pps - mean_pps).powi(2))
                .sum::<f64>()
                / (n as f64);
            let stddev_pps = var_pps.sqrt();

            let mut sorted_mib: Vec<f64> = trials.iter().map(|t| t.mib_per_sec).collect();
            sorted_mib.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let median_mib_per_sec = sorted_mib[n / 2];
            let mean_mib_per_sec = trials.iter().map(|t| t.mib_per_sec).sum::<f64>() / (n as f64);

            let mut sorted_cpu: Vec<f64> = trials.iter().map(|t| t.total_cpu_pct).collect();
            sorted_cpu.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let median_cpu_pct = sorted_cpu[n / 2];
            let mean_cpu_pct = trials.iter().map(|t| t.total_cpu_pct).sum::<f64>() / (n as f64);

            let mut sorted_cpu_ns: Vec<f64> = trials.iter().map(|t| t.cpu_ns_per_pkt).collect();
            sorted_cpu_ns.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let median_cpu_ns_per_pkt = sorted_cpu_ns[n / 2];
            let mean_cpu_ns_per_pkt =
                trials.iter().map(|t| t.cpu_ns_per_pkt).sum::<f64>() / (n as f64);

            let mut sorted_io_ops: Vec<f64> = trials.iter().map(|t| t.io_ops_per_pkt).collect();
            sorted_io_ops.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let median_io_ops_per_pkt = sorted_io_ops[n / 2];
            let mean_io_ops_per_pkt =
                trials.iter().map(|t| t.io_ops_per_pkt).sum::<f64>() / (n as f64);

            let mut sorted_loss: Vec<f64> = trials.iter().map(|t| t.packet_loss_pct).collect();
            sorted_loss.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let median_loss_pct = sorted_loss[n / 2];
            let mean_loss_pct = trials.iter().map(|t| t.packet_loss_pct).sum::<f64>() / (n as f64);

            let calc_median_opt = |extract: fn(&TrialResult) -> Option<f64>| -> Option<f64> {
                let mut vals: Vec<f64> = trials.iter().filter_map(extract).collect();
                if vals.is_empty() {
                    None
                } else {
                    vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    Some(vals[vals.len() / 2])
                }
            };

            let median_p50_rtt_us = calc_median_opt(|t| t.p50_rtt_us);
            let median_p95_rtt_us = calc_median_opt(|t| t.p95_rtt_us);
            let median_p99_rtt_us = calc_median_opt(|t| t.p99_rtt_us);

            let representative = sorted_by_pps[n / 2].clone();

            Self {
                mode,
                packet_size,
                concurrency,
                trials: n,
                median_pps,
                mean_pps,
                stddev_pps,
                min_pps,
                max_pps,
                median_mib_per_sec,
                mean_mib_per_sec,
                median_cpu_pct,
                mean_cpu_pct,
                median_cpu_ns_per_pkt,
                mean_cpu_ns_per_pkt,
                median_io_ops_per_pkt,
                mean_io_ops_per_pkt,
                median_loss_pct,
                mean_loss_pct,
                median_p50_rtt_us,
                median_p95_rtt_us,
                median_p99_rtt_us,
                representative,
                raw_trials: trials,
            }
        }

        pub fn to_json(&self) -> String {
            let mode_str = match self.mode {
                Mode::Baseline => "baseline",
                Mode::Grouped(b) => &format!("grouped_{b}"),
                Mode::Batched(b) => &format!("batched_{b}"),
            };
            let trials_json: Vec<String> = self.raw_trials.iter().map(|t| t.to_json()).collect();
            format!(
                "{{\"mode\":\"{}\",\"packet_size\":{},\"concurrency\":{},\"trials\":{},\"median_pps\":{:.2},\"mean_pps\":{:.2},\"stddev_pps\":{:.2},\"min_pps\":{:.2},\"max_pps\":{:.2},\"median_mib_per_sec\":{:.2},\"median_cpu_pct\":{:.2},\"median_cpu_ns_per_pkt\":{:.2},\"median_io_ops_per_pkt\":{:.2},\"median_loss_pct\":{:.2},\"median_p50_rtt_us\":{},\"median_p95_rtt_us\":{},\"median_p99_rtt_us\":{},\"raw_trials\":[\n    {}\n  ]}}",
                mode_str,
                self.packet_size,
                self.concurrency,
                self.trials,
                self.median_pps,
                self.mean_pps,
                self.stddev_pps,
                self.min_pps,
                self.max_pps,
                self.median_mib_per_sec,
                self.median_cpu_pct,
                self.median_cpu_ns_per_pkt,
                self.median_io_ops_per_pkt,
                self.median_loss_pct,
                self.median_p50_rtt_us.map(|v| format!("{v:.2}")).unwrap_or_else(|| "null".to_string()),
                self.median_p95_rtt_us.map(|v| format!("{v:.2}")).unwrap_or_else(|| "null".to_string()),
                self.median_p99_rtt_us.map(|v| format!("{v:.2}")).unwrap_or_else(|| "null".to_string()),
                trials_json.join(",\n    ")
            )
        }
    }

    pub fn compute_latency_percentiles(
        samples: &mut [f64],
        min_samples: usize,
    ) -> (Option<f64>, Option<f64>, Option<f64>) {
        if samples.is_empty() || samples.len() < min_samples {
            return (None, None, None);
        }
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        (
            Some(samples[samples.len() * 50 / 100]),
            Some(samples[samples.len() * 95 / 100]),
            Some(samples[samples.len() * 99 / 100]),
        )
    }

    fn get_rusage() -> io::Result<libc::rusage> {
        unsafe {
            let mut r = std::mem::MaybeUninit::<libc::rusage>::zeroed();
            let ret = libc::getrusage(libc::RUSAGE_SELF, r.as_mut_ptr());
            if ret != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(r.assume_init())
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
        trial_index: usize,
        config: &BenchConfig,
    ) -> io::Result<TrialResult> {
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
        tokio::time::sleep(config.warmup_duration).await;
        warmup_running.store(false, Ordering::Relaxed);
        for h in &warmup_handles {
            h.abort();
        }
        for (i, h) in warmup_handles.into_iter().enumerate() {
            match tokio::time::timeout(Duration::from_millis(200), h).await {
                Ok(Ok(_)) => {}                                    // Clean exit
                Ok(Err(join_err)) if join_err.is_cancelled() => {} // Expected cancellation
                Ok(Err(join_err)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("Warmup task {i} panicked: {join_err}"),
                    ));
                }
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("Warmup task {i} failed to terminate within timeout"),
                    ));
                }
            }
        }
        // Drain all remaining warmup packets completely after all tasks have stopped
        for r in &receivers {
            let mut drain_buf = [0u8; 2048];
            while r.try_recv_from(&mut drain_buf).is_ok() {}
        }

        // Spawn receiver tasks with plain local integer counters
        let mut recv_handles = Vec::with_capacity(concurrency);
        for r in &receivers {
            let r = r.clone();
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
                                res = r.recv_from(&mut buf) => {
                                    res?;
                                    local_packets += 1;
                                    local_io_ops += 1;
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
                                    res?;
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
                                            Err(e) => return Err(e),
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
                                res = r.recv_many(&mut inbound) => {
                                    let n = res?;
                                    local_packets += n as u64;
                                    local_io_ops += 1;
                                    tokio::task::yield_now().await;
                                }
                            }
                        }
                    }
                }
                Ok(WorkerStats {
                    packets: local_packets,
                    io_ops: local_io_ops,
                })
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
                            s.send_to(&payload, target).await?;
                            local_packets += 1;
                            local_io_ops += 1;
                        }
                    }
                    Mode::Grouped(batch_size) => {
                        let payload = vec![0x42u8; packet_size];
                        while run.load(Ordering::Relaxed) {
                            s.writable().await?;
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
                                    Err(e) => return Err(e),
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
                            let n = s.send_many(&batch).await?;
                            local_packets += n as u64;
                            local_io_ops += 1;
                            tokio::task::yield_now().await;
                        }
                    }
                }
                Ok(WorkerStats {
                    packets: local_packets,
                    io_ops: local_io_ops,
                })
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

        let probe_interval = config.probe_interval;
        let probe_timeout = config.probe_timeout;
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
                let ping_res = tokio::time::timeout(probe_timeout, async {
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
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "invalid probe seq",
                                ));
                            }
                        } else {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "truncated probe reply",
                            ));
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
                tokio::time::sleep(probe_interval).await;
            }
            (samples, timeouts, invalid_replies)
        });

        // Sample process resource usage and timestamp immediately before releasing the barrier.
        // As the coordinator is the final task to reach the barrier, no worker can execute
        // any operations before this start timestamp.
        let rusage_start = get_rusage()?;
        let start_time = Instant::now();
        start_barrier.wait().await;

        // Run measurement for the configured nominal duration
        tokio::time::sleep(config.duration).await;
        let nominal_stop_time = Instant::now();
        let nominal_duration = nominal_stop_time.duration_since(start_time);

        // Two-phase coordinated shutdown:
        // Phase 1: Halt sender generation so no new packets enter the network
        running.store(false, Ordering::Release);

        let mut sent = 0u64;
        let mut send_io_ops = 0u64;
        let mut worker_error = None;

        for (i, h) in send_handles.into_iter().enumerate() {
            match h.await {
                Ok(Ok(stats)) => {
                    sent += stats.packets;
                    send_io_ops += stats.io_ops;
                }
                Ok(Err(e)) => {
                    if worker_error.is_none() {
                        worker_error = Some(io::Error::new(
                            e.kind(),
                            format!("Sender task {i} error: {e}"),
                        ));
                    }
                }
                Err(e) => {
                    if worker_error.is_none() {
                        worker_error = Some(io::Error::new(
                            io::ErrorKind::Other,
                            format!("Sender task {i} join failure: {e}"),
                        ));
                    }
                }
            }
        }

        // Phase 2: Halt active receivers and wait for all tasks to terminate
        let _ = shutdown_tx.send(true);

        let mut timed_recv = 0u64;
        let mut recv_io_ops = 0u64;
        for (i, h) in recv_handles.into_iter().enumerate() {
            match h.await {
                Ok(Ok(stats)) => {
                    timed_recv += stats.packets;
                    recv_io_ops += stats.io_ops;
                }
                Ok(Err(e)) => {
                    if worker_error.is_none() {
                        worker_error = Some(io::Error::new(
                            e.kind(),
                            format!("Receiver task {i} error: {e}"),
                        ));
                    }
                }
                Err(e) => {
                    if worker_error.is_none() {
                        worker_error = Some(io::Error::new(
                            io::ErrorKind::Other,
                            format!("Receiver task {i} join failure: {e}"),
                        ));
                    }
                }
            }
        }

        let join_timeout = config.probe_timeout + Duration::from_millis(500);
        let (mut latency_samples, latency_timeouts, latency_invalid) =
            match tokio::time::timeout(join_timeout, probe_h).await {
                Ok(Ok(stats)) => stats,
                Ok(Err(panic_err)) => {
                    if worker_error.is_none() {
                        worker_error = Some(io::Error::new(
                            io::ErrorKind::Other,
                            format!("Latency probe task panicked: {panic_err}"),
                        ));
                    }
                    (Vec::new(), 0, 0)
                }
                Err(_) => {
                    if worker_error.is_none() {
                        worker_error = Some(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "Latency probe task join timed out",
                        ));
                    }
                    (Vec::new(), 0, 0)
                }
            };
        probe_echo_h.abort();
        let _ = probe_echo_h.await;

        // Sample post-measurement timestamp and rusage immediately after all workers finish.
        // This ensures every sent/received packet and every CPU microsecond are strictly
        // contained within the [start_time, active_stop_time] window.
        let active_stop_time = Instant::now();
        let rusage_stop = get_rusage()?;
        let actual_measurement_duration = active_stop_time.duration_since(start_time);

        // Invalidate run if any worker encountered an unexpected failure
        if let Some(err) = worker_error {
            return Err(err);
        }

        // Drain any remaining buffered packets from the kernel queues post-stop
        let mut drained_recv = 0u64;
        for r in &receivers {
            let mut drain_buf = [0u8; 2048];
            while r.try_recv_from(&mut drain_buf).is_ok() {
                drained_recv += 1;
            }
        }

        let total_recv = timed_recv + drained_recv;
        let total_io_ops = send_io_ops + recv_io_ops;

        let utime_diff = timeval_to_duration(rusage_stop.ru_utime)
            .saturating_sub(timeval_to_duration(rusage_start.ru_utime));
        let stime_diff = timeval_to_duration(rusage_stop.ru_stime)
            .saturating_sub(timeval_to_duration(rusage_start.ru_stime));
        let duration_sec = actual_measurement_duration.as_secs_f64();

        let user_cpu_pct = (utime_diff.as_secs_f64() / duration_sec) * 100.0;
        let sys_cpu_pct = (stime_diff.as_secs_f64() / duration_sec) * 100.0;
        let total_cpu_pct = user_cpu_pct + sys_cpu_pct;

        let pps = (timed_recv as f64) / duration_sec;
        let bytes_recv = timed_recv * (packet_size as u64);
        let mib_per_sec = (bytes_recv as f64) / (1024.0 * 1024.0 * duration_sec);

        let total_cpu_time = utime_diff + stime_diff;
        let cpu_ns_per_pkt = if timed_recv > 0 {
            (total_cpu_time.as_nanos() as f64) / (timed_recv as f64)
        } else {
            0.0
        };
        let io_ops_per_pkt = if timed_recv > 0 {
            (total_io_ops as f64) / (timed_recv as f64)
        } else {
            0.0
        };

        let packet_loss_pct = if sent > 0 {
            let lost = sent.saturating_sub(total_recv);
            (lost as f64) / (sent as f64) * 100.0
        } else {
            0.0
        };

        let vol_cs = rusage_stop.ru_nvcsw - rusage_start.ru_nvcsw;
        let invol_cs = rusage_stop.ru_nivcsw - rusage_start.ru_nivcsw;
        let peak_rss_kb = rusage_stop.ru_maxrss;

        let (p50, p95, p99) =
            compute_latency_percentiles(&mut latency_samples, config.min_latency_samples);

        let batch_size = match mode {
            Mode::Baseline => 1,
            Mode::Grouped(b) | Mode::Batched(b) => b,
        };

        Ok(TrialResult {
            trial_index,
            mode,
            packet_size,
            concurrency,
            batch_size,
            nominal_duration,
            elapsed: actual_measurement_duration,
            packets_sent: sent,
            packets_recv_timed: timed_recv,
            packets_drained: drained_recv,
            total_recv,
            bytes_recv,
            successful_io_sends: send_io_ops,
            successful_io_recvs: recv_io_ops,
            pps,
            mib_per_sec,
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

    fn deterministic_permute(n: usize, seed: u64) -> Vec<usize> {
        let mut indices: Vec<usize> = (0..n).collect();
        let mut state = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        for i in (1..n).rev() {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let j = ((state >> 32) as usize) % (i + 1);
            indices.swap(i, j);
        }
        indices
    }

    /// Runs a batch of benchmark cases using interleaved execution across trial rounds to
    /// eliminate thermal throttling and CPU frequency scaling bias.
    pub async fn run_cases(
        cases: &[(Mode, usize, usize)],
        config: &BenchConfig,
    ) -> io::Result<Vec<BenchSummary>> {
        let n = cases.len();
        if n == 0 {
            return Ok(Vec::new());
        }

        let mut trial_collectors: Vec<Vec<TrialResult>> =
            vec![Vec::with_capacity(config.trials); n];

        for trial_index in 0..config.trials {
            // Generate deterministic permutation of cases for this trial round
            let round_seed = config
                .seed
                .wrapping_add((trial_index as u64).wrapping_mul(0x9E3779B97F4A7C15));
            let order = deterministic_permute(n, round_seed);

            for &case_idx in &order {
                let (mode, pkt_size, conc) = cases[case_idx];
                let res = run_throughput_bench(mode, pkt_size, conc, trial_index, config).await?;
                trial_collectors[case_idx].push(res);
                if config.trials > 1 {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
        }

        let mut summaries = Vec::with_capacity(n);
        for trials in trial_collectors {
            summaries.push(BenchSummary::from_trials(trials));
        }
        Ok(summaries)
    }

    #[allow(dead_code)]
    pub async fn run_bench(
        mode: Mode,
        packet_size: usize,
        concurrency: usize,
        config: &BenchConfig,
    ) -> io::Result<BenchSummary> {
        let summaries = run_cases(&[(mode, packet_size, concurrency)], config).await?;
        Ok(summaries.into_iter().next().unwrap())
    }

    fn print_header() {
        println!(
        "| Mode | PktSz | Conc | Pkts/sec |  MiB/s  | CPU% (U+S) | CPU-ns/Pkt | IO-Ops/Pkt | Loss % | p50 (us) | p95 (us) | p99 (us) | Pings (TO) | Peak RSS |"
    );
        println!(
            "|:---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|"
        );
    }

    fn print_result(s: &BenchSummary) {
        let r = &s.representative;
        let mode_str = match r.mode {
            Mode::Baseline => "Baseline".to_string(),
            Mode::Grouped(b) => format!("Group-{}", b),
            Mode::Batched(b) => format!("Batch-{}", b),
        };
        let pings_str = if r.latency_invalid > 0 {
            format!(
                "{}/{}(+{})",
                r.latency_samples, r.latency_timeouts, r.latency_invalid
            )
        } else {
            format!("{}/{}", r.latency_samples, r.latency_timeouts)
        };
        let p50_str = match s.median_p50_rtt_us {
            Some(v) => format!("{:>8.1}", v),
            None => "     N/A".to_string(),
        };
        let p95_str = match s.median_p95_rtt_us {
            Some(v) => format!("{:>8.1}", v),
            None => "     N/A".to_string(),
        };
        let p99_str = match s.median_p99_rtt_us {
            Some(v) => format!("{:>8.1}", v),
            None => "     N/A".to_string(),
        };
        println!(
            "| {:<10} | {:>5} | {:>4} | {:>10.0} | {:>7.2} | {:>5.1}% ({:>4.1}+{:<4.1}) | {:>10.0} | {:>12.2} | {:>5.2}% | {} | {} | {} | {:>10} | {:>7} KB |",
            mode_str,
            r.packet_size,
            r.concurrency,
            s.median_pps,
            s.median_mib_per_sec,
            s.median_cpu_pct,
            r.user_cpu_pct,
            r.sys_cpu_pct,
            s.median_cpu_ns_per_pkt,
            s.median_io_ops_per_pkt,
            s.median_loss_pct,
            p50_str,
            p95_str,
            p99_str,
            pings_str,
            r.peak_rss_kb,
        );
    }

    pub async fn run() -> io::Result<()> {
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
        config_tests::run_self_tests();
        println!(
        "Host: Linux x86_64, local loopback. Effective socket buffers: SO_RCVBUF = {} bytes ({} KB), SO_SNDBUF = {} bytes ({} KB).",
        eff_rcv,
        eff_rcv / 1024,
        eff_snd,
        eff_snd / 1024
    );
        println!();

        let config = BenchConfig::from_env()?;
        if config.trials > 1 {
            println!(
                "Running with {} trials per benchmark case (interleaved deterministic ordering, seed: {}).",
                config.trials, config.seed
            );
            println!();
        }

        let packet_sizes = [64, 512, 1200, 1472];
        let concurrencies = [1, 4, 16, 64];
        let batch_sizes = [4, 8, 16, 32, 64];
        let mut all_results = Vec::new();

        // Section 1: Baseline Tokio across all Packet Sizes and Concurrencies
        println!(
            "### PART 1: Baseline Tokio UDP Operations (Single-Packet async send_to/recv_from)"
        );
        print_header();
        let mut s1_cases = Vec::new();
        for &pkt in &packet_sizes {
            for &conc in &concurrencies {
                s1_cases.push((Mode::Baseline, pkt, conc));
            }
        }
        for s in run_cases(&s1_cases, &config).await? {
            print_result(&s);
            all_results.push(s);
        }
        println!();

        // Section 2: Three-Way Comparison: Baseline vs Grouped vs Batched (64B packets, 16x16 concurrency)
        println!("### PART 2: Three-Way Comparison (Packet Size: 64B, Concurrency: 16x16)");
        print_header();
        let mut s2_cases = vec![(Mode::Baseline, 64, 16)];
        for &bs in &batch_sizes {
            s2_cases.push((Mode::Grouped(bs), 64, 16));
            s2_cases.push((Mode::Batched(bs), 64, 16));
        }
        for s in run_cases(&s2_cases, &config).await? {
            print_result(&s);
            all_results.push(s);
        }
        println!();

        // Section 3: Three-Way Comparison: Baseline vs Grouped vs Batched (1472B packets, 16x16 concurrency)
        println!("### PART 3: Three-Way Comparison (Packet Size: 1472B, Concurrency: 16x16)");
        print_header();
        let mut s3_cases = vec![(Mode::Baseline, 1472, 16)];
        for &bs in &batch_sizes {
            s3_cases.push((Mode::Grouped(bs), 1472, 16));
            s3_cases.push((Mode::Batched(bs), 1472, 16));
        }
        for s in run_cases(&s3_cases, &config).await? {
            print_result(&s);
            all_results.push(s);
        }
        println!();

        // Section 4: Concurrency Scaling (Packet Size: 64B): Baseline vs Group-32 vs Batch-32
        println!(
            "### PART 4: Concurrency Scaling (Packet Size: 64B: Baseline vs Group-32 vs Batch-32)"
        );
        print_header();
        let mut s4_cases = Vec::new();
        for &conc in &concurrencies {
            s4_cases.push((Mode::Baseline, 64, conc));
            s4_cases.push((Mode::Grouped(32), 64, conc));
            s4_cases.push((Mode::Batched(32), 64, conc));
        }
        for s in run_cases(&s4_cases, &config).await? {
            print_result(&s);
            all_results.push(s);
        }
        println!();

        // Section 5: Concurrency Scaling (Packet Size: 1472B): Baseline vs Group-32 vs Batch-32
        println!(
            "### PART 5: Concurrency Scaling (Packet Size: 1472B: Baseline vs Group-32 vs Batch-32)"
        );
        print_header();
        let mut s5_cases = Vec::new();
        for &conc in &concurrencies {
            s5_cases.push((Mode::Baseline, 1472, conc));
            s5_cases.push((Mode::Grouped(32), 1472, conc));
            s5_cases.push((Mode::Batched(32), 1472, conc));
        }
        for s in run_cases(&s5_cases, &config).await? {
            print_result(&s);
            all_results.push(s);
        }
        println!();

        if let Some(path) = &config.csv_path {
            let mut content = String::from(TrialResult::csv_header());
            for summary in &all_results {
                for trial in &summary.raw_trials {
                    content.push_str(&trial.to_csv_row());
                }
            }
            std::fs::write(path, content)?;
            let total_trials: usize = all_results.iter().map(|s| s.raw_trials.len()).sum();
            println!(
                "Exported {} individual trial observations across {} configurations to CSV: {path}",
                total_trials,
                all_results.len()
            );
        }

        if let Some(path) = &config.json_path {
            let json_rows: Vec<String> = all_results.iter().map(|s| s.to_json()).collect();
            let json_content = format!("[\n  {}\n]\n", json_rows.join(",\n  "));
            std::fs::write(path, json_content)?;
            println!(
                "Exported {} benchmark summaries with raw trials to JSON: {path}",
                all_results.len()
            );
        }

        println!("Benchmark execution completed successfully.");
        Ok(())
    }

    mod config_tests {
        use super::*;

        pub fn run_self_tests() {
            test_default_config_is_valid();
            test_validation_rejects_zero_duration();
            test_validation_rejects_excessive_duration();
            test_validation_rejects_zero_probe_interval();
            test_validation_rejects_zero_probe_timeout();
            test_validation_rejects_zero_min_samples();
            test_validation_rejects_zero_trials();
            test_validation_rejects_excessive_trials();
            test_percentiles_empty_samples();
            test_percentiles_insufficient_samples();
            test_percentiles_valid_samples();
        }

        fn test_default_config_is_valid() {
            let cfg = BenchConfig::default();
            assert!(cfg.validate().is_ok());
        }

        fn test_validation_rejects_zero_duration() {
            let cfg = BenchConfig {
                duration: Duration::ZERO,
                ..Default::default()
            };
            assert!(cfg.validate().is_err());
        }

        fn test_validation_rejects_excessive_duration() {
            let cfg = BenchConfig {
                duration: Duration::from_secs(3601),
                ..Default::default()
            };
            assert!(cfg.validate().is_err());
        }

        fn test_validation_rejects_zero_probe_interval() {
            let cfg = BenchConfig {
                probe_interval: Duration::ZERO,
                ..Default::default()
            };
            assert!(cfg.validate().is_err());
        }

        fn test_validation_rejects_zero_probe_timeout() {
            let cfg = BenchConfig {
                probe_timeout: Duration::ZERO,
                ..Default::default()
            };
            assert!(cfg.validate().is_err());
        }

        fn test_validation_rejects_zero_min_samples() {
            let cfg = BenchConfig {
                min_latency_samples: 0,
                ..Default::default()
            };
            assert!(cfg.validate().is_err());
        }

        fn test_validation_rejects_zero_trials() {
            let cfg = BenchConfig {
                trials: 0,
                ..Default::default()
            };
            assert!(cfg.validate().is_err());
        }

        fn test_validation_rejects_excessive_trials() {
            let cfg = BenchConfig {
                trials: 1001,
                ..Default::default()
            };
            assert!(cfg.validate().is_err());
        }

        fn test_percentiles_empty_samples() {
            let mut empty: Vec<f64> = Vec::new();
            let (p50, p95, p99) = compute_latency_percentiles(&mut empty, 1);
            assert!(p50.is_none());
            assert!(p95.is_none());
            assert!(p99.is_none());
        }

        fn test_percentiles_insufficient_samples() {
            let mut samples = vec![10.0, 20.0];
            let (p50, p95, p99) = compute_latency_percentiles(&mut samples, 5);
            assert!(p50.is_none());
            assert!(p95.is_none());
            assert!(p99.is_none());
        }

        fn test_percentiles_valid_samples() {
            let mut samples: Vec<f64> = (1..=100).map(|x| x as f64).collect();
            let (p50, p95, p99) = compute_latency_percentiles(&mut samples, 10);
            assert_eq!(p50, Some(51.0));
            assert_eq!(p95, Some(96.0));
            assert_eq!(p99, Some(100.0));
        }
    }
}

#[cfg(target_os = "linux")]
#[tokio::main]
async fn main() -> std::io::Result<()> {
    linux::run().await
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("tokio udp_bench is only supported on Linux targets; skipping.");
}
