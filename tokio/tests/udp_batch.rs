#![warn(rust_2018_idioms)]
#![cfg(all(
    target_os = "linux",
    feature = "net",
    feature = "macros",
    feature = "rt",
    feature = "time",
    not(miri)
))]

use std::future::poll_fn;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::udp_batch::{Datagram, InboundDatagram, UdpSocketBatchExt, MAX_BATCH_SIZE};
use tokio::net::UdpSocket;
use tokio::time;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn test_empty_batch() -> io::Result<()> {
    time::timeout(TEST_TIMEOUT, async {
        let socket = UdpSocket::bind("127.0.0.1:0").await?;

        let sent = socket.send_many(&[]).await?;
        assert_eq!(sent, 0);

        let mut recv_batch: [InboundDatagram<'_>; 0] = [];
        let received = socket.recv_many(&mut recv_batch).await?;
        assert_eq!(received, 0);

        let try_sent = socket.try_send_many(&[])?;
        assert_eq!(try_sent, 0);

        let try_received = socket.try_recv_many(&mut recv_batch)?;
        assert_eq!(try_received, 0);

        Ok::<(), io::Error>(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
async fn test_single_datagram() -> io::Result<()> {
    time::timeout(TEST_TIMEOUT, async {
        let sender = UdpSocket::bind("127.0.0.1:0").await?;
        let receiver = UdpSocket::bind("127.0.0.1:0").await?;
        let recv_addr = receiver.local_addr()?;
        let send_addr = sender.local_addr()?;

        let msg = b"single packet payload";
        let sent = sender
            .send_many(&[Datagram::to_addr(msg, recv_addr)])
            .await?;
        assert_eq!(sent, 1);

        let mut buf = [0u8; 64];
        let mut inbound = [InboundDatagram::new(&mut buf)];
        let received = receiver.recv_many(&mut inbound).await?;
        assert_eq!(received, 1);
        assert_eq!(inbound[0].payload(), msg);
        assert_eq!(inbound[0].source, Some(send_addr));
        assert!(!inbound[0].truncated);

        Ok::<(), io::Error>(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
async fn test_multiple_datagrams() -> io::Result<()> {
    time::timeout(TEST_TIMEOUT, async {
        let sender = UdpSocket::bind("127.0.0.1:0").await?;
        let receiver = UdpSocket::bind("127.0.0.1:0").await?;
        let recv_addr = receiver.local_addr()?;
        let send_addr = sender.local_addr()?;

        let p1 = b"packet one";
        let p2 = b"packet two is longer";
        let p3 = b"packet three";
        let p4 = b"packet four payload";

        let packets = [
            Datagram::to_addr(p1, recv_addr),
            Datagram::to_addr(p2, recv_addr),
            Datagram::to_addr(p3, recv_addr),
            Datagram::to_addr(p4, recv_addr),
        ];

        let sent = sender.send_many(&packets).await?;
        assert_eq!(sent, 4);

        time::sleep(Duration::from_millis(5)).await;

        let mut b1 = [0u8; 64];
        let mut b2 = [0u8; 64];
        let mut b3 = [0u8; 64];
        let mut b4 = [0u8; 64];

        let mut inbound = [
            InboundDatagram::new(&mut b1),
            InboundDatagram::new(&mut b2),
            InboundDatagram::new(&mut b3),
            InboundDatagram::new(&mut b4),
        ];

        let received = receiver.recv_many(&mut inbound).await?;
        assert_eq!(received, 4);

        assert_eq!(inbound[0].payload(), p1);
        assert_eq!(inbound[0].source, Some(send_addr));
        assert_eq!(inbound[1].payload(), p2);
        assert_eq!(inbound[1].source, Some(send_addr));
        assert_eq!(inbound[2].payload(), p3);
        assert_eq!(inbound[2].source, Some(send_addr));
        assert_eq!(inbound[3].payload(), p4);
        assert_eq!(inbound[3].source, Some(send_addr));

        Ok::<(), io::Error>(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
async fn test_multiple_destinations() -> io::Result<()> {
    time::timeout(TEST_TIMEOUT, async {
        let sender = UdpSocket::bind("127.0.0.1:0").await?;
        let r1 = UdpSocket::bind("127.0.0.1:0").await?;
        let r2 = UdpSocket::bind("127.0.0.1:0").await?;
        let r3 = UdpSocket::bind("127.0.0.1:0").await?;

        let addr1 = r1.local_addr()?;
        let addr2 = r2.local_addr()?;
        let addr3 = r3.local_addr()?;
        let send_addr = sender.local_addr()?;

        let packets = [
            Datagram::to_addr(b"to r1", addr1),
            Datagram::to_addr(b"to r2", addr2),
            Datagram::to_addr(b"to r3", addr3),
        ];

        let sent = sender.send_many(&packets).await?;
        assert_eq!(sent, 3);

        time::sleep(Duration::from_millis(5)).await;

        let mut b1 = [0u8; 32];
        let mut b2 = [0u8; 32];
        let mut b3 = [0u8; 32];

        let mut in1 = [InboundDatagram::new(&mut b1)];
        let mut in2 = [InboundDatagram::new(&mut b2)];
        let mut in3 = [InboundDatagram::new(&mut b3)];

        assert_eq!(r1.recv_many(&mut in1).await?, 1);
        assert_eq!(r2.recv_many(&mut in2).await?, 1);
        assert_eq!(r3.recv_many(&mut in3).await?, 1);

        assert_eq!(in1[0].payload(), b"to r1");
        assert_eq!(in1[0].source, Some(send_addr));
        assert_eq!(in2[0].payload(), b"to r2");
        assert_eq!(in2[0].source, Some(send_addr));
        assert_eq!(in3[0].payload(), b"to r3");
        assert_eq!(in3[0].source, Some(send_addr));

        Ok::<(), io::Error>(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
async fn test_connected_socket_batch() -> io::Result<()> {
    time::timeout(TEST_TIMEOUT, async {
        let s1 = UdpSocket::bind("127.0.0.1:0").await?;
        let s2 = UdpSocket::bind("127.0.0.1:0").await?;

        let addr1 = s1.local_addr()?;
        let addr2 = s2.local_addr()?;

        s1.connect(addr2).await?;
        s2.connect(addr1).await?;

        let msgs = [b"conn msg 1", b"conn msg 2", b"conn msg 3"];
        let packets = [
            Datagram::connected(msgs[0]),
            Datagram::connected(msgs[1]),
            Datagram::connected(msgs[2]),
        ];

        let sent = s1.send_many(&packets).await?;
        assert_eq!(sent, 3);

        time::sleep(Duration::from_millis(5)).await;

        let mut b1 = [0u8; 32];
        let mut b2 = [0u8; 32];
        let mut b3 = [0u8; 32];
        let mut inbound = [
            InboundDatagram::new(&mut b1),
            InboundDatagram::new(&mut b2),
            InboundDatagram::new(&mut b3),
        ];

        let received = s2.recv_many(&mut inbound).await?;
        assert_eq!(received, 3);
        assert_eq!(inbound[0].payload(), msgs[0]);
        assert_eq!(inbound[1].payload(), msgs[1]);
        assert_eq!(inbound[2].payload(), msgs[2]);

        Ok::<(), io::Error>(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
async fn test_partial_receives() -> io::Result<()> {
    time::timeout(TEST_TIMEOUT, async {
        let sender = UdpSocket::bind("127.0.0.1:0").await?;
        let receiver = UdpSocket::bind("127.0.0.1:0").await?;
        let recv_addr = receiver.local_addr()?;

        sender
            .send_many(&[
                Datagram::to_addr(b"alpha", recv_addr),
                Datagram::to_addr(b"beta", recv_addr),
            ])
            .await?;

        time::sleep(Duration::from_millis(5)).await;

        let mut buffers: Vec<Vec<u8>> = (0..8).map(|_| vec![0u8; 64]).collect();
        let mut inbound: Vec<InboundDatagram<'_>> = buffers
            .iter_mut()
            .map(|b| InboundDatagram::new(b.as_mut_slice()))
            .collect();

        let received = receiver.recv_many(&mut inbound).await?;
        assert_eq!(received, 2);
        assert_eq!(inbound[0].payload(), b"alpha");
        assert_eq!(inbound[1].payload(), b"beta");

        let err = receiver.try_recv_many(&mut inbound).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

        Ok::<(), io::Error>(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
async fn test_zero_length_datagram() -> io::Result<()> {
    time::timeout(TEST_TIMEOUT, async {
        let sender = UdpSocket::bind("127.0.0.1:0").await?;
        let receiver = UdpSocket::bind("127.0.0.1:0").await?;
        let recv_addr = receiver.local_addr()?;

        let sent = sender
            .send_many(&[Datagram::to_addr(b"", recv_addr)])
            .await?;
        assert_eq!(sent, 1);

        time::sleep(Duration::from_millis(5)).await;

        let mut buf = [0u8; 32];
        let mut inbound = [InboundDatagram::new(&mut buf)];
        let received = receiver.recv_many(&mut inbound).await?;
        assert_eq!(received, 1);
        assert_eq!(inbound[0].len, 0);
        assert_eq!(inbound[0].payload(), b"");
        assert!(!inbound[0].truncated);

        Ok::<(), io::Error>(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
async fn test_zero_capacity_receive_buffer() -> io::Result<()> {
    time::timeout(TEST_TIMEOUT, async {
        let sender = UdpSocket::bind("127.0.0.1:0").await?;
        let receiver = UdpSocket::bind("127.0.0.1:0").await?;
        let recv_addr = receiver.local_addr()?;

        sender
            .send_many(&[Datagram::to_addr(b"non-empty", recv_addr)])
            .await?;
        time::sleep(Duration::from_millis(5)).await;

        let mut empty_buf = [0u8; 0];
        let mut inbound = [InboundDatagram::new(&mut empty_buf)];
        let received = receiver.recv_many(&mut inbound).await?;
        assert_eq!(received, 1);
        assert_eq!(inbound[0].len, 0);
        assert_eq!(inbound[0].payload(), b"");
        assert!(
            inbound[0].truncated,
            "Zero capacity buffer should mark truncation"
        );

        Ok::<(), io::Error>(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
async fn test_datagram_truncation() -> io::Result<()> {
    time::timeout(TEST_TIMEOUT, async {
        let sender = UdpSocket::bind("127.0.0.1:0").await?;
        let receiver = UdpSocket::bind("127.0.0.1:0").await?;
        let recv_addr = receiver.local_addr()?;

        let big_payload = vec![42u8; 100];
        sender
            .send_many(&[Datagram::to_addr(&big_payload, recv_addr)])
            .await?;

        time::sleep(Duration::from_millis(5)).await;

        let mut small_buf = [0u8; 30];
        let mut inbound = [InboundDatagram::new(&mut small_buf)];
        let received = receiver.recv_many(&mut inbound).await?;
        assert_eq!(received, 1);
        assert_eq!(inbound[0].len, 30);
        assert_eq!(inbound[0].payload(), &big_payload[..30]);
        assert!(
            inbound[0].truncated,
            "Expected packet to be marked truncated"
        );

        Ok::<(), io::Error>(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
async fn test_oversized_batch_bounded() -> io::Result<()> {
    time::timeout(TEST_TIMEOUT, async {
        let sender = UdpSocket::bind("127.0.0.1:0").await?;
        let receiver = UdpSocket::bind("127.0.0.1:0").await?;
        let recv_addr = receiver.local_addr()?;

        // Supply 1200 packets (> MAX_BATCH_SIZE 1024)
        let large_count = 1200;
        let payload = [0x55u8; 32];
        let outbound: Vec<Datagram<'_>> = (0..large_count)
            .map(|_| Datagram::to_addr(&payload, recv_addr))
            .collect();

        // send_many must clamp to MAX_BATCH_SIZE and not fail with EINVAL
        let sent = sender.send_many(&outbound).await?;
        assert!(sent <= MAX_BATCH_SIZE);
        assert!(sent > 0);

        Ok::<(), io::Error>(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
async fn test_ipv6_batch() -> io::Result<()> {
    time::timeout(TEST_TIMEOUT, async {
        let sender = match UdpSocket::bind("[::1]:0").await {
            Ok(s) => s,
            Err(_) => {
                eprintln!("IPv6 loopback not available on this host, skipping");
                return Ok(());
            }
        };
        let receiver = UdpSocket::bind("[::1]:0").await?;
        let recv_addr = receiver.local_addr()?;
        let send_addr = sender.local_addr()?;

        assert!(recv_addr.is_ipv6());
        assert!(send_addr.is_ipv6());

        let msgs = [b"ipv6-pkt-1", b"ipv6-pkt-2"];
        let packets = [
            Datagram::to_addr(msgs[0], recv_addr),
            Datagram::to_addr(msgs[1], recv_addr),
        ];

        let sent = sender.send_many(&packets).await?;
        assert_eq!(sent, 2);

        time::sleep(Duration::from_millis(5)).await;

        let mut b1 = [0u8; 32];
        let mut b2 = [0u8; 32];
        let mut inbound = [InboundDatagram::new(&mut b1), InboundDatagram::new(&mut b2)];

        let received = receiver.recv_many(&mut inbound).await?;
        assert_eq!(received, 2);
        assert_eq!(inbound[0].payload(), msgs[0]);
        assert_eq!(inbound[0].source, Some(send_addr));
        assert_eq!(inbound[1].payload(), msgs[1]);
        assert_eq!(inbound[1].source, Some(send_addr));

        Ok::<(), io::Error>(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
async fn test_cancellation_before_readiness() -> io::Result<()> {
    time::timeout(TEST_TIMEOUT, async {
        let receiver = UdpSocket::bind("127.0.0.1:0").await?;
        let mut buf = [0u8; 64];
        let mut inbound = [InboundDatagram::new(&mut buf)];

        let res = time::timeout(Duration::from_millis(20), receiver.recv_many(&mut inbound)).await;
        assert!(res.is_err(), "Expected timeout");

        let sender = UdpSocket::bind("127.0.0.1:0").await?;
        let recv_addr = receiver.local_addr()?;
        sender
            .send_many(&[Datagram::to_addr(b"after cancel", recv_addr)])
            .await?;

        let received = receiver.recv_many(&mut inbound).await?;
        assert_eq!(received, 1);
        assert_eq!(inbound[0].payload(), b"after cancel");

        Ok::<(), io::Error>(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
async fn test_readiness_clearing_and_rearm() -> io::Result<()> {
    time::timeout(TEST_TIMEOUT, async {
        let sender = UdpSocket::bind("127.0.0.1:0").await?;
        let receiver = UdpSocket::bind("127.0.0.1:0").await?;
        let recv_addr = receiver.local_addr()?;

        // Send 1 packet
        sender
            .send_many(&[Datagram::to_addr(b"packet 1", recv_addr)])
            .await?;
        time::sleep(Duration::from_millis(5)).await;

        let mut b1 = [0u8; 32];
        let mut inbound = [InboundDatagram::new(&mut b1)];

        // Drain the packet
        let n = receiver.recv_many(&mut inbound).await?;
        assert_eq!(n, 1);
        assert_eq!(inbound[0].payload(), b"packet 1");

        // Now socket is empty; try_recv_many must return WouldBlock and clear readiness
        let err = receiver.try_recv_many(&mut inbound).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

        // Send second packet; should trigger rearmed readiness notification
        sender
            .send_many(&[Datagram::to_addr(b"packet 2", recv_addr)])
            .await?;
        receiver.readable().await?;

        let n2 = receiver.try_recv_many(&mut inbound)?;
        assert_eq!(n2, 1);
        assert_eq!(inbound[0].payload(), b"packet 2");

        Ok::<(), io::Error>(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
async fn test_concurrent_send_recv() -> io::Result<()> {
    time::timeout(TEST_TIMEOUT, async {
        let s1 = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
        let s2 = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);

        let addr2 = s2.local_addr()?;

        let total_packets = 200;
        let batch_size = 16;

        let s1_clone = s1.clone();
        let send_task = tokio::spawn(async move {
            let msg = [7u8; 128];
            let mut sent = 0;
            while sent < total_packets {
                let to_send = (total_packets - sent).min(batch_size);
                let batch: Vec<Datagram<'_>> = (0..to_send)
                    .map(|_| Datagram::to_addr(&msg, addr2))
                    .collect();
                let n = s1_clone.send_many(&batch).await.unwrap();
                sent += n;
                tokio::task::yield_now().await;
            }
            sent
        });

        let s2_clone = s2.clone();
        let recv_task = tokio::spawn(async move {
            let mut buffers = vec![vec![0u8; 256]; batch_size];
            let mut received = 0;
            while received < total_packets {
                let mut inbound: Vec<InboundDatagram<'_>> = buffers
                    .iter_mut()
                    .map(|b| InboundDatagram::new(b.as_mut_slice()))
                    .collect();
                let n = s2_clone.recv_many(&mut inbound).await.unwrap();
                received += n;
            }
            received
        });

        let (sent_count, recv_count) = tokio::try_join!(send_task, recv_task)?;
        assert_eq!(sent_count, total_packets);
        assert_eq!(recv_count, total_packets);

        Ok::<(), io::Error>(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
async fn test_buffer_reuse() -> io::Result<()> {
    time::timeout(TEST_TIMEOUT, async {
        let sender = UdpSocket::bind("127.0.0.1:0").await?;
        let receiver = UdpSocket::bind("127.0.0.1:0").await?;
        let recv_addr = receiver.local_addr()?;

        let mut b0 = vec![0u8; 64];
        let mut b1 = vec![0u8; 64];

        for iter in 0..5 {
            let data = format!("iter-{}", iter);
            let batch = [
                Datagram::to_addr(data.as_bytes(), recv_addr),
                Datagram::to_addr(data.as_bytes(), recv_addr),
            ];
            sender.send_many(&batch).await?;

            let mut inbound = [InboundDatagram::new(&mut b0), InboundDatagram::new(&mut b1)];
            let n = receiver.recv_many(&mut inbound).await?;
            assert_eq!(n, 2);
            assert_eq!(inbound[0].payload(), data.as_bytes());
            assert_eq!(inbound[1].payload(), data.as_bytes());
        }

        Ok::<(), io::Error>(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
async fn test_poll_methods() -> io::Result<()> {
    time::timeout(TEST_TIMEOUT, async {
        let sender = UdpSocket::bind("127.0.0.1:0").await?;
        let receiver = UdpSocket::bind("127.0.0.1:0").await?;
        let recv_addr = receiver.local_addr()?;
        let send_addr = sender.local_addr()?;

        let p1 = b"poll msg 1";
        let p2 = b"poll msg 2";
        let packets = [
            Datagram::to_addr(p1, recv_addr),
            Datagram::to_addr(p2, recv_addr),
        ];

        let sent = poll_fn(|cx| sender.poll_send_many(cx, &packets)).await?;
        assert_eq!(sent, 2);

        time::sleep(Duration::from_millis(5)).await;

        let mut b1 = [0u8; 32];
        let mut b2 = [0u8; 32];
        let mut inbound = [InboundDatagram::new(&mut b1), InboundDatagram::new(&mut b2)];

        let received = poll_fn(|cx| receiver.poll_recv_many(cx, &mut inbound)).await?;
        assert_eq!(received, 2);
        assert_eq!(inbound[0].payload(), p1);
        assert_eq!(inbound[0].source, Some(send_addr));
        assert_eq!(inbound[1].payload(), p2);
        assert_eq!(inbound[1].source, Some(send_addr));

        Ok::<(), io::Error>(())
    })
    .await
    .expect("test timed out")
}

#[tokio::test]
async fn test_recv_many_heap_fallback() -> io::Result<()> {
    time::timeout(TEST_TIMEOUT, async {
        let sender = UdpSocket::bind("127.0.0.1:0").await?;
        let receiver = UdpSocket::bind("127.0.0.1:0").await?;
        let recv_addr = receiver.local_addr()?;
        let send_addr = sender.local_addr()?;

        // Test boundary at 65 datagrams (exceeding STACK_BATCH_SIZE = 64 by 1)
        // and 80 datagrams to verify receive-side heap allocation fallback.
        for &batch_count in &[65, 80] {
            let payloads: Vec<Vec<u8>> = (0..batch_count)
                .map(|i| format!("heap-pkt-{i:03}").into_bytes())
                .collect();
            let outbound: Vec<Datagram<'_>> = payloads
                .iter()
                .map(|p| Datagram::to_addr(p, recv_addr))
                .collect();

            let sent = sender.send_many(&outbound).await?;
            assert_eq!(sent, batch_count);

            let mut raw_bufs: Vec<Vec<u8>> = (0..batch_count).map(|_| vec![0u8; 64]).collect();
            let mut inbound: Vec<InboundDatagram<'_>> = raw_bufs
                .iter_mut()
                .map(|b| InboundDatagram::new(b.as_mut_slice()))
                .collect();

            let mut total_received = 0;
            while total_received < batch_count {
                let n = receiver.recv_many(&mut inbound[total_received..]).await?;
                assert!(n > 0);
                total_received += n;
            }

            assert_eq!(total_received, batch_count);
            for i in 0..batch_count {
                assert_eq!(inbound[i].payload(), payloads[i].as_slice());
                assert_eq!(inbound[i].source, Some(send_addr));
                assert!(!inbound[i].truncated);
            }
        }

        Ok::<(), io::Error>(())
    })
    .await
    .expect("test timed out")
}
