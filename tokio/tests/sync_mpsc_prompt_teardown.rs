use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use tokio::sync::{mpsc, oneshot};

struct CountedDrop {
    counter: Arc<AtomicUsize>,
}

impl CountedDrop {
    fn new(counter: &Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, SeqCst);
        Self {
            counter: counter.clone(),
        }
    }
}

impl Drop for CountedDrop {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, SeqCst);
    }
}

#[tokio::test]
async fn permit_send_after_receiver_drop_drops_immediately() {
    let (tx, rx) = mpsc::channel::<CountedDrop>(1);
    let active = Arc::new(AtomicUsize::new(0));

    let permit = tx.reserve().await.unwrap();
    let msg = CountedDrop::new(&active);
    assert_eq!(active.load(SeqCst), 1);

    drop(rx);

    // Send on permit after receiver is dropped.
    permit.send(msg);

    // Message must be dropped immediately, even though tx is still alive!
    assert_eq!(active.load(SeqCst), 0);
    assert!(tx.is_closed());
}

#[tokio::test]
async fn owned_permit_send_after_receiver_drop_drops_immediately() {
    let (tx, rx) = mpsc::channel::<CountedDrop>(1);
    let active = Arc::new(AtomicUsize::new(0));

    let permit = tx.clone().reserve_owned().await.unwrap();
    let msg = CountedDrop::new(&active);
    assert_eq!(active.load(SeqCst), 1);

    drop(rx);

    // Send on owned permit after receiver is dropped.
    permit.send(msg);

    // Message must be dropped immediately.
    assert_eq!(active.load(SeqCst), 0);
    assert!(tx.is_closed());
}

#[tokio::test]
async fn receiver_close_still_receives_reserved_permits() {
    let (tx, mut rx) = mpsc::channel::<usize>(3);

    let p1 = tx.reserve().await.unwrap();
    let p2 = tx.reserve().await.unwrap();
    let p3 = tx.reserve().await.unwrap();

    rx.close();

    p1.send(10);
    p2.send(20);
    p3.send(30);

    assert_eq!(rx.recv().await, Some(10));
    assert_eq!(rx.recv().await, Some(20));
    assert_eq!(rx.recv().await, Some(30));
    assert_eq!(rx.recv().await, None);
}

#[tokio::test]
async fn late_oneshot_envelope_cancels_promptly() {
    let (tx, rx) = mpsc::channel::<oneshot::Sender<()>>(1);
    let permit = tx.reserve().await.unwrap();
    let (reply_tx, mut reply_rx) = oneshot::channel();

    drop(rx);
    permit.send(reply_tx);

    // Reply receiver must see Closed immediately, without needing tx to be dropped.
    assert!(matches!(
        reply_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    ));
    drop(tx);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_senders_racing_with_receiver_drop() {
    for _ in 0..50 {
        let (tx, rx) = mpsc::channel::<CountedDrop>(4);
        let active = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(std::sync::Barrier::new(5));
        let mut handles = Vec::new();

        for _ in 0..4 {
            let tx = tx.clone();
            let active = active.clone();
            let barrier = barrier.clone();

            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..20 {
                    if let Ok(permit) = tx.try_reserve() {
                        let msg = CountedDrop::new(&active);
                        permit.send(msg);
                    }
                }
            }));
        }

        barrier.wait();
        // Concurrently drop the receiver while producers are sending.
        drop(rx);

        for h in handles {
            h.join().unwrap();
        }

        // Keep tx alive to prove that everything drained without waiting for channel destruction.
        assert_eq!(active.load(SeqCst), 0);
        drop(tx);
    }
}

#[tokio::test]
async fn panicking_message_destructor_does_not_leak_other_messages() {
    struct MaybePanicDrop {
        should_panic: bool,
        active: Arc<AtomicUsize>,
    }

    impl Drop for MaybePanicDrop {
        fn drop(&mut self) {
            self.active.fetch_sub(1, SeqCst);
            if self.should_panic {
                panic!("boom from message drop");
            }
        }
    }

    let (tx, rx) = mpsc::channel(4);
    let active = Arc::new(AtomicUsize::new(3));

    let _ = tx.try_send(MaybePanicDrop {
        should_panic: false,
        active: active.clone(),
    });
    let _ = tx.try_send(MaybePanicDrop {
        should_panic: true,
        active: active.clone(),
    });
    let _ = tx.try_send(MaybePanicDrop {
        should_panic: false,
        active: active.clone(),
    });

    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        drop(rx);
    }));

    assert!(result.is_err());
    // All 3 messages must have been dropped despite the panic in message 2.
    assert_eq!(active.load(SeqCst), 0);
    drop(tx);
}

#[tokio::test]
async fn destructor_reentrancy() {
    struct ReentrantDrop {
        tx: mpsc::Sender<usize>,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for ReentrantDrop {
        fn drop(&mut self) {
            self.dropped.store(true, SeqCst);
            // Attempt to send or reserve inside destructor.
            let _ = self.tx.try_send(999);
        }
    }

    let (tx, rx) = mpsc::channel(2);
    let (tx2, _rx2) = mpsc::channel(2);
    let dropped = Arc::new(AtomicBool::new(false));

    let permit = tx.reserve().await.unwrap();
    drop(rx);

    permit.send(ReentrantDrop {
        tx: tx2,
        dropped: dropped.clone(),
    });

    assert!(dropped.load(SeqCst));
}

#[tokio::test]
async fn block_boundary_transitions() {
    const CAP: usize = 32;
    let (tx, rx) = mpsc::channel::<CountedDrop>(CAP * 3);
    let active = Arc::new(AtomicUsize::new(0));

    // Send across multiple blocks (each block is 32 slots).
    for _ in 0..50 {
        let permit = tx.reserve().await.unwrap();
        permit.send(CountedDrop::new(&active));
    }
    assert_eq!(active.load(SeqCst), 50);

    // Reserve another 20 permits before dropping rx.
    let mut permits = Vec::new();
    for _ in 0..20 {
        permits.push(tx.reserve().await.unwrap());
    }

    drop(rx);
    // The initial 50 messages were drained and dropped upon rx drop.
    assert_eq!(active.load(SeqCst), 0);

    // Now send through the 20 reserved permits across block boundaries after rx drop.
    for permit in permits {
        permit.send(CountedDrop::new(&active));
        assert_eq!(active.load(SeqCst), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn racing_try_send_and_receiver_drop_never_leaks_or_double_drops() {
    for _ in 0..200 {
        let (tx, rx) = mpsc::channel::<CountedDrop>(2);
        let active = Arc::new(AtomicUsize::new(0));
        let flag = active.clone();
        let sender = tx.clone();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let sender_barrier = barrier.clone();
        let handle = std::thread::spawn(move || {
            sender_barrier.wait();
            let _ = sender.try_send(CountedDrop::new(&flag));
        });
        barrier.wait();
        drop(rx);
        handle.join().unwrap();
        assert_eq!(active.load(SeqCst), 0);
        drop(tx);
    }
}

#[tokio::test]
async fn unbounded_send_after_receiver_drop_promptly_drops() {
    let (tx, rx) = mpsc::unbounded_channel::<CountedDrop>();
    let active = Arc::new(AtomicUsize::new(0));

    let msg = CountedDrop::new(&active);
    assert_eq!(active.load(SeqCst), 1);

    drop(rx);

    // Unbounded send after receiver is dropped returns an error containing the message.
    let err = tx.send(msg).unwrap_err();
    assert_eq!(active.load(SeqCst), 1);
    drop(err);
    assert_eq!(active.load(SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unbounded_racing_send_and_receiver_drop_never_leaks() {
    for _ in 0..200 {
        let (tx, rx) = mpsc::unbounded_channel::<CountedDrop>();
        let active = Arc::new(AtomicUsize::new(0));
        let flag = active.clone();
        let sender = tx.clone();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let sender_barrier = barrier.clone();
        let handle = std::thread::spawn(move || {
            sender_barrier.wait();
            let _ = sender.send(CountedDrop::new(&flag));
        });
        barrier.wait();
        drop(rx);
        handle.join().unwrap();
        assert_eq!(active.load(SeqCst), 0);
        drop(tx);
    }
}
