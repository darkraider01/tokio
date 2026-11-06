use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use tokio::sync::mpsc;

struct SynchronousDropWaker {
    rx: Mutex<Option<mpsc::Receiver<()>>>,
}

impl Wake for SynchronousDropWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Ok(mut lock) = self.rx.try_lock() {
            let _ = lock.take();
        }
    }
}

#[test]
fn synchronous_waker_dropping_receiver() {
    let (tx, rx) = mpsc::channel::<()>(1);
    let drop_waker = Arc::new(SynchronousDropWaker {
        rx: Mutex::new(Some(rx)),
    });
    let waker = Waker::from(drop_waker.clone());
    let mut cx = Context::from_waker(&waker);

    // Register the waker on the receiver
    {
        let mut rx_guard = drop_waker.rx.lock().unwrap();
        let rx = rx_guard.as_mut().unwrap();
        assert_eq!(rx.poll_recv(&mut cx), Poll::Pending);
    }

    // Run the send on a background thread with a timeout so the test doesn't hang forever
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let permit = tx.try_reserve().unwrap();
        permit.send(());
        let _ = done_tx.send(());
    });

    match done_rx.recv_timeout(std::time::Duration::from_millis(500)) {
        Ok(()) => {
            println!("Test finished successfully without deadlock");
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!(
                "DEADLOCK DETECTED: sender hung waiting for Rx::drop which was waiting for SendGuard!"
            );
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

#[tokio::test]
async fn rejected_send_destructor_reentering_channel() {
    struct ReenteringMsg {
        tx: mpsc::Sender<ReenteringMsg>,
        dropped: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Drop for ReenteringMsg {
        fn drop(&mut self) {
            let count = self
                .dropped
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if count == 0 {
                // Reenter the exact same channel during drop on the rejected-send path once
                let _ = self.tx.try_send(ReenteringMsg {
                    tx: self.tx.clone(),
                    dropped: self.dropped.clone(),
                });
            }
        }
    }

    let (tx, rx) = mpsc::channel(1);
    let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let permit = tx.reserve().await.unwrap();
    drop(rx);

    // Send on permit after Rx is dropped.
    // This triggers the rejected-send path.
    // The message destructor is invoked, which attempts to reenter tx.try_send.
    // If the active-send guard was held during drop(value), it would corrupt or deadlock.
    permit.send(ReenteringMsg {
        tx: tx.clone(),
        dropped: dropped.clone(),
    });

    // Both the original message and the re-entered message should have dropped cleanly
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn rejected_send_destructor_dropping_another_sender() {
    struct SenderDropper {
        _other_tx: Option<mpsc::Sender<()>>,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Drop for SenderDropper {
        fn drop(&mut self) {
            // Drop the sender handle from inside the message destructor
            let _ = self._other_tx.take();
            self.dropped
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    let (tx1, rx1) = mpsc::channel(1);
    let (tx2, _rx2) = mpsc::channel(1);
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let permit = tx1.reserve().await.unwrap();
    drop(rx1);

    permit.send(SenderDropper {
        _other_tx: Some(tx2),
        dropped: dropped.clone(),
    });

    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn rejected_send_while_holding_application_mutex() {
    struct MutexAcquiringMsg {
        lock: Arc<Mutex<usize>>,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Drop for MutexAcquiringMsg {
        fn drop(&mut self) {
            // Inside the destructor, acquire the application lock
            let mut val = self.lock.lock().unwrap();
            *val += 1;
            self.dropped
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    let (tx, rx) = mpsc::channel(1);
    let app_lock = Arc::new(Mutex::new(0));
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let permit = tx.reserve().await.unwrap();
    drop(rx);

    // Call send while the current thread does NOT hold app_lock,
    // but the destructor acquires it cleanly.
    permit.send(MutexAcquiringMsg {
        lock: app_lock.clone(),
        dropped: dropped.clone(),
    });

    assert_eq!(*app_lock.lock().unwrap(), 1);
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
}
