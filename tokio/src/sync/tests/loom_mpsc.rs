use crate::sync::mpsc::{self, BLOCK_CAP};

use loom::future::block_on;
use loom::sync::Arc;
use loom::thread;
use std::future::poll_fn;
use tokio_test::assert_ok;

#[test]
fn closing_tx() {
    loom::model(|| {
        let (tx, mut rx) = mpsc::channel(16);

        thread::spawn(move || {
            tx.try_send(()).unwrap();
            drop(tx);
        });

        let v = block_on(rx.recv());
        assert!(v.is_some());

        let v = block_on(rx.recv());
        assert!(v.is_none());
    });
}

#[test]
fn closing_unbounded_tx() {
    loom::model(|| {
        let (tx, mut rx) = mpsc::unbounded_channel();

        thread::spawn(move || {
            tx.send(()).unwrap();
            drop(tx);
        });

        let v = block_on(rx.recv());
        assert!(v.is_some());

        let v = block_on(rx.recv());
        assert!(v.is_none());
    });
}

#[test]
fn closing_bounded_rx() {
    loom::model(|| {
        let (tx1, rx) = mpsc::channel::<()>(16);
        let tx2 = tx1.clone();
        thread::spawn(move || {
            drop(rx);
        });

        block_on(tx1.closed());
        block_on(tx2.closed());
    });
}

#[test]
fn closing_and_sending() {
    loom::model(|| {
        let (tx1, mut rx) = mpsc::channel::<()>(16);
        let tx1 = Arc::new(tx1);
        let tx2 = tx1.clone();

        let th1 = thread::spawn(move || {
            tx1.try_send(()).unwrap();
        });

        let th2 = thread::spawn(move || {
            block_on(tx2.closed());
        });

        let th3 = thread::spawn(move || {
            let v = block_on(rx.recv());
            assert!(v.is_some());
            drop(rx);
        });

        assert_ok!(th1.join());
        assert_ok!(th2.join());
        assert_ok!(th3.join());
    });
}

#[test]
fn closing_unbounded_rx() {
    loom::model(|| {
        let (tx1, rx) = mpsc::unbounded_channel::<()>();
        let tx2 = tx1.clone();
        thread::spawn(move || {
            drop(rx);
        });

        block_on(tx1.closed());
        block_on(tx2.closed());
    });
}

#[test]
fn dropping_tx() {
    loom::model(|| {
        let (tx, mut rx) = mpsc::channel::<()>(16);

        for _ in 0..2 {
            let tx = tx.clone();
            thread::spawn(move || {
                drop(tx);
            });
        }
        drop(tx);

        let v = block_on(rx.recv());
        assert!(v.is_none());
    });
}

#[test]
fn dropping_unbounded_tx() {
    loom::model(|| {
        let (tx, mut rx) = mpsc::unbounded_channel::<()>();

        for _ in 0..2 {
            let tx = tx.clone();
            thread::spawn(move || {
                drop(tx);
            });
        }
        drop(tx);

        let v = block_on(rx.recv());
        assert!(v.is_none());
    });
}

#[test]
fn try_recv() {
    loom::model(|| {
        use crate::sync::{Semaphore, mpsc};
        use loom::sync::{Arc, Mutex};

        const PERMITS: usize = 2;
        const TASKS: usize = 2;
        const CYCLES: usize = 1;

        struct Context {
            sem: Arc<Semaphore>,
            tx: mpsc::Sender<()>,
            rx: Mutex<mpsc::Receiver<()>>,
        }

        fn run(ctx: &Context) {
            block_on(async {
                let permit = ctx.sem.acquire().await;
                assert_ok!(ctx.rx.lock().unwrap().try_recv());
                crate::task::yield_now().await;
                assert_ok!(ctx.tx.clone().try_send(()));
                drop(permit);
            });
        }

        let (tx, rx) = mpsc::channel(PERMITS);
        let sem = Arc::new(Semaphore::new(PERMITS));
        let ctx = Arc::new(Context {
            sem,
            tx,
            rx: Mutex::new(rx),
        });

        for _ in 0..PERMITS {
            assert_ok!(ctx.tx.clone().try_send(()));
        }

        let mut threads = Vec::new();

        for _ in 0..TASKS {
            let ctx = ctx.clone();

            threads.push(thread::spawn(move || {
                run(&ctx);
            }));
        }

        run(&ctx);

        for thread in threads {
            thread.join().unwrap();
        }
    });
}

#[test]
fn len_nonzero_after_send() {
    loom::model(|| {
        let (send, recv) = mpsc::channel(10);
        let send2 = send.clone();

        let join = thread::spawn(move || {
            block_on(send2.send("message2")).unwrap();
        });

        block_on(send.send("message1")).unwrap();
        assert!(recv.len() != 0);

        join.join().unwrap();
    });
}

#[test]
fn nonempty_after_send() {
    loom::model(|| {
        let (send, recv) = mpsc::channel(10);
        let send2 = send.clone();

        let join = thread::spawn(move || {
            block_on(send2.send("message2")).unwrap();
        });

        block_on(send.send("message1")).unwrap();
        assert!(!recv.is_empty());

        join.join().unwrap();
    });
}

#[test]
fn is_empty_during_close() {
    loom::model(|| {
        let (tx, rx) = mpsc::channel::<()>(1);

        let th1 = thread::spawn(move || {
            assert!(rx.is_empty());
        });

        drop(tx);

        th1.join().unwrap();
    });
}

fn len_during_close_helper(n: usize) {
    loom::model(move || {
        let (tx, rx) = mpsc::channel::<()>(n + 1);

        for _ in 0..n {
            tx.try_send(()).unwrap();
        }

        let th1 = thread::spawn(move || {
            assert_eq!(rx.len(), n);
        });

        drop(tx);

        th1.join().unwrap();
    });
}

#[test]
fn len_during_close_0() {
    len_during_close_helper(0);
}

#[test]
fn len_during_close_1() {
    len_during_close_helper(1);
}

#[test]
fn len_during_close_block_cap() {
    len_during_close_helper(BLOCK_CAP);
}

#[test]
fn len_during_close_block_cap_plus_1() {
    len_during_close_helper(BLOCK_CAP + 1);
}

#[test]
fn loom_permit_send_and_rx_drop() {
    loom::model(|| {
        use loom::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::SeqCst;

        struct Msg(Arc<AtomicUsize>);
        impl Drop for Msg {
            fn drop(&mut self) {
                self.0.fetch_add(1, SeqCst);
            }
        }

        let (tx, rx) = mpsc::channel(1);
        let permit = block_on(tx.clone().reserve_owned()).unwrap();
        let dropped = Arc::new(AtomicUsize::new(0));
        let flag = dropped.clone();

        let th1 = thread::spawn(move || {
            permit.send(Msg(flag));
        });

        let th2 = thread::spawn(move || {
            drop(rx);
        });

        th1.join().unwrap();
        th2.join().unwrap();

        assert_eq!(dropped.load(SeqCst), 1);
        drop(tx);
    });
}

#[test]
fn loom_racing_send_and_rx_drop() {
    loom::model(|| {
        use loom::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::SeqCst;

        struct Msg(Arc<AtomicUsize>);
        impl Drop for Msg {
            fn drop(&mut self) {
                self.0.fetch_add(1, SeqCst);
            }
        }

        let (tx, rx) = mpsc::channel(1);
        let dropped = Arc::new(AtomicUsize::new(0));
        let flag = dropped.clone();

        let th1 = thread::spawn(move || {
            let _ = block_on(tx.send(Msg(flag)));
        });

        let th2 = thread::spawn(move || {
            drop(rx);
        });

        th1.join().unwrap();
        th2.join().unwrap();

        assert_eq!(dropped.load(SeqCst), 1);
    });
}

#[test]
fn loom_two_senders_racing_rx_drop() {
    loom::model(|| {
        use loom::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering::SeqCst;

        struct Msg(Arc<AtomicUsize>);
        impl Drop for Msg {
            fn drop(&mut self) {
                self.0.fetch_add(1, SeqCst);
            }
        }

        let (tx1, rx) = mpsc::channel(2);
        let tx2 = tx1.clone();
        let dropped = Arc::new(AtomicUsize::new(0));

        let flag1 = dropped.clone();
        let th1 = thread::spawn(move || {
            let _ = tx1.try_send(Msg(flag1));
        });

        let flag2 = dropped.clone();
        let th2 = thread::spawn(move || {
            let _ = tx2.try_send(Msg(flag2));
        });

        let th3 = thread::spawn(move || {
            drop(rx);
        });

        th1.join().unwrap();
        th2.join().unwrap();
        th3.join().unwrap();

        assert_eq!(dropped.load(SeqCst), 2);
    });
}
