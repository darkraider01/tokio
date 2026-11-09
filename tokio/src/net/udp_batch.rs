//! Experimental batched UDP operations using Linux `recvmmsg(2)` and `sendmmsg(2)`.
//!
//! This module provides batched UDP I/O primitives that integrate with Tokio's
//! edge-triggered epoll readiness system, cooperative task scheduling, and cancellation
//! safety guarantees.

#![cfg(all(target_os = "linux", feature = "net"))]

use crate::io::Interest;
use crate::net::UdpSocket;

use std::io;
use std::mem::MaybeUninit;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::fd::AsRawFd;
use std::task::{ready, Context, Poll};

/// Maximum batch size allocated on the stack to avoid heap allocations
/// on the hot path while keeping stack frames bounded.
pub const STACK_BATCH_SIZE: usize = 64;

/// Upper bound on the number of datagrams processed in a single system call.
///
/// Bounded by Linux `UIO_MAXIOV` (1024) to avoid kernel `EINVAL`, and to preserve
/// task scheduling fairness by preventing a single asynchronous poll from
/// monopolizing a worker thread for an unbounded batch.
pub const MAX_BATCH_SIZE: usize = 1024;

/// An outbound UDP datagram for batched transmission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Datagram<'a> {
    /// Payload slice to transmit.
    pub buf: &'a [u8],
    /// Destination address.
    ///
    /// If `None`, the socket must be connected (e.g. via [`UdpSocket::connect`]).
    /// If `Some`, the datagram is transmitted to the specified remote address.
    pub destination: Option<SocketAddr>,
}

impl<'a> Datagram<'a> {
    /// Creates a new datagram with an optional destination address.
    #[inline]
    pub const fn new(buf: &'a [u8], destination: Option<SocketAddr>) -> Self {
        Self { buf, destination }
    }

    /// Creates a new datagram targeted to a specific destination address.
    #[inline]
    pub const fn to_addr(buf: &'a [u8], destination: SocketAddr) -> Self {
        Self {
            buf,
            destination: Some(destination),
        }
    }

    /// Creates a new datagram for a connected socket (implicit destination).
    #[inline]
    pub const fn connected(buf: &'a [u8]) -> Self {
        Self {
            buf,
            destination: None,
        }
    }
}

/// An inbound UDP datagram buffer and reception metadata.
#[derive(Debug)]
pub struct InboundDatagram<'a> {
    /// User-provided buffer to store the received payload.
    pub buf: &'a mut [u8],
    /// Number of bytes actually written into `buf`.
    pub len: usize,
    /// Originating address of the received datagram.
    pub source: Option<SocketAddr>,
    /// Set to true if the kernel datagram was larger than `buf.len()` and truncated.
    pub truncated: bool,
}

impl<'a> InboundDatagram<'a> {
    /// Creates an empty inbound datagram wrapper around a mutable slice.
    #[inline]
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self {
            buf,
            len: 0,
            source: None,
            truncated: false,
        }
    }

    /// Returns the received payload slice, or an empty slice if nothing was received.
    #[inline]
    pub fn payload(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

/// Extension trait providing batched UDP transmission and reception on [`UdpSocket`].
pub trait UdpSocketBatchExt {
    /// Sends multiple datagrams using Linux `sendmmsg(2)`.
    ///
    /// Returns the number of datagrams successfully transmitted. This may be
    /// less than `packets.len()` if the socket buffer becomes full (partial send)
    /// or if `packets.len() > MAX_BATCH_SIZE`. Callers should inspect the return
    /// value and advance their slice accordingly.
    fn send_many(
        &self,
        packets: &[Datagram<'_>],
    ) -> impl std::future::Future<Output = io::Result<usize>> + Send;

    /// Attempts to send multiple datagrams without blocking.
    ///
    /// If the socket is not ready to write, returns `Err(io::ErrorKind::WouldBlock)`.
    fn try_send_many(&self, packets: &[Datagram<'_>]) -> io::Result<usize>;

    /// Polls the socket for batched write readiness and transmits up to `packets.len()` datagrams.
    fn poll_send_many(
        &self,
        cx: &mut Context<'_>,
        packets: &[Datagram<'_>],
    ) -> Poll<io::Result<usize>>;

    /// Receives multiple datagrams using Linux `recvmmsg(2)`.
    ///
    /// Drains available datagrams into `packets` up to `packets.len().min(MAX_BATCH_SIZE)`.
    /// Returns the number of datagrams received.
    fn recv_many(
        &self,
        packets: &mut [InboundDatagram<'_>],
    ) -> impl std::future::Future<Output = io::Result<usize>> + Send;

    /// Attempts to receive multiple datagrams without blocking.
    ///
    /// If no datagrams are available, returns `Err(io::ErrorKind::WouldBlock)`.
    fn try_recv_many(&self, packets: &mut [InboundDatagram<'_>]) -> io::Result<usize>;

    /// Polls the socket for batched read readiness and receives available datagrams.
    fn poll_recv_many(
        &self,
        cx: &mut Context<'_>,
        packets: &mut [InboundDatagram<'_>],
    ) -> Poll<io::Result<usize>>;
}

impl UdpSocketBatchExt for UdpSocket {
    async fn send_many(&self, packets: &[Datagram<'_>]) -> io::Result<usize> {
        if packets.is_empty() {
            return Ok(0);
        }
        let bounded = &packets[..packets.len().min(MAX_BATCH_SIZE)];
        self.async_io(Interest::WRITABLE, || {
            raw_sendmmsg(self.as_raw_fd(), bounded)
        })
        .await
    }

    fn try_send_many(&self, packets: &[Datagram<'_>]) -> io::Result<usize> {
        if packets.is_empty() {
            return Ok(0);
        }
        let bounded = &packets[..packets.len().min(MAX_BATCH_SIZE)];
        self.try_io(Interest::WRITABLE, || {
            raw_sendmmsg(self.as_raw_fd(), bounded)
        })
    }

    fn poll_send_many(
        &self,
        cx: &mut Context<'_>,
        packets: &[Datagram<'_>],
    ) -> Poll<io::Result<usize>> {
        if packets.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let bounded = &packets[..packets.len().min(MAX_BATCH_SIZE)];
        loop {
            ready!(self.poll_send_ready(cx))?;
            match self.try_send_many(bounded) {
                Ok(n) => return Poll::Ready(Ok(n)),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }

    async fn recv_many(&self, packets: &mut [InboundDatagram<'_>]) -> io::Result<usize> {
        if packets.is_empty() {
            return Ok(0);
        }
        let bound = packets.len().min(MAX_BATCH_SIZE);
        let bounded = &mut packets[..bound];
        // Use Interest::READABLE exclusively to adhere to UdpSocket::async_io's documented contract.
        // Linux epoll reports EPOLLHUP and EPOLLERR as readable events.
        self.async_io(Interest::READABLE, || {
            raw_recvmmsg(self.as_raw_fd(), bounded)
        })
        .await
    }

    fn try_recv_many(&self, packets: &mut [InboundDatagram<'_>]) -> io::Result<usize> {
        if packets.is_empty() {
            return Ok(0);
        }
        let bound = packets.len().min(MAX_BATCH_SIZE);
        let bounded = &mut packets[..bound];
        self.try_io(Interest::READABLE, || {
            raw_recvmmsg(self.as_raw_fd(), bounded)
        })
    }

    fn poll_recv_many(
        &self,
        cx: &mut Context<'_>,
        packets: &mut [InboundDatagram<'_>],
    ) -> Poll<io::Result<usize>> {
        if packets.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let bound = packets.len().min(MAX_BATCH_SIZE);
        let bounded = &mut packets[..bound];
        loop {
            ready!(self.poll_recv_ready(cx))?;
            match self.try_recv_many(bounded) {
                Ok(n) => return Poll::Ready(Ok(n)),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }
}

/// Invokes `sendmmsg(2)` on the raw socket descriptor.
fn raw_sendmmsg(fd: std::os::fd::RawFd, packets: &[Datagram<'_>]) -> io::Result<usize> {
    let count = packets.len();
    if count == 0 {
        return Ok(0);
    }

    if count <= STACK_BATCH_SIZE {
        let mut mmsg_hdrs = [MaybeUninit::<libc::mmsghdr>::uninit(); STACK_BATCH_SIZE];
        let mut iovs = [MaybeUninit::<libc::iovec>::uninit(); STACK_BATCH_SIZE];
        let mut addrs = [MaybeUninit::<libc::sockaddr_storage>::uninit(); STACK_BATCH_SIZE];

        setup_and_call_sendmmsg(
            fd,
            packets,
            &mut mmsg_hdrs[..count],
            &mut iovs[..count],
            &mut addrs[..count],
        )
    } else {
        let mut mmsg_hdrs = vec![MaybeUninit::<libc::mmsghdr>::uninit(); count];
        let mut iovs = vec![MaybeUninit::<libc::iovec>::uninit(); count];
        let mut addrs = vec![MaybeUninit::<libc::sockaddr_storage>::uninit(); count];

        setup_and_call_sendmmsg(
            fd,
            packets,
            &mut mmsg_hdrs,
            &mut iovs,
            &mut addrs,
        )
    }
}

fn setup_and_call_sendmmsg(
    fd: std::os::fd::RawFd,
    packets: &[Datagram<'_>],
    mmsg_hdrs: &mut [MaybeUninit<libc::mmsghdr>],
    iovs: &mut [MaybeUninit<libc::iovec>],
    addrs: &mut [MaybeUninit<libc::sockaddr_storage>],
) -> io::Result<usize> {
    let count = packets.len();

    for i in 0..count {
        let iov = iovs[i].as_mut_ptr();
        // SAFETY: `iovec::iov_base` in POSIX is `*mut c_void`, but for `sendmmsg`/`sendmsg`
        // the kernel reads exclusively from the buffer. Casting `*const u8` to `*mut c_void`
        // does not violate Rust's aliasing rules because the underlying data is never mutated.
        // `packets[i].buf` remains valid and pinned throughout the call.
        unsafe {
            (*iov).iov_base = packets[i].buf.as_ptr() as *mut libc::c_void;
            (*iov).iov_len = packets[i].buf.len();
        }

        let hdr = mmsg_hdrs[i].as_mut_ptr();
        // SAFETY: `hdr` points to valid uninitialized memory in `mmsg_hdrs[i]`. We zero all fields
        // using `ptr::write_bytes`, then set `msg_iov`, `msg_iovlen`, and the destination address.
        // `msg_name` points to `addrs[i]` which remains allocated and valid on the current stack frame
        // throughout the syscall.
        unsafe {
            std::ptr::write_bytes(hdr, 0, 1);
            let msg = &mut (*hdr).msg_hdr;
            msg.msg_iov = iov;
            msg.msg_iovlen = 1;

            if let Some(target) = packets[i].destination {
                let addr_ptr = addrs[i].as_mut_ptr();
                // SAFETY: Zero-initialize the sockaddr_storage memory before constructing a
                // mutable reference to it, ensuring the reference never aliases uninitialized memory.
                std::ptr::write_bytes(addr_ptr, 0, 1);
                let addr_len = encode_sockaddr(target, &mut *addr_ptr);
                msg.msg_name = addr_ptr as *mut libc::c_void;
                msg.msg_namelen = addr_len;
            } else {
                msg.msg_name = std::ptr::null_mut();
                msg.msg_namelen = 0;
            }
        }
    }

    loop {
        // SAFETY: `fd` is a valid file descriptor. `mmsg_hdrs` points to an array of `count`
        // initialized `mmsghdr` structs. `count <= MAX_BATCH_SIZE <= UIO_MAXIOV`. Flags is 0
        // because the socket has O_NONBLOCK set.
        let ret = unsafe {
            libc::sendmmsg(
                fd,
                mmsg_hdrs.as_mut_ptr() as *mut libc::mmsghdr,
                count as libc::c_uint,
                0,
            )
        };

        if ret < 0 {
            let err = io::Error::last_os_error();
            // In Linux sendmmsg(2), if interrupted by a signal after sending >= 1 datagram,
            // the kernel returns the number of datagrams sent. It returns -1 with EINTR only if
            // interrupted before any message was transmitted. Retrying from index 0 is safe.
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }

        return Ok(ret as usize);
    }
}

/// Invokes `recvmmsg(2)` on the raw socket descriptor.
fn raw_recvmmsg(
    fd: std::os::fd::RawFd,
    packets: &mut [InboundDatagram<'_>],
) -> io::Result<usize> {
    let count = packets.len();
    if count == 0 {
        return Ok(0);
    }

    if count <= STACK_BATCH_SIZE {
        let mut mmsg_hdrs = [MaybeUninit::<libc::mmsghdr>::uninit(); STACK_BATCH_SIZE];
        let mut iovs = [MaybeUninit::<libc::iovec>::uninit(); STACK_BATCH_SIZE];
        let mut addrs = [MaybeUninit::<libc::sockaddr_storage>::uninit(); STACK_BATCH_SIZE];

        setup_and_call_recvmmsg(
            fd,
            packets,
            &mut mmsg_hdrs[..count],
            &mut iovs[..count],
            &mut addrs[..count],
        )
    } else {
        let mut mmsg_hdrs = vec![MaybeUninit::<libc::mmsghdr>::uninit(); count];
        let mut iovs = vec![MaybeUninit::<libc::iovec>::uninit(); count];
        let mut addrs = vec![MaybeUninit::<libc::sockaddr_storage>::uninit(); count];

        setup_and_call_recvmmsg(
            fd,
            packets,
            &mut mmsg_hdrs,
            &mut iovs,
            &mut addrs,
        )
    }
}

fn setup_and_call_recvmmsg(
    fd: std::os::fd::RawFd,
    packets: &mut [InboundDatagram<'_>],
    mmsg_hdrs: &mut [MaybeUninit<libc::mmsghdr>],
    iovs: &mut [MaybeUninit<libc::iovec>],
    addrs: &mut [MaybeUninit<libc::sockaddr_storage>],
) -> io::Result<usize> {
    let count = packets.len();

    for i in 0..count {
        packets[i].len = 0;
        packets[i].source = None;
        packets[i].truncated = false;

        let iov = iovs[i].as_mut_ptr();
        // SAFETY: `packets[i].buf` is a valid mutable slice whose lifetime encloses this call.
        // The pointer and length given to `iovec` are valid for the duration of `recvmmsg`.
        unsafe {
            (*iov).iov_base = packets[i].buf.as_mut_ptr() as *mut libc::c_void;
            (*iov).iov_len = packets[i].buf.len();
        }

        let addr_ptr = addrs[i].as_mut_ptr();
        // SAFETY: `addr_ptr` points to `addrs[i]`. We zero out the storage buffer before passing it
        // to recvmmsg to ensure clean family and padding initialization.
        unsafe {
            std::ptr::write_bytes(addr_ptr, 0, 1);
        }

        let hdr = mmsg_hdrs[i].as_mut_ptr();
        // SAFETY: Zero-initialize the `mmsghdr` structure and link its `msg_iov` and `msg_name`
        // pointers to the local stack/heap buffers that live throughout this function.
        unsafe {
            std::ptr::write_bytes(hdr, 0, 1);
            let msg = &mut (*hdr).msg_hdr;
            msg.msg_iov = iov;
            msg.msg_iovlen = 1;
            msg.msg_name = addr_ptr as *mut libc::c_void;
            msg.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        }
    }

    loop {
        // MSG_DONTWAIT: return immediately if no data is currently in the socket buffer.
        // MSG_TRUNC: return the real datagram length in `msg_len` even when truncated.
        let flags = libc::MSG_DONTWAIT | libc::MSG_TRUNC;
        // SAFETY: `fd` is a valid nonblocking socket. `mmsg_hdrs` points to `count` initialized
        // `mmsghdr` entries. `timeout` is null, which under nonblocking socket mode drains available
        // packets up to `count` and returns EAGAIN if empty.
        let ret = unsafe {
            libc::recvmmsg(
                fd,
                mmsg_hdrs.as_mut_ptr() as *mut libc::mmsghdr,
                count as libc::c_uint,
                flags,
                std::ptr::null_mut(),
            )
        };

        if ret < 0 {
            let err = io::Error::last_os_error();
            // In Linux recvmmsg(2), if interrupted after reading >= 1 message, it returns the
            // count received. It returns -1 with EINTR only if interrupted before any message was read.
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }

        let received = ret as usize;
        for i in 0..received {
            // SAFETY: `ret` messages were successfully written by the kernel into `mmsg_hdrs[0..received]`.
            let hdr = unsafe { &*mmsg_hdrs[i].as_ptr() };
            let raw_len = hdr.msg_len as usize;
            let buf_cap = packets[i].buf.len();

            // Clamp `len` strictly to `buf_cap` to guarantee `payload()` slice indexing never panics.
            packets[i].len = raw_len.min(buf_cap);
            packets[i].truncated = raw_len > buf_cap || (hdr.msg_hdr.msg_flags & libc::MSG_TRUNC != 0);

            // SAFETY: `addrs[i]` was populated by the kernel with the sender address, and `msg_namelen`
            // reports the written sockaddr length.
            let addr_storage = unsafe { &*addrs[i].as_ptr() };
            packets[i].source = decode_sockaddr(addr_storage, hdr.msg_hdr.msg_namelen);
        }

        return Ok(received);
    }
}

/// Encodes a `SocketAddr` into a raw `sockaddr_storage` structure.
pub fn encode_sockaddr(addr: SocketAddr, storage: &mut libc::sockaddr_storage) -> libc::socklen_t {
    match addr {
        SocketAddr::V4(v4) => {
            let sin = storage as *mut _ as *mut libc::sockaddr_in;
            // SAFETY: `storage` is properly aligned for `sockaddr_storage` (8-byte alignment),
            // which exceeds `sockaddr_in` (4-byte alignment). We zero the entire struct, ensuring
            // `sin_zero` padding is clean.
            unsafe {
                std::ptr::write_bytes(sin, 0, 1);
                (*sin).sin_family = libc::AF_INET as libc::sa_family_t;
                (*sin).sin_port = v4.port().to_be();
                (*sin).sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
            }
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
        }
        SocketAddr::V6(v6) => {
            let sin6 = storage as *mut _ as *mut libc::sockaddr_in6;
            // SAFETY: `storage` is properly aligned for `sockaddr_storage` (8-byte alignment),
            // which exceeds `sockaddr_in6` (4-byte alignment). We zero the entire struct.
            // Port is converted to network byte order (`to_be()`). Flowinfo and scope_id are passed
            // in standard layout matching the Rust standard library representation.
            unsafe {
                std::ptr::write_bytes(sin6, 0, 1);
                (*sin6).sin6_family = libc::AF_INET6 as libc::sa_family_t;
                (*sin6).sin6_port = v6.port().to_be();
                (*sin6).sin6_addr.s6_addr = v6.ip().octets();
                (*sin6).sin6_flowinfo = v6.flowinfo();
                (*sin6).sin6_scope_id = v6.scope_id();
            }
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
        }
    }
}

/// Decodes a `sockaddr_storage` structure into a `SocketAddr`.
pub fn decode_sockaddr(storage: &libc::sockaddr_storage, len: libc::socklen_t) -> Option<SocketAddr> {
    if len < std::mem::size_of::<libc::sa_family_t>() as libc::socklen_t {
        return None;
    }

    match storage.ss_family as libc::c_int {
        libc::AF_INET => {
            if len < std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t {
                return None;
            }
            // SAFETY: Validated that `len >= sizeof(sockaddr_in)` and family is `AF_INET`.
            let sin = unsafe { &*(storage as *const _ as *const libc::sockaddr_in) };
            let ip = Ipv4Addr::from(sin.sin_addr.s_addr.to_ne_bytes());
            let port = u16::from_be(sin.sin_port);
            Some(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        libc::AF_INET6 => {
            if len < std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t {
                return None;
            }
            // SAFETY: Validated that `len >= sizeof(sockaddr_in6)` and family is `AF_INET6`.
            let sin6 = unsafe { &*(storage as *const _ as *const libc::sockaddr_in6) };
            let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            let port = u16::from_be(sin6.sin6_port);
            Some(SocketAddr::V6(SocketAddrV6::new(
                ip,
                port,
                sin6.sin6_flowinfo,
                sin6.sin6_scope_id,
            )))
        }
        _ => None,
    }
}
