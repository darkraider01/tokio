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

/// Maximum batch size allocated on the stack to prevent heap allocations
/// on the hot path while keeping stack frames bounded.
const STACK_BATCH_SIZE: usize = 64;

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
    /// less than `packets.len()` if the socket buffer becomes full (partial send).
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
    /// Drains available datagrams into `packets` up to `packets.len()`.
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
        self.async_io(Interest::WRITABLE, || {
            raw_sendmmsg(self.as_raw_fd(), packets)
        })
        .await
    }

    fn try_send_many(&self, packets: &[Datagram<'_>]) -> io::Result<usize> {
        if packets.is_empty() {
            return Ok(0);
        }
        self.try_io(Interest::WRITABLE, || {
            raw_sendmmsg(self.as_raw_fd(), packets)
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
        loop {
            ready!(self.poll_send_ready(cx))?;
            match self.try_send_many(packets) {
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
        self.async_io(Interest::READABLE | Interest::ERROR, || {
            raw_recvmmsg(self.as_raw_fd(), packets)
        })
        .await
    }

    fn try_recv_many(&self, packets: &mut [InboundDatagram<'_>]) -> io::Result<usize> {
        if packets.is_empty() {
            return Ok(0);
        }
        self.try_io(Interest::READABLE, || {
            raw_recvmmsg(self.as_raw_fd(), packets)
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
        loop {
            ready!(self.poll_recv_ready(cx))?;
            match self.try_recv_many(packets) {
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
        let mut mmsg_hdrs = [const { MaybeUninit::<libc::mmsghdr>::zeroed() }; STACK_BATCH_SIZE];
        let mut iovs = [const { MaybeUninit::<libc::iovec>::zeroed() }; STACK_BATCH_SIZE];
        let mut addrs = [const { MaybeUninit::<libc::sockaddr_storage>::zeroed() }; STACK_BATCH_SIZE];

        setup_and_call_sendmmsg(
            fd,
            packets,
            &mut mmsg_hdrs[..count],
            &mut iovs[..count],
            &mut addrs[..count],
        )
    } else {
        let mut mmsg_hdrs = vec![const { MaybeUninit::<libc::mmsghdr>::zeroed() }; count];
        let mut iovs = vec![const { MaybeUninit::<libc::iovec>::zeroed() }; count];
        let mut addrs = vec![const { MaybeUninit::<libc::sockaddr_storage>::zeroed() }; count];

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
        unsafe {
            (*iov).iov_base = packets[i].buf.as_ptr() as *mut libc::c_void;
            (*iov).iov_len = packets[i].buf.len();
        }

        let hdr = mmsg_hdrs[i].as_mut_ptr();
        unsafe {
            // zero out the entire header struct
            std::ptr::write_bytes(hdr, 0, 1);
            let msg = &mut (*hdr).msg_hdr;
            msg.msg_iov = iov;
            msg.msg_iovlen = 1;

            if let Some(target) = packets[i].destination {
                let addr_ptr = addrs[i].as_mut_ptr();
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
        let mut mmsg_hdrs = [const { MaybeUninit::<libc::mmsghdr>::zeroed() }; STACK_BATCH_SIZE];
        let mut iovs = [const { MaybeUninit::<libc::iovec>::zeroed() }; STACK_BATCH_SIZE];
        let mut addrs = [const { MaybeUninit::<libc::sockaddr_storage>::zeroed() }; STACK_BATCH_SIZE];

        setup_and_call_recvmmsg(
            fd,
            packets,
            &mut mmsg_hdrs[..count],
            &mut iovs[..count],
            &mut addrs[..count],
        )
    } else {
        let mut mmsg_hdrs = vec![const { MaybeUninit::<libc::mmsghdr>::zeroed() }; count];
        let mut iovs = vec![const { MaybeUninit::<libc::iovec>::zeroed() }; count];
        let mut addrs = vec![const { MaybeUninit::<libc::sockaddr_storage>::zeroed() }; count];

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
        unsafe {
            (*iov).iov_base = packets[i].buf.as_mut_ptr() as *mut libc::c_void;
            (*iov).iov_len = packets[i].buf.len();
        }

        let addr_ptr = addrs[i].as_mut_ptr();
        unsafe {
            std::ptr::write_bytes(addr_ptr, 0, 1);
        }

        let hdr = mmsg_hdrs[i].as_mut_ptr();
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
        // MSG_DONTWAIT: return immediately if no data is available.
        // MSG_TRUNC: return true datagram size even if truncated.
        let flags = libc::MSG_DONTWAIT | libc::MSG_TRUNC;
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
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }

        let received = ret as usize;
        for i in 0..received {
            let hdr = unsafe { &*mmsg_hdrs[i].as_ptr() };
            let raw_len = hdr.msg_len as usize;
            let buf_cap = packets[i].buf.len();

            packets[i].len = raw_len.min(buf_cap);
            packets[i].truncated = raw_len > buf_cap || (hdr.msg_hdr.msg_flags & libc::MSG_TRUNC != 0);

            let addr_storage = unsafe { &*addrs[i].as_ptr() };
            packets[i].source = decode_sockaddr(addr_storage, hdr.msg_hdr.msg_namelen);
        }

        return Ok(received);
    }
}

/// Encodes a `SocketAddr` into a raw `sockaddr_storage` structure.
fn encode_sockaddr(addr: SocketAddr, storage: &mut libc::sockaddr_storage) -> libc::socklen_t {
    match addr {
        SocketAddr::V4(v4) => {
            let sin = storage as *mut _ as *mut libc::sockaddr_in;
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
fn decode_sockaddr(storage: &libc::sockaddr_storage, len: libc::socklen_t) -> Option<SocketAddr> {
    if len < std::mem::size_of::<libc::sa_family_t>() as libc::socklen_t {
        return None;
    }

    match storage.ss_family as libc::c_int {
        libc::AF_INET => {
            if len < std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t {
                return None;
            }
            let sin = unsafe { &*(storage as *const _ as *const libc::sockaddr_in) };
            let ip = Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
            let port = u16::from_be(sin.sin_port);
            Some(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        libc::AF_INET6 => {
            if len < std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t {
                return None;
            }
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

