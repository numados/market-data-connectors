//! Bounded loss-intolerant delivery channel with reserved terminal fault slot.
//!
//! Provides an in-memory bounded FIFO queue for transferring [`MarketEvent`] streams
//! from synchronous MDC producer callbacks to downstream asynchronous consumers (such as MRS).
//!
//! In contrast to lossy ring buffers, on queue capacity exhaustion, closed receiver, or
//! connection failure, the channel stops enqueueing further events, places a single terminal
//! [`DeliveryFault`] in a reserved slot behind the previously queued prefix, and signals
//! cancellation to the producer.

use mdc_core::DeliveryStamp;
use serde::{Deserialize, Serialize};

#[cfg(feature = "tokio")]
use futures_util::Stream;
#[cfg(feature = "tokio")]
use futures_util::task::AtomicWaker;
#[cfg(feature = "tokio")]
use mdc_core::{MarketDataKind, MarketEvent, sink::MarketDataSink};
#[cfg(feature = "tokio")]
use std::collections::VecDeque;
#[cfg(feature = "tokio")]
use std::pin::Pin;
#[cfg(feature = "tokio")]
use std::sync::{Arc, Mutex, MutexGuard};
#[cfg(feature = "tokio")]
use std::task::{Context, Poll};
#[cfg(feature = "tokio")]
use tokio_util::sync::CancellationToken;

/// Cause of a delivery failure across the loss-intolerant channel boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryFaultReason {
    /// Bounded capacity exceeded (channel full).
    #[error("loss-intolerant channel capacity exceeded (max {capacity} events)")]
    CapacityExceeded {
        /// Configured event capacity.
        capacity: usize,
    },
    /// Consumer dropped or closed the receiver.
    #[error("consumer receiver closed")]
    ReceiverClosed,
    /// Explicit producer termination due to connection error.
    #[error("connection explicitly terminated: {0}")]
    ConnectionTerminated(String),
}

/// In-band terminal delivery fault indicating a loss-intolerant channel delivery failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("delivery fault on connection {affected_connection}: {reason}")]
pub struct DeliveryFault {
    /// Delivery stamp at the time the fault occurred.
    pub stamp: DeliveryStamp,
    /// The root cause of the delivery fault.
    pub reason: DeliveryFaultReason,
    /// Connection identifier affected by this fault.
    pub affected_connection: u32,
}

/// Error returned by [`LossIntolerantStream::try_recv`] when no item is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TryRecvError {
    /// No events currently available in the queue and channel is not closed.
    #[error("channel is currently empty")]
    Empty,
}

#[cfg(feature = "tokio")]
#[derive(Debug)]
struct ChannelState {
    queue: VecDeque<MarketEvent<MarketDataKind>>,
    terminal_fault: Option<DeliveryFault>,
    fault_delivered: bool,
    sender_count: usize,
    receiver_closed: bool,
}

#[cfg(feature = "tokio")]
impl ChannelState {
    fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(capacity),
            terminal_fault: None,
            fault_delivered: false,
            sender_count: 1,
            receiver_closed: false,
        }
    }
}

/// Producer half of a bounded loss-intolerant channel.
#[cfg(feature = "tokio")]
#[derive(Debug)]
pub struct LossIntolerantSender {
    capacity: usize,
    affected_connection: u32,
    cancellation_token: CancellationToken,
    state: Arc<Mutex<ChannelState>>,
    waker: Arc<AtomicWaker>,
}

#[cfg(feature = "tokio")]
impl LossIntolerantSender {
    fn lock_state(&self) -> MutexGuard<'_, ChannelState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Returns the configured event capacity of this channel.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the connection identifier associated with this channel.
    pub fn affected_connection(&self) -> u32 {
        self.affected_connection
    }

    /// Returns the current number of queued ordinary events.
    pub fn len(&self) -> usize {
        self.lock_state().queue.len()
    }

    /// Returns `true` if the queue is currently empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Attempts to enqueue a market event synchronously and non-blockingly.
    ///
    /// If the queue is at capacity or the receiver has closed:
    /// 1. A terminal [`DeliveryFault`] is recorded in the reserved slot.
    /// 2. The producer [`CancellationToken`] is cancelled.
    /// 3. Returns `Err(DeliveryFault)`.
    pub fn try_send(&self, event: MarketEvent<MarketDataKind>) -> Result<(), DeliveryFault> {
        let mut state = self.lock_state();

        if state.receiver_closed {
            let fault = state.terminal_fault.clone().unwrap_or(DeliveryFault {
                stamp: event.delivery,
                reason: DeliveryFaultReason::ReceiverClosed,
                affected_connection: self.affected_connection,
            });
            if state.terminal_fault.is_none() {
                state.terminal_fault = Some(fault.clone());
            }
            self.cancellation_token.cancel();
            self.waker.wake();
            return Err(fault);
        }

        if let Some(fault) = &state.terminal_fault {
            return Err(fault.clone());
        }

        if state.queue.len() >= self.capacity {
            let fault = DeliveryFault {
                stamp: event.delivery,
                reason: DeliveryFaultReason::CapacityExceeded {
                    capacity: self.capacity,
                },
                affected_connection: self.affected_connection,
            };
            state.terminal_fault = Some(fault.clone());
            self.cancellation_token.cancel();
            self.waker.wake();
            return Err(fault);
        }

        state.queue.push_back(event);
        self.waker.wake();
        Ok(())
    }

    /// Explicitly terminates the channel with a terminal fault.
    pub fn fail(&self, stamp: DeliveryStamp, reason: DeliveryFaultReason) -> DeliveryFault {
        let mut state = self.lock_state();
        if let Some(existing) = &state.terminal_fault {
            existing.clone()
        } else {
            let f = DeliveryFault {
                stamp,
                reason,
                affected_connection: self.affected_connection,
            };
            state.terminal_fault = Some(f.clone());
            self.cancellation_token.cancel();
            self.waker.wake();
            f
        }
    }
}

#[cfg(feature = "tokio")]
impl Clone for LossIntolerantSender {
    fn clone(&self) -> Self {
        self.lock_state().sender_count += 1;
        Self {
            capacity: self.capacity,
            affected_connection: self.affected_connection,
            cancellation_token: self.cancellation_token.clone(),
            state: Arc::clone(&self.state),
            waker: Arc::clone(&self.waker),
        }
    }
}

#[cfg(feature = "tokio")]
impl Drop for LossIntolerantSender {
    fn drop(&mut self) {
        let mut state = self.lock_state();
        if state.sender_count > 0 {
            state.sender_count -= 1;
        }
        self.waker.wake();
    }
}

#[cfg(feature = "tokio")]
impl MarketDataSink for LossIntolerantSender {
    type Error = DeliveryFault;

    fn on_event(&self, event: MarketEvent<MarketDataKind>) -> Result<(), Self::Error> {
        self.try_send(event)
    }
}

/// Consumer stream half of a bounded loss-intolerant channel.
///
/// Yields `Ok(event)` for each queued event in FIFO order. If a terminal fault occurs,
/// yields `Err(fault)` exactly once after the queued prefix is drained, followed by EOF (`None`).
#[cfg(feature = "tokio")]
#[derive(Debug)]
pub struct LossIntolerantStream {
    cancellation_token: CancellationToken,
    affected_connection: u32,
    state: Arc<Mutex<ChannelState>>,
    waker: Arc<AtomicWaker>,
}

#[cfg(feature = "tokio")]
impl LossIntolerantStream {
    fn lock_state(&self) -> MutexGuard<'_, ChannelState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Connection identifier associated with this channel.
    pub fn affected_connection(&self) -> u32 {
        self.affected_connection
    }

    /// Synchronously attempts to receive the next item without blocking.
    pub fn try_recv(
        &mut self,
    ) -> Result<Option<Result<MarketEvent<MarketDataKind>, DeliveryFault>>, TryRecvError> {
        let mut state = self.lock_state();
        if let Some(event) = state.queue.pop_front() {
            return Ok(Some(Ok(event)));
        }

        if let Some(fault) = state.terminal_fault.clone() {
            if !state.fault_delivered {
                state.fault_delivered = true;
                return Ok(Some(Err(fault)));
            }
            return Ok(None);
        }

        if state.sender_count == 0 {
            return Ok(None);
        }

        Err(TryRecvError::Empty)
    }

    /// Asynchronously receives the next item.
    pub async fn recv(&mut self) -> Option<Result<MarketEvent<MarketDataKind>, DeliveryFault>> {
        use futures_util::StreamExt;
        self.next().await
    }
}

#[cfg(feature = "tokio")]
impl Stream for LossIntolerantStream {
    type Item = Result<MarketEvent<MarketDataKind>, DeliveryFault>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state = self.lock_state();
        if let Some(event) = state.queue.pop_front() {
            return Poll::Ready(Some(Ok(event)));
        }

        if let Some(fault) = state.terminal_fault.clone() {
            if !state.fault_delivered {
                state.fault_delivered = true;
                return Poll::Ready(Some(Err(fault)));
            }
            return Poll::Ready(None);
        }

        if state.sender_count == 0 {
            return Poll::Ready(None);
        }

        self.waker.register(cx.waker());
        Poll::Pending
    }
}

#[cfg(feature = "tokio")]
impl Drop for LossIntolerantStream {
    fn drop(&mut self) {
        let mut state = self.lock_state();
        state.receiver_closed = true;
        self.cancellation_token.cancel();
    }
}

/// Creates a new bounded loss-intolerant channel pair.
///
/// # Arguments
/// * `capacity` - Maximum number of ordinary data events that can be queued.
/// * `affected_connection` - Connection identifier used in terminal [`DeliveryFault`] reporting.
/// * `cancellation_token` - Cancellation token cancelled when the channel overflows or receiver drops.
#[cfg(feature = "tokio")]
pub fn loss_intolerant_channel(
    capacity: usize,
    affected_connection: u32,
    cancellation_token: CancellationToken,
) -> (LossIntolerantSender, LossIntolerantStream) {
    let state = Arc::new(Mutex::new(ChannelState::new(capacity)));
    let waker = Arc::new(AtomicWaker::new());

    let sender = LossIntolerantSender {
        capacity,
        affected_connection,
        cancellation_token: cancellation_token.clone(),
        state: Arc::clone(&state),
        waker: Arc::clone(&waker),
    };

    let stream = LossIntolerantStream {
        cancellation_token,
        affected_connection,
        state,
        waker,
    };

    (sender, stream)
}
