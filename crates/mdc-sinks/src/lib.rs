//! Market data sinks, reference order books, and quote projectors.

#![forbid(unsafe_code)]

pub mod book;
pub mod loss_intolerant;
pub mod top_of_book;

pub use book::{BookError, L2Book};
pub use loss_intolerant::{DeliveryFault, DeliveryFaultReason, TryRecvError};
#[cfg(feature = "tokio")]
pub use loss_intolerant::{LossIntolerantSender, LossIntolerantStream, loss_intolerant_channel};
pub use top_of_book::{QuoteProjection, TopOfBookProjector, UnavailableReason};
