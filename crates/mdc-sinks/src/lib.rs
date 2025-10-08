//! Market data sinks, reference order books, and quote projectors.

#![forbid(unsafe_code)]

pub mod book;
pub mod top_of_book;

pub use book::{BookError, L2Book};
pub use top_of_book::{QuoteProjection, TopOfBookProjector, UnavailableReason};
