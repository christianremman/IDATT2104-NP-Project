//! Counter CRDTs.
//!
//! - [`GCounter`] : grow-only (increment only, never decrements)
//! - [`PNCounter`] : positive-negative (supports both increment and decrement)
pub mod g_counter;
pub mod pn_counter;

pub use g_counter::GCounter;
pub use pn_counter::PNCounter;
