//! Set CRDTs with varying removal semantics.
//!
//! - [`GSet`] : grow-only (elements can never be removed)
//! - [`TwoPSet`] : two-phase (removed elements can never be re-added)
//! - [`ORSet`] : observed-remove (add-wins on concurrent add/remove)
pub mod gset;
pub mod orset;
pub mod two_pset;

pub use gset::GSet;
pub use orset::{ORSet, ORSetDelta};
pub use two_pset::TwoPSet;
