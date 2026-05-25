//! Map CRDTs.
//!
//! - [`LWWMap`]: per-key last-writer-wins map
pub mod lww_map;

pub use lww_map::LWWMap;
