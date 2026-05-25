//! Register CRDTs for single-value storage.
//!
//! - [`LWWRegister`] : last-writer-wins (highest timestamp wins)
//! - [`MVRegister`] : multi-value (concurrent writes produce multiple values)
pub mod lww_register;
pub mod mv_register;

pub use lww_register::LWWRegister;
pub use mv_register::MVRegister;
