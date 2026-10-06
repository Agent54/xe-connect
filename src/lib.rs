//! Embeddable core for the standalone xe-connect HTTP bridge.

pub mod agent;
pub mod config;
mod http;
mod protocol;
mod runtime;
pub mod state;
mod tasks;

pub use agent::Agent;
pub use config::{Config, Peer};
pub use state::{Profile, StateStore};
