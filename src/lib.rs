// Allow dead code for config fields and infrastructure that will be used later
#![allow(dead_code)]

pub mod config;
pub mod decoder;
pub mod shutdown;
pub mod state;
pub mod storage;

pub mod datadog;
pub mod kafka;
pub mod observability;
pub mod redis;
pub mod sampling;

#[cfg(test)]
mod tests;
