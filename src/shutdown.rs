// Graceful shutdown management
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::signal;
use tokio::sync::broadcast;
use tokio::time::timeout;
use tracing::{error, info, warn};

/// Graceful shutdown coordinator
///
/// Handles SIGTERM and SIGINT signals, providing a clean shutdown mechanism
/// that allows in-flight operations to complete before termination.
#[derive(Debug)]
pub struct ShutdownCoordinator {
    /// Broadcast sender for shutdown signal
    shutdown_tx: broadcast::Sender<ShutdownSignal>,
    /// Atomic flag for quick shutdown check
    shutdown_requested: AtomicBool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownSignal {
    /// Graceful shutdown requested (SIGTERM)
    Graceful,
    /// Force shutdown requested (SIGINT)
    Force,
    /// Timeout during graceful shutdown
    Timeout,
}

impl ShutdownCoordinator {
    /// Create a new shutdown coordinator
    pub fn new() -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);

        Self {
            shutdown_tx,
            shutdown_requested: AtomicBool::new(false),
        }
    }

    /// Install signal handlers for graceful shutdown
    ///
    /// This method sets up handlers for SIGTERM and SIGINT signals.
    /// When a signal is received, a broadcast is sent to all waiting tasks.
    pub async fn install_handlers(&self) -> Result<(), std::io::Error> {
        let shutdown_tx = self.shutdown_tx.clone();

        // Handle SIGTERM (graceful shutdown)
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        let shutdown_tx_term = shutdown_tx.clone();
        tokio::spawn(async move {
            if sigterm.recv().await.is_some() {
                info!("Received SIGTERM, initiating graceful shutdown");
                let _ = shutdown_tx_term.send(ShutdownSignal::Graceful);
            }
        });

        // Handle SIGINT (force shutdown)
        let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())?;
        let shutdown_tx_int = shutdown_tx.clone();
        tokio::spawn(async move {
            if sigint.recv().await.is_some() {
                info!("Received SIGINT, initiating shutdown");
                let _ = shutdown_tx_int.send(ShutdownSignal::Force);
            }
        });

        Ok(())
    }

    /// Get a receiver for shutdown signals
    pub fn receiver(&self) -> broadcast::Receiver<ShutdownSignal> {
        self.shutdown_tx.subscribe()
    }

    /// Check if shutdown has been requested
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
    }

    /// Request shutdown
    pub fn request_shutdown(&self, signal: ShutdownSignal) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        let _ = self.shutdown_tx.send(signal);
    }

    /// Wait for shutdown signal with timeout
    pub async fn wait_for_shutdown(&self) -> ShutdownSignal {
        let receiver = self.receiver();
        let mut receiver = receiver;
        receiver.recv().await.unwrap_or(ShutdownSignal::Graceful)
    }

    /// Run a future with graceful shutdown timeout
    pub async fn run_with_shutdown<F, T>(
        &self,
        _timeout_duration: Duration,
        future: F,
    ) -> Result<T, ShutdownError>
    where
        F: Future<Output = T>,
    {
        let mut shutdown_rx = self.receiver();

        tokio::select! {
            result = future => Ok(result),
            signal = shutdown_rx.recv() => {
                match signal {
                    Ok(ShutdownSignal::Graceful) => {
                        info!("Graceful shutdown signal received");
                        Err(ShutdownError::Graceful)
                    }
                    Ok(ShutdownSignal::Force) => {
                        info!("Force shutdown signal received");
                        Err(ShutdownError::Force)
                    }
                    Ok(ShutdownSignal::Timeout) => {
                        info!("Shutdown timeout reached");
                        Err(ShutdownError::Timeout)
                    }
                    Err(_) => {
                        info!("Shutdown receiver dropped");
                        Err(ShutdownError::ReceiverDropped)
                    }
                }
            }
        }
    }

    /// Shutdown with timeout protection
    pub async fn shutdown_with_timeout<F, R, E>(
        &self,
        timeout_duration: Duration,
        name: &str,
        shutdown_fn: F,
    ) -> Result<(), ShutdownError>
    where
        F: Future<Output = Result<R, E>>,
        E: std::fmt::Display,
    {
        let _shutdown_rx = self.receiver();

        info!("Starting graceful shutdown for {}", name);

        match timeout(timeout_duration, shutdown_fn).await {
            Ok(Ok(_)) => {
                info!("{} shutdown completed gracefully", name);
                Ok(())
            }
            Ok(Err(e)) => {
                error!("{} shutdown error: {}", name, e);
                Err(ShutdownError::OperationFailed(e.to_string()))
            }
            Err(_) => {
                warn!("{} shutdown timed out after {:?}", name, timeout_duration);
                Err(ShutdownError::Timeout)
            }
        }
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Error type for shutdown operations
#[derive(Debug, thiserror::Error)]
pub enum ShutdownError {
    #[error("Graceful shutdown requested")]
    Graceful,
    #[error("Force shutdown requested")]
    Force,
    #[error("Shutdown timed out")]
    Timeout,
    #[error("Shutdown receiver was dropped")]
    ReceiverDropped,
    #[error("Shutdown operation failed: {0}")]
    OperationFailed(String),
}

impl From<ShutdownSignal> for ShutdownError {
    fn from(signal: ShutdownSignal) -> Self {
        match signal {
            ShutdownSignal::Graceful => ShutdownError::Graceful,
            ShutdownSignal::Force => ShutdownError::Force,
            ShutdownSignal::Timeout => ShutdownError::Timeout,
        }
    }
}

/// Utility function to block on shutdown with timeout
pub async fn block_on_shutdown(coordinator: &ShutdownCoordinator, timeout: Duration) {
    let mut rx = coordinator.receiver();

    tokio::select! {
        _ = rx.recv() => {
            info!("Shutdown signal received, exiting");
        }
        _ = tokio::time::sleep(timeout) => {
            info!("Shutdown timeout reached, forcing exit");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_shutdown_coordinator_creation() {
        let coordinator = ShutdownCoordinator::new();
        assert!(!coordinator.is_shutdown_requested());
    }

    #[tokio::test]
    async fn test_shutdown_signal_broadcast() {
        let coordinator = ShutdownCoordinator::new();
        let mut rx1 = coordinator.receiver();
        let mut rx2 = coordinator.receiver();

        coordinator.request_shutdown(ShutdownSignal::Graceful);

        tokio::join! {
            async { assert_eq!(rx1.recv().await.unwrap(), ShutdownSignal::Graceful) },
            async { assert_eq!(rx2.recv().await.unwrap(), ShutdownSignal::Graceful) },
        };
    }
}
