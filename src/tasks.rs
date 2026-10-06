use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use tokio_util::{sync::CancellationToken, task::TaskTracker};

/// Admission and shutdown share a gate so no new task can escape a completed wait.
#[derive(Default)]
pub(crate) struct Tasks {
    closed: Mutex<bool>,
    tracker: TaskTracker,
    pub cancel: CancellationToken,
}

#[derive(Clone)]
pub(crate) struct Executor(pub Arc<Tasks>);

impl<F: Future + Send + 'static> hyper::rt::Executor<F> for Executor {
    fn execute(&self, future: F) {
        self.0.spawn(async move {
            let _ = future.await;
        });
    }
}

impl Tasks {
    pub fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) -> bool {
        let closed = self.closed.lock().unwrap();
        if *closed {
            return false;
        }
        let cancel = self.cancel.clone();
        self.tracker.spawn(async move {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {},
                _ = future => {},
            }
        });
        true
    }

    pub fn cancel(&self) {
        *self.closed.lock().unwrap() = true;
        self.cancel.cancel();
        self.tracker.close();
    }

    pub async fn wait(&self) {
        self.tracker.wait().await;
    }
}
