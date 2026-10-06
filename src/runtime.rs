use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Result, ensure};
use tokio::sync::Mutex;

use crate::{Config, StateStore};

/// Runtime reads are memory-only. An owned gate keeps writes serialized even if
/// their requesting future is cancelled after spawn_blocking has started.
pub(crate) struct RuntimeConfig {
    store: StateStore,
    snapshot: Arc<RwLock<Arc<Config>>>,
    writes: Arc<Mutex<()>>,
    closed: AtomicBool,
}

impl RuntimeConfig {
    pub async fn open(store: StateStore) -> Result<Self> {
        let reader = store.clone();
        let config = tokio::task::spawn_blocking(move || {
            let config = reader.read::<Config>()?;
            config.validate()?;
            Ok::<_, anyhow::Error>(config)
        })
        .await??;
        Ok(Self {
            store,
            snapshot: Arc::new(RwLock::new(Arc::new(config))),
            writes: Arc::new(Mutex::new(())),
            closed: AtomicBool::new(false),
        })
    }

    pub fn get(&self) -> Arc<Config> {
        self.snapshot.read().unwrap().clone()
    }

    pub async fn update<R: Send + 'static>(
        &self,
        update: impl FnOnce(&mut Config) -> Result<R> + Send + 'static,
    ) -> Result<R> {
        ensure!(!self.closed.load(Ordering::Acquire), "agent is closing");
        let guard = self.writes.clone().lock_owned().await;
        ensure!(!self.closed.load(Ordering::Acquire), "agent is closing");
        let store = self.store.clone();
        let snapshot = self.snapshot.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let (result, config) = store.update::<Config, _>(move |config| {
                let result = update(config)?;
                config.validate()?;
                Ok((result, config.clone()))
            })?;
            *snapshot.write().unwrap() = Arc::new(config);
            Ok(result)
        })
        .await?
    }

    pub async fn close(&self) {
        self.closed.store(true, Ordering::Release);
        // A cancelled update may still be committing on a blocking worker.
        let _drained = self.writes.lock().await;
    }
}
