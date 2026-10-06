use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    fs::File,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use futures_util::{
    FutureExt, StreamExt,
    future::{BoxFuture, Shared},
    stream::FuturesUnordered,
};
use http_body_util::{BodyExt, Limited};
use hyper::{
    Method, Request, Response, StatusCode, Uri,
    body::Incoming,
    header::{AUTHORIZATION, HOST, UPGRADE},
    service::service_fn,
};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::{TokioIo, TokioTimer},
};
use iroh::{
    Endpoint, EndpointAddr, PublicKey, Watcher,
    endpoint::{Connection, RecvStream, SendStream, presets},
};
use tokio::{
    net::TcpListener,
    sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore, oneshot},
    time::{Instant as Deadline, timeout, timeout_at},
};

use crate::{
    config::{Config, Invitation, Peer, Settings, Ticket, new_token, now, validate_profile},
    http::{self, Body, ERROR_HEADER, GATEWAY_HEADER, SOURCE_HEADER},
    protocol::{self, Operation, Reply, Scheme},
    runtime::RuntimeConfig,
    state::StateStore,
    tasks::{Executor, Tasks},
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HEADER_TIMEOUT: Duration = Duration::from_secs(30);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(10);
const HINT_TIMEOUT: Duration = Duration::from_secs(3);
const CACHED_TIMEOUT: Duration = Duration::from_secs(4);
const AUTHENTICATION_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_PEER_CONNECTIONS: usize = 32;
const MAX_INCOMING_HANDSHAKES: usize = 8;
const MAX_PENDING_PAIRINGS: usize = 4;
const MAX_HTTP_CONNECTIONS: usize = 64;
const MAX_REQUESTS: usize = 64;
const PAIRING_WINDOW: Duration = Duration::from_secs(60);
type Stream = (SendStream, RecvStream);
type Dial = Shared<BoxFuture<'static, std::result::Result<Connection, Arc<str>>>>;

struct PeerConnection {
    attempt: Dial,
    started: Instant,
}

enum ConnectionSlot {
    Trusted(OwnedSemaphorePermit),
    Pending(OwnedSemaphorePermit),
}

#[derive(Default)]
struct PairingRate {
    attempts: VecDeque<(PublicKey, Instant)>,
}

impl PairingRate {
    fn allow(&mut self, caller: PublicKey) -> bool {
        self.attempts
            .retain(|(_, time)| time.elapsed() < PAIRING_WINDOW);
        if self.attempts.len() >= 32
            || self.attempts.iter().filter(|(id, _)| *id == caller).count() >= 8
        {
            return false;
        }
        self.attempts.push_back((caller, Instant::now()));
        true
    }
}

/// The parent owns lifecycle and persistent configuration; all peers are symmetric.
pub struct Agent {
    store: StateStore,
    state: RuntimeConfig,
    endpoint: Endpoint,
    client: Client<HttpsConnector<HttpConnector>, Body>,
    connections: Mutex<HashMap<PublicKey, PeerConnection>>,
    live_connections: Mutex<Vec<Connection>>,
    lookups: Mutex<HashMap<String, Weak<Mutex<()>>>>,
    misses: Mutex<HashMap<String, Instant>>,
    tasks: Arc<Tasks>,
    peer_slots: Arc<Semaphore>,
    handshake_slots: Arc<Semaphore>,
    pending_slots: Arc<Semaphore>,
    peer_changes: Notify,
    http_slots: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    pairing_rate: Mutex<PairingRate>,
    closing: Mutex<()>,
    agent_lock: std::sync::Mutex<Option<File>>,
}

impl Agent {
    pub async fn start(store: StateStore) -> Result<Arc<Self>> {
        let initializer = store.clone();
        let (guard, key) = tokio::task::spawn_blocking(move || {
            let guard = initializer.acquire_agent()?;
            let key = initializer.identity()?;
            let id = key.public().to_string();
            initializer.update::<Config, _>(|config| {
                if config.profile.device_name.is_empty() {
                    config.profile.device_name = format!("computer-{}", &id[..8]);
                }
                config.validate()
            })?;
            Ok::<_, anyhow::Error>((guard, key))
        })
        .await??;
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(key)
            .alpns(vec![protocol::ALPN.to_vec()])
            .bind()
            .await?;
        Self::with_endpoint(store, endpoint, guard).await
    }

    async fn with_endpoint(
        store: StateStore,
        endpoint: Endpoint,
        guard: File,
    ) -> Result<Arc<Self>> {
        let connector = tokio::task::spawn_blocking(|| {
            Ok::<_, anyhow::Error>(
                HttpsConnectorBuilder::new()
                    .with_native_roots()?
                    .https_or_http()
                    .enable_http1()
                    .build(),
            )
        })
        .await??;
        let state = RuntimeConfig::open(store.clone()).await?;
        let tasks = Arc::new(Tasks::default());
        let agent = Arc::new(Self {
            store,
            state,
            endpoint,
            client: Client::builder(Executor(tasks.clone()))
                .pool_timer(TokioTimer::new())
                .pool_idle_timeout(Duration::from_secs(30))
                .pool_max_idle_per_host(4)
                .build(connector),
            connections: Mutex::new(HashMap::new()),
            live_connections: Mutex::new(Vec::new()),
            lookups: Mutex::new(HashMap::new()),
            misses: Mutex::new(HashMap::new()),
            tasks,
            peer_slots: Arc::new(Semaphore::new(MAX_PEER_CONNECTIONS)),
            handshake_slots: Arc::new(Semaphore::new(MAX_INCOMING_HANDSHAKES)),
            pending_slots: Arc::new(Semaphore::new(MAX_PENDING_PAIRINGS)),
            peer_changes: Notify::new(),
            http_slots: Arc::new(Semaphore::new(MAX_HTTP_CONNECTIONS)),
            requests: Arc::new(Semaphore::new(MAX_REQUESTS)),
            pairing_rate: Mutex::new(PairingRate::default()),
            closing: Mutex::new(()),
            agent_lock: std::sync::Mutex::new(Some(guard)),
        });
        let acceptor = agent.clone();
        agent.tasks.spawn(async move {
            while let Some(incoming) = acceptor.endpoint.accept().await {
                let Ok(handshake) = acceptor.handshake_slots.clone().try_acquire_owned() else {
                    incoming.refuse();
                    continue;
                };
                let agent = acceptor.clone();
                let auth_deadline = Deadline::now() + AUTHENTICATION_TIMEOUT;
                acceptor.tasks.spawn(async move {
                    if let Ok(Ok(connection)) = timeout(CONNECT_TIMEOUT, incoming).await {
                        // Only the completed handshake provides a verified device identity.
                        let trusted = agent
                            .state
                            .get()
                            .peers
                            .contains_key(&connection.remote_id().to_string());
                        let slots = if trusted {
                            &agent.peer_slots
                        } else {
                            &agent.pending_slots
                        };
                        let Ok(permit) = slots.clone().try_acquire_owned() else {
                            connection.close(3u8.into(), b"connection limit reached");
                            return;
                        };
                        drop(handshake);
                        let slot = if trusted {
                            ConnectionSlot::Trusted(permit)
                        } else {
                            ConnectionSlot::Pending(permit)
                        };
                        agent.accept_streams(connection, slot, auth_deadline).await;
                    }
                });
            }
        });
        let publisher = agent.clone();
        agent.tasks.spawn(async move {
            let mut addresses = publisher.endpoint.watch_addr().stream();
            loop {
                tokio::select! {
                    _ = publisher.endpoint.closed() => break,
                    address = addresses.next() => {
                        let Some(address) = address else { break };
                        if let Err(error) = publisher.state.update(move |config| {
                            config.last_address = Some(address);
                            Ok(())
                        }).await { eprintln!("persist endpoint address: {error:#}"); }
                    }
                }
            }
        });
        Ok(agent)
    }

    pub fn peer(&self) -> Result<Peer> {
        Ok(Peer {
            address: self.endpoint.addr(),
            profile: self.state.get().profile.clone(),
        })
    }

    pub fn store(&self) -> &StateStore {
        &self.store
    }

    pub async fn close(&self) {
        let _closing = self.closing.lock().await;
        self.tasks.cancel();
        self.endpoint.close().await;
        self.connections.lock().await.clear();
        self.tasks.wait().await;
        self.state.close().await;
        self.agent_lock.lock().unwrap().take();
    }

    pub fn config(&self) -> Arc<Config> {
        self.state.get()
    }

    pub async fn update_config<R: Send + 'static>(
        &self,
        update: impl FnOnce(&mut Config) -> Result<R> + Send + 'static,
    ) -> Result<R> {
        let listen = self.state.get().listen;
        let result = self
            .state
            .update(move |config| {
                let result = update(config)?;
                ensure!(
                    config.listen == listen,
                    "stop the agent before changing its listener address"
                );
                Ok(result)
            })
            .await?;
        self.peer_changes.notify_waiters();
        Ok(result)
    }

    pub async fn remove_peer(&self, id: PublicKey) -> Result<()> {
        self.state
            .update(move |config| {
                config.remove_peer(&id.to_string());
                Ok(())
            })
            .await?;
        for connection in self
            .live_connections
            .lock()
            .await
            .iter()
            .filter(|connection| connection.remote_id() == id)
        {
            connection.close(1u8.into(), b"peer removed");
        }
        self.connections.lock().await.remove(&id);
        self.misses.lock().await.clear();
        Ok(())
    }

    pub async fn invite(&self, lifetime: Duration) -> Result<Ticket> {
        let ticket = new_invitation(self.peer()?, lifetime)?;
        let stored = ticket.clone();
        self.state
            .update(move |config| {
                remember_invitation(config, &stored);
                Ok(())
            })
            .await?;
        Ok(ticket)
    }

    pub async fn pair(self: &Arc<Self>, ticket: Ticket) -> Result<Peer> {
        self.pair_until(ticket, Deadline::now() + OPERATION_TIMEOUT)
            .await
    }

    async fn pair_until(self: &Arc<Self>, ticket: Ticket, deadline: Deadline) -> Result<Peer> {
        ensure!(
            ticket.version == 1 && ticket.expires_at >= now(),
            "invalid or expired invitation"
        );
        ensure!(
            ticket.peer.address.id != self.endpoint.id(),
            "cannot pair a computer with itself"
        );
        let reply: Reply = self
            .operation(deadline, async {
                let connection = self.connection(&ticket.peer.address, deadline).await?;
                let (mut send, mut receive) = connection.open_bi().await?;
                protocol::write(
                    &mut send,
                    &Operation::Pair {
                        token: ticket.token,
                        peer: self.peer()?,
                    },
                )
                .await?;
                send.finish()?;
                protocol::read(&mut receive).await
            })
            .await?;
        ensure!(
            reply.ok,
            "pairing rejected: {}",
            reply.error.as_deref().unwrap_or("unknown error")
        );
        let peer = reply
            .peer
            .context("pairing reply is missing device identity")?;
        ensure!(
            peer.address.id == ticket.peer.address.id,
            "pairing reply identity mismatch"
        );
        validate_profile(&peer.profile)?;
        let stored = peer.clone();
        self.state
            .update(move |config| {
                config.peers.insert(stored.address.id.to_string(), stored);
                Ok(())
            })
            .await?;
        self.peer_changes.notify_waiters();
        self.misses.lock().await.clear();
        Ok(peer)
    }

    async fn operation<T>(
        &self,
        deadline: Deadline,
        future: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        tokio::select! {
            biased;
            _ = self.tasks.cancel.cancelled() => anyhow::bail!("agent is closing"),
            result = timeout_at(deadline, future) => result.context("peer operation timed out")?,
        }
    }

    /// The same request handler can later be attached to a Unix listener.
    pub async fn serve_http(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        ensure!(
            listener.local_addr()?.ip().is_loopback(),
            "listener must be loopback-only"
        );
        // Own the listener task; a parent can stop polling this observer future.
        let (send, receive) = oneshot::channel();
        let agent = self.clone();
        ensure!(
            self.tasks.spawn(async move {
                let _ = send.send(agent.accept_http(listener).await);
            }),
            "agent is closing"
        );
        match receive.await {
            Ok(result) => result,
            Err(_) if self.tasks.cancel.is_cancelled() => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn accept_http(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        loop {
            tokio::select! {
                _ = self.tasks.cancel.cancelled() => return Ok(()),
                incoming = listener.accept() => {
                    let (stream, _) = incoming?;
                    let Ok(permit) = self.http_slots.clone().try_acquire_owned() else { continue };
                    let lease = Arc::new(permit);
                    let agent = self.clone();
                    self.tasks.spawn(async move {
                        let _connection_lease = lease.clone();
                        let service = service_fn(move |request| {
                            let agent = agent.clone();
                            let lease = lease.clone();
                            async move { Ok::<_, Infallible>(agent.handle_local(request, lease).await) }
                        });
                        let mut builder = hyper::server::conn::http1::Builder::new();
                        builder.timer(TokioTimer::new()).header_read_timeout(HEADER_TIMEOUT);
                        let connection = builder
                            .serve_connection(TokioIo::new(stream), service).with_upgrades();
                        let _ = connection.await;
                    });
                }
            }
        }
    }

    async fn handle_local(
        self: Arc<Self>,
        request: Request<Incoming>,
        lease: Arc<OwnedSemaphorePermit>,
    ) -> Response<Body> {
        let Ok(_request) = self.requests.clone().try_acquire_owned() else {
            return http::error(
                StatusCode::SERVICE_UNAVAILABLE,
                "busy",
                "request limit reached",
            );
        };
        let control = request
            .headers()
            .get(HOST)
            .and_then(|v| v.to_str().ok())
            .and_then(|host| host.parse::<hyper::http::uri::Authority>().ok())
            .is_some_and(|host| {
                host.host() == "localhost"
                    || host
                        .host()
                        .trim_matches(['[', ']'])
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback())
            });
        if control && request.uri().path().starts_with("/_xe/") {
            return match self.control(request).await {
                Ok(response) => response,
                Err(error) => http::error(StatusCode::BAD_REQUEST, "control_failed", error),
            };
        }
        let hostname = match http::request_hostname(request.uri(), request.headers()) {
            Ok(hostname) => hostname,
            Err(error) => return http::error(StatusCode::BAD_REQUEST, "invalid_host", error),
        };
        if request.method() == Method::CONNECT {
            return http::error(
                StatusCode::METHOD_NOT_ALLOWED,
                "unsupported_method",
                "CONNECT is not supported",
            );
        }
        let scheme = match self.gateway_scheme(&request) {
            Ok(scheme) => scheme,
            Err(error) => {
                return http::error(StatusCode::BAD_REQUEST, "invalid_gateway_scheme", error);
            }
        };
        if self.probe(&hostname).await {
            return match self
                .forward_local(request, &hostname, None, scheme, lease)
                .await
            {
                Ok(response) => response,
                Err(error) => http::error(StatusCode::BAD_GATEWAY, "upstream_failed", error),
            };
        }
        match self.remote_stream(&hostname, scheme).await {
            Ok(Some((peer_id, stream))) => {
                match self.forward_remote(request, stream, lease).await {
                    Ok(response) => {
                        if response.headers().contains_key(ERROR_HEADER) {
                            let _ = self.invalidate(&hostname, &peer_id).await;
                        }
                        response
                    }
                    Err(error) => {
                        let _ = self.invalidate(&hostname, &peer_id).await;
                        http::error(StatusCode::BAD_GATEWAY, "peer_request_failed", error)
                    }
                }
            }
            Ok(None) => http::error(
                StatusCode::NOT_FOUND,
                "service_not_found",
                "no paired computer offers this hostname",
            ),
            Err(error) => http::error(StatusCode::BAD_GATEWAY, "discovery_failed", error),
        }
    }

    async fn control(self: &Arc<Self>, request: Request<Incoming>) -> Result<Response<Body>> {
        let config = self.state.get();
        let expected = format!("Bearer {}", config.control_token);
        if request
            .headers()
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            != Some(&expected)
        {
            return Ok(http::error(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "local control token required",
            ));
        }
        match (request.method().clone(), request.uri().path()) {
            (Method::GET, "/_xe/self") => Ok(http::json(self.peer()?)),
            (Method::POST, "/_xe/config") => {
                let bytes = Limited::new(request.into_body(), 4096)
                    .collect()
                    .await
                    .map_err(anyhow::Error::from_boxed)?
                    .to_bytes();
                let settings: Settings = serde_json::from_slice(&bytes)?;
                self.update_config(move |config| settings.apply(config))
                    .await?;
                Ok(http::json(serde_json::json!({ "updated": true })))
            }
            (Method::POST, "/_xe/invite") => {
                let bytes = Limited::new(request.into_body(), 64)
                    .collect()
                    .await
                    .map_err(anyhow::Error::from_boxed)?
                    .to_bytes();
                let seconds: u64 = serde_json::from_slice(&bytes)?;
                Ok(http::json(self.invite(Duration::from_secs(seconds)).await?))
            }
            (Method::POST, "/_xe/pair") => {
                let bytes = Limited::new(request.into_body(), 32_768)
                    .collect()
                    .await
                    .map_err(anyhow::Error::from_boxed)?
                    .to_bytes();
                let ticket: String = serde_json::from_slice(&bytes)?;
                Ok(http::json(self.pair(Ticket::decode(&ticket)?).await?))
            }
            (Method::POST, "/_xe/unpair") => {
                let bytes = Limited::new(request.into_body(), 256)
                    .collect()
                    .await
                    .map_err(anyhow::Error::from_boxed)?
                    .to_bytes();
                let id: String = serde_json::from_slice(&bytes)?;
                self.remove_peer(id.parse()?).await?;
                Ok(http::json(serde_json::json!({ "removed": id })))
            }
            _ => Ok(http::error(
                StatusCode::NOT_FOUND,
                "unknown_control",
                "unknown local control endpoint",
            )),
        }
    }

    async fn probe(&self, hostname: &str) -> bool {
        let config = self.state.get();
        let Ok((uri, host)) = upstream_target(&config, hostname, "/") else {
            return false;
        };
        let Ok(request) = Request::builder()
            .method(Method::HEAD)
            .uri(uri)
            .header(HOST, host)
            .body(http::full(""))
        else {
            return false;
        };
        matches!(timeout(Duration::from_millis(config.probe_timeout_ms), self.client.request(request)).await,
            Ok(Ok(response)) if response.status() != StatusCode::NOT_FOUND)
    }

    async fn connection(
        self: &Arc<Self>,
        address: &EndpointAddr,
        deadline: Deadline,
    ) -> Result<Connection> {
        self.operation(deadline, async {
            let attempt = {
                let mut connections = self.connections.lock().await;
                connections.retain(|_, entry| match entry.attempt.peek() {
                    None => true,
                    Some(Ok(connection)) => connection.close_reason().is_none(),
                    Some(Err(_)) => entry.started.elapsed() < Duration::from_secs(1),
                });
                if let Some(entry) = connections.get(&address.id) {
                    entry.attempt.clone()
                } else {
                    let (send, receive) = oneshot::channel();
                    let attempt = async move {
                        receive
                            .await
                            .unwrap_or_else(|_| Err(Arc::from("connection attempt cancelled")))
                    }
                    .boxed()
                    .shared();
                    connections.insert(
                        address.id,
                        PeerConnection {
                            attempt: attempt.clone(),
                            started: Instant::now(),
                        },
                    );
                    let agent = self.clone();
                    let address = address.clone();
                    self.tasks.spawn(async move {
                        let result = agent
                            .dial(address)
                            .await
                            .map_err(|error| Arc::<str>::from(format!("{error:#}")));
                        let _ = send.send(result);
                    });
                    attempt
                }
            };
            attempt.await.map_err(|error| anyhow::anyhow!("{error}"))
        })
        .await
    }

    async fn dial(self: &Arc<Self>, address: EndpointAddr) -> Result<Connection> {
        let deadline = Deadline::now() + OPERATION_TIMEOUT;
        self.operation(deadline, async {
            let trusted = self.state.get().peers.contains_key(&address.id.to_string());
            let slots = if trusted {
                &self.peer_slots
            } else {
                &self.pending_slots
            };
            let permit = slots.clone().acquire_owned().await?;
            let slot = if trusted {
                ConnectionSlot::Trusted(permit)
            } else {
                ConnectionSlot::Pending(permit)
            };
            let hint_deadline = (Deadline::now() + HINT_TIMEOUT).min(deadline);
            let connection = match timeout_at(
                hint_deadline,
                self.endpoint.connect(address.clone(), protocol::ALPN),
            )
            .await
            {
                Ok(Ok(connection)) => connection,
                _ if !address.is_empty() => {
                    self.endpoint.connect(address.id, protocol::ALPN).await?
                }
                Ok(Err(error)) => return Err(error.into()),
                Err(error) => return Err(error.into()),
            };
            let agent = self.clone();
            let accepted = connection.clone();
            if !self.tasks.spawn(async move {
                agent.accept_streams(accepted, slot, deadline).await;
            }) {
                connection.close(1u8.into(), b"agent closing");
                anyhow::bail!("agent is closing");
            }
            Ok(connection)
        })
        .await
    }

    async fn accept_streams(
        self: Arc<Self>,
        connection: Connection,
        slot: ConnectionSlot,
        auth_deadline: Deadline,
    ) {
        {
            let mut live = self.live_connections.lock().await;
            live.retain(|connection| connection.close_reason().is_none());
            live.push(connection.clone());
        }
        let _permit = match slot {
            ConnectionSlot::Trusted(permit) => permit,
            ConnectionSlot::Pending(pending) => {
                // One absolute deadline covers silence, partial frames and all retries.
                // Pending streams are processed serially, bounding their request use too.
                match timeout_at(auth_deadline, self.authenticate(&connection)).await {
                    Ok(Ok(permit)) => {
                        drop(pending);
                        permit
                    }
                    _ => {
                        connection.close(
                            1u8.into(),
                            b"pairing required before authentication deadline",
                        );
                        return;
                    }
                }
            }
        };
        while let Ok((mut send, mut receive)) = connection.accept_bi().await {
            let Ok(permit) = self.requests.clone().try_acquire_owned() else {
                let _ = send.reset(3u8.into());
                let _ = receive.stop(3u8.into());
                continue;
            };
            let agent = self.clone();
            let caller = connection.remote_id();
            let connection = connection.clone();
            self.tasks.spawn(async move {
                if let Err(error) = agent
                    .incoming(caller, connection, send, receive, Arc::new(permit))
                    .await
                {
                    eprintln!("peer {caller}: {error:#}");
                }
            });
        }
    }

    async fn authenticate(
        self: &Arc<Self>,
        connection: &Connection,
    ) -> Result<OwnedSemaphorePermit> {
        let caller = connection.remote_id();
        loop {
            let changed = self.peer_changes.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.state.get().peers.contains_key(&caller.to_string()) {
                return Ok(self.peer_slots.clone().try_acquire_owned()?);
            }
            tokio::select! {
                _ = &mut changed => {},
                stream = connection.accept_bi() => {
                    let (mut send, mut receive) = stream?;
                    let Ok(permit) = self.requests.clone().try_acquire_owned() else {
                        let _ = send.reset(3u8.into());
                        let _ = receive.stop(3u8.into());
                        continue;
                    };
                    if let Some(trusted) = self.clone().incoming(caller, connection.clone(), send, receive, Arc::new(permit)).await? {
                        return Ok(trusted);
                    }
                }
            }
        }
    }

    async fn incoming(
        self: Arc<Self>,
        caller: PublicKey,
        connection: Connection,
        mut send: SendStream,
        mut receive: RecvStream,
        lease: Arc<OwnedSemaphorePermit>,
    ) -> Result<Option<OwnedSemaphorePermit>> {
        let deadline = Deadline::now() + OPERATION_TIMEOUT;
        let operation: Operation = self
            .operation(deadline, protocol::read(&mut receive))
            .await?;
        if let Operation::Pair { token, peer } = operation {
            return self
                .operation(deadline, async {
                    if !self.pairing_rate.lock().await.allow(caller) {
                        protocol::write(&mut send, &Reply::no("pairing rate limit reached"))
                            .await?;
                        send.finish()?;
                        return Ok(None);
                    }
                    ensure!(peer.address.id == caller, "pairing identity mismatch");
                    validate_profile(&peer.profile)?;
                    let id = caller.to_string();
                    let mut promotion = None;
                    let reply = if !self.state.get().invitation_authorizes(&token, &id) {
                        // Unknown/expired/revoked tickets never reach the persistence worker.
                        Reply::no("invalid, expired, or revoked invitation")
                    } else {
                        if !self.state.get().peers.contains_key(&id) {
                            // Reserve trusted capacity before saving or acknowledging a new peer.
                            match self.peer_slots.clone().try_acquire_owned() {
                                Ok(permit) => promotion = Some(permit),
                                Err(_) => {
                                    protocol::write(
                                        &mut send,
                                        &Reply::no("trusted peer connection limit reached"),
                                    )
                                    .await?;
                                    send.finish()?;
                                    return Ok(None);
                                }
                            }
                        }
                        self.state
                            .update(move |config| {
                                // Recheck under the update lock: unpair may have won the race.
                                if !config.invitation_authorizes(&token, &id) {
                                    return Ok(Reply::no(
                                        "invalid, expired, or revoked invitation",
                                    ));
                                }
                                config.invitations.get_mut(&token).unwrap().redeemed_by =
                                    Some(id.clone());
                                config.peers.insert(id, peer);
                                Ok(Reply::ok(None))
                            })
                            .await?
                    };
                    let reply = if reply.ok {
                        self.peer_changes.notify_waiters();
                        self.misses.lock().await.clear();
                        Reply::ok(Some(self.peer()?))
                    } else {
                        promotion = None;
                        reply
                    };
                    protocol::write(&mut send, &reply).await?;
                    send.finish()?;
                    Ok(promotion)
                })
                .await;
        }
        if !self.state.get().peers.contains_key(&caller.to_string()) {
            self.operation(deadline, async {
                protocol::write(&mut send, &Reply::no("peer is not paired")).await?;
                send.finish()?;
                let _ = send.stopped().await;
                Ok(())
            })
            .await?;
            connection.close(1u8.into(), b"not paired");
            return Ok(None);
        }
        let (hostname, forwarding, scheme) = match operation {
            Operation::Resolve { hostname } => (http::hostname(&hostname)?, false, Scheme::Http),
            Operation::Http { hostname, scheme } => (http::hostname(&hostname)?, true, scheme),
            Operation::Pair { .. } => unreachable!(),
        };
        let available = self
            .operation(deadline, async {
                let available = self.probe(&hostname).await;
                let reply = if available {
                    Reply::ok(Some(self.peer()?))
                } else {
                    Reply::no("hostname unavailable")
                };
                protocol::write(&mut send, &reply).await?;
                Ok(available)
            })
            .await?;
        if !available || !forwarding {
            send.finish()?;
            return Ok(None);
        }
        let io = TokioIo::new(tokio::io::join(receive, send));
        let _stream_lease = lease.clone();
        let service = service_fn(move |request: Request<Incoming>| {
            let agent = self.clone();
            let hostname = hostname.clone();
            let lease = lease.clone();
            async move {
                let response = match http::request_hostname(request.uri(), request.headers()) {
                    Ok(actual) if actual == hostname => {
                        if !agent.state.get().peers.contains_key(&caller.to_string()) {
                            http::error(StatusCode::FORBIDDEN, "not_paired", "peer was removed")
                        } else {
                            match agent
                                .forward_local(request, &hostname, Some(caller), scheme, lease)
                                .await
                            {
                                Ok(response) => response,
                                Err(error) => {
                                    http::error(StatusCode::BAD_GATEWAY, "upstream_failed", error)
                                }
                            }
                        }
                    }
                    _ => http::error(
                        StatusCode::BAD_REQUEST,
                        "invalid_host",
                        "HTTP hostname does not match stream target",
                    ),
                };
                Ok::<_, Infallible>(response)
            }
        });
        hyper::server::conn::http1::Builder::new()
            .keep_alive(false)
            .timer(TokioTimer::new())
            .header_read_timeout(HEADER_TIMEOUT)
            .serve_connection(io, service)
            .with_upgrades()
            .await?;
        Ok(None)
    }

    async fn open_http(
        self: &Arc<Self>,
        peer: &Peer,
        hostname: &str,
        scheme: Scheme,
        deadline: Deadline,
    ) -> Result<Option<Stream>> {
        self.operation(deadline, async {
            let connection = self.connection(&peer.address, deadline).await?;
            let (mut send, mut receive) = connection.open_bi().await?;
            protocol::write(
                &mut send,
                &Operation::Http {
                    hostname: hostname.into(),
                    scheme,
                },
            )
            .await?;
            let reply: Reply = protocol::read(&mut receive).await?;
            Ok(reply.ok.then_some((send, receive)))
        })
        .await
    }

    async fn remote_stream(
        self: &Arc<Self>,
        hostname: &str,
        scheme: Scheme,
    ) -> Result<Option<(String, Stream)>> {
        let deadline = Deadline::now() + OPERATION_TIMEOUT;
        self.operation(
            deadline,
            self.remote_stream_until(hostname, scheme, deadline),
        )
        .await
    }

    async fn remote_stream_until(
        self: &Arc<Self>,
        hostname: &str,
        scheme: Scheme,
        deadline: Deadline,
    ) -> Result<Option<(String, Stream)>> {
        let config = self.state.get();
        if let Some(id) = config.routes.get(hostname) {
            if let Some(peer) = config.peers.get(id)
                && let Ok(Some(stream)) = self
                    .open_http(
                        peer,
                        hostname,
                        scheme,
                        (Deadline::now() + CACHED_TIMEOUT).min(deadline),
                    )
                    .await
            {
                return Ok(Some((id.clone(), stream)));
            }
            self.invalidate(hostname, id).await?;
        }
        let Some(peer) = self.discover(hostname, deadline).await? else {
            return Ok(None);
        };
        let id = peer.address.id.to_string();
        match self.open_http(&peer, hostname, scheme, deadline).await {
            Ok(Some(stream)) => Ok(Some((id, stream))),
            _ => {
                self.invalidate(hostname, &id).await?;
                Ok(None)
            }
        }
    }

    async fn discover(
        self: &Arc<Self>,
        hostname: &str,
        deadline: Deadline,
    ) -> Result<Option<Peer>> {
        let gate = {
            let mut lookups = self.lookups.lock().await;
            lookups.retain(|_, gate| gate.strong_count() > 0);
            match lookups.get(hostname).and_then(Weak::upgrade) {
                Some(gate) => gate,
                None => {
                    let gate = Arc::new(Mutex::new(()));
                    lookups.insert(hostname.into(), Arc::downgrade(&gate));
                    gate
                }
            }
        };
        let _guard = gate.lock().await;
        let config = self.state.get();
        if let Some(peer) = config
            .routes
            .get(hostname)
            .and_then(|id| config.peers.get(id))
        {
            return Ok(Some(peer.clone()));
        }
        if self
            .misses
            .lock()
            .await
            .get(hostname)
            .is_some_and(|time| time.elapsed() < Duration::from_secs(1))
        {
            return Ok(None);
        }
        let mut probes = FuturesUnordered::new();
        for peer in config.peers.values().cloned() {
            let agent = self.clone();
            probes.push(async move {
                agent
                    .operation(deadline, async {
                        let connection = agent.connection(&peer.address, deadline).await?;
                        let (mut send, mut receive) = connection.open_bi().await?;
                        protocol::write(
                            &mut send,
                            &Operation::Resolve {
                                hostname: hostname.into(),
                            },
                        )
                        .await?;
                        send.finish()?;
                        let reply: Reply = protocol::read(&mut receive).await?;
                        let advertised = reply.peer.unwrap_or(peer.clone());
                        ensure!(
                            advertised.address.id == peer.address.id,
                            "discovery identity mismatch"
                        );
                        validate_profile(&advertised.profile)?;
                        Ok::<_, anyhow::Error>(reply.ok.then_some(advertised))
                    })
                    .await
                    .ok()
                    .flatten()
            });
        }
        while let Some(result) = probes.next().await {
            if let Some(peer) = result {
                let stored = peer.clone();
                let hostname = hostname.to_string();
                let accepted = self
                    .state
                    .update(move |config| {
                        let id = stored.address.id.to_string();
                        if !config.peers.contains_key(&id) {
                            return Ok(false);
                        }
                        config.peers.insert(id.clone(), stored);
                        config.routes.insert(hostname, id);
                        Ok(true)
                    })
                    .await?;
                if accepted {
                    return Ok(Some(peer));
                }
            }
        }
        let mut misses = self.misses.lock().await;
        misses.retain(|_, time| time.elapsed() < Duration::from_secs(1));
        misses.insert(hostname.into(), Instant::now());
        Ok(None)
    }

    async fn invalidate(&self, hostname: &str, peer: &str) -> Result<()> {
        let hostname = hostname.to_string();
        let peer = peer.to_string();
        self.state
            .update(move |config| {
                if config.routes.get(&hostname).is_some_and(|id| *id == peer) {
                    config.routes.remove(&hostname);
                }
                Ok(())
            })
            .await
    }

    async fn forward_local(
        &self,
        mut request: Request<Incoming>,
        hostname: &str,
        caller: Option<PublicKey>,
        scheme: Scheme,
        lease: Arc<OwnedSemaphorePermit>,
    ) -> Result<Response<Body>> {
        let config = self.state.get();
        let path = request
            .uri()
            .path_and_query()
            .map_or("/", |path| path.as_str());
        let (uri, host) = upstream_target(&config, hostname, path)?;
        let upgrade = request.headers().contains_key(UPGRADE);
        let downstream = upgrade.then(|| hyper::upgrade::on(&mut request));
        let public_host = request.headers().get(HOST).cloned();
        http::clean_headers(request.headers_mut(), upgrade);
        request.headers_mut().insert(HOST, host.parse()?);
        if let Some(host) = public_host {
            request.headers_mut().insert("x-forwarded-host", host);
        }
        request
            .headers_mut()
            .insert("x-forwarded-proto", scheme.as_str().parse()?);
        if let Some(caller) = caller {
            request
                .headers_mut()
                .insert(SOURCE_HEADER, caller.to_string().parse()?);
        }
        *request.uri_mut() = uri;
        let mut response = timeout(HEADER_TIMEOUT, self.client.request(request.map(http::body)))
            .await
            .context("upstream response timed out")??;
        self.bridge_upgrade(downstream, &mut response, lease);
        let upgraded = response.status() == StatusCode::SWITCHING_PROTOCOLS;
        http::clean_headers(response.headers_mut(), upgraded);
        Ok(response.map(http::body))
    }

    async fn forward_remote(
        &self,
        mut request: Request<Incoming>,
        (send, receive): Stream,
        lease: Arc<OwnedSemaphorePermit>,
    ) -> Result<Response<Body>> {
        let upgrade = request.headers().contains_key(UPGRADE);
        let downstream = upgrade.then(|| hyper::upgrade::on(&mut request));
        http::clean_headers(request.headers_mut(), upgrade);
        *request.uri_mut() = request
            .uri()
            .path_and_query()
            .map_or("/", |path| path.as_str())
            .parse()?;
        let deadline = Deadline::now() + HEADER_TIMEOUT;
        let (mut sender, connection) = timeout_at(
            deadline,
            hyper::client::conn::http1::handshake(TokioIo::new(tokio::io::join(receive, send))),
        )
        .await??;
        self.tasks.spawn(async move {
            let _ = connection.with_upgrades().await;
        });
        let mut response = timeout_at(deadline, sender.send_request(request.map(http::body)))
            .await
            .context("peer response timed out; request was not replayed")??;
        self.bridge_upgrade(downstream, &mut response, lease);
        let upgraded = response.status() == StatusCode::SWITCHING_PROTOCOLS;
        // Preserve the bridge error marker, but never routing or identity headers.
        let bridge_error = response.headers().get(ERROR_HEADER).cloned();
        http::clean_headers(response.headers_mut(), upgraded);
        if let Some(error) = bridge_error {
            response.headers_mut().insert(ERROR_HEADER, error);
        }
        Ok(response.map(http::body))
    }

    fn gateway_scheme(&self, request: &Request<Incoming>) -> Result<Scheme> {
        let config = self.state.get();
        if request
            .headers()
            .get(GATEWAY_HEADER)
            .and_then(|v| v.to_str().ok())
            != Some(config.control_token.as_str())
        {
            return Ok(Scheme::Http);
        }
        ensure!(
            request.headers().get_all(GATEWAY_HEADER).iter().count() == 1
                && request
                    .headers()
                    .get_all("x-forwarded-proto")
                    .iter()
                    .count()
                    == 1,
            "trusted gateway must provide exactly one token and one scheme"
        );
        match request
            .headers()
            .get("x-forwarded-proto")
            .and_then(|v| v.to_str().ok())
        {
            Some("http") => Ok(Scheme::Http),
            Some("https") => Ok(Scheme::Https),
            _ => anyhow::bail!(
                "trusted gateway must supply one http or https X-Forwarded-Proto value"
            ),
        }
    }

    fn bridge_upgrade(
        &self,
        downstream: Option<hyper::upgrade::OnUpgrade>,
        response: &mut Response<Incoming>,
        lease: Arc<OwnedSemaphorePermit>,
    ) {
        if response.status() != StatusCode::SWITCHING_PROTOCOLS {
            return;
        }
        if let Some(downstream) = downstream {
            let upstream = hyper::upgrade::on(response);
            self.tasks.spawn(async move {
                let _lease = lease;
                if let (Ok(downstream), Ok(upstream)) = tokio::join!(downstream, upstream) {
                    let _ = tokio::io::copy_bidirectional(
                        &mut TokioIo::new(downstream),
                        &mut TokioIo::new(upstream),
                    )
                    .await;
                }
            });
        }
    }
}

pub fn create_invitation(store: &StateStore, peer: Peer, lifetime: Duration) -> Result<Ticket> {
    let ticket = new_invitation(peer, lifetime)?;
    store.update::<Config, _>(|config| {
        remember_invitation(config, &ticket);
        Ok(())
    })?;
    Ok(ticket)
}

fn new_invitation(peer: Peer, lifetime: Duration) -> Result<Ticket> {
    ensure!(
        (1..=86_400).contains(&lifetime.as_secs()),
        "invitation lifetime must be between 1 second and 24 hours"
    );
    let token = new_token();
    let expires_at = now() + lifetime.as_secs();
    Ok(Ticket {
        version: 1,
        peer,
        token,
        expires_at,
    })
}

fn remember_invitation(config: &mut Config, ticket: &Ticket) {
    config
        .invitations
        .retain(|_, invitation| invitation.expires_at >= now());
    config.invitations.insert(
        ticket.token.clone(),
        Invitation {
            expires_at: ticket.expires_at,
            redeemed_by: None,
        },
    );
}

fn upstream_target(config: &Config, hostname: &str, path: &str) -> Result<(Uri, String)> {
    let upstream: Uri = config.upstream.parse()?;
    let authority = upstream
        .authority()
        .context("upstream is missing an authority")?;
    let uri = format!(
        "{}://{}{path}",
        upstream.scheme_str().unwrap_or("http"),
        authority
    )
    .parse()?;
    let host = match upstream.port_u16() {
        Some(port) => format!("{hostname}:{port}"),
        None => hostname.into(),
    };
    Ok((uri, host))
}

#[cfg(test)]
#[path = "agent_tests.rs"]
mod tests;
