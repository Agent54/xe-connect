use super::*;
use crate::state::Profile;
use hyper_util::rt::TokioExecutor;
use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicBool, AtomicU16, AtomicU64, AtomicUsize, Ordering},
};
use tokio::task::JoinHandle;

struct Gateway {
    address: std::net::SocketAddr,
    available: Arc<AtomicBool>,
    delay_ms: Arc<AtomicU64>,
    heads: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
    head_status: Arc<AtomicU16>,
    fail_writes: Arc<AtomicBool>,
    active_connections: Arc<AtomicUsize>,
    peak_connections: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl Gateway {
    async fn start(hosts: &[&str]) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let available = Arc::new(AtomicBool::new(true));
        let delay_ms = Arc::new(AtomicU64::new(0));
        let heads = Arc::new(AtomicUsize::new(0));
        let writes = Arc::new(AtomicUsize::new(0));
        let head_status = Arc::new(AtomicU16::new(200));
        let fail_writes = Arc::new(AtomicBool::new(false));
        let active_connections = Arc::new(AtomicUsize::new(0));
        let peak_connections = Arc::new(AtomicUsize::new(0));
        let active = active_connections.clone();
        let peak = peak_connections.clone();
        let hosts: BTreeSet<String> = hosts.iter().map(|name| name.to_string()).collect();
        let state = (
            available.clone(),
            delay_ms.clone(),
            heads.clone(),
            writes.clone(),
            head_status.clone(),
            fail_writes.clone(),
        );
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(count, Ordering::SeqCst);
                let active = active.clone();
                let hosts = hosts.clone();
                let (available, delay, heads, writes, head_status, fail_writes) = state.clone();
                tokio::spawn(async move {
                    let _connection = ActiveConnection(active);
                    let service = service_fn(move |mut request: Request<Incoming>| {
                        let hosts = hosts.clone();
                        let available = available.clone();
                        let delay = delay.clone();
                        let heads = heads.clone();
                        let writes = writes.clone();
                        let head_status = head_status.clone();
                        let fail_writes = fail_writes.clone();
                        async move {
                            let host = request.headers().get(HOST).unwrap().to_str().unwrap();
                            let name = http::hostname(host).unwrap_or_default();
                            if request.method() == Method::HEAD {
                                heads.fetch_add(1, Ordering::SeqCst);
                                tokio::time::sleep(Duration::from_millis(
                                    delay.load(Ordering::SeqCst),
                                ))
                                .await;
                                let status =
                                    if hosts.contains(&name) && available.load(Ordering::SeqCst) {
                                        StatusCode::from_u16(head_status.load(Ordering::SeqCst))
                                            .unwrap()
                                    } else {
                                        StatusCode::NOT_FOUND
                                    };
                                return Ok::<_, std::io::Error>(
                                    Response::builder()
                                        .status(status)
                                        .body(http::full(""))
                                        .unwrap(),
                                );
                            }
                            if request.uri().path() == "/upgrade" {
                                let upgraded = hyper::upgrade::on(&mut request);
                                tokio::spawn(async move {
                                    if let Ok(upgraded) = upgraded.await {
                                        let (mut read, mut write) =
                                            tokio::io::split(TokioIo::new(upgraded));
                                        let _ = tokio::io::copy(&mut read, &mut write).await;
                                    }
                                });
                                return Ok(Response::builder()
                                    .status(StatusCode::SWITCHING_PROTOCOLS)
                                    .header("connection", "upgrade")
                                    .header("upgrade", "test-echo")
                                    .body(http::full(""))
                                    .unwrap());
                            }
                            if request.method() == Method::POST {
                                writes.fetch_add(1, Ordering::SeqCst);
                            }
                            let (parts, body) = request.into_parts();
                            let bytes = body.collect().await.unwrap().to_bytes();
                            if parts.uri.path() == "/hold" {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                            if parts.method == Method::POST && fail_writes.load(Ordering::SeqCst) {
                                return Err(std::io::Error::other("connection lost after write"));
                            }
                            let mut response = http::json(serde_json::json!({
                                "method": parts.method.as_str(), "path": parts.uri.to_string(),
                                "host": parts.headers[HOST].to_str().unwrap(),
                                "public_host": parts.headers.get("x-forwarded-host").and_then(|v| v.to_str().ok()),
                                "source_peer": parts.headers.get(SOURCE_HEADER).and_then(|v| v.to_str().ok()),
                                "cookie": parts.headers.get("cookie").and_then(|v| v.to_str().ok()),
                                "authorization": parts.headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()),
                                "proto": parts.headers.get("x-forwarded-proto").and_then(|v| v.to_str().ok()),
                                "gateway_token": parts.headers.get(GATEWAY_HEADER).and_then(|v| v.to_str().ok()),
                                "body": String::from_utf8_lossy(&bytes),
                            }));
                            if parts.uri.path() == "/missing-document" {
                                *response.status_mut() = StatusCode::NOT_FOUND;
                            }
                            response
                                .headers_mut()
                                .insert(ERROR_HEADER, "forged-app-error".parse().unwrap());
                            response
                                .headers_mut()
                                .insert(SOURCE_HEADER, "forged-app-peer".parse().unwrap());
                            Ok(response)
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .with_upgrades()
                        .await;
                });
            }
        });
        Ok(Self {
            address,
            available,
            delay_ms,
            heads,
            writes,
            head_status,
            fail_writes,
            active_connections,
            peak_connections,
            task,
        })
    }
}

struct ActiveConnection(Arc<AtomicUsize>);

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Node {
    agent: Arc<Agent>,
    address: std::net::SocketAddr,
    _directory: tempfile::TempDir,
    task: JoinHandle<Result<()>>,
}

impl Node {
    async fn start(gateway: &Gateway, name: &str) -> Result<Self> {
        let directory = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR"))?;
        let store = StateStore::open(directory.path())?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        store.update::<Config, _>(|config| {
            config.listen = address;
            config.upstream = format!("http://{}", gateway.address);
            config.profile = Profile {
                device_name: name.into(),
                user_name: Some("owner".into()),
            };
            config.probe_timeout_ms = 700;
            config.validate()
        })?;
        let guard = store.acquire_agent()?;
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(store.identity()?)
            .alpns(vec![protocol::ALPN.to_vec()])
            .clear_ip_transports()
            .bind_addr("127.0.0.1:0")?
            .bind()
            .await?;
        let agent = Agent::with_endpoint(store, endpoint, guard).await?;
        let task = tokio::spawn(agent.clone().serve_http(listener));
        Ok(Self {
            agent,
            address,
            _directory: directory,
            task,
        })
    }

    async fn pair_with(&self, other: &Self) -> Result<Ticket> {
        let ticket = other.agent.invite(Duration::from_secs(600)).await?;
        self.agent.pair(ticket.clone()).await?;
        Ok(ticket)
    }

    async fn restart(&mut self) -> Result<()> {
        self.agent.close().await;
        self.task.abort();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        self.address = listener.local_addr()?;
        let store = self.agent.store.clone();
        store.update::<Config, _>(|config| {
            config.listen = self.address;
            Ok(())
        })?;
        let guard = store.acquire_agent()?;
        let endpoint = Endpoint::builder(presets::Minimal)
            .secret_key(store.identity()?)
            .alpns(vec![protocol::ALPN.to_vec()])
            .clear_ip_transports()
            .bind_addr("127.0.0.1:0")?
            .bind()
            .await?;
        self.agent = Agent::with_endpoint(store, endpoint, guard).await?;
        self.task = tokio::spawn(self.agent.clone().serve_http(listener));
        Ok(())
    }

    async fn request(&self, method: Method, host: &str, path: &str) -> Result<Response<Incoming>> {
        let request = Request::builder()
            .method(method)
            .uri(format!("http://{}{path}", self.address))
            .header(HOST, format!("{host}:{}", self.address.port()))
            .header("cookie", "session=example")
            .header(AUTHORIZATION, "Bearer app-token")
            .header(SOURCE_HEADER, "forged-browser-peer")
            .body(http::full("payload"))?;
        Ok(Client::builder(TokioExecutor::new())
            .build_http()
            .request(request)
            .await?)
    }

    async fn diagnostics(&self, token: Option<&str>) -> Result<Response<Incoming>> {
        let mut request =
            Request::builder().uri(format!("http://{}/_xe/diagnostics", self.address));
        if let Some(token) = token {
            request = request.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        Ok(Client::builder(TokioExecutor::new())
            .build_http()
            .request(request.body(http::full(""))?)
            .await?)
    }

    async fn control_request(
        &self,
        method: Method,
        path: &str,
        value: serde_json::Value,
        authorized: bool,
    ) -> Result<Response<Incoming>> {
        let mut request = Request::builder()
            .method(method)
            .uri(format!("http://{}{path}", self.address));
        if authorized {
            request = request.header(
                AUTHORIZATION,
                format!("Bearer {}", self.agent.config().control_token),
            );
        }
        Ok(Client::builder(TokioExecutor::new())
            .build_http()
            .request(request.body(http::full(serde_json::to_vec(&value)?))?)
            .await?)
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.task.abort();
        let agent = self.agent.clone();
        tokio::spawn(async move {
            agent.close().await;
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_test_uses_normal_routing_without_forwarding_control_credentials() -> Result<()> {
    let a_gateway = Gateway::start(&["alpha.localhost"]).await?;
    let b_gateway = Gateway::start(&["beta.web.localhost"]).await?;
    let a = Node::start(&a_gateway, "A").await?;
    let b = Node::start(&b_gateway, "B").await?;
    let body = serde_json::json!({ "hostname": "beta.web.localhost" });
    let rejected = a
        .control_request(Method::POST, "/_xe/test", body.clone(), false)
        .await?;
    assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(a_gateway.heads.load(Ordering::SeqCst), 0);
    assert!(a.agent.test_service("example.com").await.is_err());
    assert!(
        a.agent
            .test_service("http://alpha.localhost/")
            .await
            .is_err()
    );
    assert_eq!(a_gateway.heads.load(Ordering::SeqCst), 0);

    let local = a.agent.test_service("alpha.localhost").await?;
    assert_eq!(local["provider"], "local");
    assert_eq!(local["status"], 200);
    let missing = a.agent.test_service("beta.web.localhost").await?;
    assert_eq!(missing["provider"], "none");
    assert_eq!(missing["status"], 404);
    a.pair_with(&b).await?;
    let response = a
        .control_request(Method::POST, "/_xe/test", body, true)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await?.to_bytes();
    let remote: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(remote["provider"], "peer");
    assert_eq!(remote["peer_id"], b.agent.endpoint.id().to_string());
    assert_eq!(remote["status"], 200);
    let preview = remote["body_preview"].as_str().unwrap();
    assert!(!preview.contains(&a.agent.config().control_token));
    assert!(!preview.contains("Bearer"));
    assert_eq!(
        a.agent.config().routes["beta.web.localhost"],
        b.agent.endpoint.id().to_string()
    );
    a.agent.close().await;
    b.agent.close().await;
    Ok(())
}

#[cfg(feature = "web-ui")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_ui_is_a_public_shell_only_on_the_local_control_host() -> Result<()> {
    let gateway = Gateway::start(&[]).await?;
    let node = Node::start(&gateway, "UI").await?;
    let response = node
        .control_request(Method::GET, "/_xe/ui/", serde_json::Value::Null, false)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-type"],
        "text/html; charset=utf-8"
    );
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert!(
        response.headers()["content-security-policy"]
            .to_str()?
            .contains("frame-ancestors 'none'")
    );
    let html = String::from_utf8(response.into_body().collect().await?.to_bytes().to_vec())?;
    assert!(!html.contains(&node.agent.config().control_token));
    let assets: Vec<_> = html
        .split('"')
        .filter(|value| value.starts_with("/_xe/ui/assets/"))
        .collect();
    assert_eq!(assets.len(), 2);
    for path in assets {
        let response = node
            .control_request(Method::GET, path, serde_json::Value::Null, false)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!response.into_body().collect().await?.to_bytes().is_empty());
        let response = node
            .control_request(Method::HEAD, path, serde_json::Value::Null, false)
            .await?;
        assert!(response.into_body().collect().await?.to_bytes().is_empty());
    }
    let unknown = node
        .control_request(
            Method::GET,
            "/_xe/ui/../config.json",
            serde_json::Value::Null,
            false,
        )
        .await?;
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
    let app = node
        .request(Method::GET, "some.localhost", "/_xe/ui/")
        .await?;
    assert_eq!(app.status(), StatusCode::NOT_FOUND);
    let post = node
        .control_request(Method::POST, "/_xe/ui/", serde_json::Value::Null, false)
        .await?;
    assert_eq!(post.status(), StatusCode::METHOD_NOT_ALLOWED);
    node.agent.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pairing_forwarding_cache_and_application_errors() -> Result<()> {
    let empty = Gateway::start(&[]).await?;
    let backend = Gateway::start(&["notes.localhost", "notes.web.localhost"]).await?;
    let mut a = Node::start(&empty, "Mac").await?;
    let b = Node::start(&backend, "Cloud").await?;
    let ticket = a.pair_with(&b).await?;
    // A lost pairing acknowledgement can be recovered by the same identity.
    a.agent.pair(ticket).await?;
    assert!(
        a.agent
            .store
            .read::<Config>()?
            .peers
            .contains_key(&b.agent.endpoint.id().to_string())
    );
    assert!(
        b.agent
            .store
            .read::<Config>()?
            .peers
            .contains_key(&a.agent.endpoint.id().to_string())
    );

    let response = a
        .request(Method::POST, "notes.localhost", "/documents?q=one%2Ftwo")
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.headers().contains_key(ERROR_HEADER));
    assert!(!response.headers().contains_key(SOURCE_HEADER));
    let value: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await?.to_bytes())?;
    assert_eq!(value["method"], "POST");
    assert_eq!(value["path"], "/documents?q=one%2Ftwo");
    assert_eq!(value["body"], "payload");
    assert_eq!(value["cookie"], "session=example");
    assert_eq!(value["authorization"], "Bearer app-token");
    assert_eq!(value["source_peer"], a.agent.endpoint.id().to_string());
    assert_eq!(
        value["public_host"],
        format!("notes.localhost:{}", a.address.port())
    );
    assert_eq!(backend.writes.load(Ordering::SeqCst), 1);
    assert_eq!(backend.heads.load(Ordering::SeqCst), 2); // discovery, then forwarding availability
    let id = b.agent.endpoint.id().to_string();
    assert_eq!(
        a.agent.store.read::<Config>()?.routes["notes.localhost"],
        id
    );

    let response = a
        .request(Method::GET, "notes.localhost", "/missing-document")
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    response.into_body().collect().await?;
    assert_eq!(backend.heads.load(Ordering::SeqCst), 3); // cached provider, no discovery
    assert_eq!(
        a.agent.store.read::<Config>()?.routes["notes.localhost"],
        id
    );
    assert_eq!(
        a.request(Method::GET, "notes.web.localhost", "/")
            .await?
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        a.request(Method::GET, "example.com", "/").await?.status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        a.request(Method::GET, "too.many.labels.localhost", "/")
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    let identity = a.agent.endpoint.id();
    let heads = backend.heads.load(Ordering::SeqCst);
    a.restart().await?;
    assert_eq!(a.agent.endpoint.id(), identity);
    let response = a.request(Method::GET, "notes.localhost", "/").await?;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await?;
    assert_eq!(backend.heads.load(Ordering::SeqCst), heads + 1);
    a.agent.close().await;
    b.agent.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_positive_probe_wins_and_failed_provider_is_replaced() -> Result<()> {
    let empty = Gateway::start(&[]).await?;
    let slow = Gateway::start(&["app.localhost"]).await?;
    let fast = Gateway::start(&["app.localhost"]).await?;
    slow.delay_ms.store(350, Ordering::SeqCst);
    let a = Node::start(&empty, "A").await?;
    let b = Node::start(&slow, "Slow").await?;
    let c = Node::start(&fast, "Fast").await?;
    a.pair_with(&b).await?;
    a.pair_with(&c).await?;
    let started = Instant::now();
    let response = a.request(Method::POST, "app.localhost", "/").await?;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await?;
    assert!(started.elapsed() < Duration::from_millis(300));
    assert_eq!(
        a.agent.store.read::<Config>()?.routes["app.localhost"],
        c.agent.endpoint.id().to_string()
    );
    assert_eq!(fast.writes.load(Ordering::SeqCst), 1);
    assert_eq!(slow.writes.load(Ordering::SeqCst), 0);

    fast.available.store(false, Ordering::SeqCst);
    slow.delay_ms.store(0, Ordering::SeqCst);
    let response = a.request(Method::POST, "app.localhost", "/").await?;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await?;
    assert_eq!(
        a.agent.store.read::<Config>()?.routes["app.localhost"],
        b.agent.endpoint.id().to_string()
    );
    assert_eq!(fast.writes.load(Ordering::SeqCst), 1);
    assert_eq!(slow.writes.load(Ordering::SeqCst), 1);
    a.agent.close().await;
    b.agent.close().await;
    c.agent.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invitation_cannot_pair_a_second_identity_and_unpaired_requests_are_denied() -> Result<()> {
    let gateway = Gateway::start(&["app.localhost"]).await?;
    let a = Node::start(&gateway, "A").await?;
    let b = Node::start(&gateway, "B").await?;
    let c = Node::start(&gateway, "C").await?;
    let ticket = a.pair_with(&b).await?;
    assert!(c.agent.pair(ticket).await.is_err());
    assert!(
        !b.agent
            .store
            .read::<Config>()?
            .peers
            .contains_key(&c.agent.endpoint.id().to_string())
    );
    let connection = c
        .agent
        .connection(
            &b.agent.endpoint.addr(),
            Deadline::now() + OPERATION_TIMEOUT,
        )
        .await?;
    let (mut send, mut receive) = connection.open_bi().await?;
    protocol::write(
        &mut send,
        &Operation::Resolve {
            hostname: "app.localhost".into(),
        },
    )
    .await?;
    let reply: Reply = timeout(CONNECT_TIMEOUT, protocol::read(&mut receive)).await??;
    assert!(!reply.ok);
    assert_eq!(reply.error.as_deref(), Some("peer is not paired"));
    assert_eq!(gateway.heads.load(Ordering::SeqCst), 0);
    a.agent.remove_peer(b.agent.endpoint.id()).await?;
    assert!(a.agent.store.read::<Config>()?.peers.is_empty());
    assert_eq!(
        a.request(Method::GET, "127.0.0.1", "/_xe/self")
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    a.agent.close().await;
    b.agent.close().await;
    c.agent.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_upgrade_survives_the_iroh_stream() -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let empty = Gateway::start(&[]).await?;
    let backend = Gateway::start(&["app.localhost"]).await?;
    let a = Node::start(&empty, "A").await?;
    let b = Node::start(&backend, "B").await?;
    a.pair_with(&b).await?;
    let stream = tokio::net::TcpStream::connect(a.address).await?;
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    let driver = tokio::spawn(async move { connection.with_upgrades().await });
    let request = Request::builder()
        .uri("/upgrade")
        .header(HOST, "app.localhost")
        .header("connection", "upgrade")
        .header("upgrade", "test-echo")
        .body(http::full(""))?;
    let mut response = sender.send_request(request).await?;
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    let upgraded = hyper::upgrade::on(&mut response).await?;
    let mut io = TokioIo::new(upgraded);
    io.write_all(b"streamed bytes").await?;
    let mut reply = [0; 14];
    timeout(Duration::from_secs(2), io.read_exact(&mut reply)).await??;
    assert_eq!(&reply, b"streamed bytes");
    drop(io);
    driver.abort();
    a.agent.close().await;
    b.agent.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_non_404_wins_and_a_timed_out_probe_falls_back() -> Result<()> {
    let local = Gateway::start(&["app.localhost"]).await?;
    let remote = Gateway::start(&["app.localhost"]).await?;
    let a = Node::start(&local, "A").await?;
    let b = Node::start(&remote, "B").await?;
    a.pair_with(&b).await?;
    local.head_status.store(503, Ordering::SeqCst);
    let response = a.request(Method::POST, "app.localhost", "/").await?;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await?;
    assert_eq!(local.writes.load(Ordering::SeqCst), 1);
    assert_eq!(remote.heads.load(Ordering::SeqCst), 0);

    a.agent
        .update_config(|config| {
            config.probe_timeout_ms = 30;
            Ok(())
        })
        .await?;
    local.delay_ms.store(150, Ordering::SeqCst);
    let response = a.request(Method::POST, "app.localhost", "/").await?;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await?;
    assert_eq!(local.writes.load(Ordering::SeqCst), 1);
    assert_eq!(remote.writes.load(Ordering::SeqCst), 1);
    a.agent.close().await;
    b.agent.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_requests_share_discovery_and_each_write_is_forwarded_once() -> Result<()> {
    let empty = Gateway::start(&[]).await?;
    let backend = Gateway::start(&["app.localhost"]).await?;
    let a = Node::start(&empty, "A").await?;
    let b = Node::start(&backend, "B").await?;
    a.pair_with(&b).await?;
    let replies = futures_util::future::join_all((0..8).map(|_| async {
        let response = a.request(Method::POST, "app.localhost", "/").await?;
        assert_eq!(response.status(), StatusCode::OK);
        response.into_body().collect().await?;
        Ok::<_, anyhow::Error>(())
    }))
    .await;
    for reply in replies {
        reply?;
    }
    assert_eq!(backend.writes.load(Ordering::SeqCst), 8);
    assert_eq!(backend.heads.load(Ordering::SeqCst), 9);
    a.agent.close().await;
    b.agent.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_write_is_not_replayed_to_another_provider() -> Result<()> {
    let empty = Gateway::start(&[]).await?;
    let failing = Gateway::start(&["app.localhost"]).await?;
    let alternative = Gateway::start(&["app.localhost"]).await?;
    failing.fail_writes.store(true, Ordering::SeqCst);
    alternative.delay_ms.store(200, Ordering::SeqCst);
    let a = Node::start(&empty, "A").await?;
    let b = Node::start(&failing, "B").await?;
    let c = Node::start(&alternative, "C").await?;
    a.pair_with(&b).await?;
    a.pair_with(&c).await?;
    let response = a.request(Method::POST, "app.localhost", "/").await?;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    response.into_body().collect().await?;
    assert_eq!(failing.writes.load(Ordering::SeqCst), 1);
    assert_eq!(alternative.writes.load(Ordering::SeqCst), 0);
    assert!(
        !a.agent
            .store
            .read::<Config>()?
            .routes
            .contains_key("app.localhost")
    );
    a.agent.close().await;
    b.agent.close().await;
    c.agent.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unpair_revokes_old_tickets_but_a_new_invitation_can_authorize_again() -> Result<()> {
    let gateway = Gateway::start(&[]).await?;
    let a = Node::start(&gateway, "A").await?;
    let b = Node::start(&gateway, "B").await?;
    let ticket = a.pair_with(&b).await?;
    a.agent.pair(ticket.clone()).await?;
    b.agent.remove_peer(a.agent.endpoint.id()).await?;
    assert!(!b.agent.config().invitations.contains_key(&ticket.token));
    assert!(a.agent.pair(ticket).await.is_err());
    assert!(b.agent.config().peers.is_empty());
    a.pair_with(&b).await?;
    assert!(
        b.agent
            .config()
            .peers
            .contains_key(&a.agent.endpoint.id().to_string())
    );
    a.agent.close().await;
    b.agent.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_pairing_is_rate_limited_and_never_waits_for_or_writes_disk() -> Result<()> {
    let gateway = Gateway::start(&[]).await?;
    let a = Node::start(&gateway, "A").await?;
    let b = Node::start(&gateway, "B").await?;
    timeout(Duration::from_secs(2), async {
        while b.agent.config().last_address.is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let connection = a
        .agent
        .connection(
            &b.agent.endpoint.addr(),
            Deadline::now() + OPERATION_TIMEOUT,
        )
        .await?;
    let path = b.agent.store.directory().join("config.json");
    let before = std::fs::metadata(&path)?;
    let locked = File::options()
        .read(true)
        .write(true)
        .open(b.agent.store.directory().join("config.lock"))?;
    locked.lock()?;
    for index in 0..10 {
        let (mut send, mut receive) = connection.open_bi().await?;
        let reply: Reply = timeout(Duration::from_secs(1), async {
            protocol::write(
                &mut send,
                &Operation::Pair {
                    token: new_token(),
                    peer: a.agent.peer()?,
                },
            )
            .await?;
            send.finish()?;
            protocol::read(&mut receive).await
        })
        .await??;
        assert!(!reply.ok);
        if index >= 8 {
            assert_eq!(reply.error.as_deref(), Some("pairing rate limit reached"));
        }
    }
    let after = std::fs::metadata(&path)?;
    assert_eq!(before.modified()?, after.modified()?);
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(before.ino(), after.ino());
    }
    locked.unlock()?;
    assert!(b.agent.config().peers.is_empty());
    let mut rate = PairingRate::default();
    for _ in 0..32 {
        assert!(rate.allow(iroh::SecretKey::generate().public()));
    }
    assert!(!rate.allow(iroh::SecretKey::generate().public()));
    a.agent.close().await;
    b.agent.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn silent_unpaired_connections_expire_without_using_trusted_capacity() -> Result<()> {
    let empty = Gateway::start(&[]).await?;
    let backend = Gateway::start(&["app.localhost"]).await?;
    let a = Node::start(&empty, "A").await?;
    let b = Node::start(&backend, "B").await?;
    a.pair_with(&b).await?;
    let pairing_attempts = b.agent.pairing_rate.lock().await.attempts.len();
    timeout(Duration::from_secs(2), async {
        while a.agent.pending_slots.available_permits() != MAX_PENDING_PAIRINGS
            || b.agent.pending_slots.available_permits() != MAX_PENDING_PAIRINGS
        {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let original = a
        .agent
        .connection(
            &b.agent.endpoint.addr(),
            Deadline::now() + OPERATION_TIMEOUT,
        )
        .await?;
    original.close(0u8.into(), b"test reconnect");
    timeout(Duration::from_secs(2), async {
        while b.agent.peer_slots.available_permits() != MAX_PEER_CONNECTIONS {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    let mut strangers = Vec::new();
    for _ in 0..MAX_PENDING_PAIRINGS + 1 {
        let endpoint = Endpoint::builder(presets::Minimal)
            .clear_ip_transports()
            .bind_addr("127.0.0.1:0")?
            .transport_config(
                iroh::endpoint::QuicTransportConfig::builder()
                    .keep_alive_interval(Duration::from_millis(50))
                    .max_idle_timeout(Some(Duration::from_secs(2).try_into()?))
                    .build(),
            )
            .bind()
            .await?;
        // No application stream is ever opened on these connections.
        let connection = timeout(
            Duration::from_secs(2),
            endpoint.connect(b.agent.endpoint.addr(), protocol::ALPN),
        )
        .await??;
        strangers.push((endpoint, connection));
    }
    let extra = &strangers.last().unwrap().1;
    let reason = timeout(Duration::from_secs(2), extra.closed()).await?;
    assert!(format!("{reason:?}").contains("connection limit reached"));
    assert_eq!(b.agent.pending_slots.available_permits(), 0);
    assert_eq!(b.agent.peer_slots.available_permits(), MAX_PEER_CONNECTIONS);
    assert_eq!(
        b.agent.pairing_rate.lock().await.attempts.len(),
        pairing_attempts
    );

    // The inbound side must classify this verified identity as trusted even
    // while every pending-pairing slot is occupied by an unknown device.
    let response = timeout(
        Duration::from_secs(2),
        a.request(Method::GET, "app.localhost", "/"),
    )
    .await??;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await?;
    let trusted = a
        .agent
        .connection(
            &b.agent.endpoint.addr(),
            Deadline::now() + OPERATION_TIMEOUT,
        )
        .await?;
    assert_eq!(
        b.agent.peer_slots.available_permits(),
        MAX_PEER_CONNECTIONS - 1
    );

    timeout(AUTHENTICATION_TIMEOUT + Duration::from_secs(2), async {
        for (_, connection) in strangers.iter().take(MAX_PENDING_PAIRINGS) {
            let reason = connection.closed().await;
            assert!(format!("{reason:?}").contains("authentication deadline"));
        }
        while b.agent.pending_slots.available_permits() != MAX_PENDING_PAIRINGS {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(
        trusted.close_reason().is_none(),
        "trusted connections do not expire at the pairing deadline"
    );
    assert_eq!(
        a.request(Method::GET, "app.localhost", "/").await?.status(),
        StatusCode::OK
    );
    for (endpoint, _) in strangers {
        endpoint.close().await;
    }
    a.agent.close().await;
    b.agent.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pairing_retries_and_partial_frames_cannot_extend_authentication() -> Result<()> {
    let gateway = Gateway::start(&[]).await?;
    let node = Node::start(&gateway, "Node").await?;
    let endpoint = Endpoint::builder(presets::Minimal)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")?
        .bind()
        .await?;
    let connection = endpoint
        .connect(node.agent.endpoint.addr(), protocol::ALPN)
        .await?;
    let started = Deadline::now();
    let peer = Peer {
        address: endpoint.addr(),
        profile: Profile::default(),
    };
    for _ in 0..3 {
        let (mut send, mut receive) = connection.open_bi().await?;
        protocol::write(
            &mut send,
            &Operation::Pair {
                token: new_token(),
                peer: peer.clone(),
            },
        )
        .await?;
        send.finish()?;
        let reply: Reply = protocol::read(&mut receive).await?;
        assert!(!reply.ok);
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    let (mut send, _receive) = connection.open_bi().await?;
    // A fresh, incomplete operation would have its own ten-second timeout;
    // the connection must close at the original deadline instead.
    send.write_all(&[0]).await?;
    let remaining = (started + AUTHENTICATION_TIMEOUT + Duration::from_secs(1))
        .saturating_duration_since(Deadline::now());
    let reason = timeout(remaining, connection.closed()).await?;
    assert!(format!("{reason:?}").contains("authentication deadline"));
    assert!(node.agent.config().peers.is_empty());
    assert_eq!(
        node.agent.peer_slots.available_permits(),
        MAX_PEER_CONNECTIONS
    );
    endpoint.close().await;
    node.agent.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_credit_and_connection_queue_are_inside_operation_deadlines() -> Result<()> {
    let gateway = Gateway::start(&[]).await?;
    let a = Node::start(&gateway, "A").await?;
    let remote = Endpoint::builder(presets::Minimal)
        .alpns(vec![protocol::ALPN.to_vec()])
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")?
        .transport_config(
            iroh::endpoint::QuicTransportConfig::builder()
                .max_concurrent_bidi_streams(0u8.into())
                .build(),
        )
        .bind()
        .await?;
    let acceptor = remote.clone();
    let accepting = tokio::spawn(async move { acceptor.accept().await.unwrap().await });
    a.agent
        .connection(&remote.addr(), Deadline::now() + OPERATION_TIMEOUT)
        .await?;
    let _remote_connection = accepting.await??;
    let peer = Peer {
        address: remote.addr(),
        profile: Profile::default(),
    };
    let ticket = Ticket {
        version: 1,
        peer: peer.clone(),
        token: new_token(),
        expires_at: now() + 60,
    };
    let result = timeout(
        Duration::from_secs(1),
        a.agent
            .pair_until(ticket.clone(), Deadline::now() + Duration::from_millis(80)),
    )
    .await?;
    assert!(result.unwrap_err().to_string().contains("timed out"));
    let result = timeout(
        Duration::from_secs(1),
        a.agent.open_http(
            &peer,
            "app.localhost",
            Scheme::Http,
            Deadline::now() + Duration::from_millis(80),
        ),
    )
    .await?;
    assert!(result.is_err());
    let queue = a.agent.connections.lock().await;
    let result = timeout(
        Duration::from_secs(1),
        a.agent
            .pair_until(ticket, Deadline::now() + Duration::from_millis(80)),
    )
    .await?;
    assert!(result.unwrap_err().to_string().contains("timed out"));
    drop(queue);
    a.agent.close().await;
    remote.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simultaneous_callers_share_a_failed_reconnect() -> Result<()> {
    let gateway = Gateway::start(&[]).await?;
    let a = Node::start(&gateway, "A").await?;
    let remote = Endpoint::builder(presets::Minimal)
        .alpns(vec![protocol::ALPN.to_vec()])
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")?
        .bind()
        .await?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed = attempts.clone();
    let acceptor = remote.clone();
    let task = tokio::spawn(async move {
        while let Some(incoming) = acceptor.accept().await {
            observed.fetch_add(1, Ordering::SeqCst);
            incoming.refuse();
        }
    });
    let address = remote.addr();
    let deadline = Deadline::now() + Duration::from_secs(2);
    let results = timeout(
        Duration::from_secs(3),
        futures_util::future::join_all((0..8).map(|_| a.agent.connection(&address, deadline))),
    )
    .await?;
    let errors: Vec<String> = results
        .into_iter()
        .map(|result| result.unwrap_err().to_string())
        .collect();
    assert!(errors.iter().all(|error| *error == errors[0]));
    assert!(
        attempts.load(Ordering::SeqCst) <= 2,
        "one hint attempt and one fallback, shared by all callers"
    );
    assert!(attempts.load(Ordering::SeqCst) > 0);
    a.agent.close().await;
    remote.close().await;
    task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_terminates_local_upgrades_before_releasing_the_identity_lock() -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let gateway = Gateway::start(&["app.localhost"]).await?;
    let a = Node::start(&gateway, "A").await?;
    let stream = tokio::net::TcpStream::connect(a.address).await?;
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    let driver = tokio::spawn(async move { connection.with_upgrades().await });
    let request = Request::builder()
        .uri("/upgrade")
        .header(HOST, "app.localhost")
        .header("connection", "upgrade")
        .header("upgrade", "test-echo")
        .body(http::full(""))?;
    let mut response = sender.send_request(request).await?;
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    let mut io = TokioIo::new(hyper::upgrade::on(&mut response).await?);
    io.write_all(b"hello").await?;
    let mut reply = [0; 5];
    timeout(Duration::from_secs(1), io.read_exact(&mut reply)).await??;
    assert_eq!(&reply, b"hello");
    timeout(Duration::from_secs(2), a.agent.close()).await?;
    let mut byte = [0; 1];
    assert_eq!(
        timeout(Duration::from_secs(1), io.read(&mut byte)).await??,
        0
    );
    let _new_owner = a.agent.store.acquire_agent()?;
    assert!(tokio::net::TcpStream::connect(a.address).await.is_err());
    driver.abort();
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn file_locks_do_not_block_http_and_shutdown_drains_cancelled_persistence() -> Result<()> {
    let gateway = Gateway::start(&["app.localhost"]).await?;
    let a = Node::start(&gateway, "A").await?;
    timeout(Duration::from_secs(2), async {
        while a.agent.config().last_address.is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let locked = File::options()
        .read(true)
        .write(true)
        .open(a.agent.store.directory().join("config.lock"))?;
    locked.lock()?;
    let response = timeout(
        Duration::from_secs(1),
        a.request(Method::GET, "app.localhost", "/"),
    )
    .await??;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await?;
    let mut write = Box::pin(a.agent.update_config(|config| {
        config.profile.device_name = "Committed".into();
        Ok(())
    }));
    assert!(futures_util::poll!(write.as_mut()).is_pending());
    // Drop the caller while its blocking write owns the update gate.
    drop(write);
    let closer = a.agent.clone();
    let closing = tokio::spawn(async move { closer.close().await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(a.agent.store.acquire_agent().is_err());
    locked.unlock()?;
    timeout(Duration::from_secs(2), closing).await??;
    assert_eq!(
        a.agent.store.read::<Config>()?.profile.device_name,
        "Committed"
    );
    let _new_owner = a.agent.store.acquire_agent()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trusted_gateway_preserves_https_locally_and_across_peers() -> Result<()> {
    let local = Gateway::start(&["local.localhost"]).await?;
    let remote = Gateway::start(&["remote.localhost"]).await?;
    let a = Node::start(&local, "A").await?;
    let b = Node::start(&remote, "B").await?;
    a.pair_with(&b).await?;
    for hostname in ["local.localhost", "remote.localhost"] {
        for (token, expected) in [
            ("spoofed", "http"),
            (a.agent.config().control_token.as_str(), "https"),
        ] {
            let request = Request::builder()
                .uri(format!("http://{}/", a.address))
                .header(HOST, hostname)
                .header(GATEWAY_HEADER, token)
                .header("x-forwarded-proto", "https")
                .header(AUTHORIZATION, "Bearer app-token")
                .body(http::full(""))?;
            let response = Client::builder(TokioExecutor::new())
                .build_http()
                .request(request)
                .await?;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await?.to_bytes();
            let value: serde_json::Value = serde_json::from_slice(&body)?;
            assert_eq!(value["proto"], expected);
            assert!(value["gateway_token"].is_null());
            assert_eq!(value["authorization"], "Bearer app-token");
        }
    }
    a.agent.close().await;
    b.agent.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diagnostics_are_authenticated_secret_free_and_read_only() -> Result<()> {
    let empty = Gateway::start(&[]).await?;
    let backend = Gateway::start(&["app.localhost"]).await?;
    let a = Node::start(&empty, "A").await?;
    let b = Node::start(&backend, "B").await?;
    a.pair_with(&b).await?;
    let response = a
        .request(Method::POST, "app.localhost", "/diagnostic-traffic")
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await?;
    let invitation = a.agent.invite(Duration::from_secs(600)).await?;

    for token in [None, Some("wrong-token")] {
        let response = a.diagnostics(token).await?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = response.into_body().collect().await?.to_bytes();
        assert!(!String::from_utf8_lossy(&body).contains("endpoint_id"));
    }
    // An application hostname must never enter the local control handler.
    let response = a
        .request(Method::GET, "app.localhost", "/_xe/diagnostics")
        .await?;
    let app_body: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await?.to_bytes())?;
    assert!(app_body.get("schema_version").is_none());

    let stranger = Endpoint::builder(presets::Minimal)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")?
        .bind()
        .await?;
    let pending = stranger
        .connect(a.agent.endpoint.addr(), protocol::ALPN)
        .await?;
    timeout(Duration::from_secs(2), async {
        while a.agent.pending_slots.available_permits() != MAX_PENDING_PAIRINGS - 1
            || a.agent
                .live_connections
                .lock()
                .await
                .iter()
                .all(|live| live.connection.remote_id() != stranger.id())
        {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    let lock = File::options()
        .read(true)
        .write(true)
        .open(a.agent.store.directory().join("config.lock"))?;
    lock.lock()?;
    let path = a.agent.store.directory().join("config.json");
    let before = std::fs::metadata(&path)?.modified()?;
    let heads = backend.heads.load(Ordering::SeqCst);
    let token = a.agent.config().control_token.clone();
    let response = timeout(Duration::from_secs(2), a.diagnostics(Some(&token))).await??;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let bytes = response.into_body().collect().await?.to_bytes();
    let encoded = String::from_utf8(bytes.to_vec())?;
    for secret in [
        &token,
        &b.agent.config().control_token,
        &invitation.token,
        &invitation.encode()?,
    ] {
        assert!(!encoded.contains(secret), "diagnostics leaked a secret");
    }
    let value: serde_json::Value = serde_json::from_str(&encoded)?;
    assert_eq!(value["schema_version"], 1);
    assert_eq!(
        value["agent"]["endpoint_id"],
        a.agent.endpoint.id().to_string()
    );
    assert_eq!(value["agent"]["profile"]["device_name"], "A");
    assert_eq!(
        value["discovery"]["routes"]["app.localhost"],
        b.agent.endpoint.id().to_string()
    );
    assert_eq!(value["resources"]["trusted_connections"]["used"], 1);
    assert_eq!(value["resources"]["pending_pairings"]["used"], 1);
    assert_eq!(
        value["resources"]["pending_pairings"]["limit"],
        MAX_PENDING_PAIRINGS
    );
    assert_eq!(
        value["resources"]["request_handlers"]["used"], 1,
        "the diagnostic request counts as a handler"
    );
    assert_eq!(value["pairing"]["invitations"]["unredeemed"], 1);
    assert_eq!(value["dial_cache"][0]["state"], "connected");
    assert!(
        value["iroh"]["metrics"]["socket"]["num_conns_opened"]["value"]
            .as_u64()
            .unwrap()
            >= 2
    );
    assert!(value["iroh"]["metric_descriptions"]["socket"]["paths_direct"].is_string());
    assert!(value["iroh"]["metric_descriptions"]["address_lookup"]["service_errors"].is_string());
    assert_eq!(value["iroh"]["home_relays"], serde_json::json!([]));
    let connections = value["connections"].as_array().unwrap();
    let trusted = connections
        .iter()
        .find(|entry| entry["peer_id"] == b.agent.endpoint.id().to_string())
        .unwrap();
    assert_eq!(trusted["admission"], "trusted");
    assert_eq!(trusted["direction"], "outbound");
    assert!(trusted["authentication_remaining_ms"].is_null());
    assert!(trusted["stats"]["tx"]["bytes"].as_u64().unwrap() > 0);
    assert!(trusted["stats"]["frames_tx"]["stream"].as_u64().unwrap() > 0);
    let selected = trusted["paths"]
        .as_array()
        .unwrap()
        .iter()
        .find(|path| path["selected"] == true)
        .unwrap();
    assert_eq!(selected["kind"], "direct_ip");
    assert!(selected["stats"]["rtt_ms"].as_f64().unwrap() > 0.0);
    assert!(selected["stats"]["mtu_bytes"].as_u64().unwrap() > 0);
    let unknown = connections
        .iter()
        .find(|entry| entry["peer_id"] == stranger.id().to_string())
        .unwrap();
    assert_eq!(unknown["admission"], "pending_pairing");
    assert!(unknown["authentication_remaining_ms"].as_u64().unwrap() <= 10_000);
    assert_eq!(std::fs::metadata(&path)?.modified()?, before);
    assert_eq!(
        backend.heads.load(Ordering::SeqCst),
        heads,
        "diagnostics must not probe apps"
    );
    lock.unlock()?;
    pending.close(0u8.into(), b"test complete");
    stranger.close().await;
    a.agent.close().await;
    b.agent.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diagnostics_report_unknown_remote_state_and_completed_shutdown_without_dialing()
-> Result<()> {
    let gateway = Gateway::start(&[]).await?;
    let a = Node::start(&gateway, "A").await?;
    let b = Node::start(&gateway, "B").await?;
    let peer = b.agent.peer()?;
    let id = peer.address.id.to_string();
    a.agent
        .update_config(move |config| {
            config
                .routes
                .insert("saved.localhost".into(), peer.address.id.to_string());
            config.peers.insert(peer.address.id.to_string(), peer);
            Ok(())
        })
        .await?;
    let value = a.agent.diagnostics().await;
    assert_eq!(value["peers"][0]["peer_id"], id);
    assert_eq!(value["peers"][0]["iroh_remote_info"]["status"], "not_known");
    assert_eq!(
        value["peers"][0]["active_connection_ids"],
        serde_json::json!([])
    );
    assert_eq!(value["connections"], serde_json::json!([]));
    assert_eq!(value["dial_cache"], serde_json::json!([]));
    assert_eq!(value["discovery"]["routes"]["saved.localhost"], id);
    assert_eq!(
        value["iroh"]["metrics"]["socket"]["num_conns_opened"]["value"],
        0
    );
    assert_eq!(gateway.heads.load(Ordering::SeqCst), 0);
    a.agent.close().await;
    let value = timeout(Duration::from_secs(2), a.agent.diagnostics()).await?;
    assert_eq!(value["agent"]["closing"], true);
    assert_eq!(value["agent"]["identity_lock_held"], false);
    assert_eq!(value["agent"]["persistence"]["closed"], true);
    assert_eq!(value["iroh"]["closed"], true);
    assert_eq!(value["resources"]["tracked_tasks"], 0);
    assert_eq!(value["resources"]["trusted_connections"]["used"], 0);
    b.agent.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_limits_and_upstream_idle_pool_are_bounded() -> Result<()> {
    let gateway = Gateway::start(&["app.localhost"]).await?;
    let a = Node::start(&gateway, "A").await?;
    let occupied = a
        .agent
        .requests
        .clone()
        .acquire_many_owned(MAX_REQUESTS as u32)
        .await?;
    assert_eq!(
        a.request(Method::GET, "app.localhost", "/").await?.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    drop(occupied);
    let occupied = a
        .agent
        .http_slots
        .clone()
        .acquire_many_owned(MAX_HTTP_CONNECTIONS as u32)
        .await?;
    assert!(a.request(Method::GET, "app.localhost", "/").await.is_err());
    drop(occupied);
    let replies = futures_util::future::join_all((0..16).map(|_| async {
        a.request(Method::GET, "app.localhost", "/hold")
            .await?
            .into_body()
            .collect()
            .await?;
        Ok::<_, anyhow::Error>(())
    }))
    .await;
    for reply in replies {
        reply?;
    }
    assert!(gateway.peak_connections.load(Ordering::SeqCst) > 4);
    timeout(Duration::from_secs(2), async {
        while gateway.active_connections.load(Ordering::SeqCst) > 4 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let b = Node::start(&gateway, "B").await?;
    let occupied = b
        .agent
        .peer_slots
        .clone()
        .acquire_many_owned(MAX_PEER_CONNECTIONS as u32)
        .await?;
    let ticket = b.agent.invite(Duration::from_secs(60)).await?;
    assert!(
        a.agent
            .pair_until(ticket, Deadline::now() + Duration::from_millis(150))
            .await
            .is_err()
    );
    assert!(b.agent.config().peers.is_empty());
    assert_eq!(
        b.request(Method::GET, "app.localhost", "/").await?.status(),
        StatusCode::OK
    );
    drop(occupied);
    b.agent.close().await;
    a.agent.close().await;
    timeout(Duration::from_secs(1), async {
        while gateway.active_connections.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_owns_a_listener_even_when_the_parent_stops_polling_serve() -> Result<()> {
    let gateway = Gateway::start(&["app.localhost"]).await?;
    let a = Node::start(&gateway, "A").await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let mut serving = Box::pin(a.agent.clone().serve_http(listener));
    assert!(futures_util::poll!(serving.as_mut()).is_pending());
    let request = Request::builder()
        .uri(format!("http://{address}/"))
        .header(HOST, "app.localhost")
        .body(http::full(""))?;
    let response = Client::builder(TokioExecutor::new())
        .build_http()
        .request(request)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    response.into_body().collect().await?;
    // Do not poll `serving` while close runs, as a parent's select loop may do.
    timeout(Duration::from_secs(2), a.agent.close()).await?;
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
    serving.await?;
    let _new_owner = a.agent.store.acquire_agent()?;
    Ok(())
}
