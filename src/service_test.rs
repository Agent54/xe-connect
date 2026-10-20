//! Explicit, authenticated service checks. Passive diagnostics never call this.

use super::*;

impl Agent {
    /// Run an unauthenticated GET / through normal local-first service routing.
    /// This can discover/cache a provider or start a service through the gateway.
    /// No redirects are followed; at most 4 KiB of response data is returned as text.
    pub async fn test_service(self: &Arc<Self>, hostname: &str) -> Result<serde_json::Value> {
        let hostname = http::hostname(hostname)?;
        let lease = Arc::new(self.requests.clone().try_acquire_owned()?);
        let started = Instant::now();
        self.operation(Deadline::now() + Duration::from_secs(20), async {
            let request = Request::builder()
                .uri("/")
                .header(HOST, &hostname)
                .body(http::full(""))?;
            let (provider, mut response) = if self.probe(&hostname).await {
                (
                    None,
                    self.forward_local(request, &hostname, None, Scheme::Http, lease)
                        .await?,
                )
            } else if let Some((id, stream)) = self.remote_stream(&hostname, Scheme::Http).await? {
                let result = self.forward_remote(request, stream, lease).await;
                if !result
                    .as_ref()
                    .is_ok_and(|response| !response.headers().contains_key(ERROR_HEADER))
                {
                    self.invalidate(&hostname, &id).await?;
                }
                (Some(id), result?)
            } else {
                return Ok(serde_json::json!({
                    "hostname": hostname, "provider": "none", "peer_id": null,
                    "status": 404, "elapsed_ms": started.elapsed().as_millis(),
                    "body_preview": "No paired computer offers this hostname.", "truncated": false,
                }));
            };
            let mut preview = Vec::new();
            let mut truncated = false;
            while let Some(frame) = response.body_mut().frame().await {
                if let Ok(data) = frame?.into_data() {
                    let remaining = 4096 - preview.len();
                    preview.extend_from_slice(&data[..data.len().min(remaining)]);
                    if data.len() > remaining {
                        truncated = true;
                        break;
                    }
                }
            }
            Ok(serde_json::json!({
                "hostname": hostname, "provider": if provider.is_some() { "peer" } else { "local" },
                "peer_id": provider, "status": response.status().as_u16(),
                "elapsed_ms": started.elapsed().as_millis(),
                "body_preview": String::from_utf8_lossy(&preview), "truncated": truncated,
            }))
        })
        .await
    }
}
