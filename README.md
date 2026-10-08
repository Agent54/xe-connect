# xe-connect

A noninteractive standalone HTTP bridge between paired iroh computers. Apps use
ordinary `.localhost` HTTP addresses; iroh handles encrypted direct connections
and relay fallback between computers.

This first version has a Rust library and a thin CLI. It does not install DNS,
manage containers, or require stdin. Workerd or another local gateway remains
responsible for dispatching hostnames to Docker services and OS daemons.

## Build

Requires Rust 1.91 or newer.

```sh
cargo build --release --locked
```

## Run

On each computer, start the agent with its local gateway's address:

```sh
xe-connect --state-dir ./computer-state \
  --device-name "Jan's Mac" --user-name "Jan" \
  --listen 127.0.0.1:5198 --upstream http://127.0.0.1:5196
```

No command means `serve`. Arguments are persisted, so subsequent starts only
need `xe-connect --state-dir ./computer-state`. The state directory defaults to
the current working directory. The default listener is `127.0.0.1:5198`; the
default upstream is `http://127.0.0.1:5196`.

The agent prints one JSON readiness record to stdout. Diagnostics go to stderr.
SIGINT or SIGTERM shuts it down. It refuses a non-loopback HTTP listener and a
second agent using the same state directory.

## Pair

Leave the first agent running. In another terminal on that computer:

```sh
xe-connect --state-dir ./computer-state invite
```

Copy the printed `xe1:...` ticket to the other computer:

```sh
xe-connect --state-dir ./other-state \
  --device-name "Cloud Computer" --user-name "Jan" pair 'xe1:...'
```

Pairing saves both authenticated endpoint identities, display names, and address
hints. The inviting computer must be running. The joining computer can already
be running or start its HTTP agent afterward. Commands sent to a running agent
use a local authenticated control request; there are no prompts.

Tickets expire after ten minutes. `invite --expires-in 3600` changes that
lifetime. Each ticket can authorize one identity; retrying with that same
identity is allowed until expiry so a lost reply does not break pairing.
Retries require the peer to remain paired. Unpairing revokes invitations already
redeemed by that identity, so an old ticket cannot restore its access.
Device and user names are display metadata, not verified account identities.
**Pairing means full trust in that computer.** Paired computers have equal access
to the HTTP services reachable through each other's configured gateway.
First-positive discovery sends the actual request, including app cookies and
authorization, to whichever paired computer first claims that hostname. Device
and user display names provide no access control or account verification.

```sh
xe-connect --state-dir ./computer-state id
xe-connect --state-dir ./computer-state peers
xe-connect --state-dir ./computer-state unpair ENDPOINT_ID
```

Unpair removes cached routes, revokes that computer's redeemed invitations, and
closes its active connections. Unpairing applies to the local computer's trust
list; accepting it again requires a fresh invitation.

## Route HTTP

```sh
curl http://notes.localhost:5198/documents
curl http://notes.web.localhost:5198/documents
```

If the client does not resolve `.localhost` subdomains to loopback, use:

```sh
curl --resolve notes.localhost:5198:127.0.0.1 \
  http://notes.localhost:5198/documents
```

Only hostnames with one or two labels before `.localhost` are accepted. The
listener port is the port of this agent, not a remote container's port. Workerd
can interpret a second hostname label as a service port selector as usual.

Resolution is demand-driven:

1. Send an unauthenticated `HEAD /` to the local gateway with the requested
   hostname. Any response except 404 counts as an available local service.
2. If the local probe fails, try the saved remote provider for this hostname.
3. If no provider is cached, or the cached provider is unavailable, probe all
   paired computers concurrently. The first positive answer wins.
4. Persist the winning provider until it becomes unavailable. There is no route
   TTL or periodic service polling. Concurrent misses share discovery, and a
   failed discovery has a one-second backoff.

The local HEAD timeout defaults to 500 ms; set `--probe-timeout-ms 250` to change
it. Pairing and remote route establishment have a ten-second overall deadline
covering queueing, connection, stream capacity, and protocol exchange. An address
hint gets up to three seconds, leaving time for identity lookup and fallback.
Requests share both successful and failed reconnect attempts. Cached providers
get at most four seconds within the same route deadline, leaving time to query
alternatives. Application response headers have a separate 30-second deadline;
bodies and upgraded connections remain streaming.

The agent reserves 32 connection slots for trusted peers. Incoming handshakes
have a separate eight-slot quota and five-second timeout; after the handshake,
unknown identities use a separate four-slot pending-pairing quota. Pending
connections must authenticate within ten seconds of admission, including the
handshake. Silence, QUIC keepalives, partial frames, and retries do not extend
that deadline. Successful pairing moves the connection into trusted capacity
and releases its pending slot. Pending connections process one stream at a time.

Local HTTP connections are limited to 64, and active incoming peer streams/local
request handlers to 64. Upgrades retain their connection or stream slot.
Pairing attempts allow eight per identity and 32
globally in a rolling minute. Invalid invitations are rejected from memory
without reading or writing configuration files. The upstream HTTP pool retains
at most four idle connections per origin and expires them after 30 seconds.

HEAD probes do not include the app request's cookies, authorization, body, or
path. Discovery never races the actual app request. Writes are sent to only one
provider and are not replayed after a forwarding failure. An application's
ordinary 404 response does not invalidate a cached provider.

The heuristic is intentionally simple: a gateway-wide 503 also counts as an
available service, and an app whose `HEAD /` returns 404 is undiscoverable even
if another path works. A HEAD request may trigger the gateway's container-start
behavior on multiple peers during discovery.

HTTP forwarding preserves method, path, query, app authorization, cookies, and
response status, and streams bodies and HTTP upgrades. The upstream receives
the requested hostname with its own listener port in `Host`, plus
`X-Forwarded-Host` for the caller's original address and `X-Forwarded-Proto` for
its original scheme (HTTP by default; see the trusted gateway interface below).
Incoming peer calls also carry `X-Xe-Source-Peer`, set from iroh authentication.
Caller-supplied routing headers and app-supplied bridge error headers are removed.
Redirects are returned rather than followed. Apps generating absolute links
should honor their configured public origin or the forwarded headers.

This version exposes HTTP on loopback. HTTPS web apps can use it behind
Workerd's existing local HTTPS gateway. An HTTPS upstream is accepted with
normal certificate validation against its configured upstream address; there
is no insecure certificate bypass.

## State and embedding

The chosen state folder contains `identity.key` (a raw 32-byte iroh secret key),
`config.json` (settings, peers, route cache, invitations, and local control
token), and lock files. New state files are owner-only on Unix. Configuration
updates use a lock and atomic replacement in that same folder. A malformed
identity or config is reported rather than overwritten. An existing valid
`identity.key` is reused.

The library exposes `StateStore`, `Config`, `Agent::start`, `Agent::pair`,
`Agent::invite`, `Agent::remove_peer`, `Agent::serve_http`, and `Agent::close`.
Pairing, invitation creation, and removal are async. `Agent::config()` returns an
in-memory snapshot; use async `Agent::update_config()` for runtime changes. All
runtime persistence runs on Tokio's blocking pool behind a serialized update
gate. Unchanged updates do not replace or sync the JSON file. Running CLI commands
apply settings and create invitations through authenticated control requests, so
the snapshot and disk stay consistent. Direct file/`StateStore` edits are for a
stopped agent; a running agent does not poll for external edits.

Always await `Agent::close()` before reusing its state folder. It cancels and
joins owned listeners, HTTP/iroh drivers, pool maintenance, and upgrade bridges,
then drains outstanding persistence before releasing the identity lock. The
agent owns a started listener even when its parent stops polling `serve_http()`.

The CLI only applies arguments and drives that lifecycle. A parent application
can use the same core and supply its state location and names. A Unix listener
adapter and Swift bindings are subsequent integration work; no Workerd or
launcher files are changed by this crate.

**The upstream must serve local services only.** It must return 404 for unknown
hostnames and must never route an unknown hostname back into xe-connect. Use a
separate local-service path in Workerd when Workerd also fronts remote services;
otherwise HEAD probing and forwarding create a loop.

**Preserve HTTPS through the trusted gateway interface.** A local gateway that
terminates HTTPS sends the original public `Host`, `X-Forwarded-Proto: https`, and
`X-Xe-Gateway-Token` containing the agent's local `control_token`. The embedding
parent can get that token from `Agent::config()`; keep it in the gateway, never in
browser code. The gateway must replace client-supplied scheme/token headers with
values derived from its own request context. Only requests with the correct
token may set the original scheme, and only one `http` or `https` value is
accepted. Other callers' forwarded-scheme headers are ignored. The scheme crosses
the authenticated iroh HTTP-stream preface and reaches the provider's local
upstream as `X-Forwarded-Proto`; the private gateway token is stripped before any
forwarding. App authorization remains separate and is preserved.

The peer protocol uses ALPN `xe-connect/1` and three bounded, length-prefixed
JSON operations: pairing, hostname probing, and opening an HTTP stream. Actual
requests use HTTP/1.1 over a bidirectional iroh stream. Bodies are not encoded
into JSON. Browser TLS and container lifecycle stay with the local gateway.
The HTTP-stream operation carries an optional original `scheme`, defaulting to
HTTP for older requests.

## Diagnostics

`GET /_xe/diagnostics` returns a JSON snapshot with `schema_version: 1` and
`Cache-Control: no-store`. Use a loopback/`localhost` Host and the existing local
control token, not app authorization. For example:

```sh
curl --fail -H "Authorization: Bearer $XE_CONTROL_TOKEN" \
  http://127.0.0.1:5198/_xe/diagnostics
```

The embedding parent can call `Agent::diagnostics().await` directly. A Workerd
socket wrapper can forward the HTTP request to the agent's local control
interface, replace Host with `localhost`, and inject the control token privately.
With a service binding named `XE_CONNECT`, its internal fetch would be:

```ts
await env.XE_CONNECT.fetch("http://localhost/_xe/diagnostics", {
  headers: { Authorization: `Bearer ${env.XE_CONTROL_TOKEN}` },
});
```

The wrapper should apply its own user authentication before exposing diagnostics
to a browser. Public endpoint IDs, IP addresses, relay URLs, hostnames, and display
names are included. Secret keys, control tokens, invitation tokens/tickets, app
cookies, authorization, and request bodies are excluded. There is no new peer
protocol operation: this is a local control endpoint.

Diagnostics do not dial peers, HEAD-probe services, start containers, rerun network
checks, acquire configuration file locks, or write state. They read iroh's existing
network report and metric counters. Remote-address actor queries share a one-second
budget with at most eight queries in flight. `not_known` and `timed_out` are explicit
results; neither proves a computer is offline. The snapshot is best effort rather
than an atomic view of every subsystem.

| Section | Information |
| --- | --- |
| `schema_version`, `sampled_at_unix_ms` | Response format version and collection start time in Unix milliseconds. |
| `agent` | Crate version, uptime, shutdown state, identity-lock ownership, public endpoint ID, device/user profile, state directory, last saved endpoint address, configured listener, local upstream, and probe timeout. |
| `agent.persistence` | Whether the serialized persistence gate is busy, and whether runtime persistence has closed. Busy includes a commit or gate holder; it is not a disk-error indicator. |
| `iroh` | Pinned iroh version, application ALPN, endpoint shutdown state, currently advertised address and locally bound UDP sockets. |
| `iroh.home_relays` | Each home relay URL, connected state, most recent connection error, and relay authentication-denial reason. |
| `iroh.net_report` | Last report's IPv4/IPv6 UDP reachability, destination-dependent NAT mapping, discovered public addresses/ports, preferred relay, relay probe latencies and probe types, and captive-portal detection. Null means no report is available. |
| `iroh.metrics` | All three native metric groups: socket, network reports, and address lookup. Native counters use `{ "value": N }`; lookup-service families contain labeled counter values. |
| `iroh.metric_descriptions` | iroh's own description for every scalar counter and labeled family, keyed by group and metric name. This includes metrics currently unused by this iroh version. |
| `peers[]` | Paired and observed identities, paired state, known display profile, saved address hints, active connection IDs, and iroh's known addresses with active/inactive usage and query status. |
| `connections[]` | Live and retained closed connections: process-local connection ID, peer ID, inbound/outbound direction, actual trusted/pending admission, connection age, remaining authentication deadline, ALPN, close state/reason, datagram capability/buffer space, aggregate transport statistics, and individual paths. |
| `connections[].paths[]` | Path ID, direct-IP/relay/custom kind, selected state, local/remote addresses, RTT, congestion window, traffic, frame counts, loss, congestion events, spurious congestion, MTU/probe loss and black-hole detections. Controller metrics add slow-start threshold, pacing rate, and send quantum when available. |
| `dial_cache[]` | Shared dial result state, peer ID, age since the attempt started, associated connection ID, and failure/close reason. `pending_or_unobserved` also covers a completed result that no waiter has polled yet. Reading it never starts or polls the attempt. |
| `discovery` | Saved hostname-to-peer routes, hostnames with an active discovery gate, and remaining one-second negative-discovery backoffs. |
| `resources` | Limit, used and available slots for trusted connections, handshakes, pending pairing, HTTP connections and request handlers; tracked task count; upstream pool limits; operation deadlines. Hyper does not expose its current idle-pool count, so that field is null. |
| `pairing` | Rolling-window duration, per-identity/global attempt limits, attempts by identity and in total, remaining global allowance, and aggregate unredeemed/redeemed/expired invitation counts. |

Transport statistics include TX/RX bytes and datagrams, lost packets/bytes, and
every public TX/RX QUIC frame counter: ACK/path-ACK/frequency, crypto/handshake,
close, data/stream capacity and blockage, datagrams, stream data, resets/stops,
connection IDs, tokens, path challenge/response/status/abandonment, ping, observed
addresses, and address/path management. These count protocol frames, not HTTP
requests, open streams, or app payload bytes.

The complete native metric groups currently include:

- Socket traffic: `send_ipv4`, `send_ipv6`, `send_relay`, `recv_data_ipv4`,
  `recv_data_ipv6`, `recv_data_relay`, `recv_data_custom`, `recv_datagrams`, and
  `recv_gro_datagrams` (coalesced receive batches).
- Relay lifecycle: `relay_home_change`, `relay_conns_success`, `relay_conns_failed`,
  `relay_conns_closed`, and `relay_conns_ratelimited`.
- NAT/path activity: `holepunch_attempts`, `paths_direct`, `paths_relay`,
  `paths_custom`, `num_conns_direct`, `transport_ip_paths_added/removed`,
  `transport_relay_paths_added/removed`, and `transport_custom_paths_added/removed`.
- Connection lifecycle: `num_conns_opened` and `num_conns_closed`.
- Internal actor activity: `actor_tick_main`, `actor_tick_msg`, `actor_tick_re_stun`,
  `actor_tick_portmap_changed`, `actor_link_change`, and `actor_tick_other`.
  `update_direct_addrs` and `actor_tick_direct_addr_heartbeat` are included but
  are currently never incremented by iroh.
- Network reports: `reports`, `reports_full`, `portmap_attempts`, and
  `portmap_external_address_updated`.
- Address lookup: `lookups`, `lookups_failed`, and labeled `service_results` /
  `service_errors` by resolver provenance such as DNS or pkarr. This discovers
  endpoint addresses, not `.localhost` service providers.

Use a path's **selected** flag to see where app traffic currently travels. A relay
can remain connected while a direct path carries traffic, and historical direct/
relay counters do not tell you which path is selected now. Relay transport still
carries encrypted QUIC traffic. RTT measures transport round-trip time, not app
response time. The congestion window limits bytes in flight; it is not a bandwidth
measurement. MTU is the supported packet size, and black-hole detection concerns
oversized packets disappearing. Loss is normally recovered by QUIC and does not
itself mean an HTTP request failed. Connection totals include earlier paths;
the current path list does not.

Counters reset with a new endpoint; compare successive snapshots for rates.
Connection/path IDs are process-local correlation IDs rather than device IDs.
Configured routes and saved addresses may be stale and are not health checks.
`paired` records authorization, not reachability. Resource usage measures occupied
permits, which may include queueing/dialing or upgraded streams; the diagnostics
request itself occupies a request and HTTP slot. Closed connection entries are
kept only until the next connection registration prunes them, not as a durable
history. Pairing attempt counts include attempts admitted by the limiter; attempts
rejected because the window is full do not increase these counts. Optional/null
values mean not observed or not exposed, not zero.

The network-report API is marked unstable by iroh. This crate pins iroh to 1.3.0
and maps that report into the diagnostics schema, so upgrading iroh requires
reviewing the mapping. The feature exposes existing reports; this endpoint does
not trigger extra network probing.

## Verify

Try the pairing workflow locally with Deno 2:

```sh
deno task demo
```

This one script builds the binary and starts six separate processes. Each
computer has its own persistent identity folder, agent, dummy gateway, and dummy
service. The gateways proxy only their own service's hostname and return 404 for
the other computer's service.

| Computer | Agent (`listen`) | Gateway (`upstream`) | Dummy service | Hostname |
| --- | --- | --- | --- | --- |
| A | 5210 | 5211 | 5212 | `alpha.localhost` |
| B | 5220 | 5221 | 5222 | `beta.web.localhost` |

All listeners use `127.0.0.1`. Pairing stays manual: the script prints the exact
`invite` and `pair` commands to run in another terminal. Before pairing, each
agent serves only its own hostname. After pairing, open
`http://beta.web.localhost:5210/` to reach B through A, or
`http://alpha.localhost:5220/` to reach A through B. The response identifies the
computer, gateway, service port, and authenticated source peer. The script also
prints `curl --resolve` commands for clients without `.localhost` resolution.

Ctrl+C stops every child process. State remains under
`target/manual-demo/computer-a` and `computer-b`. Subsequent runs keep pairing;
use `--state-dir target/another-demo` for a fresh setup. Use `--base-port 5310`
to move all six ports, or `--no-build` to reuse the debug binary.

The Deno integration test imports that same setup, uses fresh state and unused
ports, then checks local-only access before pairing, both saved peers, forwarding
in both directions, POST bodies and query strings, cached routes, and unpairing:

```sh
deno task test:demo
```

```sh
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --check
```

The core network tests use local gateways and direct loopback iroh endpoints,
without depending on public relays or DNS. They cover identity persistence,
pairing, peer authorization, request forwarding, concurrent discovery, route
reuse across restarts, provider replacement, application errors, avoiding write
replays, and HTTP upgrades. A CLI integration test exercises invitation creation,
pairing before startup and while running, persistent names, forwarding, and
unpairing through the actual binary using the default iroh configuration.

Real cross-network NAT traversal and relay fallback still need a two-computer
smoke test before bundling this with the launcher.
