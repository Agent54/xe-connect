<script lang="ts">
  import { onMount } from "svelte";
  import { api, encodeTicket, shortId, yesNo } from "./api.ts";
  import type { Snapshot, TestResult } from "./api.ts";

  let token = $state("");
  let enteredToken = $state("");
  let data = $state<Snapshot | null>(null);
  let error = $state("");
  let notice = $state("");
  let refreshing = $state(false);
  let busy = $state(false);
  let invitation = $state("");
  let ticket = $state("");
  let hostname = $state("");
  let result = $state<TestResult | null>(null);
  let tab = $state<"devices" | "network" | "raw">("devices");
  const views = ["devices", "network", "raw"] as const;
  const peers = $derived(data?.peers.filter((peer) => peer.paired) ?? []);
  const connections = $derived(
    data?.connections.filter((connection) => !connection.closed) ?? [],
  );
  const routes = $derived(Object.entries(data?.discovery.routes ?? {}));

  async function refresh() {
    if (!token || refreshing) return;
    refreshing = true;
    const current = token;
    try {
      const snapshot = await api<Snapshot>(current, "diagnostics");
      if (token === current) {
        data = snapshot;
        error = "";
      }
    } catch (cause) {
      if (token === current) {
        error = String(cause instanceof Error ? cause.message : cause);
      }
    } finally {
      refreshing = false;
    }
  }

  async function action(work: () => Promise<void>) {
    busy = true;
    error = "";
    notice = "";
    try {
      await work();
      await refresh();
    } catch (cause) {
      error = String(cause instanceof Error ? cause.message : cause);
    } finally {
      busy = false;
    }
  }

  function paths(id: string) {
    return connections.filter((connection) => connection.peer_id === id)
      .flatMap((connection) =>
        connection.paths.filter((path) => path.selected)
      );
  }

  function preview(text: string) {
    try {
      return JSON.stringify(JSON.parse(text), null, 2);
    } catch {
      return text;
    }
  }

  async function copy(value: string) {
    try {
      await navigator.clipboard.writeText(value);
      notice = "Copied to clipboard.";
    } catch {
      notice = "Clipboard unavailable. Select the text and copy it manually.";
    }
  }

  onMount(() => {
    token = new URLSearchParams(location.hash.slice(1)).get("token") ?? "";
    // Keep the credential only in memory, out of request URLs and browser history.
    if (location.hash) {
      history.replaceState(null, "", location.pathname + location.search);
    }
    void refresh();
    const timer = setInterval(() => {
      if (document.visibilityState === "visible" && !busy) void refresh();
    }, 5000);
    return () => clearInterval(timer);
  });
</script>

<svelte:head><title>
    {data ? `${data.agent.profile.device_name} · xe-connect` : "xe-connect"}
  </title></svelte:head>

<div class="shell">
  <header class="toolbar">
    <span class="program">xe-connect <span class="version">{
        data?.agent.version ?? ""
      }</span></span>
    {#if data}<div class="session-controls">
        <span>Agent keeps running</span><button
          class="quiet"
          title="Remove this tab's control token. The agent and peer connections keep running."
          disabled={busy}
          onclick={() => {
            token = "";
            data = null;
            invitation = "";
            ticket = "";
            result = null;
            error = "";
            notice = "UI token cleared. The agent and peer connections keep running.";
          }}
        >
          Forget token
        </button>
      </div>{/if}
  </header>

  {#if error}<div class="message error" role="alert">{error}</div>{/if}
  {#if notice}<div class="message" role="status">{notice}</div>{/if}

  {#if !data}
    <main class="login">
      <h1>Connect to agent</h1>
      <p>Open the private link printed by the running agent.</p>
      <div class="panel login-panel">
        <h2>Get a UI link</h2>
        <p>
          Run this command using your agent’s state folder, then open the
          printed link.
        </p>
        <code>xe-connect --state-dir ./computer-state ui</code>
        <details>
          <summary>Enter a control token manually</summary>
          <form
            onsubmit={(event) => {
              event.preventDefault();
              notice = "";
              token = enteredToken.trim();
              enteredToken = "";
              void refresh();
            }}
          >
            <label for="control-token">Local control token</label>
            <input
              id="control-token"
              type="password"
              bind:value={enteredToken}
              autocomplete="off"
              required
            />
            <button class="primary" disabled={refreshing}>
              {refreshing ? "Connecting…" : "Connect to agent"}
            </button>
          </form>
        </details>
      </div>
    </main>
  {:else}
    <main>
      <section class="intro">
        <div>
          <h1>{data.agent.profile.device_name}</h1>
          <p>
            {data.agent.profile.user_name ?? "No user name set"} <span
              class="dot-divider"
            >·</span> <button
              class="id quiet"
              aria-label="Copy endpoint ID"
              title={data.agent.endpoint_id}
              onclick={() => copy(data!.agent.endpoint_id)}
            >
              {shortId(data.agent.endpoint_id)}
            </button>
          </p>
        </div>
        <div class="live">
          <span class:warning={!!error} class="status-dot"></span>{
            error ? "Update failed" : "Agent connected"
          }<small>Snapshot {
              new Date(data.sampled_at_unix_ms).toLocaleTimeString()
            } · every 5 s</small>
        </div>
      </section>

      <section class="summary" aria-label="Connection summary">
        <div>
          <span>Paired computers</span><strong>{peers.length}</strong>
        </div>
        <div>
          <span>Open connections</span><strong>{connections.length}</strong>
        </div>
        <div>
          <span>Cached services</span><strong>{routes.length}</strong>
        </div>
        <div>
          <span>Agent uptime</span><strong>{
              Math.floor(data.agent.uptime_ms / 60000)
            }<small> min</small></strong>
        </div>
      </section>

      <nav class="tabs" aria-label="Diagnostics views">
        {#each views as view}
          <button
            class:active={tab === view}
            aria-current={tab === view ? "page" : undefined}
            onclick={() => tab = view}
          >
            {
              view === "devices"
              ? "Computers & services"
              : view === "network"
              ? "Network"
              : "Raw diagnostics"
            }
          </button>
        {/each}
        <button class="refresh quiet" disabled={refreshing} onclick={refresh}>
          {refreshing ? "Updating…" : "Refresh"}
        </button>
      </nav>

      {#if tab === "devices"}
        <div class="workspace">
          <div class="main-column">
            <section class="panel">
              <div class="section-heading">
                <h2>Paired computers</h2>
              </div>
              {#if !peers.length}<div class="empty">
                  <h3>
                    No computers paired yet
                  </h3>
                  <p>
                    Create an invitation, then paste it into the UI on your
                    other computer.
                  </p>
                </div>{/if}
              {#each peers as peer (peer.peer_id)}
                {@const selected = paths(peer.peer_id)}
                <article class="peer">
                  <div class="peer-top">
                    <div class="peer-name">
                      <h3>
                        {peer.profile?.device_name ?? shortId(peer.peer_id)}
                      </h3>
                      <p>{peer.profile?.user_name ?? "No user name"}</p>
                    </div><span
                      class:online={peer.active_connection_ids.length > 0}
                      class="badge"
                    >{
                      peer.active_connection_ids.length ? "Connected" : "No open connection"
                    }</span>
                  </div>
                  <div class="peer-transport">
                    <span>Transport:</span><span>{
                      selected.length
                      ? [
                        ...new Set(selected.map((path) =>
                          path.kind === "direct_ip"
                            ? "Direct"
                            : path.kind === "relay"
                            ? "Relay"
                            : "Custom"
                        )),
                      ].join(" + ")
                      : "No active path"
                    }</span>
                  </div>
                  <div class="peer-bottom">
                    <code title={peer.peer_id}>{
                      shortId(peer.peer_id)
                    }</code><span>{
                      selected.length
                      ? `${
                        Math.min(...selected.map((path) => path.stats.rtt_ms)).toFixed(1)
                      } ms RTT`
                      : "Reachability not checked"
                    }</span><button
                      class="quiet danger"
                      disabled={busy}
                      onclick={() => {
                        if (
                          confirm(
                            `Unpair ${
                              peer.profile?.device_name ?? "this computer"
                            }? Its active connections and cached routes will be removed.`,
                          )
                        ) {
                          void action(async () => {
                            await api(token, "unpair", peer.peer_id);
                          });
                        }
                      }}
                    >
                      Unpair
                    </button>
                  </div>
                </article>
              {/each}
              <p class="footnote">
                A saved pairing grants access. A connection opens when a service
                is requested.
              </p>
            </section>

            <section class="panel">
              <div class="section-heading">
                <h2>Cached services</h2>
              </div>
              {#if !routes.length}<p class="empty-small">
                  Request or test a remote hostname to discover its provider.
                </p>{/if}
              {#each routes as [host, id]}
                <div class="route">
                  <code>{host}</code><span>{
                    peers.find((peer) => peer.peer_id === id)?.profile?.device_name ?? shortId(id)
                  }</span><button
                    class="quiet"
                    disabled={busy}
                    onclick={() => {
                      hostname = host;
                      void action(async () => {
                        result = null;
                        result = await api<TestResult>(token, "test", { hostname: host });
                      });
                    }}
                  >
                    Test
                  </button>
                </div>
              {/each}
              <p class="footnote">
                Routes stay cached until a provider becomes unavailable. This is
                not a service inventory.
              </p>
            </section>
          </div>

          <aside>
            <section class="panel pairing">
              <h2>Pair computer</h2>
              <p>
                Pair only computers you trust with your local services and app
                credentials.
              </p>
              <button
                class="primary wide"
                disabled={busy}
                onclick={() =>
                action(async () => {
                  invitation = encodeTicket(await api(token, "invite", 600));
                })}
              >
                Create invitation
              </button>
              {#if invitation}<label for="invitation"
                >One-use ticket · expires in 10 minutes</label><textarea
                  id="invitation"
                  rows="3"
                  readonly
                  value={invitation}
                ></textarea><div class="ticket-actions">
                  <button class="quiet" onclick={() => copy(invitation)}>
                    Copy ticket
                  </button>
                  <button class="quiet" onclick={() => invitation = ""}>
                    Hide ticket
                  </button>
                </div>{/if}
              <form
                class="join-form"
                onsubmit={(event) => {
                  event.preventDefault();
                  void action(async () => {
                    await api(token, "pair", ticket.trim());
                    ticket = "";
                    notice = "Computer paired. Both computers have saved the connection.";
                  });
                }}
              >
                <label for="ticket"
                >Invitation from another computer</label><textarea
                  id="ticket"
                  bind:value={ticket}
                  placeholder="Paste xe1:… ticket"
                  rows="2"
                  required
                ></textarea><button
                  class="secondary wide"
                  disabled={busy || !ticket.trim()}
                >
                  Pair computer
                </button>
              </form>
            </section>
            <section class="panel test-panel">
              <h2>Test service</h2>
              <p>
                Find the provider and send a GET / without app credentials. This
                may start its container.
              </p>
              <form
                onsubmit={(event) => {
                  event.preventDefault();
                  void action(async () => {
                    result = null;
                    result = await api<TestResult>(token, "test", {
                      hostname: hostname.trim(),
                    });
                  });
                }}
              >
                <label for="hostname">Service hostname</label><input
                  id="hostname"
                  bind:value={hostname}
                  placeholder="notes.localhost"
                  required
                /><button
                  class="secondary wide"
                  disabled={busy || !hostname.trim()}
                >
                  {busy ? "Working…" : "Test service"}
                </button>
              </form>
              {#if result}<div class="test-result" aria-live="polite">
                  <div>
                    <strong class:failure={result.status >= 400}>HTTP {
                        result.status
                      }</strong><span>{result.elapsed_ms} ms</span>
                  </div>
                  <p>
                    {
                      result.provider === "peer"
                      ? `Via ${
                        peers.find((peer) => peer.peer_id === result!.peer_id)?.profile
                          ?.device_name ?? shortId(result.peer_id!)
                      }`
                      : result.provider === "local"
                      ? "Served on this computer"
                      : "No provider found"
                    }
                  </p>
                  <pre>{preview(result.body_preview)}</pre>
                  {#if result.truncated}<small>Preview limited to 4
                      KiB.</small>{/if}
                </div>{/if}
            </section>
          </aside>
        </div>
      {:else if tab === "network"}
        <div class="network-grid">
          <section class="panel">
            <div class="section-heading">
              <h2>Local network</h2><span class="tag">Last report</span>
            </div>
            <dl>
              <dt>IPv4 UDP reachable</dt>
              <dd>{yesNo(data.iroh.net_report?.udp_v4)}</dd>
              <dt>IPv6 UDP reachable</dt>
              <dd>{yesNo(data.iroh.net_report?.udp_v6)}</dd>
              <dt>Destination-dependent NAT</dt>
              <dd>
                {yesNo(data.iroh.net_report?.mapping_varies_by_dest_ipv4)}
              </dd>
              <dt>Public IPv4</dt>
              <dd>{data.iroh.net_report?.global_v4 ?? "Not observed"}</dd>
              <dt>Public IPv6</dt>
              <dd>{data.iroh.net_report?.global_v6 ?? "Not observed"}</dd>
              <dt>Local upstream</dt>
              <dd>{data.agent.upstream}</dd>
              <dt>Listener</dt>
              <dd>{data.agent.configured_listen}</dd>
            </dl>
            <p class="footnote">
              Destination-dependent NAT can make direct connections harder. Iroh
              can fall back to a relay.
            </p>
          </section>
          <section class="panel">
            <div class="section-heading"><h2>Home relays</h2></div>
            {#each data.iroh.home_relays as relay}<div class="relay">
                <span class:online={relay.connected} class="badge">{
                  relay.connected ? "Connected" : "Disconnected"
                }</span><code>{relay.url}</code>{#if relay.last_error}<p>
                    {relay.last_error}
                  </p>{/if}
              </div>{/each}
            {#if !data.iroh.home_relays.length}<p class="empty-small">
                No home relay observed yet.
              </p>{/if}
            <p class="footnote">
              A relay can stay connected while requests take a direct path.
              Check each computer’s selected path.
            </p>
          </section>
          <section class="panel">
            <div class="section-heading"><h2>Resource usage</h2></div>
            <dl>
              {#each [
                "trusted_connections",
                "pending_pairings",
                "incoming_handshakes",
                "http_connections",
                "request_handlers",
              ] as key}<dt>{key.replaceAll("_", " ")}</dt><dd>
                  {data.resources[key]?.used ?? "—"} / {
                    data.resources[key]?.limit ?? "—"
                  }
                </dd>{/each}
            </dl>
          </section>
          <section class="panel">
            <div class="section-heading"><h2>Connection details</h2></div>
            {#each data.connections as connection}<details class="connection">
                <summary>
                  {shortId(connection.peer_id)} · {connection.direction} · {
                    connection.closed ? "closed" : "open"
                  }
                </summary>
                <pre>{JSON.stringify(connection, null, 2)}</pre>
              </details>{/each}
            {#if !data.connections.length}<p class="empty-small">
                Test a peer’s service to establish a connection.
              </p>{/if}
          </section>
        </div>
      {:else}
        <section class="panel raw">
          <div class="section-heading">
            <h2>Diagnostics snapshot</h2>
            <button
              class="quiet"
              onclick={() => copy(JSON.stringify(data, null, 2))}
            >
              Copy JSON
            </button>
          </div>
          <p>
            Includes public identities, network addresses, display names, and
            internal counters. Review before sharing.
          </p>
          <pre>{JSON.stringify(data, null, 2)}</pre>
        </section>
      {/if}
    </main>
    <footer>
      RTT measures transport latency, not application response time.
    </footer>
  {/if}
</div>
