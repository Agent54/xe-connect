export interface Profile {
  device_name: string;
  user_name: string | null;
}

export interface Path {
  path_id: string;
  selected: boolean;
  kind: string;
  remote_address: unknown;
  stats: { rtt_ms: number; tx: { bytes: number }; rx: { bytes: number } };
}

export interface Connection {
  connection_id: number;
  peer_id: string;
  closed: boolean;
  direction: string;
  close_reason: string | null;
  paths: Path[];
}

export interface Snapshot {
  sampled_at_unix_ms: number;
  agent: {
    version: string;
    uptime_ms: number;
    endpoint_id: string;
    profile: Profile;
    upstream: string;
    configured_listen: string;
  };
  iroh: {
    home_relays: {
      url: string;
      connected: boolean;
      last_error: string | null;
    }[];
    net_report: {
      udp_v4: boolean | null;
      udp_v6: boolean | null;
      mapping_varies_by_dest_ipv4: boolean | null;
      global_v4: string | null;
      global_v6: string | null;
      preferred_relay: string | null;
    } | null;
  };
  peers: {
    peer_id: string;
    paired: boolean;
    profile: Profile | null;
    active_connection_ids: number[];
  }[];
  connections: Connection[];
  discovery: { routes: Record<string, string>; in_flight_hostnames: string[] };
  resources: Record<string, { used: number; limit: number }>;
}

export interface TestResult {
  hostname: string;
  provider: "local" | "peer" | "none";
  peer_id: string | null;
  status: number;
  elapsed_ms: number;
  body_preview: string;
  truncated: boolean;
}

export async function api<T>(
  token: string,
  path: string,
  value?: unknown,
): Promise<T> {
  const response = await fetch(`/_xe/${path}`, {
    method: value === undefined ? "GET" : "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: value === undefined ? undefined : JSON.stringify(value),
    credentials: "omit",
    cache: "no-store",
    signal: AbortSignal.timeout(25_000),
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(
      response.status === 401
        ? "Access denied. Run the ui command again to get a fresh private link."
        : payload.message ?? `Request failed (${response.status}).`,
    );
  }
  return payload as T;
}

export function encodeTicket(ticket: unknown): string {
  const bytes = new TextEncoder().encode(JSON.stringify(ticket));
  return "xe1:" + btoa(String.fromCharCode(...bytes))
    .replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}

export const shortId = (id: string) => `${id.slice(0, 8)}…${id.slice(-6)}`;
export const yesNo = (value: boolean | null | undefined) =>
  value == null ? "Not observed" : value ? "Yes" : "No";
