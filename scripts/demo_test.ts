import assert from "node:assert/strict";
import { join } from "node:path";
import { buildBinary, Computer, Demo, ROOT } from "./demo.ts";

function freeBasePort() {
  for (let attempt = 0; attempt < 100; attempt++) {
    const base = 20000 + Math.floor(Math.random() * 40000);
    const listeners: Deno.TcpListener[] = [];
    try {
      for (const offset of [0, 1, 2, 10, 11, 12]) {
        listeners.push(
          Deno.listen({ hostname: "127.0.0.1", port: base + offset }),
        );
      }
      return base;
    } catch (error) {
      if (!(error instanceof Deno.errors.AddrInUse)) throw error;
    } finally {
      for (const listener of listeners) listener.close();
    }
  }
  throw new Error("Could not find six unused ports");
}

async function request(
  client: Deno.HttpClient,
  computer: Computer,
  hostname: string,
  method = "GET",
  path = "/",
  body?: string,
) {
  const response = await fetch(`http://127.0.0.1:${computer.listen}${path}`, {
    client,
    method,
    body,
    headers: { host: `${hostname}:${computer.listen}` },
    signal: AbortSignal.timeout(20000),
  });
  return { status: response.status, payload: await response.json() };
}

Deno.test("manual pairing makes both isolated dummy services reachable", async () => {
  await buildBinary();
  const testDirectory = join(ROOT, "target/demo-tests");
  await Deno.mkdir(testDirectory, { recursive: true });
  const directory = await Deno.makeTempDir({
    dir: testDirectory,
    prefix: "demo-test-",
  });
  const demo = new Demo(directory, freeBasePort());
  const client = Deno.createHttpClient({ allowHost: true, http2: false });
  try {
    await demo.start();
    const { a, b } = demo;
    assert.equal(
      new Set([a.listen, a.gateway, a.service, b.listen, b.gateway, b.service])
        .size,
      6,
    );
    const aId = await a.command("id"), bId = await b.command("id");
    assert.notEqual(aId, bId);
    for (const computer of [a, b]) {
      assert.deepEqual(JSON.parse(await computer.command("peers")).peers, {});
      const { status, payload } = await request(
        client,
        computer,
        computer.hostname,
      );
      assert.equal(status, 200);
      assert.equal(payload.computer, computer.name);
      assert.equal(payload.gateway, computer.name);
      assert.equal(payload.service_port, computer.service);
      assert.equal(payload.source_peer, null);
    }
    assert.equal((await request(client, a, b.hostname)).status, 404);
    assert.equal((await request(client, b, a.hostname)).status, 404);
    const ticket = await a.command("invite");
    assert.ok(ticket.startsWith("xe1:"));
    await b.command("pair", ticket);
    for (
      const [caller, provider, callerId, providerId] of [[a, b, aId, bId], [
        b,
        a,
        bId,
        aId,
      ]] as const
    ) {
      const { status, payload } = await request(
        client,
        caller,
        provider.hostname,
        "POST",
        "/echo?q=test",
        "hello",
      );
      assert.equal(status, 200, JSON.stringify(payload));
      assert.equal(payload.computer, provider.name);
      assert.equal(payload.gateway, provider.name);
      assert.equal(payload.service_port, provider.service);
      assert.equal(payload.source_peer, callerId);
      assert.equal(payload.host, `${provider.hostname}:${provider.gateway}`);
      assert.equal(payload.method, "POST");
      assert.equal(payload.path, "/echo?q=test");
      assert.equal(payload.body, "hello");
      const peers = JSON.parse(await caller.command("peers"));
      assert.equal(peers.peers[providerId].profile.device_name, provider.name);
      assert.equal(peers.routes[provider.hostname], providerId);
    }
    await a.command("unpair", bId);
    assert.deepEqual(JSON.parse(await a.command("peers")).routes, {});
    assert.equal((await request(client, a, b.hostname)).status, 404);
    assert.equal((await request(client, a, a.hostname)).status, 200);
  } finally {
    client.close();
    await demo.close();
    // Verify all six HTTP ports were released before removing this test's state.
    for (const computer of [demo.a, demo.b]) {
      for (
        const port of [computer.listen, computer.gateway, computer.service]
      ) {
        Deno.listen({ hostname: "127.0.0.1", port }).close();
      }
    }
    await Deno.remove(directory, { recursive: true });
  }
});
