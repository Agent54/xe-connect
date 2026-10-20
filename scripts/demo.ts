// One script contains the dummy services, hostname gateways, and demo launcher.
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const ROOT = fileURLToPath(new URL("../", import.meta.url));
const SCRIPT = fileURLToPath(import.meta.url);
const BINARY = join(ROOT, "target/debug/xe-connect");

export async function buildBinary() {
  const ui = await new Deno.Command("deno", {
    args: ["task", "build:ui"],
    cwd: ROOT,
    stdout: "inherit",
    stderr: "inherit",
  }).output();
  if (!ui.success) throw new Error("UI build failed");
  const result = await new Deno.Command("cargo", {
    args: ["build", "--locked"],
    cwd: ROOT,
    stdout: "inherit",
    stderr: "inherit",
  }).output();
  if (!result.success) throw new Error("cargo build failed");
}

interface ServerConfig {
  kind: "service" | "gateway";
  computer: string;
  hostname: string;
  port: number;
  servicePort: number;
}

async function serve(config: ServerConfig) {
  const client = Deno.createHttpClient({ allowHost: true, http2: false });
  const abort = new AbortController();
  const stop = () => abort.abort();
  Deno.addSignalListener("SIGINT", stop);
  Deno.addSignalListener("SIGTERM", stop);
  const server = Deno.serve({
    hostname: "127.0.0.1",
    port: config.port,
    signal: abort.signal,
    onListen() {},
  }, async (request) => {
    const hostname = request.headers.get("host")?.split(":")[0].toLowerCase();
    if (hostname !== config.hostname) {
      return Response.json({ error: "service not on this computer" }, {
        status: 404,
      });
    }
    const url = new URL(request.url);
    if (config.kind === "gateway") {
      const headers = new Headers(request.headers);
      for (
        const name of ["connection", "transfer-encoding", "content-length"]
      ) headers.delete(name);
      headers.set("x-demo-gateway", config.computer);
      try {
        return await fetch(
          `http://127.0.0.1:${config.servicePort}${url.pathname}${url.search}`,
          {
            client,
            method: request.method,
            headers,
            body: ["GET", "HEAD"].includes(request.method)
              ? undefined
              : await request.arrayBuffer(),
            redirect: "manual",
            signal: AbortSignal.timeout(3000),
          },
        );
      } catch {
        return Response.json({ error: "dummy service unavailable" }, {
          status: 502,
        });
      }
    }
    if (request.method === "HEAD") return new Response(null);
    return Response.json({
      computer: config.computer,
      service: config.hostname,
      service_port: config.port,
      gateway: request.headers.get("x-demo-gateway"),
      host: request.headers.get("host"),
      source_peer: request.headers.get("x-xe-source-peer"),
      method: request.method,
      path: url.pathname + url.search,
      body: await request.text(),
    }, { headers: { "cache-control": "no-store" } });
  });
  try {
    await server.finished;
  } finally {
    client.close();
    Deno.removeSignalListener("SIGINT", stop);
    Deno.removeSignalListener("SIGTERM", stop);
  }
}

export class Computer {
  constructor(
    readonly name: string,
    readonly hostname: string,
    readonly directory: string,
    readonly listen: number,
    readonly gateway: number,
    readonly service: number,
  ) {}

  argv(...args: string[]) {
    return ["--state-dir", join(this.directory, "state"), ...args];
  }

  async command(...args: string[]) {
    const result = await new Deno.Command(BINARY, {
      args: this.argv(...args),
      stdin: "null",
      stdout: "piped",
      stderr: "piped",
      signal: AbortSignal.timeout(35000),
    }).output();
    if (!result.success) {
      throw new Error(new TextDecoder().decode(result.stderr));
    }
    return new TextDecoder().decode(result.stdout).trim();
  }
}

interface Child {
  process: Deno.ChildProcess;
  done: Promise<Deno.CommandStatus>;
  exited: boolean;
}

export class Demo {
  readonly a: Computer;
  readonly b: Computer;
  private children: Child[] = [];
  private client?: Deno.HttpClient;

  constructor(directory = join(ROOT, "target/manual-demo"), basePort = 5210) {
    if (!Number.isInteger(basePort) || basePort < 1 || basePort > 65523) {
      throw new Error("base port must be between 1 and 65523");
    }
    directory = resolve(directory);
    this.a = new Computer(
      "Computer A",
      "alpha.localhost",
      join(directory, "computer-a"),
      basePort,
      basePort + 1,
      basePort + 2,
    );
    this.b = new Computer(
      "Computer B",
      "beta.web.localhost",
      join(directory, "computer-b"),
      basePort + 10,
      basePort + 11,
      basePort + 12,
    );
  }

  private spawn(program: string, args: string[]) {
    const process = new Deno.Command(program, {
      args,
      cwd: ROOT,
      stdin: "null",
      stdout: "null",
      stderr: "inherit",
    }).spawn();
    const child: Child = {
      process,
      exited: false,
      done: process.status,
    };
    child.done = process.status.then((status) => {
      child.exited = true;
      return status;
    });
    this.children.push(child);
    return child;
  }

  async start() {
    const reservations: Deno.TcpListener[] = [];
    try {
      for (const computer of [this.a, this.b]) {
        for (
          const port of [computer.listen, computer.gateway, computer.service]
        ) {
          reservations.push(Deno.listen({ hostname: "127.0.0.1", port }));
        }
      }
    } catch (error) {
      throw new Error("Demo ports unavailable; choose another --base-port", {
        cause: error,
      });
    } finally {
      for (const listener of reservations) listener.close();
    }
    this.client = Deno.createHttpClient({ allowHost: true, http2: false });
    try {
      for (const computer of [this.a, this.b]) {
        await Deno.mkdir(computer.directory, { recursive: true });
        for (const kind of ["service", "gateway"] as const) {
          const port = kind === "service" ? computer.service : computer.gateway;
          const config: ServerConfig = {
            kind,
            port,
            computer: computer.name,
            hostname: computer.hostname,
            servicePort: computer.service,
          };
          const child = this.spawn("deno", [
            "run",
            "--no-config",
            "--no-prompt",
            "--allow-net=127.0.0.1",
            SCRIPT,
            "server",
            JSON.stringify(config),
          ]);
          await this.waitReady(child, port, computer.hostname);
        }
        const child = this.spawn(
          BINARY,
          computer.argv(
            "--device-name",
            computer.name,
            "--user-name",
            "Demo User",
            "--listen",
            `127.0.0.1:${computer.listen}`,
            "--upstream",
            `http://127.0.0.1:${computer.gateway}`,
          ),
        );
        await this.waitReady(child, computer.listen, computer.hostname);
      }
    } catch (error) {
      await this.close();
      throw error;
    }
  }

  private async waitReady(child: Child, port: number, hostname: string) {
    const deadline = Date.now() + 15000;
    while (Date.now() < deadline) {
      if (child.exited) throw new Error(`Server on port ${port} exited`);
      try {
        const response = await fetch(`http://127.0.0.1:${port}/`, {
          client: this.client,
          method: "HEAD",
          headers: { host: hostname },
          signal: AbortSignal.timeout(300),
        });
        if (response.status === 200) return;
      } catch { /* Starting up. */ }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    throw new Error(`Server on port ${port} did not become ready`);
  }

  async close() {
    const kill = (signal: Deno.Signal) => {
      for (const child of this.children) {
        if (!child.exited) {
          try {
            child.process.kill(signal);
          } catch { /* Already exited. */ }
        }
      }
    };
    kill("SIGTERM");
    const timer = setTimeout(() => kill("SIGKILL"), 5000);
    try {
      await Promise.all(this.children.map((child) => child.done));
    } finally {
      clearTimeout(timer);
      this.children = [];
      this.client?.close();
      this.client = undefined;
    }
  }

  async instructions() {
    const command = (computer: Computer, ...args: string[]) =>
      [BINARY, ...computer.argv(...args)].map((arg) =>
        `'${arg.replaceAll("'", "'\\''")}'`
      ).join(" ");
    console.log(
      "Demo ready. Pairing is manual; existing identities and pairing are kept.",
    );
    console.table([this.a, this.b].map((computer) => ({
      computer: computer.name,
      agent: computer.listen,
      gateway: computer.gateway,
      dummy_service: computer.service,
      hostname: computer.hostname,
    })));
    console.log(
      "\nOpen these private UI links to pair and test in the browser:",
    );
    for (const computer of [this.a, this.b]) {
      console.log(`${computer.name}: ${await computer.command("ui")}`);
    }
    console.log(
      "Create an invitation in A, paste it in B, then test beta.web.localhost in A.",
    );
    console.log(
      "In another terminal, create a ticket on A, then paste it into B's command:",
    );
    console.log(command(this.a, "invite"));
    console.log(command(this.b, "pair", "PASTE_TICKET_HERE"));
    console.log("\nTry B's service from A, and A's service from B:");
    for (
      const [computer, hostname] of [[this.a, this.b.hostname], [
        this.b,
        this.a.hostname,
      ]] as const
    ) {
      const url = `http://${hostname}:${computer.listen}/`;
      console.log(url);
      console.log(
        `curl --resolve ${hostname}:${computer.listen}:127.0.0.1 ${url}`,
      );
    }
    console.log(`\nState folders: ${this.a.directory} and ${this.b.directory}`);
    console.log(
      "Use another --state-dir for fresh unpaired identities. Ctrl+C stops all six processes.",
    );
  }
}

async function main() {
  let directory = join(ROOT, "target/manual-demo"),
    basePort = 5210,
    build = true;
  for (let i = 0; i < Deno.args.length; i++) {
    switch (Deno.args[i]) {
      case "--state-dir":
        directory = Deno.args[++i];
        break;
      case "--base-port":
        basePort = Number(Deno.args[++i]);
        break;
      case "--no-build":
        build = false;
        break;
      default:
        throw new Error(
          "Usage: deno task demo [--state-dir target/fresh] [--base-port 5310] [--no-build]",
        );
    }
  }
  if (build) await buildBinary();
  const demo = new Demo(directory, basePort);
  const stopped = Promise.withResolvers<void>();
  const stop = () => stopped.resolve();
  Deno.addSignalListener("SIGINT", stop);
  Deno.addSignalListener("SIGTERM", stop);
  try {
    await demo.start();
    await demo.instructions();
    await stopped.promise;
  } finally {
    await demo.close();
    Deno.removeSignalListener("SIGINT", stop);
    Deno.removeSignalListener("SIGTERM", stop);
  }
}

if (import.meta.main) {
  if (Deno.args[0] === "server") await serve(JSON.parse(Deno.args[1]));
  else await main();
}
