// Bounded browser-only regressions: actual React components, mocked API, no Cairn data.
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer } from "vite";

let profileDir;
let server;
let chrome;
let chromeClosed;
let socket;
let failure;

async function within(promise, milliseconds, description) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${description} timed out`)), milliseconds);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function stopChrome() {
  // A spawn failure has no process to signal, but still emits close after error.
  if (!chrome?.pid) return;
  chrome.kill("SIGTERM");
  try {
    await within(chromeClosed, 2000, "Chrome graceful exit");
  } catch {
    chrome.kill("SIGKILL");
    await within(chromeClosed, 2000, "Chrome forced exit");
  }
}

try {
profileDir = await mkdtemp(join(tmpdir(), "cairn-console-performance-"));
server = await createServer({
  server: { host: "127.0.0.1", port: 0 },
  plugins: [{
    name: "performance-fixture",
    configureServer(server) {
      server.middlewares.use("/__performance", async (_req, res, next) => {
        try {
          const html = await server.transformIndexHtml("/__performance", '<html><body><script type="module" src="/scripts/fixtures/performance.tsx"></script></body></html>');
          res.setHeader("Content-Type", "text/html");
          res.end(html);
        } catch (error) {
          next(error);
        }
      });
    },
  }],
});
await server.listen();
const baseUrl = `http://127.0.0.1:${server.httpServer.address().port}/__performance`;
chrome = spawn(process.env.CHROME_BIN ?? "google-chrome", [
  "--headless=new", "--disable-gpu", "--disable-dev-shm-usage", "--no-first-run",
  "--no-default-browser-check", "--remote-debugging-port=0", `--user-data-dir=${profileDir}`,
  "about:blank",
], { stdio: ["ignore", "ignore", "pipe"] });
// Subscribe immediately: ENOENT emits error instead of exit, and close may happen
// before cleanup starts. Keep the error listener after startup to avoid uncaught events.
chrome.on("error", (error) => { failure ??= error; });
chromeClosed = new Promise((resolve) => chrome.once("close", resolve));
function debuggerUrl() {
  return new Promise((resolve, reject) => {
    let output = "";
    const finish = (error, url) => {
      clearTimeout(timer);
      chrome.stderr.off("data", onData);
      chrome.off("error", onError);
      chrome.off("exit", onExit);
      if (error) reject(error);
      else resolve(url);
    };
    const onError = (error) => finish(error);
    const onExit = (code, signal) => finish(new Error(`Chrome exited before DevTools was ready (${signal ?? code})`));
    const onData = (chunk) => {
      output += chunk;
      const match = output.match(/DevTools listening on (ws:\/\/[^\s]+)/);
      if (match) finish(null, match[1]);
    };
    const timer = setTimeout(() => finish(new Error("Chrome did not expose DevTools")), 10_000);
    chrome.stderr.setEncoding("utf8");
    chrome.stderr.on("data", onData);
    chrome.once("error", onError);
    chrome.once("exit", onExit);
  });
}

const browserWsUrl = new URL(await debuggerUrl());
const target = await fetch(
  `http://${browserWsUrl.host}/json/new?${encodeURIComponent("about:blank")}`,
  { method: "PUT", signal: AbortSignal.timeout(10_000) },
).then((response) => response.json());
socket = new WebSocket(target.webSocketDebuggerUrl);
await within(new Promise((resolve, reject) => {
  socket.addEventListener("open", resolve, { once: true });
  socket.addEventListener("error", reject, { once: true });
}), 10_000, "DevTools connection");

let sequence = 0;
const pending = new Map();
const browserErrors = [];
socket.addEventListener("message", (event) => {
  const message = JSON.parse(event.data);
  if (message.id) {
    const waiter = pending.get(message.id);
    if (!waiter) return;
    pending.delete(message.id);
    if (message.error) waiter.reject(new Error(message.error.message));
    else waiter.resolve(message.result);
    return;
  }
  if (message.method === "Runtime.exceptionThrown") {
    browserErrors.push(message.params.exceptionDetails.text);
  }
  if (
    message.method === "Runtime.consoleAPICalled" &&
    ["error", "assert"].includes(message.params.type)
  ) {
    browserErrors.push(
      message.params.args.map((arg) => arg.value ?? arg.description ?? "console error").join(" "),
    );
  }
});

function command(method, params = {}) {
  const id = ++sequence;
  return within(new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    socket.send(JSON.stringify({ id, method, params }));
  }), 10_000, `DevTools ${method}`).finally(() => pending.delete(id));
}

async function evaluate(expression) {
  const result = await command("Runtime.evaluate", {
    expression,
    awaitPromise: true,
    returnByValue: true,
  });
  if (result.exceptionDetails) {
    throw new Error(result.exceptionDetails.exception?.description ?? result.exceptionDetails.text);
  }
  return result.result.value;
}

async function waitFor(expression, description, timeoutMs = 12_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await evaluate(expression)) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`Timed out waiting for ${description}: ${browserErrors.join("; ")}`);
}


await command("Runtime.enable");
for (const [width, height] of [[1280, 900], [390, 844]]) {
  await command("Emulation.setDeviceMetricsOverride", { width, height, deviceScaleFactor: 1, mobile: false });
  await evaluate("window.performanceRegression = undefined");
  await command("Page.navigate", { url: `${baseUrl}?width=${width}` });
  await waitFor("window.performanceRegression", "performance regressions", 30_000);
  const result = await evaluate("window.performanceRegression");
  if (!result.ok) throw new Error(result.message);
  process.stdout.write(`${width}px: ${result.message}\n`);
}
if (browserErrors.length) throw new Error(browserErrors.join("\n"));
} catch (error) {
  failure = error;
} finally {
  const cleanupErrors = [];
  socket?.close();
  try {
    await stopChrome();
  } catch (error) {
    cleanupErrors.push(error);
  }
  try {
    await server?.close();
  } catch (error) {
    cleanupErrors.push(error);
  }
  try {
    // Renderer processes can finish profile writes shortly after the browser closes.
    if (profileDir) await rm(profileDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  } catch (error) {
    cleanupErrors.push(error);
  }
  if (cleanupErrors.length) {
    failure = new AggregateError(
      [...(failure ? [failure] : []), ...cleanupErrors],
      "Browser regression cleanup failed",
    );
  }
}
if (failure) throw failure;
