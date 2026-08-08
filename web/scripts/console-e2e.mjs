// Browser-level console smoke and accessibility coverage. Run against a disposable Cairn node:
// CAIRN_E2E_BASE_URL=http://127.0.0.1:7374 CAIRN_E2E_ACCESS_KEY=... \
// CAIRN_E2E_SECRET_KEY=... npm run e2e

import axe from "axe-core";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const baseUrl = process.env.CAIRN_E2E_BASE_URL;
const accessKey = process.env.CAIRN_E2E_ACCESS_KEY;
const secretKey = process.env.CAIRN_E2E_SECRET_KEY;
const chromeBin = process.env.CHROME_BIN ?? "google-chrome";

if (!baseUrl || !accessKey || !secretKey) {
  throw new Error(
    "CAIRN_E2E_BASE_URL, CAIRN_E2E_ACCESS_KEY, and CAIRN_E2E_SECRET_KEY are required",
  );
}

const profileDir = await mkdtemp(join(tmpdir(), "cairn-console-e2e-"));
const chrome = spawn(
  chromeBin,
  [
    "--headless=new",
    "--disable-gpu",
    "--disable-dev-shm-usage",
    "--no-first-run",
    "--no-default-browser-check",
    "--remote-debugging-port=0",
    `--user-data-dir=${profileDir}`,
    "about:blank",
  ],
  { stdio: ["ignore", "ignore", "pipe"] },
);

function debuggerUrl() {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("Chrome did not expose DevTools")), 10_000);
    let output = "";
    chrome.stderr.setEncoding("utf8");
    chrome.stderr.on("data", (chunk) => {
      output += chunk;
      const match = output.match(/DevTools listening on (ws:\/\/[^\s]+)/);
      if (match) {
        clearTimeout(timer);
        resolve(match[1]);
      }
    });
    chrome.once("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`Chrome exited before DevTools was ready (${code})`));
    });
  });
}

const browserWsUrl = new URL(await debuggerUrl());
const target = await fetch(
  `http://${browserWsUrl.host}/json/new?${encodeURIComponent(baseUrl)}`,
  { method: "PUT" },
).then((response) => response.json());
const socket = new WebSocket(target.webSocketDebuggerUrl);
await new Promise((resolve, reject) => {
  socket.addEventListener("open", resolve, { once: true });
  socket.addEventListener("error", reject, { once: true });
});

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
  return new Promise((resolve, reject) => {
    const id = ++sequence;
    pending.set(id, { resolve, reject });
    socket.send(JSON.stringify({ id, method, params }));
  });
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
  throw new Error(`Timed out waiting for ${description}`);
}

async function request(method, path, body) {
  const result = await evaluate(`(async () => {
    const response = await fetch(${JSON.stringify(`/api/v1${path}`)}, {
      method: ${JSON.stringify(method)},
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: ${body === undefined ? "undefined" : JSON.stringify(JSON.stringify(body))},
    });
    const text = await response.text();
    return { status: response.status, value: text ? JSON.parse(text) : null };
  })()`);
  if (result.status < 200 || result.status >= 300) {
    throw new Error(`${method} ${path} failed (${result.status}): ${JSON.stringify(result.value)}`);
  }
  return result.value;
}

async function accessibilityViolations() {
  return evaluate(`axe.run(document, {
    resultTypes: ["violations"],
  }).then((result) => result.violations.map((violation) => ({
    id: violation.id,
    impact: violation.impact,
    targets: violation.nodes.map((node) => node.target.join(" ")),
  })))`);
}

async function inspectRoute({ path, heading, title }) {
  await evaluate(`location.hash = ${JSON.stringify(`#${path}`)}`);
  await waitFor(
    `document.querySelector("h1")?.textContent?.trim() === ${JSON.stringify(heading)}`,
    `${path} heading`,
  );
  await waitFor(`document.title === ${JSON.stringify(`${title} — Cairn`)}`, `${path} title`);
  // Audit the settled state, not the deliberately translucent frames of the 240 ms route entrance.
  await new Promise((resolve) => setTimeout(resolve, 300));

  const integrity = await evaluate(`(() => {
    const visible = (element) => {
      const style = getComputedStyle(element);
      const box = element.getBoundingClientRect();
      return style.visibility !== "hidden" && style.display !== "none" && box.width > 0 && box.height > 0;
    };
    const ids = [...document.querySelectorAll("[id]")].map((element) => element.id);
    const duplicateIds = [...new Set(ids.filter((id, index) => ids.indexOf(id) !== index))];
    return {
      duplicateIds,
      horizontalOverflow: document.documentElement.scrollWidth - innerWidth,
      routeError: document.body.textContent.includes("Page unavailable"),
      mainCount: [...document.querySelectorAll("main")].filter(visible).length,
    };
  })()`);
  if (integrity.duplicateIds.length) {
    throw new Error(`${path} has duplicate ids: ${integrity.duplicateIds.join(", ")}`);
  }
  if (integrity.horizontalOverflow > 1) {
    throw new Error(`${path} overflows horizontally by ${integrity.horizontalOverflow}px`);
  }
  if (integrity.routeError || integrity.mainCount !== 1) {
    throw new Error(`${path} did not render one healthy main region`);
  }

  const violations = await accessibilityViolations();
  if (violations.length) {
    throw new Error(`${path} accessibility violations: ${JSON.stringify(violations)}`);
  }
}

let bucketName;
let userId;
try {
  await command("Page.enable");
  await command("Runtime.enable");
  await command("Network.enable");
  await command("Emulation.setDeviceMetricsOverride", {
    width: 1440,
    height: 1000,
    deviceScaleFactor: 1,
    mobile: false,
  });
  await waitFor("document.readyState === 'complete'", "initial page load");
  await waitFor("document.querySelector('h1')?.textContent?.includes('Sign in')", "login page");
  await waitFor("document.title === 'Sign in — Cairn'", "login title");
  await evaluate(axe.source);
  const loginViolations = await accessibilityViolations();
  if (loginViolations.length) {
    throw new Error(`login accessibility violations: ${JSON.stringify(loginViolations)}`);
  }

  await request("POST", "/session", { access_key: accessKey, secret_key: secretKey });
  await evaluate(`location.hash = "#/overview"; location.reload();`);
  await waitFor("document.querySelector('h1')?.textContent?.trim() === 'Overview'", "signed-in console");
  await evaluate(axe.source);

  bucketName = `console-e2e-${Date.now()}`;
  const userName = `Console E2E ${Date.now()}`;
  await request("POST", "/buckets", { name: bucketName, object_lock: false });
  const user = await request("POST", "/users", { display_name: userName, role: "member" });
  userId = user.id;

  const routes = [
    { path: "/overview", heading: "Overview", title: "Overview" },
    { path: "/metrics", heading: "Metrics", title: "Metrics" },
    { path: "/buckets", heading: "Buckets", title: "Buckets" },
    { path: `/buckets/${bucketName}/browser`, heading: bucketName, title: bucketName },
    { path: `/buckets/${bucketName}/uploads`, heading: bucketName, title: bucketName },
    { path: `/buckets/${bucketName}/settings`, heading: bucketName, title: bucketName },
    { path: "/users", heading: "Users", title: "Users" },
    { path: `/users/${userId}`, heading: userName, title: "User" },
    { path: "/credentials", heading: "Temporary credentials", title: "Credentials" },
    { path: "/tags", heading: "Tags", title: "Tags" },
    { path: "/activity", heading: "Activity", title: "Activity" },
    { path: "/replication", heading: "Replication", title: "Replication" },
    { path: "/imports", heading: "Import", title: "Import" },
  ];

  for (const route of routes) await inspectRoute(route);

  await command("Emulation.setDeviceMetricsOverride", {
    width: 390,
    height: 844,
    deviceScaleFactor: 1,
    mobile: true,
  });
  for (const route of routes) await inspectRoute(route);

  if (browserErrors.length) {
    throw new Error(`browser errors: ${browserErrors.join(" | ")}`);
  }
  process.stdout.write(`console e2e: ${routes.length} routes passed at desktop and mobile widths\n`);
} finally {
  if (userId) await request("DELETE", `/users/${userId}`).catch(() => {});
  if (bucketName) await request("DELETE", `/buckets/${bucketName}`).catch(() => {});
  socket.close();
  const exited = once(chrome, "exit");
  chrome.kill("SIGTERM");
  await Promise.race([
    exited,
    new Promise((resolve) => setTimeout(resolve, 5_000)),
  ]);
  await rm(profileDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
}
