import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { EventEmitter, once } from "node:events";
import { PassThrough } from "node:stream";
import test from "node:test";
import { debuggerUrl } from "./chrome-startup.mjs";

function fakeChrome() {
  const chrome = new EventEmitter();
  chrome.stderr = new PassThrough();
  chrome.exitCode = null;
  chrome.signalCode = null;
  return chrome;
}

function listenersReleased(chrome) {
  assert.equal(chrome.stderr.listenerCount("data"), 0);
  assert.equal(chrome.listenerCount("exit"), 0);
  assert.equal(chrome.listenerCount("error"), 0);
}

test("partial pipe chunks cannot publish a truncated debugger URL", async () => {
  const chrome = fakeChrome();
  let resolved = false;
  const url = debuggerUrl(chrome).then((value) => { resolved = true; return value; });
  chrome.stderr.write("startup diagnostics\nDevTools listen");
  chrome.stderr.write("ing on ws://127.0.0.1:1234/devtools/brow");
  await Promise.resolve();
  assert.equal(resolved, false);
  chrome.stderr.write("ser/complete-endpoint\n");
  assert.equal(await url, "ws://127.0.0.1:1234/devtools/browser/complete-endpoint");
  listenersReleased(chrome);
});

test("30-second startup allowance is fixed and retains only the last 4 KiB of stderr", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const chrome = fakeChrome();
  let resolved = false;
  const url = debuggerUrl(chrome).finally(() => { resolved = true; });
  const rejected = assert.rejects(url, (error) => {
    assert.match(error.message, /within 30000 ms/);
    assert.ok(error.message.endsWith("last diagnostic"));
    assert.ok(!error.message.includes("discarded prefix"));
    assert.ok(error.message.length < 4300);
    return true;
  });
  chrome.stderr.write("discarded prefix" + "x".repeat(32 * 1024) + "last diagnostic");
  context.mock.timers.tick(10_001);
  await Promise.resolve();
  assert.equal(resolved, false);
  chrome.stderr.write("last diagnostic"); // New output does not restart the deadline.
  context.mock.timers.tick(19_999);
  await rejected;
  listenersReleased(chrome);
});

test("early exit includes stderr and removes only startup listeners", async () => {
  const chrome = fakeChrome();
  const ownerError = () => {};
  chrome.on("error", ownerError);
  const url = debuggerUrl(chrome);
  chrome.stderr.write("browser initialization failed\n");
  chrome.emit("exit", 17, null);
  await assert.rejects(url, /Chrome exited before DevTools was ready \(17\).*\nChrome stderr.*\nbrowser initialization failed/s);
  assert.equal(chrome.listenerCount("error"), 1);
  chrome.off("error", ownerError);
  listenersReleased(chrome);
});

test("a previously exited child fails without waiting for the startup deadline", async () => {
  const chrome = fakeChrome();
  chrome.exitCode = 23;
  await assert.rejects(debuggerUrl(chrome), /ready \(23\)/);
  listenersReleased(chrome);
});

test("an actual spawn error rejects promptly and leaves child cleanup with the caller", async () => {
  const chrome = spawn("/cairn-missing-chrome-startup-fixture", [], { stdio: ["ignore", "ignore", "pipe"] });
  const closed = new Promise((resolve) => chrome.once("close", resolve));
  await assert.rejects(debuggerUrl(chrome), (error) => {
    assert.equal(error.cause.code, "ENOENT");
    assert.match(error.message, /Chrome stderr.*\n\(empty\)/);
    return true;
  });
  await closed;
  listenersReleased(chrome);
});

test("an owned real child handshake succeeds across asynchronous writes", async () => {
  const chrome = spawn(process.execPath, ["-e", `
    process.stderr.write('DevTools listening on ws://127.0.0.1:9222/devtools/');
    setImmediate(() => process.stderr.write('browser/fixture\\n'));
  `], { stdio: ["ignore", "ignore", "pipe"] });
  const closed = once(chrome, "close");
  assert.equal(await debuggerUrl(chrome), "ws://127.0.0.1:9222/devtools/browser/fixture");
  assert.deepEqual(await closed, [0, null]);
  listenersReleased(chrome);
});
