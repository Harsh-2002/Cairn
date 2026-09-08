// Browser initialization allowance, separate from every product performance assertion.
const STARTUP_TIMEOUT_MS = 30_000;
const STDERR_LIMIT = 4096;

export function debuggerUrl(chrome) {
  return new Promise((resolve, reject) => {
    let output = Buffer.alloc(0);
    let settled = false;
    const finish = (error, url) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      chrome.stderr.off("data", onData);
      chrome.off("error", onError);
      chrome.off("exit", onExit);
      if (error) {
        const diagnostic = output.toString("utf8").trim();
        reject(new Error(`${error.message}\nChrome stderr (last ${STDERR_LIMIT} bytes):\n${diagnostic || "(empty)"}`, { cause: error }));
      } else {
        resolve(url);
      }
    };
    const onError = (error) => finish(error);
    const onExit = (code, signal) => finish(new Error(`Chrome exited before DevTools was ready (${signal ?? code})`));
    const onData = (chunk) => {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      output = Buffer.concat([output, bytes.subarray(-STDERR_LIMIT)]).subarray(-STDERR_LIMIT);
      // Wait for whitespace: a URL split across pipe chunks is not a complete endpoint.
      const match = output.toString("utf8").match(/DevTools listening on (ws:\/\/[^\s]+)(?=\s)/);
      if (match) finish(null, match[1]);
    };
    const timer = setTimeout(() => finish(new Error(`Chrome did not expose DevTools within ${STARTUP_TIMEOUT_MS} ms`)), STARTUP_TIMEOUT_MS);
    chrome.stderr.on("data", onData);
    chrome.once("error", onError);
    chrome.once("exit", onExit);
    if (chrome.exitCode !== null && chrome.exitCode !== undefined) onExit(chrome.exitCode, chrome.signalCode);
    else if (chrome.signalCode) onExit(chrome.exitCode, chrome.signalCode);
  });
}
