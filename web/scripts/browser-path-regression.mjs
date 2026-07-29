// WHATWG URL regression for the console transfer boundary. Browsers normalize percent-encoded
// dot segments just like literal ones, so neither spelling can safely identify an S3 key in a
// path-style presigned URL. The server/UI rejection tests own the policy; this pins the platform
// behavior that makes the rejection necessary.

import { stdout } from "node:process";
import { URL } from "node:url";

const normalized = new Map([
  ["/bucket/./object", "/bucket/object"],
  ["/bucket/../object", "/object"],
  ["/bucket/%2E/object", "/bucket/object"],
  ["/bucket/%2e%2E/object", "/object"],
  ["/bucket/.%2e/object", "/object"],
]);

for (const [input, expected] of normalized) {
  const actual = new URL(input, "https://data.example.test").pathname;
  if (actual !== expected) {
    throw new Error(`WHATWG path normalization changed: ${input} -> ${actual}, expected ${expected}`);
  }
}

// A percent sign encoded one level deeper is not normalized as a dot by WHATWG. Cairn still rejects
// this alias at its JSON presign boundary so an accidentally pre-encoded caller fails closed.
const doubleEncoded = new URL(
  "/bucket/%252E/object",
  "https://data.example.test",
).pathname;
if (doubleEncoded !== "/bucket/%252E/object") {
  throw new Error(`unexpected double-encoded path normalization: ${doubleEncoded}`);
}

stdout.write("browser path regression: dot-segment normalization pinned\n");
