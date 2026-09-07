// Focused browser regressions. Served only by console-performance.mjs, never embedded in Cairn.
import { act, StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { Buckets } from "../../src/views/buckets";
import { useResource, type Resource } from "../../src/lib/use-resource";
import { api } from "../../src/lib/api";
import "../../src/globals.css";

declare global {
  var IS_REACT_ACT_ENVIRONMENT: boolean;
  interface Window { performanceRegression?: { ok: boolean; message: string } }
}
globalThis.IS_REACT_ACT_ENVIRONMENT = true;

function assert(value: unknown, message: string): asserts value {
  if (!value) throw new Error(message);
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((ok, fail) => { resolve = ok; reject = fail; });
  return { promise, resolve, reject };
}

async function testResource() {
  const host = document.createElement("div");
  document.body.append(host);
  const root = createRoot(host);
  const calls: ReturnType<typeof deferred<string>>[] = [];
  let resource!: Resource<string>;
  function Probe({ generation }: { generation: string }) {
    resource = useResource(() => {
      const call = deferred<string>();
      calls.push(call);
      return call.promise;
    }, [generation]);
    return <p>{resource.data ?? "loading"}</p>;
  }
  await act(async () => root.render(<Probe generation="a" />));
  assert(calls.length === 1, "one initial load");
  await act(async () => { for (let i = 0; i < 100; i++) resource.refresh(); });
  assert(calls.length === 1, "refresh burst must not overlap the first load");
  await act(async () => calls[0].resolve("first"));
  assert(Number(calls.length) === 2, "burst must produce exactly one trailing load");
  assert(resource.data === "first" && resource.refreshing && !resource.loading, "retain first result during trailing load");
  await act(async () => calls[1].resolve("second"));
  assert(!resource.loading && !resource.refreshing, "settled flags");
  await act(async () => resource.refresh());
  await act(async () => calls[2].reject(new Error("expected failure")));
  assert(String(resource.data) === "second" && resource.error, "failed refresh retains stale data and exposes error");
  await act(async () => resource.refresh());
  await act(async () => { resource.refresh(); root.render(<Probe generation="b" />); });
  assert(Number(calls.length) === 5 && resource.data === undefined, "dependency change starts a fresh generation");
  await act(async () => calls[4].resolve("new route"));
  await act(async () => calls[3].resolve("old route"));
  assert(resource.data === "new route" && Number(calls.length) === 5, "old generation cannot overwrite or restart queued work");
  await act(async () => { resource.refresh(); resource.refresh(); });
  await act(async () => root.unmount());
  await act(async () => calls[5].resolve("after unmount"));
  assert(Number(calls.length) === 6, "unmount drops queued work");
  host.remove();

  const strictHost = document.createElement("div");
  document.body.append(strictHost);
  const strictRoot = createRoot(strictHost);
  await act(async () => strictRoot.render(<StrictMode><Probe generation="strict" /></StrictMode>));
  assert(Number(calls.length) === 8, "StrictMode effect replay creates separate generations");
  await act(async () => calls[7].resolve("current"));
  await act(async () => calls[6].reject(new Error("obsolete failure")));
  assert(String(resource.data) === "current" && resource.error === null, "StrictMode stale errors are ignored");
  await act(async () => strictRoot.unmount());
  strictHost.remove();
}

async function testBuckets() {
  let buckets = Array.from({ length: 2000 }, (_, i) => ({
    name: `bucket-${String(i).padStart(4, "0")}`, owner_id: "test-owner", created_at_ms: 0, versioning: "Disabled" as const,
  }));
  api.listBuckets = async () => ({ buckets });
  api.overviewBuckets = async () => ({ buckets: [] });
  // Use the existing graceful live-update fallback; no network/server fixtures.
  api.eventsTicket = async () => { throw new Error("mock SSE disabled"); };
  api.deleteBucket = async (name: string) => { buckets = buckets.filter((b) => b.name !== name); return null; };
  const host = document.createElement("div");
  document.body.append(host);
  const root = createRoot(host);
  await act(async () => root.render(<MemoryRouter><Buckets /></MemoryRouter>));
  const rows = () => host.querySelectorAll("tbody tr").length;
  const button = (label: string) => {
    const result = [...document.querySelectorAll("button")].find((b) => b.textContent?.trim() === label);
    assert(result, `button ${label}`);
    return result;
  };
  const checkbox = (label: string) => {
    const result = host.querySelector<HTMLButtonElement>(`button[aria-label="${label}"]`);
    assert(result, `checkbox ${label}`);
    return result;
  };
  assert(rows() === 50, "2000 buckets render only 50 rows");
  assert(host.textContent?.includes("Page 1 of 40"), "page count");
  await act(async () => checkbox("Select bucket-0000").click());
  await act(async () => button("Next").click());
  assert(checkbox("Select all buckets on this page").getAttribute("data-state") === "unchecked", "off-page selection does not mark page header");
  await act(async () => checkbox("Select all buckets on this page").click());
  assert(host.textContent?.includes("51 selected"), "select page preserves previous selection");
  await act(async () => checkbox("Select all buckets on this page").click());
  assert(host.textContent?.includes("1 selected"), "deselect page preserves previous selection");
  await act(async () => button("Previous").click());
  assert(checkbox("Select bucket-0000").getAttribute("data-state") === "checked", "selection survives navigation");
  await act(async () => button("Clear").click());
  for (let i = 0; i < 39; i++) await act(async () => button("Next").click());
  assert(button("Next").disabled, "last page has no next action");
  await act(async () => checkbox("Select all buckets on this page").click());
  await act(async () => button("Delete selected").click());
  const input = document.querySelector<HTMLInputElement>('[role="alertdialog"] input');
  assert(input, "confirmation input");
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, "delete 50 buckets");
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
  const confirm = [...document.querySelectorAll<HTMLButtonElement>('[role="alertdialog"] button')].find((b) => b.textContent === "Delete selected");
  assert(confirm && !confirm.disabled, "typed confirmation unlocks delete");
  await act(async () => confirm.click());
  assert(buckets.length === 1950 && rows() === 50, "delete final page refreshes list");
  assert(host.textContent?.includes("Page 39 of 39"), "deleted last page clamps");
  assert(!host.textContent?.includes("selected"), "deleted selections cleared");
  assert(document.documentElement.scrollWidth <= window.innerWidth, "pagination does not overflow viewport");
  await act(async () => root.unmount());
  host.remove();
}

try {
  await testResource();
  await testBuckets();
  window.performanceRegression = { ok: true, message: "resource lifecycle and 2000-bucket pagination passed" };
} catch (error) {
  window.performanceRegression = { ok: false, message: String(error instanceof Error ? error.stack : error) };
}
