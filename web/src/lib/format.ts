// Small presentation helpers shared across views.

export function bytes(n: number | null | undefined): string {
  if (n === null || n === undefined) return "—";
  const num = Number(n);
  if (!Number.isFinite(num)) return "—";
  if (num === 0) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  const i = Math.min(
    units.length - 1,
    Math.floor(Math.log(num) / Math.log(1024)),
  );
  const v = num / Math.pow(1024, i);
  return `${i === 0 ? v : v.toFixed(2)} ${units[i]}`;
}

/** A transfer rate in BYTES per second → "2.34 MiB/s" (never bits/kbps). */
export function speed(bytesPerSec: number | null | undefined): string {
  if (bytesPerSec === null || bytesPerSec === undefined) return "—";
  const n = Number(bytesPerSec);
  if (!Number.isFinite(n) || n <= 0) return "—";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(n) / Math.log(1024)));
  const v = n / Math.pow(1024, i);
  return `${i === 0 ? Math.round(v) : v.toFixed(1)} ${units[i]}/s`;
}

export function count(n: number | null | undefined): string {
  if (n === null || n === undefined) return "—";
  return Number(n).toLocaleString();
}

/** Compact count for tight spots like chart axis ticks: 1234 → "1.2K", 1.2e9 → "1.2B". Keep
 *  `count()` for tiles and tooltips where the exact figure matters. */
export function compactNum(n: number | null | undefined): string {
  if (n === null || n === undefined) return "—";
  const num = Number(n);
  if (!Number.isFinite(num)) return "—";
  if (Math.abs(num) < 1000) return String(Math.round(num));
  return new Intl.NumberFormat(undefined, {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(num);
}

/** A coarse "time since" for a last-updated cue: "just now", "3m ago", "2h ago", "5d ago". */
export function relTime(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return "";
  const secs = Math.max(0, Math.floor((Date.now() - Number(ms)) / 1000));
  if (secs < 45) return "just now";
  const m = Math.round(secs / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.round(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.round(h / 24)}d ago`;
}

export function ratio(f: number | null | undefined): string {
  if (f === null || f === undefined) return "—";
  const v = Number(f);
  if (!Number.isFinite(v)) return "—";
  return `${v.toFixed(2)}×`;
}

export function whenMs(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return "—";
  const d = new Date(Number(ms));
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString();
}

/** "3d 4h", "2h 10m", "5m", "12s" — for the node card's uptime. */
export function duration(totalSecs: number | null | undefined): string {
  if (totalSecs === null || totalSecs === undefined) return "—";
  const s = Math.max(0, Math.floor(Number(totalSecs)));
  if (!Number.isFinite(s)) return "—";
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m`;
  return `${s}s`;
}
