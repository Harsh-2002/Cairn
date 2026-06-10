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

export function count(n: number | null | undefined): string {
  if (n === null || n === undefined) return "—";
  return Number(n).toLocaleString();
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
