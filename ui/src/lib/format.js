// Small presentation helpers shared across views.

export function bytes(n) {
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

export function count(n) {
  if (n === null || n === undefined) return "—";
  return Number(n).toLocaleString();
}

export function ratio(f) {
  if (f === null || f === undefined) return "—";
  const v = Number(f);
  if (!Number.isFinite(v)) return "—";
  return `${v.toFixed(2)}×`;
}

export function whenMs(ms) {
  if (ms === null || ms === undefined) return "—";
  const d = new Date(Number(ms));
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString();
}
