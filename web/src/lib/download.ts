/** A filename is presentation metadata, never a path or a URL to decode. */
function safeBasename(value: string): string | null {
  const name = (value.split(/[\\/]/).pop() ?? "").replace(/\p{Cc}/gu, "").trim();
  return name && name !== "." && name !== ".." ? name : null;
}

export function objectFilename(key: string): string {
  return safeBasename(key) ?? "download";
}

/** Parse parameters without splitting quoted filenames containing semicolons or escaped quotes. */
function dispositionParameters(header: string): Map<string, string> {
  const fields: string[] = [];
  let field = "";
  let quoted = false;
  let escaped = false;
  for (const c of header) {
    if (c === ";" && !quoted) {
      fields.push(field);
      field = "";
      continue;
    }
    field += c;
    if (escaped) escaped = false;
    else if (c === "\\" && quoted) escaped = true;
    else if (c === '"') quoted = !quoted;
  }
  if (quoted || escaped) return new Map();
  fields.push(field);
  const parameters = new Map<string, string>();
  for (const part of fields.slice(1)) {
    const eq = part.indexOf("=");
    if (eq < 0) continue;
    const name = part.slice(0, eq).trim().toLowerCase();
    if (name !== "filename" && name !== "filename*") continue;
    if (parameters.has(name)) return new Map();
    const value = part.slice(eq + 1).trim();
    if (/^"(?:[^"\\]|\\.)*"$/u.test(value)) {
      parameters.set(name, value.slice(1, -1).replace(/\\(.)/gu, "$1"));
    } else if (/^[!#$%&'*+\-.^_`|~\dA-Za-z]+$/u.test(value)) {
      parameters.set(name, value);
    }
  }
  return parameters;
}

/** RFC 6266: a valid UTF-8 filename* wins; filename is not percent-decoded. */
export function downloadFilename(header: string | null, key: string): string {
  const parameters = dispositionParameters(header ?? "");
  const extended = parameters.get("filename*")?.match(/^UTF-8'[^']*'(.*)$/i);
  if (extended) {
    try {
      const name = safeBasename(decodeURIComponent(extended[1]));
      if (name) return name;
    } catch {
      // Malformed escapes/UTF-8 do not hide a valid ordinary filename or object basename.
    }
  }
  return safeBasename(parameters.get("filename") ?? "") ?? objectFilename(key);
}

export interface ObjectDownload {
  blob: Blob;
  filename: string;
}

export function saveDownload({ blob, filename }: ObjectDownload): void {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  try {
    anchor.click();
  } finally {
    anchor.remove();
    URL.revokeObjectURL(url);
  }
}
