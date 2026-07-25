// Object preview: type detection + safe-rendering policy for the in-console viewer.
//
// SECURITY (audit #13): the console must NEVER let object bytes become *active* content in its own
// origin — a stored `text/html` or `image/svg+xml` object would otherwise run script as the console.
// So this module classifies a key into a `PreviewKind`, and object-preview.tsx renders each kind only
// through a script-inert path:
//   image  -> <img>            (image decode; even SVG loaded via <img> cannot script)
//   video  -> <video controls> (media element; inert)
//   audio  -> <audio controls> (media element; inert)
//   pdf    -> <iframe> whose URL the server is FORCED to label `application/pdf` (+ nosniff), so a
//             mistyped/HTML object is handed to the PDF viewer, never the HTML parser
//   text   -> fetched as text and rendered as inert text nodes (never innerHTML, never navigation)
// Detection is by key extension because object listings carry no content-type; the detected MIME also
// drives the `response-content-type` override so an object stored as octet-stream still renders.

export type PreviewKind =
  | "image"
  | "video"
  | "audio"
  | "pdf"
  | "markdown"
  | "json"
  | "csv"
  | "text"
  | "none";

interface TypeInfo {
  kind: PreviewKind;
  /** Safe MIME to force via response-content-type when the byte source is a media/pdf element. */
  mime: string;
}

// Extension (lowercase, no dot) -> kind + forced MIME. Media/pdf MIMEs must be the real, non-HTML
// type. Text-family kinds fetch bytes as text, so their MIME is never used as a navigation type.
const BY_EXT: Record<string, TypeInfo> = {
  // images
  png: { kind: "image", mime: "image/png" },
  jpg: { kind: "image", mime: "image/jpeg" },
  jpeg: { kind: "image", mime: "image/jpeg" },
  gif: { kind: "image", mime: "image/gif" },
  webp: { kind: "image", mime: "image/webp" },
  avif: { kind: "image", mime: "image/avif" },
  bmp: { kind: "image", mime: "image/bmp" },
  ico: { kind: "image", mime: "image/x-icon" },
  svg: { kind: "image", mime: "image/svg+xml" },
  // video
  mp4: { kind: "video", mime: "video/mp4" },
  m4v: { kind: "video", mime: "video/mp4" },
  webm: { kind: "video", mime: "video/webm" },
  ogv: { kind: "video", mime: "video/ogg" },
  mov: { kind: "video", mime: "video/quicktime" },
  mkv: { kind: "video", mime: "video/x-matroska" },
  // audio
  mp3: { kind: "audio", mime: "audio/mpeg" },
  wav: { kind: "audio", mime: "audio/wav" },
  ogg: { kind: "audio", mime: "audio/ogg" },
  oga: { kind: "audio", mime: "audio/ogg" },
  opus: { kind: "audio", mime: "audio/ogg" },
  flac: { kind: "audio", mime: "audio/flac" },
  m4a: { kind: "audio", mime: "audio/mp4" },
  aac: { kind: "audio", mime: "audio/aac" },
  // documents
  pdf: { kind: "pdf", mime: "application/pdf" },
  // structured text (rendered specially)
  md: { kind: "markdown", mime: "text/plain" },
  markdown: { kind: "markdown", mime: "text/plain" },
  mdx: { kind: "markdown", mime: "text/plain" },
  json: { kind: "json", mime: "text/plain" },
  json5: { kind: "json", mime: "text/plain" },
  csv: { kind: "csv", mime: "text/plain" },
  tsv: { kind: "csv", mime: "text/plain" },
  // plain text + source code (shown as source, never executed)
  txt: { kind: "text", mime: "text/plain" },
  text: { kind: "text", mime: "text/plain" },
  log: { kind: "text", mime: "text/plain" },
  conf: { kind: "text", mime: "text/plain" },
  cfg: { kind: "text", mime: "text/plain" },
  ini: { kind: "text", mime: "text/plain" },
  env: { kind: "text", mime: "text/plain" },
  toml: { kind: "text", mime: "text/plain" },
  yaml: { kind: "text", mime: "text/plain" },
  yml: { kind: "text", mime: "text/plain" },
  xml: { kind: "text", mime: "text/plain" },
  html: { kind: "text", mime: "text/plain" }, // shown as SOURCE — never rendered as a document
  htm: { kind: "text", mime: "text/plain" },
  css: { kind: "text", mime: "text/plain" },
  scss: { kind: "text", mime: "text/plain" },
  less: { kind: "text", mime: "text/plain" },
  js: { kind: "text", mime: "text/plain" },
  mjs: { kind: "text", mime: "text/plain" },
  cjs: { kind: "text", mime: "text/plain" },
  jsx: { kind: "text", mime: "text/plain" },
  ts: { kind: "text", mime: "text/plain" },
  tsx: { kind: "text", mime: "text/plain" },
  py: { kind: "text", mime: "text/plain" },
  rb: { kind: "text", mime: "text/plain" },
  rs: { kind: "text", mime: "text/plain" },
  go: { kind: "text", mime: "text/plain" },
  java: { kind: "text", mime: "text/plain" },
  kt: { kind: "text", mime: "text/plain" },
  kts: { kind: "text", mime: "text/plain" },
  c: { kind: "text", mime: "text/plain" },
  h: { kind: "text", mime: "text/plain" },
  cpp: { kind: "text", mime: "text/plain" },
  cc: { kind: "text", mime: "text/plain" },
  hpp: { kind: "text", mime: "text/plain" },
  cs: { kind: "text", mime: "text/plain" },
  php: { kind: "text", mime: "text/plain" },
  swift: { kind: "text", mime: "text/plain" },
  sh: { kind: "text", mime: "text/plain" },
  bash: { kind: "text", mime: "text/plain" },
  zsh: { kind: "text", mime: "text/plain" },
  fish: { kind: "text", mime: "text/plain" },
  sql: { kind: "text", mime: "text/plain" },
  graphql: { kind: "text", mime: "text/plain" },
  gql: { kind: "text", mime: "text/plain" },
  diff: { kind: "text", mime: "text/plain" },
  patch: { kind: "text", mime: "text/plain" },
  lock: { kind: "text", mime: "text/plain" },
  properties: { kind: "text", mime: "text/plain" },
};

// Extension-less files that are conventionally text.
const TEXT_BY_NAME = new Set([
  "dockerfile",
  "makefile",
  "readme",
  "license",
  "licence",
  "copying",
  "changelog",
  "authors",
  "notice",
  "gitignore",
  "dockerignore",
  "npmrc",
  "editorconfig",
  "gitattributes",
]);

/** Last path segment, trailing slash stripped. Empty for a "folder/" key. */
export function basename(key: string): string {
  const k = key.replace(/\/+$/, "");
  const i = k.lastIndexOf("/");
  return i === -1 ? k : k.slice(i + 1);
}

/** Lowercased extension without the dot, or "" if none. */
export function extname(key: string): string {
  const base = basename(key);
  const i = base.lastIndexOf(".");
  // A leading dot (".env") is a name, not an extension separator.
  return i > 0 ? base.slice(i + 1).toLowerCase() : "";
}

function typeInfo(key: string): TypeInfo | undefined {
  const ext = extname(key);
  if (ext && BY_EXT[ext]) return BY_EXT[ext];
  const nameLower = basename(key).toLowerCase().replace(/^\./, "");
  if (TEXT_BY_NAME.has(nameLower)) return { kind: "text", mime: "text/plain" };
  return undefined;
}

export function previewKindOf(key: string): PreviewKind {
  return typeInfo(key)?.kind ?? "none";
}

export function previewMimeOf(key: string): string {
  return typeInfo(key)?.mime ?? "application/octet-stream";
}

export function isPreviewable(key: string): boolean {
  return previewKindOf(key) !== "none";
}

// Size ceilings. Media/PDF stream via Range so there's no in-memory ceiling for them; text is fetched
// into the DOM, so it's capped. A generous PDF ceiling still guards against an accidental multi-GB
// inline load being framed.
export const TEXT_MAX_BYTES = 2 * 1024 * 1024; // 2 MiB of text/code/json/csv/markdown
export const PDF_MAX_BYTES = 100 * 1024 * 1024; // above this, prefer download over an inline frame

// --- CSV / TSV parsing (client-side, capped) ------------------------------------------------------

export interface ParsedTable {
  rows: string[][];
  truncatedRows: boolean;
  truncatedCols: boolean;
}

const CSV_MAX_ROWS = 500;
const CSV_MAX_COLS = 50;

/**
 * A small RFC-4180-ish parser: handles quoted fields, escaped quotes (""), and CR/LF (incl. inside
 * quotes). Purely structural — every cell is rendered as an inert text node, so malformed input is
 * a display quirk, never a security issue. Caps rows/cols so a huge sheet can't stall the DOM.
 */
export function parseDelimited(text: string, delimiter: string): ParsedTable {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;
  let truncatedRows = false;

  const pushField = () => {
    row.push(field);
    field = "";
  };
  const pushRow = () => {
    pushField();
    rows.push(row);
    row = [];
  };

  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        field += c;
      }
      continue;
    }
    if (c === '"') {
      inQuotes = true;
    } else if (c === delimiter) {
      pushField();
    } else if (c === "\n") {
      pushRow();
      if (rows.length >= CSV_MAX_ROWS) {
        truncatedRows = true;
        break;
      }
    } else if (c === "\r") {
      // swallow; a following \n triggers the row, a lone \r also ends the row
      if (text[i + 1] !== "\n") {
        pushRow();
        if (rows.length >= CSV_MAX_ROWS) {
          truncatedRows = true;
          break;
        }
      }
    } else {
      field += c;
    }
  }
  // Trailing field/row if the text didn't end on a newline.
  if (!truncatedRows && (field.length > 0 || row.length > 0)) pushRow();

  let truncatedCols = false;
  const capped = rows.map((r) => {
    if (r.length > CSV_MAX_COLS) {
      truncatedCols = true;
      return r.slice(0, CSV_MAX_COLS);
    }
    return r;
  });
  return { rows: capped, truncatedRows, truncatedCols };
}
