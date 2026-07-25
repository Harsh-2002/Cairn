// The in-console object preview: a lightbox that renders supported file types inline instead of
// forcing a download. Opened from the bucket browser (object name or the row "Preview" action).
//
// SECURITY (audit #13): object bytes are only ever rendered through script-inert paths —
//   image -> <img>, video -> <video>, audio -> <audio>  (media element contexts cannot script)
//   pdf   -> <iframe> whose URL the server is forced to label application/pdf + nosniff (a mistyped
//            HTML object is handed to the PDF viewer, never the HTML parser)
//   text/json/csv/markdown -> fetched as text and rendered as inert React text nodes / a safe
//            Markdown subset (never innerHTML, never navigation)
// Nothing here navigates the console origin to raw object bytes with their stored content-type.

import { useEffect, useState } from "react";
import {
  ChevronLeft,
  ChevronRight,
  Download,
  File as FileIcon,
  ImageOff,
  Loader2,
  Music,
  Share2,
  TriangleAlert,
  X,
} from "lucide-react";

import { Button } from "@/components/primitives/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from "@/components/primitives/dialog";
import { bytes } from "@/lib/format";
import { renderMarkdown } from "@/lib/markdown";
import {
  basename,
  parseDelimited,
  PDF_MAX_BYTES,
  previewKindOf,
  previewMimeOf,
  type PreviewKind,
  TEXT_MAX_BYTES,
} from "@/lib/preview";
import { getObjectText, previewUrl, type TextSlice } from "@/lib/s3";
import { cn } from "@/lib/utils";

export interface PreviewItem {
  key: string;
  size: number;
  versionId?: string;
}

interface ObjectPreviewProps {
  bucket: string;
  /** The previewable set (files only) for prev/next navigation. */
  items: PreviewItem[];
  index: number;
  open: boolean;
  onIndexChange: (index: number) => void;
  onOpenChange: (open: boolean) => void;
  onDownload: (item: PreviewItem) => void;
  onShare?: (item: PreviewItem) => void;
}

const KIND_LABEL: Record<PreviewKind, string> = {
  image: "Image",
  video: "Video",
  audio: "Audio",
  pdf: "PDF",
  markdown: "Markdown",
  json: "JSON",
  csv: "CSV",
  text: "Text",
  none: "File",
};

/**
 * The dialog takes its shape from what it is showing, rather than forcing every object into one
 * frame: a 16:9 video and an A4 PDF and a 40-second audio clip want very different boxes.
 *
 * `fluid` means the dialog has no fixed height and shrink-wraps its content (media, and the small
 * cards) — the stage caps itself against the viewport instead. Everything else gets a deliberate
 * height because its content scrolls: a document column for prose, a portrait page for PDF, a wide
 * frame for tables and long code lines.
 *
 * For images the shape is derived from the image's own aspect ratio once it loads, so a panorama
 * opens wide and a phone photo opens tall.
 */
interface Shape {
  /** Ideal width in px on a roomy screen. Narrow viewports clamp it to near-full-bleed. */
  ideal: number;
  /** A fixed height for content that scrolls; null shrink-wraps to the content. */
  height: string | null;
  /**
   * For an image, its decoded aspect ratio. The frame is then also capped at the width the image
   * can actually occupy once its height is limited by the viewport, so the dialog hugs the picture
   * instead of leaving an empty gutter down each side of a tall one.
   */
  aspect?: number;
}

function shapeFor(kind: PreviewKind, aspect: number | null): Shape {
  switch (kind) {
    case "image":
      if (aspect === null) return { ideal: 880, height: null };
      if (aspect >= 2.2) return { ideal: 1360, height: null, aspect }; // panorama
      if (aspect >= 1.25) return { ideal: 1180, height: null, aspect }; // landscape
      if (aspect <= 0.8) return { ideal: 700, height: null, aspect }; // portrait
      return { ideal: 820, height: null, aspect }; // roughly square
    case "video":
      // Video is nearly always landscape and sizes itself from its own intrinsic ratio.
      return { ideal: 1180, height: null };
    case "audio":
    case "none":
      // A player and a fallback card need a card, not a full-screen frame.
      return { ideal: 520, height: null };
    case "pdf":
      // Pages are portrait; give it height and let the viewer paginate.
      return { ideal: 980, height: "min(92vh, calc(100dvh - 2rem))" };
    case "markdown":
      // Prose wants a readable measure, not the full width of a monitor.
      return { ideal: 860, height: "min(88vh, calc(100dvh - 2rem))" };
    default:
      // text / code / json / csv — long lines and wide tables benefit from width.
      return { ideal: 1280, height: "min(85vh, calc(100dvh - 2rem))" };
  }
}

/**
 * Geometry as inline style rather than utility classes.
 *
 * The dialog primitive is vendored and ships `sm:max-w-lg`; a media-query utility outranks a plain
 * `max-w-none`, so every per-kind width was silently clamped to 512px — a wide CSV and a panorama
 * were being squeezed into the same narrow column as a phone photo. Inline style settles it without
 * a specificity war, and without hand-editing a file the shadcn CLI regenerates.
 *
 * Width is `min(ideal, 100vw - gutter)`, so the same rule that gives a monitor a 1280px frame gives
 * a 390px phone a near-full-bleed sheet — the scarce axis on a phone is space, not margin.
 */
function geometryFor(shape: Shape): React.CSSProperties {
  // `IMAGE_CHROME` is the header plus the stage padding the picture cannot use; keep it in step with
  // the image's own max-height below.
  const IMAGE_CHROME = "9rem";
  const caps = [`${shape.ideal}px`, "calc(100vw - 2rem)"];
  if (shape.aspect) {
    // The width the image can actually fill once its height is viewport-capped. CSS calc multiplies
    // a length by a unitless number, so this stays correct as the viewport changes — no JS needed.
    caps.push(`calc((100dvh - ${IMAGE_CHROME}) * ${shape.aspect.toFixed(4)} + 2rem)`);
  }
  const width = `min(${caps.join(", ")})`;
  return {
    width,
    maxWidth: width,
    height: shape.height ?? undefined,
    maxHeight: "calc(100dvh - 2rem)",
  };
}

export function ObjectPreview({
  bucket,
  items,
  index,
  open,
  onIndexChange,
  onOpenChange,
  onDownload,
  onShare,
}: ObjectPreviewProps) {
  const current = items[index];
  // An image reports its natural aspect once decoded so the dialog can take that shape. Reset it
  // whenever the shown object changes, or a portrait photo would briefly inherit the last one's box.
  const [imgAspect, setImgAspect] = useState<number | null>(null);
  const currentKey = current?.key;
  useEffect(() => {
    setImgAspect(null);
  }, [currentKey]);

  if (!current) return null;

  const name = basename(current.key);
  const kind = previewKindOf(current.key);
  const hasPrev = index > 0;
  const hasNext = index < items.length - 1;
  const shape = shapeFor(kind, imgAspect);

  function onKeyDown(e: React.KeyboardEvent) {
    // Don't hijack arrows while the user is inside a media control, iframe, or text field.
    const t = e.target as HTMLElement | null;
    // Leave arrows alone inside media controls, iframes, text fields, and the scrollable text pane
    // (so a code/log file can be arrow-scrolled without paging to the next object).
    if (t?.closest("video, audio, input, textarea, iframe, [contenteditable], [data-preview-scroll]"))
      return;
    if (e.key === "ArrowLeft" && hasPrev) {
      e.preventDefault();
      onIndexChange(index - 1);
    } else if (e.key === "ArrowRight" && hasNext) {
      e.preventDefault();
      onIndexChange(index + 1);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        showCloseButton={false}
        onKeyDown={onKeyDown}
        style={geometryFor(shape)}
        className="flex flex-col gap-0 overflow-hidden p-0 transition-[width,height] duration-200 ease-out motion-reduce:transition-none"
      >
        <DialogTitle className="sr-only">Preview: {name}</DialogTitle>
        <DialogDescription className="sr-only">
          Inline preview of {name}. Use the left and right arrow keys to move between files, Escape to
          close.
        </DialogDescription>
        {/* Announce the current file to assistive tech on every paging step (WCAG SC 4.1.3), since
            Radix only announces the dialog title/description at open time. */}
        <p className="sr-only" role="status" aria-live="polite">
          {name}, {KIND_LABEL[kind]}, item {index + 1} of {items.length}
        </p>

        {/* Header: identity + toolbar */}
        <header className="flex shrink-0 items-center gap-3 border-b px-4 py-2.5">
          <div className="min-w-0 flex-1">
            <p className="truncate font-mono text-sm" title={current.key}>
              {name}
            </p>
            <p className="mt-0.5 text-xs text-muted-foreground">
              {KIND_LABEL[kind]} · {bytes(current.size)}
              {items.length > 1 && (
                <>
                  {" · "}
                  {index + 1} of {items.length}
                </>
              )}
            </p>
          </div>
          <div className="flex items-center gap-1">
            <Button
              variant="ghost"
              size="icon-sm"
              className="[@media(pointer:coarse)]:size-11"
              onClick={() => onDownload(current)}
              title="Download"
              aria-label="Download"
            >
              <Download />
            </Button>
            {onShare && (
              <Button
                variant="ghost"
                size="icon-sm"
                className="[@media(pointer:coarse)]:size-11"
                onClick={() => onShare(current)}
                title="Share"
                aria-label="Share"
              >
                <Share2 />
              </Button>
            )}
            <Button
              variant="ghost"
              size="icon-sm"
              className="[@media(pointer:coarse)]:size-11"
              onClick={() => onOpenChange(false)}
              title="Close"
              aria-label="Close preview"
            >
              <X />
            </Button>
          </div>
        </header>

        {/* Stage. A shrink-wrapping shape lets the media set the height (it caps itself against the
            viewport); a fixed-height shape stretches so the scrolling panes fill the frame. */}
        <div
          className={cn(
            "relative flex min-h-0 items-stretch justify-center bg-muted/30",
            shape.height !== null && "flex-1",
          )}
        >
          {items.length > 1 && (
            <>
              <NavButton
                side="left"
                disabled={!hasPrev}
                onClick={() => onIndexChange(index - 1)}
              />
              <NavButton
                side="right"
                disabled={!hasNext}
                onClick={() => onIndexChange(index + 1)}
              />
            </>
          )}
          {/* Remount the stage per object so per-item state (load, fetch, zoom) resets cleanly. */}
          <Stage
            key={`${current.key} ${current.versionId ?? ""}`}
            bucket={bucket}
            item={current}
            kind={kind}
            onDownload={() => onDownload(current)}
            onNaturalSize={setImgAspect}
          />
        </div>
      </DialogContent>
    </Dialog>
  );
}

function NavButton({
  side,
  disabled,
  onClick,
}: {
  side: "left" | "right";
  disabled: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      // aria-disabled (not `disabled`) keeps the control in the tab order at list boundaries, so
      // keyboard focus is never dropped when the button becomes non-actionable.
      aria-disabled={disabled || undefined}
      onClick={() => {
        if (!disabled) onClick();
      }}
      aria-label={side === "left" ? "Previous file" : "Next file"}
      className={cn(
        // 36px reads well with a mouse; a finger needs 44 (WCAG 2.5.5 / platform guidance), so the
        // hit area grows on coarse pointers.
        "absolute top-1/2 z-10 flex size-9 -translate-y-1/2 items-center justify-center rounded-full border bg-background/80 text-foreground shadow-sm backdrop-blur transition-opacity duration-150 ease-out motion-reduce:transition-none [@media(pointer:coarse)]:size-11",
        disabled ? "cursor-default opacity-30" : "hover:bg-background",
        side === "left" ? "left-3" : "right-3",
      )}
    >
      {side === "left" ? <ChevronLeft className="size-5" /> : <ChevronRight className="size-5" />}
    </button>
  );
}

// --- stage dispatch -------------------------------------------------------------------------------

function Stage({
  bucket,
  item,
  kind,
  onDownload,
  onNaturalSize,
}: {
  bucket: string;
  item: PreviewItem;
  kind: PreviewKind;
  onDownload: () => void;
  onNaturalSize: (aspect: number | null) => void;
}) {
  switch (kind) {
    case "image":
      return (
        <ImageStage
          bucket={bucket}
          item={item}
          onDownload={onDownload}
          onNaturalSize={onNaturalSize}
        />
      );
    case "video":
      return <VideoStage bucket={bucket} item={item} onDownload={onDownload} />;
    case "audio":
      return <AudioStage bucket={bucket} item={item} />;
    case "pdf":
      return <PdfStage bucket={bucket} item={item} onDownload={onDownload} />;
    case "text":
    case "json":
    case "csv":
    case "markdown":
      return <TextStage bucket={bucket} item={item} kind={kind} onDownload={onDownload} />;
    default:
      return (
        <StageMessage
          icon={<FileIcon className="size-10" />}
          title="No preview for this file type"
          detail={bytes(item.size)}
          onDownload={onDownload}
        />
      );
  }
}

function StageMessage({
  icon,
  title,
  detail,
  onDownload,
  tone = "muted",
}: {
  icon: React.ReactNode;
  title: string;
  detail?: string;
  onDownload?: () => void;
  tone?: "muted" | "warning";
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-3 p-8 text-center">
      <div className={tone === "warning" ? "text-muted-foreground" : "text-muted-foreground"}>
        {icon}
      </div>
      <div>
        <p className="text-sm font-medium">{title}</p>
        {detail && <p className="mt-0.5 text-xs text-muted-foreground">{detail}</p>}
      </div>
      {onDownload && (
        <Button variant="outline" size="sm" onClick={onDownload}>
          <Download />
          Download
        </Button>
      )}
    </div>
  );
}

function Spinner() {
  return (
    <div className="flex items-center justify-center p-8 text-muted-foreground">
      <Loader2 className="size-6 animate-spin" />
      <span className="sr-only">Loading preview…</span>
    </div>
  );
}

// --- media stages ---------------------------------------------------------------------------------

function ImageStage({
  bucket,
  item,
  onDownload,
  onNaturalSize,
}: {
  bucket: string;
  item: PreviewItem;
  onDownload: () => void;
  onNaturalSize: (aspect: number | null) => void;
}) {
  const [errored, setErrored] = useState(false);
  if (errored) {
    return (
      <StageMessage
        icon={<ImageOff className="size-10" />}
        title="This image couldn't be displayed"
        detail={bytes(item.size)}
        onDownload={onDownload}
      />
    );
  }
  const src = previewUrl(bucket, item.key, {
    mime: previewMimeOf(item.key),
    versionId: item.versionId,
  });
  return (
    <div className="flex w-full items-center justify-center overflow-auto p-4">
      <img
        src={src}
        alt={basename(item.key)}
        onError={() => {
          setErrored(true);
          onNaturalSize(null);
        }}
        // Report the decoded aspect so the dialog takes the image's own shape.
        onLoad={(e) => {
          const el = e.currentTarget;
          if (el.naturalWidth > 0 && el.naturalHeight > 0) {
            onNaturalSize(el.naturalWidth / el.naturalHeight);
          }
        }}
        className="max-h-[calc(100dvh-9rem)] max-w-full object-contain select-none"
        // A checkerboard shows through transparent PNGs/SVGs so alpha reads clearly.
        style={{
          backgroundImage:
            "conic-gradient(from 45deg, var(--muted) 25%, transparent 0 50%, var(--muted) 0 75%, transparent 0)",
          backgroundSize: "16px 16px",
        }}
      />
    </div>
  );
}

function VideoStage({
  bucket,
  item,
  onDownload,
}: {
  bucket: string;
  item: PreviewItem;
  onDownload: () => void;
}) {
  const [errored, setErrored] = useState(false);
  if (errored) {
    return (
      <StageMessage
        icon={<TriangleAlert className="size-10" />}
        title="This video can't be played here"
        detail="Your browser may not support this format. Download it to view locally."
        onDownload={onDownload}
      />
    );
  }
  return (
    <div className="flex w-full items-center justify-center p-4">
      <video
        src={previewUrl(bucket, item.key, {
          mime: previewMimeOf(item.key),
          versionId: item.versionId,
        })}
        controls
        preload="metadata"
        onError={() => setErrored(true)}
        className="max-h-[calc(100dvh-9rem)] w-full rounded-md bg-black"
      >
        <track kind="captions" />
      </video>
    </div>
  );
}

function AudioStage({ bucket, item }: { bucket: string; item: PreviewItem }) {
  return (
    <div className="flex w-full flex-col items-center justify-center gap-5 px-8 py-10">
      <div className="flex size-20 items-center justify-center rounded-2xl border bg-background text-muted-foreground">
        <Music className="size-9" />
      </div>
      <p className="max-w-full truncate px-4 font-mono text-sm" title={item.key}>
        {basename(item.key)}
      </p>
      <audio
        controls
        preload="metadata"
        src={previewUrl(bucket, item.key, {
          mime: previewMimeOf(item.key),
          versionId: item.versionId,
        })}
        className="w-full max-w-md"
      >
        <track kind="captions" />
      </audio>
    </div>
  );
}

function PdfStage({
  bucket,
  item,
  onDownload,
}: {
  bucket: string;
  item: PreviewItem;
  onDownload: () => void;
}) {
  if (item.size > PDF_MAX_BYTES) {
    return (
      <StageMessage
        icon={<FileIcon className="size-10" />}
        title="This PDF is too large to preview inline"
        detail={`${bytes(item.size)} — download to view`}
        onDownload={onDownload}
      />
    );
  }
  // SECURITY (audit #13): the server is FORCED to label this response application/pdf and always
  // stamps x-content-type-options: nosniff (pinned by a server regression test), so an object whose
  // stored bytes are HTML is handed to the PDF viewer, never parsed as a document — verified: an
  // HTML-bytes ".pdf" shows a PDF load error and runs no script.
  //
  // A `sandbox` attribute is deliberately NOT used: the browser's built-in PDF viewer is script-
  // driven, so `sandbox` without `allow-scripts` fails to render the PDF at all, and adding
  // `allow-scripts` alongside `allow-same-origin` would let a framed document escape its own sandbox
  // — sandbox cannot add safety here without breaking the viewer. Broader defense-in-depth for every
  // preview path belongs in a console-wide Content-Security-Policy (frame-src 'self'; object-src
  // 'none'), tracked as a separate server-side change.
  return (
    <iframe
      title={`PDF preview: ${basename(item.key)}`}
      src={previewUrl(bucket, item.key, {
        mime: "application/pdf",
        disposition: "inline",
        versionId: item.versionId,
      })}
      className="h-full w-full border-0 bg-white"
    />
  );
}

// --- text / json / csv / markdown -----------------------------------------------------------------

function TextStage({
  bucket,
  item,
  kind,
  onDownload,
}: {
  bucket: string;
  item: PreviewItem;
  kind: PreviewKind;
  onDownload: () => void;
}) {
  const [slice, setSlice] = useState<TextSlice | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [asSource, setAsSource] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setSlice(null);
    setError(null);
    getObjectText(bucket, item.key, {
      versionId: item.versionId,
      maxBytes: TEXT_MAX_BYTES,
    })
      .then((s) => {
        if (!cancelled) setSlice(s);
      })
      .catch((e: unknown) => {
        if (!cancelled) setError(e instanceof Error ? e.message : "Couldn't load this file.");
      });
    return () => {
      cancelled = true;
    };
  }, [bucket, item.key, item.versionId]);

  if (error) {
    return (
      <StageMessage
        icon={<TriangleAlert className="size-10" />}
        title="Couldn't load this file"
        detail={error}
        onDownload={onDownload}
      />
    );
  }
  if (!slice) return <Spinner />;

  const showMarkdownRendered = kind === "markdown" && !asSource;

  return (
    <div className="flex h-full w-full flex-col">
      {(slice.truncated || kind === "markdown") && (
        <div className="flex shrink-0 items-center justify-between gap-3 border-b bg-muted/40 px-4 py-1.5 text-xs text-muted-foreground">
          <span>
            {slice.truncated
              ? `Showing the first ${bytes(TEXT_MAX_BYTES)} — download for the full file.`
              : "Rendered Markdown"}
          </span>
          {kind === "markdown" && (
            <button
              type="button"
              onClick={() => setAsSource((s) => !s)}
              className="rounded px-1.5 py-0.5 font-medium text-foreground hover:bg-accent"
            >
              {asSource ? "Rendered" : "Source"}
            </button>
          )}
        </div>
      )}
      <div
        className="min-h-0 flex-1 overflow-auto bg-background focus-visible:outline-none"
        /* A focusable, keyboard-scrollable region for long text — a valid WAI-ARIA pattern the lint
           rule doesn't allow-list on role=region. */
        /* eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex */
        tabIndex={0}
        role="region"
        aria-label={basename(item.key)}
        data-preview-scroll
      >
        {showMarkdownRendered ? (
          <div className="mx-auto max-w-[76ch] px-6 py-6">{renderMarkdown(slice.text)}</div>
        ) : kind === "csv" ? (
          <CsvTable text={slice.text} tab={/\.tsv$/i.test(item.key)} />
        ) : (
          <pre className="w-full whitespace-pre px-4 py-3 font-mono text-xs leading-relaxed">
            <code>{kind === "json" ? prettyJson(slice.text) : slice.text}</code>
          </pre>
        )}
      </div>
    </div>
  );
}

function prettyJson(text: string): string {
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text; // not valid JSON (or truncated) — show raw
  }
}

function CsvTable({ text, tab }: { text: string; tab: boolean }) {
  const { rows, truncatedRows, truncatedCols } = parseDelimited(text, tab ? "\t" : ",");
  if (rows.length === 0) {
    return <div className="p-6 text-sm text-muted-foreground">Empty file.</div>;
  }
  const [head, ...body] = rows;
  return (
    <div className="p-4">
      <table className="w-full border-collapse text-xs">
        <thead>
          <tr className="border-b bg-muted/50">
            {head.map((c, i) => (
              <th key={i} className="whitespace-nowrap px-3 py-1.5 text-left font-medium font-mono">
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {body.map((r, ri) => (
            <tr key={ri} className="border-b last:border-0">
              {head.map((_, ci) => (
                <td key={ci} className="whitespace-nowrap px-3 py-1 align-top font-mono text-muted-foreground">
                  {r[ci] ?? ""}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {(truncatedRows || truncatedCols) && (
        <p className="mt-3 text-xs text-muted-foreground">
          {truncatedRows && "Showing the first rows. "}
          {truncatedCols && "Some columns hidden. "}
          Download for the full table.
        </p>
      )}
    </div>
  );
}
