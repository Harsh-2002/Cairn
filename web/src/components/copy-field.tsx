import { useId, useRef, useState } from "react";
import { Check, Copy, Eye, EyeOff } from "lucide-react";
import { Button } from "@/components/primitives/button";
import { Label } from "@/components/primitives/label";
import { cn } from "@/lib/utils";

/**
 * Copy text to the clipboard, falling back to the hidden-textarea trick for
 * plain-http origins where `navigator.clipboard` is unavailable (the common
 * self-hosted LAN deployment).
 */
export async function copyText(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {
    /* fall through to the legacy path */
  }
  try {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return ok;
  } catch {
    return false;
  }
}

/**
 * A labelled, read-only, monospace value with a copy button. `secret` masks
 * the value behind a reveal toggle. Copy state is announced via a polite live
 * region; if the clipboard is unavailable the text stays selectable.
 */
export function CopyField({
  label,
  value,
  secret = false,
  className,
}: {
  label: string;
  value: string;
  secret?: boolean;
  className?: string;
}) {
  const id = useId();
  const [copied, setCopied] = useState(false);
  const [revealed, setRevealed] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const masked = secret && !revealed;

  async function onCopy() {
    const ok = await copyText(value);
    if (!ok) return;
    setCopied(true);
    if (timer.current) clearTimeout(timer.current);
    timer.current = setTimeout(() => setCopied(false), 2000);
  }

  return (
    <div className={cn("min-w-0", className)}>
      <Label htmlFor={id} className="mb-1.5 text-[13px] text-muted-foreground">
        {label}
      </Label>
      <div className="flex items-center gap-1.5">
        <input
          id={id}
          readOnly
          value={masked ? "•".repeat(Math.min(24, Math.max(8, value.length))) : value}
          onFocus={(e) => e.currentTarget.select()}
          className="h-8 w-full min-w-0 flex-1 rounded-md border bg-muted/50 px-2.5 font-mono text-[13px] text-foreground outline-none"
        />
        {secret ? (
          <Button
            type="button"
            variant="outline"
            size="icon"
            className="size-9 shrink-0 sm:size-8"
            aria-pressed={revealed}
            aria-label={revealed ? `Hide ${label}` : `Show ${label}`}
            onClick={() => setRevealed((r) => !r)}
          >
            {revealed ? <EyeOff aria-hidden="true" /> : <Eye aria-hidden="true" />}
          </Button>
        ) : null}
        <Button
          type="button"
          variant="outline"
          size="icon"
          className="size-9 shrink-0 sm:size-8"
          aria-label={`Copy ${label}`}
          onClick={onCopy}
        >
          {copied ? (
            <Check aria-hidden="true" className="text-success" />
          ) : (
            <Copy aria-hidden="true" />
          )}
        </Button>
      </div>
      <span aria-live="polite" className="sr-only">
        {copied ? `${label} copied to clipboard` : ""}
      </span>
    </div>
  );
}
