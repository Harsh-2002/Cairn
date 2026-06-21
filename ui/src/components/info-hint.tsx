// A small, accessible "what's this?" affordance for explaining S3 jargon inline. It's a click /
// keyboard-triggered popover (not a hover tooltip) so it works the same on touch and desktop, and
// the content renders in a portal so it escapes any clipping/overflow of the row it sits in.

import type { ReactNode } from "react";
import { Info } from "lucide-react";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";

export function InfoHint({
  label,
  children,
  className,
}: {
  /** Accessible name for the trigger, e.g. "About retention modes". */
  label: string;
  /** The explanation shown in the popover. */
  children: ReactNode;
  className?: string;
}) {
  return (
    <Popover>
      <PopoverTrigger
        type="button"
        aria-label={label}
        className="inline-flex size-4 shrink-0 items-center justify-center rounded-full text-muted-foreground/70 align-middle transition-colors hover:text-foreground"
      >
        <Info className="size-3.5" aria-hidden="true" />
      </PopoverTrigger>
      <PopoverContent
        align="start"
        className={className ?? "w-80 text-[13px] leading-relaxed"}
      >
        {children}
      </PopoverContent>
    </Popover>
  );
}
