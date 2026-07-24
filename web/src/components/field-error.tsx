// One source for inline field/validation errors. Replaces the
// `role="alert" text-[13px] text-destructive` recipe that was copy-pasted across
// dialogs and forms (and had already drifted to `text-sm` in one place). Renders
// nothing when there is no message, so call sites can pass a nullable string.

import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

export function FieldError({
  children,
  className,
  id,
}: {
  children?: ReactNode;
  className?: string;
  /** Optional id so an input can point at this message via `aria-describedby`. */
  id?: string;
}) {
  if (!children) return null;
  return (
    <p
      id={id}
      role="alert"
      className={cn("text-[13px] text-destructive", className)}
    >
      {children}
    </p>
  );
}
