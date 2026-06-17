// One source for inline field/validation errors. Replaces the
// `role="alert" text-[13px] text-destructive` recipe that was copy-pasted across
// dialogs and forms (and had already drifted to `text-sm` in one place). Renders
// nothing when there is no message, so call sites can pass a nullable string.

import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

export function FieldError({
  children,
  className,
}: {
  children?: ReactNode;
  className?: string;
}) {
  if (!children) return null;
  return (
    <p role="alert" className={cn("text-[13px] text-destructive", className)}>
      {children}
    </p>
  );
}
