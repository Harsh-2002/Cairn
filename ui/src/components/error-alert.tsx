// The one load-error surface. Renders the destructive Alert with a consistent
// icon, spacing, and an optional "Try again" affordance, so every view reports a
// failed fetch identically. Render it ABOVE retained content (non-destructive
// refresh keeps stale data on screen), never as an early-return replacement.

import { CircleAlert } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export function ErrorAlert({
  title = "Something went wrong",
  message,
  onRetry,
  className,
}: {
  title?: string;
  message: string;
  onRetry?: () => void;
  className?: string;
}) {
  return (
    <Alert variant="destructive" className={cn("mb-4", className)}>
      <CircleAlert aria-hidden="true" />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>
        <span>{message}</span>
        {onRetry ? (
          <Button
            variant="outline"
            size="sm"
            onClick={onRetry}
            className="mt-1"
          >
            Try again
          </Button>
        ) : null}
      </AlertDescription>
    </Alert>
  );
}
