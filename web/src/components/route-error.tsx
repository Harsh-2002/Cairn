import { CircleAlert, RotateCcw } from "lucide-react";
import { isRouteErrorResponse, useRouteError } from "react-router";
import { Button } from "@/components/primitives/button";

function routeErrorMessage(error: unknown): string {
  if (isRouteErrorResponse(error)) {
    if (error.status === 404) return "This console page could not be found.";
    return `The console could not load this page (${error.status}).`;
  }
  return "The console could not load this page. The server and your data are unaffected.";
}

/** Last-resort route boundary, including a failed lazy chunk after a deployment. */
export function RouteError() {
  const error = useRouteError();

  return (
    <main className="flex min-h-svh items-center justify-center bg-background px-4 py-10">
      <section className="w-full max-w-md rounded-lg border p-6" aria-labelledby="route-error-title">
        <CircleAlert aria-hidden="true" className="mb-4 size-6 text-destructive" />
        <h1 id="route-error-title" className="text-lg font-semibold tracking-tight">
          Page unavailable
        </h1>
        <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
          {routeErrorMessage(error)} Reload to fetch the latest console files.
        </p>
        <Button className="mt-5" onClick={() => window.location.reload()}>
          <RotateCcw aria-hidden="true" />
          Reload console
        </Button>
      </section>
    </main>
  );
}
