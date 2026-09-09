import { type ReactNode } from "react";
import { api, errorMessage } from "@/lib/api";
import { useResource } from "@/lib/use-resource";
import { ErrorAlert } from "@/components/error-alert";

import { EndpointContext } from "@/lib/use-endpoints";

/** Scoped to the authenticated shell; logout discards the endpoint state. */
export function EndpointsProvider({ children }: { children: ReactNode }) {
  const { data, error, refresh } = useResource(api.endpoints, []);
  return (
    <EndpointContext value={data ?? null}>
      {error || data?.issues.length ? (
        <div className="px-4 pt-4 md:px-8">
          <ErrorAlert
            title="Endpoint configuration"
            message={error ? errorMessage(error, "Could not check endpoint configuration.") : data!.issues.map((issue) => issue.message).join(" ")}
            onRetry={refresh}
          />
        </div>
      ) : null}
      {children}
    </EndpointContext>
  );
}
