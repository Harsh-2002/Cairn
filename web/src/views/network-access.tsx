// Read-only network configuration for this Cairn node. Configuration remains
// environment-owned; this view makes the exact listener and public addresses
// available without crowding the operational Overview.

import { CopyField } from "@/components/copy-field";
import { ErrorAlert } from "@/components/error-alert";
import { Page, PageHeader } from "@/components/page-header";
import { StatusBadge } from "@/components/status-badge";
import { Card, CardContent } from "@/components/primitives/card";
import { Skeleton } from "@/components/primitives/skeleton";
import { api } from "@/lib/api";
import { useEndpoints } from "@/lib/use-endpoints";
import { useResource } from "@/lib/use-resource";

function AddressField({
  label,
  value,
  source,
}: {
  label: string;
  value: string | null;
  source?: "Configured" | "Inferred from this request";
}) {
  if (value) {
    return (
      <div>
        <CopyField label={label} value={value} />
        {source ? (
          <p className="mt-1.5 text-xs text-muted-foreground">{source}</p>
        ) : null}
      </div>
    );
  }

  return (
    <div className="min-w-0">
      <p className="mb-1.5 text-[13px] text-muted-foreground">{label}</p>
      <p className="flex h-8 items-center rounded-md border bg-muted/50 px-2.5 text-[13px] text-muted-foreground">
        Not configured
      </p>
    </div>
  );
}

export function NetworkAccess() {
  const { data, error, loading, refresh } = useResource(api.system, []);
  const { status } = useEndpoints();
  const consoleDisabled = ["", "off", "none", "disabled"].includes(
    data?.console_addr.trim().toLowerCase() ?? "",
  );
  const apiPublicUrl = data?.api_public_url ?? status?.api_url ?? null;
  const consolePublicUrl =
    data?.console_public_url ?? status?.console_url ?? null;
  const apiPublicSource = data?.api_public_url
    ? "Configured"
    : status?.api_url
      ? "Inferred from this request"
      : undefined;
  const consolePublicSource = data?.console_public_url
    ? "Configured"
    : status?.console_url
      ? "Inferred from this request"
      : undefined;

  return (
    <Page>
      <PageHeader
        title="Network & Access"
        description="Listener addresses, public endpoints, and transport security for this node."
      />

      {error ? (
        <ErrorAlert
          title="Could not load network and access details"
          message={error}
          onRetry={refresh}
        />
      ) : null}

      {loading ? (
        <div>
          <p className="sr-only" role="status">
            Loading network and access details…
          </p>
          <Skeleton className="h-80 rounded-lg" aria-hidden="true" />
        </div>
      ) : data ? (
        <Card className="gap-0">
          <CardContent>
            <section className="pb-6" aria-labelledby="listen-addresses">
              <div className="mb-4">
                <h2 id="listen-addresses" className="font-semibold">
                  Listen addresses
                </h2>
                <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
                  Where Cairn accepts API and console traffic on this host.
                </p>
              </div>
              <div className="grid gap-4 sm:grid-cols-2">
                <AddressField label="API listen address" value={data.api_addr} />
                <div>
                  <AddressField
                    label="Console listen address"
                    value={data.console_addr}
                  />
                  {consoleDisabled ? (
                    <p className="mt-1.5 text-xs text-muted-foreground">
                      The web console listener is disabled.
                    </p>
                  ) : null}
                </div>
              </div>
            </section>

            <section className="border-t py-6" aria-labelledby="public-urls">
              <div className="mb-4">
                <h2 id="public-urls" className="font-semibold">
                  Public URLs
                </h2>
                <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
                  Addresses clients and browsers use to reach Cairn. Each value
                  identifies whether it was configured explicitly or inferred
                  for this session.
                </p>
              </div>
              <div className="grid gap-4 sm:grid-cols-2">
                <AddressField
                  label="API public URL"
                  value={apiPublicUrl}
                  source={apiPublicSource}
                />
                <AddressField
                  label="Console public URL"
                  value={consolePublicUrl}
                  source={consolePublicSource}
                />
              </div>
            </section>

            <section className="border-t pt-6" aria-labelledby="transport-security">
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div>
                  <h2 id="transport-security" className="font-semibold">
                    Transport security
                  </h2>
                  <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
                    Native TLS means Cairn terminates TLS itself. HTTPS may
                    instead terminate at a trusted reverse proxy.
                  </p>
                </div>
                <StatusBadge tone={data.tls ? "positive" : "neutral"}>
                  Native TLS {data.tls ? "enabled" : "disabled"}
                </StatusBadge>
              </div>
            </section>
          </CardContent>
        </Card>
      ) : null}
    </Page>
  );
}
