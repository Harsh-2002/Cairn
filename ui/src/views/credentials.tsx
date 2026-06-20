// Mint STS-style temporary session credentials (ARCH 14.6). The operator picks a lifetime and a
// scoped policy (via the shared PermissionBuilder), and receives a temporary access key, secret,
// and session token shown exactly once. A session is least-privilege: it can do only what its
// policy grants and never inherits the parent admin's owner/admin bypass.

import { useEffect, useState } from "react";
import { KeyRound } from "lucide-react";
import { toast } from "sonner";
import { api, errorMessage } from "@/lib/api";
import { whenMs } from "@/lib/format";
import type { PolicyDoc } from "@/lib/policy";
import type { MintSessionResp } from "@/lib/types";
import { CredentialsPanel } from "@/components/credentials-panel";
import { ErrorAlert } from "@/components/error-alert";
import { Page, PageHeader } from "@/components/page-header";
import { PermissionBuilder } from "@/components/permission-builder";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const DURATIONS: { label: string; secs: number }[] = [
  { label: "15 minutes", secs: 900 },
  { label: "1 hour", secs: 3600 },
  { label: "4 hours", secs: 14400 },
  { label: "8 hours", secs: 28800 },
  { label: "12 hours", secs: 43200 },
];

export function Credentials() {
  const [buckets, setBuckets] = useState<string[]>([]);
  const [bucketsLoading, setBucketsLoading] = useState(true);
  const [duration, setDuration] = useState("3600");
  const [doc, setDoc] = useState<PolicyDoc | null>(null);
  const [minting, setMinting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [minted, setMinted] = useState<MintSessionResp | null>(null);

  useEffect(() => {
    api
      .listBuckets()
      .then((r) => setBuckets(r.buckets.map((b) => b.name)))
      .catch(() => setBuckets([]))
      .finally(() => setBucketsLoading(false));
  }, []);

  async function mint() {
    if (!doc) return;
    setMinting(true);
    setError(null);
    try {
      const resp = await api.mintSessionCredential({
        duration_secs: Number(duration),
        policy: doc,
      });
      setMinted(resp);
      toast.success("Temporary credential minted");
    } catch (e) {
      setError(errorMessage(e, "Could not mint credential"));
    } finally {
      setMinting(false);
    }
  }

  return (
    <Page>
      <PageHeader
        title="Temporary credentials"
        description="Mint a short-lived, scoped S3 credential (STS-style). Use it with any S3 SDK that sends a session token."
      />

      {error ? (
        <ErrorAlert title="Mint failed" message={error} onRetry={() => mint()} />
      ) : null}

      {minted ? (
        <Card>
          <CardContent className="pt-6">
            <CredentialsPanel
              title="Temporary credential — save it now"
              explainer={
                <>
                  Configure your S3 SDK with this access key, secret, and{" "}
                  <span className="font-medium">session token</span>. It is valid
                  until{" "}
                  <span className="font-medium">
                    {whenMs(minted.expiration_epoch_secs * 1000)}
                  </span>
                  , then access is denied automatically.
                </>
              }
              fields={[
                { label: "Access key ID", value: minted.access_key_id },
                {
                  label: "Secret access key",
                  value: minted.secret_access_key,
                  secret: true,
                },
                {
                  label: "Session token (X-Amz-Security-Token)",
                  value: minted.session_token,
                  secret: true,
                },
              ]}
              doneLabel="Done — mint another"
              onDone={() => setMinted(null)}
            />
          </CardContent>
        </Card>
      ) : (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              <KeyRound aria-hidden="true" className="size-4" /> New temporary
              credential
            </CardTitle>
            <CardDescription>
              The credential can do exactly what the policy below grants — nothing
              more — and expires automatically.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-6">
            <div className="max-w-xs space-y-1.5">
              <Label>Lifetime</Label>
              <Select value={duration} onValueChange={setDuration}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {DURATIONS.map((d) => (
                    <SelectItem key={d.secs} value={String(d.secs)}>
                      {d.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-1.5">
              <Label>Scoped policy</Label>
              <PermissionBuilder
                buckets={buckets}
                bucketsLoading={bucketsLoading}
                onChange={setDoc}
              />
            </div>

            <div className="flex justify-end border-t pt-4">
              <Button disabled={!doc || minting} onClick={mint}>
                {minting ? "Minting…" : "Mint credential"}
              </Button>
            </div>
          </CardContent>
        </Card>
      )}
    </Page>
  );
}
