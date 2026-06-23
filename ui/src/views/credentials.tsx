// Mint STS-style temporary session credentials (ARCH 14.6). The operator picks a lifetime and a
// scoped policy (via the shared PermissionBuilder), and receives a temporary access key, secret,
// and session token shown exactly once. A session is least-privilege: it can do only what its
// policy grants and never inherits the parent admin's owner/admin bypass.

import { useEffect, useState } from "react";
import { KeyRound } from "lucide-react";
import { toast } from "sonner";
import { api, ApiError, errorMessage } from "@/lib/api";
import { count, relTime, whenMs } from "@/lib/format";
import { summarizePolicy, type PolicyDoc } from "@/lib/policy";
import { useResource } from "@/lib/use-resource";
import { useLiveTopic } from "@/lib/live";
import type { MintSessionResp } from "@/lib/types";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { CredentialsPanel } from "@/components/credentials-panel";
import { DataTable, SkeletonRows, type Column } from "@/components/data-table";
import { EmptyState } from "@/components/empty-state";
import { ErrorAlert } from "@/components/error-alert";
import { Page, PageHeader } from "@/components/page-header";
import { PermissionBuilder } from "@/components/permission-builder";
import { StatusBadge } from "@/components/status-badge";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { TableCell, TableRow } from "@/components/ui/table";
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

const SESSION_COLUMNS: Column[] = [
  { key: "key", label: "Access key" },
  { key: "scope", label: "Scope" },
  { key: "created", label: "Created" },
  { key: "expires", label: "Expires" },
  { key: "actions", label: "Actions", srOnly: true },
];

/** "in 52m" / "in 3h" / "in 2d" for a future expiry (the moment access is cut off). */
function expiresIn(ms: number): string {
  const s = Math.max(0, Math.floor((ms - Date.now()) / 1000));
  if (s < 60) return `in ${s}s`;
  const m = Math.round(s / 60);
  if (m < 60) return `in ${m}m`;
  const h = Math.round(m / 60);
  if (h < 24) return `in ${h}h`;
  return `in ${Math.round(h / 24)}d`;
}

/**
 * A mint failure, classified by cause so the operator knows what to do next. A
 * rejected policy (4xx) needs an edit, not a retry of the same document; a
 * network blip or a server fault (status 0 / 5xx) is transient and worth retrying.
 * In every case the draft below is left untouched, so "adjust and mint again" works.
 */
type MintError = { title: string; message: string; retryable: boolean };

function classifyMintError(e: unknown): MintError {
  if (e instanceof ApiError) {
    // status 0: fetch never reached the server. Retrying the same request can succeed.
    if (e.status === 0) {
      return {
        title: "Couldn't reach the server",
        message:
          "The mint request didn't get through. Check your connection, then retry — your policy below is unchanged.",
        retryable: true,
      };
    }
    // 5xx: the server faulted. Usually transient.
    if (e.status >= 500) {
      return {
        title: "Server error",
        message: `The server couldn't mint the credential (${e.status}). This is usually transient — retry in a moment. Your policy below is unchanged.`,
        retryable: true,
      };
    }
    // 403: the account isn't permitted to mint. Retrying won't change that.
    if (e.status === 403) {
      return {
        title: "Not allowed to mint",
        message:
          e.message ||
          "Your account can't mint temporary credentials. Ask an administrator.",
        retryable: false,
      };
    }
    // Any other 4xx (typically 400): the policy itself was rejected. Surface the
    // server's reason and point at the editor — retrying the same document fails again.
    const reason = e.message.replace(/[.\s]+$/, "");
    return {
      title: "This policy can't be minted",
      message: `${reason} — adjust the scoped policy below, then mint again.`,
      retryable: false,
    };
  }
  return {
    title: "Mint failed",
    message: errorMessage(e, "Could not mint credential"),
    retryable: true,
  };
}

export function Credentials() {
  const [buckets, setBuckets] = useState<string[]>([]);
  const [bucketsLoading, setBucketsLoading] = useState(true);
  const [duration, setDuration] = useState("3600");
  const [doc, setDoc] = useState<PolicyDoc | null>(null);
  const [minting, setMinting] = useState(false);
  const [error, setError] = useState<MintError | null>(null);
  const [minted, setMinted] = useState<MintSessionResp | null>(null);

  const durationLabel =
    DURATIONS.find((d) => String(d.secs) === duration)?.label ?? "";

  // Active (non-expired) sessions, so an operator can see and revoke what's outstanding.
  const sessions = useResource(() => api.listSessions(), []);
  // Live: sessions expire on their own, so a "credentials" pulse keeps the list current without a
  // manual refresh (e.g. an expired session drops off, a freshly minted one appears).
  useLiveTopic("credentials", sessions.refresh);
  const [revoking, setRevoking] = useState<string | null>(null);
  const [revokeBusy, setRevokeBusy] = useState(false);

  async function confirmRevoke() {
    const id = revoking;
    if (!id || revokeBusy) return;
    setRevokeBusy(true);
    try {
      await api.revokeSession(id);
      toast.success("Session revoked");
      setRevoking(null);
      sessions.refresh();
    } catch (e) {
      toast.error(errorMessage(e, "Could not revoke the session"));
    } finally {
      setRevokeBusy(false);
    }
  }

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
      sessions.refresh();
      toast.success("Temporary credential minted");
    } catch (e) {
      setError(classifyMintError(e));
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
        <ErrorAlert
          title={error.title}
          message={error.message}
          onRetry={error.retryable ? () => mint() : undefined}
        />
      ) : null}

      {minted ? (
        <Card>
          <CardContent className="pt-6">
            <CredentialsPanel
              title="Temporary credential — save it now"
              headingLevel="h2"
              explainer={
                <>
                  Configure your S3 SDK with this access key, secret, and{" "}
                  <span className="font-medium">session token</span>. Valid for{" "}
                  <span className="font-medium">{durationLabel}</span> — until{" "}
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

            {/* Echo the resolved grant + lifetime right at the action, so Mint is a deliberate
                confirm (the page sells least-privilege; the default scope is the widest). */}
            <div className="flex flex-wrap items-center justify-between gap-3 border-t pt-4">
              <p className="text-[13px] text-muted-foreground">
                {doc ? (
                  <>
                    Will mint:{" "}
                    <span className="font-medium text-foreground">
                      {summarizePolicy(doc)}
                    </span>{" "}
                    · {durationLabel}
                  </>
                ) : (
                  "Choose what this credential can access above."
                )}
              </p>
              <Button disabled={!doc || minting} onClick={mint}>
                {minting ? "Minting…" : "Mint credential"}
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Active sessions: what's outstanding right now, with an early-revoke. They expire on their
          own, but an operator can cut one off immediately here. */}
      <Card className="mt-4 gap-4">
        <CardHeader className="gap-1">
          <CardTitle className="text-base">Active sessions</CardTitle>
          <CardDescription>
            {sessions.data
              ? `${count(sessions.data.sessions.length)} temporary credential${
                  sessions.data.sessions.length === 1 ? "" : "s"
                } currently valid.`
              : "Temporary credentials currently valid."}
          </CardDescription>
        </CardHeader>
        <CardContent>
          {sessions.error ? (
            <ErrorAlert
              title="Could not load sessions"
              message={sessions.error}
              onRetry={sessions.refresh}
            />
          ) : sessions.loading ? (
            <DataTable columns={SESSION_COLUMNS} minWidth={620}>
              <SkeletonRows
                rows={2}
                widths={["w-48", "w-16", "w-20", "w-16", "w-8"]}
              />
            </DataTable>
          ) : (sessions.data?.sessions.length ?? 0) === 0 ? (
            <EmptyState
              icon={KeyRound}
              title="No active sessions"
              body="Temporary credentials you mint appear here until they expire."
            />
          ) : (
            <DataTable columns={SESSION_COLUMNS} minWidth={620}>
              {sessions.data!.sessions.map((s) => (
                <TableRow key={s.access_key_id}>
                  <TableCell className="font-mono text-[13px]">
                    <span
                      className="block max-w-[26ch] truncate"
                      title={s.access_key_id}
                    >
                      {s.access_key_id}
                    </span>
                  </TableCell>
                  <TableCell data-label="Scope">
                    <StatusBadge tone={s.scoped ? "positive" : "neutral"}>
                      {s.scoped ? "Scoped" : "Inherits parent"}
                    </StatusBadge>
                  </TableCell>
                  <TableCell
                    data-label="Created"
                    className="text-[13px] text-muted-foreground tabular-nums"
                    title={whenMs(s.created_at_ms)}
                  >
                    {relTime(s.created_at_ms)}
                  </TableCell>
                  <TableCell
                    data-label="Expires"
                    className="text-[13px] text-muted-foreground tabular-nums"
                    title={whenMs(s.expires_at_ms)}
                  >
                    {expiresIn(s.expires_at_ms)}
                  </TableCell>
                  <TableCell className="text-right">
                    <Button
                      variant="destructive-outline"
                      size="sm"
                      onClick={() => setRevoking(s.access_key_id)}
                    >
                      Revoke
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </DataTable>
          )}
        </CardContent>
      </Card>

      <ConfirmDialog
        open={revoking !== null}
        onOpenChange={(open) => {
          if (!open && !revokeBusy) setRevoking(null);
        }}
        title="Revoke session?"
        description={
          <>
            This immediately invalidates the temporary credential{" "}
            <span className="font-mono text-[13px] break-all text-foreground">
              {revoking}
            </span>
            . Any client still using it is denied on its next request.
          </>
        }
        confirmLabel={revokeBusy ? "Revoking…" : "Revoke session"}
        cancelLabel="Keep session"
        destructive
        busy={revokeBusy}
        onConfirm={() => void confirmRevoke()}
      />
    </Page>
  );
}
