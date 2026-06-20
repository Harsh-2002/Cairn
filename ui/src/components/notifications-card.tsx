// Event notifications (webhooks) editor for a bucket (ARCH 20.6). Lists the configured webhook
// endpoints and lets the operator add / edit / remove them inline: a URL, the event selectors,
// optional key prefix/suffix filters, and an optional HMAC signing secret. The management API
// replaces the whole list on save, and secrets are write-only — the server preserves an unchanged
// endpoint's secret when the field is left blank (so editing one endpoint never wipes another's).

import { useId, useState } from "react";
import { Bell, Lock, Pencil, Plus, Trash2 } from "lucide-react";
import { toast } from "sonner";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { EmptyState } from "@/components/empty-state";
import { FieldError } from "@/components/field-error";
import { api, errorMessage } from "@/lib/api";
import type { WebhookEndpointInput, WebhookEndpointView } from "@/lib/types";

const EVENT_GROUPS = [
  {
    label: "Object created",
    wildcard: "s3:ObjectCreated:*",
    items: [
      { value: "s3:ObjectCreated:Put", label: "Put (single upload)" },
      { value: "s3:ObjectCreated:Copy", label: "Copy" },
      {
        value: "s3:ObjectCreated:CompleteMultipartUpload",
        label: "Multipart complete",
      },
    ],
  },
  {
    label: "Object removed",
    wildcard: "s3:ObjectRemoved:*",
    items: [
      { value: "s3:ObjectRemoved:Delete", label: "Permanent delete" },
      {
        value: "s3:ObjectRemoved:DeleteMarkerCreated",
        label: "Delete marker created",
      },
    ],
  },
];

type EventGroup = (typeof EVENT_GROUPS)[number];

/** A short human label for a stored event selector, for the list view. */
function eventLabel(sel: string): string {
  if (sel === "s3:*") return "All events";
  if (sel === "s3:ObjectCreated:*") return "All creates";
  if (sel === "s3:ObjectRemoved:*") return "All removes";
  for (const g of EVENT_GROUPS) {
    const it = g.items.find((i) => i.value === sel);
    if (it) return it.label;
  }
  return sel.replace(/^s3:/, "");
}

interface Draft {
  id: string;
  url: string;
  events: string[];
  prefix: string;
  suffix: string;
  secret: string;
  /** True when editing an endpoint that already has a signing secret stored. */
  hadSecret: boolean;
  /** Existing id when editing, so we replace rather than append; null when adding. */
  original: string | null;
}

function blankDraft(): Draft {
  return {
    id: "",
    url: "",
    events: ["s3:ObjectCreated:*"],
    prefix: "",
    suffix: "",
    secret: "",
    hadSecret: false,
    original: null,
  };
}

export function NotificationsCard({
  bucket,
  endpoints,
  onChanged,
}: {
  bucket: string;
  endpoints: WebhookEndpointView[];
  onChanged: () => void;
}) {
  const [draft, setDraft] = useState<Draft | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const idBase = useId();

  // Build the full PUT body. Endpoints other than the one being edited carry `secret: null` so the
  // server preserves their stored secret; the edited/added one carries the draft's values.
  async function persist(next: WebhookEndpointInput[]) {
    setBusy(true);
    setErr(null);
    try {
      if (next.length === 0) await api.clearNotifications(bucket);
      else await api.setNotifications(bucket, { endpoints: next });
      setDraft(null);
      onChanged();
      toast.success("Notifications updated");
    } catch (e) {
      setErr(errorMessage(e, "Could not update notifications"));
    } finally {
      setBusy(false);
    }
  }

  function existingAsInputs(excludeId?: string): WebhookEndpointInput[] {
    return endpoints
      .filter((e) => e.id !== excludeId)
      .map((e) => ({
        id: e.id,
        url: e.url,
        events: e.events,
        prefix: e.prefix,
        suffix: e.suffix,
        secret: null, // null = keep the stored secret
      }));
  }

  function saveDraft() {
    if (!draft) return;
    const id = draft.id.trim();
    if (!id) return setErr("Endpoint id is required");
    if (!/^https?:\/\//.test(draft.url.trim()))
      return setErr("URL must start with http:// or https://");
    if (draft.events.length === 0)
      return setErr("Select at least one event");
    if (
      draft.original === null &&
      endpoints.some((e) => e.id === id)
    )
      return setErr(`An endpoint named "${id}" already exists`);

    const input: WebhookEndpointInput = {
      id,
      url: draft.url.trim(),
      events: draft.events,
      prefix: draft.prefix.trim() || null,
      suffix: draft.suffix.trim() || null,
      // Blank + had a secret → null (preserve). Blank + no secret → null (none). Typed → set.
      secret: draft.secret ? draft.secret : null,
    };
    void persist([...existingAsInputs(draft.original ?? undefined), input]);
  }

  function remove(id: string) {
    void persist(existingAsInputs(id));
  }

  function startEdit(e: WebhookEndpointView) {
    setErr(null);
    setDraft({
      id: e.id,
      url: e.url,
      events: [...e.events],
      prefix: e.prefix ?? "",
      suffix: e.suffix ?? "",
      secret: "",
      hadSecret: e.has_secret,
      original: e.id,
    });
  }

  function toggleEvent(group: EventGroup, value: string) {
    if (!draft) return;
    let evts = draft.events;
    const isWildcard = value === group.wildcard;
    if (evts.includes(value)) {
      evts = evts.filter((v) => v !== value);
    } else if (isWildcard) {
      // Checking the wildcard supersedes the group's specific selectors.
      const specifics = group.items.map((i) => i.value);
      evts = [...evts.filter((v) => !specifics.includes(v)), value];
    } else {
      // Checking a specific selector clears the group wildcard.
      evts = [...evts.filter((v) => v !== group.wildcard), value];
    }
    setDraft({ ...draft, events: evts });
  }

  return (
    <CardContent className="space-y-4">
      {err ? <FieldError>{err}</FieldError> : null}

      {endpoints.length === 0 && !draft ? (
        <EmptyState
          icon={Bell}
          title="No webhook endpoints"
          body="Add an endpoint to POST a JSON event record when objects are created or removed."
        />
      ) : null}

      {endpoints.length > 0 ? (
        <ul className="divide-y rounded-md border">
          {endpoints.map((e) => (
            <li
              key={e.id}
              className="flex items-start justify-between gap-3 p-3"
            >
              <div className="min-w-0 space-y-1.5">
                <div className="flex items-center gap-2">
                  <span className="font-medium">{e.id}</span>
                  {e.has_secret ? (
                    <Lock
                      className="size-3.5 text-muted-foreground"
                      aria-label="HMAC signed"
                    />
                  ) : null}
                </div>
                <div className="truncate font-mono text-[13px] text-muted-foreground">
                  {e.url}
                </div>
                <div className="flex flex-wrap gap-1">
                  {e.events.map((ev) => (
                    <Badge key={ev} variant="secondary" className="font-normal">
                      {eventLabel(ev)}
                    </Badge>
                  ))}
                  {e.prefix ? (
                    <Badge variant="outline" className="font-normal">
                      prefix: {e.prefix}
                    </Badge>
                  ) : null}
                  {e.suffix ? (
                    <Badge variant="outline" className="font-normal">
                      suffix: {e.suffix}
                    </Badge>
                  ) : null}
                </div>
              </div>
              <div className="flex shrink-0 gap-1">
                <Button
                  variant="ghost"
                  size="icon"
                  aria-label={`Edit ${e.id}`}
                  disabled={busy}
                  onClick={() => startEdit(e)}
                >
                  <Pencil className="size-4" />
                </Button>
                <Button
                  variant="ghost"
                  size="icon"
                  aria-label={`Delete ${e.id}`}
                  disabled={busy}
                  onClick={() => remove(e.id)}
                >
                  <Trash2 className="size-4" />
                </Button>
              </div>
            </li>
          ))}
        </ul>
      ) : null}

      {draft ? (
        <div className="space-y-4 rounded-md border p-4">
          <div className="grid gap-3 sm:grid-cols-2">
            <div className="space-y-1.5">
              <Label htmlFor={`${idBase}-id`}>Endpoint id</Label>
              <Input
                id={`${idBase}-id`}
                value={draft.id}
                disabled={draft.original !== null}
                placeholder="image-pipeline"
                onChange={(ev) => setDraft({ ...draft, id: ev.target.value })}
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor={`${idBase}-url`}>Destination URL</Label>
              <Input
                id={`${idBase}-url`}
                value={draft.url}
                placeholder="https://hooks.example.com/cairn"
                onChange={(ev) => setDraft({ ...draft, url: ev.target.value })}
              />
            </div>
          </div>

          <fieldset className="space-y-2">
            <legend className="text-sm font-medium">Events</legend>
            <div className="grid gap-4 sm:grid-cols-2">
              {EVENT_GROUPS.map((g) => {
                const wildcardOn = draft.events.includes(g.wildcard);
                return (
                  <div key={g.label} className="space-y-1.5">
                    <div className="text-[13px] font-medium text-muted-foreground">
                      {g.label}
                    </div>
                    <label className="flex items-center gap-2 text-sm">
                      <Checkbox
                        checked={wildcardOn}
                        onCheckedChange={() => toggleEvent(g, g.wildcard)}
                      />
                      All ({g.wildcard.replace(/^s3:/, "")})
                    </label>
                    {g.items.map((it) => (
                      <label
                        key={it.value}
                        className="flex items-center gap-2 pl-5 text-sm data-[muted=true]:opacity-50"
                        data-muted={wildcardOn}
                      >
                        <Checkbox
                          checked={draft.events.includes(it.value)}
                          disabled={wildcardOn}
                          onCheckedChange={() => toggleEvent(g, it.value)}
                        />
                        {it.label}
                      </label>
                    ))}
                  </div>
                );
              })}
            </div>
          </fieldset>

          <div className="grid gap-3 sm:grid-cols-2">
            <div className="space-y-1.5">
              <Label htmlFor={`${idBase}-prefix`}>Key prefix filter</Label>
              <Input
                id={`${idBase}-prefix`}
                value={draft.prefix}
                placeholder="uploads/"
                onChange={(ev) =>
                  setDraft({ ...draft, prefix: ev.target.value })
                }
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor={`${idBase}-suffix`}>Key suffix filter</Label>
              <Input
                id={`${idBase}-suffix`}
                value={draft.suffix}
                placeholder=".jpg"
                onChange={(ev) =>
                  setDraft({ ...draft, suffix: ev.target.value })
                }
              />
            </div>
          </div>

          <div className="space-y-1.5">
            <Label htmlFor={`${idBase}-secret`}>
              HMAC signing secret{" "}
              <span className="font-normal text-muted-foreground">
                (optional)
              </span>
            </Label>
            <Input
              id={`${idBase}-secret`}
              type="password"
              value={draft.secret}
              placeholder={
                draft.hadSecret
                  ? "•••••• (leave blank to keep current)"
                  : "Sign deliveries with X-Cairn-Signature"
              }
              onChange={(ev) => setDraft({ ...draft, secret: ev.target.value })}
            />
            {draft.hadSecret ? (
              <p className="text-[13px] text-muted-foreground">
                A secret is set. Leave blank to keep it.
              </p>
            ) : null}
          </div>

          <div className="flex justify-end gap-2 border-t pt-3">
            <Button
              variant="ghost"
              disabled={busy}
              onClick={() => {
                setDraft(null);
                setErr(null);
              }}
            >
              Cancel
            </Button>
            <Button disabled={busy} onClick={saveDraft}>
              {draft.original ? "Save endpoint" : "Add endpoint"}
            </Button>
          </div>
        </div>
      ) : (
        <Button
          variant="outline"
          disabled={busy}
          onClick={() => {
            setErr(null);
            setDraft(blankDraft());
          }}
        >
          <Plus className="size-4" /> Add endpoint
        </Button>
      )}
    </CardContent>
  );
}
