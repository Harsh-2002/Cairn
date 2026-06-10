import { useEffect, useId, useMemo, useRef, useState } from "react";
import { Info } from "lucide-react";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { JsonEditor } from "@/components/json-editor";
import { Label } from "@/components/ui/label";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Skeleton } from "@/components/ui/skeleton";
import {
  ACTION_GLOSS,
  ACTION_GROUPS,
  LEVELS,
  LEVEL_ACTIONS,
  actionSummary,
  advancedToPolicy,
  grantsAccess,
  policyToPreset,
  presetToPolicy,
  pretty,
  validate,
  type Level,
  type PolicyDoc,
  type Scope,
} from "@/lib/policy";
import { cn } from "@/lib/utils";

type Mode = "builder" | "split" | "code";

interface BuilderModel {
  scope: Scope;
  pickedBuckets: string[];
  level: Level;
  advanced: boolean;
  pickedActions: string[];
}

function docFor(m: BuilderModel): PolicyDoc {
  return m.advanced
    ? advancedToPolicy({
        scope: m.scope,
        buckets: m.pickedBuckets,
        actions: m.pickedActions,
      })
    : presetToPolicy({ scope: m.scope, buckets: m.pickedBuckets, level: m.level });
}

function grantsNothing(m: BuilderModel): boolean {
  return (
    (m.scope === "specific" && m.pickedBuckets.length === 0) ||
    (m.advanced && m.pickedActions.length === 0)
  );
}

/**
 * A policy editor with two synced views: a visual Builder (plain-language
 * choices) and a JSON Code editor, plus Split where editing either side
 * updates the other. Emits the current policy doc via `onChange(docOrNull)` —
 * null means the JSON is invalid OR the selection grants nothing, so the
 * parent disables save and shows why.
 */
export function PermissionBuilder({
  buckets,
  bucketsLoading = false,
  initial = null,
  onChange,
}: {
  buckets: string[];
  bucketsLoading?: boolean;
  initial?: PolicyDoc | null;
  onChange: (doc: PolicyDoc | null) => void;
}) {
  const uid = useId();

  // `initial` is a one-time seed read at mount; parents remount via key= when
  // the target changes, so capturing it once is intentional.
  const seed = useRef(initial).current;
  const seedPreset = useMemo(() => (seed ? policyToPreset(seed) : null), [seed]);

  // Default to the Builder for new users; editing an existing policy starts in
  // Split so the JSON they may already understand stays visible.
  const [mode, setMode] = useState<Mode>(seed ? "split" : "builder");
  const [model, setModel] = useState<BuilderModel>(() =>
    seedPreset?.recognized
      ? {
          scope: seedPreset.scope,
          pickedBuckets: seedPreset.buckets,
          level: seedPreset.level,
          advanced: false,
          pickedActions: [...LEVEL_ACTIONS[seedPreset.level]],
        }
      : {
          scope: "all",
          pickedBuckets: [],
          level: "read",
          advanced: false,
          pickedActions: [...LEVEL_ACTIONS.read],
        },
  );
  // True when the JSON is an unrecognized (but valid) doc — JSON authoritative.
  const [custom, setCustom] = useState(!!seed && !seedPreset?.recognized);
  const [rawText, setRawText] = useState(() =>
    seed ? pretty(seed) : pretty(docFor(model)),
  );
  const [jsonError, setJsonError] = useState<string | null>(null);
  const [bucketFilter, setBucketFilter] = useState("");

  // Emit the initial doc once on mount (or null if the seed grants nothing).
  const emittedOnce = useRef(false);
  useEffect(() => {
    if (emittedOnce.current) return;
    emittedOnce.current = true;
    if (custom) {
      const v = validate(rawText);
      onChange(v.ok && grantsAccess(v.doc) ? v.doc : null);
    } else {
      const doc = docFor(model);
      onChange(!grantsNothing(model) && grantsAccess(doc) ? doc : null);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Builder edits → apply the new model, regenerate JSON, emit.
  function applyBuilder(next: Partial<BuilderModel>) {
    const m: BuilderModel = { ...model, ...next };
    setModel(m);
    setCustom(false);
    setJsonError(null);
    const doc = docFor(m);
    setRawText(pretty(doc));
    onChange(!grantsNothing(m) && grantsAccess(doc) ? doc : null);
  }

  // Code edits → validate, re-derive the builder (best-effort), emit.
  function fromCode(text: string) {
    setRawText(text);
    const v = validate(text);
    if (!v.ok) {
      setJsonError(v.error);
      onChange(null);
      return;
    }
    setJsonError(null);
    const p = policyToPreset(v.doc);
    if (p.recognized) {
      setCustom(false);
      setModel({
        scope: p.scope,
        pickedBuckets: p.buckets,
        level: p.level,
        advanced: false,
        pickedActions: [...LEVEL_ACTIONS[p.level]],
      });
    } else {
      setCustom(true);
    }
    onChange(grantsAccess(v.doc) ? v.doc : null);
  }

  function switchMode(m: Mode) {
    setMode(m);
    // Re-emit from whichever side is now authoritative.
    if (m === "code") fromCode(rawText);
  }

  const noBuckets = model.scope === "specific" && model.pickedBuckets.length === 0;
  const noActions = model.advanced && model.pickedActions.length === 0;
  const builderGrantsNothing = !custom && (noBuckets || noActions);

  // The preset levels grant wildcard patterns (s3:Get*) the per-action gloss
  // can't translate, so each level carries its own plain-language phrasing.
  const LEVEL_PHRASES: Record<Level, string[]> = {
    read: ["See the list of files in a bucket", "Download files"],
    write: [
      "See the list of files in a bucket",
      "Download files",
      "Upload and overwrite files",
      "Delete files",
      "Cancel an in-progress large upload",
    ],
    full: ["Do everything S3 allows on the chosen buckets"],
  };
  const allowPhrases = model.advanced
    ? actionSummary(model.pickedActions)
    : LEVEL_PHRASES[model.level];
  const scopePhrase =
    model.scope === "all"
      ? "every bucket on this server"
      : model.pickedBuckets.length === 0
        ? "no buckets yet"
        : model.pickedBuckets.length === 1
          ? `the bucket ${model.pickedBuckets[0]}`
          : `${model.pickedBuckets.length} buckets`;

  const filter = bucketFilter.trim().toLowerCase();
  const filteredBuckets = filter
    ? buckets.filter((b) => b.toLowerCase().includes(filter))
    : buckets;
  const allFilteredPicked =
    filteredBuckets.length > 0 &&
    filteredBuckets.every((b) => model.pickedBuckets.includes(b));

  function toggleSelectAll() {
    if (allFilteredPicked) {
      const drop = new Set(filteredBuckets);
      applyBuilder({
        pickedBuckets: model.pickedBuckets.filter((b) => !drop.has(b)),
      });
    } else {
      applyBuilder({
        pickedBuckets: [...new Set([...model.pickedBuckets, ...filteredBuckets])],
      });
    }
  }

  const showBuilder = mode !== "code";
  const showCode = mode !== "builder";

  return (
    <div>
      {/* View switch: a toggle group, not a tab-panel set. */}
      <div
        role="group"
        aria-label="Policy editor view"
        className="mb-4 inline-flex rounded-lg bg-muted p-1"
      >
        {(
          [
            ["builder", "Builder"],
            ["split", "Split"],
            ["code", "Code"],
          ] as [Mode, string][]
        ).map(([m, label]) => (
          <button
            key={m}
            type="button"
            aria-pressed={mode === m}
            onClick={() => switchMode(m)}
            className={cn(
              "rounded-md px-3.5 py-1.5 text-sm font-medium transition-colors",
              mode === m
                ? "border bg-background text-foreground"
                : "text-muted-foreground hover:text-foreground",
            )}
          >
            {label}
          </button>
        ))}
      </div>

      <div className={cn("min-w-0", mode === "split" && "grid items-start gap-5 lg:grid-cols-2")}>
        {showBuilder ? (
          <div className="min-w-0 space-y-5">
            {custom ? (
              <Alert role="status">
                <Info aria-hidden="true" />
                <AlertDescription>
                  This policy isn't one the visual builder recognizes, so edit it
                  as JSON in the Code view. Changing a control below will replace
                  it.
                </AlertDescription>
              </Alert>
            ) : null}

            <fieldset className="space-y-2.5">
              <legend className="mb-1.5 text-[13px] font-medium text-muted-foreground">
                Which buckets
              </legend>
              <RadioGroup
                value={model.scope}
                onValueChange={(v) => applyBuilder({ scope: v as Scope })}
                className="flex flex-wrap gap-2"
              >
                {(
                  [
                    ["all", "All buckets"],
                    ["specific", "Specific buckets"],
                  ] as [Scope, string][]
                ).map(([s, label]) => (
                  <Label
                    key={s}
                    className={cn(
                      "flex cursor-pointer items-center gap-2 rounded-full border px-3.5 py-2 text-sm",
                      model.scope === s
                        ? "border-foreground"
                        : "text-muted-foreground",
                    )}
                  >
                    <RadioGroupItem value={s} className="sr-only" />
                    {label}
                  </Label>
                ))}
              </RadioGroup>

              {model.scope === "specific" ? (
                bucketsLoading ? (
                  <div className="space-y-2 rounded-md border p-3" role="status" aria-label="Loading buckets">
                    <Skeleton className="h-4 w-2/3" />
                    <Skeleton className="h-4 w-1/2" />
                  </div>
                ) : buckets.length === 0 ? (
                  <p className="rounded-md border bg-muted/50 px-3 py-2.5 text-[13px] text-muted-foreground" role="status">
                    There are no buckets yet. Create a bucket first, then come
                    back to scope this user to it.
                  </p>
                ) : (
                  <div className="space-y-2">
                    {buckets.length > 6 ? (
                      <div className="flex items-center gap-2">
                        <Input
                          type="search"
                          value={bucketFilter}
                          onChange={(e) => setBucketFilter(e.target.value)}
                          placeholder="Filter buckets"
                          aria-label="Filter buckets"
                          className="h-8"
                        />
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          onClick={toggleSelectAll}
                          disabled={filteredBuckets.length === 0}
                          className="shrink-0"
                        >
                          {allFilteredPicked ? "Clear shown" : "Select shown"}
                        </Button>
                      </div>
                    ) : null}
                    <div
                      role="group"
                      aria-label="Buckets this user may use"
                      className="max-h-44 space-y-1 overflow-auto rounded-md border p-2"
                    >
                      {filteredBuckets.length === 0 ? (
                        <p className="px-1 py-0.5 text-[13px] text-muted-foreground">
                          No buckets match “{bucketFilter}”.
                        </p>
                      ) : (
                        filteredBuckets.map((b) => (
                          <Label
                            key={b}
                            className="flex cursor-pointer items-center gap-2 rounded px-1.5 py-1 font-normal hover:bg-accent"
                          >
                            <Checkbox
                              checked={model.pickedBuckets.includes(b)}
                              onCheckedChange={() =>
                                applyBuilder({
                                  pickedBuckets: model.pickedBuckets.includes(b)
                                    ? model.pickedBuckets.filter((x) => x !== b)
                                    : [...model.pickedBuckets, b],
                                })
                              }
                            />
                            <span className="font-mono text-[13px]">{b}</span>
                          </Label>
                        ))
                      )}
                    </div>
                    {noBuckets ? (
                      <p className="text-[13px] text-destructive" role="alert">
                        Pick at least one bucket, or switch to All buckets. With
                        none selected this user gets no access.
                      </p>
                    ) : (
                      <p className="text-[13px] text-muted-foreground">
                        {model.pickedBuckets.length} selected.
                      </p>
                    )}
                  </div>
                )
              ) : null}
            </fieldset>

            <fieldset className="space-y-2.5">
              <legend className="mb-1.5 text-[13px] font-medium text-muted-foreground">
                What they can do
              </legend>
              <Label className="flex cursor-pointer items-center gap-2 font-normal">
                <Checkbox
                  checked={model.advanced}
                  onCheckedChange={(v) => {
                    const on = v === true;
                    applyBuilder({
                      advanced: on,
                      pickedActions:
                        on && model.pickedActions.length === 0
                          ? [...LEVEL_ACTIONS[model.level]]
                          : model.pickedActions,
                    });
                  }}
                />
                <span className="text-sm">Advanced: pick individual actions</span>
              </Label>

              {!model.advanced ? (
                <RadioGroup
                  value={model.level}
                  onValueChange={(v) => {
                    const level = v as Level;
                    applyBuilder({
                      level,
                      pickedActions: model.advanced
                        ? [...LEVEL_ACTIONS[level]]
                        : model.pickedActions,
                    });
                  }}
                  className="gap-2"
                >
                  {LEVELS.map((l) => (
                    <Label
                      key={l.id}
                      className={cn(
                        "flex cursor-pointer flex-col items-start gap-0.5 rounded-lg border px-3.5 py-2.5",
                        model.level === l.id ? "border-foreground" : "",
                      )}
                    >
                      <span className="flex items-center gap-2 text-sm font-medium">
                        <RadioGroupItem value={l.id} />
                        {l.label}
                      </span>
                      <span className="pl-6 text-[13px] font-normal text-muted-foreground">
                        {l.hint}
                      </span>
                    </Label>
                  ))}
                </RadioGroup>
              ) : (
                <div className="space-y-3">
                  <div className="grid gap-x-5 gap-y-3 sm:grid-cols-2">
                    {ACTION_GROUPS.map((g) => (
                      <div key={g.label} className="space-y-1">
                        <p className="text-[13px] font-medium text-muted-foreground">
                          {g.label}
                        </p>
                        {g.actions.map((a) => (
                          <Label
                            key={a}
                            className="flex cursor-pointer items-start gap-2 rounded px-1 py-1 font-normal hover:bg-accent"
                          >
                            <Checkbox
                              className="mt-0.5"
                              checked={model.pickedActions.includes(a)}
                              onCheckedChange={() =>
                                applyBuilder({
                                  pickedActions: model.pickedActions.includes(a)
                                    ? model.pickedActions.filter((x) => x !== a)
                                    : [...model.pickedActions, a],
                                })
                              }
                            />
                            <span className="flex flex-col leading-tight">
                              <span className="text-sm">{ACTION_GLOSS[a] ?? a}</span>
                              <span className="font-mono text-[11px] text-muted-foreground">
                                {a}
                              </span>
                            </span>
                          </Label>
                        ))}
                      </div>
                    ))}
                  </div>
                  {noActions ? (
                    <p className="text-[13px] text-destructive" role="alert">
                      Pick at least one action. With none selected this user gets
                      no access.
                    </p>
                  ) : null}
                </div>
              )}
            </fieldset>

            {/* Running, plain-language summary so the JSON is never the only
                thing explaining intent. */}
            {!custom ? (
              <div
                aria-live="polite"
                className="rounded-lg border bg-muted/50 px-3.5 py-3"
              >
                <p className="text-[13px] font-medium text-muted-foreground">
                  This lets the user
                </p>
                {builderGrantsNothing ? (
                  <p className="mt-1 text-sm text-muted-foreground">
                    Nothing yet.{" "}
                    {noBuckets ? "Pick at least one bucket" : "Pick at least one action"}{" "}
                    to grant access.
                  </p>
                ) : (
                  <>
                    <ul className="mt-1.5 list-disc space-y-0.5 pl-5 text-sm">
                      {allowPhrases.map((phrase) => (
                        <li key={phrase}>{phrase}</li>
                      ))}
                    </ul>
                    <p className="mt-1.5 text-[13px] text-muted-foreground">
                      on {scopePhrase}.
                    </p>
                  </>
                )}
              </div>
            ) : null}
          </div>
        ) : null}

        {showCode ? (
          <div className={cn("min-w-0", mode === "code" ? "" : "lg:sticky lg:top-20")}>
            <Label
              htmlFor={`${uid}-json`}
              className="mb-1.5 text-[13px] text-muted-foreground"
            >
              Policy JSON
            </Label>
            <JsonEditor
              value={rawText}
              onChange={fromCode}
              error={jsonError}
              label="Policy JSON"
              rows={mode === "code" ? 16 : 14}
            />
          </div>
        ) : null}
      </div>
    </div>
  );
}
