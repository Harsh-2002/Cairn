import { useEffect, useId, useRef, useState, type ReactNode } from "react";
import { TriangleAlert } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { CopyField } from "@/components/copy-field";

export interface CredentialField {
  label: string;
  value: string;
  secret?: boolean;
}

/**
 * The one-time credentials reveal. High gravity by design: focus moves to the
 * heading when it appears, the secrets are copyable, and Done stays disabled
 * until the operator confirms they saved them. Hosts (dialog or card) should
 * also block dismissal until `onDone` fires.
 */
export function CredentialsPanel({
  title = "Save these credentials now",
  fields,
  explainer,
  doneLabel = "Done — I saved them",
  headingLevel: Heading = "h3",
  onDone,
}: {
  title?: string;
  fields: CredentialField[];
  explainer?: ReactNode;
  doneLabel?: string;
  /** Heading level for the panel title, so the page outline stays correct (default h3 — it sits
   *  under a dialog/card h2; pass h2 when the panel is a top-level section under the page h1). */
  headingLevel?: "h2" | "h3";
  onDone: () => void;
}) {
  const headingRef = useRef<HTMLHeadingElement>(null);
  const checkboxId = useId();
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    headingRef.current?.focus();
  }, []);

  return (
    <section aria-label={title} className="space-y-4">
      <div>
        <Heading
          ref={headingRef}
          tabIndex={-1}
          className="text-base font-semibold tracking-tight outline-none"
        >
          {title}
        </Heading>
      </div>

      <Alert role="alert">
        <TriangleAlert aria-hidden="true" />
        <AlertTitle>Shown only once</AlertTitle>
        <AlertDescription>
          The secrets below are not stored anywhere you can read them again.
          Copy them to a safe place before closing this panel.
        </AlertDescription>
      </Alert>

      <div className="space-y-3">
        {fields.map((f) => (
          <CopyField
            key={f.label}
            label={f.label}
            value={f.value}
            secret={f.secret}
          />
        ))}
      </div>

      {explainer ? (
        <div className="rounded-md border bg-muted/50 px-3 py-2.5 text-[13px] leading-relaxed text-muted-foreground">
          {explainer}
        </div>
      ) : null}

      <div className="flex items-center gap-2">
        <Checkbox
          id={checkboxId}
          checked={saved}
          onCheckedChange={(v) => setSaved(v === true)}
        />
        <label htmlFor={checkboxId} className="text-sm">
          I have saved these credentials
        </label>
      </div>

      <Button type="button" disabled={!saved} onClick={onDone}>
        {doneLabel}
      </Button>
    </section>
  );
}
