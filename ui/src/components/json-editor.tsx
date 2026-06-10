import { useId } from "react";
import { CircleAlert, CircleCheck } from "lucide-react";
import { Textarea } from "@/components/ui/textarea";
import { cn } from "@/lib/utils";

/**
 * A monospace JSON textarea with a live validation status line. The status is
 * announced politely so screen-reader users hear validity changes without
 * losing their place in the editor.
 */
export function JsonEditor({
  value,
  onChange,
  error,
  label,
  rows = 14,
  validLabel = "Valid policy document",
  className,
}: {
  value: string;
  onChange: (next: string) => void;
  /** null/undefined = valid; a string = the validation error to show. */
  error?: string | null;
  label: string;
  rows?: number;
  validLabel?: string;
  className?: string;
}) {
  const id = useId();
  const statusId = `${id}-status`;
  const valid = !error;
  return (
    <div className={cn("space-y-1.5", className)}>
      <Textarea
        id={id}
        aria-label={label}
        aria-invalid={valid ? undefined : true}
        aria-describedby={statusId}
        spellCheck={false}
        rows={rows}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="resize-y font-mono text-[13px] leading-relaxed"
      />
      <p
        id={statusId}
        aria-live="polite"
        className={cn(
          "flex items-start gap-1.5 text-[13px]",
          valid ? "text-success" : "text-destructive",
        )}
      >
        {valid ? (
          <CircleCheck aria-hidden="true" className="mt-0.5 size-3.5 shrink-0" />
        ) : (
          <CircleAlert aria-hidden="true" className="mt-0.5 size-3.5 shrink-0" />
        )}
        <span>{valid ? validLabel : error}</span>
      </p>
    </div>
  );
}
