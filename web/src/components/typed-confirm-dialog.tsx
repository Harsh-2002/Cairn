import { useEffect, useId, useState, type ReactNode } from "react";
import {
  AlertDialog,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/primitives/alert-dialog";
import { Button } from "@/components/primitives/button";
import { Input } from "@/components/primitives/input";
import { Label } from "@/components/primitives/label";

/**
 * The high-gravity destructive confirm: the user must type the resource name
 * before the destructive button enables. For bucket deletion and similar
 * irreversible, named operations.
 */
export function TypedConfirmDialog({
  open,
  onOpenChange,
  title,
  description,
  requireText,
  confirmLabel = "Delete",
  cancelLabel = "Cancel",
  busy = false,
  onConfirm,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description: ReactNode;
  /** The exact string the user must type (e.g. the bucket name). */
  requireText: string;
  confirmLabel?: string;
  cancelLabel?: string;
  busy?: boolean;
  onConfirm: () => void;
}) {
  const id = useId();
  const [typed, setTyped] = useState("");
  const match = typed === requireText;

  // Reset the gate every time the dialog opens for a (possibly different) target.
  useEffect(() => {
    if (open) setTyped("");
  }, [open, requireText]);

  return (
    <AlertDialog open={open} onOpenChange={busy ? undefined : onOpenChange}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>{title}</AlertDialogTitle>
          <AlertDialogDescription>{description}</AlertDialogDescription>
        </AlertDialogHeader>
        <div className="space-y-1.5">
          <Label htmlFor={id} className="text-[13px] text-muted-foreground">
            Type <span className="font-mono font-medium text-foreground">{requireText}</span> to
            confirm
          </Label>
          <Input
            id={id}
            value={typed}
            autoComplete="off"
            spellCheck={false}
            className="font-mono"
            onChange={(e) => setTyped(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && match && !busy) {
                e.preventDefault();
                onConfirm();
              }
            }}
          />
        </div>
        <AlertDialogFooter>
          <AlertDialogCancel disabled={busy}>{cancelLabel}</AlertDialogCancel>
          <Button
            variant="destructive"
            disabled={!match || busy}
            onClick={onConfirm}
          >
            {confirmLabel}
          </Button>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
