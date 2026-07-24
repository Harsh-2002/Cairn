// Edit an object's tag set (S3 ?tagging). Loads the current tags on open, lets
// the operator add/edit/remove key-value pairs, and saves the whole set (or
// clears it when empty). Mirrors the S3 limits client-side for fast feedback;
// the server is the final authority and its errors are surfaced.

import { useEffect, useState } from "react";
import { Plus, X } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/primitives/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/primitives/dialog";
import { FieldError } from "@/components/field-error";
import { Input } from "@/components/primitives/input";
import { errorMessage } from "@/lib/api";
import {
  deleteObjectTagging,
  getObjectTagging,
  putObjectTagging,
  type ObjectTag,
} from "@/lib/s3";

const MAX_TAGS = 10;

export function ObjectTagsDialog({
  bucket,
  objectKey,
  open,
  onOpenChange,
  onSaved,
}: {
  bucket: string;
  objectKey: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSaved?: () => void;
}) {
  const [tags, setTags] = useState<ObjectTag[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!open || !objectKey) return;
    let alive = true;
    setLoading(true);
    setError(null);
    getObjectTagging(bucket, objectKey)
      .then((t) => {
        if (alive) setTags(t);
      })
      .catch((e) => {
        if (alive) setError(errorMessage(e, "Could not load tags."));
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [open, bucket, objectKey]);

  function update(i: number, patch: Partial<ObjectTag>) {
    setTags((cur) => cur.map((t, j) => (j === i ? { ...t, ...patch } : t)));
  }

  async function save() {
    const cleaned = tags
      .map((t) => ({ key: t.key.trim(), value: t.value }))
      .filter((t) => t.key !== "");
    if (cleaned.length > MAX_TAGS) {
      toast.error(`At most ${MAX_TAGS} tags per object.`);
      return;
    }
    const keys = new Set(cleaned.map((t) => t.key));
    if (keys.size !== cleaned.length) {
      toast.error("Tag keys must be unique.");
      return;
    }
    setSaving(true);
    try {
      if (cleaned.length === 0) {
        await deleteObjectTagging(bucket, objectKey);
      } else {
        await putObjectTagging(bucket, objectKey, cleaned);
      }
      toast.success("Tags saved");
      onSaved?.();
      onOpenChange(false);
    } catch (e) {
      toast.error(errorMessage(e, "Failed to save tags."));
    } finally {
      setSaving(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={saving ? undefined : onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>Object tags</DialogTitle>
          <DialogDescription className="break-all">
            {objectKey}
          </DialogDescription>
        </DialogHeader>

        {loading ? (
          <p className="py-4 text-sm text-muted-foreground">Loading tags…</p>
        ) : error ? (
          <FieldError className="py-4">{error}</FieldError>
        ) : (
          <div className="space-y-2">
            {tags.length === 0 ? (
              <p className="text-[13px] text-muted-foreground">
                No tags. Add one below.
              </p>
            ) : (
              tags.map((t, i) => (
                <div key={i} className="flex items-center gap-2">
                  <Input
                    aria-label={`Tag ${i + 1} key`}
                    placeholder="Key"
                    value={t.key}
                    className="font-mono"
                    onChange={(e) => update(i, { key: e.target.value })}
                  />
                  <Input
                    aria-label={`Tag ${i + 1} value`}
                    placeholder="Value"
                    value={t.value}
                    className="font-mono"
                    onChange={(e) => update(i, { value: e.target.value })}
                  />
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon"
                    aria-label={`Remove tag ${i + 1}`}
                    onClick={() =>
                      setTags((cur) => cur.filter((_, j) => j !== i))
                    }
                  >
                    <X aria-hidden="true" />
                  </Button>
                </div>
              ))
            )}
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={tags.length >= MAX_TAGS}
              onClick={() => setTags((cur) => [...cur, { key: "", value: "" }])}
            >
              <Plus aria-hidden="true" />
              Add tag
            </Button>
          </div>
        )}

        <DialogFooter>
          <Button
            variant="outline"
            onClick={() => onOpenChange(false)}
            disabled={saving}
          >
            Cancel
          </Button>
          <Button onClick={save} disabled={saving || loading}>
            {saving ? "Saving…" : "Save tags"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
