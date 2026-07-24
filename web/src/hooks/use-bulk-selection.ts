// Multi-select state for list tables: a Set of row ids plus the toggle/select-all/clear helpers
// every bulk-action surface needs. Extracted from the object browser so Buckets and Users share the
// exact same selection semantics (and the same BulkBar above the table).

import { useCallback, useMemo, useState } from "react";

export function useBulkSelection() {
  const [selected, setSelected] = useState<Set<string>>(new Set());

  const toggle = useCallback((id: string) => {
    setSelected((cur) => {
      const next = new Set(cur);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const setAll = useCallback((ids: string[], on: boolean) => {
    setSelected(on ? new Set(ids) : new Set());
  }, []);

  const clear = useCallback(() => setSelected(new Set()), []);

  return useMemo(
    () => ({
      selected,
      count: selected.size,
      has: (id: string) => selected.has(id),
      toggle,
      setAll,
      clear,
    }),
    [selected, toggle, setAll, clear],
  );
}
