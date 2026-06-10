import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type DependencyList,
} from "react";
import { errorMessage } from "./api";

export interface Resource<T> {
  data: T | undefined;
  error: string | null;
  /** True only before the FIRST data arrives — drives skeletons. */
  loading: boolean;
  /** True while re-fetching with stale data still on screen. */
  refreshing: boolean;
  refresh: () => void;
}

/**
 * Load-on-mount + explicit-refresh data fetching, with the console's
 * non-destructive refresh semantics: once data is on screen it stays rendered
 * while a refresh is in flight, so the page never tears down to a skeleton.
 *
 * `deps` re-runs the load from scratch (e.g. a bucket-name route param);
 * stale responses are discarded via a sequence counter.
 */
export function useResource<T>(
  load: () => Promise<T>,
  deps: DependencyList,
): Resource<T> {
  const [data, setData] = useState<T | undefined>(undefined);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const seq = useRef(0);
  const loadRef = useRef(load);
  loadRef.current = load;
  const hasData = useRef(false);

  const run = useCallback(async (fresh: boolean) => {
    const ticket = ++seq.current;
    if (fresh || !hasData.current) {
      hasData.current = false;
      setData(undefined);
      setLoading(true);
      setRefreshing(false);
    } else {
      setRefreshing(true);
    }
    setError(null);
    try {
      const next = await loadRef.current();
      if (ticket !== seq.current) return;
      hasData.current = true;
      setData(next);
    } catch (e) {
      if (ticket !== seq.current) return;
      setError(errorMessage(e, "Request failed."));
    } finally {
      if (ticket === seq.current) {
        setLoading(false);
        setRefreshing(false);
      }
    }
  }, []);

  useEffect(() => {
    void run(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  const refresh = useCallback(() => {
    void run(false);
  }, [run]);

  return { data, error, loading, refreshing, refresh };
}
