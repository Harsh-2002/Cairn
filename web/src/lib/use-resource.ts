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
 * each generation allows one active load and one queued refresh. Cleanup
 * invalidates its responses and drops queued work.
 */
export function useResource<T>(
  load: () => Promise<T>,
  deps: DependencyList,
): Resource<T> {
  const [data, setData] = useState<T | undefined>(undefined);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const loadRef = useRef(load);
  loadRef.current = load;
  const refreshRef = useRef<(() => void) | null>(null);

  useEffect(() => {
    // State belongs to this dependency generation. Old requests may still finish,
    // but cannot publish data or start queued work after cleanup (also in StrictMode).
    let disposed = false;
    let running = false;
    let queued = false;
    let hasData = false;
    setData(undefined);
    setError(null);
    setLoading(true);
    setRefreshing(false);

    async function run() {
      if (disposed) return;
      if (running) {
        queued = true;
        return;
      }
      running = true;
      setLoading(!hasData);
      setRefreshing(hasData);
      setError(null);
      try {
        const next = await loadRef.current();
        if (disposed) return;
        hasData = true;
        setData(next);
      } catch (e) {
        if (disposed) return;
        setError(errorMessage(e, "Couldn't load this. Refresh to try again."));
      } finally {
        running = false;
        if (!disposed) {
          if (queued) {
            queued = false;
            void run();
          } else {
            setLoading(false);
            setRefreshing(false);
          }
        }
      }
    }

    refreshRef.current = () => { void run(); };
    void run();
    return () => {
      disposed = true;
      queued = false;
      refreshRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  const refresh = useCallback(() => {
    refreshRef.current?.();
  }, []);

  return { data, error, loading, refreshing, refresh };
}
