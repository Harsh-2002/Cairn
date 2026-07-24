import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { Navigate, useLocation } from "react-router";
import { toast } from "sonner";
import { ApiError, api, onUnauthorized } from "@/lib/api";
import { stopLive } from "@/lib/live";

interface AuthContextValue {
  authed: boolean;
  /** True until the initial session probe resolves, so guards don't flash the login screen. */
  checking: boolean;
  /**
   * Exchange an access key + secret for a console session. The server validates the credential,
   * confirms it is an administrator, and sets an httpOnly session cookie; throws an ApiError with
   * operator-readable copy on 401/403.
   */
  login: (accessKey: string, secretKey: string) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [authed, setAuthed] = useState(false);
  const [checking, setChecking] = useState(true);
  // Read the live authed flag from the 401 handler without re-subscribing it.
  const authedRef = useRef(authed);
  authedRef.current = authed;

  const logout = useCallback(() => {
    // Tear down the live SSE stream explicitly so it doesn't keep minting tickets / reconnecting
    // after the session is gone (views unmount too, but don't rely on that ordering).
    stopLive();
    // Best-effort: ask the server to expire the cookie. We don't block the web console on it — the local
    // state is what gates the routes, and DELETE /session always succeeds.
    void api.endSession().catch(() => {});
    setAuthed(false);
  }, []);

  // Probe the existing session once on mount: the cookie is httpOnly, so the only way to know
  // whether we're signed in is to ask the server. A 401 here is expected (signed out) and must not
  // trigger the "session expired" toast — that's why `onExpired` only fires while already authed.
  useEffect(() => {
    let cancelled = false;
    api
      .session()
      .then(() => {
        if (!cancelled) setAuthed(true);
      })
      .catch(() => {
        if (!cancelled) setAuthed(false);
      })
      .finally(() => {
        if (!cancelled) setChecking(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Any 401 from any request while signed in means the session is gone: drop local state so
  // RequireAuth bounces to the login screen. Announce it only when we WERE signed in — a 401 during
  // login or the initial probe is handled by those paths and would otherwise double up with a
  // spurious "session expired" toast.
  useEffect(() => {
    const onExpired = () => {
      if (authedRef.current) {
        toast.error("Your session expired. Please sign in again.");
        logout();
      }
    };
    onUnauthorized(onExpired);
    return () => onUnauthorized(null);
  }, [logout]);

  const login = useCallback(async (accessKey: string, secretKey: string) => {
    try {
      await api.createSession(accessKey.trim(), secretKey);
    } catch (e) {
      if (e instanceof ApiError && e.status === 401) {
        throw new ApiError("Access key or secret key is incorrect.", 401);
      }
      if (e instanceof ApiError && e.status === 403) {
        throw new ApiError(
          "That credential works, but it is not an administrator. Only the root admin can use the console.",
          403,
        );
      }
      throw e;
    }
    setAuthed(true);
  }, []);

  return (
    <AuthContext.Provider value={{ authed, checking, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used inside <AuthProvider>");
  return ctx;
}

/** Route guard: unauthenticated visits bounce to /login (once the session probe has resolved). */
export function RequireAuth({ children }: { children: ReactNode }) {
  const { authed, checking } = useAuth();
  const location = useLocation();
  if (checking) {
    return (
      <div className="flex min-h-screen items-center justify-center text-sm text-muted-foreground">
        Loading…
      </div>
    );
  }
  if (!authed) {
    return <Navigate to="/login" replace state={{ from: location.pathname }} />;
  }
  return children;
}
