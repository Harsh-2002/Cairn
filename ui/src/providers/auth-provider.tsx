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
import {
  ApiError,
  api,
  clearToken,
  hasToken,
  onUnauthorized,
  setToken,
} from "@/lib/api";
import { stopLive } from "@/lib/live";

interface AuthContextValue {
  authed: boolean;
  /**
   * Exchange an access key + secret for a console session. Verifies the
   * credential is an administrator by calling /overview; throws an ApiError
   * with operator-readable copy on 401/403.
   */
  login: (accessKey: string, secretKey: string) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [authed, setAuthed] = useState<boolean>(hasToken);
  // Read the live authed flag from the 401 handler without re-subscribing it.
  const authedRef = useRef(authed);
  authedRef.current = authed;

  const logout = useCallback(() => {
    // Tear down the live SSE stream explicitly so it doesn't keep minting tickets / reconnecting
    // after the session is gone (views unmount too, but don't rely on that ordering).
    stopLive();
    clearToken();
    setAuthed(false);
  }, []);

  // Any 401 from any request means the session is gone: drop the token so
  // RequireAuth bounces to the login screen. Announce it only when we WERE
  // signed in — a 401 during a login attempt is handled by `login` itself, and
  // would otherwise double up with a spurious "session expired" toast.
  useEffect(() => {
    const onExpired = () => {
      if (authedRef.current) {
        toast.error("Your session expired. Please sign in again.");
      }
      logout();
    };
    onUnauthorized(onExpired);
    return () => onUnauthorized(null);
  }, [logout]);

  const login = useCallback(
    async (accessKey: string, secretKey: string) => {
      // Provisional token; verified (and the admin role with it) by /overview.
      setToken(`${accessKey.trim()}.${secretKey}`);
      try {
        await api.overview();
      } catch (e) {
        clearToken();
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
    },
    [],
  );

  return (
    <AuthContext.Provider value={{ authed, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used inside <AuthProvider>");
  return ctx;
}

/** Route guard: unauthenticated visits bounce to /login. */
export function RequireAuth({ children }: { children: ReactNode }) {
  const { authed } = useAuth();
  const location = useLocation();
  if (!authed) {
    return <Navigate to="/login" replace state={{ from: location.pathname }} />;
  }
  return children;
}
