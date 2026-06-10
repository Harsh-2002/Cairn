import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";
import { Navigate, useLocation } from "react-router";
import {
  ApiError,
  api,
  clearToken,
  hasToken,
  onUnauthorized,
  setToken,
} from "@/lib/api";

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

  const logout = useCallback(() => {
    clearToken();
    setAuthed(false);
  }, []);

  // Any 401 from any request means the session is gone: drop the token so
  // RequireAuth bounces to the login screen.
  useEffect(() => {
    onUnauthorized(logout);
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
