// Sign-in screen for the admin console. Renders standalone (no AppShell):
// the management API authenticates with a Bearer token of the form
// `<access-key>.<secret>`, which the auth provider assembles and verifies
// against an admin-gated endpoint before letting the session in.

import {
  useEffect,
  useId,
  useRef,
  useState,
  type FormEvent,
  type KeyboardEvent,
} from "react";
import { Navigate, useLocation, useNavigate } from "react-router";
import { Eye, EyeOff } from "lucide-react";
import { errorMessage } from "@/lib/api";
import { useAuth } from "@/providers/auth-provider";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardFooter, CardHeader } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export function Login() {
  const { authed, login } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  // Where to land after signing in: the page a session-expiry bounce came from
  // (captured by RequireAuth), otherwise the overview.
  const from =
    (location.state as { from?: string } | null)?.from ?? "/overview";

  const [accessKey, setAccessKey] = useState("");
  const [secretKey, setSecretKey] = useState("");
  const [showSecret, setShowSecret] = useState(false);
  const [capsOn, setCapsOn] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const accessKeyId = useId();
  const secretKeyId = useId();
  const capsHintId = useId();

  const accessKeyRef = useRef<HTMLInputElement>(null);
  useEffect(() => {
    accessKeyRef.current?.focus();
  }, []);

  // Surface a Caps Lock warning while typing the secret, since it is masked
  // by default and a wrong-case secret fails with no other clue.
  function onSecretModifier(e: KeyboardEvent<HTMLInputElement>) {
    if (typeof e.getModifierState === "function") {
      setCapsOn(e.getModifierState("CapsLock"));
    }
  }

  async function onSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError(null);
    const id = accessKey.trim();
    if (!id || !secretKey) {
      setError("Enter your access key and secret key.");
      return;
    }
    setBusy(true);
    try {
      await login(id, secretKey);
      navigate(from, { replace: true });
    } catch (err) {
      setError(errorMessage(err, "Could not sign in."));
    } finally {
      setBusy(false);
    }
  }

  if (authed) {
    return <Navigate to={from} replace />;
  }

  return (
    <main className="flex min-h-svh items-center justify-center bg-background px-4 py-10">
      <Card className="w-full max-w-sm gap-5 rounded-lg border shadow-none">
        <CardHeader className="gap-1.5">
          <div className="mb-2 flex items-center gap-2">
            <span aria-hidden="true" className="size-4 rounded bg-foreground" />
            <span className="text-sm font-semibold">Cairn</span>
          </div>
          <h1 className="text-lg font-semibold tracking-tight">
            Sign in to the console
          </h1>
          <p className="text-sm text-muted-foreground">
            Manage buckets, users, and storage on this node.
          </p>
        </CardHeader>
        <CardContent>
          <form onSubmit={onSubmit} className="space-y-4" noValidate>
            {error ? (
              <Alert variant="destructive" role="alert">
                <AlertDescription>{error}</AlertDescription>
              </Alert>
            ) : null}

            <div className="space-y-2">
              <Label htmlFor={accessKeyId}>Access key</Label>
              <Input
                id={accessKeyId}
                ref={accessKeyRef}
                type="text"
                value={accessKey}
                onChange={(e) => setAccessKey(e.target.value)}
                placeholder="Your admin access key"
                autoComplete="username"
                autoCapitalize="off"
                autoCorrect="off"
                spellCheck={false}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor={secretKeyId}>Secret key</Label>
              <div className="relative">
                <Input
                  id={secretKeyId}
                  type={showSecret ? "text" : "password"}
                  value={secretKey}
                  onChange={(e) => setSecretKey(e.target.value)}
                  onKeyDown={onSecretModifier}
                  onKeyUp={onSecretModifier}
                  placeholder="Your admin secret key"
                  autoComplete="current-password"
                  autoCapitalize="off"
                  autoCorrect="off"
                  spellCheck={false}
                  className="pr-10"
                  aria-describedby={capsOn ? capsHintId : undefined}
                />
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-sm"
                  className="absolute top-1/2 right-1 size-8 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                  aria-pressed={showSecret}
                  aria-label={
                    showSecret ? "Hide secret key" : "Show secret key"
                  }
                  onClick={() => setShowSecret((v) => !v)}
                >
                  {showSecret ? (
                    <EyeOff aria-hidden="true" className="size-4" />
                  ) : (
                    <Eye aria-hidden="true" className="size-4" />
                  )}
                </Button>
              </div>
              {capsOn ? (
                <p
                  id={capsHintId}
                  role="status"
                  className="text-[13px] text-warning"
                >
                  Caps Lock is on.
                </p>
              ) : null}
            </div>

            <Button
              type="submit"
              className="w-full"
              disabled={busy}
              aria-busy={busy}
            >
              {busy ? "Signing in…" : "Sign in"}
            </Button>
          </form>
        </CardContent>
        <CardFooter>
          <p className="text-xs leading-relaxed text-muted-foreground">
            Use the root administrator access key and secret (the{" "}
            <code className="font-mono">CAIRN_ROOT_*</code> credentials).
          </p>
        </CardFooter>
      </Card>
    </main>
  );
}
