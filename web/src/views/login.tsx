// Sign-in screen for the admin console. Renders standalone (no AppShell):
// the server validates the two credential strings and issues an httpOnly session cookie.

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
import { FieldError } from "@/components/field-error";
import { Button } from "@/components/primitives/button";
import { Card, CardContent, CardHeader } from "@/components/primitives/card";
import { Input } from "@/components/primitives/input";
import { Label } from "@/components/primitives/label";

export function Login() {
  const { authed, login } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  // Where to land after signing in: the page a session-expiry bounce came from
  // (captured by RequireAuth), otherwise the overview.
  const from =
    (location.state as { from?: string } | null)?.from ?? "/overview";

  const [showSecret, setShowSecret] = useState(false);
  const [capsOn, setCapsOn] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const accessKeyId = "cairn-username";
  const secretKeyId = "cairn-current-password";
  const capsHintId = useId();

  const accessKeyRef = useRef<HTMLInputElement>(null);
  useEffect(() => {
    document.title = "Sign in — Cairn";
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
    if (busy) return;
    // Password managers can fill the DOM without firing React change events. Read the actual
    // form controls and preserve both strings, including whitespace and punctuation.
    const form = new FormData(e.currentTarget);
    const id = form.get("username");
    const secret = form.get("password");
    setError(null);
    if (typeof id !== "string" || typeof secret !== "string" || !id || !secret) {
      setError("Enter your access key and secret key.");
      return;
    }
    setBusy(true);
    try {
      await login(id, secret);
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
      <Card className="w-full max-w-sm">
        <CardHeader className="gap-1.5">
          <div className="mb-2 flex items-center gap-2">
            <span
              aria-hidden="true"
              className="size-4 rounded-[4px] bg-foreground"
            />
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
          <form
            id="cairn-login"
            method="post"
            action="/api/v1/session"
            autoComplete="on"
            onSubmit={onSubmit}
            className="space-y-4"
            noValidate
          >
            <FieldError>{error}</FieldError>

            <div className="space-y-2">
              <Label htmlFor={accessKeyId}>Access key</Label>
              <Input
                id={accessKeyId}
                name="username"
                ref={accessKeyRef}
                type="text"
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
                  name="password"
                  type={showSecret ? "text" : "password"}
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
      </Card>
    </main>
  );
}
