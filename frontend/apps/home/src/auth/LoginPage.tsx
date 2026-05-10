import { useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { PageBanner } from "@/components/layout/PageBanner";
import { useAuth } from "@shared/auth/useAuth";

export function LoginPage() {
  const auth = useAuth();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [info, setInfo] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function handlePasswordLogin() {
    setError(null);
    setInfo(null);
    setLoading(true);
    try {
      await auth.signInPassword(email, password);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to sign in");
    } finally {
      setLoading(false);
    }
  }

  async function handleMagicLink() {
    setError(null);
    setInfo(null);
    setLoading(true);
    try {
      await auth.signInMagicLink(email);
      setInfo("Magic link sent. Check your email.");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to send magic link");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="mx-auto flex w-full max-w-[720px] flex-col gap-6">
      <PageBanner
        eyebrow="Auth"
        title="Sign In"
        description="Use email/password or email magic link."
      />
      <section className="rounded-2xl border border-blue-100 bg-white/90 p-5 shadow-soft space-y-4">
        <Input
          placeholder="Email"
          value={email}
          onChange={(event) => setEmail(event.target.value)}
          type="email"
        />
        <Input
          placeholder="Password"
          value={password}
          onChange={(event) => setPassword(event.target.value)}
          type="password"
        />
        <div className="flex flex-wrap gap-3">
          <Button onClick={handlePasswordLogin} disabled={loading || !email || !password}>
            Sign In
          </Button>
          <Button variant="secondary" onClick={handleMagicLink} disabled={loading || !email}>
            Send Magic Link
          </Button>
        </div>
        {error ? <p className="text-sm text-rose-700">{error}</p> : null}
        {info ? <p className="text-sm text-emerald-700">{info}</p> : null}
      </section>
    </div>
  );
}
