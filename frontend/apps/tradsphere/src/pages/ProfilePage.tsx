import { Save } from "lucide-react";
import { useEffect, useMemo, useState } from "react";

import { PageBanner } from "@/components/layout/PageBanner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Spinner } from "@/components/ui/spinner";
import { useApiRequest } from "@/hooks/useApiRequest";
import { useAuth } from "@shared/auth/useAuth";

type ProfileDraft = {
  firstName: string;
  lastName: string;
};

function splitFullName(value: string | null | undefined): ProfileDraft {
  const normalized = String(value || "").trim();
  if (!normalized) {
    return { firstName: "", lastName: "" };
  }
  const parts = normalized.split(/\s+/);
  if (parts.length === 1) {
    return { firstName: parts[0], lastName: "" };
  }
  return {
    firstName: parts[0],
    lastName: parts.slice(1).join(" "),
  };
}

function composeFullName(firstName: string, lastName: string): string {
  return [firstName.trim(), lastName.trim()].filter(Boolean).join(" ").trim();
}

function readUserName(auth: ReturnType<typeof useAuth>): ProfileDraft {
  return splitFullName(auth.accessProfile?.user?.fullName || auth.user?.fullName || "");
}

export default function ProfilePage() {
  const auth = useAuth();
  const { requestJson } = useApiRequest();
  const [draft, setDraft] = useState<ProfileDraft>(() => readUserName(auth));
  const [baseline, setBaseline] = useState<ProfileDraft>(() => readUserName(auth));
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const next = readUserName(auth);
    setDraft(next);
    setBaseline(next);
  }, [auth.accessProfile?.user?.fullName, auth.user?.fullName]);

  const fullNamePreview = useMemo(() => composeFullName(draft.firstName, draft.lastName), [draft.firstName, draft.lastName]);
  const canSave = useMemo(() => {
    return (
      Boolean(draft.firstName.trim())
      && Boolean(draft.lastName.trim())
      && (draft.firstName.trim() !== baseline.firstName.trim() || draft.lastName.trim() !== baseline.lastName.trim())
    );
  }, [baseline.firstName, baseline.lastName, draft.firstName, draft.lastName]);

  async function handleSave() {
    if (!canSave) {
      return;
    }
    const normalizedFirstName = draft.firstName.trim();
    const normalizedLastName = draft.lastName.trim();
    const normalizedFullName = composeFullName(normalizedFirstName, normalizedLastName);
    if (!normalizedFirstName || !normalizedLastName || !normalizedFullName) {
      setError("First name and last name are required.");
      return;
    }

    setSaving(true);
    setError(null);
    try {
      const payload = await requestJson("/api/auth/v1/session/me/profile", {
        method: "PATCH",
        body: {
          profile: {
            firstName: normalizedFirstName,
            lastName: normalizedLastName,
            fullName: normalizedFullName,
          },
        },
        successToast: {
          title: "Profile updated",
          message: "Your profile name has been saved.",
        },
      });

      const resolvedData =
        payload && typeof payload === "object" && "data" in (payload as Record<string, unknown>)
          ? ((payload as { data?: unknown }).data as Record<string, unknown> | undefined)
          : ((payload as Record<string, unknown> | null) ?? {});
      const userRecord = (resolvedData && typeof resolvedData.user === "object" ? resolvedData.user : null) as Record<string, unknown> | null;
      const fullName = String(userRecord?.fullName || normalizedFullName).trim();
      const next = splitFullName(fullName);
      setDraft(next);
      setBaseline(next);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="mx-auto flex w-full max-w-[960px] flex-col gap-6">
      <PageBanner
        eyebrow="TheSphereWorks"
        title="My Profile"
        description="Update your basic profile details used across workspace access."
        gradientVariant="workspace"
      />

      <section className="rounded-2xl border border-blue-100 bg-white/95 p-5 shadow-soft">
        <div className="grid gap-4 sm:grid-cols-2">
          <div>
            <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Email</p>
            <Input value={auth.user?.email || ""} disabled />
          </div>
          <div>
            <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Tenant</p>
            <Input value={auth.tenantSlug || ""} disabled />
          </div>
          <div>
            <p className="text-xs uppercase tracking-[0.16em] text-slate-500">First name</p>
            <Input
              value={draft.firstName}
              onChange={(event) => setDraft((current) => ({ ...current, firstName: event.target.value }))}
              autoComplete="given-name"
              maxLength={120}
            />
          </div>
          <div>
            <p className="text-xs uppercase tracking-[0.16em] text-slate-500">Last name</p>
            <Input
              value={draft.lastName}
              onChange={(event) => setDraft((current) => ({ ...current, lastName: event.target.value }))}
              autoComplete="family-name"
              maxLength={120}
            />
          </div>
        </div>
        <p className="mt-3 text-xs text-slate-500">Full name preview: {fullNamePreview || "-"}</p>
        {error ? (
          <p className="mt-3 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">{error}</p>
        ) : null}
        <div className="mt-4">
          <Button onClick={() => void handleSave()} disabled={!canSave || saving}>
            {saving ? <Spinner className="size-4" /> : <Save className="size-4" />}
            {saving ? "Saving..." : "Save profile"}
          </Button>
        </div>
      </section>
    </div>
  );
}
