import { useEffect, useMemo, useRef, useState } from "react";
import { Loader2, X } from "lucide-react";

import { LabeledField, ReadOnlyValue } from "@/components/dashboard/FormFieldRow";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { canModalClose, shouldBlockOutsideClose } from "@/components/ui/modal-close-guard";
import { Textarea } from "@/components/ui/textarea";
import { UnsavedChangesDialog } from "@/components/ui/unsaved-changes-dialog";
import { ModalFooter, ModalShell } from "@shared/components";

import { UsedByStationsSection } from "./UsedByStationsSection";
import type { ContactRecord } from "./types";

export type ContactModalMode = "create" | "edit";

type ContactFormState = {
  firstName: string;
  lastName: string;
  fullName: string;
  email: string;
  company: string;
  jobTitle: string;
  office: string;
  cell: string;
  note: string;
  active: boolean;
};

export type ContactModalSubmitPayload = {
  mode: ContactModalMode;
  id: number | null;
  form: ContactFormState;
};

type ContactModalProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  mode: ContactModalMode;
  initialContact: ContactRecord | null;
  focusUsageToken: number;
  onSubmit: (payload: ContactModalSubmitPayload) => Promise<void>;
};

const EMAIL_RE = /^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+$/;

function asString(value: unknown): string {
  if (typeof value === "string") {
    return value.trim();
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  return "";
}

function buildFormFromContact(contact: ContactRecord | null): ContactFormState {
  if (!contact) {
    return {
      firstName: "",
      lastName: "",
      fullName: "",
      email: "",
      company: "",
      jobTitle: "",
      office: "",
      cell: "",
      note: "",
      active: true,
    };
  }

  return {
    firstName: asString(contact.firstName),
    lastName: asString(contact.lastName),
    fullName: asString(contact.fullName),
    email: asString(contact.email),
    company: asString(contact.company),
    jobTitle: asString(contact.jobTitle),
    office: asString(contact.office),
    cell: asString(contact.cell),
    note: asString(contact.note),
    active: Boolean(contact.active),
  };
}

function normalizeForm(form: ContactFormState): ContactFormState {
  return {
    firstName: asString(form.firstName),
    lastName: asString(form.lastName),
    fullName: asString(form.fullName),
    email: asString(form.email).toLowerCase(),
    company: asString(form.company),
    jobTitle: asString(form.jobTitle),
    office: asString(form.office),
    cell: asString(form.cell),
    note: asString(form.note),
    active: Boolean(form.active),
  };
}

function formsEqual(left: ContactFormState, right: ContactFormState): boolean {
  return JSON.stringify(normalizeForm(left)) === JSON.stringify(normalizeForm(right));
}

function getErrorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim();
  }
  return "Could not save contact. Please try again.";
}

function RequiredMark() {
  return <span className="ml-1 text-rose-600">*</span>;
}

export function ContactModal({
  open,
  onOpenChange,
  mode,
  initialContact,
  focusUsageToken,
  onSubmit,
}: ContactModalProps) {
  const usageSectionRef = useRef<HTMLDivElement | null>(null);

  const [form, setForm] = useState<ContactFormState>(() => buildFormFromContact(initialContact));
  const [baseline, setBaseline] = useState<ContactFormState>(() => buildFormFromContact(initialContact));
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);

  useEffect(() => {
    if (!open) {
      return;
    }
    const next = buildFormFromContact(initialContact);
    setForm(next);
    setBaseline(next);
    setSubmitError(null);
    setIsSubmitting(false);
  }, [initialContact, open]);

  useEffect(() => {
    if (!open) {
      return;
    }
    if (focusUsageToken <= 0) {
      return;
    }
    const frameId = window.requestAnimationFrame(() => {
      usageSectionRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
    });
    return () => window.cancelAnimationFrame(frameId);
  }, [focusUsageToken, open]);

  const hasUnsavedChanges = useMemo(() => !formsEqual(form, baseline), [baseline, form]);

  const email = asString(form.email).toLowerCase();
  const isEmailValid = EMAIL_RE.test(email);
  const isFormValid = Boolean(email) && isEmailValid;

  const canSubmit = hasUnsavedChanges && isFormValid && !isSubmitting;
  const modalTitle = mode === "create" ? "Add Contact" : "Edit Contact";
  const submitLabel = mode === "create" ? "Create Contact" : "Save Changes";

  const contactTypesText = initialContact?.contactTypes.length
    ? initialContact.contactTypes.join(", ")
    : "Not linked yet";

  function updateForm<K extends keyof ContactFormState>(field: K, value: ContactFormState[K]) {
    setForm((current) => ({
      ...current,
      [field]: value,
    }));
    if (submitError) {
      setSubmitError(null);
    }
  }

  function handleDialogOpenChange(nextOpen: boolean) {
    const allowClose = canModalClose({
      nextOpen,
      isBusy: isSubmitting,
      hasUnsavedChanges,
    });
    if (!allowClose) {
      if (!nextOpen && hasUnsavedChanges && !isSubmitting) {
        setIsDiscardDialogOpen(true);
      }
      return;
    }
    onOpenChange(nextOpen);
  }

  async function handleSubmit() {
    if (!canSubmit) {
      return;
    }

    setSubmitError(null);
    setIsSubmitting(true);

    try {
      await onSubmit({
        mode,
        id: initialContact?.id ?? null,
        form: normalizeForm(form),
      });
      onOpenChange(false);
    } catch (error) {
      setSubmitError(getErrorMessage(error));
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <>
      <Dialog open={open} onOpenChange={handleDialogOpenChange}>
        <DialogContent
          className="w-[calc(100%-2rem)] max-w-4xl rounded-2xl border border-slate-200 bg-white p-0"
          onInteractOutside={(event) => {
            if (shouldBlockOutsideClose({ isBusy: isSubmitting, hasUnsavedChanges })) {
              event.preventDefault();
            }
          }}
        >
          <DialogClose className="absolute right-4 top-4 z-20 rounded-md p-1 text-slate-500 transition hover:bg-slate-100 hover:text-slate-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-300">
            <X className="size-4" />
          </DialogClose>

          <ModalShell className="h-[min(90vh,760px)] rounded-2xl px-5 py-5 sm:px-6">
            <DialogHeader>
              <DialogTitle>{modalTitle}</DialogTitle>
              <DialogDescription>
                Manage contact details and review station usage.
              </DialogDescription>
            </DialogHeader>

            <div className="mt-4 min-h-0 flex-1 space-y-6 overflow-y-auto pr-1">
              <section className="space-y-3 rounded-xl border border-slate-200 bg-slate-50/50 p-4">
                <LabeledField label="Contact Type(s)">
                  <ReadOnlyValue value={contactTypesText} className="min-h-10 rounded-md bg-white px-3 py-2" />
                </LabeledField>

                <LabeledField label="First Name">
                  <Input
                    value={form.firstName}
                    onChange={(event) => updateForm("firstName", event.target.value)}
                    autoComplete="off"
                  />
                </LabeledField>

                <LabeledField label="Last Name">
                  <Input
                    value={form.lastName}
                    onChange={(event) => updateForm("lastName", event.target.value)}
                    autoComplete="off"
                  />
                </LabeledField>

                <LabeledField label="Full Name">
                  <Input
                    value={form.fullName}
                    onChange={(event) => updateForm("fullName", event.target.value)}
                    autoComplete="off"
                  />
                </LabeledField>

                <LabeledField
                  label={
                    <>
                      Email<RequiredMark />
                    </>
                  }
                >
                  <Input
                    value={form.email}
                    onChange={(event) => updateForm("email", event.target.value)}
                    inputMode="email"
                    autoComplete="off"
                    spellCheck={false}
                  />
                </LabeledField>

                {!email || isEmailValid ? null : <p className="text-sm text-rose-600">Email must be valid.</p>}

                <LabeledField label="Company">
                  <Input
                    value={form.company}
                    onChange={(event) => updateForm("company", event.target.value)}
                    autoComplete="off"
                  />
                </LabeledField>

                <LabeledField label="Job Title">
                  <Input
                    value={form.jobTitle}
                    onChange={(event) => updateForm("jobTitle", event.target.value)}
                    autoComplete="off"
                  />
                </LabeledField>

                <LabeledField label="Office">
                  <Input
                    value={form.office}
                    onChange={(event) => updateForm("office", event.target.value)}
                    inputMode="tel"
                    autoComplete="off"
                  />
                </LabeledField>

                <LabeledField label="Cell">
                  <Input
                    value={form.cell}
                    onChange={(event) => updateForm("cell", event.target.value)}
                    inputMode="tel"
                    autoComplete="off"
                  />
                </LabeledField>

                <LabeledField label="Note" alignStart>
                  <Textarea value={form.note} onChange={(event) => updateForm("note", event.target.value)} rows={3} />
                </LabeledField>

                <LabeledField label="Active">
                  <label className="flex h-10 cursor-pointer items-center gap-2 text-sm text-slate-700">
                    <input
                      type="checkbox"
                      checked={form.active}
                      onChange={(event) => updateForm("active", event.target.checked)}
                      className="size-4 rounded border-slate-300 text-blue-600"
                    />
                    <span>{form.active ? "Active" : "Inactive"}</span>
                  </label>
                </LabeledField>
              </section>

              <div ref={usageSectionRef} className="scroll-mt-4">
                <UsedByStationsSection usage={initialContact?.usage ?? []} />
              </div>

              {submitError ? <p className="text-sm text-rose-600">{submitError}</p> : null}
            </div>

            <ModalFooter className="mt-4 border-t border-slate-100 pt-3">
              <Button onClick={handleSubmit} disabled={!canSubmit}>
                {isSubmitting ? (
                  <>
                    <Loader2 className="size-4 animate-spin" />
                    Saving...
                  </>
                ) : (
                  submitLabel
                )}
              </Button>
            </ModalFooter>
          </ModalShell>
        </DialogContent>
      </Dialog>

      <UnsavedChangesDialog
        open={isDiscardDialogOpen}
        onKeepEditing={() => setIsDiscardDialogOpen(false)}
        onDiscardChanges={() => {
          setIsDiscardDialogOpen(false);
          onOpenChange(false);
        }}
      />
    </>
  );
}
