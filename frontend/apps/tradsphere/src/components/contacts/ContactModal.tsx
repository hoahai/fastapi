import { useEffect, useMemo, useRef, useState } from "react";
import { Loader2, X } from "lucide-react";

import { LabeledField } from "@/components/dashboard/FormFieldRow";
import { Button } from "@/components/ui/button";
import { CacheStatusChip } from "@/components/ui/cache-status-chip";
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
import { ModalCloseButton, ModalFooter, ModalShell, Section, SectionHeader } from "@shared/components";
import { useCommittedTextField } from "@shared/hooks/useCommittedTextField";
import {
  normalizeUsPhoneDisplay,
  normalizeUsPhoneOnInput,
  validateUsPhoneField,
} from "@shared/utils/phone";

import { UsedByAccountsSection } from "./UsedByAccountsSection";
import { UsedByEstNumsSection } from "./UsedByEstNumsSection";
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

type ContactFieldTouchedState = {
  email: string | null;
  office: string | null;
  cell: string | null;
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
  canEdit?: boolean;
  initialContact: ContactRecord | null;
  focusUsageToken: number;
  detailCacheStatusText?: string | null;
  detailCacheRefreshing?: boolean;
  detailCacheRefreshDisabled?: boolean;
  onRefreshDetailCache?: () => void;
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

function buildFormFromContact(
  contact: ContactRecord | null,
): ContactFormState {
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

function composeFullName(firstName: string, lastName: string): string {
  return [asString(firstName), asString(lastName)].filter(Boolean).join(" ").trim();
}

function toNameCase(value: string): string {
  const raw = asString(value).replace(/\s+/g, " ");
  if (!raw) {
    return "";
  }
  let output = "";
  let shouldUppercase = true;
  for (const character of raw.toLowerCase()) {
    if (character >= "a" && character <= "z") {
      output += shouldUppercase ? character.toUpperCase() : character;
      shouldUppercase = false;
      continue;
    }
    output += character;
    shouldUppercase =
      character === " " ||
      character === "-" ||
      character === "'" ||
      character === "’";
  }
  return output.trim();
}

function parseNameParts(fullName: string): { firstName: string; lastName: string } {
  const parts = asString(fullName)
    .split(/\s+/)
    .map((part) => part.trim())
    .filter(Boolean);
  if (!parts.length) {
    return { firstName: "", lastName: "" };
  }
  return {
    firstName: parts[0],
    lastName: parts.slice(1).join(" "),
  };
}

function isAppDropdownInteractionTarget(target: EventTarget | null): boolean {
  if (!(target instanceof Element)) {
    return false;
  }
  return Boolean(target.closest("[data-app-dropdown-root='true'], [data-app-dropdown-menu='true']"));
}

function isAppDropdownInteractionEvent(event: { target: EventTarget | null; detail?: unknown }): boolean {
  const detail = event.detail;
  if (!detail || typeof detail !== "object") {
    return isAppDropdownInteractionTarget(event.target);
  }
  const originalEvent = (detail as { originalEvent?: unknown }).originalEvent;
  const originalTarget =
    originalEvent && typeof originalEvent === "object"
      ? (originalEvent as { target?: EventTarget | null }).target ?? null
      : null;
  return isAppDropdownInteractionTarget(originalTarget) || isAppDropdownInteractionTarget(event.target);
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
  canEdit = true,
  initialContact,
  focusUsageToken,
  detailCacheStatusText,
  detailCacheRefreshing = false,
  detailCacheRefreshDisabled = false,
  onRefreshDetailCache,
  onSubmit,
}: ContactModalProps) {
  const usageSectionRef = useRef<HTMLDivElement | null>(null);
  const emailInputRef = useRef<HTMLInputElement | null>(null);

  const [form, setForm] = useState<ContactFormState>(() =>
    buildFormFromContact(initialContact),
  );
  const [baseline, setBaseline] = useState<ContactFormState>(() =>
    buildFormFromContact(initialContact),
  );
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);
  const [hasDeferredServerUpdate, setHasDeferredServerUpdate] = useState(false);
  const [isFullNameManuallyEdited, setIsFullNameManuallyEdited] = useState(false);
  const pendingServerContactRef = useRef<ContactRecord | null>(null);
  const [fieldTouched, setFieldTouched] = useState<ContactFieldTouchedState>({
    email: null,
    office: null,
    cell: null,
  });
  const hasUnsavedChanges = useMemo(() => !formsEqual(form, baseline), [baseline, form]);
  const firstNameField = useCommittedTextField<HTMLInputElement>(
    form.firstName,
    (value) => updateNameField("firstName", value),
    { normalizeOnBlur: toNameCase },
  );
  const lastNameField = useCommittedTextField<HTMLInputElement>(
    form.lastName,
    (value) => updateNameField("lastName", value),
    { normalizeOnBlur: toNameCase },
  );

  function applyIncomingContact(nextContact: ContactRecord | null) {
    const next = buildFormFromContact(nextContact);
    setForm(next);
    setBaseline(next);
    setSubmitError(null);
    setIsSubmitting(false);
    setIsFullNameManuallyEdited(false);
    setFieldTouched({
      email: null,
      office: null,
      cell: null,
    });
  }

  useEffect(() => {
    if (!open) {
      pendingServerContactRef.current = null;
      setHasDeferredServerUpdate(false);
      setIsDiscardDialogOpen(false);
      applyIncomingContact(null);
      return;
    }

    if (mode !== "edit") {
      applyIncomingContact(initialContact);
      pendingServerContactRef.current = null;
      setHasDeferredServerUpdate(false);
      return;
    }

    if (isSubmitting) {
      return;
    }

    if (hasUnsavedChanges && !isSubmitting) {
      pendingServerContactRef.current = initialContact;
      setHasDeferredServerUpdate(true);
      return;
    }

    applyIncomingContact(initialContact);
    pendingServerContactRef.current = null;
    setHasDeferredServerUpdate(false);
  }, [initialContact, isSubmitting, mode, open]);

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

  const showUsageSections = mode === "edit";
  const isReadOnly = !canEdit;

  useEffect(() => {
    if (!open || hasUnsavedChanges) {
      return;
    }
    if (!pendingServerContactRef.current) {
      return;
    }
    applyIncomingContact(pendingServerContactRef.current);
    pendingServerContactRef.current = null;
    setHasDeferredServerUpdate(false);
  }, [hasUnsavedChanges, open]);

  const email = asString(form.email).toLowerCase();
  const isEmailValid = EMAIL_RE.test(email);
  const emailError = !email ? "Email is required." : isEmailValid ? null : "Email must be valid.";
  const officeError = useMemo(
    () =>
      validateUsPhoneField(form.office, {
        field: "Office",
        maxLength: 35,
        allowExtension: true,
      }),
    [form.office],
  );
  const cellError = useMemo(
    () =>
      validateUsPhoneField(form.cell, {
        field: "Cell",
        maxLength: 20,
        allowExtension: false,
      }),
    [form.cell],
  );
  const isFormValid = !emailError && !officeError && !cellError;
  const visibleEmailError = fieldTouched.email;
  const visibleOfficeError = fieldTouched.office;
  const visibleCellError = fieldTouched.cell;

  const canSubmit = hasUnsavedChanges && isFormValid && !isSubmitting && canEdit;
  const shouldShowSubmitButton = canEdit && (isSubmitting || canSubmit);
  const modalTitle = mode === "create" ? "Add Contact" : "Edit Contact";
  const submitLabel = "Save";

  function updateForm<K extends keyof ContactFormState>(field: K, value: ContactFormState[K]) {
    setForm((current) => ({
      ...current,
      [field]: value,
    }));
    if (submitError) {
      setSubmitError(null);
    }
  }

  function markTouched(field: keyof ContactFieldTouchedState, nextError: string | null) {
    setFieldTouched((current) => ({ ...current, [field]: nextError }));
  }

  function updateNameField(field: "firstName" | "lastName", value: string) {
    setForm((current) => {
      const next = {
        ...current,
        [field]: value,
      };
      if (!isFullNameManuallyEdited) {
        next.fullName = composeFullName(next.firstName, next.lastName);
      }
      return next;
    });
    if (submitError) {
      setSubmitError(null);
    }
  }

  function applyFullNameFromUserInput(rawValue: string) {
    const normalizedFullName = toNameCase(rawValue);
    const parsed = parseNameParts(normalizedFullName);
    setForm((current) => ({
      ...current,
      fullName: normalizedFullName,
      firstName: toNameCase(parsed.firstName),
      lastName: toNameCase(parsed.lastName),
    }));
    setIsFullNameManuallyEdited(false);
    if (submitError) {
      setSubmitError(null);
    }
  }

  function handleRevertChanges() {
    if (!hasUnsavedChanges || isSubmitting || isReadOnly) {
      return;
    }
    setForm(baseline);
    setSubmitError(null);
    setIsFullNameManuallyEdited(false);
    setFieldTouched({
      email: null,
      office: null,
      cell: null,
    });
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
    if (!canSubmit || isReadOnly) {
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
          className={`flex max-h-[90vh] ${
            showUsageSections ? "w-[min(92vw,1180px)]" : "w-[min(92vw,560px)]"
          } max-w-none flex-col overflow-hidden rounded-xl bg-white px-8 py-6`}
          onInteractOutside={(event) => {
            if (isAppDropdownInteractionEvent(event)) {
              event.preventDefault();
              return;
            }
            if (shouldBlockOutsideClose({ isBusy: isSubmitting, hasUnsavedChanges })) {
              event.preventDefault();
            }
          }}
        >
          <ModalShell busy={isSubmitting} busyMessage="Saving contact..." className="min-h-0 flex-1">
            <DialogClose asChild aria-label="Close contact modal">
              <ModalCloseButton icon={<X className="size-4" />} className="absolute right-0 top-0 z-20" />
            </DialogClose>

            <DialogHeader className="pr-8">
              <DialogTitle>{modalTitle}</DialogTitle>
              <DialogDescription>
                Manage contact details and review station usage.
              </DialogDescription>
            </DialogHeader>

            <div className="mt-4 min-h-0 flex-1 overflow-y-auto pr-1">
              <div
                className={`grid grid-cols-1 items-start gap-8 ${
                  showUsageSections ? "lg:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]" : ""
                }`}
              >
                <Section className="space-y-3">
                  <SectionHeader
                    title="Contact"
                    description={isReadOnly ? "View contact profile fields." : "Edit contact profile fields."}
                  />
                  <LabeledField label="Full Name">
                    <Input
                      value={form.fullName}
                      onChange={(event) => {
                        setIsFullNameManuallyEdited(true);
                        updateForm("fullName", event.target.value);
                      }}
                      onBlur={(event) => applyFullNameFromUserInput(event.target.value)}
                      onPaste={(event) => {
                        const pasted = event.clipboardData.getData("text");
                        if (!pasted.trim()) {
                          return;
                        }
                        event.preventDefault();
                        applyFullNameFromUserInput(pasted);
                      }}
                      onKeyDown={(event) => {
                        const isEnter = event.key === "Enter";
                        const isForwardTab = event.key === "Tab" && !event.shiftKey;
                        if (!isEnter && !isForwardTab) {
                          return;
                        }
                        event.preventDefault();
                        applyFullNameFromUserInput((event.currentTarget as HTMLInputElement).value);
                        emailInputRef.current?.focus();
                      }}
                      maxLength={255}
                      autoComplete="off"
                      disabled={isSubmitting || isReadOnly}
                    />
                  </LabeledField>

                  <LabeledField label="First Name">
                    <Input
                      value={firstNameField.value}
                      onChange={firstNameField.onChange}
                      onBlur={firstNameField.onBlur}
                      maxLength={255}
                      autoComplete="off"
                      disabled={isSubmitting || isReadOnly}
                    />
                  </LabeledField>

                  <LabeledField label="Last Name">
                    <Input
                      value={lastNameField.value}
                      onChange={lastNameField.onChange}
                      onBlur={lastNameField.onBlur}
                      maxLength={255}
                      autoComplete="off"
                      disabled={isSubmitting || isReadOnly}
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
                      ref={emailInputRef}
                      value={form.email}
                      onChange={(event) => updateForm("email", event.target.value)}
                      onBlur={(event) => {
                        const nextEmail = asString(event.target.value).toLowerCase();
                        const nextError = !nextEmail
                          ? "Email is required."
                          : EMAIL_RE.test(nextEmail)
                            ? null
                            : "Email must be valid.";
                        markTouched("email", nextError);
                      }}
                      inputMode="email"
                      maxLength={255}
                      autoComplete="off"
                      spellCheck={false}
                      disabled={isSubmitting || isReadOnly}
                    />
                  </LabeledField>

                  {visibleEmailError ? <p className="text-sm text-rose-600">{visibleEmailError}</p> : null}

                  <LabeledField label="Company">
                    <Input
                      value={form.company}
                      onChange={(event) => updateForm("company", event.target.value)}
                      maxLength={255}
                      autoComplete="off"
                      disabled={isSubmitting || isReadOnly}
                    />
                  </LabeledField>

                  <LabeledField label="Job Title">
                    <Input
                      value={form.jobTitle}
                      onChange={(event) => updateForm("jobTitle", event.target.value)}
                      maxLength={255}
                      autoComplete="off"
                      disabled={isSubmitting || isReadOnly}
                    />
                  </LabeledField>

                  <LabeledField label="Office">
                    <Input
                      value={form.office}
                      onChange={(event) =>
                        updateForm("office", normalizeUsPhoneOnInput(event.target.value, true))
                      }
                      onBlur={(event) => {
                        const normalized = normalizeUsPhoneDisplay(event.target.value, true);
                        updateForm("office", normalized);
                        markTouched(
                          "office",
                          validateUsPhoneField(normalized, {
                            field: "Office",
                            maxLength: 35,
                            allowExtension: true,
                          }),
                        );
                      }}
                      inputMode="tel"
                      maxLength={35}
                      autoComplete="off"
                      disabled={isSubmitting || isReadOnly}
                    />
                  </LabeledField>
                  {visibleOfficeError ? <p className="text-sm text-rose-600">{visibleOfficeError}</p> : null}

                  <LabeledField label="Cell">
                    <Input
                      value={form.cell}
                      onChange={(event) =>
                        updateForm("cell", normalizeUsPhoneOnInput(event.target.value, false))
                      }
                      onBlur={(event) => {
                        const normalized = normalizeUsPhoneDisplay(event.target.value, false);
                        updateForm("cell", normalized);
                        markTouched(
                          "cell",
                          validateUsPhoneField(normalized, {
                            field: "Cell",
                            maxLength: 20,
                            allowExtension: false,
                          }),
                        );
                      }}
                      inputMode="tel"
                      maxLength={20}
                      autoComplete="off"
                      disabled={isSubmitting || isReadOnly}
                    />
                  </LabeledField>
                  {visibleCellError ? <p className="text-sm text-rose-600">{visibleCellError}</p> : null}

                  <LabeledField label="Note" alignStart>
                    <Textarea
                      value={form.note}
                      onChange={(event) => updateForm("note", event.target.value)}
                      rows={3}
                      maxLength={2048}
                      disabled={isSubmitting || isReadOnly}
                    />
                  </LabeledField>

                  <LabeledField label="Active">
                    <button
                      type="button"
                      role="switch"
                      aria-checked={form.active}
                      aria-label="Active"
                      onClick={() => updateForm("active", !form.active)}
                      disabled={isSubmitting || isReadOnly}
                      aria-disabled={isSubmitting || isReadOnly}
                      className={`inline-flex h-10 w-fit items-center gap-3 px-1 py-2 text-sm transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                        form.active
                          ? "text-blue-700"
                          : "text-slate-600"
                      }`}
                    >
                      <span
                        className={`relative inline-flex h-5 w-9 items-center rounded-full transition ${
                          form.active ? "bg-blue-500" : "bg-slate-300"
                        }`}
                      >
                        <span
                          className={`inline-block h-4 w-4 rounded-full bg-white shadow transition ${
                            form.active ? "translate-x-[18px]" : "translate-x-[2px]"
                          }`}
                        />
                      </span>
                    </button>
                  </LabeledField>
                </Section>

                {showUsageSections ? (
                  <div ref={usageSectionRef} className="scroll-mt-4 min-h-0 space-y-4">
                    <UsedByStationsSection usage={initialContact?.usage ?? []} className="min-h-0" />
                    <UsedByAccountsSection accounts={initialContact?.usedByAccounts ?? []} className="min-h-0" />
                    <UsedByEstNumsSection estNums={initialContact?.usedByEstNums ?? []} className="min-h-0" />
                  </div>
                ) : null}
            </div>

              {submitError ? <p className="mt-3 text-sm text-rose-600">{submitError}</p> : null}
              {hasDeferredServerUpdate ? (
                <p className="mt-3 text-sm text-amber-700">
                  Newer contact data is available and will apply after you save or discard current edits.
                </p>
              ) : null}
            </div>

            <ModalFooter className="mt-4 flex-col gap-2 border-t border-slate-100 pt-3 sm:flex-row sm:items-center sm:justify-between">
              {mode === "edit" && detailCacheStatusText && onRefreshDetailCache ? (
                <CacheStatusChip
                  text={detailCacheStatusText}
                  onRefresh={onRefreshDetailCache}
                  disabled={detailCacheRefreshDisabled || detailCacheRefreshing || hasUnsavedChanges || isSubmitting}
                  refreshing={detailCacheRefreshing}
                  refreshLabel="Refresh contact details"
                  tooltipText={
                    hasUnsavedChanges
                      ? "Save or discard changes before refreshing detail data."
                      : "Click to refresh this contact detail data"
                  }
                  className="max-w-[min(90vw,34rem)]"
                />
              ) : (
                <span />
              )}
              <div className="flex items-center gap-2">
                {canEdit && hasUnsavedChanges ? (
                  <Button variant="outline" onClick={handleRevertChanges} disabled={isSubmitting}>
                    Revert
                  </Button>
                ) : null}
                {shouldShowSubmitButton ? (
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
                ) : null}
              </div>
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
