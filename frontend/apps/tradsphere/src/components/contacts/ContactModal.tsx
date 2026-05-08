import { useEffect, useMemo, useRef, useState } from "react";
import { Loader2, X } from "lucide-react";

import { LabeledField } from "@/components/dashboard/FormFieldRow";
import { AppDropdown } from "@/components/ui/app-dropdown";
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
import { ModalFooter, Section, SectionHeader } from "@shared/components";

import { UsedByAccountsSection } from "./UsedByAccountsSection";
import { UsedByEstNumsSection } from "./UsedByEstNumsSection";
import { UsedByStationsSection } from "./UsedByStationsSection";
import type { ContactRecord } from "./types";

export type ContactModalMode = "create" | "edit";

type ContactFormState = {
  contactType: string;
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
  contactTypeOptions: string[];
  focusUsageToken: number;
  onSubmit: (payload: ContactModalSubmitPayload) => Promise<void>;
};

const EMAIL_RE = /^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+$/;
const OFFICE_EXT_RE = /^(?<base>.+?)(?:\s*x(?<ext>\d{1,6}))?$/i;
const PHONE_ALLOWED_RE = /^[\d\s().+-]+$/;

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
  defaultContactType: string,
): ContactFormState {
  if (!contact) {
    return {
      contactType: defaultContactType,
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
    contactType: asString(contact.contactTypes[0] ?? defaultContactType).toUpperCase(),
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
    contactType: asString(form.contactType).toUpperCase(),
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

function isValidUsPhoneBase(value: string): boolean {
  if (!value || !PHONE_ALLOWED_RE.test(value)) {
    return false;
  }
  if (value.includes("+") && !value.trim().startsWith("+")) {
    return false;
  }
  if (value.split("+").length > 2) {
    return false;
  }
  const digits = value.replace(/\D/g, "");
  if (digits.length === 10) {
    return true;
  }
  return digits.length === 11 && digits.startsWith("1");
}

function formatUsPhoneBase(value: string): string | null {
  const digits = value.replace(/\D/g, "");
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  if (digits.length === 11 && digits.startsWith("1")) {
    const local = digits.slice(1);
    return `(${local.slice(0, 3)}) ${local.slice(3, 6)}-${local.slice(6)}`;
  }
  return null;
}

function normalizePhoneDisplay(value: string, allowExtension: boolean): string {
  const text = asString(value);
  if (!text) {
    return "";
  }

  if (allowExtension) {
    const match = OFFICE_EXT_RE.exec(text);
    if (!match || typeof match.groups?.base !== "string") {
      return text;
    }
    const base = match.groups.base.trim();
    const ext = typeof match.groups.ext === "string" ? match.groups.ext : "";
    const formattedBase = formatUsPhoneBase(base);
    if (!formattedBase) {
      return text;
    }
    return ext ? `${formattedBase} x${ext}` : formattedBase;
  }

  const formatted = formatUsPhoneBase(text);
  return formatted ?? text;
}

function validatePhoneField(value: string, options: { field: string; maxLength: number; allowExtension: boolean }): string | null {
  const text = asString(value);
  if (!text) {
    return null;
  }
  if (text.length > options.maxLength) {
    return `${options.field} must be <= ${options.maxLength} characters.`;
  }
  if (options.allowExtension) {
    const match = OFFICE_EXT_RE.exec(text);
    if (!match || typeof match.groups?.base !== "string") {
      return `${options.field} must be a US phone format; optional extension x####.`;
    }
    if (!isValidUsPhoneBase(match.groups.base.trim())) {
      return `${options.field} must be all digits (10/11) or valid US phone format.`;
    }
    return null;
  }
  if (/\bx\d+\s*$/i.test(text)) {
    return `${options.field} cannot include extension; use Office for x####.`;
  }
  if (!isValidUsPhoneBase(text)) {
    return `${options.field} must be all digits (10/11) or valid US phone format.`;
  }
  return null;
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
  initialContact,
  contactTypeOptions,
  focusUsageToken,
  onSubmit,
}: ContactModalProps) {
  const usageSectionRef = useRef<HTMLDivElement | null>(null);
  const resolvedContactTypeOptions = useMemo(() => {
    const values = new Set<string>();
    for (const option of contactTypeOptions) {
      const normalized = asString(option).toUpperCase();
      if (normalized) {
        values.add(normalized);
      }
    }
    if (initialContact?.contactTypes?.length) {
      for (const option of initialContact.contactTypes) {
        const normalized = asString(option).toUpperCase();
        if (normalized) {
          values.add(normalized);
        }
      }
    }
    if (!values.size) {
      values.add("REP");
    }
    return [...values].sort((a, b) => a.localeCompare(b));
  }, [contactTypeOptions, initialContact?.contactTypes]);
  const defaultContactType = resolvedContactTypeOptions.includes("REP")
    ? "REP"
    : (resolvedContactTypeOptions[0] ?? "REP");

  const [form, setForm] = useState<ContactFormState>(() =>
    buildFormFromContact(initialContact, defaultContactType),
  );
  const [baseline, setBaseline] = useState<ContactFormState>(() =>
    buildFormFromContact(initialContact, defaultContactType),
  );
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);
  const [isFullNameManuallyEdited, setIsFullNameManuallyEdited] = useState(false);

  useEffect(() => {
    if (!open) {
      return;
    }
    const next = buildFormFromContact(initialContact, defaultContactType);
    setForm(next);
    setBaseline(next);
    setSubmitError(null);
    setIsSubmitting(false);
    setIsFullNameManuallyEdited(false);
  }, [defaultContactType, initialContact, open]);

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
  const contactTypeError = !asString(form.contactType) ? "Contact Type is required." : null;
  const emailError = !email ? "Email is required." : isEmailValid ? null : "Email must be valid.";
  const officeError = useMemo(
    () =>
      validatePhoneField(form.office, {
        field: "Office",
        maxLength: 35,
        allowExtension: true,
      }),
    [form.office],
  );
  const cellError = useMemo(
    () =>
      validatePhoneField(form.cell, {
        field: "Cell",
        maxLength: 20,
        allowExtension: false,
      }),
    [form.cell],
  );
  const isFormValid = !contactTypeError && !emailError && !officeError && !cellError;

  const canSubmit = hasUnsavedChanges && isFormValid && !isSubmitting;
  const shouldShowSubmitButton = isSubmitting || canSubmit;
  const modalTitle = mode === "create" ? "Add Contact" : "Edit Contact";
  const submitLabel = "Save";

  const contactTypeDropdownOptions = useMemo(
    () => resolvedContactTypeOptions.map((option) => ({ value: option, label: option })),
    [resolvedContactTypeOptions],
  );

  function updateForm<K extends keyof ContactFormState>(field: K, value: ContactFormState[K]) {
    setForm((current) => ({
      ...current,
      [field]: value,
    }));
    if (submitError) {
      setSubmitError(null);
    }
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
          className="flex max-h-[90vh] w-[min(92vw,1180px)] max-w-none flex-col overflow-hidden rounded-xl bg-white px-8 py-6"
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
          <DialogClose className="absolute right-4 top-4 z-20 rounded-md p-1 text-slate-500 transition hover:bg-slate-100 hover:text-slate-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-300">
            <X className="size-4" />
          </DialogClose>

          <DialogHeader>
            <DialogTitle>{modalTitle}</DialogTitle>
            <DialogDescription>
              Manage contact details and review station usage.
            </DialogDescription>
          </DialogHeader>

          <div className="mt-4 min-h-0 flex-1 overflow-y-auto pr-1">
            <div className="grid grid-cols-1 items-start gap-8 lg:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
                <Section className="space-y-3">
                  <SectionHeader
                    title="Contact"
                    description="Edit contact profile fields."
                  />
                  <LabeledField
                    label={
                      <>
                        Contact Type<RequiredMark />
                      </>
                    }
                  >
                    <AppDropdown
                      ariaLabel="Contact type"
                      value={form.contactType}
                      options={contactTypeDropdownOptions}
                      onValueChange={(value) => updateForm("contactType", value)}
                      searchable={false}
                      className="w-full"
                    />
                  </LabeledField>
                  {contactTypeError ? <p className="text-sm text-rose-600">{contactTypeError}</p> : null}

                  <LabeledField label="First Name">
                    <Input
                      value={form.firstName}
                      onChange={(event) => updateNameField("firstName", event.target.value)}
                      onBlur={(event) => updateNameField("firstName", toNameCase(event.target.value))}
                      maxLength={255}
                      autoComplete="off"
                    />
                  </LabeledField>

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
                      maxLength={255}
                      autoComplete="off"
                    />
                  </LabeledField>

                  <LabeledField label="Last Name">
                    <Input
                      value={form.lastName}
                      onChange={(event) => updateNameField("lastName", event.target.value)}
                      onBlur={(event) => updateNameField("lastName", toNameCase(event.target.value))}
                      maxLength={255}
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
                      maxLength={255}
                      autoComplete="off"
                      spellCheck={false}
                    />
                  </LabeledField>

                  {emailError ? <p className="text-sm text-rose-600">{emailError}</p> : null}

                  <LabeledField label="Company">
                    <Input
                      value={form.company}
                      onChange={(event) => updateForm("company", event.target.value)}
                      maxLength={255}
                      autoComplete="off"
                    />
                  </LabeledField>

                  <LabeledField label="Job Title">
                    <Input
                      value={form.jobTitle}
                      onChange={(event) => updateForm("jobTitle", event.target.value)}
                      maxLength={255}
                      autoComplete="off"
                    />
                  </LabeledField>

                  <LabeledField label="Office">
                    <Input
                      value={form.office}
                      onChange={(event) => updateForm("office", event.target.value)}
                      onBlur={(event) =>
                        updateForm("office", normalizePhoneDisplay(event.target.value, true))
                      }
                      inputMode="tel"
                      maxLength={35}
                      autoComplete="off"
                      placeholder="(512) 431-2479 x123"
                    />
                  </LabeledField>
                  {officeError ? <p className="text-sm text-rose-600">{officeError}</p> : null}

                  <LabeledField label="Cell">
                    <Input
                      value={form.cell}
                      onChange={(event) => updateForm("cell", event.target.value)}
                      onBlur={(event) =>
                        updateForm("cell", normalizePhoneDisplay(event.target.value, false))
                      }
                      inputMode="tel"
                      maxLength={20}
                      autoComplete="off"
                      placeholder="(512) 431-2479"
                    />
                  </LabeledField>
                  {cellError ? <p className="text-sm text-rose-600">{cellError}</p> : null}

                  <LabeledField label="Note" alignStart>
                    <Textarea
                      value={form.note}
                      onChange={(event) => updateForm("note", event.target.value)}
                      rows={3}
                      maxLength={2048}
                    />
                  </LabeledField>

                  <LabeledField label="Active">
                    <button
                      type="button"
                      role="switch"
                      aria-checked={form.active}
                      aria-label="Active"
                      onClick={() => updateForm("active", !form.active)}
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
                      <span>{form.active ? "Active" : "Inactive"}</span>
                    </button>
                  </LabeledField>
                </Section>

                <div ref={usageSectionRef} className="scroll-mt-4 min-h-0 space-y-4">
                  <UsedByStationsSection usage={initialContact?.usage ?? []} className="min-h-0" />
                  <UsedByAccountsSection accounts={initialContact?.usedByAccounts ?? []} className="min-h-0" />
                  <UsedByEstNumsSection estNums={initialContact?.usedByEstNums ?? []} className="min-h-0" />
                </div>
            </div>

            {submitError ? <p className="mt-3 text-sm text-rose-600">{submitError}</p> : null}
          </div>

          <ModalFooter className="mt-4 border-t border-slate-100 pt-3">
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
          </ModalFooter>
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
