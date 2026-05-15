import { useEffect, useMemo, useRef, useState } from "react";
import { Check, Loader2, Search, X } from "lucide-react";

import { Button } from "@/components/ui/button";
import { CacheStatusChip } from "@/components/ui/cache-status-chip";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { canModalClose, shouldBlockOutsideClose } from "@/components/ui/modal-close-guard";
import { Select } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { UnsavedChangesDialog } from "@/components/ui/unsaved-changes-dialog";
import { useToast } from "@/components/ui/toast";
import { useApiRequest } from "@/hooks/useApiRequest";
import { readBrowserCacheSnapshot, removeBrowserCache, writeBrowserCache } from "@/lib/browserCache";
import { TRADSPHERE_CACHE_TTL_MS } from "@shared/cache";

import {
  StationBasicInfoSection,
  type StationDraftStation,
} from "./StationBasicInfoSection";
import {
  StationDeliveryMethodSection,
  type StationDraftDeliveryMethod,
} from "./StationDeliveryMethodSection";
import { StationContactsSection } from "./StationContactsSection";
import { type StationDraftContact } from "./StationContactCard";
import { LabeledField } from "./FormFieldRow";

const STATION_DETAIL_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.STATION_DETAIL;
const STATION_DETAIL_BASE_URL = "/api/tradsphere/v1/stations";
const STATION_DETAIL_CREATE_URL = "/api/tradsphere/v1/stations/detail";
const DELIVERY_METHODS_URL = "/api/tradsphere/v1/stations/deliveryMethods";
const CONTACTS_URL = "/api/tradsphere/v1/contacts";
const DEFAULT_MEDIA_TYPE = "TV";
const DEFAULT_LANGUAGE = "English";
const DEFAULT_DEADLINE = "10 AM";
const DEFAULT_DELIVERY_METHOD_ID = 1;
const DEFAULT_CONTACT_TYPE = "REP";
const FALLBACK_CONTACT_TYPE_OPTIONS = ["REP", "TRAFFIC", "BILLING"] as const;
const DELIVERY_METHOD_PASSWORD_STORED = "Stored";
const DELIVERY_METHOD_PASSWORD_NOT_SET = "Not set";
const SHARED_LABEL_CLASS = "text-sm font-medium leading-5 text-slate-600";
const DELIVERY_METHOD_USAGE_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.DELIVERY_METHOD_USAGE_MEMORY;
const DELIVERY_METHOD_OPTIONS_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.DELIVERY_METHOD_OPTIONS;
const EXISTING_CONTACTS_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.EXISTING_CONTACTS;
const DELIVERY_METHOD_OPTIONS_BROWSER_CACHE_KEY = "station-delivery-method-options:v1";
const EXISTING_CONTACTS_BROWSER_CACHE_KEY = "station-existing-contacts:v1";

export type StationModalMode = "create" | "edit";

export type StationDraftContactLink = {
  stationCode: string;
  contactId?: number | null;
  contactClientKey?: string | null;
  contactType: string;
  primaryContact: boolean;
  note?: string | null;
};

export type StationModalDraft = {
  station: StationDraftStation;
  deliveryMethod: StationDraftDeliveryMethod;
  contacts: StationDraftContact[];
  contactLinks: StationDraftContactLink[];
};

export type StationModalSaveResult = {
  mode: StationModalMode;
  stationCode: string;
  stationName: string;
  repContacts?: Array<{
    fullName?: string | null;
    email?: string | null;
  }>;
};

type DeliveryMethodEditorForm = {
  id: number | null;
  name: string;
  url: string;
  username: string;
  password: string;
  deadline: string;
  note: string;
};

type ContactEditorForm = {
  contactType: string;
  firstName: string;
  lastName: string;
  fullName: string;
  email: string;
  office: string;
  cell: string;
  company: string;
  jobTitle: string;
  note: string;
  linkNote: string;
  primaryContact: boolean;
};

type ExistingContactOption = {
  id: number;
  firstName: string;
  lastName: string;
  fullName: string;
  email: string;
  office: string;
  cell: string;
  company: string;
  jobTitle: string;
  note: string;
};

type DeliveryMethodOption = {
  id: number;
  name: string;
  url: string;
  username: string;
  password: string;
  deadline: string;
  note: string;
  passwordStatus: string;
  searchText: string;
};

type StationCatalogItem = {
  code: string;
  name?: string | null;
  mediaType?: string | null;
  deliveryMethodId?: number | null;
};

type DeliveryMethodUsageItem = {
  stationCode: string;
  stationName: string;
  mediaType: string;
  estNums: number[];
};

type AddExistingContactFormState = {
  selectedExistingContactId: string;
  selectedExistingContactType: string;
  selectedExistingPrimaryContact: boolean;
};

type BrowserCachedDeliveryMethodOption = {
  id: number;
  name: string;
  url: string;
  username: string;
  deadline: string;
  note: string;
  passwordStatus: string;
};

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

const deliveryMethodOptionsMemoryCache = new Map<string, DeliveryMethodOption[]>();
const existingContactsMemoryCache = new Map<string, ExistingContactOption[]>();
const deliveryMethodUsageMemoryCache = new Map<
  string,
  { updatedAt: number; usage: DeliveryMethodUsageItem[] }
>();

interface StationModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  mode: StationModalMode;
  canEdit?: boolean;
  stationCode?: string | null;
  stationCatalog?: StationCatalogItem[];
  headers: HeadersInit;
  onSuccess?: (result: StationModalSaveResult) => Promise<void> | void;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function getHeaderValue(headers: HeadersInit, name: string): string {
  const normalizedName = name.toLowerCase();
  if (headers instanceof Headers) {
    return headers.get(name) ?? headers.get(normalizedName) ?? "";
  }
  if (Array.isArray(headers)) {
    const found = headers.find(([headerName]) => headerName.toLowerCase() === normalizedName);
    return found?.[1] ?? "";
  }
  if (isRecord(headers)) {
    for (const [headerName, headerValue] of Object.entries(headers)) {
      if (headerName.toLowerCase() === normalizedName) {
        return typeof headerValue === "string" ? headerValue : String(headerValue ?? "");
      }
    }
  }
  return "";
}

function resolveTenantCacheScope(headers: HeadersInit): string {
  const tenantId = getHeaderValue(headers, "X-Tenant-Id").trim().toLowerCase();
  return tenantId || "default";
}

function buildScopedCacheKey(baseKey: string, scope: string): string {
  return `${baseKey}:${scope}`;
}

function asString(value: unknown): string {
  if (typeof value === "string") {
    return value.trim();
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  return "";
}

function asNullableString(value: unknown): string | null {
  const text = asString(value);
  return text || null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function asBoolean(value: unknown): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    return ["1", "true", "yes", "y", "on"].includes(value.trim().toLowerCase());
  }
  return false;
}

function asEstNumList(value: unknown): number[] {
  const rawValues = Array.isArray(value) ? value : [value];
  const unique = new Set<number>();
  for (const item of rawValues) {
    const parsed = asNumber(item);
    if (parsed === null) {
      continue;
    }
    unique.add(Math.trunc(parsed));
  }
  return [...unique].filter((entry) => Number.isFinite(entry) && entry > 0).sort((a, b) => a - b);
}

function unwrapData(payload: unknown): unknown {
  if (isRecord(payload) && "data" in payload) {
    return payload.data;
  }
  return payload;
}

function normalizeLanguage(value: unknown): string {
  const normalized = asString(value).toUpperCase();
  if (normalized === "EN") {
    return "English";
  }
  if (normalized === "ES") {
    return "Spanish";
  }
  if (normalized === "ENGLISH") {
    return "English";
  }
  if (normalized === "SPANISH") {
    return "Spanish";
  }
  return "";
}

function normalizeContactType(value: unknown): string {
  const normalized = asString(value).toUpperCase();
  if (!normalized) {
    return DEFAULT_CONTACT_TYPE;
  }
  return normalized;
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

function buildContactTypeOptions(values: unknown[]): Array<{ value: string; label: string }> {
  const ordered: string[] = [];
  const seen = new Set<string>();

  for (const rawValue of [...FALLBACK_CONTACT_TYPE_OPTIONS, ...values]) {
    const normalized = normalizeContactType(rawValue);
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    ordered.push(normalized);
  }

  if (!seen.has(DEFAULT_CONTACT_TYPE)) {
    ordered.unshift(DEFAULT_CONTACT_TYPE);
  }

  return ordered.map((value) => ({ value, label: value }));
}

function resolveDeliveryMethodPasswordStatus(value: unknown, fallbackStored = false): string {
  if (!isRecord(value)) {
    return fallbackStored ? DELIVERY_METHOD_PASSWORD_STORED : DELIVERY_METHOD_PASSWORD_NOT_SET;
  }

  const explicitStatus = asString(value.passwordStatus).toLowerCase();
  if (["stored", "set", "masked"].includes(explicitStatus)) {
    return DELIVERY_METHOD_PASSWORD_STORED;
  }
  if (["not set", "unset", "none"].includes(explicitStatus)) {
    return DELIVERY_METHOD_PASSWORD_NOT_SET;
  }

  const passwordFlagKeys = ["hasPassword", "passwordSet", "passwordStored", "has_password", "password_set"];
  for (const key of passwordFlagKeys) {
    if (!(key in value)) {
      continue;
    }
    return asBoolean(value[key]) ? DELIVERY_METHOD_PASSWORD_STORED : DELIVERY_METHOD_PASSWORD_NOT_SET;
  }

  if (asString(value.password)) {
    return DELIVERY_METHOD_PASSWORD_STORED;
  }

  return fallbackStored ? DELIVERY_METHOD_PASSWORD_STORED : DELIVERY_METHOD_PASSWORD_NOT_SET;
}

function extractDeliveryMethodPassword(value: unknown): string {
  if (!isRecord(value)) {
    return "";
  }

  const candidateKeys = [
    "password",
    "pwd",
    "pass",
    "passwd",
    "ftpPassword",
    "ftpPass",
    "ftp_password",
    "ftp_pass",
    "deliveryMethodPassword",
    "delivery_method_password",
    "deliveryPassword",
    "delivery_password",
    "passwordValue",
    "password_value",
  ];
  for (const key of candidateKeys) {
    const resolved = asString(value[key]);
    if (resolved) {
      return resolved;
    }
  }

  const nestedCandidates = [value.credentials, value.auth, value.login, value.security];
  for (const nested of nestedCandidates) {
    if (!isRecord(nested)) {
      continue;
    }
    for (const key of candidateKeys) {
      const resolved = asString(nested[key]);
      if (resolved) {
        return resolved;
      }
    }
  }

  return "";
}

function redactDeliveryMethodPassword(draft: StationModalDraft): StationModalDraft {
  return {
    ...draft,
    deliveryMethod: {
      ...draft.deliveryMethod,
      password: "",
      passwordStatus:
        draft.deliveryMethod.passwordStatus ||
        (asString(draft.deliveryMethod.password) ? DELIVERY_METHOD_PASSWORD_STORED : DELIVERY_METHOD_PASSWORD_NOT_SET),
    },
  };
}

function formsEqual<T>(left: T, right: T): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function toComparableDeliveryMethodEditorForm(form: DeliveryMethodEditorForm): DeliveryMethodEditorForm {
  return {
    id: form.id,
    name: asString(form.name),
    url: asString(form.url),
    username: asString(form.username),
    password: asString(form.password),
    deadline: asString(form.deadline),
    note: asString(form.note),
  };
}

function toComparableContactEditorForm(form: ContactEditorForm): ContactEditorForm {
  return {
    contactType: normalizeContactType(form.contactType),
    firstName: asString(form.firstName),
    lastName: asString(form.lastName),
    fullName: asString(form.fullName),
    email: asString(form.email).toLowerCase(),
    office: asString(form.office),
    cell: asString(form.cell),
    company: asString(form.company),
    jobTitle: asString(form.jobTitle),
    note: asString(form.note),
    linkNote: asString(form.linkNote),
    primaryContact: Boolean(form.primaryContact),
  };
}

function extractStationDeliveryMethodId(value: unknown): number | null {
  if (!isRecord(value)) {
    return null;
  }
  const topLevel = asNumber(value.deliveryMethodId ?? value.delivery_method_id);
  if (topLevel !== null) {
    return topLevel;
  }
  if (!isRecord(value.deliveryMethod)) {
    return null;
  }
  return asNumber(value.deliveryMethod.id);
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

function buildStationCatalogUsageSignature(stationCatalog: StationCatalogItem[]): string {
  return stationCatalog
    .map((item) => {
      const code = asString(item.code).toUpperCase();
      const deliveryMethodId = asNumber(item.deliveryMethodId);
      const mediaType = asString(item.mediaType).toUpperCase();
      return `${code}:${deliveryMethodId ?? "null"}:${mediaType}`;
    })
    .filter(Boolean)
    .sort((left, right) => left.localeCompare(right))
    .join("|");
}

function buildContactFullName(contact: Record<string, unknown>): string {
  const explicitName = asString(contact.name);
  if (explicitName) {
    return explicitName;
  }
  const firstName = asString(contact.firstName);
  const lastName = asString(contact.lastName);
  return [firstName, lastName].filter(Boolean).join(" ").trim();
}

function splitContactName(contact: StationDraftContact): { firstName: string; lastName: string | null } {
  const firstName = asString(contact.firstName);
  const lastName = asString(contact.lastName);
  if (firstName || lastName) {
    return {
      firstName,
      lastName: lastName || null,
    };
  }

  const fullName = asString(contact.fullName);
  if (!fullName) {
    return { firstName: "", lastName: null };
  }
  const parts = fullName.split(/\s+/).filter(Boolean);
  if (!parts.length) {
    return { firstName: "", lastName: null };
  }
  return {
    firstName: parts[0],
    lastName: parts.length > 1 ? parts.slice(1).join(" ") : null,
  };
}

function normalizeNote(value: string): string {
  return value.trim();
}

function createContactClientKey(seed: string): string {
  return `contact-${seed}-${Math.random().toString(36).slice(2, 10)}`;
}

function buildContactLinksFromContacts(
  contacts: StationDraftContact[],
  stationCode: string,
): StationDraftContactLink[] {
  const normalizedStationCode = asString(stationCode).toUpperCase();
  const links: StationDraftContactLink[] = [];
  const seenFingerprints = new Set<string>();

  for (const contact of contacts) {
    const contactId = contact.contactId ?? null;
    const contactClientKey = contactId === null ? asNullableString(contact.clientKey) : null;
    if (contactId === null && !contactClientKey) {
      continue;
    }

    const contactType = normalizeContactType(contact.contactType);
    const fingerprint = `${contactId ?? contactClientKey}:${contactType}`;
    if (seenFingerprints.has(fingerprint)) {
      continue;
    }
    seenFingerprints.add(fingerprint);

    links.push({
      stationCode: normalizedStationCode,
      contactId,
      contactClientKey,
      contactType,
      primaryContact: Boolean(contact.primaryContact),
      note: asNullableString(contact.linkNote),
    });
  }

  return links;
}

function ensureContactClientKeys(contacts: StationDraftContact[]): StationDraftContact[] {
  return contacts.map((contact, index) => {
    if (contact.contactId !== undefined && contact.contactId !== null) {
      return {
        ...contact,
        clientKey: null,
      };
    }
    if (asString(contact.clientKey)) {
      return contact;
    }
    return {
      ...contact,
      clientKey: createContactClientKey(String(index + 1)),
    };
  });
}

function syncContactsAndLinks(
  contacts: StationDraftContact[],
  stationCode: string,
): { contacts: StationDraftContact[]; contactLinks: StationDraftContactLink[] } {
  const normalizedContacts = ensureContactClientKeys(contacts).map((contact) => ({
    ...contact,
    contactType: normalizeContactType(contact.contactType),
  }));
  return {
    contacts: normalizedContacts,
    contactLinks: buildContactLinksFromContacts(normalizedContacts, stationCode),
  };
}

function createEmptyDraft(): StationModalDraft {
  return {
    station: {
      code: "",
      name: "",
      affiliation: "",
      mediaType: DEFAULT_MEDIA_TYPE,
      syscode: "",
      language: DEFAULT_LANGUAGE,
      ownership: "",
      note: "",
      deliveryMethodId: DEFAULT_DELIVERY_METHOD_ID,
    },
    deliveryMethod: {
      id: DEFAULT_DELIVERY_METHOD_ID,
      name: "",
      url: "",
      username: "",
      password: "",
      passwordStatus: DELIVERY_METHOD_PASSWORD_NOT_SET,
      deadline: DEFAULT_DEADLINE,
      note: "",
    },
    contacts: [],
    contactLinks: [],
  };
}

function hasBlankDeliveryMethodDetails(deliveryMethod: StationDraftDeliveryMethod): boolean {
  return !(
    asString(deliveryMethod.name) ||
    asString(deliveryMethod.url) ||
    asString(deliveryMethod.username) ||
    asString(deliveryMethod.password) ||
    asString(deliveryMethod.note)
  );
}

function applyDeliveryMethodOptionToDraft(
  baseDraft: StationModalDraft,
  option: DeliveryMethodOption,
): StationModalDraft {
  return {
    ...baseDraft,
    station: {
      ...baseDraft.station,
      deliveryMethodId: option.id,
    },
    deliveryMethod: {
      ...baseDraft.deliveryMethod,
      id: option.id,
      name: option.name,
      url: option.url,
      username: option.username,
      password: option.password,
      passwordStatus: option.passwordStatus,
      deadline: option.deadline,
      note: option.note,
    },
  };
}

function parseStationDetailDraft(payload: unknown, targetCode: string): StationModalDraft | null {
  const data = unwrapData(payload);
  if (!isRecord(data)) {
    return null;
  }

  const stationPayload = isRecord(data.station) ? data.station : null;
  if (!stationPayload) {
    return null;
  }

  const stationCode = asString(stationPayload.code).toUpperCase();
  const normalizedTarget = targetCode.trim().toUpperCase();
  if (normalizedTarget && stationCode !== normalizedTarget) {
    return null;
  }
  if (!stationCode) {
    return null;
  }

  const deliveryMethodPayload = isRecord(data.deliveryMethod) ? data.deliveryMethod : {};
  const parsedDeliveryMethodId = asNumber(deliveryMethodPayload.id);
  const contactsPayload = Array.isArray(data.contacts) ? data.contacts : [];
  const contactLinksPayload = Array.isArray(data.contactLinks) ? data.contactLinks : [];

  const contacts: StationDraftContact[] = [];
  for (const rawContact of contactsPayload) {
    if (!isRecord(rawContact)) {
      continue;
    }
    contacts.push({
      clientKey: null,
      linkId: null,
      linkNote: null,
      contactType: DEFAULT_CONTACT_TYPE,
      contactId: asNumber(rawContact.id),
      firstName: asNullableString(rawContact.firstName),
      lastName: asNullableString(rawContact.lastName),
      fullName: buildContactFullName(rawContact),
      email: asNullableString(rawContact.email),
      office: asNullableString(rawContact.office),
      cell: asNullableString(rawContact.cell),
      company: asNullableString(rawContact.company),
      jobTitle: asNullableString(rawContact.jobTitle),
      note: asNullableString(rawContact.note),
      primaryContact: false,
    });
  }

  const contactsById = new Map<number, StationDraftContact>();
  for (const contact of contacts) {
    if (contact.contactId === null || contact.contactId === undefined) {
      continue;
    }
    contactsById.set(contact.contactId, contact);
  }

  const parsedLinks: StationDraftContactLink[] = [];
  for (const rawLink of contactLinksPayload) {
    if (!isRecord(rawLink)) {
      continue;
    }
    const linkContactId = asNumber(rawLink.contactId);
    const contactType = normalizeContactType(rawLink.contactType);
    const primaryContact = asBoolean(rawLink.primaryContact);
    const linkNote = asNullableString(rawLink.note);
    const linkId = asNumber(rawLink.id);
    parsedLinks.push({
      stationCode: asString(rawLink.stationCode).toUpperCase() || stationCode,
      contactId: linkContactId,
      contactType,
      primaryContact,
      note: linkNote,
      contactClientKey: asNullableString(rawLink.contactClientKey),
    });

    if (linkContactId !== null) {
      const linkedContact = contactsById.get(linkContactId);
      if (linkedContact) {
        linkedContact.contactType = contactType;
        linkedContact.primaryContact = primaryContact;
        linkedContact.linkNote = linkNote;
        linkedContact.linkId = linkId;
      }
    }
  }

  const syncedContacts = ensureContactClientKeys(contacts);
  const syncedLinks = parsedLinks.length
    ? parsedLinks
    : buildContactLinksFromContacts(syncedContacts, stationCode);

  const resolvedDeliveryPassword =
    extractDeliveryMethodPassword(deliveryMethodPayload) ||
    extractDeliveryMethodPassword(data) ||
    extractDeliveryMethodPassword(stationPayload);

  return {
    station: {
      code: stationCode,
      name: asString(stationPayload.name),
      affiliation: asString(stationPayload.affiliation),
      mediaType: asString(stationPayload.mediaType).toUpperCase() || DEFAULT_MEDIA_TYPE,
      syscode: asString(stationPayload.syscode),
      language: normalizeLanguage(stationPayload.language) || DEFAULT_LANGUAGE,
      ownership: asString(stationPayload.ownership),
      note: asString(stationPayload.note),
      deliveryMethodId: asNumber(stationPayload.deliveryMethodId),
    },
    deliveryMethod: {
      id: parsedDeliveryMethodId,
      name: asString(deliveryMethodPayload.name),
      url: asString(deliveryMethodPayload.url),
      username: asString(deliveryMethodPayload.username),
      password: resolvedDeliveryPassword,
      passwordStatus: resolveDeliveryMethodPasswordStatus(deliveryMethodPayload, Boolean(resolvedDeliveryPassword)),
      deadline: asString(deliveryMethodPayload.deadline) || DEFAULT_DEADLINE,
      note: asString(deliveryMethodPayload.note),
    },
    contacts: syncedContacts,
    contactLinks: syncedLinks,
  };
}

function toComparableDraft(draft: StationModalDraft) {
  return {
    station: {
      code: draft.station.code.trim().toUpperCase(),
      name: draft.station.name.trim(),
      affiliation: draft.station.affiliation.trim(),
      mediaType: draft.station.mediaType.trim().toUpperCase(),
      syscode: draft.station.syscode.trim(),
      language: normalizeLanguage(draft.station.language) || draft.station.language.trim(),
      ownership: draft.station.ownership.trim(),
      note: normalizeNote(draft.station.note),
      deliveryMethodId:
        draft.station.deliveryMethodId !== undefined && draft.station.deliveryMethodId !== null
          ? Number(draft.station.deliveryMethodId)
          : null,
    },
    deliveryMethod: {
      id:
        draft.deliveryMethod.id !== undefined && draft.deliveryMethod.id !== null
          ? Number(draft.deliveryMethod.id)
          : null,
      name: draft.deliveryMethod.name.trim(),
      url: draft.deliveryMethod.url.trim(),
      username: draft.deliveryMethod.username.trim(),
      password: draft.deliveryMethod.password,
      deadline: draft.deliveryMethod.deadline.trim(),
      note: normalizeNote(draft.deliveryMethod.note),
    },
    contacts: draft.contacts
      .map((contact) => ({
        clientKey: asString(contact.clientKey),
        contactType: normalizeContactType(contact.contactType),
        contactId: contact.contactId ?? null,
        fullName: asString(contact.fullName),
        email: asString(contact.email).toLowerCase(),
        office: asString(contact.office),
        cell: asString(contact.cell),
        primaryContact: Boolean(contact.primaryContact),
        linkNote: asString(contact.linkNote),
      }))
      .sort((a, b) =>
        `${a.contactType}:${a.contactId ?? a.clientKey}:${a.email}`.localeCompare(
          `${b.contactType}:${b.contactId ?? b.clientKey}:${b.email}`,
        ),
      ),
    contactLinks: draft.contactLinks
      .map((link) => ({
        stationCode: asString(link.stationCode).toUpperCase(),
        contactId: link.contactId ?? null,
        contactClientKey: asString(link.contactClientKey),
        contactType: normalizeContactType(link.contactType),
        primaryContact: Boolean(link.primaryContact),
        note: asString(link.note),
      }))
      .sort((a, b) =>
        `${a.stationCode}:${a.contactType}:${a.contactId ?? a.contactClientKey}`.localeCompare(
          `${b.stationCode}:${b.contactType}:${b.contactId ?? b.contactClientKey}`,
        ),
      ),
  };
}

function hasDraftChanges(current: StationModalDraft, original: StationModalDraft | null): boolean {
  if (!original) {
    return false;
  }
  return JSON.stringify(toComparableDraft(current)) !== JSON.stringify(toComparableDraft(original));
}

function validateDraft(draft: StationModalDraft, mode: StationModalMode): string | null {
  const stationCode = draft.station.code.trim().toUpperCase();
  const stationName = draft.station.name.trim();
  const mediaType = draft.station.mediaType.trim().toUpperCase();
  const language = normalizeLanguage(draft.station.language);

  if (mode === "create" && !stationCode) {
    return "Code is required.";
  }
  if (!stationName) {
    return "Name is required.";
  }
  if (!mediaType) {
    return "Media Type is required.";
  }
  if (!language) {
    return "Language is required.";
  }
  if (mediaType === "CA") {
    const syscodeText = draft.station.syscode.trim();
    if (!syscodeText) {
      return "Syscode is required when Media Type is CA.";
    }
    const parsedSyscode = Number(syscodeText);
    if (!Number.isInteger(parsedSyscode) || parsedSyscode < 0) {
      return "Syscode must be an unsigned integer.";
    }
  }
  if (draft.station.note.trim().length > 2048) {
    return "Station note must be 2048 characters or fewer.";
  }
  if (draft.deliveryMethod.note.trim().length > 2048) {
    return "Delivery Method note must be 2048 characters or fewer.";
  }

  for (let index = 0; index < draft.contacts.length; index += 1) {
    const contact = draft.contacts[index];
    if (!asString(contact.email)) {
      return `Contact ${index + 1}: email is required.`;
    }
    if (!normalizeContactType(contact.contactType)) {
      return `Contact ${index + 1}: type is required.`;
    }
  }

  return null;
}

function buildBundledPayload(draft: StationModalDraft): Record<string, unknown> {
  const mediaType = draft.station.mediaType.trim().toUpperCase();
  const stationPayload: Record<string, unknown> = {
    code: draft.station.code.trim().toUpperCase(),
    name: draft.station.name.trim(),
    affiliation: draft.station.affiliation.trim() || null,
    mediaType: mediaType || DEFAULT_MEDIA_TYPE,
    language: normalizeLanguage(draft.station.language) || DEFAULT_LANGUAGE,
    ownership: draft.station.ownership.trim() || null,
    note: draft.station.note.trim() || null,
  };
  if (mediaType === "CA") {
    stationPayload.syscode = Number(draft.station.syscode.trim());
  }

  const deliveryMethodId =
    draft.deliveryMethod.id !== undefined && draft.deliveryMethod.id !== null
      ? Number(draft.deliveryMethod.id)
      : null;
  const deliveryName = draft.deliveryMethod.name.trim();
  const deliveryUrl = draft.deliveryMethod.url.trim();
  const deliveryUsername = draft.deliveryMethod.username.trim();
  const deliveryDeadline = draft.deliveryMethod.deadline.trim() || DEFAULT_DEADLINE;
  const deliveryNote = draft.deliveryMethod.note.trim() || null;
  const hasInlineDeliveryDetails = Boolean(
    deliveryName || deliveryUrl || deliveryUsername || draft.deliveryMethod.password.trim() || deliveryNote,
  );

  let deliveryPayload: Record<string, unknown> = {};
  if (deliveryMethodId !== null && !hasInlineDeliveryDetails) {
    deliveryPayload = { id: deliveryMethodId };
  } else if (deliveryMethodId !== null || hasInlineDeliveryDetails) {
    deliveryPayload = {
      id: deliveryMethodId,
      name: deliveryName,
      url: deliveryUrl,
      username: deliveryUsername,
      deadline: deliveryDeadline,
      note: deliveryNote,
    };
  }

  const password = draft.deliveryMethod.password.trim();
  if (password) {
    deliveryPayload.password = password;
  }

  const contactPayload = ensureContactClientKeys(draft.contacts).map((contact, index) => {
    const splitName = splitContactName(contact);
    const clientKey = asString(contact.clientKey) || createContactClientKey(String(index + 1));

    return {
      id: contact.contactId ?? null,
      clientKey,
      email: asString(contact.email).toLowerCase(),
      firstName: splitName.firstName,
      lastName: splitName.lastName,
      company: asString(contact.company) || null,
      jobTitle: asString(contact.jobTitle) || null,
      office: asString(contact.office) || null,
      cell: asString(contact.cell) || null,
      active: true,
      note: asString(contact.note) || null,
    };
  });

  const contactLinksPayload = buildContactLinksFromContacts(
    ensureContactClientKeys(draft.contacts),
    draft.station.code,
  ).map((link) => ({
    id: null,
    contactId: link.contactId ?? null,
    contactClientKey: link.contactClientKey ?? null,
    contactType: normalizeContactType(link.contactType),
    primaryContact: Boolean(link.primaryContact),
    note: asString(link.note) || null,
    active: true,
  }));

  return {
    station: stationPayload,
    deliveryMethod: deliveryPayload,
    contacts: contactPayload,
    contactLinks: contactLinksPayload,
  };
}

function buildDeliveryMethodOption(option: Omit<DeliveryMethodOption, "searchText">): DeliveryMethodOption {
  const normalizedName = asString(option.name);
  const normalizedUrl = asString(option.url);
  const normalizedUsername = asString(option.username);
  const normalizedPassword = asString(option.password);
  const normalizedDeadline = asString(option.deadline) || DEFAULT_DEADLINE;
  const normalizedNote = asString(option.note);
  const normalizedPasswordStatus = asString(option.passwordStatus) || DELIVERY_METHOD_PASSWORD_NOT_SET;

  return {
    ...option,
    name: normalizedName,
    url: normalizedUrl,
    username: normalizedUsername,
    password: normalizedPassword,
    deadline: normalizedDeadline,
    note: normalizedNote,
    passwordStatus: normalizedPasswordStatus,
    searchText: [
      normalizedName,
      normalizedUrl,
      normalizedUsername,
      normalizedPassword,
      normalizedDeadline,
      normalizedPasswordStatus,
      String(option.id),
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase(),
  };
}

function toBrowserCachedDeliveryMethodOptions(options: DeliveryMethodOption[]): BrowserCachedDeliveryMethodOption[] {
  return options.map((option) => ({
    id: option.id,
    name: option.name,
    url: option.url,
    username: option.username,
    deadline: option.deadline,
    note: option.note,
    passwordStatus: option.password ? DELIVERY_METHOD_PASSWORD_STORED : option.passwordStatus,
  }));
}

function parseBrowserCachedDeliveryMethodOptions(payload: unknown): DeliveryMethodOption[] {
  if (!Array.isArray(payload)) {
    return [];
  }

  return payload
    .filter((item) => isRecord(item))
    .map((item) => {
      const id = asNumber(item.id);
      if (id === null) {
        return null;
      }
      return buildDeliveryMethodOption({
        id,
        name: asString(item.name),
        url: asString(item.url),
        username: asString(item.username),
        password: "",
        deadline: asString(item.deadline) || DEFAULT_DEADLINE,
        note: asString(item.note),
        passwordStatus: asString(item.passwordStatus) || DELIVERY_METHOD_PASSWORD_NOT_SET,
      });
    })
    .filter((item): item is DeliveryMethodOption => item !== null);
}

function hasPopulatedDeliveryMethodOptions(options: DeliveryMethodOption[]): boolean {
  return options.some((option) => asString(option.name) || asString(option.url) || asString(option.username));
}

function parseDeliveryMethods(payload: unknown): DeliveryMethodOption[] {
  const data = unwrapData(payload);
  if (!Array.isArray(data)) {
    return [];
  }

  return data
    .filter((item) => isRecord(item))
    .map((item) => {
      const id = asNumber(item.id);
      if (id === null) {
        return null;
      }
      return buildDeliveryMethodOption({
        id,
        name: asString(item.name),
        url: asString(item.url),
        username: asString(item.username),
        password: extractDeliveryMethodPassword(item),
        deadline: asString(item.deadline) || DEFAULT_DEADLINE,
        note: asString(item.note),
        passwordStatus: extractDeliveryMethodPassword(item)
          ? DELIVERY_METHOD_PASSWORD_STORED
          : resolveDeliveryMethodPasswordStatus(item, false),
      });
    })
    .filter((item): item is DeliveryMethodOption => item !== null)
    .sort((a, b) =>
      `${a.name || "Unnamed"} ${a.url} ${a.username} ${a.id}`.localeCompare(
        `${b.name || "Unnamed"} ${b.url} ${b.username} ${b.id}`,
      ),
    );
}

function upsertDeliveryMethodOption(options: DeliveryMethodOption[], draftMethod: StationDraftDeliveryMethod): DeliveryMethodOption[] {
  const methodId = draftMethod.id;
  if (methodId === null || methodId === undefined) {
    return options;
  }

  const nextPassword = asString(draftMethod.password);
  const safePasswordStatus = nextPassword
    ? DELIVERY_METHOD_PASSWORD_STORED
    : asString(draftMethod.passwordStatus) || DELIVERY_METHOD_PASSWORD_NOT_SET;

  const nextOption = buildDeliveryMethodOption({
    id: Number(methodId),
    name: asString(draftMethod.name),
    url: asString(draftMethod.url),
    username: asString(draftMethod.username),
    password: nextPassword,
    deadline: asString(draftMethod.deadline) || DEFAULT_DEADLINE,
    note: asString(draftMethod.note),
    passwordStatus: safePasswordStatus,
  });

  const existingIndex = options.findIndex((option) => option.id === nextOption.id);
  if (existingIndex < 0) {
    return [...options, nextOption].sort((a, b) =>
      `${a.name || "Unnamed"} ${a.url} ${a.username} ${a.id}`.localeCompare(
        `${b.name || "Unnamed"} ${b.url} ${b.username} ${b.id}`,
      ),
    );
  }

  const updatedOptions = [...options];
  updatedOptions[existingIndex] = nextOption;
  return updatedOptions.sort((a, b) =>
    `${a.name || "Unnamed"} ${a.url} ${a.username} ${a.id}`.localeCompare(
      `${b.name || "Unnamed"} ${b.url} ${b.username} ${b.id}`,
    ),
  );
}

function parseExistingContacts(payload: unknown): ExistingContactOption[] {
  const data = unwrapData(payload);
  if (!Array.isArray(data)) {
    return [];
  }

  const parsed: ExistingContactOption[] = [];
  for (const item of data) {
    if (!isRecord(item)) {
      continue;
    }
    const id = asNumber(item.id);
    if (id === null) {
      continue;
    }
    const firstName = asString(item.firstName);
    const lastName = asString(item.lastName);
    const fullName = [firstName, lastName].filter(Boolean).join(" ").trim() || asString(item.name);

    parsed.push({
      id,
      firstName,
      lastName,
      fullName,
      email: asString(item.email),
      office: asString(item.office),
      cell: asString(item.cell),
      company: asString(item.company),
      jobTitle: asString(item.jobTitle),
      note: asString(item.note),
    });
  }

  return parsed.sort((a, b) => `${a.fullName} ${a.email}`.localeCompare(`${b.fullName} ${b.email}`));
}

function buildDeliveryMethodEditorForm(source?: StationDraftDeliveryMethod | null): DeliveryMethodEditorForm {
  return {
    id: source?.id ?? null,
    name: asString(source?.name),
    url: asString(source?.url),
    username: asString(source?.username),
    password: asString(source?.password),
    deadline: asString(source?.deadline) || DEFAULT_DEADLINE,
    note: asString(source?.note),
  };
}

function buildContactEditorForm(source?: StationDraftContact | null): ContactEditorForm {
  const firstName = asString(source?.firstName);
  const lastName = asString(source?.lastName);
  const fullName = asString(source?.fullName) || composeFullName(firstName, lastName);
  return {
    contactType: normalizeContactType(source?.contactType),
    firstName,
    lastName,
    fullName,
    email: asString(source?.email),
    office: asString(source?.office),
    cell: asString(source?.cell),
    company: asString(source?.company),
    jobTitle: asString(source?.jobTitle),
    note: asString(source?.note),
    linkNote: asString(source?.linkNote),
    primaryContact: Boolean(source?.primaryContact),
  };
}

function buildAddExistingContactFormState(
  source?: Partial<AddExistingContactFormState> | null,
): AddExistingContactFormState {
  return {
    selectedExistingContactId: asString(source?.selectedExistingContactId),
    selectedExistingContactType: normalizeContactType(source?.selectedExistingContactType),
    selectedExistingPrimaryContact: Boolean(source?.selectedExistingPrimaryContact),
  };
}

function buildContactCopyValue(contact: StationDraftContact): string {
  const fullName = asString(contact.fullName);
  const email = asString(contact.email);
  if (!email) {
    return "";
  }
  return fullName ? `${fullName} <${email}>` : email;
}

function buildRepContactSummaries(contacts: StationDraftContact[]): Array<{ fullName?: string | null; email?: string | null }> {
  const repContacts = contacts
    .filter((contact) => normalizeContactType(contact.contactType) === "REP")
    .map((contact) => {
      const fullName = asString(contact.fullName) || [asString(contact.firstName), asString(contact.lastName)].filter(Boolean).join(" ");
      const email = asString(contact.email);
      return {
        fullName: fullName || null,
        email: email || null,
      };
    })
    .filter((contact) => contact.fullName || contact.email);

  return repContacts.length ? repContacts : [];
}

async function copyContactToClipboard(value: string): Promise<void> {
  if (!value) {
    throw new Error("Contact value is empty");
  }

  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(value);
    return;
  }

  const textArea = document.createElement("textarea");
  textArea.value = value;
  textArea.setAttribute("readonly", "");
  textArea.style.position = "fixed";
  textArea.style.left = "-9999px";
  textArea.style.top = "0";
  document.body.appendChild(textArea);
  textArea.select();
  try {
    const didCopy = document.execCommand("copy");
    if (!didCopy) {
      throw new Error("execCommand copy failed");
    }
  } finally {
    document.body.removeChild(textArea);
  }
}

function RequiredMark() {
  return <span className="ml-1 text-rose-600">*</span>;
}

function buildModalCacheStatusText(options: {
  isLoading: boolean;
  status: CacheStatus | null;
}): string {
  const { isLoading, status } = options;
  if (isLoading) {
    return status ? "Refreshing..." : "Loading...";
  }
  if (status) {
    return `Data source: ${status.source}. Last updated ${formatRelativeTime(status.fetchedAt)}.`;
  }
  return "No cached data yet";
}

export function StationModal({
  open,
  onOpenChange,
  mode,
  canEdit = true,
  stationCode,
  stationCatalog = [],
  headers,
  onSuccess,
}: StationModalProps) {
  const { requestJson } = useApiRequest();
  const toast = useToast();
  const tenantCacheScope = useMemo(() => resolveTenantCacheScope(headers), [headers]);
  const deliveryMethodOptionsCacheKey = useMemo(
    () => buildScopedCacheKey(DELIVERY_METHOD_OPTIONS_BROWSER_CACHE_KEY, tenantCacheScope),
    [tenantCacheScope],
  );
  const existingContactsCacheKey = useMemo(
    () => buildScopedCacheKey(EXISTING_CONTACTS_BROWSER_CACHE_KEY, tenantCacheScope),
    [tenantCacheScope],
  );
  const [draft, setDraft] = useState<StationModalDraft>(() => createEmptyDraft());
  const [originalDraft, setOriginalDraft] = useState<StationModalDraft | null>(null);
  const [isLoadingDetail, setIsLoadingDetail] = useState(false);
  const [isRefreshingDetail, setIsRefreshingDetail] = useState(false);
  const [hasAttemptedDetailLoad, setHasAttemptedDetailLoad] = useState(false);
  const [detailRefreshToken, setDetailRefreshToken] = useState(0);
  const [detailCacheStatus, setDetailCacheStatus] = useState<{ source: "cache" | "network"; fetchedAt: number } | null>(null);
  const [hasDeferredDetailUpdate, setHasDeferredDetailUpdate] = useState(false);
  const handledDetailRefreshTokenRef = useRef(0);
  const pendingDetailDraftRef = useRef<{
    draft: StationModalDraft;
    cacheStatus: { source: "cache" | "network"; fetchedAt: number };
  } | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);

  const [isDeliveryMethodSelectorOpen, setIsDeliveryMethodSelectorOpen] = useState(false);
  const [deliveryMethodOptions, setDeliveryMethodOptions] = useState<DeliveryMethodOption[]>([]);
  const [deliveryMethodsCacheStatus, setDeliveryMethodsCacheStatus] = useState<CacheStatus | null>(null);
  const [isLoadingDeliveryMethods, setIsLoadingDeliveryMethods] = useState(false);
  const [deliveryMethodSelectorError, setDeliveryMethodSelectorError] = useState<string | null>(null);
  const [selectedDeliveryMethodId, setSelectedDeliveryMethodId] = useState<string>("");
  const [deliveryMethodSearch, setDeliveryMethodSearch] = useState("");
  const [initialSelectedDeliveryMethodId, setInitialSelectedDeliveryMethodId] = useState<string>("");
  const [isDeliveryMethodSelectorDiscardDialogOpen, setIsDeliveryMethodSelectorDiscardDialogOpen] = useState(false);

  const [isDeliveryMethodEditorOpen, setIsDeliveryMethodEditorOpen] = useState(false);
  const [deliveryMethodEditorForm, setDeliveryMethodEditorForm] = useState<DeliveryMethodEditorForm>(
    buildDeliveryMethodEditorForm(),
  );
  const [deliveryMethodEditorBaseline, setDeliveryMethodEditorBaseline] = useState<DeliveryMethodEditorForm>(
    buildDeliveryMethodEditorForm(),
  );
  const [deliveryMethodEditorError, setDeliveryMethodEditorError] = useState<string | null>(null);
  const [isDeliveryMethodEditorDiscardDialogOpen, setIsDeliveryMethodEditorDiscardDialogOpen] = useState(false);
  const [deliveryMethodUsage, setDeliveryMethodUsage] = useState<DeliveryMethodUsageItem[] | null>(null);
  const [isLoadingDeliveryMethodUsage, setIsLoadingDeliveryMethodUsage] = useState(false);
  const [deliveryMethodUsageError, setDeliveryMethodUsageError] = useState<string | null>(null);

  const [isContactEditorOpen, setIsContactEditorOpen] = useState(false);
  const [contactEditorForm, setContactEditorForm] = useState<ContactEditorForm>(buildContactEditorForm());
  const [contactEditorBaseline, setContactEditorBaseline] = useState<ContactEditorForm>(buildContactEditorForm());
  const [contactEditorIndex, setContactEditorIndex] = useState<number | null>(null);
  const [contactEditorError, setContactEditorError] = useState<string | null>(null);
  const [isContactEditorFullNameManuallyEdited, setIsContactEditorFullNameManuallyEdited] = useState(false);
  const [isContactEditorDiscardDialogOpen, setIsContactEditorDiscardDialogOpen] = useState(false);

  const [isAddExistingContactOpen, setIsAddExistingContactOpen] = useState(false);
  const [existingContactsCatalog, setExistingContactsCatalog] = useState<ExistingContactOption[]>([]);
  const [existingContactsCacheStatus, setExistingContactsCacheStatus] = useState<CacheStatus | null>(null);
  const [isLoadingExistingContacts, setIsLoadingExistingContacts] = useState(false);
  const [existingContactsError, setExistingContactsError] = useState<string | null>(null);
  const [existingContactSearch, setExistingContactSearch] = useState("");
  const [selectedExistingContactId, setSelectedExistingContactId] = useState<string>("");
  const [selectedExistingContactType, setSelectedExistingContactType] = useState(DEFAULT_CONTACT_TYPE);
  const [selectedExistingPrimaryContact, setSelectedExistingPrimaryContact] = useState(false);
  const [addExistingContactBaseline, setAddExistingContactBaseline] = useState<AddExistingContactFormState>(
    buildAddExistingContactFormState(),
  );
  const [isAddExistingContactDiscardDialogOpen, setIsAddExistingContactDiscardDialogOpen] = useState(false);

  const isEditMode = mode === "edit";
  const isReadOnly = !canEdit;
  const isDetailReady = !isEditMode || (hasAttemptedDetailLoad && !isLoadingDetail && !detailError && !!originalDraft);
  const hasUnsavedChanges = hasDraftChanges(draft, originalDraft);
  const validationError = isDetailReady ? validateDraft(draft, mode) : null;
  const canSubmit = Boolean(
    isDetailReady &&
      !validationError &&
      !isSubmitting &&
      (!isEditMode || hasUnsavedChanges) &&
      canEdit,
  );
  const hasDeliveryMethodSelectorChanges =
    isDeliveryMethodSelectorOpen && selectedDeliveryMethodId !== initialSelectedDeliveryMethodId;
  const canApplySelectedDeliveryMethod =
    selectedDeliveryMethodId !== "" &&
    selectedDeliveryMethodId !== initialSelectedDeliveryMethodId &&
    Number.isFinite(Number(selectedDeliveryMethodId));
  const isDeliveryMethodEditorValid = Boolean(
    asString(deliveryMethodEditorForm.name) &&
      asString(deliveryMethodEditorForm.url) &&
      asString(deliveryMethodEditorForm.username) &&
      asString(deliveryMethodEditorForm.deadline),
  );
  const hasDeliveryMethodEditorChanges = !formsEqual(
    toComparableDeliveryMethodEditorForm(deliveryMethodEditorForm),
    toComparableDeliveryMethodEditorForm(deliveryMethodEditorBaseline),
  );
  const canApplyDeliveryMethodEditor = isDeliveryMethodEditorValid && hasDeliveryMethodEditorChanges;
  const isContactEditorValid = Boolean(
    asString(contactEditorForm.email).toLowerCase() &&
      normalizeContactType(contactEditorForm.contactType),
  );
  const hasContactEditorChanges = !formsEqual(
    toComparableContactEditorForm(contactEditorForm),
    toComparableContactEditorForm(contactEditorBaseline),
  );
  const canApplyContactEditor = isContactEditorValid && hasContactEditorChanges;
  const addExistingContactState = buildAddExistingContactFormState({
    selectedExistingContactId,
    selectedExistingContactType,
    selectedExistingPrimaryContact,
  });
  const contactTypeOptions = useMemo(
    () =>
      buildContactTypeOptions([
        ...draft.contacts.map((contact) => contact.contactType),
        contactEditorForm.contactType,
        selectedExistingContactType,
      ]),
    [contactEditorForm.contactType, draft.contacts, selectedExistingContactType],
  );
  const hasAddExistingContactChanges = !formsEqual(addExistingContactState, addExistingContactBaseline);
  const canAddExistingContact = Boolean(
    selectedExistingContactId &&
      !isLoadingExistingContacts &&
      Number.isFinite(Number(selectedExistingContactId)) &&
      hasAddExistingContactChanges,
  );
  const hasUnsavedChangesRef = useRef(hasUnsavedChanges);

  useEffect(() => {
    hasUnsavedChangesRef.current = hasUnsavedChanges;
  }, [hasUnsavedChanges]);

  function applyLoadedDetailDraft(
    nextDraft: StationModalDraft,
    cacheStatus: { source: "cache" | "network"; fetchedAt: number },
  ) {
    setDraft(nextDraft);
    setOriginalDraft(nextDraft);
    setHasDeferredDetailUpdate(false);
    pendingDetailDraftRef.current = null;
    setDetailCacheStatus(cacheStatus);
    setHasAttemptedDetailLoad(true);
    setDetailError(null);
  }

  const linkedContactIdSet = useMemo(
    () =>
      new Set(
        draft.contacts
          .map((contact) => (contact.contactId !== null && contact.contactId !== undefined ? Number(contact.contactId) : null))
          .filter((value): value is number => value !== null),
      ),
    [draft.contacts],
  );

  const filteredExistingContacts = useMemo(() => {
    const query = asString(existingContactSearch).toLowerCase();
    const base = existingContactsCatalog.filter((contact) => !linkedContactIdSet.has(contact.id));
    if (!query) {
      return base;
    }

    return base.filter((contact) =>
      [contact.fullName, contact.email, contact.company, contact.jobTitle].join(" ").toLowerCase().includes(query),
    );
  }, [existingContactSearch, existingContactsCatalog, linkedContactIdSet]);

  useEffect(() => {
    if (!open) {
      pendingDetailDraftRef.current = null;
      setHasDeferredDetailUpdate(false);
      setIsLoadingDetail(false);
      setIsRefreshingDetail(false);
      setHasAttemptedDetailLoad(false);
      setDetailCacheStatus(null);
      setDetailError(null);
      setSubmitError(null);
      setIsDiscardDialogOpen(false);
      setIsSubmitting(false);

      setIsDeliveryMethodSelectorOpen(false);
      setDeliveryMethodsCacheStatus(null);
      setDeliveryMethodSelectorError(null);
      setSelectedDeliveryMethodId("");
      setInitialSelectedDeliveryMethodId("");
      setDeliveryMethodSearch("");
      setIsDeliveryMethodSelectorDiscardDialogOpen(false);
      setIsDeliveryMethodEditorOpen(false);
      setDeliveryMethodEditorForm(buildDeliveryMethodEditorForm());
      setDeliveryMethodEditorBaseline(buildDeliveryMethodEditorForm());
      setDeliveryMethodEditorError(null);
      setIsDeliveryMethodEditorDiscardDialogOpen(false);
      setDeliveryMethodUsage(null);
      setIsLoadingDeliveryMethodUsage(false);
      setDeliveryMethodUsageError(null);

      setIsContactEditorOpen(false);
      setContactEditorForm(buildContactEditorForm());
      setContactEditorBaseline(buildContactEditorForm());
      setContactEditorIndex(null);
      setContactEditorError(null);
      setIsContactEditorFullNameManuallyEdited(false);
      setIsContactEditorDiscardDialogOpen(false);
      setIsAddExistingContactOpen(false);
      setExistingContactsCacheStatus(null);
      setExistingContactsError(null);
      setSelectedExistingContactId("");
      setSelectedExistingContactType(DEFAULT_CONTACT_TYPE);
      setSelectedExistingPrimaryContact(false);
      setExistingContactSearch("");
      setAddExistingContactBaseline(buildAddExistingContactFormState());
      setIsAddExistingContactDiscardDialogOpen(false);
      return;
    }

    setSubmitError(null);
    setDetailError(null);
    setIsDiscardDialogOpen(false);
    const isManualRefresh = detailRefreshToken !== handledDetailRefreshTokenRef.current;
    if (isManualRefresh) {
      handledDetailRefreshTokenRef.current = detailRefreshToken;
    }
    setIsRefreshingDetail(isManualRefresh);

    if (mode === "create") {
      const baseDraft = createEmptyDraft();
      setDraft(baseDraft);
      setOriginalDraft(baseDraft);
      setHasAttemptedDetailLoad(true);
      setIsLoadingDetail(false);
      setIsRefreshingDetail(false);
      void loadDeliveryMethods();
      return;
    }

    const normalizedStationCode = asString(stationCode).toUpperCase();
    if (!normalizedStationCode) {
      setDraft(createEmptyDraft());
      setOriginalDraft(null);
      setDetailError("Missing Station code for edit mode.");
      setHasAttemptedDetailLoad(true);
      setIsLoadingDetail(false);
      setIsRefreshingDetail(false);
      return;
    }

    let isMounted = true;
    setIsLoadingDetail(true);
    setHasAttemptedDetailLoad(false);
    setOriginalDraft(null);

    const cacheKey = `station-detail:${normalizedStationCode}`;
    if (!isManualRefresh) {
      const cachedSnapshot = readBrowserCacheSnapshot<StationModalDraft>(cacheKey);
      if (cachedSnapshot && !cachedSnapshot.isExpired) {
        const sanitizedCached = redactDeliveryMethodPassword(cachedSnapshot.data);
        const synced = syncContactsAndLinks(sanitizedCached.contacts, sanitizedCached.station.code);
        const cachedDraft = { ...sanitizedCached, ...synced };
        applyLoadedDetailDraft(cachedDraft, {
          source: "cache",
          fetchedAt: cachedSnapshot.fetchedAt,
        });
        setIsLoadingDetail(false);
        setIsRefreshingDetail(false);
        return;
      }
    }

    void requestJson(
      `${STATION_DETAIL_BASE_URL}/${encodeURIComponent(normalizedStationCode)}/detail`,
      {
        headers,
        errorToast: false,
      },
    )
      .then((payload) => {
        if (!isMounted) {
          return;
        }
        const parsed = parseStationDetailDraft(payload, normalizedStationCode);
        if (!parsed) {
          setDetailError("Unable to load station detail.");
          setHasAttemptedDetailLoad(true);
          return;
        }
        const synced = syncContactsAndLinks(parsed.contacts, parsed.station.code);
        const nextDraft = { ...parsed, ...synced };
        const fetchedAt = Date.now();
        writeBrowserCache(cacheKey, redactDeliveryMethodPassword(nextDraft), STATION_DETAIL_CACHE_TTL_MS, {
          source: "network",
          fetchedAt,
        });
        const nextCacheStatus = {
          source: "network",
          fetchedAt,
        } as const;
        if (hasUnsavedChangesRef.current) {
          pendingDetailDraftRef.current = {
            draft: nextDraft,
            cacheStatus: nextCacheStatus,
          };
          setHasDeferredDetailUpdate(true);
          setHasAttemptedDetailLoad(true);
          return;
        }
        applyLoadedDetailDraft(nextDraft, nextCacheStatus);
      })
      .catch((error) => {
        if (!isMounted) {
          return;
        }
        setDetailError(error instanceof Error ? error.message : "Unable to load station detail.");
        setHasAttemptedDetailLoad(true);
      })
      .finally(() => {
        if (isMounted) {
          setIsLoadingDetail(false);
          setIsRefreshingDetail(false);
        }
      });

    return () => {
      isMounted = false;
    };
  }, [detailRefreshToken, headers, mode, open, requestJson, stationCode]);

  useEffect(() => {
    if (!open || hasUnsavedChanges) {
      return;
    }
    const pending = pendingDetailDraftRef.current;
    if (!pending) {
      return;
    }
    applyLoadedDetailDraft(pending.draft, pending.cacheStatus);
  }, [hasUnsavedChanges, open]);

  useEffect(() => {
    if (!open || mode !== "create" || deliveryMethodOptions.length === 0) {
      return;
    }

    const defaultOption = deliveryMethodOptions.find((item) => item.id === DEFAULT_DELIVERY_METHOD_ID);
    if (!defaultOption) {
      return;
    }

    setDraft((current) => {
      if (
        current.deliveryMethod.id !== DEFAULT_DELIVERY_METHOD_ID ||
        !hasBlankDeliveryMethodDetails(current.deliveryMethod)
      ) {
        return current;
      }
      return applyDeliveryMethodOptionToDraft(current, defaultOption);
    });

    setOriginalDraft((current) => {
      if (!current) {
        return current;
      }
      if (
        current.deliveryMethod.id !== DEFAULT_DELIVERY_METHOD_ID ||
        !hasBlankDeliveryMethodDetails(current.deliveryMethod)
      ) {
        return current;
      }
      return applyDeliveryMethodOptionToDraft(current, defaultOption);
    });
  }, [deliveryMethodOptions, mode, open]);

  function updateDraft(next: Partial<StationModalDraft>) {
    setDraft((current) => ({ ...current, ...next }));
    setSubmitError(null);
  }

  function updateStation(next: Partial<StationDraftStation>) {
    setDraft((current) => {
      const station = { ...current.station, ...next };
      const syncedLinks = current.contactLinks.map((link) => ({
        ...link,
        stationCode: asString(station.code).toUpperCase(),
      }));
      return {
        ...current,
        station,
        contactLinks: syncedLinks,
      };
    });
    setSubmitError(null);
  }

  function updateContacts(nextContacts: StationDraftContact[]) {
    setDraft((current) => {
      const synced = syncContactsAndLinks(nextContacts, current.station.code);
      return {
        ...current,
        ...synced,
      };
    });
    setSubmitError(null);
  }

  async function loadDeliveryMethods(force = false) {
    if (!force) {
      if (deliveryMethodOptions.length > 0 && hasPopulatedDeliveryMethodOptions(deliveryMethodOptions)) {
        setDeliveryMethodsCacheStatus((current) => current ?? { source: "cache", fetchedAt: Date.now() });
        return;
      }
      const memoryCachedOptions = deliveryMethodOptionsMemoryCache.get(tenantCacheScope) ?? null;
      if (memoryCachedOptions && memoryCachedOptions.length > 0 && hasPopulatedDeliveryMethodOptions(memoryCachedOptions)) {
        setDeliveryMethodOptions(memoryCachedOptions);
        setDeliveryMethodsCacheStatus({ source: "cache", fetchedAt: Date.now() });
        return;
      }

      const browserCacheSnapshot = readBrowserCacheSnapshot<unknown>(deliveryMethodOptionsCacheKey);
      const browserCachedOptions = parseBrowserCachedDeliveryMethodOptions(browserCacheSnapshot?.data);
      if (browserCachedOptions.length > 0 && hasPopulatedDeliveryMethodOptions(browserCachedOptions)) {
        deliveryMethodOptionsMemoryCache.set(tenantCacheScope, browserCachedOptions);
        setDeliveryMethodOptions(browserCachedOptions);
        setDeliveryMethodsCacheStatus({
          source: "cache",
          fetchedAt: browserCacheSnapshot?.fetchedAt ?? Date.now(),
        });
        return;
      }
    }
    setIsLoadingDeliveryMethods(true);
    setDeliveryMethodSelectorError(null);
    try {
      const payload = await requestJson(DELIVERY_METHODS_URL, {
        headers,
        errorToast: false,
      });
      const parsedOptions = parseDeliveryMethods(payload);
      deliveryMethodOptionsMemoryCache.set(tenantCacheScope, parsedOptions);
      setDeliveryMethodOptions(parsedOptions);
      const fetchedAt = Date.now();
      setDeliveryMethodsCacheStatus({
        source: "network",
        fetchedAt,
      });
      writeBrowserCache(
        deliveryMethodOptionsCacheKey,
        toBrowserCachedDeliveryMethodOptions(parsedOptions),
        DELIVERY_METHOD_OPTIONS_CACHE_TTL_MS,
        {
          source: "network",
          fetchedAt,
        },
      );
    } catch (error) {
      setDeliveryMethodSelectorError(error instanceof Error ? error.message : "Unable to load delivery methods.");
    } finally {
      setIsLoadingDeliveryMethods(false);
    }
  }

  async function loadExistingContacts(force = false) {
    if (!force && existingContactsCatalog.length > 0) {
      setExistingContactsCacheStatus((current) => current ?? { source: "cache", fetchedAt: Date.now() });
      return;
    }
    if (!force) {
      const memoryCachedContacts = existingContactsMemoryCache.get(tenantCacheScope) ?? null;
      if (memoryCachedContacts && memoryCachedContacts.length > 0) {
        setExistingContactsCatalog(memoryCachedContacts);
        setExistingContactsCacheStatus({ source: "cache", fetchedAt: Date.now() });
        return;
      }

      const browserCacheSnapshot = readBrowserCacheSnapshot<unknown>(existingContactsCacheKey);
      const browserCachedContacts = parseExistingContacts(browserCacheSnapshot?.data);
      if (browserCachedContacts.length > 0) {
        existingContactsMemoryCache.set(tenantCacheScope, browserCachedContacts);
        setExistingContactsCatalog(browserCachedContacts);
        setExistingContactsCacheStatus({
          source: "cache",
          fetchedAt: browserCacheSnapshot?.fetchedAt ?? Date.now(),
        });
        return;
      }
    }
    setIsLoadingExistingContacts(true);
    setExistingContactsError(null);
    try {
      const payload = await requestJson(`${CONTACTS_URL}?active=true`, {
        headers,
        errorToast: false,
      });
      const parsedContacts = parseExistingContacts(payload);
      existingContactsMemoryCache.set(tenantCacheScope, parsedContacts);
      setExistingContactsCatalog(parsedContacts);
      const fetchedAt = Date.now();
      setExistingContactsCacheStatus({
        source: "network",
        fetchedAt,
      });
      writeBrowserCache(existingContactsCacheKey, parsedContacts, EXISTING_CONTACTS_CACHE_TTL_MS, {
        source: "network",
        fetchedAt,
      });
    } catch (error) {
      setExistingContactsError(error instanceof Error ? error.message : "Unable to load contacts.");
    } finally {
      setIsLoadingExistingContacts(false);
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

  function closeDeliveryMethodSelector(force = false) {
    if (!force && hasDeliveryMethodSelectorChanges) {
      setIsDeliveryMethodSelectorDiscardDialogOpen(true);
      return;
    }
    setIsDeliveryMethodSelectorOpen(false);
    setIsDeliveryMethodSelectorDiscardDialogOpen(false);
  }

  function handleDeliveryMethodSelectorOpenChange(nextOpen: boolean) {
    if (isReadOnly && nextOpen) {
      return;
    }
    const allowClose = canModalClose({
      nextOpen,
      hasUnsavedChanges: hasDeliveryMethodSelectorChanges,
    });
    if (!allowClose) {
      if (!nextOpen && hasDeliveryMethodSelectorChanges) {
        setIsDeliveryMethodSelectorDiscardDialogOpen(true);
      }
      return;
    }
    setIsDeliveryMethodSelectorOpen(nextOpen);
  }

  async function loadDeliveryMethodUsage(methodId: number | null, currentStationCode: string) {
    setDeliveryMethodUsageError(null);
    if (methodId === null) {
      setDeliveryMethodUsage(null);
      return;
    }

    const stationCodes = stationCatalog
      .map((item) => asString(item.code).toUpperCase())
      .filter(Boolean);
    const uniqueStationCodes = Array.from(new Set(stationCodes));
    if (!uniqueStationCodes.length) {
      setDeliveryMethodUsage(null);
      return;
    }

    const usageCacheKey = [
      `method:${methodId}`,
      `current:${currentStationCode}`,
      `catalog:${buildStationCatalogUsageSignature(stationCatalog)}`,
    ].join("::");
    const cachedUsageEntry = deliveryMethodUsageMemoryCache.get(usageCacheKey);
    if (
      cachedUsageEntry &&
      Date.now() - cachedUsageEntry.updatedAt <= DELIVERY_METHOD_USAGE_CACHE_TTL_MS
    ) {
      setDeliveryMethodUsage(cachedUsageEntry.usage);
      return;
    }

    const allCatalogRowsHaveDeliveryMethod = stationCatalog.length > 0 && stationCatalog.every((item) => {
      if (asString(item.code) === "") {
        return true;
      }
      return item.deliveryMethodId !== undefined && item.deliveryMethodId !== null;
    });

    if (allCatalogRowsHaveDeliveryMethod) {
      const nextUsage = stationCatalog
        .map((item) => {
          const stationCode = asString(item.code).toUpperCase();
          if (!stationCode || stationCode === currentStationCode || asNumber(item.deliveryMethodId) !== methodId) {
            return null;
          }
          return {
            stationCode,
            stationName: asString(item.name),
            mediaType: asString(item.mediaType).toUpperCase(),
            estNums: [] as number[],
          };
        })
        .filter((item): item is DeliveryMethodUsageItem => item !== null)
        .sort((left, right) => left.stationCode.localeCompare(right.stationCode));
      deliveryMethodUsageMemoryCache.set(usageCacheKey, {
        updatedAt: Date.now(),
        usage: nextUsage,
      });
      setDeliveryMethodUsage(nextUsage);
      return;
    }

    setIsLoadingDeliveryMethodUsage(true);
    try {
      const payload = await requestJson(
        `${STATION_DETAIL_BASE_URL}?codes=${encodeURIComponent(uniqueStationCodes.join(","))}&deliveryMethodDetail=false&contactDetail=false`,
        {
          headers,
          errorToast: false,
        },
      );
      const rows = unwrapData(payload);
      const parsedRows = Array.isArray(rows) ? rows : [];
      const nextUsage = parsedRows
        .map((item) => {
          if (!isRecord(item)) {
            return null;
          }
          const stationCode = asString(item.code).toUpperCase();
          if (!stationCode || stationCode === currentStationCode) {
            return null;
          }
          if (extractStationDeliveryMethodId(item) !== methodId) {
            return null;
          }
          return {
            stationCode,
            stationName: asString(item.name),
            mediaType: asString(item.mediaType).toUpperCase(),
            estNums: asEstNumList(item.estNums ?? item.estNum),
          };
        })
        .filter((item): item is DeliveryMethodUsageItem => item !== null)
        .sort((left, right) => left.stationCode.localeCompare(right.stationCode));
      deliveryMethodUsageMemoryCache.set(usageCacheKey, {
        updatedAt: Date.now(),
        usage: nextUsage,
      });
      setDeliveryMethodUsage(nextUsage);
    } catch (error) {
      setDeliveryMethodUsageError(error instanceof Error ? error.message : "Unable to load station usage.");
      setDeliveryMethodUsage(null);
    } finally {
      setIsLoadingDeliveryMethodUsage(false);
    }
  }

  function openDeliveryMethodSelector() {
    if (isReadOnly) {
      return;
    }
    const currentId =
      draft.deliveryMethod.id !== undefined && draft.deliveryMethod.id !== null
        ? String(draft.deliveryMethod.id)
        : "";
    setSelectedDeliveryMethodId(currentId);
    setInitialSelectedDeliveryMethodId(currentId);
    setDeliveryMethodSearch("");
    setDeliveryMethodSelectorError(null);
    setIsDeliveryMethodSelectorOpen(true);
    void loadDeliveryMethods();
  }

  function applySelectedDeliveryMethod() {
    if (isReadOnly) {
      return;
    }
    if (!canApplySelectedDeliveryMethod) {
      return;
    }
    const selectedId = Number(selectedDeliveryMethodId);
    if (!Number.isFinite(selectedId)) {
      setDeliveryMethodSelectorError("Please select a delivery method.");
      return;
    }

    const matched = deliveryMethodOptions.find((item) => item.id === selectedId);
    if (!matched) {
      setDeliveryMethodSelectorError("Selected delivery method was not found.");
      return;
    }

    updateDraft({
      deliveryMethod: {
        id: matched.id,
        name: matched.name,
        url: matched.url,
        username: matched.username,
        password: matched.password,
        passwordStatus: matched.passwordStatus,
        deadline: matched.deadline,
        note: matched.note,
      },
      station: {
        ...draft.station,
        deliveryMethodId: matched.id ?? null,
      },
    });
    setIsDeliveryMethodSelectorOpen(false);
    setIsDeliveryMethodSelectorDiscardDialogOpen(false);
  }

  function closeDeliveryMethodEditor(force = false) {
    if (!force && hasDeliveryMethodEditorChanges) {
      setIsDeliveryMethodEditorDiscardDialogOpen(true);
      return;
    }
    setIsDeliveryMethodEditorOpen(false);
    setIsDeliveryMethodEditorDiscardDialogOpen(false);
  }

  function handleDeliveryMethodEditorOpenChange(nextOpen: boolean) {
    if (isReadOnly && nextOpen) {
      return;
    }
    const allowClose = canModalClose({
      nextOpen,
      hasUnsavedChanges: hasDeliveryMethodEditorChanges,
    });
    if (!allowClose) {
      if (!nextOpen && hasDeliveryMethodEditorChanges) {
        setIsDeliveryMethodEditorDiscardDialogOpen(true);
      }
      return;
    }
    setIsDeliveryMethodEditorOpen(nextOpen);
  }

  function openCreateDeliveryMethodEditor() {
    if (isReadOnly) {
      return;
    }
    const initialForm = buildDeliveryMethodEditorForm();
    setDeliveryMethodEditorForm(initialForm);
    setDeliveryMethodEditorBaseline(initialForm);
    setDeliveryMethodEditorError(null);
    setDeliveryMethodUsage(null);
    setDeliveryMethodUsageError(null);
    setIsDeliveryMethodEditorOpen(true);
  }

  function openEditDeliveryMethodEditor() {
    if (isReadOnly) {
      return;
    }
    if (!asString(draft.deliveryMethod.name) && draft.deliveryMethod.id === null) {
      setSubmitError("Select or add a delivery method first.");
      return;
    }
    const initialForm = buildDeliveryMethodEditorForm(draft.deliveryMethod);
    setDeliveryMethodEditorForm(initialForm);
    setDeliveryMethodEditorBaseline(initialForm);
    setDeliveryMethodEditorError(null);
    setDeliveryMethodUsage(null);
    setDeliveryMethodUsageError(null);
    setIsDeliveryMethodEditorOpen(true);
    void loadDeliveryMethodUsage(
      draft.deliveryMethod.id ?? null,
      asString(draft.station.code).toUpperCase(),
    );
  }

  function saveDeliveryMethodEditor() {
    if (isReadOnly) {
      return;
    }
    if (!asString(deliveryMethodEditorForm.name)) {
      setDeliveryMethodEditorError("Name is required.");
      return;
    }
    if (!asString(deliveryMethodEditorForm.url)) {
      setDeliveryMethodEditorError("URL is required.");
      return;
    }
    if (!asString(deliveryMethodEditorForm.username)) {
      setDeliveryMethodEditorError("Username is required.");
      return;
    }
    if (!asString(deliveryMethodEditorForm.deadline)) {
      setDeliveryMethodEditorError("Deadline is required.");
      return;
    }
    if (!canApplyDeliveryMethodEditor) {
      return;
    }

    const nextPassword = asString(deliveryMethodEditorForm.password);
    const existingPasswordStatus = asString(
      draft.deliveryMethod.id === deliveryMethodEditorForm.id ? draft.deliveryMethod.passwordStatus : "",
    );
    const nextDeliveryMethod: StationDraftDeliveryMethod = {
      id: deliveryMethodEditorForm.id,
      name: asString(deliveryMethodEditorForm.name),
      url: asString(deliveryMethodEditorForm.url),
      username: asString(deliveryMethodEditorForm.username),
      password: nextPassword,
      passwordStatus: nextPassword
        ? DELIVERY_METHOD_PASSWORD_STORED
        : existingPasswordStatus || DELIVERY_METHOD_PASSWORD_NOT_SET,
      deadline: asString(deliveryMethodEditorForm.deadline) || DEFAULT_DEADLINE,
      note: asString(deliveryMethodEditorForm.note),
    };

    updateDraft({
      deliveryMethod: nextDeliveryMethod,
      station: {
        ...draft.station,
        deliveryMethodId: nextDeliveryMethod.id ?? null,
      },
    });
    if (nextDeliveryMethod.id !== null) {
      setDeliveryMethodOptions((current) => {
        const nextOptions = upsertDeliveryMethodOption(current, nextDeliveryMethod);
        deliveryMethodOptionsMemoryCache.set(tenantCacheScope, nextOptions);
        writeBrowserCache(
          deliveryMethodOptionsCacheKey,
          toBrowserCachedDeliveryMethodOptions(nextOptions),
          DELIVERY_METHOD_OPTIONS_CACHE_TTL_MS,
        );
        return nextOptions;
      });
    }
    setIsDeliveryMethodEditorOpen(false);
    setIsDeliveryMethodEditorDiscardDialogOpen(false);
  }

  function closeContactEditor(force = false) {
    if (!force && hasContactEditorChanges) {
      setIsContactEditorDiscardDialogOpen(true);
      return;
    }
    setIsContactEditorOpen(false);
    setIsContactEditorDiscardDialogOpen(false);
  }

  function handleContactEditorOpenChange(nextOpen: boolean) {
    if (isReadOnly && nextOpen) {
      return;
    }
    const allowClose = canModalClose({
      nextOpen,
      hasUnsavedChanges: hasContactEditorChanges,
    });
    if (!allowClose) {
      if (!nextOpen && hasContactEditorChanges) {
        setIsContactEditorDiscardDialogOpen(true);
      }
      return;
    }
    setIsContactEditorOpen(nextOpen);
  }

  function openCreateContactEditor() {
    if (isReadOnly) {
      return;
    }
    const initialForm = buildContactEditorForm();
    setContactEditorIndex(null);
    setContactEditorForm(initialForm);
    setContactEditorBaseline(initialForm);
    setContactEditorError(null);
    setIsContactEditorFullNameManuallyEdited(false);
    setIsContactEditorOpen(true);
  }

  function openEditContactEditor(index: number) {
    if (isReadOnly) {
      return;
    }
    const contact = draft.contacts[index];
    if (!contact) {
      return;
    }
    const initialForm = buildContactEditorForm(contact);
    setContactEditorIndex(index);
    setContactEditorForm(initialForm);
    setContactEditorBaseline(initialForm);
    setContactEditorError(null);
    setIsContactEditorFullNameManuallyEdited(false);
    setIsContactEditorOpen(true);
  }

  function updateContactEditorNameField(field: "firstName" | "lastName", value: string) {
    setContactEditorForm((current) => {
      const next = {
        ...current,
        [field]: value,
      };
      if (!isContactEditorFullNameManuallyEdited) {
        next.fullName = composeFullName(next.firstName, next.lastName);
      }
      return next;
    });
  }

  function applyContactEditorFullNameFromUserInput(rawValue: string) {
    const normalizedFullName = toNameCase(rawValue);
    const parsed = parseNameParts(normalizedFullName);
    setContactEditorForm((current) => ({
      ...current,
      fullName: normalizedFullName,
      firstName: toNameCase(parsed.firstName),
      lastName: toNameCase(parsed.lastName),
    }));
    setIsContactEditorFullNameManuallyEdited(false);
  }

  function saveContactEditor() {
    if (isReadOnly) {
      return;
    }
    const normalizedEmail = asString(contactEditorForm.email).toLowerCase();
    if (!normalizedEmail) {
      setContactEditorError("Email is required.");
      return;
    }
    if (!canApplyContactEditor) {
      return;
    }

    const normalizedType = normalizeContactType(contactEditorForm.contactType);
    const normalizedFullName = asString(contactEditorForm.fullName);

    const duplicateIndex = draft.contacts.findIndex((contact, index) => {
      if (contactEditorIndex !== null && index === contactEditorIndex) {
        return false;
      }
      const emailMatch = asString(contact.email).toLowerCase() === normalizedEmail;
      const sameContactId =
        contact.contactId !== null &&
        contact.contactId !== undefined &&
        contactEditorIndex !== null &&
        draft.contacts[contactEditorIndex]?.contactId === contact.contactId;
      return emailMatch && !sameContactId;
    });
    if (duplicateIndex >= 0) {
      setContactEditorError("A contact with this email is already linked in this station draft.");
      return;
    }

    const baseContact =
      contactEditorIndex !== null && draft.contacts[contactEditorIndex]
        ? draft.contacts[contactEditorIndex]
        : null;

    const nextContact: StationDraftContact = {
      clientKey:
        baseContact?.contactId !== null && baseContact?.contactId !== undefined
          ? null
          : asString(baseContact?.clientKey) || createContactClientKey(normalizedEmail || "new"),
      linkId: baseContact?.linkId ?? null,
      linkNote: asNullableString(contactEditorForm.linkNote),
      contactType: normalizedType,
      contactId: baseContact?.contactId ?? null,
      firstName: asNullableString(contactEditorForm.firstName),
      lastName: asNullableString(contactEditorForm.lastName),
      fullName: normalizedFullName,
      email: normalizedEmail,
      office: asNullableString(contactEditorForm.office),
      cell: asNullableString(contactEditorForm.cell),
      company: asNullableString(contactEditorForm.company),
      jobTitle: asNullableString(contactEditorForm.jobTitle),
      note: asNullableString(contactEditorForm.note),
      primaryContact: Boolean(contactEditorForm.primaryContact),
    };

    const nextContacts = [...draft.contacts];
    if (contactEditorIndex === null) {
      nextContacts.push(nextContact);
    } else {
      nextContacts[contactEditorIndex] = nextContact;
    }

    updateContacts(nextContacts);
    setIsContactEditorOpen(false);
    setIsContactEditorDiscardDialogOpen(false);
  }

  function closeAddExistingContact(force = false) {
    if (!force && hasAddExistingContactChanges) {
      setIsAddExistingContactDiscardDialogOpen(true);
      return;
    }
    setIsAddExistingContactOpen(false);
    setIsAddExistingContactDiscardDialogOpen(false);
  }

  function handleAddExistingContactOpenChange(nextOpen: boolean) {
    if (isReadOnly && nextOpen) {
      return;
    }
    const allowClose = canModalClose({
      nextOpen,
      hasUnsavedChanges: hasAddExistingContactChanges,
    });
    if (!allowClose) {
      if (!nextOpen && hasAddExistingContactChanges) {
        setIsAddExistingContactDiscardDialogOpen(true);
      }
      return;
    }
    setIsAddExistingContactOpen(nextOpen);
  }

  function removeContact(index: number) {
    if (isReadOnly) {
      return;
    }
    const nextContacts = draft.contacts.filter((_, itemIndex) => itemIndex !== index);
    updateContacts(nextContacts);
  }

  function openAddExistingContactDialog() {
    if (isReadOnly) {
      return;
    }
    const initialState = buildAddExistingContactFormState({
      selectedExistingContactId: "",
      selectedExistingContactType: DEFAULT_CONTACT_TYPE,
      selectedExistingPrimaryContact: false,
    });
    setIsAddExistingContactOpen(true);
    setSelectedExistingContactId(initialState.selectedExistingContactId);
    setSelectedExistingContactType(initialState.selectedExistingContactType);
    setSelectedExistingPrimaryContact(initialState.selectedExistingPrimaryContact);
    setExistingContactSearch("");
    setAddExistingContactBaseline(initialState);
    setExistingContactsError(null);
    void loadExistingContacts();
  }

  useEffect(() => {
    if (!isAddExistingContactOpen) {
      return;
    }
    const hasCurrent = filteredExistingContacts.some((item) => String(item.id) === selectedExistingContactId);
    if (!hasCurrent) {
      setSelectedExistingContactId("");
    }
  }, [filteredExistingContacts, isAddExistingContactOpen, selectedExistingContactId]);

  function addExistingContactToDraft() {
    if (isReadOnly) {
      return;
    }
    if (!canAddExistingContact) {
      return;
    }
    const selectedId = Number(selectedExistingContactId);
    if (!Number.isFinite(selectedId)) {
      setExistingContactsError("Select an existing contact.");
      return;
    }

    const selectedContact = filteredExistingContacts.find((item) => item.id === selectedId);
    if (!selectedContact) {
      setExistingContactsError("Selected contact was not found.");
      return;
    }

    const fullName = selectedContact.fullName || [selectedContact.firstName, selectedContact.lastName].filter(Boolean).join(" ").trim();

    const nextContact: StationDraftContact = {
      clientKey: null,
      linkId: null,
      linkNote: null,
      contactType: normalizeContactType(selectedExistingContactType),
      contactId: selectedContact.id,
      firstName: selectedContact.firstName || null,
      lastName: selectedContact.lastName || null,
      fullName,
      email: selectedContact.email || null,
      office: selectedContact.office || null,
      cell: selectedContact.cell || null,
      company: selectedContact.company || null,
      jobTitle: selectedContact.jobTitle || null,
      note: selectedContact.note || null,
      primaryContact: Boolean(selectedExistingPrimaryContact),
    };

    updateContacts([...draft.contacts, nextContact]);
    setIsAddExistingContactOpen(false);
    setIsAddExistingContactDiscardDialogOpen(false);
  }

  async function handleCopyContact(index: number) {
    const contact = draft.contacts[index];
    if (!contact) {
      return;
    }

    const copyValue = buildContactCopyValue(contact);
    if (!copyValue) {
      toast.error("Could not copy contact", "Contact is missing an email address.");
      return;
    }

    try {
      await copyContactToClipboard(copyValue);
      toast.success("Copied contact");
    } catch {
      toast.error("Could not copy contact");
    }
  }

  async function handleSubmit() {
    if (isReadOnly) {
      return;
    }
    if (!canSubmit) {
      return;
    }

    if (validationError) {
      setSubmitError(validationError);
      return;
    }

    setSubmitError(null);
    setIsSubmitting(true);

    try {
      const normalizedStationCode = draft.station.code.trim().toUpperCase();
      const bundledPayload = buildBundledPayload(draft);
      const saveUrl =
        mode === "create"
          ? STATION_DETAIL_CREATE_URL
          : `${STATION_DETAIL_BASE_URL}/${encodeURIComponent(normalizedStationCode)}/detail`;
      const saveMethod = mode === "create" ? "POST" : "PUT";

      const saveResponse = await requestJson(saveUrl, {
        method: saveMethod,
        headers,
        body: bundledPayload,
        successToast: false,
        errorToast: false,
      });

      const parsedSavedDraft = parseStationDetailDraft(saveResponse, normalizedStationCode);
      const nextDraft = parsedSavedDraft
        ? {
            ...parsedSavedDraft,
            ...syncContactsAndLinks(parsedSavedDraft.contacts, parsedSavedDraft.station.code),
            deliveryMethod: {
              ...parsedSavedDraft.deliveryMethod,
              password:
                asString(parsedSavedDraft.deliveryMethod.password) || asString(draft.deliveryMethod.password),
              passwordStatus:
                parsedSavedDraft.deliveryMethod.passwordStatus ||
                (asString(parsedSavedDraft.deliveryMethod.password) || asString(draft.deliveryMethod.password)
                  ? DELIVERY_METHOD_PASSWORD_STORED
                  : DELIVERY_METHOD_PASSWORD_NOT_SET),
            },
          }
        : {
            ...draft,
            station: {
              ...draft.station,
              code: normalizedStationCode,
            },
            ...syncContactsAndLinks(draft.contacts, normalizedStationCode),
            deliveryMethod: {
              ...draft.deliveryMethod,
              password: asString(draft.deliveryMethod.password),
              passwordStatus:
                draft.deliveryMethod.passwordStatus ||
                (asString(draft.deliveryMethod.password)
                  ? DELIVERY_METHOD_PASSWORD_STORED
                  : DELIVERY_METHOD_PASSWORD_NOT_SET),
            },
          };

      writeBrowserCache(
        `station-detail:${normalizedStationCode}`,
        redactDeliveryMethodPassword(nextDraft),
        STATION_DETAIL_CACHE_TTL_MS,
        { source: "network" },
      );
      setDetailCacheStatus({
        source: "network",
        fetchedAt: Date.now(),
      });

      setDraft(nextDraft);
      setOriginalDraft(nextDraft);
      setDeliveryMethodOptions((current) => {
        const nextOptions = upsertDeliveryMethodOption(current, nextDraft.deliveryMethod);
        deliveryMethodOptionsMemoryCache.set(tenantCacheScope, nextOptions);
        writeBrowserCache(
          deliveryMethodOptionsCacheKey,
          toBrowserCachedDeliveryMethodOptions(nextOptions),
          DELIVERY_METHOD_OPTIONS_CACHE_TTL_MS,
        );
        return nextOptions;
      });
      existingContactsMemoryCache.delete(tenantCacheScope);
      removeBrowserCache(existingContactsCacheKey);
      deliveryMethodUsageMemoryCache.clear();

      toast.success(
        mode === "create" ? "Station created" : "Station updated",
        `${normalizedStationCode} ${mode === "create" ? "was created" : "was updated"} successfully.`,
      );

      await onSuccess?.({
        mode,
        stationCode: normalizedStationCode,
        stationName: nextDraft.station.name.trim(),
        repContacts: buildRepContactSummaries(nextDraft.contacts),
      });
      onOpenChange(false);
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : mode === "create"
            ? "Unable to create station."
            : "Unable to update station.";
      setSubmitError(message);
      toast.error(mode === "create" ? "Create failed" : "Update failed", message);
    } finally {
      setIsSubmitting(false);
    }
  }

  const selectedDeliveryMethodOption = useMemo(
    () =>
      selectedDeliveryMethodId
        ? deliveryMethodOptions.find((option) => String(option.id) === selectedDeliveryMethodId) ?? null
        : null,
    [deliveryMethodOptions, selectedDeliveryMethodId],
  );

  const filteredDeliveryMethodOptions = useMemo(() => {
    const query = asString(deliveryMethodSearch).toLowerCase();
    if (!query) {
      return deliveryMethodOptions;
    }
    return deliveryMethodOptions.filter((item) => item.searchText.includes(query));
  }, [deliveryMethodOptions, deliveryMethodSearch]);

  const modalTitle = isEditMode ? "Edit Station" : "Create Station";
  const description = isEditMode
    ? "Update station, delivery method, and linked contacts in one draft."
    : "Create a station with delivery method and contacts using one local draft.";
  const submitLabel = isEditMode ? "Save Changes" : "Create Station";
  const detailStatusText = isEditMode
    ? isLoadingDetail
      ? isRefreshingDetail
        ? "Refreshing..."
        : "Loading..."
      : detailCacheStatus
        ? `Data source: ${detailCacheStatus.source}. Last updated ${formatRelativeTime(detailCacheStatus.fetchedAt)}.`
        : "No cached data yet"
    : null;
  const deliveryMethodsStatusText = buildModalCacheStatusText({
    isLoading: isLoadingDeliveryMethods,
    status: deliveryMethodsCacheStatus,
  });
  const existingContactsStatusText = buildModalCacheStatusText({
    isLoading: isLoadingExistingContacts,
    status: existingContactsCacheStatus,
  });
  const shouldShowSubmitButton = canEdit && (isSubmitting || canSubmit);

  return (
    <>
      <Dialog open={open} onOpenChange={handleDialogOpenChange}>
        <DialogContent
          className="flex max-h-[90vh] w-[min(88vw,1320px)] max-w-none flex-col overflow-hidden rounded-xl bg-white px-8 py-6"
          onEscapeKeyDown={(event) => {
            if (isSubmitting) {
              event.preventDefault();
            }
          }}
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
        <DialogClose
          className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none"
          aria-label="Close station modal"
          disabled={isSubmitting}
        >
          <X className="size-4" />
        </DialogClose>

          <DialogHeader>
            <DialogTitle>{modalTitle}</DialogTitle>
            <DialogDescription>{description}</DialogDescription>
          </DialogHeader>

          <div className="mt-4 min-h-0 flex-1 overflow-y-auto pr-1">
            {isEditMode && !isDetailReady ? (
              <div className="flex min-h-[420px] flex-col items-center justify-center gap-3 text-center">
                {detailError ? (
                  <p className="text-sm text-amber-700">{detailError}</p>
                ) : (
                  <>
                    <Loader2 className="size-5 animate-spin text-slate-500" />
                    <p className="text-sm text-slate-600">Loading station detail...</p>
                  </>
                )}
              </div>
            ) : (
              <div className="grid items-start gap-8 xl:gap-10 [grid-template-columns:repeat(auto-fit,minmax(min(100%,340px),1fr))]">
                <StationBasicInfoSection
                  station={draft.station}
                  isEditMode={isEditMode}
                  isSubmitting={isSubmitting}
                  isReadOnly={isReadOnly}
                  onChange={(next) => {
                    updateStation(next);
                  }}
                />

                <StationDeliveryMethodSection
                  deliveryMethod={draft.deliveryMethod}
                  isSubmitting={isSubmitting}
                  isReadOnly={isReadOnly}
                  onSelectDeliveryMethod={openDeliveryMethodSelector}
                  onAddDeliveryMethod={openCreateDeliveryMethodEditor}
                  onEditDeliveryMethod={openEditDeliveryMethodEditor}
                />

                <StationContactsSection
                  contacts={draft.contacts}
                  isSubmitting={isSubmitting}
                  isReadOnly={isReadOnly}
                  onAddExistingContact={openAddExistingContactDialog}
                  onCreateContact={openCreateContactEditor}
                  onEditContact={openEditContactEditor}
                  onRemoveContact={removeContact}
                  onCopyContact={(index) => {
                    void handleCopyContact(index);
                  }}
                />
              </div>
            )}

            {submitError ? <p className="mt-2 text-sm text-rose-600">{submitError}</p> : null}
          </div>

          <DialogFooter>
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
          </DialogFooter>

          {detailStatusText ? (
            <footer className="shrink-0 border-t border-slate-100 bg-white px-0 pt-2 pb-[calc(1rem+env(safe-area-inset-bottom))]">
              <CacheStatusChip
                text={detailStatusText}
                onRefresh={() => {
                  if (!isLoadingDetail && !isSubmitting && isEditMode && !hasUnsavedChanges) {
                    setDetailRefreshToken((current) => current + 1);
                  }
                }}
                disabled={!isEditMode || isLoadingDetail || isSubmitting || hasUnsavedChanges}
                refreshing={isRefreshingDetail}
                refreshLabel="Refresh station detail"
                tooltipText={
                  hasUnsavedChanges
                    ? "Save or discard your edits before refreshing station detail."
                    : "Click to refresh this data"
                }
              />
            </footer>
          ) : null}
          {hasDeferredDetailUpdate ? (
            <p className="mt-2 text-sm text-amber-700">
              Newer station detail is available and will apply after your current edits are saved or discarded.
            </p>
          ) : null}
        </DialogContent>

        <UnsavedChangesDialog
          open={isDiscardDialogOpen}
          onKeepEditing={() => {
            setIsDiscardDialogOpen(false);
          }}
          onDiscardChanges={() => {
            setIsDiscardDialogOpen(false);
            onOpenChange(false);
          }}
        />
      </Dialog>

      <Dialog open={isDeliveryMethodSelectorOpen} onOpenChange={handleDeliveryMethodSelectorOpenChange}>
        <DialogContent
          className="flex max-h-[85vh] max-w-[640px] flex-col overflow-hidden rounded-xl bg-white p-5"
          onInteractOutside={(event) => {
            if (isAppDropdownInteractionEvent(event)) {
              event.preventDefault();
              return;
            }
            if (shouldBlockOutsideClose({ hasUnsavedChanges: hasDeliveryMethodSelectorChanges })) {
              event.preventDefault();
            }
          }}
        >
          <button
            type="button"
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            aria-label="Close select delivery method modal"
            onClick={() => closeDeliveryMethodSelector()}
          >
            <X className="size-4" />
          </button>
          <DialogHeader>
            <DialogTitle>Select Delivery Method</DialogTitle>
            <DialogDescription>
              Choose an existing delivery method to use for this station draft.
            </DialogDescription>
          </DialogHeader>

          <div className="mt-4 min-h-0 flex-1 overflow-y-auto pr-1">
            <div className="space-y-3">
            <div className="space-y-1">
              <p className={SHARED_LABEL_CLASS}>Selected Method</p>
              {selectedDeliveryMethodOption ? (
                <div className="space-y-1 rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
                  <p className="font-medium text-slate-800">
                    {selectedDeliveryMethodOption.name || "Unnamed"} (#{selectedDeliveryMethodOption.id})
                  </p>
                  <p className="break-all text-xs text-slate-600">
                    URL: {selectedDeliveryMethodOption.url || "-"}
                  </p>
                  <p className="text-xs text-slate-600">
                    Username: {selectedDeliveryMethodOption.username || "-"}
                  </p>
                  <p className="text-xs text-slate-600">
                    Password:{" "}
                    {selectedDeliveryMethodOption.password ||
                      (selectedDeliveryMethodOption.passwordStatus === DELIVERY_METHOD_PASSWORD_STORED
                        ? "Stored (hidden)"
                        : "-")}
                  </p>
                  <p className="text-xs text-slate-600">
                    Deadline: {selectedDeliveryMethodOption.deadline || "-"}
                  </p>
                </div>
              ) : (
                <p className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
                  None selected
                </p>
              )}
            </div>

            <div className="space-y-1">
              <p className={SHARED_LABEL_CLASS}>Search</p>
              <div className="relative">
                <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-slate-400" />
                <Input
                  value={deliveryMethodSearch}
                  onChange={(event) => setDeliveryMethodSearch(event.target.value)}
                  placeholder="Search delivery methods"
                  className="pl-9"
                  disabled={isLoadingDeliveryMethods || isReadOnly}
                />
              </div>
            </div>

            <div className="space-y-1">
              <p className={SHARED_LABEL_CLASS}>Available Methods</p>
              <div className="max-h-[48vh] overflow-y-auto rounded-md border border-slate-200 bg-white">
                {isLoadingDeliveryMethods ? (
                  <div className="flex items-center justify-center gap-2 px-3 py-6 text-sm text-slate-500">
                    <Loader2 className="size-4 animate-spin" />
                    Loading delivery methods...
                  </div>
                ) : filteredDeliveryMethodOptions.length ? (
                  <ul className="p-1">
                    {filteredDeliveryMethodOptions.map((option) => {
                      const isSelected = String(option.id) === selectedDeliveryMethodId;
                      return (
                        <li key={option.id}>
                          <button
                            type="button"
                            className={`flex w-full items-start justify-between gap-3 rounded-md px-3 py-2 text-left text-sm transition-colors ${
                              isSelected ? "bg-blue-50 text-blue-700" : "text-slate-700 hover:bg-slate-100"
                            }`}
                            onClick={() => setSelectedDeliveryMethodId(String(option.id))}
                            disabled={isReadOnly}
                          >
                            <span className="min-w-0 space-y-1">
                              <span className="block truncate font-medium">
                                {option.name || "Unnamed"} (#{option.id})
                              </span>
                              <span className="block text-xs text-slate-600">
                                URL: {option.url || "-"}
                              </span>
                              <span className="block text-xs text-slate-600">
                                Username: {option.username || "-"}
                              </span>
                              <span className="block text-xs text-slate-600">
                                Password:{" "}
                                {option.password ||
                                  (option.passwordStatus === DELIVERY_METHOD_PASSWORD_STORED ? "Stored (hidden)" : "-")}
                              </span>
                              <span className="block text-xs text-slate-600">
                                Deadline: {option.deadline || "-"}
                              </span>
                            </span>
                            {isSelected ? <Check className="size-4 shrink-0" /> : null}
                          </button>
                        </li>
                      );
                    })}
                  </ul>
                ) : (
                  <p className="px-3 py-6 text-center text-sm text-slate-500">No delivery methods found.</p>
                )}
              </div>
            </div>

            {deliveryMethodSelectorError ? (
              <p className="text-sm text-rose-600">{deliveryMethodSelectorError}</p>
            ) : null}

            {canEdit && canApplySelectedDeliveryMethod ? (
              <div className="flex justify-end">
                <Button size="sm" onClick={applySelectedDeliveryMethod} disabled={isReadOnly}>
                  Use
                </Button>
              </div>
            ) : null}
            </div>
          </div>

          <footer className="shrink-0 border-t border-slate-100 bg-white px-0 pt-2 pb-[calc(1rem+env(safe-area-inset-bottom))]">
            <CacheStatusChip
              text={deliveryMethodsStatusText}
              onRefresh={() => {
                if (!isLoadingDeliveryMethods) {
                  void loadDeliveryMethods(true);
                }
              }}
              disabled={isLoadingDeliveryMethods}
              refreshing={isLoadingDeliveryMethods}
              refreshLabel="Refresh delivery methods"
              tooltipText="Click to refresh this data"
            />
          </footer>
        </DialogContent>
      </Dialog>

      <Dialog open={isDeliveryMethodEditorOpen} onOpenChange={handleDeliveryMethodEditorOpenChange}>
        <DialogContent
          className="flex max-h-[90vh] max-w-[700px] flex-col overflow-hidden rounded-xl bg-white p-5"
          onInteractOutside={(event) => {
            if (isAppDropdownInteractionEvent(event)) {
              event.preventDefault();
              return;
            }
            if (shouldBlockOutsideClose({ hasUnsavedChanges: hasDeliveryMethodEditorChanges })) {
              event.preventDefault();
            }
          }}
        >
          <button
            type="button"
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            aria-label="Close delivery method editor modal"
            onClick={() => closeDeliveryMethodEditor()}
          >
            <X className="size-4" />
          </button>
          <DialogHeader>
            <DialogTitle>
              {deliveryMethodEditorForm.id !== null ? "Edit Delivery Method" : "Add Delivery Method"}
            </DialogTitle>
            <DialogDescription>
              Update delivery method details in a separate workflow, then apply to this station draft.
            </DialogDescription>
          </DialogHeader>

          <div className="mt-4 min-h-0 flex-1 overflow-y-auto pr-1">
            <div className="space-y-3">
            <LabeledField
              label={
                <>
                  Name<RequiredMark />
                </>
              }
            >
              <Input
                value={deliveryMethodEditorForm.name}
                onChange={(event) => {
                  setDeliveryMethodEditorForm((current) => ({ ...current, name: event.target.value }));
                  setDeliveryMethodEditorError(null);
                }}
                placeholder="Delivery method name"
                maxLength={255}
                disabled={isReadOnly}
              />
            </LabeledField>
            <LabeledField
              label={
                <>
                  URL<RequiredMark />
                </>
              }
            >
              <Input
                value={deliveryMethodEditorForm.url}
                onChange={(event) => {
                  setDeliveryMethodEditorForm((current) => ({ ...current, url: event.target.value }));
                  setDeliveryMethodEditorError(null);
                }}
                placeholder="URL"
                maxLength={2048}
                disabled={isReadOnly}
              />
            </LabeledField>
            <LabeledField
              label={
                <>
                  Username<RequiredMark />
                </>
              }
            >
              <Input
                value={deliveryMethodEditorForm.username}
                onChange={(event) => {
                  setDeliveryMethodEditorForm((current) => ({ ...current, username: event.target.value }));
                  setDeliveryMethodEditorError(null);
                }}
                placeholder="Username"
                maxLength={255}
                disabled={isReadOnly}
              />
            </LabeledField>
            <LabeledField label="Password">
              <Input
                type="text"
                value={deliveryMethodEditorForm.password}
                onChange={(event) => {
                  setDeliveryMethodEditorForm((current) => ({ ...current, password: event.target.value }));
                }}
                placeholder="Password (optional)"
                disabled={isReadOnly}
              />
            </LabeledField>
            <LabeledField
              label={
                <>
                  Deadline<RequiredMark />
                </>
              }
            >
              <Input
                value={deliveryMethodEditorForm.deadline}
                onChange={(event) => {
                  setDeliveryMethodEditorForm((current) => ({ ...current, deadline: event.target.value }));
                  setDeliveryMethodEditorError(null);
                }}
                placeholder="Deadline"
                maxLength={50}
                disabled={isReadOnly}
              />
            </LabeledField>
            <LabeledField label="Note" alignStart>
              <Textarea
                value={deliveryMethodEditorForm.note}
                onChange={(event) => {
                  setDeliveryMethodEditorForm((current) => ({ ...current, note: event.target.value }));
                }}
                placeholder="Note"
                maxLength={2048}
                disabled={isReadOnly}
              />
            </LabeledField>

            {deliveryMethodEditorForm.id !== null ? (
              <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2">
                <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">Also linked stations</p>
                {isLoadingDeliveryMethodUsage ? (
                  <p className="mt-2 text-xs text-slate-500">Loading station usage...</p>
                ) : deliveryMethodUsageError ? (
                  <p className="mt-2 text-xs text-amber-700">{deliveryMethodUsageError}</p>
                ) : deliveryMethodUsage && deliveryMethodUsage.length ? (
                  <ul className="mt-2 grid grid-cols-1 gap-1.5 text-xs text-slate-700 sm:grid-cols-2">
                    {deliveryMethodUsage.map((usage) => (
                      <li
                        key={usage.stationCode}
                        className="rounded-md border border-slate-200/90 bg-white px-2.5 py-2 shadow-[0_1px_2px_rgba(15,23,42,0.05)] transition hover:border-blue-200 hover:shadow-sm"
                      >
                        <div className="flex items-start justify-between gap-2">
                          <span className="font-semibold text-slate-800">{usage.stationCode}</span>
                          {usage.mediaType ? (
                            <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] font-medium text-slate-700">
                              {usage.mediaType}
                            </span>
                          ) : null}
                        </div>
                        <p className="mt-0.5 truncate text-[11px] text-slate-500">{usage.stationName || "-"}</p>
                        <p className="mt-1 text-[11px] text-slate-600">
                          <span className="text-slate-500">EstNum: </span>
                          <span>{usage.estNums.length ? usage.estNums.join(", ") : "-"}</span>
                        </p>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="mt-2 text-xs text-slate-500">No other stations in the current account use this method.</p>
                )}
              </div>
            ) : null}

            {deliveryMethodEditorError ? <p className="text-sm text-rose-600">{deliveryMethodEditorError}</p> : null}

            {canEdit && canApplyDeliveryMethodEditor ? (
              <div className="flex justify-end">
                <Button size="sm" onClick={saveDeliveryMethodEditor} disabled={isReadOnly}>
                  Apply
                </Button>
              </div>
            ) : null}
            </div>
          </div>

          <footer className="shrink-0 border-t border-slate-100 bg-white px-0 pt-2 pb-[calc(1rem+env(safe-area-inset-bottom))]">
            <CacheStatusChip
              text={deliveryMethodsStatusText}
              onRefresh={() => {
                if (!isLoadingDeliveryMethods) {
                  void loadDeliveryMethods(true);
                }
              }}
              disabled={isLoadingDeliveryMethods}
              refreshing={isLoadingDeliveryMethods}
              refreshLabel="Refresh delivery methods"
              tooltipText="Click to refresh this data"
            />
          </footer>
        </DialogContent>
      </Dialog>

      <Dialog open={isContactEditorOpen} onOpenChange={handleContactEditorOpenChange}>
        <DialogContent
          className="max-h-[90vh] max-w-[760px] overflow-y-auto rounded-xl bg-white p-5"
          onInteractOutside={(event) => {
            if (isAppDropdownInteractionEvent(event)) {
              event.preventDefault();
              return;
            }
            if (shouldBlockOutsideClose({ hasUnsavedChanges: hasContactEditorChanges })) {
              event.preventDefault();
            }
          }}
        >
          <button
            type="button"
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            aria-label="Close contact editor modal"
            onClick={() => closeContactEditor()}
          >
            <X className="size-4" />
          </button>
          <DialogHeader>
            <DialogTitle>{contactEditorIndex === null ? "Create Contact" : "Edit Contact"}</DialogTitle>
            <DialogDescription>
              Contact changes remain local in this station draft until you save the station.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-3">
            <LabeledField label="Full Name">
              <Input
                value={contactEditorForm.fullName}
                onChange={(event) => {
                  setIsContactEditorFullNameManuallyEdited(true);
                  setContactEditorForm((current) => ({ ...current, fullName: event.target.value }));
                }}
                onBlur={(event) => applyContactEditorFullNameFromUserInput(event.target.value)}
                onPaste={(event) => {
                  const pasted = event.clipboardData.getData("text");
                  if (!pasted.trim()) {
                    return;
                  }
                  event.preventDefault();
                  applyContactEditorFullNameFromUserInput(pasted);
                }}
                placeholder="Full Name"
                autoComplete="off"
                maxLength={255}
                disabled={isReadOnly}
              />
            </LabeledField>
            <LabeledField label="First Name">
              <Input
                value={contactEditorForm.firstName}
                onChange={(event) => updateContactEditorNameField("firstName", event.target.value)}
                onBlur={(event) => updateContactEditorNameField("firstName", toNameCase(event.target.value))}
                placeholder="First Name"
                autoComplete="off"
                maxLength={255}
                disabled={isReadOnly}
              />
            </LabeledField>
            <LabeledField label="Last Name">
              <Input
                value={contactEditorForm.lastName}
                onChange={(event) => updateContactEditorNameField("lastName", event.target.value)}
                onBlur={(event) => updateContactEditorNameField("lastName", toNameCase(event.target.value))}
                placeholder="Last Name"
                autoComplete="off"
                maxLength={255}
                disabled={isReadOnly}
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
                value={contactEditorForm.email}
                onChange={(event) => {
                  setContactEditorForm((current) => ({ ...current, email: event.target.value }));
                  setContactEditorError(null);
                }}
                placeholder="Email"
                maxLength={255}
                disabled={isReadOnly}
              />
            </LabeledField>
            <LabeledField label="Office">
              <Input
                value={contactEditorForm.office}
                onChange={(event) => {
                  setContactEditorForm((current) => ({ ...current, office: event.target.value }));
                }}
                placeholder="Office"
                maxLength={35}
                disabled={isReadOnly}
              />
            </LabeledField>
            <LabeledField label="Cell">
              <Input
                value={contactEditorForm.cell}
                onChange={(event) => {
                  setContactEditorForm((current) => ({ ...current, cell: event.target.value }));
                }}
                placeholder="Cell"
                maxLength={20}
                disabled={isReadOnly}
              />
            </LabeledField>
            <LabeledField label="Company">
              <Input
                value={contactEditorForm.company}
                onChange={(event) => {
                  setContactEditorForm((current) => ({ ...current, company: event.target.value }));
                }}
                placeholder="Company"
                maxLength={255}
                disabled={isReadOnly}
              />
            </LabeledField>
            <LabeledField label="Job Title">
              <Input
                value={contactEditorForm.jobTitle}
                onChange={(event) => {
                  setContactEditorForm((current) => ({ ...current, jobTitle: event.target.value }));
                }}
                placeholder="Job title"
                maxLength={255}
                disabled={isReadOnly}
              />
            </LabeledField>
            <LabeledField label="Station Link Note">
              <Input
                value={contactEditorForm.linkNote}
                onChange={(event) => {
                  setContactEditorForm((current) => ({ ...current, linkNote: event.target.value }));
                }}
                placeholder="Station contact note"
                maxLength={2048}
                disabled={isReadOnly}
              />
            </LabeledField>
            <LabeledField label="Contact Note" alignStart>
              <Textarea
                value={contactEditorForm.note}
                onChange={(event) => {
                  setContactEditorForm((current) => ({ ...current, note: event.target.value }));
                }}
                placeholder="Contact note"
                maxLength={2048}
                disabled={isReadOnly}
              />
            </LabeledField>
            <LabeledField
              label={
                <>
                  Contact Type<RequiredMark />
                </>
              }
            >
              <Select
                value={normalizeContactType(contactEditorForm.contactType)}
                onValueChange={(value) => {
                  setContactEditorForm((current) => ({ ...current, contactType: normalizeContactType(value) }));
                  setContactEditorError(null);
                }}
                options={contactTypeOptions}
                disabled={isReadOnly}
              />
            </LabeledField>
          </div>

          <div className="mt-2 sm:pl-[126px]">
            <label className="inline-flex items-center gap-2 rounded-md border border-slate-200 px-3 py-2 text-sm text-slate-700">
                <input
                  type="checkbox"
                  className="size-4 rounded border border-slate-300"
                  checked={contactEditorForm.primaryContact}
                  onChange={(event) => {
                    setContactEditorForm((current) => ({ ...current, primaryContact: event.target.checked }));
                  }}
                  disabled={isReadOnly}
                />
              Mark as primary
            </label>
          </div>

          {contactEditorError ? <p className="text-sm text-rose-600">{contactEditorError}</p> : null}

          {canEdit && canApplyContactEditor ? (
            <div className="flex justify-end">
              <Button size="sm" onClick={saveContactEditor} disabled={isReadOnly}>
                Apply
              </Button>
            </div>
          ) : null}
        </DialogContent>
      </Dialog>

      <Dialog open={isAddExistingContactOpen} onOpenChange={handleAddExistingContactOpenChange}>
        <DialogContent
          className="flex max-h-[85vh] max-w-[640px] flex-col overflow-hidden rounded-xl bg-white p-5"
          onInteractOutside={(event) => {
            if (isAppDropdownInteractionEvent(event)) {
              event.preventDefault();
              return;
            }
            if (shouldBlockOutsideClose({ hasUnsavedChanges: hasAddExistingContactChanges })) {
              event.preventDefault();
            }
          }}
        >
          <button
            type="button"
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            aria-label="Close add existing contact modal"
            onClick={() => closeAddExistingContact()}
          >
            <X className="size-4" />
          </button>
          <DialogHeader>
            <DialogTitle>Add Existing Contact</DialogTitle>
            <DialogDescription>
              Select an existing contact and link it to this station in draft state.
            </DialogDescription>
          </DialogHeader>

          <div className="mt-4 min-h-0 flex-1 overflow-y-auto pr-1">
            <div className="space-y-3">
            <div className="space-y-1">
              <p className={SHARED_LABEL_CLASS}>Search</p>
              <div className="relative">
                <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-slate-400" />
                <Input
                  value={existingContactSearch}
                  onChange={(event) => setExistingContactSearch(event.target.value)}
                  placeholder="Search contacts"
                  className="pl-9"
                  maxLength={255}
                  disabled={isLoadingExistingContacts || isReadOnly}
                />
              </div>
            </div>

            <div className="space-y-1">
              <p className={SHARED_LABEL_CLASS}>
                Available Contacts<RequiredMark />
              </p>
              <div className="max-h-[44vh] overflow-y-auto rounded-md border border-slate-200 bg-white">
                {isLoadingExistingContacts ? (
                  <div className="flex items-center justify-center gap-2 px-3 py-6 text-sm text-slate-500">
                    <Loader2 className="size-4 animate-spin" />
                    Loading contacts...
                  </div>
                ) : filteredExistingContacts.length ? (
                  <ul className="p-1">
                    {filteredExistingContacts.map((contact) => {
                      const isSelected = String(contact.id) === selectedExistingContactId;
                      return (
                        <li key={contact.id}>
                          <button
                            type="button"
                            className={`flex w-full items-start justify-between gap-3 rounded-md px-3 py-2 text-left text-sm transition-colors ${
                              isSelected ? "bg-blue-50 text-blue-700" : "text-slate-700 hover:bg-slate-100"
                            }`}
                            onClick={() => setSelectedExistingContactId(String(contact.id))}
                            disabled={isReadOnly}
                          >
                            <span className="min-w-0 space-y-1">
                              <span className="block truncate font-medium">
                                {contact.fullName ? `${contact.fullName} (#${contact.id})` : `#${contact.id}`}
                              </span>
                              {contact.email ? (
                                <span className="block break-all text-xs text-slate-600">Email: {contact.email}</span>
                              ) : null}
                              {contact.company ? (
                                <span className="block text-xs text-slate-600">Company: {contact.company}</span>
                              ) : null}
                              {contact.jobTitle ? (
                                <span className="block text-xs text-slate-600">Job Title: {contact.jobTitle}</span>
                              ) : null}
                            </span>
                            {isSelected ? <Check className="size-4 shrink-0" /> : null}
                          </button>
                        </li>
                      );
                    })}
                  </ul>
                ) : (
                  <p className="px-3 py-6 text-center text-sm text-slate-500">No matching contacts available.</p>
                )}
              </div>
            </div>

            <LabeledField
              label={
                <>
                  Contact Type<RequiredMark />
                </>
              }
            >
              <Select
                value={normalizeContactType(selectedExistingContactType)}
                onValueChange={(value) => {
                  setSelectedExistingContactType(normalizeContactType(value));
                  setExistingContactsError(null);
                }}
                options={contactTypeOptions}
                disabled={isLoadingExistingContacts || isReadOnly}
              />
            </LabeledField>

            <div className="sm:pl-[126px]">
              <label className="inline-flex items-center gap-2 rounded-md border border-slate-200 px-3 py-2 text-sm text-slate-700">
                <input
                  type="checkbox"
                  className="size-4 rounded border border-slate-300"
                  checked={selectedExistingPrimaryContact}
                  onChange={(event) => setSelectedExistingPrimaryContact(event.target.checked)}
                  disabled={isReadOnly}
                />
                Mark as primary
              </label>
            </div>

            {existingContactsError ? <p className="text-sm text-rose-600">{existingContactsError}</p> : null}

            {canEdit && canAddExistingContact ? (
              <div className="flex justify-end">
                <Button size="sm" onClick={addExistingContactToDraft} disabled={isReadOnly}>
                  Add
                </Button>
              </div>
            ) : null}
            </div>
          </div>

          <footer className="shrink-0 border-t border-slate-100 bg-white px-0 pt-2 pb-[calc(1rem+env(safe-area-inset-bottom))]">
            <CacheStatusChip
              text={existingContactsStatusText}
              onRefresh={() => {
                if (!isLoadingExistingContacts) {
                  void loadExistingContacts(true);
                }
              }}
              disabled={isLoadingExistingContacts}
              refreshing={isLoadingExistingContacts}
              refreshLabel="Refresh contacts"
              tooltipText="Click to refresh this data"
            />
          </footer>
        </DialogContent>
      </Dialog>

      <UnsavedChangesDialog
        open={isDeliveryMethodSelectorDiscardDialogOpen}
        onKeepEditing={() => {
          setIsDeliveryMethodSelectorDiscardDialogOpen(false);
        }}
        onDiscardChanges={() => {
          closeDeliveryMethodSelector(true);
        }}
      />

      <UnsavedChangesDialog
        open={isDeliveryMethodEditorDiscardDialogOpen}
        onKeepEditing={() => {
          setIsDeliveryMethodEditorDiscardDialogOpen(false);
        }}
        onDiscardChanges={() => {
          closeDeliveryMethodEditor(true);
        }}
      />

      <UnsavedChangesDialog
        open={isContactEditorDiscardDialogOpen}
        onKeepEditing={() => {
          setIsContactEditorDiscardDialogOpen(false);
        }}
        onDiscardChanges={() => {
          closeContactEditor(true);
        }}
      />

      <UnsavedChangesDialog
        open={isAddExistingContactDiscardDialogOpen}
        onKeepEditing={() => {
          setIsAddExistingContactDiscardDialogOpen(false);
        }}
        onDiscardChanges={() => {
          closeAddExistingContact(true);
        }}
      />
    </>
  );
}

function formatRelativeTime(timestamp: number): string {
  const ageMs = Math.max(0, Date.now() - timestamp);
  if (ageMs < 30_000) {
    return "just now";
  }

  const minutes = Math.floor(ageMs / 60_000);
  if (minutes < 60) {
    return `${minutes} min ago`;
  }

  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    return `${hours} hr ago`;
  }

  const days = Math.floor(hours / 24);
  return `${days} day${days > 1 ? "s" : ""} ago`;
}
