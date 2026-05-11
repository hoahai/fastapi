import { useEffect, useMemo, useRef, useState } from "react";
import { Loader2 } from "lucide-react";

import { ContactModal, type ContactModalMode, type ContactModalSubmitPayload } from "@/components/contacts/ContactModal";
import { ContactResults } from "@/components/contacts/ContactResults";
import { ContactSearchForm } from "@/components/contacts/ContactSearchForm";
import type {
  ContactAccountUsage,
  ContactEstNumUsage,
  ContactGroup,
  ContactRecord,
  ContactSearchFormValues,
  ContactUsageRow,
} from "@/components/contacts/types";
import { PageBanner } from "@/components/layout/PageBanner";
import { Button } from "@/components/ui/button";
import { CacheStatusChip } from "@/components/ui/cache-status-chip";
import { useToast } from "@/components/ui/toast";
import { useApiRequest } from "@/hooks/useApiRequest";
import { usePersistentState } from "@/hooks/usePersistentState";
import {
  listBrowserCacheSnapshotsByPrefix,
  readBrowserCacheSnapshot,
  removeBrowserCache,
  writeBrowserCache,
} from "@/lib/browserCache";
import { buildAuthHeaders as buildSharedAuthHeaders } from "@shared/api/authHeaders";
import { shouldFetchNetwork, type CachePolicy } from "@shared/cache";
import { useAuth } from "@shared/auth/useAuth";
import { shouldProtectTradsphereFrontend } from "@shared/auth/guards";
import { hasAppEditAccess } from "@shared/auth/permissions";
import { hasAtLeastOneSearchCriterion, shouldFetchSubmittedSearchNetwork } from "@shared/search";

const CONTACTS_SEARCH_CACHE_TTL_MS = 5 * 60 * 1000;
const CONTACT_DETAIL_CACHE_TTL_MS = 5 * 60 * 1000;
const CONTACTS_SEARCH_COLLECTION_PREFIX = "contacts:search:";
const CONTACTS_SEARCH_COLLECTION_LIMIT = 40;
const SIDEBAR_COLLAPSED_STORAGE_KEY = "workspace.sidebar.collapsed";
const LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY = "tradsphere:ui:sidebarCollapsed:v1";
const SIDEBAR_COLLAPSED_EVENT = "workspace-sidebar-collapsed-change";
const CONTACTS_SEARCH_DRAFT_STORAGE_KEY = "tradsphere.contacts.searchDraft.v1";
const CONTACTS_SUBMITTED_SEARCH_STORAGE_KEY = "tradsphere.contacts.submittedSearch.v1";

type SearchUiState = "idle" | "loading" | "ready" | "empty" | "error";

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type SubmittedSearch = {
  params: ContactSearchFormValues;
  cacheKey: string;
};

type SearchLoadOptions = {
  policy: CachePolicy;
};

type ContactUsageJoinRow = ContactUsageRow & {
  contactId: number;
};
type StationAccountUsageMap = Record<string, ContactAccountUsage[]>;
type StationEstNumUsageMap = Record<string, ContactEstNumUsage[]>;
type EstNumUsageMeta = {
  accountCode: string;
  month: number | null;
  quarter: number | null;
  year: number | null;
  periodLabel: string;
  mediaType: string;
  broadcastMonths: number[];
  broadcastYears: number[];
};

type BuildSearchResult =
  | {
      ok: true;
      submitted: SubmittedSearch;
    }
  | {
      ok: false;
      clearResults: boolean;
      message: string;
    };

const INITIAL_SEARCH_FORM: ContactSearchFormValues = {
  name: "",
  email: "",
  phone: "",
  station: "",
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function unwrapData(payload: unknown): unknown {
  if (isRecord(payload) && "data" in payload) {
    return payload.data;
  }
  return payload;
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
    const normalized = value.trim().toLowerCase();
    return ["1", "true", "yes", "y", "on"].includes(normalized);
  }
  return false;
}

function asStringArray(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value
      .map((item) => asString(item).toUpperCase())
      .filter(Boolean);
  }

  const text = asString(value);
  if (!text) {
    return [];
  }

  return text
    .split(",")
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);
}

function asIntArray(value: unknown): number[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item) => asNumber(item))
    .filter((item): item is number => item !== null && Number.isFinite(item))
    .map((item) => Math.trunc(item));
}

function uniqStrings(values: string[]): string[] {
  const seen = new Set<string>();
  const output: string[] = [];
  for (const value of values) {
    const normalized = value.trim();
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    output.push(normalized);
  }
  return output;
}

function normalizeSearchForm(draft: ContactSearchFormValues): ContactSearchFormValues {
  return {
    name: asString(draft.name),
    email: asString(draft.email).toLowerCase(),
    phone: asString(draft.phone),
    station: asString(draft.station),
  };
}

function isContactSearchFormValues(value: unknown): value is ContactSearchFormValues {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.name === "string" &&
    typeof value.email === "string" &&
    typeof value.phone === "string" &&
    typeof value.station === "string"
  );
}

function encodeKeyPart(value: string): string {
  return encodeURIComponent(value.trim().toLowerCase());
}

function buildSearchCacheKey(params: ContactSearchFormValues): string {
  return [
    "contacts:search",
    `name=${encodeKeyPart(params.name)}`,
    `email=${encodeKeyPart(params.email)}`,
    `phone=${encodeKeyPart(params.phone)}`,
    `station=${encodeKeyPart(params.station)}`,
    "v4",
  ].join(":");
}

function buildContactDetailCacheKey(contactId: number): string {
  return `contacts:detail:${contactId}:v1`;
}

function normalizeContactRecords(items: unknown): ContactRecord[] {
  if (!Array.isArray(items)) {
    return [];
  }

  const normalized: ContactRecord[] = [];
  for (const item of items) {
    if (!isRecord(item)) {
      continue;
    }
    const id = asNumber(item.id);
    if (id === null) {
      continue;
    }

    const usage = Array.isArray(item.usage)
      ? item.usage
          .filter((row): row is Record<string, unknown> => isRecord(row))
          .map((row) => ({
            linkId: asNumber(row.linkId),
            stationCode: asString(row.stationCode).toUpperCase(),
            stationName: asString(row.stationName),
            mediaType: asString(row.mediaType).toUpperCase(),
            language: asString(row.language) || null,
            syscode: asString(row.syscode) || null,
            affiliation: asString(row.affiliation) || null,
            market: asString(row.market) || null,
            contactType: asString(row.contactType).toUpperCase(),
            primaryContact: asBoolean(row.primaryContact),
            active: asBoolean(row.active),
          }))
      : [];

    const usedByAccounts = Array.isArray(item.usedByAccounts)
      ? item.usedByAccounts
          .filter((row): row is Record<string, unknown> => isRecord(row))
          .map((row) => ({
            accountCode: asString(row.accountCode).toUpperCase(),
            accountName: asString(row.accountName),
          }))
      : [];

    const usedByEstNums = Array.isArray(item.usedByEstNums)
      ? item.usedByEstNums
          .filter((row): row is Record<string, unknown> => isRecord(row))
          .map((row) => ({
            estNum: asNumber(row.estNum) ?? 0,
            accountCode: asString(row.accountCode).toUpperCase(),
            accountName: asString(row.accountName),
            month: asNumber(row.month),
            quarter: asNumber(row.quarter),
            year: asNumber(row.year),
            periodLabel: asString(row.periodLabel),
            mediaType: asString(row.mediaType).toUpperCase(),
            broadcastMonths: asIntArray(row.broadcastMonths),
            broadcastYears: asIntArray(row.broadcastYears),
            stationCodes: Array.isArray(row.stationCodes)
              ? row.stationCodes.map((code) => asString(code).toUpperCase()).filter(Boolean)
              : [],
          }))
          .filter((row) => Number.isFinite(row.estNum) && row.estNum > 0)
      : [];

    const stationCodes = uniqStrings(asStringArray(item.stationCodes));
    normalized.push({
      id: Math.trunc(id),
      email: asString(item.email).toLowerCase(),
      firstName: asString(item.firstName),
      lastName: asString(item.lastName),
      fullName:
        asString(item.fullName) ||
        [asString(item.firstName), asString(item.lastName)].filter(Boolean).join(" ").trim(),
      company: asString(item.company),
      jobTitle: asString(item.jobTitle),
      office: asString(item.office),
      cell: asString(item.cell),
      note: asString(item.note),
      active: asBoolean(item.active),
      stationCodes,
      contactTypes: Array.isArray(item.contactTypes)
        ? item.contactTypes.map((type) => asString(type).toUpperCase()).filter(Boolean)
        : [],
      usage,
      usedByAccounts,
      usedByEstNums,
      usedByStationCount:
        typeof item.usedByStationCount === "number" ? item.usedByStationCount : stationCodes.length,
      usedByAccountCount:
        typeof item.usedByAccountCount === "number" ? item.usedByAccountCount : usedByAccounts.length,
      usedByEstNumCount:
        typeof item.usedByEstNumCount === "number" ? item.usedByEstNumCount : usedByEstNums.length,
      isPrimaryContact: asBoolean(item.isPrimaryContact),
    });
  }

  return normalized;
}

function buildSearchSubmission(draft: ContactSearchFormValues): BuildSearchResult {
  const normalized = normalizeSearchForm(draft);
  const hasAnyInput = hasAtLeastOneSearchCriterion(normalized);

  if (!hasAnyInput) {
    return {
      ok: false,
      clearResults: true,
      message: "Enter at least one field before searching.",
    };
  }

  return {
    ok: true,
    submitted: {
      params: normalized,
      cacheKey: buildSearchCacheKey(normalized),
    },
  };
}

function isSubmittedSearch(value: unknown): value is SubmittedSearch {
  if (!isRecord(value)) {
    return false;
  }
  return isContactSearchFormValues(value.params) && typeof value.cacheKey === "string";
}

function normalizeSearchText(value: string): string {
  return value.trim().toLowerCase();
}

function normalizePhoneText(value: string): string {
  return value.replace(/\D+/g, "");
}

function isContactMatchSubmittedSearch(contact: ContactRecord, submitted: SubmittedSearch): boolean {
  const params = submitted.params;

  const nameQuery = normalizeSearchText(params.name);
  if (nameQuery) {
    const nameCandidates = [
      contact.fullName,
      contact.firstName,
      contact.lastName,
      [contact.firstName, contact.lastName].filter(Boolean).join(" "),
    ]
      .map((item) => normalizeSearchText(asString(item)))
      .filter(Boolean);
    if (!nameCandidates.some((item) => item.includes(nameQuery))) {
      return false;
    }
  }

  const emailQuery = normalizeSearchText(params.email);
  if (emailQuery) {
    const email = normalizeSearchText(asString(contact.email));
    if (!email.includes(emailQuery)) {
      return false;
    }
  }

  const phoneQuery = normalizePhoneText(params.phone);
  if (phoneQuery) {
    const phoneCandidates = [contact.office, contact.cell]
      .map((value) => normalizePhoneText(asString(value)))
      .filter(Boolean);
    if (!phoneCandidates.some((value) => value.includes(phoneQuery))) {
      return false;
    }
  }

  const stationQuery = normalizeSearchText(params.station);
  if (stationQuery) {
    const stationCandidates = [
      ...contact.stationCodes,
      ...contact.usage.map((item) => item.stationCode),
      ...contact.usage.map((item) => item.stationName),
    ]
      .map((value) => normalizeSearchText(asString(value)))
      .filter(Boolean);
    if (!stationCandidates.some((value) => value.includes(stationQuery))) {
      return false;
    }
  }

  return true;
}

function sortContactRecords(items: ContactRecord[]): ContactRecord[] {
  return [...items].sort((a, b) => {
    const nameDiff = buildContactFullName(a).localeCompare(buildContactFullName(b));
    if (nameDiff !== 0) {
      return nameDiff;
    }
    return asString(a.email).localeCompare(asString(b.email));
  });
}

function buildLocalCachedContactsResult(submitted: SubmittedSearch): {
  contacts: ContactRecord[];
  fetchedAt: number | null;
  isPartial: boolean;
} {
  const exactSnapshot = readBrowserCacheSnapshot<ContactRecord[]>(submitted.cacheKey);
  const exactContacts = normalizeContactRecords(exactSnapshot?.data);
  const deduped: ContactRecord[] = [];
  const seen = new Set<number>();
  let newestFetchedAt = exactSnapshot?.fetchedAt ?? 0;

  const appendContact = (contact: ContactRecord) => {
    if (seen.has(contact.id)) {
      return;
    }
    seen.add(contact.id);
    deduped.push(contact);
  };

  for (const contact of exactContacts) {
    appendContact(contact);
  }

  const cacheEntries = listBrowserCacheSnapshotsByPrefix<ContactRecord[]>(
    CONTACTS_SEARCH_COLLECTION_PREFIX,
    {
      allowExpired: true,
      limit: CONTACTS_SEARCH_COLLECTION_LIMIT,
    },
  );

  for (const entry of cacheEntries) {
    newestFetchedAt = Math.max(newestFetchedAt, entry.snapshot.fetchedAt);
    const contacts = normalizeContactRecords(entry.snapshot.data);
    for (const contact of contacts) {
      if (!isContactMatchSubmittedSearch(contact, submitted)) {
        continue;
      }
      appendContact(contact);
    }
  }

  if (deduped.length === 0 && exactSnapshot && Array.isArray(exactSnapshot.data)) {
    return {
      contacts: [],
      fetchedAt: exactSnapshot.fetchedAt,
      isPartial: false,
    };
  }

  return {
    contacts: sortContactRecords(deduped),
    fetchedAt: newestFetchedAt || null,
    isPartial: exactContacts.length === 0 && deduped.length > 0,
  };
}

function resolveInitialContactsSearchView(submittedSearch: SubmittedSearch | null): {
  state: SearchUiState;
  contacts: ContactRecord[];
  cacheStatus: CacheStatus | null;
} {
  if (!submittedSearch) {
    return {
      state: "idle",
      contacts: [],
      cacheStatus: null,
    };
  }

  const localResult = buildLocalCachedContactsResult(submittedSearch);
  if (!localResult.contacts.length && localResult.fetchedAt === null) {
    return {
      state: "idle",
      contacts: [],
      cacheStatus: null,
    };
  }

  return {
    state: localResult.contacts.length ? "ready" : "empty",
    contacts: localResult.contacts,
    cacheStatus: {
      source: "cache",
      fetchedAt: localResult.fetchedAt ?? Date.now(),
    },
  };
}

function formatResultText(total: number): string {
  if (total === 1) {
    return "1 contact found.";
  }
  return `${total} contacts found.`;
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

function getErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim();
  }
  return fallback;
}

function readSidebarCollapsedState(): boolean {
  if (typeof window === "undefined") {
    return false;
  }

  try {
    const nextValue = window.localStorage.getItem(SIDEBAR_COLLAPSED_STORAGE_KEY);
    if (nextValue !== null) {
      return nextValue === "1";
    }
    return window.localStorage.getItem(LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY) === "1";
  } catch {
    return false;
  }
}

function buildContactFullName(contact: {
  firstName: string;
  lastName: string;
  fullName: string;
}): string {
  const explicit = asString(contact.fullName);
  if (explicit) {
    return explicit;
  }
  return [asString(contact.firstName), asString(contact.lastName)].filter(Boolean).join(" ").trim();
}

function buildContactCopyValue(contact: ContactRecord): string {
  const fullName = buildContactFullName(contact);
  const email = asString(contact.email).toLowerCase();
  if (!email) {
    return "";
  }
  return fullName ? `${fullName} <${email}>` : email;
}

async function copyTextToClipboard(value: string): Promise<void> {
  if (!value) {
    throw new Error("Missing text for clipboard copy.");
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

function buildGroups(items: ContactRecord[]): ContactGroup[] {
  const buckets = new Map<string, { label: string; rank: number; items: ContactRecord[] }>();
  for (const item of items) {
    const groupLabel = item.active ? "Active" : "Inactive";
    const groupKey = item.active ? "ACTIVE" : "INACTIVE";
    const groupRank = item.active ? 0 : 1;
    const bucket = buckets.get(groupKey);
    if (bucket) {
      bucket.items.push(item);
    } else {
      buckets.set(groupKey, { label: groupLabel, rank: groupRank, items: [item] });
    }
  }

  const groups = [...buckets.entries()].map(([key, bucket]) => ({
    key,
    rank: bucket.rank,
    label: bucket.label,
    items: [...bucket.items].sort((a, b) => {
      const byName = buildContactFullName(a).localeCompare(buildContactFullName(b));
      if (byName !== 0) {
        return byName;
      }
      return a.email.localeCompare(b.email);
    }),
  }));

  return groups
    .sort((a, b) => a.rank - b.rank)
    .map(({ rank: _rank, ...group }) => group);
}

function parseContacts(payload: unknown): ContactRecord[] {
  const data = unwrapData(payload);
  if (!Array.isArray(data)) {
    return [];
  }

  const contacts: ContactRecord[] = [];
  for (const row of data) {
    if (!isRecord(row)) {
      continue;
    }
    const id = asNumber(row.id);
    if (id === null) {
      continue;
    }

    const firstName = asString(row.firstName);
    const lastName = asString(row.lastName);
    const fullName = asString(row.name) || [firstName, lastName].filter(Boolean).join(" ").trim();

    contacts.push({
      id: Math.trunc(id),
      email: asString(row.email).toLowerCase(),
      firstName,
      lastName,
      fullName,
      company: asString(row.company),
      jobTitle: asString(row.jobTitle),
      office: asString(row.office),
      cell: asString(row.cell),
      note: asString(row.note),
      active: asBoolean(row.active),
      stationCodes: uniqStrings(asStringArray(row.stationCodes)),
      contactTypes: [],
      usage: [],
      usedByAccounts: [],
      usedByEstNums: [],
      usedByStationCount: 0,
      usedByAccountCount: 0,
      usedByEstNumCount: 0,
      isPrimaryContact: false,
    });
  }

  return contacts;
}

function parseUsageRows(payload: unknown): ContactUsageJoinRow[] {
  const data = unwrapData(payload);
  if (!Array.isArray(data)) {
    return [];
  }

  const rows: ContactUsageJoinRow[] = [];
  for (const row of data) {
    if (!isRecord(row)) {
      continue;
    }

    const contactId = asNumber(row.contactId);
    if (contactId === null) {
      continue;
    }
    const linkId = asNumber(row.id);

    rows.push({
      contactId: Math.trunc(contactId),
      linkId: linkId !== null ? Math.trunc(linkId) : null,
      stationCode: asString(row.stationCode).toUpperCase(),
      stationName: asString(row.stationName),
      mediaType: asString(row.mediaType).toUpperCase(),
      language: asString(row.language) || null,
      syscode: asString(row.syscode) || null,
      affiliation: asString(row.affiliation) || null,
      market: asString(row.market) || null,
      contactType: asString(row.contactType).toUpperCase(),
      primaryContact: asBoolean(row.primaryContact),
      active: asBoolean(row.active),
    });
  }

  return rows;
}

function parseStationScheduleRows(payload: unknown): Array<{ stationCode: string; estNum: number }> {
  const data = unwrapData(payload);
  if (!Array.isArray(data)) {
    return [];
  }

  const rows: Array<{ stationCode: string; estNum: number }> = [];
  for (const row of data) {
    if (!isRecord(row)) {
      continue;
    }
    const stationCode = asString(row.stationCode).toUpperCase();
    const estNum = asNumber(row.estNum);
    if (!stationCode || estNum === null) {
      continue;
    }
    rows.push({ stationCode, estNum: Math.trunc(estNum) });
  }
  return rows;
}

function deriveQuarterFromMonth(month: number | null): number | null {
  if (month === null || month < 1 || month > 12) {
    return null;
  }
  return Math.floor((month - 1) / 3) + 1;
}

function parseEstNumUsageMetaRows(payload: unknown): Map<number, EstNumUsageMeta> {
  const data = unwrapData(payload);
  const map = new Map<number, EstNumUsageMeta>();
  if (!Array.isArray(data)) {
    return map;
  }

  for (const row of data) {
    if (!isRecord(row)) {
      continue;
    }
    const estNum = asNumber(row.estNum);
    const accountCode = asString(row.accountCode).toUpperCase();
    if (estNum === null || !accountCode) {
      continue;
    }
    const broadcastMonths = asIntArray(row.broadcastMonths).filter((item) => item >= 1 && item <= 12);
    const broadcastYears = asIntArray(row.broadcastYears).filter((item) => item >= 1901 && item <= 2155);
    const month = asNumber(row.month) ?? (broadcastMonths.length === 1 ? broadcastMonths[0] : null);
    const year = asNumber(row.year) ?? (broadcastYears.length === 1 ? broadcastYears[0] : null);
    const quarter = asNumber(row.quarter) ?? deriveQuarterFromMonth(month);
    map.set(Math.trunc(estNum), {
      accountCode,
      month: month !== null ? Math.trunc(month) : null,
      quarter: quarter !== null ? Math.trunc(quarter) : null,
      year: year !== null ? Math.trunc(year) : null,
      periodLabel: asString(row.periodLabel),
      mediaType: asString(row.mediaType).toUpperCase(),
      broadcastMonths,
      broadcastYears,
    });
  }
  return map;
}

function parseAccountNameMap(payload: unknown): Record<string, string> {
  const data = unwrapData(payload);
  if (!Array.isArray(data)) {
    return {};
  }

  const map: Record<string, string> = {};
  for (const row of data) {
    if (!isRecord(row)) {
      continue;
    }
    const accountCode = asString(row.accountCode).toUpperCase();
    if (!accountCode) {
      continue;
    }
    map[accountCode] = asString(row.name);
  }
  return map;
}

function buildStationAccountUsageMap(
  scheduleRows: Array<{ stationCode: string; estNum: number }>,
  estNumMetaMap: Map<number, EstNumUsageMeta>,
  accountNameMap: Record<string, string>,
): StationAccountUsageMap {
  const byStation = new Map<string, Map<string, ContactAccountUsage>>();
  for (const row of scheduleRows) {
    const accountCode = estNumMetaMap.get(row.estNum)?.accountCode;
    if (!accountCode) {
      continue;
    }
    const stationCode = row.stationCode;
    const stationBucket = byStation.get(stationCode) ?? new Map<string, ContactAccountUsage>();
    stationBucket.set(accountCode, {
      accountCode,
      accountName: asString(accountNameMap[accountCode]),
    });
    byStation.set(stationCode, stationBucket);
  }

  const result: StationAccountUsageMap = {};
  for (const [stationCode, bucket] of byStation.entries()) {
    result[stationCode] = [...bucket.values()].sort((a, b) => a.accountCode.localeCompare(b.accountCode));
  }
  return result;
}

function buildStationEstNumUsageMap(
  scheduleRows: Array<{ stationCode: string; estNum: number }>,
  estNumMetaMap: Map<number, EstNumUsageMeta>,
  accountNameMap: Record<string, string>,
): StationEstNumUsageMap {
  const byStation = new Map<string, Map<number, ContactEstNumUsage>>();
  for (const row of scheduleRows) {
    const meta = estNumMetaMap.get(row.estNum);
    if (!meta?.accountCode) {
      continue;
    }
    const accountCode = meta.accountCode;
    const stationCode = row.stationCode;
    const stationBucket = byStation.get(stationCode) ?? new Map<number, ContactEstNumUsage>();
    const existing = stationBucket.get(row.estNum);
    if (existing) {
      const nextStationCodes = uniqStrings([...existing.stationCodes, stationCode]);
      stationBucket.set(row.estNum, {
        ...existing,
        stationCodes: nextStationCodes,
      });
    } else {
      stationBucket.set(row.estNum, {
        estNum: row.estNum,
        accountCode,
        accountName: asString(accountNameMap[accountCode]),
        month: meta.month,
        quarter: meta.quarter,
        year: meta.year,
        periodLabel: meta.periodLabel,
        mediaType: meta.mediaType,
        broadcastMonths: meta.broadcastMonths,
        broadcastYears: meta.broadcastYears,
        stationCodes: [stationCode],
      });
    }
    byStation.set(stationCode, stationBucket);
  }

  const result: StationEstNumUsageMap = {};
  for (const [stationCode, bucket] of byStation.entries()) {
    result[stationCode] = [...bucket.values()].sort((a, b) => b.estNum - a.estNum);
  }
  return result;
}

function mergeContactUsage(
  contacts: ContactRecord[],
  usageRows: ContactUsageJoinRow[],
  stationAccountUsageMap: StationAccountUsageMap,
  stationEstNumUsageMap: StationEstNumUsageMap,
): ContactRecord[] {
  const byContactId = new Map<number, ContactUsageRow[]>();

  for (const row of usageRows) {
    const contactId = row.contactId;
    const normalized: ContactUsageRow = {
      linkId: row.linkId,
      stationCode: row.stationCode,
      stationName: row.stationName,
      mediaType: row.mediaType,
      language: row.language,
      syscode: row.syscode,
      affiliation: row.affiliation,
      market: row.market,
      contactType: row.contactType,
      primaryContact: row.primaryContact,
      active: row.active,
    };

    const bucket = byContactId.get(contactId);
    if (bucket) {
      bucket.push(normalized);
    } else {
      byContactId.set(contactId, [normalized]);
    }
  }

  return contacts.map((contact) => {
    const usage = (byContactId.get(contact.id) || []).sort((a, b) => {
      const byCode = a.stationCode.localeCompare(b.stationCode);
      if (byCode !== 0) {
        return byCode;
      }
      return a.contactType.localeCompare(b.contactType);
    });

    const types = uniqStrings(usage.map((item) => asString(item.contactType).toUpperCase()).filter(Boolean));
    const usageStationCodes = uniqStrings(usage.map((item) => item.stationCode));
    const combinedStationCodes = uniqStrings([...contact.stationCodes, ...usageStationCodes]);
    const accountBucket = new Map<string, ContactAccountUsage>();
    for (const stationCode of usageStationCodes) {
      const accounts = stationAccountUsageMap[stationCode] || [];
      for (const account of accounts) {
        const normalizedCode = asString(account.accountCode).toUpperCase();
        if (!normalizedCode || accountBucket.has(normalizedCode)) {
          continue;
        }
        accountBucket.set(normalizedCode, {
          accountCode: normalizedCode,
          accountName: asString(account.accountName),
        });
      }
    }
    const usedByAccounts = [...accountBucket.values()].sort((a, b) => a.accountCode.localeCompare(b.accountCode));
    const estNumBucket = new Map<number, ContactEstNumUsage>();
    for (const stationCode of usageStationCodes) {
      const estNums = stationEstNumUsageMap[stationCode] || [];
      for (const item of estNums) {
        const existing = estNumBucket.get(item.estNum);
        if (existing) {
          estNumBucket.set(item.estNum, {
            ...existing,
            stationCodes: uniqStrings([...existing.stationCodes, ...item.stationCodes]),
          });
          continue;
        }
        estNumBucket.set(item.estNum, {
          estNum: item.estNum,
          accountCode: asString(item.accountCode).toUpperCase(),
          accountName: asString(item.accountName),
          month: item.month,
          quarter: item.quarter,
          year: item.year,
          periodLabel: asString(item.periodLabel),
          mediaType: asString(item.mediaType).toUpperCase(),
          broadcastMonths: item.broadcastMonths,
          broadcastYears: item.broadcastYears,
          stationCodes: uniqStrings(item.stationCodes),
        });
      }
    }
    const usedByEstNums = [...estNumBucket.values()].sort((a, b) => b.estNum - a.estNum);

    return {
      ...contact,
      stationCodes: combinedStationCodes,
      contactTypes: types,
      usage,
      usedByAccounts,
      usedByEstNums,
      usedByStationCount: usageStationCodes.length,
      usedByAccountCount: usedByAccounts.length,
      usedByEstNumCount: usedByEstNums.length,
      isPrimaryContact: usage.some((item) => item.primaryContact),
    };
  });
}

function buildContactPayload(form: ContactModalSubmitPayload["form"]): Record<string, unknown> {
  const normalized = {
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

  const payload: Record<string, unknown> = {
    email: normalized.email,
    company: normalized.company,
    jobTitle: normalized.jobTitle,
    office: normalized.office,
    cell: normalized.cell,
    note: normalized.note,
    active: normalized.active,
  };

  const hasBothFirstAndLast = Boolean(normalized.firstName && normalized.lastName);
  if (hasBothFirstAndLast) {
    payload.firstName = normalized.firstName;
    payload.lastName = normalized.lastName;
  } else if (normalized.fullName) {
    payload.name = normalized.fullName;
  } else {
    payload.firstName = normalized.firstName;
    payload.lastName = normalized.lastName;
  }

  return payload;
}

export default function ContactsPage() {
  const toast = useToast();
  const { requestJson } = useApiRequest();
  const auth = useAuth();
  const requestHeaders = useMemo(
    () => buildSharedAuthHeaders(auth.session, auth.tenantSlug, false),
    [auth.session, auth.tenantSlug],
  );
  const canEditTradsphere = useMemo(() => {
    if (!shouldProtectTradsphereFrontend()) {
      return true;
    }
    return hasAppEditAccess(auth.accessProfile, "tradsphere");
  }, [auth.accessProfile]);

  const [draft, setDraft] = usePersistentState<ContactSearchFormValues>(
    CONTACTS_SEARCH_DRAFT_STORAGE_KEY,
    INITIAL_SEARCH_FORM,
    { storage: "session", validate: isContactSearchFormValues },
  );
  const [submittedSearch, setSubmittedSearch] = usePersistentState<SubmittedSearch | null>(
    CONTACTS_SUBMITTED_SEARCH_STORAGE_KEY,
    null,
    { storage: "session", validate: (value): value is SubmittedSearch | null => value === null || isSubmittedSearch(value) },
  );
  const [submissionVersion, setSubmissionVersion] = useState(0);
  const [searchMessage, setSearchMessage] = useState<string | null>(null);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);

  const [state, setState] = useState<SearchUiState>(() => resolveInitialContactsSearchView(submittedSearch).state);
  const [error, setError] = useState<string | null>(null);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [contacts, setContacts] = useState<ContactRecord[]>(() => resolveInitialContactsSearchView(submittedSearch).contacts);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(() => resolveInitialContactsSearchView(submittedSearch).cacheStatus);
  const [modalDetailCacheStatus, setModalDetailCacheStatus] = useState<CacheStatus | null>(null);
  const [isModalDetailRefreshing, setIsModalDetailRefreshing] = useState(false);

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [modalMode, setModalMode] = useState<ContactModalMode>("create");
  const [modalContact, setModalContact] = useState<ContactRecord | null>(null);
  const [focusUsageToken, setFocusUsageToken] = useState(0);

  const [sidebarVisuallyExpanded, setSidebarVisuallyExpanded] = useState<boolean>(() => !readSidebarCollapsedState());

  const requestTokenRef = useRef(0);
  const modalDetailRequestTokenRef = useRef(0);
  const inFlightRef = useRef<Record<string, Promise<ContactRecord[]>>>({});

  const groups = useMemo(() => buildGroups(contacts), [contacts]);
  const resultText = contacts.length ? formatResultText(contacts.length) : null;
  const normalizedDraft = useMemo(() => normalizeSearchForm(draft), [draft]);
  const canClearDraft = useMemo(() => hasAtLeastOneSearchCriterion(normalizedDraft), [normalizedDraft]);
  const draftSearchResult = useMemo(() => buildSearchSubmission(draft), [draft]);
  const canSubmitSearch = draftSearchResult.ok;
  const shouldShowBlockingResultsOverlay = state === "loading" && contacts.length === 0;

  const cacheStatusText = isRefreshing
    ? "Refreshing..."
    : cacheStatus
      ? `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`
      : "No cached data yet";
  const modalDetailCacheStatusText = isModalDetailRefreshing
    ? "Refreshing..."
    : modalDetailCacheStatus
      ? `Data source: ${modalDetailCacheStatus.source}. Last updated ${formatRelativeTime(modalDetailCacheStatus.fetchedAt)}.`
      : "No cached data yet";

  const showCacheChip = Boolean(submittedSearch && !isModalOpen);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const handleStorage = () => {
      const collapsed = readSidebarCollapsedState();
      setSidebarVisuallyExpanded(!collapsed);
    };

    const handleSidebarEvent = (event: Event) => {
      const customEvent = event as CustomEvent<{ collapsed?: boolean; visuallyExpanded?: boolean }>;
      if (typeof customEvent.detail?.visuallyExpanded === "boolean") {
        setSidebarVisuallyExpanded(customEvent.detail.visuallyExpanded);
        return;
      }
      if (typeof customEvent.detail?.collapsed === "boolean") {
        setSidebarVisuallyExpanded(!customEvent.detail.collapsed);
        return;
      }
      const collapsed = readSidebarCollapsedState();
      setSidebarVisuallyExpanded(!collapsed);
    };

    window.addEventListener("storage", handleStorage);
    window.addEventListener(SIDEBAR_COLLAPSED_EVENT, handleSidebarEvent as EventListener);
    return () => {
      window.removeEventListener("storage", handleStorage);
      window.removeEventListener(SIDEBAR_COLLAPSED_EVENT, handleSidebarEvent as EventListener);
    };
  }, []);

async function fetchContactsForSearch(search: SubmittedSearch): Promise<ContactRecord[]> {
    const params = new URLSearchParams();
    if (search.params.name) {
      params.set("name", search.params.name);
    }
    if (search.params.email) {
      params.set("emails", search.params.email);
    }
    if (search.params.phone) {
      params.set("phone", search.params.phone);
    }
    if (search.params.station) {
      params.set("station", search.params.station);
    }

    const contactsPayload = await requestJson(`/api/tradsphere/v1/contacts?${params.toString()}`, {
      headers: requestHeaders,
      errorToast: false,
    });
    const parsedContacts = parseContacts(contactsPayload);
    if (!parsedContacts.length) {
      return [];
    }

    return parsedContacts;
  }

  async function fetchContactDetailUsage(baseContact: ContactRecord): Promise<ContactRecord> {
    const usagePayload = await requestJson(
      `/api/tradsphere/v1/contacts/stationsContacts?contactIds=${encodeURIComponent(String(baseContact.id))}&active=true`,
      {
        headers: requestHeaders,
        errorToast: false,
      },
    );
    const usageRows = parseUsageRows(usagePayload);
    const stationCodes = uniqStrings(
      usageRows
        .map((item) => asString(item.stationCode).toUpperCase())
        .filter(Boolean),
    );
    if (!stationCodes.length) {
      return {
        ...baseContact,
        usage: [],
        usedByAccounts: [],
        usedByEstNums: [],
        usedByStationCount: 0,
        usedByAccountCount: 0,
        usedByEstNumCount: 0,
      };
    }

    const schedulesPayload = await requestJson(
      `/api/tradsphere/v1/schedules?stationCodes=${encodeURIComponent(stationCodes.join(","))}`,
      {
        headers: requestHeaders,
        errorToast: false,
      },
    );
    const stationScheduleRows = parseStationScheduleRows(schedulesPayload);
    const scheduleEstNums = uniqStrings(stationScheduleRows.map((row) => String(row.estNum)));
    let stationAccountUsageMap: StationAccountUsageMap = {};
    let stationEstNumUsageMap: StationEstNumUsageMap = {};
    if (scheduleEstNums.length > 0) {
      const estNumsPayload = await requestJson(
        `/api/tradsphere/v1/estNums?estNums=${encodeURIComponent(scheduleEstNums.join(","))}`,
        {
          headers: requestHeaders,
          errorToast: false,
        },
      );
      const estNumMetaMap = parseEstNumUsageMetaRows(estNumsPayload);
      const accountCodes = uniqStrings([...estNumMetaMap.values()].map((item) => item.accountCode));

      let accountNameMap: Record<string, string> = {};
      if (accountCodes.length > 0) {
        const accountsPayload = await requestJson(
          `/api/tradsphere/v1/accounts?accountCodes=${encodeURIComponent(accountCodes.join(","))}&active=false`,
          {
            headers: requestHeaders,
            errorToast: false,
          },
        );
        accountNameMap = parseAccountNameMap(accountsPayload);
      }

      stationAccountUsageMap = buildStationAccountUsageMap(
        stationScheduleRows,
        estNumMetaMap,
        accountNameMap,
      );
      stationEstNumUsageMap = buildStationEstNumUsageMap(
        stationScheduleRows,
        estNumMetaMap,
        accountNameMap,
      );
    }

    const merged = mergeContactUsage(
      [baseContact],
      usageRows,
      stationAccountUsageMap,
      stationEstNumUsageMap,
    );
    return merged[0] ?? baseContact;
  }

  async function loadModalContactDetail(
    baseContact: ContactRecord,
    options: { policy: CachePolicy } = { policy: "stale-while-revalidate" },
  ): Promise<void> {
    const requestToken = ++modalDetailRequestTokenRef.current;
    const normalizedBase = normalizeContactRecords([baseContact])[0];
    const cacheKey = buildContactDetailCacheKey(normalizedBase.id);
    const snapshot = readBrowserCacheSnapshot<ContactRecord>(cacheKey);
    const snapshotDetailData = snapshot?.data;
    const normalizedSnapshotDetail = snapshotDetailData ? normalizeContactRecords([snapshotDetailData]) : [];
    const hasInvalidSnapshotShape =
      snapshotDetailData !== null &&
      snapshotDetailData !== undefined &&
      normalizedSnapshotDetail.length === 0;
    const effectiveSnapshot = hasInvalidSnapshotShape ? null : snapshot;

    if (hasInvalidSnapshotShape) {
      removeBrowserCache(cacheKey);
    }

    if (normalizedSnapshotDetail.length > 0 && options.policy !== "network-only") {
      const cached = normalizedSnapshotDetail[0];
      setModalContact(cached);
      setModalDetailCacheStatus({
        source: "cache",
        fetchedAt: snapshot?.fetchedAt ?? Date.now(),
      });
    } else {
      setModalContact(normalizedBase);
      setModalDetailCacheStatus(null);
    }

    const shouldFetch = shouldFetchNetwork(options.policy, effectiveSnapshot);
    if (!shouldFetch) {
      if (requestToken === modalDetailRequestTokenRef.current) {
        setIsModalDetailRefreshing(false);
      }
      return;
    }

    setIsModalDetailRefreshing(true);
    try {
      const detailed = await fetchContactDetailUsage(normalizedBase);
      if (requestToken !== modalDetailRequestTokenRef.current) {
        return;
      }
      const normalizedDetailed = normalizeContactRecords([detailed])[0];
      const fetchedAt = Date.now();
      setModalContact(normalizedDetailed);
      setModalDetailCacheStatus({ source: "network", fetchedAt });
      writeBrowserCache(cacheKey, normalizedDetailed, CONTACT_DETAIL_CACHE_TTL_MS, {
        source: "network",
        fetchedAt,
      });
    } finally {
      if (requestToken === modalDetailRequestTokenRef.current) {
        setIsModalDetailRefreshing(false);
      }
    }
  }

  async function loadSearchData(options: SearchLoadOptions, activeSearch: SubmittedSearch | null = submittedSearch): Promise<void> {
    if (!activeSearch) {
      setState("idle");
      setError(null);
      setContacts([]);
      setCacheStatus(null);
      setRefreshMessage(null);
      return;
    }

    const requestToken = ++requestTokenRef.current;
    const cacheKey = activeSearch.cacheKey;
    const snapshot = readBrowserCacheSnapshot<ContactRecord[]>(cacheKey);
    const snapshotSearchData = snapshot?.data;
    const normalizedSnapshotData = snapshotSearchData ? normalizeContactRecords(snapshotSearchData) : [];
    const localCacheResult = buildLocalCachedContactsResult(activeSearch);
    const hasInvalidSnapshotShape =
      snapshotSearchData !== null &&
      snapshotSearchData !== undefined &&
      (!Array.isArray(snapshotSearchData) ||
        (snapshotSearchData.length > 0 && normalizedSnapshotData.length === 0));
    const effectiveSnapshot = hasInvalidSnapshotShape ? null : snapshot;

    if (hasInvalidSnapshotShape) {
      removeBrowserCache(cacheKey);
    }

    if (localCacheResult.contacts.length > 0 && options.policy !== "network-only") {
      setContacts(localCacheResult.contacts);
      setState("ready");
      setError(null);
      setCacheStatus({
        source: "cache",
        fetchedAt: localCacheResult.fetchedAt ?? Date.now(),
      });
      setRefreshMessage(localCacheResult.isPartial ? "Cached matches shown while refreshing." : null);
    } else if (options.policy !== "network-only") {
      setContacts([]);
      setState("empty");
      setCacheStatus(null);
    }

    const shouldFetch = shouldFetchSubmittedSearchNetwork(options.policy, effectiveSnapshot);
    if (!shouldFetch) {
      return;
    }

    if (localCacheResult.contacts.length > 0) {
      setIsRefreshing(true);
    } else {
      setState("loading");
      setError(null);
      setRefreshMessage(null);
    }

    const requestKey = `${cacheKey}:first`;
    let requestPromise = inFlightRef.current[requestKey];
    if (!requestPromise) {
      requestPromise = fetchContactsForSearch(activeSearch);
      inFlightRef.current[requestKey] = requestPromise;
    }

    try {
      const nextContacts = await requestPromise;
      if (requestToken !== requestTokenRef.current) {
        return;
      }
      const normalizedNextContacts = normalizeContactRecords(nextContacts);

      setContacts(normalizedNextContacts);
      setState(normalizedNextContacts.length ? "ready" : "empty");
      setError(null);
      setRefreshMessage(null);
      setCacheStatus({ source: "network", fetchedAt: Date.now() });

      writeBrowserCache(cacheKey, normalizedNextContacts, CONTACTS_SEARCH_CACHE_TTL_MS, {
        source: "network",
        fetchedAt: Date.now(),
      });
    } catch (loadError) {
      if (requestToken !== requestTokenRef.current) {
        return;
      }
      const hasVisibleCachedResults = localCacheResult.contacts.length > 0;
      if (hasVisibleCachedResults) {
        setRefreshMessage("Showing cached results. Could not refresh.");
        setState("ready");
        setError(null);
      } else {
        setState("error");
        setError(getErrorMessage(loadError, "Could not load contacts. Please try again."));
      }
    } finally {
      if (inFlightRef.current[requestKey] === requestPromise) {
        delete inFlightRef.current[requestKey];
      }
      if (requestToken === requestTokenRef.current) {
        setIsRefreshing(false);
      }
    }
  }

  useEffect(() => {
    if (!submittedSearch) {
      return;
    }
    void loadSearchData({ policy: "stale-while-revalidate" }, submittedSearch);
  }, [submittedSearch, submissionVersion]);

  function handleDraftChange<K extends keyof ContactSearchFormValues>(field: K, nextValue: ContactSearchFormValues[K]) {
    setDraft((current) => ({
      ...current,
      [field]: nextValue,
    }));
    if (searchMessage) {
      setSearchMessage(null);
    }
    if (refreshMessage) {
      setRefreshMessage(null);
    }
  }

  function handleSubmitSearch() {
    const result = draftSearchResult;
    if (!result.ok) {
      setSearchMessage(result.message);
      if (result.clearResults) {
        setSubmittedSearch(null);
        setState("idle");
        setContacts([]);
        setError(null);
        setCacheStatus(null);
        setRefreshMessage(null);
      }
      return;
    }

    setSearchMessage(null);
    setRefreshMessage(null);
    const isSameSearch = submittedSearch?.cacheKey === result.submitted.cacheKey;
    if (isSameSearch) {
      void loadSearchData({ policy: "stale-while-revalidate" }, result.submitted);
      return;
    }

    setSubmittedSearch(result.submitted);
    setSubmissionVersion((current) => current + 1);
  }

  function handleClearSearch() {
    const draftCacheKey = buildSearchCacheKey(normalizedDraft);
    const shouldClearSubmittedResults = Boolean(submittedSearch && submittedSearch.cacheKey === draftCacheKey);

    setDraft(INITIAL_SEARCH_FORM);
    setSearchMessage(null);
    setRefreshMessage(null);

    if (!shouldClearSubmittedResults) {
      return;
    }

    requestTokenRef.current += 1;
    setSubmittedSearch(null);
    setState("idle");
    setContacts([]);
    setError(null);
    setCacheStatus(null);
    setIsRefreshing(false);
  }

  function handleRefreshSearch() {
    if (!submittedSearch) {
      return;
    }
    void loadSearchData({ policy: "network-only" }, submittedSearch);
  }

  async function handleCopyContact(contact: ContactRecord) {
    const copyValue = buildContactCopyValue(contact);
    if (!copyValue) {
      toast.error("Could not copy contact", "Contact is missing an email address.");
      return;
    }

    try {
      await copyTextToClipboard(copyValue);
      toast.success("Copied contact");
    } catch {
      toast.error("Could not copy contact");
    }
  }

  function handleAddContact() {
    if (!canEditTradsphere) {
      return;
    }
    setModalMode("create");
    setModalContact(null);
    setFocusUsageToken(0);
    setModalDetailCacheStatus(null);
    setIsModalDetailRefreshing(false);
    setIsModalOpen(true);
  }

  function handleEditContact(contact: ContactRecord) {
    setModalMode("edit");
    setModalContact(contact);
    setFocusUsageToken(0);
    setModalDetailCacheStatus(null);
    setIsModalDetailRefreshing(false);
    setIsModalOpen(true);
    void loadModalContactDetail(contact, { policy: "stale-while-revalidate" });
  }

  function handleViewUsage(contact: ContactRecord) {
    setModalMode("edit");
    setModalContact(contact);
    setFocusUsageToken(Date.now());
    setModalDetailCacheStatus(null);
    setIsModalDetailRefreshing(false);
    setIsModalOpen(true);
    void loadModalContactDetail(contact, { policy: "stale-while-revalidate" });
  }

  function handleRefreshModalContactDetail() {
    if (modalMode !== "edit" || !modalContact) {
      return;
    }
    void loadModalContactDetail(modalContact, { policy: "network-only" });
  }

  async function handleModalSubmit(payload: ContactModalSubmitPayload): Promise<void> {
    const body = buildContactPayload(payload.form);

    if (payload.mode === "create") {
      await requestJson("/api/tradsphere/v1/contacts", {
        method: "POST",
        headers: requestHeaders,
        body,
        successToast: "Contact created",
      });
    } else {
      await requestJson("/api/tradsphere/v1/contacts", {
        method: "PUT",
        headers: requestHeaders,
        body: {
          id: payload.id,
          ...body,
        },
        successToast: "Contact updated",
      });
    }

    if (submittedSearch) {
      await loadSearchData({ policy: "network-only" }, submittedSearch);
    }
  }

  return (
    <div className="mx-auto flex w-full max-w-[1600px] flex-col gap-5 pb-12">
      <PageBanner
        eyebrow="TradSphere"
        title="Contacts"
        description="Find contacts, update contact details, and review station usage."
        gradientVariant="app"
        action={<Button onClick={handleAddContact} disabled={!canEditTradsphere}>Add Contact</Button>}
      />

      <ContactSearchForm
        value={draft}
        onChange={handleDraftChange}
        onSubmit={handleSubmitSearch}
        onClear={handleClearSearch}
        canSubmit={canSubmitSearch}
        canClear={canClearDraft}
        searching={state === "loading"}
        disabled={isRefreshing}
        resultText={resultText}
        message={searchMessage}
      />

      {refreshMessage ? (
        <p className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-2 text-sm text-amber-800">{refreshMessage}</p>
      ) : null}

      <div className="relative">
        <ContactResults
          state={state}
          groups={groups}
          error={error}
          onCopy={handleCopyContact}
          onEdit={handleEditContact}
          onViewUsage={handleViewUsage}
        />

        {shouldShowBlockingResultsOverlay ? (
          <div className="absolute inset-0 z-20 flex items-center justify-center rounded-2xl bg-white/70 backdrop-blur-[1px]">
            <div className="flex items-center gap-2 rounded-xl border border-slate-200 bg-white/95 px-4 py-3 text-sm font-medium text-slate-700 shadow-soft">
              <Loader2 className="size-4 animate-spin text-blue-600" />
              <span>Searching contacts...</span>
            </div>
          </div>
        ) : null}
      </div>

      {showCacheChip ? (
        <div className="pointer-events-none fixed inset-x-0 bottom-[calc(1rem+env(safe-area-inset-bottom))] z-40">
          <div className={`mx-4 sm:mx-6 lg:mr-6 ${sidebarVisuallyExpanded ? "lg:ml-[18.75rem]" : "lg:ml-[6.5rem]"}`}>
            <div className="mx-auto w-full max-w-[1600px]">
              <CacheStatusChip
                text={cacheStatusText}
                onRefresh={handleRefreshSearch}
                disabled={isRefreshing}
                refreshing={isRefreshing}
                refreshLabel="Refresh contacts"
                tooltipText="Click to refresh last submitted search"
                containerClassName="pointer-events-auto"
                className="max-w-[min(90vw,34rem)]"
              />
            </div>
          </div>
        </div>
      ) : null}

      <ContactModal
        open={isModalOpen}
        onOpenChange={(nextOpen) => {
          setIsModalOpen(nextOpen);
          if (!nextOpen) {
            modalDetailRequestTokenRef.current += 1;
            setIsModalDetailRefreshing(false);
          }
        }}
        mode={modalMode}
        canEdit={canEditTradsphere}
        initialContact={modalContact}
        focusUsageToken={focusUsageToken}
        detailCacheStatusText={modalDetailCacheStatusText}
        detailCacheRefreshing={isModalDetailRefreshing}
        detailCacheRefreshDisabled={modalMode !== "edit" || !modalContact}
        onRefreshDetailCache={handleRefreshModalContactDetail}
        onSubmit={handleModalSubmit}
      />
    </div>
  );
}
