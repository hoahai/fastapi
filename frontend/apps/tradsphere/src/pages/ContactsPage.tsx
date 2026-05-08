import { useEffect, useMemo, useRef, useState } from "react";
import { Loader2 } from "lucide-react";

import { ContactModal, type ContactModalMode, type ContactModalSubmitPayload } from "@/components/contacts/ContactModal";
import { ContactResults } from "@/components/contacts/ContactResults";
import { ContactSearchForm } from "@/components/contacts/ContactSearchForm";
import type { ContactGroup, ContactRecord, ContactSearchFormValues, ContactUsageRow } from "@/components/contacts/types";
import { PageBanner } from "@/components/layout/PageBanner";
import { Button } from "@/components/ui/button";
import { CacheStatusChip } from "@/components/ui/cache-status-chip";
import { useToast } from "@/components/ui/toast";
import { useApiRequest } from "@/hooks/useApiRequest";
import { readBrowserCacheSnapshot, writeBrowserCache } from "@/lib/browserCache";
import { shouldFetchNetwork, type CachePolicy } from "@shared/cache";

const CONTACTS_SEARCH_CACHE_TTL_MS = 5 * 60 * 1000;
const SIDEBAR_COLLAPSED_STORAGE_KEY = "workspace.sidebar.collapsed";
const LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY = "tradsphere:ui:sidebarCollapsed:v1";
const SIDEBAR_COLLAPSED_EVENT = "workspace-sidebar-collapsed-change";
const DEFAULT_CONTACT_TYPES = ["REP", "TRAFFIC", "BILLING"];

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
  company: "",
  contactType: "",
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
    company: asString(draft.company),
    contactType: asString(draft.contactType).toUpperCase(),
    phone: asString(draft.phone),
    station: asString(draft.station),
  };
}

function encodeKeyPart(value: string): string {
  return encodeURIComponent(value.trim().toLowerCase());
}

function buildSearchCacheKey(params: ContactSearchFormValues): string {
  return [
    "contacts:search",
    `name=${encodeKeyPart(params.name)}`,
    `email=${encodeKeyPart(params.email)}`,
    `company=${encodeKeyPart(params.company)}`,
    `contactType=${encodeKeyPart(params.contactType)}`,
    `phone=${encodeKeyPart(params.phone)}`,
    `station=${encodeKeyPart(params.station)}`,
    "v1",
  ].join(":");
}

function buildSearchSubmission(draft: ContactSearchFormValues): BuildSearchResult {
  const normalized = normalizeSearchForm(draft);
  const hasAnyInput = Object.values(normalized).some((value) => Boolean(asString(value)));

  if (!hasAnyInput) {
    return {
      ok: false,
      clearResults: true,
      message: "Enter at least one field before searching.",
    };
  }

  const hasServerFilter = Boolean(normalized.name || normalized.email || normalized.contactType);
  if (!hasServerFilter) {
    return {
      ok: false,
      clearResults: false,
      message:
        "Current backend search requires Name, Email, or Contact Type. Company, Phone, and Station are applied after server search.",
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

function normalizePhoneForSearch(value: string): string {
  return value.replace(/[^0-9a-z]/gi, "").toLowerCase();
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

function buildAuthHeaders(includeJsonContentType: boolean): HeadersInit {
  return {
    "X-API-Key": "6ad13c1f7c17c32fb5a4582b4be42df5",
    "X-Tenant-Id": "taaa",
    "X-User-Name": "Hai Truong",
    ...(includeJsonContentType ? { "Content-Type": "application/json" } : {}),
  };
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
  const buckets = new Map<string, { label: string; items: ContactRecord[] }>();
  for (const item of items) {
    const firstType = item.contactTypes[0] ?? "";
    const fallbackCompany = asString(item.company);
    const groupLabel = firstType || fallbackCompany || "Uncategorized";
    const groupKey = groupLabel.toUpperCase();
    const bucket = buckets.get(groupKey);
    if (bucket) {
      bucket.items.push(item);
    } else {
      buckets.set(groupKey, { label: groupLabel, items: [item] });
    }
  }

  const groups = [...buckets.entries()].map(([key, bucket]) => ({
    key,
    label: bucket.label,
    items: [...bucket.items].sort((a, b) => {
      const byName = buildContactFullName(a).localeCompare(buildContactFullName(b));
      if (byName !== 0) {
        return byName;
      }
      return a.email.localeCompare(b.email);
    }),
  }));

  return groups.sort((a, b) => a.label.localeCompare(b.label));
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
      usedByStationCount: 0,
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
      stationName: "",
      mediaType: "",
      market: null,
      contactType: asString(row.contactType).toUpperCase(),
      primaryContact: asBoolean(row.primaryContact),
      active: asBoolean(row.active),
    });
  }

  return rows;
}

function parseStationMap(payload: unknown): Record<string, { name: string; mediaType: string; market: string | null }> {
  const data = unwrapData(payload);
  if (!Array.isArray(data)) {
    return {};
  }

  const map: Record<string, { name: string; mediaType: string; market: string | null }> = {};
  for (const row of data) {
    if (!isRecord(row)) {
      continue;
    }
    const code = asString(row.code).toUpperCase();
    if (!code) {
      continue;
    }
    map[code] = {
      name: asString(row.name),
      mediaType: asString(row.mediaType).toUpperCase(),
      market: asString(row.market) || null,
    };
  }
  return map;
}

function mergeContactUsage(
  contacts: ContactRecord[],
  usageRows: ContactUsageJoinRow[],
  stationMap: Record<string, { name: string; mediaType: string; market: string | null }>,
): ContactRecord[] {
  const byContactId = new Map<number, ContactUsageRow[]>();

  for (const row of usageRows) {
    const contactId = row.contactId;
    const stationInfo = stationMap[row.stationCode] || { name: "", mediaType: "", market: null };
    const normalized: ContactUsageRow = {
      linkId: row.linkId,
      stationCode: row.stationCode,
      stationName: stationInfo.name,
      mediaType: stationInfo.mediaType,
      market: stationInfo.market,
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

    return {
      ...contact,
      stationCodes: combinedStationCodes,
      contactTypes: types,
      usage,
      usedByStationCount: usageStationCodes.length,
      isPrimaryContact: usage.some((item) => item.primaryContact),
    };
  });
}

function applyRefinementFilters(items: ContactRecord[], params: ContactSearchFormValues): ContactRecord[] {
  const normalizedCompany = asString(params.company).toLowerCase();
  const normalizedPhone = normalizePhoneForSearch(asString(params.phone));
  const normalizedStation = asString(params.station).toLowerCase();

  return items.filter((item) => {
    if (normalizedCompany) {
      const company = asString(item.company).toLowerCase();
      if (!company.includes(normalizedCompany)) {
        return false;
      }
    }

    if (normalizedPhone) {
      const office = normalizePhoneForSearch(item.office);
      const cell = normalizePhoneForSearch(item.cell);
      if (!office.includes(normalizedPhone) && !cell.includes(normalizedPhone)) {
        return false;
      }
    }

    if (normalizedStation) {
      const stationMatched = item.usage.some((usage) => {
        const stationCode = asString(usage.stationCode).toLowerCase();
        const stationName = asString(usage.stationName).toLowerCase();
        return stationCode.includes(normalizedStation) || stationName.includes(normalizedStation);
      });
      if (!stationMatched) {
        return false;
      }
    }

    return true;
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
  const requestHeaders = useMemo(() => buildAuthHeaders(false), []);

  const [draft, setDraft] = useState<ContactSearchFormValues>(INITIAL_SEARCH_FORM);
  const [submittedSearch, setSubmittedSearch] = useState<SubmittedSearch | null>(null);
  const [submissionVersion, setSubmissionVersion] = useState(0);
  const [searchMessage, setSearchMessage] = useState<string | null>(null);

  const [state, setState] = useState<SearchUiState>("idle");
  const [error, setError] = useState<string | null>(null);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [contacts, setContacts] = useState<ContactRecord[]>([]);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [modalMode, setModalMode] = useState<ContactModalMode>("create");
  const [modalContact, setModalContact] = useState<ContactRecord | null>(null);
  const [focusUsageToken, setFocusUsageToken] = useState(0);

  const [sidebarVisuallyExpanded, setSidebarVisuallyExpanded] = useState<boolean>(() => !readSidebarCollapsedState());

  const requestTokenRef = useRef(0);
  const inFlightRef = useRef<Record<string, Promise<ContactRecord[]>>>({});

  const groups = useMemo(() => buildGroups(contacts), [contacts]);
  const resultText = contacts.length ? formatResultText(contacts.length) : null;

  const discoveredContactTypes = useMemo(() => {
    const values = new Set<string>(DEFAULT_CONTACT_TYPES);
    for (const contact of contacts) {
      for (const type of contact.contactTypes) {
        const normalized = asString(type).toUpperCase();
        if (normalized) {
          values.add(normalized);
        }
      }
    }
    return [...values].sort((a, b) => a.localeCompare(b));
  }, [contacts]);

  const cacheStatusText = isRefreshing
    ? "Refreshing..."
    : cacheStatus
      ? `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`
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
    if (search.params.contactType) {
      params.set("contactType", search.params.contactType);
    }

    if (!params.toString()) {
      throw new Error("Search requires Name, Email, or Contact Type.");
    }

    const contactsPayload = await requestJson(`/api/tradsphere/v1/contacts?${params.toString()}`, {
      headers: requestHeaders,
      errorToast: false,
    });
    const parsedContacts = parseContacts(contactsPayload);
    if (!parsedContacts.length) {
      return [];
    }

    const contactIds = uniqStrings(parsedContacts.map((item) => String(item.id)));
    const usagePayload = await requestJson(
      `/api/tradsphere/v1/contacts/stationsContacts?contactIds=${encodeURIComponent(contactIds.join(","))}&active=true`,
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

    let stationMap: Record<string, { name: string; mediaType: string; market: string | null }> = {};
    if (stationCodes.length > 0) {
      const stationsPayload = await requestJson(
        `/api/tradsphere/v1/stations?codes=${encodeURIComponent(stationCodes.join(","))}&deliveryMethodDetail=false&contactDetail=false`,
        {
          headers: requestHeaders,
          errorToast: false,
        },
      );
      stationMap = parseStationMap(stationsPayload);
    }

    const withUsage = mergeContactUsage(parsedContacts, usageRows, stationMap);
    return applyRefinementFilters(withUsage, search.params);
  }

  async function loadSearchData(options: SearchLoadOptions, activeSearch: SubmittedSearch | null = submittedSearch): Promise<void> {
    if (!activeSearch) {
      setState("idle");
      setError(null);
      setContacts([]);
      setCacheStatus(null);
      return;
    }

    const requestToken = ++requestTokenRef.current;
    const cacheKey = activeSearch.cacheKey;
    const snapshot = readBrowserCacheSnapshot<ContactRecord[]>(cacheKey);

    if (snapshot?.data && options.policy !== "network-only") {
      setContacts(snapshot.data);
      setState(snapshot.data.length ? "ready" : "empty");
      setError(null);
      setCacheStatus({
        source: "cache",
        fetchedAt: snapshot.fetchedAt,
      });
    }

    const shouldFetch = shouldFetchNetwork(options.policy, snapshot);
    if (!shouldFetch) {
      return;
    }

    if (snapshot?.data) {
      setIsRefreshing(true);
    } else {
      setState("loading");
      setError(null);
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

      setContacts(nextContacts);
      setState(nextContacts.length ? "ready" : "empty");
      setError(null);
      setCacheStatus({ source: "network", fetchedAt: Date.now() });

      writeBrowserCache(cacheKey, nextContacts, CONTACTS_SEARCH_CACHE_TTL_MS, {
        source: "network",
        fetchedAt: Date.now(),
      });
    } catch (loadError) {
      if (requestToken !== requestTokenRef.current) {
        return;
      }
      setState("error");
      setError(getErrorMessage(loadError, "Could not load contacts. Please try again."));
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
  }

  function handleSubmitSearch() {
    const result = buildSearchSubmission(draft);
    if (!result.ok) {
      setSearchMessage(result.message);
      if (result.clearResults) {
        setSubmittedSearch(null);
        setState("idle");
        setContacts([]);
        setError(null);
        setCacheStatus(null);
      }
      return;
    }

    setSearchMessage(null);
    const isSameSearch = submittedSearch?.cacheKey === result.submitted.cacheKey;
    if (isSameSearch) {
      void loadSearchData({ policy: "network-only" }, result.submitted);
      return;
    }

    setSubmittedSearch(result.submitted);
    setSubmissionVersion((current) => current + 1);
  }

  function handleClearSearch() {
    setDraft(INITIAL_SEARCH_FORM);
    setSearchMessage(null);
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
    setModalMode("create");
    setModalContact(null);
    setFocusUsageToken(0);
    setIsModalOpen(true);
  }

  function handleEditContact(contact: ContactRecord) {
    setModalMode("edit");
    setModalContact(contact);
    setFocusUsageToken(0);
    setIsModalOpen(true);
  }

  function handleViewUsage(contact: ContactRecord) {
    setModalMode("edit");
    setModalContact(contact);
    setFocusUsageToken(Date.now());
    setIsModalOpen(true);
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
        action={<Button onClick={handleAddContact}>Add Contact</Button>}
      />

      <ContactSearchForm
        value={draft}
        contactTypeOptions={discoveredContactTypes}
        onChange={handleDraftChange}
        onSubmit={handleSubmitSearch}
        onClear={handleClearSearch}
        searching={state === "loading"}
        disabled={isRefreshing}
        resultText={resultText}
        message={searchMessage}
      />

      <div className="relative">
        <ContactResults
          state={state}
          groups={groups}
          error={error}
          disabled={isRefreshing}
          onCopy={handleCopyContact}
          onEdit={handleEditContact}
          onViewUsage={handleViewUsage}
        />

        {state === "loading" || isRefreshing ? (
          <div className="absolute inset-0 z-20 flex items-center justify-center rounded-2xl bg-white/70 backdrop-blur-[1px]">
            <div className="flex items-center gap-2 rounded-xl border border-slate-200 bg-white/95 px-4 py-3 text-sm font-medium text-slate-700 shadow-soft">
              <Loader2 className="size-4 animate-spin text-blue-600" />
              <span>{isRefreshing ? "Refreshing contacts..." : "Searching contacts..."}</span>
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
        onOpenChange={setIsModalOpen}
        mode={modalMode}
        initialContact={modalContact}
        focusUsageToken={focusUsageToken}
        onSubmit={handleModalSubmit}
      />
    </div>
  );
}
