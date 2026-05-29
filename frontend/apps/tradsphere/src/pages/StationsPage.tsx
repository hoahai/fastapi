import { useEffect, useMemo, useRef, useState } from "react";

import {
  StationModal,
  type StationModalMode,
  type StationModalSaveResult,
} from "@/components/dashboard/StationModal";
import { PageBanner } from "@/components/layout/PageBanner";
import { StationResults } from "@/components/stations/StationResults";
import { StationSearchForm } from "@/components/stations/StationSearchForm";
import type {
  StationContactSummary,
  StationGroup,
  StationRecord,
  StationSearchFormValues,
} from "@/components/stations/types";
import { Button } from "@/components/ui/button";
import { useApiRequest } from "@/hooks/useApiRequest";
import { usePersistentState } from "@/hooks/usePersistentState";
import { useOnlineStatus } from "@/hooks/useOnlineStatus";
import {
  listBrowserCacheSnapshotsByPrefix,
  readBrowserCacheSnapshot,
  removeBrowserCache,
  writeBrowserCache,
} from "@/lib/browserCache";
import { buildAuthHeaders as buildSharedAuthHeaders } from "@shared/api/authHeaders";
import { useAuth } from "@shared/auth/useAuth";
import { shouldProtectFrontendAuth } from "@shared/auth/guards";
import { hasAppEditAccess } from "@shared/auth/permissions";
import { AppPageLayout } from "@shared/components/layout/AppPageLayout";
import { PageCacheFooter } from "@shared/components/layout/PageCacheFooter";
import { PageLoadingLayer, SectionLoadingLayer } from "@shared/components/status/LoadingOverlay";
import { PageMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { resolveSharedLoadingContract } from "@shared/components/status/loadingContract";
import { shouldFetchSubmittedSearchNetwork } from "@shared/search";
import { hasAtLeastOneSearchCriterion } from "@shared/search";
import { type CachePolicy } from "@shared/cache";

const STATIONS_SEARCH_CACHE_TTL_MS = 5 * 60 * 1000;
const STATIONS_SEARCH_COLLECTION_PREFIX = "stations:search:";
const STATIONS_SEARCH_COLLECTION_LIMIT = 40;
const STATIONS_SEARCH_DRAFT_STORAGE_KEY = "tradsphere.stations.searchDraft.v1";
const STATIONS_SUBMITTED_SEARCH_STORAGE_KEY = "tradsphere.stations.submittedSearch.v1";
const STATIONS_GROUP_OPEN_STORAGE_KEY = "tradsphere.stations.groupOpen.v1";

type SearchUiState = "idle" | "loading" | "ready" | "empty" | "error";

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type SubmittedSearch = {
  params: StationSearchFormValues;
  cacheKey: string;
};

type SearchLoadOptions = {
  policy: CachePolicy;
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

type LocalCachedResult = {
  stations: StationRecord[];
  fetchedAt: number | null;
  isPartial: boolean;
};

const INITIAL_SEARCH_FORM: StationSearchFormValues = {
  stationCode: "",
  stationName: "",
  mediaType: "",
  language: "",
  affiliation: "",
  contact: "",
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

function splitCodeTokens(value: string): string[] {
  return uniqStrings(
    value
      .split(/[\s,;|]+/)
      .map((item) => item.trim().toUpperCase())
      .filter(Boolean),
  );
}

function normalizeSearchForm(draft: StationSearchFormValues): StationSearchFormValues {
  return {
    stationCode: asString(draft.stationCode).toUpperCase(),
    stationName: asString(draft.stationName),
    mediaType: asString(draft.mediaType).toUpperCase(),
    language: asString(draft.language),
    affiliation: asString(draft.affiliation),
    contact: asString(draft.contact),
  };
}

function isStationSearchFormValues(value: unknown): value is StationSearchFormValues {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.stationCode === "string" &&
    typeof value.stationName === "string" &&
    typeof value.mediaType === "string" &&
    typeof value.language === "string" &&
    typeof value.affiliation === "string" &&
    typeof value.contact === "string"
  );
}

function encodeKeyPart(value: string): string {
  return encodeURIComponent(value.trim().toLowerCase());
}

function buildSearchCacheKey(params: StationSearchFormValues): string {
  return [
    "stations:search",
    `stationCode=${encodeKeyPart(params.stationCode)}`,
    `stationName=${encodeKeyPart(params.stationName)}`,
    `mediaType=${encodeKeyPart(params.mediaType)}`,
    `language=${encodeKeyPart(params.language)}`,
    `affiliation=${encodeKeyPart(params.affiliation)}`,
    `contact=${encodeKeyPart(params.contact)}`,
    "v2",
  ].join(":");
}

function isSubmittedSearch(value: unknown): value is SubmittedSearch {
  if (!isRecord(value)) {
    return false;
  }
  return isStationSearchFormValues(value.params) && typeof value.cacheKey === "string";
}

function normalizeSearchText(value: string): string {
  return value.trim().toLowerCase();
}

function buildDisplayName(raw: Record<string, unknown>): string {
  const explicit = asString(raw.name);
  if (explicit) {
    return explicit;
  }
  const firstName = asString(raw.firstName);
  const lastName = asString(raw.lastName);
  return [firstName, lastName].filter(Boolean).join(" ").trim();
}

function parseStationContacts(payload: unknown): {
  contacts: StationContactSummary[];
  repContacts: StationContactSummary[];
} {
  if (!isRecord(payload)) {
    return { contacts: [], repContacts: [] };
  }

  const contacts: StationContactSummary[] = [];
  for (const [rawType, rawList] of Object.entries(payload)) {
    const contactType = asString(rawType).toUpperCase() || "UNKNOWN";
    if (!Array.isArray(rawList)) {
      continue;
    }

    for (const entry of rawList) {
      if (typeof entry === "string") {
        contacts.push({
          id: null,
          contactType,
          fullName: "",
          email: asString(entry).toLowerCase(),
          primaryContact: false,
        });
        continue;
      }

      if (!isRecord(entry)) {
        continue;
      }

      const id = asNumber(entry.id);
      const email = asString(entry.email).toLowerCase();
      const fullName = buildDisplayName(entry);
      contacts.push({
        id: id !== null ? Math.trunc(id) : null,
        contactType,
        fullName,
        email,
        primaryContact: asBoolean(entry.primaryContact),
      });
    }
  }

  const repContacts = contacts
    .filter((item) => item.contactType === "REP")
    .sort((left, right) => {
      if (left.primaryContact !== right.primaryContact) {
        return left.primaryContact ? -1 : 1;
      }
      return `${left.fullName} ${left.email}`.localeCompare(`${right.fullName} ${right.email}`);
    });

  return { contacts, repContacts };
}

function parseRepContacts(payload: unknown): StationContactSummary[] {
  if (!Array.isArray(payload)) {
    return [];
  }

  const repContacts: StationContactSummary[] = [];
  const seen = new Set<string>();
  for (const entry of payload) {
    if (!isRecord(entry)) {
      continue;
    }
    const id = asNumber(entry.id);
    const fullName = asString(entry.fullName || entry.name);
    const email = asString(entry.email).toLowerCase();
    const primaryContact = asBoolean(entry.primaryContact);
    if (!fullName && !email) {
      continue;
    }
    const fingerprint = `${fullName.toLowerCase()}::${email}`;
    if (seen.has(fingerprint)) {
      continue;
    }
    seen.add(fingerprint);
    repContacts.push({
      id: id !== null ? Math.trunc(id) : null,
      contactType: "REP",
      fullName,
      email,
      primaryContact,
    });
  }

  repContacts.sort((left, right) => {
    if (left.primaryContact !== right.primaryContact) {
      return left.primaryContact ? -1 : 1;
    }
    return `${left.fullName} ${left.email}`.localeCompare(`${right.fullName} ${right.email}`);
  });

  return repContacts;
}

function normalizeStationRecords(items: unknown): StationRecord[] {
  if (!Array.isArray(items)) {
    return [];
  }

  const normalized: StationRecord[] = [];
  for (const item of items) {
    if (!isRecord(item)) {
      continue;
    }

    const code = asString(item.code).toUpperCase();
    if (!code) {
      continue;
    }

    const { contacts, repContacts } = parseStationContacts(item.contacts);
    const explicitRepContacts = parseRepContacts(item.repContacts);
    const deliveryPayload = isRecord(item.deliveryMethod) ? item.deliveryMethod : null;
    const deliveryMethodId = asNumber(item.deliveryMethodId);

    normalized.push({
      code,
      name: asString(item.name),
      mediaType: asString(item.mediaType).toUpperCase(),
      syscode: asString(item.syscode),
      language: asString(item.language),
      affiliation: asString(item.affiliation),
      deliveryMethodId: deliveryMethodId !== null ? Math.trunc(deliveryMethodId) : null,
      deliveryMethod: deliveryPayload
        ? {
            id: asNumber(deliveryPayload.id),
            name: asString(deliveryPayload.name),
            url: asString(deliveryPayload.url),
            username: asString(deliveryPayload.username),
            deadline: asString(deliveryPayload.deadline),
            note: asString(deliveryPayload.note),
          }
        : null,
      contacts,
      repContacts: explicitRepContacts.length > 0 ? explicitRepContacts : repContacts,
      note: asString(item.note),
    });
  }

  return normalized.sort((a, b) => a.code.localeCompare(b.code));
}

function buildSearchSubmission(draft: StationSearchFormValues): BuildSearchResult {
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

function stationMatchesSearch(station: StationRecord, submitted: SubmittedSearch): boolean {
  const params = submitted.params;

  const codeQuery = normalizeSearchText(params.stationCode);
  if (codeQuery && !normalizeSearchText(station.code).includes(codeQuery)) {
    return false;
  }

  const nameQuery = normalizeSearchText(params.stationName);
  if (nameQuery && !normalizeSearchText(station.name).includes(nameQuery)) {
    return false;
  }

  const mediaTypeQuery = normalizeSearchText(params.mediaType);
  if (mediaTypeQuery && !normalizeSearchText(station.mediaType).includes(mediaTypeQuery)) {
    return false;
  }

  const languageQuery = normalizeSearchText(params.language);
  if (languageQuery && !normalizeSearchText(station.language).includes(languageQuery)) {
    return false;
  }

  const affiliationQuery = normalizeSearchText(params.affiliation);
  if (affiliationQuery && !normalizeSearchText(station.affiliation).includes(affiliationQuery)) {
    return false;
  }

  const contactQuery = normalizeSearchText(params.contact);
  if (contactQuery) {
    const contactText = station.contacts
      .map((contact) => [contact.fullName, contact.email, contact.contactType].join(" "))
      .join(" ");
    if (!normalizeSearchText(contactText).includes(contactQuery)) {
      return false;
    }
  }

  return true;
}

function buildLocalCachedStationsResult(submitted: SubmittedSearch): LocalCachedResult {
  const exactSnapshot = readBrowserCacheSnapshot<StationRecord[]>(submitted.cacheKey);
  const exactStations = normalizeStationRecords(exactSnapshot?.data);
  const deduped: StationRecord[] = [];
  const seen = new Set<string>();
  let newestFetchedAt = exactSnapshot?.fetchedAt ?? 0;

  const appendStation = (station: StationRecord) => {
    const key = station.code.toUpperCase();
    if (seen.has(key)) {
      return;
    }
    seen.add(key);
    deduped.push(station);
  };

  for (const station of exactStations) {
    appendStation(station);
  }

  const cacheEntries = listBrowserCacheSnapshotsByPrefix<StationRecord[]>(
    STATIONS_SEARCH_COLLECTION_PREFIX,
    {
      allowExpired: true,
      limit: STATIONS_SEARCH_COLLECTION_LIMIT,
    },
  );

  for (const entry of cacheEntries) {
    newestFetchedAt = Math.max(newestFetchedAt, entry.snapshot.fetchedAt);
    const stations = normalizeStationRecords(entry.snapshot.data);
    for (const station of stations) {
      if (!stationMatchesSearch(station, submitted)) {
        continue;
      }
      appendStation(station);
    }
  }

  if (deduped.length === 0 && exactSnapshot && Array.isArray(exactSnapshot.data)) {
    return {
      stations: [],
      fetchedAt: exactSnapshot.fetchedAt,
      isPartial: false,
    };
  }

  return {
    stations: deduped.sort((a, b) => a.code.localeCompare(b.code)),
    fetchedAt: newestFetchedAt || null,
    isPartial: exactStations.length === 0 && deduped.length > 0,
  };
}

function resolveInitialStationsSearchView(submittedSearch: SubmittedSearch | null): {
  state: SearchUiState;
  stations: StationRecord[];
  cacheStatus: CacheStatus | null;
} {
  if (!submittedSearch) {
    return {
      state: "idle",
      stations: [],
      cacheStatus: null,
    };
  }

  const localResult = buildLocalCachedStationsResult(submittedSearch);
  if (!localResult.stations.length && localResult.fetchedAt === null) {
    return {
      state: "idle",
      stations: [],
      cacheStatus: null,
    };
  }

  return {
    state: localResult.stations.length ? "ready" : "empty",
    stations: localResult.stations,
    cacheStatus: {
      source: "cache",
      fetchedAt: localResult.fetchedAt ?? Date.now(),
    },
  };
}

function buildGroups(items: StationRecord[]): StationGroup[] {
  const buckets = new Map<string, { label: string; items: StationRecord[] }>();

  for (const item of items) {
    const mediaType = asString(item.mediaType).toUpperCase();
    const stationName = asString(item.name);
    const groupLabel =
      mediaType
        ? `Media Type: ${mediaType}`
        : `Name: ${(stationName[0] || item.code[0] || "#").toUpperCase()}`;
    const groupKey = groupLabel.toUpperCase();

    const bucket = buckets.get(groupKey);
    if (bucket) {
      bucket.items.push(item);
    } else {
      buckets.set(groupKey, { label: groupLabel, items: [item] });
    }
  }

  return [...buckets.entries()]
    .map(([key, bucket]) => ({
      key,
      label: bucket.label,
      items: [...bucket.items].sort((a, b) => a.code.localeCompare(b.code)),
    }))
    .sort((a, b) => a.label.localeCompare(b.label));
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

function formatResultText(total: number): string {
  if (total === 1) {
    return "1 station found.";
  }
  return `${total} stations found.`;
}

function getErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim();
  }
  return fallback;
}

function chunkStrings(values: string[], size: number): string[][] {
  if (values.length === 0 || size <= 0) {
    return [];
  }

  const output: string[][] = [];
  for (let index = 0; index < values.length; index += size) {
    output.push(values.slice(index, index + size));
  }
  return output;
}

function stationToCatalogItem(station: StationRecord): {
  code: string;
  name?: string | null;
  mediaType?: string | null;
  deliveryMethodId?: number | null;
} {
  return {
    code: station.code,
    name: station.name || null,
    mediaType: station.mediaType || null,
    deliveryMethodId: station.deliveryMethodId,
  };
}

function toStationRecordFromSaveResult(result: StationModalSaveResult): StationRecord | null {
  const savedStation = isRecord(result.station) ? result.station : null;
  if (!savedStation) {
    return null;
  }
  const code = asString(savedStation.code).toUpperCase() || asString(result.stationCode).toUpperCase();
  if (!code) {
    return null;
  }

  const savedContacts = Array.isArray(savedStation.contacts) ? savedStation.contacts : [];
  const contacts: StationContactSummary[] = [];
  for (const rawContact of savedContacts) {
    if (!isRecord(rawContact)) {
      continue;
    }
    const id = asNumber(rawContact.id);
    contacts.push({
      id: id !== null ? Math.trunc(id) : null,
      contactType: asString(rawContact.contactType).toUpperCase() || "UNKNOWN",
      fullName: asString(rawContact.fullName),
      email: asString(rawContact.email).toLowerCase(),
      primaryContact: asBoolean(rawContact.primaryContact),
    });
  }

  const savedRepContacts = Array.isArray(savedStation.repContacts) ? savedStation.repContacts : [];
  const repContacts: StationContactSummary[] = [];
  for (const rawContact of savedRepContacts) {
    if (!isRecord(rawContact)) {
      continue;
    }
    const id = asNumber(rawContact.id);
    repContacts.push({
      id: id !== null ? Math.trunc(id) : null,
      contactType: "REP",
      fullName: asString(rawContact.fullName),
      email: asString(rawContact.email).toLowerCase(),
      primaryContact: asBoolean(rawContact.primaryContact),
    });
  }

  const deliveryMethodRaw = isRecord(savedStation.deliveryMethod) ? savedStation.deliveryMethod : null;
  const deliveryMethodId = asNumber(savedStation.deliveryMethodId);

  return {
    code,
    name: asString(savedStation.name),
    mediaType: asString(savedStation.mediaType).toUpperCase(),
    syscode: asString(savedStation.syscode),
    language: asString(savedStation.language),
    affiliation: asString(savedStation.affiliation),
    deliveryMethodId: deliveryMethodId !== null ? Math.trunc(deliveryMethodId) : null,
    deliveryMethod: deliveryMethodRaw
      ? {
          id: asNumber(deliveryMethodRaw.id),
          name: asString(deliveryMethodRaw.name),
          url: asString(deliveryMethodRaw.url),
          username: asString(deliveryMethodRaw.username),
          deadline: asString(deliveryMethodRaw.deadline),
          note: asString(deliveryMethodRaw.note),
        }
      : null,
    contacts,
    repContacts,
    note: asString(savedStation.note),
  };
}

function applySavedStationToResults(
  current: StationRecord[],
  submitted: SubmittedSearch,
  result: StationModalSaveResult,
): StationRecord[] {
  const savedStation = toStationRecordFromSaveResult(result);
  const previousStationCode = asString(result.previousStationCode).toUpperCase();
  const normalizedStationCode = asString(result.stationCode).toUpperCase();
  const targetCode = savedStation?.code || normalizedStationCode || previousStationCode;

  const filteredCurrent = current.filter((station) => {
    const stationCode = asString(station.code).toUpperCase();
    if (previousStationCode && stationCode === previousStationCode) {
      return false;
    }
    if (stationCode === targetCode) {
      return false;
    }
    return true;
  });

  if (!savedStation) {
    return filteredCurrent.sort((a, b) => a.code.localeCompare(b.code));
  }

  if (!stationMatchesSearch(savedStation, submitted)) {
    return filteredCurrent.sort((a, b) => a.code.localeCompare(b.code));
  }

  return [...filteredCurrent, savedStation].sort((a, b) => a.code.localeCompare(b.code));
}

export default function StationsPage() {
  const { requestJson } = useApiRequest();
  const { isOnline } = useOnlineStatus();
  const auth = useAuth();
  const requestHeaders = useMemo(
    () => buildSharedAuthHeaders(auth.session, auth.tenantSlug, false),
    [auth.session, auth.tenantSlug],
  );
  const canEditTradsphere = useMemo(() => {
    if (!shouldProtectFrontendAuth()) {
      return true;
    }
    return hasAppEditAccess(auth.accessProfile, "tradsphere");
  }, [auth.accessProfile]);

  const [draft, setDraft] = usePersistentState<StationSearchFormValues>(
    STATIONS_SEARCH_DRAFT_STORAGE_KEY,
    INITIAL_SEARCH_FORM,
    { storage: "session", validate: isStationSearchFormValues },
  );
  const [submittedSearch, setSubmittedSearch] = usePersistentState<SubmittedSearch | null>(
    STATIONS_SUBMITTED_SEARCH_STORAGE_KEY,
    null,
    { storage: "session", validate: (value): value is SubmittedSearch | null => value === null || isSubmittedSearch(value) },
  );
  const [groupOpenState, setGroupOpenState] = usePersistentState<Record<string, boolean>>(
    STATIONS_GROUP_OPEN_STORAGE_KEY,
    {},
    { storage: "session", validate: (value): value is Record<string, boolean> => isRecord(value) },
  );

  const [submissionVersion, setSubmissionVersion] = useState(0);
  const [searchMessage, setSearchMessage] = useState<string | null>(null);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);

  const [state, setState] = useState<SearchUiState>(() => resolveInitialStationsSearchView(submittedSearch).state);
  const [error, setError] = useState<string | null>(null);
  const [hasHydratedPageState, setHasHydratedPageState] = useState(() => !submittedSearch);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isChipRefreshOverlayVisible, setIsChipRefreshOverlayVisible] = useState(false);
  const [stations, setStations] = useState<StationRecord[]>(() => resolveInitialStationsSearchView(submittedSearch).stations);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(() => resolveInitialStationsSearchView(submittedSearch).cacheStatus);

  const [isStationModalOpen, setIsStationModalOpen] = useState(false);
  const [stationModalMode, setStationModalMode] = useState<StationModalMode>("create");
  const [stationModalCode, setStationModalCode] = useState<string | null>(null);

  const requestTokenRef = useRef(0);
  const inFlightRef = useRef<Record<string, Promise<StationRecord[]>>>({});

  const groups = useMemo(() => buildGroups(stations), [stations]);
  const resultText = stations.length ? formatResultText(stations.length) : null;
  const normalizedDraft = useMemo(() => normalizeSearchForm(draft), [draft]);
  const canClearDraft = useMemo(() => hasAtLeastOneSearchCriterion(normalizedDraft), [normalizedDraft]);
  const draftSearchResult = useMemo(() => buildSearchSubmission(draft), [draft]);
  const canSubmitSearch = draftSearchResult.ok;
  const cacheStatusText = isRefreshing
    ? "Refreshing..."
    : !isOnline && cacheStatus
      ? `Offline. Showing cached data from ${formatRelativeTime(cacheStatus.fetchedAt)}.`
    : cacheStatus
      ? `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`
      : "No cached data yet";

  const showCacheChip = Boolean(submittedSearch && !isStationModalOpen);
  const loadingContract = resolveSharedLoadingContract(
    {
      pageInitializing: !hasHydratedPageState,
      cacheChipRefreshing: isChipRefreshOverlayVisible,
      searchLoading: state === "loading" || isRefreshing,
    },
    {
      pageInitializing: "Preparing stations workspace...",
      cacheChipRefreshing: "Refreshing stations...",
      searchLoading: isRefreshing ? "Refreshing results..." : "Searching stations...",
    },
  );
  const pageMessages: StackMessage[] = [];
  if (refreshMessage) {
    pageMessages.push({
      id: "stations-refresh-message",
      variant: refreshMessage.toLowerCase().includes("offline") ? "info" : "warning",
      message: refreshMessage,
    });
  }

  useEffect(() => {
    if (!groups.length) {
      return;
    }

    setGroupOpenState((current) => {
      const next = { ...current };
      let changed = false;
      for (const group of groups) {
        if (group.key in next) {
          continue;
        }
        next[group.key] = true;
        changed = true;
      }
      return changed ? next : current;
    });
  }, [groups, setGroupOpenState]);

  async function fetchStationCodesByContactQuery(contactQuery: string): Promise<string[]> {
    const normalizedQuery = asString(contactQuery);
    if (!normalizedQuery) {
      return [];
    }
    const params = new URLSearchParams();
    params.set("q", normalizedQuery);
    params.set("active", "true");
    const payload = await requestJson(`/api/tradsphere/v1/contacts/station-codes?${params.toString()}`, {
      headers: requestHeaders,
      errorToast: false,
    });
    const data = unwrapData(payload);
    if (!isRecord(data)) {
      return [];
    }
    const stationCodes = Array.isArray(data.stationCodes)
      ? data.stationCodes.map((value) => asString(value).toUpperCase()).filter(Boolean)
      : [];
    return uniqStrings(stationCodes);
  }

  async function fetchStationsByCodes(
    codes: string[],
    options?: {
      mediaType?: string;
      language?: string;
      affiliation?: string;
    },
  ): Promise<StationRecord[]> {
    const normalizedCodes = uniqStrings(codes.map((code) => asString(code).toUpperCase()).filter(Boolean));
    if (!normalizedCodes.length) {
      return [];
    }

    const rows: StationRecord[] = [];
    for (const chunk of chunkStrings(normalizedCodes, 80)) {
      const params = new URLSearchParams();
      params.set("codes", chunk.join(","));
      if (options?.mediaType) {
        params.set("mediaType", options.mediaType);
      }
      if (options?.language) {
        params.set("language", options.language);
      }
      if (options?.affiliation) {
        params.set("affiliation", options.affiliation);
      }
      params.set("deliveryMethodDetail", "true");
      params.set("contactDetail", "true");
      const payload = await requestJson(
        `/api/tradsphere/v1/stations?${params.toString()}`,
        {
          headers: requestHeaders,
          errorToast: false,
        },
      );
      rows.push(...normalizeStationRecords(unwrapData(payload)));
    }
    return rows;
  }

  async function fetchStationsByName(
    name: string,
    options?: {
      mediaType?: string;
      language?: string;
      affiliation?: string;
    },
  ): Promise<StationRecord[]> {
    const normalizedName = asString(name);
    if (!normalizedName) {
      return [];
    }

    const params = new URLSearchParams();
    params.set("name", normalizedName);
    if (options?.mediaType) {
      params.set("mediaType", options.mediaType);
    }
    if (options?.language) {
      params.set("language", options.language);
    }
    if (options?.affiliation) {
      params.set("affiliation", options.affiliation);
    }
    params.set("deliveryMethodDetail", "true");
    params.set("contactDetail", "true");
    const payload = await requestJson(`/api/tradsphere/v1/stations?${params.toString()}`, {
      headers: requestHeaders,
      errorToast: false,
    });
    return normalizeStationRecords(unwrapData(payload));
  }

  async function fetchStationsForSearch(search: SubmittedSearch): Promise<StationRecord[]> {
    const codeFilters = splitCodeTokens(search.params.stationCode);
    const nameFilter = asString(search.params.stationName);
    const contactFilter = asString(search.params.contact);
    const mediaTypeFilter = asString(search.params.mediaType).toUpperCase();
    const languageFilter = asString(search.params.language);
    const affiliationFilter = asString(search.params.affiliation);

    const contactCodes = contactFilter ? await fetchStationCodesByContactQuery(contactFilter) : [];
    const contactCodeSet = new Set(contactCodes);

    let candidateCodes = codeFilters;
    if (contactFilter) {
      candidateCodes =
        codeFilters.length > 0
          ? codeFilters.filter((code) => contactCodeSet.has(code))
          : contactCodes;
    }

    if (contactFilter && candidateCodes.length === 0) {
      return [];
    }

    const needsStationFetch = Boolean(nameFilter || candidateCodes.length > 0);
    const hasServerFilters = Boolean(mediaTypeFilter || languageFilter || affiliationFilter);
    if (!needsStationFetch && !hasServerFilters) {
      return [];
    }

    const mergedByCode = new Map<string, StationRecord>();

    if (nameFilter) {
      const byName = await fetchStationsByName(nameFilter, {
        mediaType: mediaTypeFilter || undefined,
        language: languageFilter || undefined,
        affiliation: affiliationFilter || undefined,
      });
      for (const station of byName) {
        mergedByCode.set(station.code, station);
      }
    }

    if (candidateCodes.length > 0) {
      const byCode = await fetchStationsByCodes(candidateCodes, {
        mediaType: mediaTypeFilter || undefined,
        language: languageFilter || undefined,
        affiliation: affiliationFilter || undefined,
      });
      for (const station of byCode) {
        mergedByCode.set(station.code, station);
      }
    }

    if (!nameFilter && candidateCodes.length === 0 && hasServerFilters) {
      const params = new URLSearchParams();
      if (mediaTypeFilter) {
        params.set("mediaType", mediaTypeFilter);
      }
      if (languageFilter) {
        params.set("language", languageFilter);
      }
      if (affiliationFilter) {
        params.set("affiliation", affiliationFilter);
      }
      params.set("deliveryMethodDetail", "true");
      params.set("contactDetail", "true");
      const payload = await requestJson(`/api/tradsphere/v1/stations?${params.toString()}`, {
        headers: requestHeaders,
        errorToast: false,
      });
      for (const station of normalizeStationRecords(unwrapData(payload))) {
        mergedByCode.set(station.code, station);
      }
    }

    const merged = [...mergedByCode.values()]
      .filter((station) => stationMatchesSearch(station, search))
      .sort((a, b) => a.code.localeCompare(b.code));

    return merged;
  }

  async function loadSearchData(options: SearchLoadOptions, activeSearch: SubmittedSearch | null = submittedSearch): Promise<void> {
    if (!activeSearch) {
      setState("idle");
      setError(null);
      setStations([]);
      setCacheStatus(null);
      setRefreshMessage(null);
      setHasHydratedPageState(true);
      return;
    }

    const requestToken = ++requestTokenRef.current;
    const cacheKey = activeSearch.cacheKey;
    const snapshot = readBrowserCacheSnapshot<StationRecord[]>(cacheKey);
    const snapshotData = snapshot?.data;
    const normalizedSnapshotData = snapshotData ? normalizeStationRecords(snapshotData) : [];
    const localCacheResult = buildLocalCachedStationsResult(activeSearch);
    const hasInvalidSnapshotShape =
      snapshotData !== null &&
      snapshotData !== undefined &&
      (!Array.isArray(snapshotData) || (snapshotData.length > 0 && normalizedSnapshotData.length === 0));
    const effectiveSnapshot = hasInvalidSnapshotShape ? null : snapshot;

    if (hasInvalidSnapshotShape) {
      removeBrowserCache(cacheKey);
    }

    if (localCacheResult.stations.length > 0 && options.policy !== "network-only") {
      setStations(localCacheResult.stations);
      setState("ready");
      setError(null);
      setCacheStatus({
        source: "cache",
        fetchedAt: localCacheResult.fetchedAt ?? Date.now(),
      });
      setRefreshMessage(localCacheResult.isPartial ? "Cached matches shown while refreshing." : null);
    } else if (options.policy !== "network-only") {
      setStations([]);
      setState("empty");
      setCacheStatus(null);
    }

    const shouldFetch = shouldFetchSubmittedSearchNetwork(options.policy, effectiveSnapshot);
    if (!shouldFetch) {
      setHasHydratedPageState(true);
      return;
    }

    if (!isOnline) {
      if (localCacheResult.stations.length > 0) {
        setRefreshMessage("You're offline. Showing cached stations.");
        setIsRefreshing(false);
        setHasHydratedPageState(true);
        return;
      }
      setState("error");
      setError("You're offline. Connect to the internet to load stations.");
      setIsRefreshing(false);
      setHasHydratedPageState(true);
      return;
    }

    if (localCacheResult.stations.length > 0) {
      setIsRefreshing(true);
    } else {
      setState("loading");
      setError(null);
      setRefreshMessage(null);
    }

    const requestKey = `${cacheKey}:first`;
    let requestPromise = inFlightRef.current[requestKey];
    if (!requestPromise) {
      requestPromise = fetchStationsForSearch(activeSearch);
      inFlightRef.current[requestKey] = requestPromise;
    }

    try {
      const nextStations = await requestPromise;
      if (requestToken !== requestTokenRef.current) {
        return;
      }

      const normalizedNextStations = normalizeStationRecords(nextStations);
      setStations(normalizedNextStations);
      setState(normalizedNextStations.length ? "ready" : "empty");
      setError(null);
      setRefreshMessage(null);
      setCacheStatus({ source: "network", fetchedAt: Date.now() });

      writeBrowserCache(cacheKey, normalizedNextStations, STATIONS_SEARCH_CACHE_TTL_MS, {
        source: "network",
        fetchedAt: Date.now(),
      });
    } catch (loadError) {
      if (requestToken !== requestTokenRef.current) {
        return;
      }
      const hasVisibleCachedResults = localCacheResult.stations.length > 0;
      if (hasVisibleCachedResults) {
        setRefreshMessage("Showing cached results. Could not refresh.");
        setState("ready");
        setError(null);
      } else {
        setState("error");
        setError(getErrorMessage(loadError, "Could not load stations. Please try again."));
      }
    } finally {
      if (inFlightRef.current[requestKey] === requestPromise) {
        delete inFlightRef.current[requestKey];
      }
      if (requestToken === requestTokenRef.current) {
        setIsRefreshing(false);
        setHasHydratedPageState(true);
      }
    }
  }

  useEffect(() => {
    if (!submittedSearch) {
      return;
    }
    void loadSearchData({ policy: "stale-while-revalidate" }, submittedSearch);
  }, [submittedSearch, submissionVersion]);

  function handleDraftChange<K extends keyof StationSearchFormValues>(field: K, nextValue: StationSearchFormValues[K]) {
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
        setStations([]);
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
    setStations([]);
    setError(null);
    setCacheStatus(null);
    setIsRefreshing(false);
  }

  async function handleRefreshSearch() {
    if (!submittedSearch) {
      return;
    }
    setIsChipRefreshOverlayVisible(true);
    try {
      await loadSearchData({ policy: "network-only" }, submittedSearch);
    } finally {
      setIsChipRefreshOverlayVisible(false);
    }
  }

  function openCreateStationModal() {
    if (!canEditTradsphere) {
      return;
    }
    setStationModalMode("create");
    setStationModalCode(null);
    setIsStationModalOpen(true);
  }

  function openEditStationModal(station: StationRecord) {
    setStationModalMode("edit");
    setStationModalCode(station.code);
    setIsStationModalOpen(true);
  }

  async function handleStationSaved(result: StationModalSaveResult): Promise<void> {
    if (!submittedSearch) {
      return;
    }
    const nextStations = applySavedStationToResults(stations, submittedSearch, result);
    const fetchedAt = Date.now();
    setStations(nextStations);
    setState(nextStations.length ? "ready" : "empty");
    setError(null);
    setRefreshMessage(null);
    setCacheStatus({
      source: "cache",
      fetchedAt,
    });
    writeBrowserCache(submittedSearch.cacheKey, nextStations, STATIONS_SEARCH_CACHE_TTL_MS, {
      source: "cache",
      fetchedAt,
    });
  }

  const stationCatalog = useMemo(
    () => stations.map((station) => stationToCatalogItem(station)),
    [stations],
  );

  return (
    <AppPageLayout
      className="pb-5"
      pageMessages={<PageMessageStack messages={pageMessages} />}
      banner={(
        <PageBanner
          eyebrow="TradSphere"
          title="Stations"
          description="Search stations, manage delivery methods, and review contacts."
          action={<Button onClick={openCreateStationModal} disabled={!canEditTradsphere}>Add Station</Button>}
        />
      )}
      footer={showCacheChip ? (
        <PageCacheFooter
          text={cacheStatusText}
          onRefresh={handleRefreshSearch}
          disabled={isRefreshing || isChipRefreshOverlayVisible || !isOnline}
          refreshing={isRefreshing || isChipRefreshOverlayVisible}
          refreshLabel="Refresh stations"
          tooltipText={isOnline ? "Click to refresh last submitted search" : "Offline. Reconnect to refresh stations."}
          containerClassName="w-full"
        />
      ) : null}
    >

      <StationSearchForm
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

      <div className="relative">
        <StationResults
          state={state}
          groups={groups}
          groupOpenState={groupOpenState}
          error={error}
          onToggleGroup={(groupKey) => {
            setGroupOpenState((current) => ({
              ...current,
              [groupKey]: !(current[groupKey] ?? true),
            }));
          }}
          onEdit={openEditStationModal}
        />

        <SectionLoadingLayer
          active={loadingContract.sectionOverlayActive}
          message={loadingContract.sectionOverlayMessage}
        />
      </div>

      <StationModal
        open={isStationModalOpen}
        onOpenChange={(nextOpen) => {
          setIsStationModalOpen(nextOpen);
          if (!nextOpen) {
            setStationModalMode("create");
            setStationModalCode(null);
          }
        }}
        mode={stationModalMode}
        canEdit={canEditTradsphere}
        stationCode={stationModalCode}
        stationCatalog={stationCatalog}
        headers={requestHeaders}
        onSuccess={handleStationSaved}
      />

      <PageLoadingLayer
        active={loadingContract.pageOverlayActive}
        message={loadingContract.pageOverlayMessage}
      />
    </AppPageLayout>
  );
}
