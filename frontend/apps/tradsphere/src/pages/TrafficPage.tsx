import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { AlertCircle, AlertTriangle, Archive, Link2, Loader2, Plus, RefreshCw, Send, Table2, Trash2, Unlink, X } from "lucide-react";

import { ActionIconButton } from "@/components/dashboard/ActionIconButton";
import { AccountSelector } from "@/components/dashboard/AccountSelector";
import { ScheduleTimelineSection } from "@/components/dashboard/ScheduleTimelineSection";
import { PageBanner } from "@/components/layout/PageBanner";
import { AppDropdown } from "@/components/ui/app-dropdown";
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
import { Textarea } from "@/components/ui/textarea";
import { useToast } from "@/components/ui/toast";
import { UnsavedChangesDialog } from "@/components/ui/unsaved-changes-dialog";
import { useApiRequest } from "@/hooks/useApiRequest";
import { useDirtyRefreshGuard } from "@/hooks/useDirtyRefreshGuard";
import { useOnlineStatus } from "@/hooks/useOnlineStatus";
import { usePersistentState } from "@/hooks/usePersistentState";
import { readBrowserCacheSnapshot, writeBrowserCache } from "@/lib/browserCache";
import { useTradsphereAccountSelections } from "@/hooks/useTradsphereAccountSelections";
import { DateInputField } from "@/components/dashboard/FlightDateRangeField";
import { buildAuthHeaders as buildSharedAuthHeaders } from "@shared/api/authHeaders";
import { useAuth } from "@shared/auth/useAuth";
import { shouldProtectTradsphereFrontend } from "@shared/auth/guards";
import { hasAppEditAccess } from "@shared/auth/permissions";
import {
  readScopedPageState,
  type CachePolicy,
  shouldFetchNetwork,
  TRADSPHERE_CACHE_TTL_MS,
  writeScopedPageState,
} from "@shared/cache";
import { SectionCard } from "@shared/components/layout/SectionCard";
import { Tooltip } from "@shared/components/actions/Tooltip";
import { SectionLoadingOverlay } from "@shared/components/status/LoadingOverlay";

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type TrafficStatus = "draft" | "ready" | "sent" | "confirmed" | "archived";

type TrafficSummary = {
  id: string;
  accountCode: string;
  campaign: string;
  status: TrafficStatus;
  note: string | null;
  dateCreated: string | null;
  dateUpdated: string | null;
  flightCount: number;
  stationCount: number;
  emailSentStatus: string | null;
  emailSentAt: string | null;
  summary: RotationSummary;
};

type RotationSummary = {
  totalRotation: number;
  rotationWarning: boolean;
  rotationWarningMessage: string | null;
  warnings: Array<{
    code: string;
    message: string;
  }>;
};

type TrafficHeader = {
  id: string;
  accountCode: string;
  campaign: string;
  status: TrafficStatus;
  note: string | null;
  dateCreated: string | null;
  dateUpdated: string | null;
};

type TrafficFlight = {
  id: number;
  trafficId: string;
  flightStart: string;
  flightEnd: string;
  medium: string;
  length: number;
  isci: string | null;
  rotation: number;
  fileUrl: string;
  scriptUrl: string | null;
  note: string | null;
  dateCreated: string | null;
  dateUpdated: string | null;
};

type TrafficStation = {
  id: number;
  trafficId: string;
  stationCode: string;
  contactsSnapshot: unknown;
  deliveryMethod: string | null;
  deliveryStatus: string;
  confirmedStatus: string;
  note: string | null;
  dateCreated: string | null;
  dateUpdated: string | null;
};

type TrafficEmail = {
  id: number;
  trafficId: string;
  toEmails: string[];
  ccEmails: string[];
  bccEmails: string[];
  subject: string;
  body: string;
  sentStatus: string;
  sentAt: string | null;
  sentByUserId: string | null;
  smtpMessageId: string | null;
  lastSendAttemptAt: string | null;
  lastSendError: string | null;
  dateCreated: string | null;
  dateUpdated: string | null;
};

type TrafficDetail = {
  traffic: TrafficHeader;
  flights: TrafficFlight[];
  stations: TrafficStation[];
  email: TrafficEmail | null;
  summary: RotationSummary & {
    flightCount: number;
    stationCount: number;
  };
};

type PendingAction =
  | { type: "account"; accountCode: string }
  | { type: "traffic"; trafficId: string }
  | { type: "refresh" }
  | { type: "route"; proceed: () => void }
  | null;

type RowModalMode = "create" | "edit";

type StationLookupMeta = {
  code: string;
  name: string;
  mediaType: string;
  deliveryMethod: {
    name: string;
    url: string;
    username: string;
    deadline: string;
    note: string;
  } | null;
};

type LoadDetailOptions = {
  policy: CachePolicy;
  deferWhenDirty?: boolean;
};

type TrafficPageSnapshot = {
  loadedAccountCode: string;
  selectedTrafficId: string | null;
  trafficList: TrafficSummary[];
  detailBaseline: TrafficDetail | null;
  detailDraft: TrafficDetail | null;
  cacheStatus: CacheStatus | null;
  refreshMessage: string | null;
};

type TrafficStationCandidate = {
  stationCode: string;
  stationName: string | null;
  deliveryMethod: string | null;
  contactsSnapshot: Record<string, unknown> | null;
};

type TrafficStationCandidatesPayload = {
  accountCode: string;
  flightStart: string;
  flightEnd: string;
  stations: TrafficStationCandidate[];
  summary: {
    candidateCount: number;
  };
};

const TRAFFIC_LIST_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.MAIN_LOAD;
const TRAFFIC_DETAIL_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.MAIN_LOAD;
const STATION_LOOKUP_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.STATION_DETAIL;
const SELECTED_ACCOUNT_STORAGE_KEY = "tradsphere.traffic.selectedAccount.v1";
const SELECTED_BY_ACCOUNT_STORAGE_KEY = "tradsphere.traffic.selectedByAccount.v1";
const TRAFFIC_PAGE_STATE_CODE = "traffic";
const DEFERRED_REFRESH_MESSAGE = "Latest traffic data loaded in the background. Save or revert local changes to apply it.";
const STATUS_OPTIONS: Array<{ value: TrafficStatus; label: string }> = [
  { value: "draft", label: "Draft" },
  { value: "ready", label: "Ready" },
  { value: "sent", label: "Sent" },
  { value: "confirmed", label: "Confirmed" },
];
const FLIGHT_MEDIA_OPTIONS = ["TV", "RA", "CA", "OD", "NP", "CINE"];
const DELIVERY_STATUS_OPTIONS = ["not_started", "needs_manual_upload", "ready_to_email", "sent", "skipped", "issue"];
const CONFIRMED_STATUS_OPTIONS = ["pending", "confirmed", "issue", "not_required"];
const SIDEBAR_COLLAPSED_STORAGE_KEY = "workspace.sidebar.collapsed";
const LEGACY_SIDEBAR_COLLAPSED_STORAGE_KEY = "tradsphere:ui:sidebarCollapsed:v1";
const SIDEBAR_COLLAPSED_EVENT = "workspace-sidebar-collapsed-change";
const LOCAL_TRAFFIC_ID_PREFIX = "local-traffic:";

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

function asNullableString(value: unknown): string | null {
  const normalized = asString(value);
  return normalized || null;
}

function asNumber(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return fallback;
}

function buildStationLookupCacheKey(stationCode: string): string {
  return `traffic:station-lookup:${asString(stationCode).toUpperCase()}`;
}

function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const seen = new Set<string>();
  const output: string[] = [];
  for (const item of value) {
    const normalized = asString(item).toLowerCase();
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    output.push(normalized);
  }
  return output;
}

function splitEmailList(raw: string): string[] {
  const seen = new Set<string>();
  const output: string[] = [];
  for (const part of raw.split(/[;,\n]+/g)) {
    const email = part.trim().toLowerCase();
    if (!email || seen.has(email)) {
      continue;
    }
    seen.add(email);
    output.push(email);
  }
  return output;
}

function mergeUniqueEmails(base: string[], additions: string[]): string[] {
  const seen = new Set<string>();
  const output: string[] = [];
  for (const item of [...base, ...additions]) {
    const normalized = asString(item).toLowerCase();
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    output.push(normalized);
  }
  return output;
}

function resolveEarliestFlightStartIso(flights: TrafficFlight[]): string {
  let earliest = "";
  for (const flight of flights) {
    const start = asString(flight.flightStart);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(start)) {
      continue;
    }
    if (!earliest || start < earliest) {
      earliest = start;
    }
  }
  return earliest;
}

function resolveFlightRangeFromFlights(flights: TrafficFlight[]): { flightStart: string; flightEnd: string } | null {
  let minStart = "";
  let maxEnd = "";
  for (const flight of flights) {
    const start = asString(flight.flightStart);
    const end = asString(flight.flightEnd);
    if (!start || !end) {
      continue;
    }
    if (!minStart || start < minStart) {
      minStart = start;
    }
    if (!maxEnd || end > maxEnd) {
      maxEnd = end;
    }
  }
  if (!minStart || !maxEnd) {
    return null;
  }
  return {
    flightStart: minStart,
    flightEnd: maxEnd,
  };
}

function formatMonthYearFromIsoDate(isoDate: string): string {
  const parsed = /^(\d{4})-(\d{2})-(\d{2})$/.exec(asString(isoDate));
  if (!parsed) {
    return "";
  }
  const year = Number(parsed[1]);
  const monthIndex = Number(parsed[2]) - 1;
  if (!Number.isFinite(year) || !Number.isFinite(monthIndex) || monthIndex < 0 || monthIndex > 11) {
    return "";
  }
  const monthName = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
  ][monthIndex];
  return `${monthName} ${year}`;
}

function buildDefaultTrafficEmailSubject(detail: TrafficDetail, accountName: string): string {
  const period = formatMonthYearFromIsoDate(resolveEarliestFlightStartIso(detail.flights));
  if (!period) {
    return "";
  }
  const campaign = asString(detail.traffic.campaign);
  if (!campaign) {
    return "";
  }
  const accountLabel = asString(accountName) || asString(detail.traffic.accountCode);
  if (!accountLabel) {
    return "";
  }
  return `[${period}] ${accountLabel} Traffic - ${campaign}`;
}

function buildTrafficEmailSubjectCandidates(
  detail: TrafficDetail,
  accountNameByCode: Record<string, string>,
): string[] {
  const candidates: string[] = [];
  const seen = new Set<string>();
  const accountCode = asString(detail.traffic.accountCode).toUpperCase();
  const accountName = accountNameByCode[accountCode] || "";
  for (const label of [accountName, accountCode]) {
    const candidate = buildDefaultTrafficEmailSubject(detail, label);
    if (!candidate || seen.has(candidate)) {
      continue;
    }
    seen.add(candidate);
    candidates.push(candidate);
  }
  return candidates;
}

function applyDefaultTrafficEmailSubject(
  detail: TrafficDetail | null,
  accountNameByCode: Record<string, string>,
): TrafficDetail | null {
  if (!detail?.email || asString(detail.email.subject)) {
    return detail;
  }
  const accountCode = asString(detail.traffic.accountCode).toUpperCase();
  const accountName = accountNameByCode[accountCode] || "";
  const defaultSubject = buildDefaultTrafficEmailSubject(detail, accountName);
  if (!defaultSubject) {
    return detail;
  }
  return {
    ...detail,
    email: {
      ...detail.email,
      subject: defaultSubject,
    },
  };
}

function syncAutoTrafficEmailSubject(
  previousDetail: TrafficDetail,
  nextDetail: TrafficDetail,
  accountNameByCode: Record<string, string>,
): TrafficDetail {
  if (!nextDetail.email) {
    return nextDetail;
  }

  const previousSubject = asString(previousDetail.email?.subject);
  const nextSubject = asString(nextDetail.email.subject);
  const previousAutoCandidates = buildTrafficEmailSubjectCandidates(previousDetail, accountNameByCode);
  const nextAutoCandidates = buildTrafficEmailSubjectCandidates(nextDetail, accountNameByCode);
  const nextAutoSubject = nextAutoCandidates[0] || "";

  if (!nextAutoSubject) {
    return nextDetail;
  }

  const isCurrentlyAutoManaged =
    !nextSubject
    || (previousSubject && previousAutoCandidates.includes(previousSubject) && nextSubject === previousSubject);

  if (!isCurrentlyAutoManaged) {
    return nextDetail;
  }

  if (nextSubject === nextAutoSubject) {
    return nextDetail;
  }

  return {
    ...nextDetail,
    email: {
      ...nextDetail.email,
      subject: nextAutoSubject,
    },
  };
}

function formatRelativeTime(timestamp: number): string {
  const deltaMs = Math.max(0, Date.now() - timestamp);
  if (deltaMs < 30_000) {
    return "just now";
  }
  const minutes = Math.floor(deltaMs / 60_000);
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

function normalizeRotationSummary(payload: unknown): RotationSummary {
  const raw = isRecord(payload) ? payload : {};
  const warningsRaw = Array.isArray(raw.warnings) ? raw.warnings : [];
  return {
    totalRotation: asNumber(raw.totalRotation, 0),
    rotationWarning: Boolean(raw.rotationWarning),
    rotationWarningMessage: asNullableString(raw.rotationWarningMessage),
    warnings: warningsRaw
      .filter(isRecord)
      .map((entry) => ({
        code: asString(entry.code),
        message: asString(entry.message),
      }))
      .filter((entry) => entry.code || entry.message),
  };
}

function normalizeTrafficSummaryList(payload: unknown): TrafficSummary[] {
  const data = unwrapData(payload);
  if (!Array.isArray(data)) {
    return [];
  }

  return data
    .filter(isRecord)
    .map((item) => {
      const id = asString(item.id);
      const statusRaw = asString(item.status).toLowerCase() as TrafficStatus;
      return {
        id,
        accountCode: asString(item.accountCode).toUpperCase(),
        campaign: asString(item.campaign),
        status: statusRaw || "draft",
        note: asNullableString(item.note),
        dateCreated: asNullableString(item.dateCreated),
        dateUpdated: asNullableString(item.dateUpdated),
        flightCount: Math.max(0, Math.trunc(asNumber(item.flightCount, 0))),
        stationCount: Math.max(0, Math.trunc(asNumber(item.stationCount, 0))),
        emailSentStatus: asNullableString(item.emailSentStatus),
        emailSentAt: asNullableString(item.emailSentAt),
        summary: normalizeRotationSummary(item.summary),
      };
    })
    .filter((row) => row.id.length > 0);
}

function normalizeTrafficDetail(payload: unknown): TrafficDetail | null {
  const data = unwrapData(payload);
  if (!isRecord(data)) {
    return null;
  }

  const trafficRaw = isRecord(data.traffic) ? data.traffic : null;
  if (!trafficRaw) {
    return null;
  }

  const flightsRaw = Array.isArray(data.flights) ? data.flights : [];
  const stationsRaw = Array.isArray(data.stations) ? data.stations : [];
  const emailRaw = isRecord(data.email) ? data.email : null;
  const summary = normalizeRotationSummary(data.summary);

  const detail: TrafficDetail = {
    traffic: {
      id: asString(trafficRaw.id),
      accountCode: asString(trafficRaw.accountCode).toUpperCase(),
      campaign: asString(trafficRaw.campaign),
      status: (asString(trafficRaw.status).toLowerCase() || "draft") as TrafficStatus,
      note: asNullableString(trafficRaw.note),
      dateCreated: asNullableString(trafficRaw.dateCreated),
      dateUpdated: asNullableString(trafficRaw.dateUpdated),
    },
    flights: flightsRaw
      .filter(isRecord)
      .map((item) => ({
        id: Math.trunc(asNumber(item.id, 0)),
        trafficId: asString(item.trafficId),
        flightStart: asString(item.flightStart),
        flightEnd: asString(item.flightEnd),
        medium: asString(item.medium).toUpperCase() || "TV",
        length: Math.max(0, Math.trunc(asNumber(item.length, 0))),
        isci: asNullableString(item.isci),
        rotation: asNumber(item.rotation, 100),
        fileUrl: asString(item.fileUrl),
        scriptUrl: asNullableString(item.scriptUrl),
        note: asNullableString(item.note),
        dateCreated: asNullableString(item.dateCreated),
        dateUpdated: asNullableString(item.dateUpdated),
      }))
      .sort((a, b) => a.id - b.id),
    stations: stationsRaw
      .filter(isRecord)
      .map((item) => ({
        id: Math.trunc(asNumber(item.id, 0)),
        trafficId: asString(item.trafficId),
        stationCode: asString(item.stationCode).toUpperCase(),
        contactsSnapshot: item.contactsSnapshot,
        deliveryMethod: asNullableString(item.deliveryMethod),
        deliveryStatus: asString(item.deliveryStatus).toLowerCase(),
        confirmedStatus: asString(item.confirmedStatus).toLowerCase(),
        note: asNullableString(item.note),
        dateCreated: asNullableString(item.dateCreated),
        dateUpdated: asNullableString(item.dateUpdated),
      }))
      .sort((a, b) => a.id - b.id),
    email: emailRaw
      ? {
          id: Math.trunc(asNumber(emailRaw.id, 0)),
          trafficId: asString(emailRaw.trafficId),
          toEmails: asStringArray(emailRaw.toEmails),
          ccEmails: asStringArray(emailRaw.ccEmails),
          bccEmails: asStringArray(emailRaw.bccEmails),
          subject: asString(emailRaw.subject),
          body: typeof emailRaw.body === "string" ? emailRaw.body : asString(emailRaw.body),
          sentStatus: asString(emailRaw.sentStatus).toLowerCase() || "draft",
          sentAt: asNullableString(emailRaw.sentAt),
          sentByUserId: asNullableString(emailRaw.sentByUserId),
          smtpMessageId: asNullableString(emailRaw.smtpMessageId),
          lastSendAttemptAt: asNullableString(emailRaw.lastSendAttemptAt),
          lastSendError: asNullableString(emailRaw.lastSendError),
          dateCreated: asNullableString(emailRaw.dateCreated),
          dateUpdated: asNullableString(emailRaw.dateUpdated),
        }
      : {
          id: 0,
          trafficId: asString(trafficRaw.id),
          toEmails: [],
          ccEmails: [],
          bccEmails: [],
          subject: "",
          body: "",
          sentStatus: "draft",
          sentAt: null,
          sentByUserId: null,
          smtpMessageId: null,
          lastSendAttemptAt: null,
          lastSendError: null,
          dateCreated: null,
          dateUpdated: null,
        },
    summary: {
      ...summary,
      flightCount: Math.max(0, Math.trunc(asNumber((isRecord(data.summary) ? data.summary.flightCount : undefined), 0))),
      stationCount: Math.max(0, Math.trunc(asNumber((isRecord(data.summary) ? data.summary.stationCount : undefined), 0))),
    },
  };

  return detail.traffic.id ? detail : null;
}

function normalizeTrafficStationCandidates(payload: unknown): TrafficStationCandidatesPayload | null {
  const data = unwrapData(payload);
  if (!isRecord(data)) {
    return null;
  }
  const accountCode = asString(data.accountCode).toUpperCase();
  const flightStart = asString(data.flightStart);
  const flightEnd = asString(data.flightEnd);
  if (!accountCode || !flightStart || !flightEnd) {
    return null;
  }
  const stationsRaw = Array.isArray(data.stations) ? data.stations : [];
  const stations = stationsRaw
    .filter(isRecord)
    .map((item) => ({
      stationCode: asString(item.stationCode).toUpperCase(),
      stationName: asNullableString(item.stationName),
      deliveryMethod: asNullableString(item.deliveryMethod),
      contactsSnapshot: isRecord(item.contactsSnapshot) ? item.contactsSnapshot : null,
    }))
    .filter((item) => Boolean(item.stationCode));
  return {
    accountCode,
    flightStart,
    flightEnd,
    stations,
    summary: {
      candidateCount: Math.max(0, Math.trunc(asNumber(isRecord(data.summary) ? data.summary.candidateCount : stations.length, stations.length))),
    },
  };
}

function normalizeStationLookupMeta(raw: unknown, fallbackCode?: string): StationLookupMeta | null {
  if (!isRecord(raw)) {
    return null;
  }
  const code = asString(raw.code || raw.stationCode || fallbackCode).toUpperCase();
  if (!code) {
    return null;
  }
  const deliveryMethodRaw = isRecord(raw.deliveryMethod) ? raw.deliveryMethod : null;
  const deliveryMethodName = deliveryMethodRaw
    ? asString(deliveryMethodRaw.name)
    : asString(raw.deliveryMethod);
  return {
    code,
    name: asString(raw.name),
    mediaType: asString(raw.mediaType).toUpperCase(),
    deliveryMethod: deliveryMethodRaw
      ? {
          name: asString(deliveryMethodRaw.name),
          url: asString(deliveryMethodRaw.url),
          username: asString(deliveryMethodRaw.username),
          deadline: asString(deliveryMethodRaw.deadline),
          note: asString(deliveryMethodRaw.note),
        }
      : (deliveryMethodName
          ? {
              name: deliveryMethodName,
              url: "",
              username: "",
              deadline: "",
              note: "",
            }
          : null),
  };
}

function cloneDetail(detail: TrafficDetail | null): TrafficDetail | null {
  if (!detail) {
    return null;
  }
  return JSON.parse(JSON.stringify(detail)) as TrafficDetail;
}

function buildFingerprint(detail: TrafficDetail | null): string {
  if (!detail) {
    return "";
  }
  const normalized = {
    traffic: {
      campaign: detail.traffic.campaign,
      status: detail.traffic.status,
      note: detail.traffic.note || "",
    },
    flights: [...detail.flights]
      .map((item) => ({
        id: item.id,
        flightStart: item.flightStart,
        flightEnd: item.flightEnd,
        medium: item.medium,
        length: item.length,
        isci: item.isci || "",
        rotation: item.rotation,
        fileUrl: item.fileUrl,
        scriptUrl: item.scriptUrl || "",
        note: item.note || "",
      }))
      .sort((a, b) => a.id - b.id),
    stations: [...detail.stations]
      .map((item) => ({
        id: item.id,
        stationCode: item.stationCode,
        contactsSnapshot: item.contactsSnapshot ?? null,
        deliveryMethod: item.deliveryMethod || "",
        deliveryStatus: item.deliveryStatus,
        confirmedStatus: item.confirmedStatus,
        note: item.note || "",
      }))
      .sort((a, b) => a.id - b.id),
    email: detail.email
      ? {
          toEmails: detail.email.toEmails,
          ccEmails: detail.email.ccEmails,
          bccEmails: detail.email.bccEmails,
          subject: detail.email.subject,
          body: detail.email.body,
          sentStatus: detail.email.sentStatus,
        }
      : null,
  };
  return JSON.stringify(normalized);
}

function buildTrafficListCacheKey(accountCode: string): string {
  const normalized = asString(accountCode).toUpperCase() || "UNKNOWN";
  return `traffic:list:${normalized}:v1`;
}

function buildTrafficDetailCacheKey(trafficId: string): string {
  const normalized = asString(trafficId) || "missing";
  return `traffic:detail:${normalized}:v1`;
}

function isLocalTrafficId(trafficId: string): boolean {
  return asString(trafficId).startsWith(LOCAL_TRAFFIC_ID_PREFIX);
}

function buildLocalTrafficId(): string {
  return `${LOCAL_TRAFFIC_ID_PREFIX}${Date.now()}`;
}

function getTrafficErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message.trim();
  }
  return fallback;
}

function buildListSummaryFromDetail(detail: TrafficDetail): TrafficSummary {
  return {
    id: detail.traffic.id,
    accountCode: detail.traffic.accountCode,
    campaign: detail.traffic.campaign,
    status: detail.traffic.status,
    note: detail.traffic.note,
    dateCreated: detail.traffic.dateCreated,
    dateUpdated: detail.traffic.dateUpdated,
    flightCount: detail.summary.flightCount,
    stationCount: detail.summary.stationCount,
    emailSentStatus: detail.email?.sentStatus || null,
    emailSentAt: detail.email?.sentAt || null,
    summary: {
      totalRotation: detail.summary.totalRotation,
      rotationWarning: detail.summary.rotationWarning,
      rotationWarningMessage: detail.summary.rotationWarningMessage,
      warnings: detail.summary.warnings,
    },
  };
}

function computeSummary(detail: TrafficDetail): TrafficDetail {
  const totalRotation = detail.flights.reduce((total, flight) => total + asNumber(flight.rotation, 0), 0);
  const roundedTotal = Math.round(totalRotation * 100) / 100;
  const hasFlights = detail.flights.length > 0;
  const warning = hasFlights && Math.abs(roundedTotal - 100) > 0.0001;
  const warningMessage = warning
    ? `Total rotation is ${roundedTotal.toFixed(2)}%; expected 100.00%`
    : null;
  return {
    ...detail,
    summary: {
      totalRotation: roundedTotal,
      rotationWarning: warning,
      rotationWarningMessage: warningMessage,
      warnings: warningMessage ? [{ code: "ROTATION_TOTAL_NOT_100", message: warningMessage }] : [],
      flightCount: detail.flights.length,
      stationCount: detail.stations.length,
    },
  };
}

function toTrafficStatusLabel(status: string): string {
  const normalized = asString(status).toLowerCase();
  if (!normalized) {
    return "Draft";
  }
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function statusChipClass(status: string): string {
  const normalized = asString(status).toLowerCase();
  if (normalized === "sent") {
    return "border-emerald-200 bg-emerald-50 text-emerald-700";
  }
  if (normalized === "ready") {
    return "border-sky-200 bg-sky-50 text-sky-700";
  }
  if (normalized === "confirmed") {
    return "border-violet-200 bg-violet-50 text-violet-700";
  }
  return "border-slate-200 bg-slate-100 text-slate-700";
}

function emailStatusChipClass(status: string): string {
  const normalized = asString(status).toLowerCase();
  if (normalized === "sent") {
    return "border-emerald-200 bg-emerald-50 text-emerald-700";
  }
  if (normalized === "ready") {
    return "border-sky-200 bg-sky-50 text-sky-700";
  }
  if (normalized === "failed") {
    return "border-rose-200 bg-rose-50 text-rose-700";
  }
  return "border-slate-200 bg-slate-100 text-slate-700";
}

function stationStatusChipClass(status: string): string {
  const normalized = asString(status).toLowerCase();
  if (normalized === "sent" || normalized === "confirmed") {
    return "border-emerald-200 bg-emerald-50 text-emerald-700";
  }
  if (normalized === "issue") {
    return "border-rose-200 bg-rose-50 text-rose-700";
  }
  if (normalized === "pending" || normalized === "not_started") {
    return "border-amber-200 bg-amber-50 text-amber-700";
  }
  if (normalized === "ready_to_email" || normalized === "needs_manual_upload") {
    return "border-sky-200 bg-sky-50 text-sky-700";
  }
  return "border-slate-200 bg-slate-100 text-slate-700";
}

function isGoogleDriveDeliveryMethod(value: string | null): boolean {
  return asString(value).toLowerCase().includes("google drive");
}

function hasAllFlightFileUrls(flights: TrafficFlight[]): boolean {
  return flights.length > 0 && flights.every((flight) => Boolean(asString(flight.fileUrl)));
}

function isAutoReadyToEmailEligibleDeliveryMethod(value: string | null): boolean {
  const normalized = asString(value);
  return !normalized || isGoogleDriveDeliveryMethod(normalized);
}

function resolveAutoReadyToEmailStatus(currentStatusRaw: string, deliveryMethod: string | null, hasCompleteFlightAssets: boolean): string {
  const currentStatus = asString(currentStatusRaw).toLowerCase();
  if (!isAutoReadyToEmailEligibleDeliveryMethod(deliveryMethod)) {
    return currentStatus;
  }
  if (!hasCompleteFlightAssets) {
    return currentStatus === "ready_to_email" ? "" : currentStatus;
  }
  if (currentStatus === "ready_to_email") {
    return currentStatus;
  }
  if (currentStatus === "sent" || currentStatus === "skipped" || currentStatus === "issue") {
    return currentStatus;
  }
  return "ready_to_email";
}

function applyAutoReadyToEmailFromFlights(detail: TrafficDetail): TrafficDetail {
  const hasCompleteFlightAssets = hasAllFlightFileUrls(detail.flights);
  if (detail.stations.length === 0) {
    return detail;
  }
  let changed = false;
  const nextStations = detail.stations.map((station) => {
    const nextStatus = resolveAutoReadyToEmailStatus(
      station.deliveryStatus,
      station.deliveryMethod,
      hasCompleteFlightAssets,
    );
    if (nextStatus === asString(station.deliveryStatus).toLowerCase()) {
      return station;
    }
    changed = true;
    return {
      ...station,
      deliveryStatus: nextStatus,
    };
  });
  if (!changed) {
    return detail;
  }
  return {
    ...detail,
    stations: nextStations,
  };
}

const DELIVERY_METHOD_CHIP_STYLES = [
  "border-blue-200 bg-blue-50 text-blue-700 hover:bg-blue-100 focus-visible:ring-blue-300",
  "border-emerald-200 bg-emerald-50 text-emerald-700 hover:bg-emerald-100 focus-visible:ring-emerald-300",
  "border-amber-200 bg-amber-50 text-amber-700 hover:bg-amber-100 focus-visible:ring-amber-300",
  "border-violet-200 bg-violet-50 text-violet-700 hover:bg-violet-100 focus-visible:ring-violet-300",
  "border-cyan-200 bg-cyan-50 text-cyan-700 hover:bg-cyan-100 focus-visible:ring-cyan-300",
  "border-rose-200 bg-rose-50 text-rose-700 hover:bg-rose-100 focus-visible:ring-rose-300",
];

function deliveryMethodChipClass(methodName: string | null): string {
  const normalized = asString(methodName).toLowerCase();
  if (!normalized) {
    return "border-slate-200 bg-slate-100 text-slate-600 hover:bg-slate-200 focus-visible:ring-slate-300";
  }
  let hash = 0;
  for (let index = 0; index < normalized.length; index += 1) {
    hash = (hash * 31 + normalized.charCodeAt(index)) >>> 0;
  }
  return DELIVERY_METHOD_CHIP_STYLES[hash % DELIVERY_METHOD_CHIP_STYLES.length];
}

function formatStatusOptionLabel(value: string): string {
  const normalized = asString(value).toLowerCase();
  if (!normalized) {
    return "-";
  }
  return normalized
    .split("_")
    .filter(Boolean)
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(" ");
}

function formatIsoDateForTable(value: string): string {
  const parsed = /^(\d{4})-(\d{2})-(\d{2})$/.exec(asString(value));
  if (!parsed) {
    return "-";
  }
  return `${parsed[2]}/${parsed[3]}/${parsed[1]}`;
}

function formatFlightRangeForTable(start: string, end: string): string {
  const startLabel = formatIsoDateForTable(start);
  const endLabel = formatIsoDateForTable(end);
  if (startLabel === "-" && endLabel === "-") {
    return "-";
  }
  return `${startLabel} - ${endLabel}`;
}

function formatRotationValue(value: number): string {
  const normalized = asNumber(value, 0);
  if (Number.isInteger(normalized)) {
    return `${normalized}%`;
  }
  return `${normalized.toFixed(2).replace(/\.?0+$/, "")}%`;
}

function formatLengthValue(value: number): string {
  const normalized = Math.max(0, Math.trunc(asNumber(value, 0)));
  return `:${normalized}`;
}

function getTodayIsoDate(): string {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function getEndOfMonthIsoDate(): string {
  const now = new Date();
  const year = now.getFullYear();
  const month = now.getMonth();
  const lastDay = new Date(year, month + 1, 0).getDate();
  return `${year}-${String(month + 1).padStart(2, "0")}-${String(lastDay).padStart(2, "0")}`;
}

function normalizeFlightForCompare(flight: TrafficFlight | null): Record<string, unknown> | null {
  if (!flight) {
    return null;
  }
  return {
    flightStart: asString(flight.flightStart),
    flightEnd: asString(flight.flightEnd),
    medium: asString(flight.medium).toUpperCase(),
    length: Math.max(0, Math.trunc(asNumber(flight.length, 0))),
    isci: asString(flight.isci || ""),
    rotation: asNumber(flight.rotation, 0),
    fileUrl: asString(flight.fileUrl),
    scriptUrl: asString(flight.scriptUrl || ""),
    note: typeof flight.note === "string" ? flight.note : asString(flight.note || ""),
  };
}

function normalizeStationForCompare(station: TrafficStation | null): Record<string, unknown> | null {
  if (!station) {
    return null;
  }
  return {
    stationCode: asString(station.stationCode).toUpperCase(),
    contactsSnapshot: station.contactsSnapshot ?? null,
    deliveryMethod: asString(station.deliveryMethod || ""),
    deliveryStatus: asString(station.deliveryStatus).toLowerCase(),
    confirmedStatus: asString(station.confirmedStatus).toLowerCase(),
    note: typeof station.note === "string" ? station.note : asString(station.note || ""),
  };
}

function extractContactEmailsFromSnapshot(snapshot: unknown, contactType: "REP" | "TRAFFIC"): string[] {
  if (!isRecord(snapshot)) {
    return [];
  }
  const bucket = snapshot[contactType];
  if (!Array.isArray(bucket)) {
    return [];
  }
  const dedupe = new Set<string>();
  const emails: string[] = [];
  for (const item of bucket) {
    let email = "";
    if (typeof item === "string") {
      email = asString(item).toLowerCase();
    } else if (isRecord(item)) {
      email = asString(item.email).toLowerCase();
    }
    if (!email || dedupe.has(email)) {
      continue;
    }
    dedupe.add(email);
    emails.push(email);
  }
  return emails;
}

function extractAllContactEmails(snapshot: unknown): string[] {
  if (!isRecord(snapshot)) {
    return [];
  }
  const dedupe = new Set<string>();
  const emails: string[] = [];
  for (const value of Object.values(snapshot)) {
    if (!Array.isArray(value)) {
      continue;
    }
    for (const item of value) {
      let email = "";
      if (typeof item === "string") {
        email = asString(item).toLowerCase();
      } else if (isRecord(item)) {
        email = asString(item.email).toLowerCase();
      }
      if (!email || dedupe.has(email)) {
        continue;
      }
      dedupe.add(email);
      emails.push(email);
    }
  }
  return emails;
}

function extractPreferredContactEmails(snapshot: unknown): string[] {
  const repEmails = extractContactEmailsFromSnapshot(snapshot, "REP");
  const trafficEmails = extractContactEmailsFromSnapshot(snapshot, "TRAFFIC");
  const allEmails = extractAllContactEmails(snapshot);
  const dedupe = new Set<string>();
  const ordered: string[] = [];
  for (const email of [...repEmails, ...trafficEmails, ...allEmails]) {
    if (!email || dedupe.has(email)) {
      continue;
    }
    dedupe.add(email);
    ordered.push(email);
  }
  return ordered;
}

function formatStationDisplayLabel(code: string, meta: StationLookupMeta | null | undefined): string {
  const normalizedCode = asString(code).toUpperCase();
  if (!meta || !asString(meta.name)) {
    return normalizedCode || "-";
  }
  if (asString(meta.mediaType).toUpperCase() === "CA") {
    return `${meta.name} (${normalizedCode})`;
  }
  return meta.name;
}

function estimateContactTagWidth(email: string): number {
  return Math.max(74, email.length * 7 + 24);
}

function StationContactsCell({
  emails,
  onCopy,
}: {
  emails: string[];
  onCopy: () => void;
}) {
  const anchorRef = useRef<HTMLButtonElement | null>(null);
  const [tooltipOpen, setTooltipOpen] = useState(false);
  const [visibleCount, setVisibleCount] = useState(emails.length);

  useLayoutEffect(() => {
    const element = anchorRef.current;
    if (!element || !emails.length) {
      setVisibleCount(emails.length);
      return;
    }

    const recalc = () => {
      const availableWidth = element.clientWidth;
      if (!availableWidth || availableWidth <= 0) {
        setVisibleCount(1);
        return;
      }
      const gapWidth = 4;
      const overflowTagWidth = 32;
      let consumed = 0;
      let nextVisibleCount = 0;
      for (let index = 0; index < emails.length; index += 1) {
        const emailWidth = estimateContactTagWidth(emails[index]);
        const needsOverflowTag = index < emails.length - 1;
        const reserveForOverflow = needsOverflowTag ? overflowTagWidth + gapWidth : 0;
        const nextConsumed = consumed + emailWidth + (index > 0 ? gapWidth : 0);
        if (nextConsumed + reserveForOverflow > availableWidth) {
          break;
        }
        consumed = nextConsumed;
        nextVisibleCount += 1;
      }
      setVisibleCount(Math.max(1, nextVisibleCount));
    };

    recalc();
    if (typeof ResizeObserver === "undefined") {
      return;
    }
    const observer = new ResizeObserver(() => {
      recalc();
    });
    observer.observe(element);
    return () => {
      observer.disconnect();
    };
  }, [emails]);

  const normalizedVisibleCount = Math.min(visibleCount, emails.length);
  const visibleEmails = emails.slice(0, normalizedVisibleCount);
  const hasOverflow = emails.length > normalizedVisibleCount;
  const tooltipText = emails.length ? "Copy all contact emails" : "No emails to copy";

  return (
    <>
      <button
        ref={anchorRef}
        type="button"
        disabled={!emails.length}
        onClick={(event) => {
          event.stopPropagation();
          if (!emails.length) {
            return;
          }
          onCopy();
        }}
        onMouseEnter={() => setTooltipOpen(true)}
        onMouseLeave={() => setTooltipOpen(false)}
        onFocus={() => setTooltipOpen(true)}
        onBlur={() => setTooltipOpen(false)}
        className="inline-flex w-full items-center justify-start gap-1 overflow-hidden rounded-md px-1 py-0.5 text-left transition hover:bg-slate-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-default disabled:hover:bg-transparent"
      >
        {visibleEmails.length ? (
          visibleEmails.map((email) => (
            <span
              key={email}
              className="inline-flex max-w-[13rem] shrink-0 truncate rounded-full border border-slate-200 bg-slate-100 px-2 py-0.5 text-[11px] font-medium text-slate-700"
            >
              {email}
            </span>
          ))
        ) : (
          <span className="inline-flex rounded-full border border-slate-200 bg-slate-100 px-2 py-0.5 text-[11px] font-medium text-slate-500">
            None
          </span>
        )}
        {hasOverflow ? (
          <span className="inline-flex shrink-0 rounded-full border border-blue-200 bg-blue-50 px-2 py-0.5 text-[11px] font-medium text-blue-700">
            +1
          </span>
        ) : null}
      </button>
      <Tooltip
        open={tooltipOpen}
        anchorRef={anchorRef}
        text={tooltipText}
      />
    </>
  );
}

function EmailChipsInput({
  value,
  onChange,
  placeholder,
  disabled,
  labelByEmail,
}: {
  value: string[];
  onChange: (nextEmails: string[]) => void;
  placeholder: string;
  disabled: boolean;
  labelByEmail: Record<string, string>;
}) {
  const [draftValue, setDraftValue] = useState("");

  const commitDraft = useCallback(() => {
    const parsed = splitEmailList(draftValue);
    if (parsed.length > 0) {
      onChange(mergeUniqueEmails(value, parsed));
    }
    setDraftValue("");
  }, [draftValue, onChange, value]);

  return (
    <div
      className={[
        "flex min-h-10 w-full flex-wrap items-center gap-1 rounded-md border border-input bg-background px-2 py-1 text-sm ring-offset-background",
        disabled ? "cursor-not-allowed opacity-70" : "focus-within:ring-2 focus-within:ring-ring focus-within:ring-offset-2",
      ].join(" ")}
    >
      {value.map((email) => {
        const normalizedEmail = asString(email).toLowerCase();
        const displayLabel = asString(labelByEmail[normalizedEmail]) || normalizedEmail;
        return (
          <span
            key={normalizedEmail}
            title={displayLabel === normalizedEmail ? normalizedEmail : `${displayLabel} <${normalizedEmail}>`}
            className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-100 px-2 py-0.5 text-xs font-medium text-slate-700"
          >
            <span className="max-w-[14rem] truncate">{displayLabel}</span>
            {!disabled ? (
              <button
                type="button"
                onClick={() => onChange(value.filter((entry) => asString(entry).toLowerCase() !== normalizedEmail))}
                className="inline-flex size-4 items-center justify-center rounded-full text-slate-500 transition hover:bg-slate-200 hover:text-slate-700 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                aria-label={`Remove ${normalizedEmail}`}
              >
                <X className="size-3" />
              </button>
            ) : null}
          </span>
        );
      })}
      <input
        value={draftValue}
        onChange={(event) => setDraftValue(event.target.value)}
        onBlur={commitDraft}
        onPaste={(event) => {
          const pasted = event.clipboardData.getData("text");
          if (!/[;,\n]/.test(pasted)) {
            return;
          }
          event.preventDefault();
          const parsed = splitEmailList(pasted);
          if (parsed.length > 0) {
            onChange(mergeUniqueEmails(value, parsed));
          }
        }}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === "Tab" || event.key === "," || event.key === ";") {
            event.preventDefault();
            commitDraft();
            return;
          }
          if (event.key === "Backspace" && !draftValue && value.length > 0) {
            event.preventDefault();
            onChange(value.slice(0, -1));
          }
        }}
        placeholder={value.length === 0 ? placeholder : ""}
        disabled={disabled}
        className="min-w-[9rem] flex-1 border-0 bg-transparent px-1 py-1 text-sm outline-none placeholder:text-muted-foreground disabled:cursor-not-allowed"
      />
    </div>
  );
}

function normalizeTrafficPageSnapshot(payload: unknown): TrafficPageSnapshot | null {
  if (!isRecord(payload)) {
    return null;
  }

  const cacheStatusRaw = isRecord(payload.cacheStatus) ? payload.cacheStatus : null;
  const cacheSource = asString(cacheStatusRaw?.source).toLowerCase();
  const cacheFetchedAt = asNumber(cacheStatusRaw?.fetchedAt, 0);
  const cacheStatus = (cacheSource === "cache" || cacheSource === "network") && cacheFetchedAt > 0
    ? {
        source: cacheSource as "cache" | "network",
        fetchedAt: cacheFetchedAt,
      }
    : null;

  return {
    loadedAccountCode: asString(payload.loadedAccountCode).toUpperCase(),
    selectedTrafficId: asString(payload.selectedTrafficId) || null,
    trafficList: normalizeTrafficSummaryList(payload.trafficList),
    detailBaseline: normalizeTrafficDetail(payload.detailBaseline),
    detailDraft: normalizeTrafficDetail(payload.detailDraft),
    cacheStatus,
    refreshMessage: asNullableString(payload.refreshMessage),
  };
}

function createEmptyFlightDraft(trafficId: string): TrafficFlight {
  return {
    id: 0,
    trafficId,
    flightStart: getTodayIsoDate(),
    flightEnd: getEndOfMonthIsoDate(),
    medium: "TV",
    length: 60,
    isci: null,
    rotation: 100,
    fileUrl: "",
    scriptUrl: null,
    note: null,
    dateCreated: null,
    dateUpdated: null,
  };
}

function createEmptyStationDraft(trafficId: string): TrafficStation {
  return {
    id: 0,
    trafficId,
    stationCode: "",
    contactsSnapshot: null,
    deliveryMethod: null,
    deliveryStatus: "",
    confirmedStatus: "",
    note: null,
    dateCreated: null,
    dateUpdated: null,
  };
}

function formatStationCodesPreview(codes: string[], limit = 5): string {
  const normalized = codes
    .map((code) => asString(code).toUpperCase())
    .filter(Boolean);
  if (!normalized.length) {
    return "";
  }
  const clipped = normalized.slice(0, limit).join(", ");
  if (normalized.length <= limit) {
    return clipped;
  }
  return `${clipped}, +${normalized.length - limit} more`;
}

function normalizeExternalUrl(value: string | null | undefined): string {
  const raw = asString(value);
  if (!raw) {
    return "";
  }
  const candidates = raw.includes("://") ? [raw] : [raw, `https://${raw}`];
  for (const candidate of candidates) {
    try {
      const parsed = new URL(candidate);
      if (parsed.protocol === "http:" || parsed.protocol === "https:") {
        return parsed.toString();
      }
    } catch {
      continue;
    }
  }
  return "";
}

function isStrictHttpUrlInput(value: string | null | undefined): boolean {
  const raw = asString(value);
  if (!raw) {
    return false;
  }
  if (!/^https?:\/\//i.test(raw)) {
    return false;
  }
  try {
    const parsed = new URL(raw);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      return false;
    }
    const hostname = parsed.hostname.toLowerCase();
    if (!hostname) {
      return false;
    }
    const isLocalhost = hostname === "localhost";
    const isIpAddress = /^\d{1,3}(\.\d{1,3}){3}$/.test(hostname);
    const isDomainLike = hostname.includes(".");
    return isLocalhost || isIpAddress || isDomainLike;
  } catch {
    return false;
  }
}

function validateFlightModalDraft(flight: TrafficFlight | null): string | null {
  if (!flight) {
    return "Flight row is unavailable.";
  }
  if (!asString(flight.flightStart) || !asString(flight.flightEnd)) {
    return "Flight start and end dates are required.";
  }
  const rotation = asNumber(flight.rotation, 0);
  if (rotation < 0 || rotation > 100) {
    return "Rotation must be between 0 and 100.";
  }
  const length = asNumber(flight.length, 0);
  if (!Number.isFinite(length) || length < 0) {
    return "Length must be zero or greater.";
  }
  const fileUrl = asString(flight.fileUrl);
  if (fileUrl && !isStrictHttpUrlInput(fileUrl)) {
    return "File URL must be a valid http(s) URL (for example, https://example.com/file).";
  }
  const scriptUrl = asString(flight.scriptUrl || "");
  if (scriptUrl && !isStrictHttpUrlInput(scriptUrl)) {
    return "Script URL must be a valid http(s) URL.";
  }
  return null;
}

function validateStationModalDraft(station: TrafficStation | null): string | null {
  if (!station) {
    return "Station row is unavailable.";
  }
  if (!asString(station.stationCode)) {
    return "Station code is required.";
  }
  return null;
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

export default function TrafficPage() {
  const { requestJson } = useApiRequest();
  const auth = useAuth();
  const toast = useToast();
  const { isOnline } = useOnlineStatus();
  const [hasHydratedPageState, setHasHydratedPageState] = useState(false);
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
  const pageStateScope = useMemo(() => {
    const userKey = auth.user?.id || auth.user?.email || "anonymous";
    const tenantSlug = auth.tenantSlug || "default";
    return {
      userKey,
      tenantSlug,
      appCode: "tradsphere",
      pageCode: TRAFFIC_PAGE_STATE_CODE,
    };
  }, [auth.tenantSlug, auth.user?.email, auth.user?.id]);

  const [selectedAccountCode, setSelectedAccountCode] = usePersistentState<string>(
    SELECTED_ACCOUNT_STORAGE_KEY,
    "",
    { storage: "session", validate: (value: unknown): value is string => typeof value === "string" },
  );
  const [selectedTrafficByAccount, setSelectedTrafficByAccount] = usePersistentState<Record<string, string>>(
    SELECTED_BY_ACCOUNT_STORAGE_KEY,
    {},
    {
      storage: "session",
      validate: (value: unknown): value is Record<string, string> => {
        if (!isRecord(value)) {
          return false;
        }
        return Object.values(value).every((entry) => typeof entry === "string");
      },
    },
  );

  const {
    accountSelections,
    isLoadingSelections,
    isRefreshingSelections,
    selectionsError,
  } = useTradsphereAccountSelections({
    requestJson,
    requestHeaders,
    loadErrorMessage: "Unable to load account selections.",
  });
  const accountNameByCode = useMemo(() => {
    const map: Record<string, string> = {};
    for (const option of accountSelections) {
      const code = asString(option.accountCode).toUpperCase();
      if (!code) {
        continue;
      }
      map[code] = asString(option.name);
    }
    return map;
  }, [accountSelections]);

  const [trafficList, setTrafficList] = useState<TrafficSummary[]>([]);
  const [selectedTrafficId, setSelectedTrafficId] = useState<string | null>(null);
  const [detailBaseline, setDetailBaseline] = useState<TrafficDetail | null>(null);
  const [detailDraft, setDetailDraft] = useState<TrafficDetail | null>(null);

  const [isLoadingAccountTraffic, setIsLoadingAccountTraffic] = useState(false);
  const [isRefreshingAccountTraffic, setIsRefreshingAccountTraffic] = useState(false);
  const [isLoadingDetail, setIsLoadingDetail] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [pendingAction, setPendingAction] = useState<PendingAction>(null);
  const [isUnsavedDialogOpen, setIsUnsavedDialogOpen] = useState(false);
  const [isArchiveDialogOpen, setIsArchiveDialogOpen] = useState(false);
  const [archiveTargetTrafficId, setArchiveTargetTrafficId] = useState<string | null>(null);
  const [isScheduleTimelineModalOpen, setIsScheduleTimelineModalOpen] = useState(false);
  const [isScheduleTimelineLoading, setIsScheduleTimelineLoading] = useState(false);
  const [flightModalMode, setFlightModalMode] = useState<RowModalMode>("create");
  const [isFlightModalOpen, setIsFlightModalOpen] = useState(false);
  const [flightModalBaseline, setFlightModalBaseline] = useState<TrafficFlight | null>(null);
  const [flightModalDraft, setFlightModalDraft] = useState<TrafficFlight | null>(null);
  const [flightModalError, setFlightModalError] = useState<string | null>(null);
  const [flightFileUrlError, setFlightFileUrlError] = useState<string | null>(null);
  const [flightScriptUrlError, setFlightScriptUrlError] = useState<string | null>(null);
  const [isFlightDiscardDialogOpen, setIsFlightDiscardDialogOpen] = useState(false);
  const [isLastFlightDeleteConfirmOpen, setIsLastFlightDeleteConfirmOpen] = useState(false);
  const [pendingFlightDeleteId, setPendingFlightDeleteId] = useState<number | null>(null);
  const [stationModalMode, setStationModalMode] = useState<RowModalMode>("create");
  const [isStationModalOpen, setIsStationModalOpen] = useState(false);
  const [stationModalBaseline, setStationModalBaseline] = useState<TrafficStation | null>(null);
  const [stationModalDraft, setStationModalDraft] = useState<TrafficStation | null>(null);
  const [stationModalError, setStationModalError] = useState<string | null>(null);
  const [stationLookupName, setStationLookupName] = useState<string | null>(null);
  const [stationLookupError, setStationLookupError] = useState<string | null>(null);
  const [isStationLookupLoading, setIsStationLookupLoading] = useState(false);
  const [isStationAutoSyncing, setIsStationAutoSyncing] = useState(false);
  const [stationLookupQueryCode, setStationLookupQueryCode] = useState("");
  const [stationLookupCacheStatus, setStationLookupCacheStatus] = useState<CacheStatus | null>(null);
  const [stationLookupRefreshToken, setStationLookupRefreshToken] = useState(0);
  const [isStationDiscardDialogOpen, setIsStationDiscardDialogOpen] = useState(false);
  const [stationLookupMetaByCode, setStationLookupMetaByCode] = useState<Record<string, StationLookupMeta>>({});
  const [isDeliveryMethodDialogOpen, setIsDeliveryMethodDialogOpen] = useState(false);
  const [selectedDeliveryStationCode, setSelectedDeliveryStationCode] = useState<string | null>(null);
  const [loadedAccountCode, setLoadedAccountCode] = useState("");
  const [sidebarVisuallyExpanded, setSidebarVisuallyExpanded] = useState<boolean>(() => !readSidebarCollapsedState());

  const listRequestIdRef = useRef(0);
  const detailRequestIdRef = useRef(0);
  const tempRowIdRef = useRef(-1);
  const stationLookupRequestIdRef = useRef(0);
  const stationMetaRequestIdRef = useRef(0);
  const stationLookupMetaByCodeRef = useRef<Record<string, StationLookupMeta>>({});

  const hasUnsavedChanges = useMemo(() => {
    return buildFingerprint(detailBaseline) !== buildFingerprint(detailDraft);
  }, [detailBaseline, detailDraft]);

  const {
    hasDeferredUpdate,
    applyOrDefer,
    clearDeferredUpdate,
  } = useDirtyRefreshGuard(hasUnsavedChanges);

  const activeAccountCode = asString(loadedAccountCode).toUpperCase();
  const activeDraft = detailDraft;
  const timelineAccountCode = asString(activeDraft?.traffic.accountCode).toUpperCase();
  const isSentLocked = asString(detailBaseline?.traffic.status).toLowerCase() === "sent";
  const stationSyncFlightRange = useMemo(() => {
    return resolveFlightRangeFromFlights(activeDraft?.flights ?? []);
  }, [activeDraft?.flights]);
  const timelineAnchorStart = stationSyncFlightRange?.flightStart ?? null;
  const timelineAnchorEnd = stationSyncFlightRange?.flightEnd ?? null;
  const shouldShowSaveActions = canEditTradsphere && (hasUnsavedChanges || isSaving);
  const hasFlightModalChanges = useMemo(() => {
    return JSON.stringify(normalizeFlightForCompare(flightModalBaseline)) !== JSON.stringify(normalizeFlightForCompare(flightModalDraft));
  }, [flightModalBaseline, flightModalDraft]);
  const hasStationModalChanges = useMemo(() => {
    return JSON.stringify(normalizeStationForCompare(stationModalBaseline)) !== JSON.stringify(normalizeStationForCompare(stationModalDraft));
  }, [stationModalBaseline, stationModalDraft]);
  const canSaveFlightModal = Boolean(
    canEditTradsphere
      && !isSaving
      && !isSentLocked
      && flightModalDraft
      && hasFlightModalChanges,
  );
  const flightModalValidationError = useMemo(() => validateFlightModalDraft(flightModalDraft), [flightModalDraft]);
  const canSubmitFlightModal = Boolean(canSaveFlightModal && !flightModalValidationError);
  const canSaveStationModal = Boolean(
    canEditTradsphere
      && !isSaving
      && !isSentLocked
      && stationModalDraft
      && hasStationModalChanges,
  );
  const stationModalValidationError = useMemo(() => validateStationModalDraft(stationModalDraft), [stationModalDraft]);
  const canSubmitStationModal = Boolean(canSaveStationModal && !stationModalValidationError);

  const hasEditableDetail = Boolean(activeDraft && activeDraft.traffic.id);
  const canSaveChanges = Boolean(
    canEditTradsphere
      && hasEditableDetail
      && hasUnsavedChanges
      && !isSaving
      && !isLoadingAccountTraffic
      && !isLoadingDetail
      && !isSentLocked,
  );

  const cacheStatusText = useMemo(() => {
    if (isRefreshingAccountTraffic || isLoadingDetail) {
      return "Refreshing...";
    }
    if (!isOnline && cacheStatus) {
      return `Offline. Showing cached data from ${formatRelativeTime(cacheStatus.fetchedAt)}.`;
    }
    if (cacheStatus) {
      return `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`;
    }
    return "No cached data yet";
  }, [cacheStatus, isLoadingDetail, isOnline, isRefreshingAccountTraffic]);

  const visibleRefreshMessage = useMemo(() => {
    if (refreshMessage === DEFERRED_REFRESH_MESSAGE && !hasUnsavedChanges) {
      return null;
    }
    return refreshMessage;
  }, [hasUnsavedChanges, refreshMessage]);

  const upsertSelectedByAccount = useCallback((accountCode: string, trafficId: string | null) => {
    const normalizedAccount = asString(accountCode).toUpperCase();
    if (!normalizedAccount) {
      return;
    }
    setSelectedTrafficByAccount((current) => {
      if (!trafficId) {
        if (!(normalizedAccount in current)) {
          return current;
        }
        const next = { ...current };
        delete next[normalizedAccount];
        return next;
      }
      if (current[normalizedAccount] === trafficId) {
        return current;
      }
      const next = { ...current };
      next[normalizedAccount] = trafficId;
      return next;
    });
  }, [setSelectedTrafficByAccount]);

  const applyDetailState = useCallback((detail: TrafficDetail | null) => {
    const cloned = applyDefaultTrafficEmailSubject(cloneDetail(detail), accountNameByCode);
    setDetailBaseline((current) => {
      if (buildFingerprint(current) === buildFingerprint(cloned)) {
        return current;
      }
      return cloned;
    });
    setDetailDraft((current) => {
      if (buildFingerprint(current) === buildFingerprint(cloned)) {
        return current;
      }
      return cloneDetail(cloned);
    });
    if (cloned?.traffic.id) {
      setSelectedTrafficId(cloned.traffic.id);
      upsertSelectedByAccount(cloned.traffic.accountCode, cloned.traffic.id);
    }
  }, [accountNameByCode, upsertSelectedByAccount]);

  const applyListState = useCallback((accountCode: string, list: TrafficSummary[], preferredId?: string | null) => {
    setTrafficList(list);
    const preferred = asString(preferredId) || asString(list[0]?.id);
    const exists = preferred && list.some((item) => item.id === preferred);
    const nextSelected = exists ? preferred : (list[0]?.id || null);
    setSelectedTrafficId(nextSelected);
    upsertSelectedByAccount(accountCode, nextSelected);
    if (!nextSelected) {
      setDetailBaseline(null);
      setDetailDraft(null);
    }
  }, [upsertSelectedByAccount]);

  const loadTrafficDetail = useCallback(async (trafficId: string, options: LoadDetailOptions) => {
    const normalizedTrafficId = asString(trafficId);
    if (!normalizedTrafficId) {
      setDetailBaseline(null);
      setDetailDraft(null);
      return;
    }

    detailRequestIdRef.current += 1;
    const requestId = detailRequestIdRef.current;
    const cacheKey = buildTrafficDetailCacheKey(normalizedTrafficId);
    const cacheSnapshot = readBrowserCacheSnapshot<TrafficDetail>(cacheKey);
    const cachedDetail = normalizeTrafficDetail(cacheSnapshot?.data);
    const shouldUseCache = options.policy !== "network-only" && Boolean(cachedDetail);
    const shouldFetchFromNetwork = shouldFetchNetwork(options.policy, cacheSnapshot);

    if (shouldUseCache && cachedDetail) {
      const cachedFetchedAt = cacheSnapshot?.fetchedAt ?? Date.now();
      const applied = applyOrDefer(() => {
        applyDetailState(cachedDetail);
        setCacheStatus({
          source: "cache",
          fetchedAt: cachedFetchedAt,
        });
      }, { deferWhenDirty: options.deferWhenDirty ?? true });
      if (applied === "deferred") {
        setRefreshMessage(DEFERRED_REFRESH_MESSAGE);
      }
      setIsLoadingDetail(false);
    } else {
      setIsLoadingDetail(true);
    }

    if (!shouldFetchFromNetwork) {
      setIsLoadingDetail(false);
      return;
    }

    if (!isOnline) {
      if (!shouldUseCache) {
        setError("You're offline. Traffic detail is unavailable until connection is restored.");
      } else {
        setRefreshMessage("You're offline. Showing cached traffic.");
      }
      setIsLoadingDetail(false);
      return;
    }

    try {
      const payload = await requestJson(`/api/tradsphere/v1/traffic?id=${encodeURIComponent(normalizedTrafficId)}`, {
        headers: requestHeaders,
        successToast: false,
        errorToast: false,
      });
      if (requestId !== detailRequestIdRef.current) {
        return;
      }

      const normalized = normalizeTrafficDetail(payload);
      if (!normalized) {
        setError("Traffic detail response was invalid.");
        setIsLoadingDetail(false);
        return;
      }

      const computed = computeSummary(normalized);
      const fetchedAt = Date.now();
      writeBrowserCache(cacheKey, computed, TRAFFIC_DETAIL_CACHE_TTL_MS, {
        source: "network",
        fetchedAt,
      });

      const applied = applyOrDefer(() => {
        applyDetailState(computed);
        setCacheStatus({ source: "network", fetchedAt });
        setError(null);
        setRefreshMessage(null);
      }, { deferWhenDirty: options.deferWhenDirty ?? true });

      if (applied === "deferred") {
        setRefreshMessage(DEFERRED_REFRESH_MESSAGE);
      }
    } catch (loadError) {
      if (shouldUseCache) {
        setRefreshMessage("Showing cached results. Could not refresh.");
      } else {
        setError(getTrafficErrorMessage(loadError, "Unable to load traffic detail."));
      }
    } finally {
      if (requestId === detailRequestIdRef.current) {
        setIsLoadingDetail(false);
      }
    }
  }, [applyDetailState, applyOrDefer, isOnline, requestHeaders, requestJson]);

  const loadAccountTraffic = useCallback(async (
    accountCodeRaw: string,
    policy: CachePolicy,
    options?: { selectedIdOverride?: string | null; deferWhenDirty?: boolean },
  ) => {
    const accountCode = asString(accountCodeRaw).toUpperCase();
    if (!accountCode) {
      setTrafficList([]);
      setSelectedTrafficId(null);
      setDetailBaseline(null);
      setDetailDraft(null);
      setIsLoadingAccountTraffic(false);
      setIsRefreshingAccountTraffic(false);
      return;
    }

    listRequestIdRef.current += 1;
    const requestId = listRequestIdRef.current;
    const listCacheKey = buildTrafficListCacheKey(accountCode);
    const cacheSnapshot = readBrowserCacheSnapshot<TrafficSummary[]>(listCacheKey);
    const cachedList = normalizeTrafficSummaryList(cacheSnapshot?.data);
    const shouldUseCache = policy !== "network-only" && cachedList.length > 0;
    const shouldFetchFromNetwork = shouldFetchNetwork(policy, cacheSnapshot);

    const preferredSelectedId = asString(options?.selectedIdOverride)
      || asString(selectedTrafficByAccount[accountCode])
      || "";
    const resolveExistingSelection = (list: TrafficSummary[], preferredId: string): string | null => {
      if (preferredId && list.some((item) => item.id === preferredId)) {
        return preferredId;
      }
      return list[0]?.id || null;
    };

    if (shouldUseCache) {
      applyListState(accountCode, cachedList, preferredSelectedId || null);
      setCacheStatus({ source: "cache", fetchedAt: cacheSnapshot?.fetchedAt ?? Date.now() });
      setError(null);
      setIsLoadingAccountTraffic(false);
      setIsRefreshingAccountTraffic(shouldFetchFromNetwork);

      const nextSelected = resolveExistingSelection(cachedList, preferredSelectedId);
      if (nextSelected) {
        await loadTrafficDetail(nextSelected, {
          policy: "stale-while-revalidate",
          deferWhenDirty: options?.deferWhenDirty,
        });
      }
    } else {
      setIsLoadingAccountTraffic(true);
      setIsRefreshingAccountTraffic(false);
      setError(null);
    }

    if (!shouldFetchFromNetwork) {
      setIsLoadingAccountTraffic(false);
      setIsRefreshingAccountTraffic(false);
      return;
    }

    if (!isOnline) {
      if (!shouldUseCache) {
        setError("You're offline. Traffic data is unavailable until connection is restored.");
      } else {
        setRefreshMessage("You're offline. Showing cached traffic.");
      }
      setIsLoadingAccountTraffic(false);
      setIsRefreshingAccountTraffic(false);
      return;
    }

    try {
      const payload = await requestJson(`/api/tradsphere/v1/traffic/account?code=${encodeURIComponent(accountCode)}`, {
        headers: requestHeaders,
        successToast: false,
        errorToast: false,
      });
      if (requestId !== listRequestIdRef.current) {
        return;
      }

      const normalizedList = normalizeTrafficSummaryList(payload);
      const fetchedAt = Date.now();
      writeBrowserCache(listCacheKey, normalizedList, TRAFFIC_LIST_CACHE_TTL_MS, {
        source: "network",
        fetchedAt,
      });

      applyListState(accountCode, normalizedList, preferredSelectedId || null);
      setCacheStatus({ source: "network", fetchedAt });
      setError(null);
      setRefreshMessage(null);

      const nextSelected = resolveExistingSelection(normalizedList, preferredSelectedId);

      if (nextSelected) {
        await loadTrafficDetail(nextSelected, {
          policy: "network-first",
          deferWhenDirty: options?.deferWhenDirty,
        });
      } else {
        setDetailBaseline(null);
        setDetailDraft(null);
      }
    } catch (loadError) {
      if (shouldUseCache) {
        setRefreshMessage("Showing cached results. Could not refresh.");
      } else {
        setError(getTrafficErrorMessage(loadError, "Unable to load traffic list."));
      }
    } finally {
      if (requestId === listRequestIdRef.current) {
        setIsLoadingAccountTraffic(false);
        setIsRefreshingAccountTraffic(false);
      }
    }
  }, [
    applyListState,
    isOnline,
    loadTrafficDetail,
    requestHeaders,
    requestJson,
    selectedTrafficByAccount,
  ]);

  useEffect(() => {
    const restored = readScopedPageState<TrafficPageSnapshot>(pageStateScope, (value): value is TrafficPageSnapshot => {
      return normalizeTrafficPageSnapshot(value) !== null;
    });
    const normalized = normalizeTrafficPageSnapshot(restored);
    if (normalized) {
      if (normalized.loadedAccountCode) {
        setSelectedAccountCode(normalized.loadedAccountCode);
      }
      setLoadedAccountCode(normalized.loadedAccountCode);
      setTrafficList(normalized.trafficList);
      setSelectedTrafficId(normalized.selectedTrafficId);
      setSelectedTrafficByAccount((current) => {
        const loadedAccount = asString(normalized.loadedAccountCode).toUpperCase();
        if (!loadedAccount || !normalized.selectedTrafficId) {
          return current;
        }
        if (current[loadedAccount] === normalized.selectedTrafficId) {
          return current;
        }
        return {
          ...current,
          [loadedAccount]: normalized.selectedTrafficId,
        };
      });
      setDetailBaseline(cloneDetail(normalized.detailBaseline));
      setDetailDraft(cloneDetail(normalized.detailDraft));
      setCacheStatus(normalized.cacheStatus);
      setRefreshMessage(normalized.refreshMessage);
    }
    setHasHydratedPageState(true);
  }, [pageStateScope, setSelectedAccountCode, setSelectedTrafficByAccount]);

  useEffect(() => {
    if (!hasHydratedPageState) {
      return;
    }
    const snapshot: TrafficPageSnapshot = {
      loadedAccountCode,
      selectedTrafficId,
      trafficList,
      detailBaseline,
      detailDraft,
      cacheStatus,
      refreshMessage,
    };
    writeScopedPageState(pageStateScope, snapshot);
  }, [
    cacheStatus,
    detailBaseline,
    detailDraft,
    hasHydratedPageState,
    loadedAccountCode,
    pageStateScope,
    refreshMessage,
    selectedTrafficId,
    trafficList,
  ]);

  useEffect(() => {
    if (!hasDeferredUpdate) {
      return;
    }
    setRefreshMessage(DEFERRED_REFRESH_MESSAGE);
  }, [hasDeferredUpdate]);

  useEffect(() => {
    if (!hasUnsavedChanges && refreshMessage === DEFERRED_REFRESH_MESSAGE) {
      setRefreshMessage(null);
      clearDeferredUpdate();
    }
  }, [clearDeferredUpdate, hasUnsavedChanges, refreshMessage]);

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

  useEffect(() => {
    const handleBeforeRouteChange = (event: Event) => {
      const customEvent = event as CustomEvent<{ to?: string; proceed?: () => void }>;
      const nextRoute = asString(customEvent.detail?.to);
      const proceed = customEvent.detail?.proceed;
      if (!nextRoute || typeof proceed !== "function") {
        return;
      }
      if (nextRoute === "/tradsphere/traffic") {
        return;
      }
      if (!hasUnsavedChanges) {
        return;
      }
      event.preventDefault();
      setPendingAction({ type: "route", proceed });
      setIsUnsavedDialogOpen(true);
    };

    window.addEventListener("workspace:before-route-change", handleBeforeRouteChange as EventListener);
    return () => {
      window.removeEventListener("workspace:before-route-change", handleBeforeRouteChange as EventListener);
    };
  }, [hasUnsavedChanges]);

  useEffect(() => {
    if (!hasUnsavedChanges) {
      return;
    }
    const handleBeforeUnload = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = "";
    };
    window.addEventListener("beforeunload", handleBeforeUnload);
    return () => {
      window.removeEventListener("beforeunload", handleBeforeUnload);
    };
  }, [hasUnsavedChanges]);

  const updateDraft = useCallback((updater: (current: TrafficDetail) => TrafficDetail) => {
    setDetailDraft((current) => {
      if (!current) {
        return current;
      }
      const previousDetail = cloneDetail(current) as TrafficDetail;
      const nextDetail = updater(cloneDetail(current) as TrafficDetail);
      const withAutoSyncedSubject = syncAutoTrafficEmailSubject(previousDetail, nextDetail, accountNameByCode);
      const withAutoReadyToEmail = applyAutoReadyToEmailFromFlights(withAutoSyncedSubject);
      const withDefaultSubject = applyDefaultTrafficEmailSubject(withAutoReadyToEmail, accountNameByCode);
      return computeSummary(withDefaultSubject as TrafficDetail);
    });
  }, [accountNameByCode]);

  const updateTrafficCardSummary = useCallback((trafficIdRaw: string, patch: Partial<Pick<TrafficSummary, "campaign" | "status">>) => {
    const trafficId = asString(trafficIdRaw);
    if (!trafficId) {
      return;
    }
    setTrafficList((current) => current.map((item) => item.id === trafficId
      ? {
          ...item,
          campaign: patch.campaign ?? item.campaign,
          status: patch.status ?? item.status,
        }
      : item));
  }, []);

  useEffect(() => {
    if (!activeDraft) {
      return;
    }
    const trafficId = asString(activeDraft.traffic.id);
    if (!trafficId) {
      return;
    }
    setTrafficList((current) => {
      let changed = false;
      const nextList = current.map((item) => {
        if (item.id !== trafficId) {
          return item;
        }
        if (item.campaign === activeDraft.traffic.campaign && item.status === activeDraft.traffic.status) {
          return item;
        }
        changed = true;
        return {
          ...item,
          campaign: activeDraft.traffic.campaign,
          status: activeDraft.traffic.status,
        };
      });
      return changed ? nextList : current;
    });
  }, [activeDraft]);

  const validateForSave = useCallback((detail: TrafficDetail): string | null => {
    if (!asString(detail.traffic.campaign)) {
      return "Campaign is required.";
    }

    for (const flight of detail.flights) {
      if (!asString(flight.flightStart) || !asString(flight.flightEnd)) {
        return "Flight start and end dates are required.";
      }
      if (asNumber(flight.rotation, 0) < 0 || asNumber(flight.rotation, 0) > 100) {
        return "Flight rotation must be between 0 and 100.";
      }
    }

    for (const station of detail.stations) {
      if (!asString(station.stationCode)) {
        return "Each station row requires a station code.";
      }
    }

    return null;
  }, []);

  const persistCurrentCaches = useCallback((detail: TrafficDetail, nextList: TrafficSummary[]) => {
    const accountCode = asString(detail.traffic.accountCode).toUpperCase();
    const fetchedAt = Date.now();
    writeBrowserCache(buildTrafficDetailCacheKey(detail.traffic.id), detail, TRAFFIC_DETAIL_CACHE_TTL_MS, {
      source: "network",
      fetchedAt,
    });
    writeBrowserCache(buildTrafficListCacheKey(accountCode), nextList, TRAFFIC_LIST_CACHE_TTL_MS, {
      source: "network",
      fetchedAt,
    });
    setCacheStatus({ source: "network", fetchedAt });
  }, []);

  const handleSaveAll = useCallback(async () => {
    if (!detailDraft || !canSaveChanges) {
      return;
    }

    const validationError = validateForSave(detailDraft);
    if (validationError) {
      setError(validationError);
      return;
    }

    setIsSaving(true);
    setError(null);

    try {
      const isLocalDraft = isLocalTrafficId(detailDraft.traffic.id);
      let trafficId = detailDraft.traffic.id;

      if (isLocalDraft) {
        const createdPayload = await requestJson("/api/tradsphere/v1/traffic", {
          method: "POST",
          headers: requestHeaders,
          body: {
            accountCode: detailDraft.traffic.accountCode,
            campaign: detailDraft.traffic.campaign,
            status: detailDraft.traffic.status,
            note: detailDraft.traffic.note,
          },
          successToast: false,
        });
        const createdHeader = isRecord(unwrapData(createdPayload)) ? unwrapData(createdPayload) : null;
        const createdTrafficId = asString(isRecord(createdHeader) ? createdHeader.id : "");
        if (!createdTrafficId) {
          throw new Error("Failed to create traffic record.");
        }
        trafficId = createdTrafficId;
      }

      if (
        !isLocalDraft
        && detailBaseline
        && (
          detailDraft.traffic.campaign !== detailBaseline.traffic.campaign
          || detailDraft.traffic.status !== detailBaseline.traffic.status
          || (detailDraft.traffic.note || "") !== (detailBaseline.traffic.note || "")
        )
      ) {
        const payload = {
          campaign: detailDraft.traffic.campaign,
          status: detailDraft.traffic.status,
          note: detailDraft.traffic.note,
        };
        await requestJson(`/api/tradsphere/v1/traffic?id=${encodeURIComponent(trafficId)}`, {
          method: "PUT",
          headers: requestHeaders,
          body: payload,
          successToast: false,
        });
      }

      const baselineFlightsById = new Map((detailBaseline?.flights ?? []).map((item) => [item.id, item]));
      const draftFlightsById = new Map(detailDraft.flights.map((item) => [item.id, item]));

      for (const baselineFlight of (detailBaseline?.flights ?? [])) {
        if (baselineFlight.id <= 0) {
          continue;
        }
        if (!draftFlightsById.has(baselineFlight.id)) {
          await requestJson(
            `/api/tradsphere/v1/traffic/flight?trafficId=${encodeURIComponent(trafficId)}&flightId=${encodeURIComponent(String(baselineFlight.id))}`,
            {
              method: "DELETE",
              headers: requestHeaders,
              successToast: false,
            },
          );
        }
      }

      for (const draftFlight of detailDraft.flights) {
        const payload = {
          flightStart: draftFlight.flightStart,
          flightEnd: draftFlight.flightEnd,
          medium: draftFlight.medium,
          length: draftFlight.length,
          isci: draftFlight.isci,
          rotation: draftFlight.rotation,
          fileUrl: draftFlight.fileUrl,
          scriptUrl: draftFlight.scriptUrl,
          note: draftFlight.note,
        };

        if (draftFlight.id <= 0) {
          const response = await requestJson(`/api/tradsphere/v1/traffic/flight?trafficId=${encodeURIComponent(trafficId)}`, {
            method: "POST",
            headers: requestHeaders,
            body: payload,
            successToast: false,
          });
          const created = normalizeTrafficDetail({
            data: {
              traffic: detailDraft.traffic,
              flights: [unwrapData(response)],
              stations: [],
              email: detailDraft.email,
              summary: detailDraft.summary,
            },
          })?.flights[0];
          if (created) {
            draftFlightsById.set(created.id, created);
          }
          continue;
        }

        const baselineFlight = baselineFlightsById.get(draftFlight.id);
        if (!baselineFlight) {
          continue;
        }

        const changed = JSON.stringify({ ...baselineFlight, id: 0, trafficId: "" }) !== JSON.stringify({ ...draftFlight, id: 0, trafficId: "" });
        if (!changed) {
          continue;
        }

        await requestJson(
          `/api/tradsphere/v1/traffic/flight?trafficId=${encodeURIComponent(trafficId)}&flightId=${encodeURIComponent(String(draftFlight.id))}`,
          {
            method: "PUT",
            headers: requestHeaders,
            body: payload,
            successToast: false,
          },
        );
      }

      const baselineStationsById = new Map((detailBaseline?.stations ?? []).map((item) => [item.id, item]));
      const draftStationsById = new Map(detailDraft.stations.map((item) => [item.id, item]));

      for (const baselineStation of (detailBaseline?.stations ?? [])) {
        if (baselineStation.id <= 0) {
          continue;
        }
        if (!draftStationsById.has(baselineStation.id)) {
          await requestJson(
            `/api/tradsphere/v1/traffic/station?trafficId=${encodeURIComponent(trafficId)}&stationId=${encodeURIComponent(String(baselineStation.id))}`,
            {
              method: "DELETE",
              headers: requestHeaders,
              successToast: false,
            },
          );
        }
      }

      for (const draftStation of detailDraft.stations) {
        const payload = {
          stationCode: draftStation.stationCode,
          contactsSnapshot: draftStation.contactsSnapshot,
          deliveryMethod: draftStation.deliveryMethod,
          deliveryStatus: draftStation.deliveryStatus,
          confirmedStatus: draftStation.confirmedStatus,
          note: draftStation.note,
        };

        if (draftStation.id <= 0) {
          await requestJson(`/api/tradsphere/v1/traffic/station?trafficId=${encodeURIComponent(trafficId)}`, {
            method: "POST",
            headers: requestHeaders,
            body: payload,
            successToast: false,
          });
          continue;
        }

        const baselineStation = baselineStationsById.get(draftStation.id);
        if (!baselineStation) {
          continue;
        }

        const changed = JSON.stringify({ ...baselineStation, id: 0, trafficId: "" }) !== JSON.stringify({ ...draftStation, id: 0, trafficId: "" });
        if (!changed) {
          continue;
        }

        await requestJson(
          `/api/tradsphere/v1/traffic/station?trafficId=${encodeURIComponent(trafficId)}&stationId=${encodeURIComponent(String(draftStation.id))}`,
          {
            method: "PUT",
            headers: requestHeaders,
            body: payload,
            successToast: false,
          },
        );
      }

      if (detailDraft.email) {
        await requestJson(`/api/tradsphere/v1/traffic/email?trafficId=${encodeURIComponent(trafficId)}`, {
          method: "PUT",
          headers: requestHeaders,
          body: {
            toEmails: detailDraft.email.toEmails,
            ccEmails: detailDraft.email.ccEmails,
            bccEmails: detailDraft.email.bccEmails,
            subject: detailDraft.email.subject,
            body: detailDraft.email.body,
            sentStatus: detailDraft.email.sentStatus,
          },
          successToast: false,
        });
      }

      await loadAccountTraffic(detailDraft.traffic.accountCode, "network-first", {
        selectedIdOverride: trafficId,
        deferWhenDirty: false,
      });

      const refreshedDetailSnapshot = readBrowserCacheSnapshot<TrafficDetail>(buildTrafficDetailCacheKey(trafficId));
      const refreshedDetail = normalizeTrafficDetail(refreshedDetailSnapshot?.data);
      const nextDetail = computeSummary(refreshedDetail || {
        ...detailDraft,
        traffic: {
          ...detailDraft.traffic,
          id: trafficId,
        },
      });
      let nextList: TrafficSummary[] = [];
      setTrafficList((current) => {
        const mapped = current.map((item) => item.id === nextDetail.traffic.id
          ? buildListSummaryFromDetail(nextDetail)
          : item);
        nextList = mapped;
        return mapped;
      });
      applyDetailState(nextDetail);
      if (nextList.length > 0) {
        persistCurrentCaches(nextDetail, nextList);
      }
      setRefreshMessage(null);
    } catch (saveError) {
      setError(getTrafficErrorMessage(saveError, "Failed to save traffic changes."));
    } finally {
      setIsSaving(false);
    }
  }, [
    applyDetailState,
    canSaveChanges,
    detailBaseline,
    detailDraft,
    loadAccountTraffic,
    persistCurrentCaches,
    requestHeaders,
    requestJson,
    trafficList,
    validateForSave,
  ]);

  const handleDiscard = useCallback(() => {
    setDetailDraft(cloneDetail(detailBaseline));
    setRefreshMessage(null);
    clearDeferredUpdate();
  }, [clearDeferredUpdate, detailBaseline]);

  const handleRefresh = useCallback(async () => {
    if (!activeAccountCode) {
      return;
    }
    await loadAccountTraffic(activeAccountCode, "network-first", {
      selectedIdOverride: selectedTrafficId,
      deferWhenDirty: false,
    });
  }, [activeAccountCode, loadAccountTraffic, selectedTrafficId]);

  const handleLoadFromSelector = useCallback(async () => {
    const targetAccountCode = asString(selectedAccountCode).toUpperCase();
    if (!targetAccountCode) {
      return;
    }
    setLoadedAccountCode(targetAccountCode);
    await loadAccountTraffic(targetAccountCode, "network-first", {
      selectedIdOverride: selectedTrafficByAccount[targetAccountCode] || null,
      deferWhenDirty: false,
    });
  }, [loadAccountTraffic, selectedAccountCode, selectedTrafficByAccount]);

  const handleAccountChange = useCallback((nextAccountCodeRaw: string) => {
    const nextAccountCode = asString(nextAccountCodeRaw).toUpperCase();
    if (nextAccountCode === asString(selectedAccountCode).toUpperCase()) {
      return;
    }
    setSelectedAccountCode(nextAccountCode);
    setRefreshMessage(null);
  }, [selectedAccountCode, setSelectedAccountCode]);

  const handleSelectTraffic = useCallback((trafficIdRaw: string) => {
    const trafficId = asString(trafficIdRaw);
    if (!trafficId || trafficId === selectedTrafficId) {
      return;
    }
    if (hasUnsavedChanges) {
      setPendingAction({ type: "traffic", trafficId });
      setIsUnsavedDialogOpen(true);
      return;
    }
    setSelectedTrafficId(trafficId);
    upsertSelectedByAccount(activeAccountCode, trafficId);
    if (isLocalTrafficId(trafficId)) {
      return;
    }
    void loadTrafficDetail(trafficId, { policy: "stale-while-revalidate", deferWhenDirty: false });
  }, [activeAccountCode, hasUnsavedChanges, loadTrafficDetail, selectedTrafficId, upsertSelectedByAccount]);

  const handleCreateTraffic = useCallback(async () => {
    if (!activeAccountCode || !canEditTradsphere || isSaving || isSentLocked) {
      return;
    }
    setError(null);
    const localTrafficId = buildLocalTrafficId();
    const localDraft = computeSummary({
      traffic: {
        id: localTrafficId,
        accountCode: activeAccountCode,
        campaign: "New Traffic",
        status: "draft",
        note: "",
        dateCreated: null,
        dateUpdated: null,
      },
      flights: [],
      stations: [],
      email: {
        id: 0,
        trafficId: localTrafficId,
        toEmails: [],
        ccEmails: [],
        bccEmails: [],
        subject: "",
        body: "",
        sentStatus: "draft",
        sentAt: null,
        sentByUserId: null,
        smtpMessageId: null,
        lastSendAttemptAt: null,
        lastSendError: null,
        dateCreated: null,
        dateUpdated: null,
      },
      summary: {
        totalRotation: 0,
        rotationWarning: false,
        rotationWarningMessage: null,
        warnings: [],
        flightCount: 0,
        stationCount: 0,
      },
    });
    const nextList = [buildListSummaryFromDetail(localDraft), ...trafficList];
    setTrafficList(nextList);
    setSelectedTrafficId(localDraft.traffic.id);
    upsertSelectedByAccount(activeAccountCode, localDraft.traffic.id);
    setDetailBaseline(null);
    setDetailDraft(localDraft);
    setRefreshMessage(null);
    toast.info("Draft traffic created", "Record will be created in the database when you click Save.");
  }, [
    activeAccountCode,
    canEditTradsphere,
    isSaving,
    isSentLocked,
    toast,
    trafficList,
    upsertSelectedByAccount,
  ]);

  const handleArchiveTraffic = useCallback(async () => {
    const targetTrafficId = asString(archiveTargetTrafficId) || asString(detailDraft?.traffic.id);
    if (!targetTrafficId) {
      return;
    }
    if (isLocalTrafficId(targetTrafficId)) {
      const nextList = trafficList.filter((item) => item.id !== targetTrafficId);
      const nextSelected = nextList[0]?.id || null;
      setTrafficList(nextList);
      setSelectedTrafficId(nextSelected);
      upsertSelectedByAccount(activeAccountCode, nextSelected);
      if (nextSelected && !isLocalTrafficId(nextSelected)) {
        void loadTrafficDetail(nextSelected, { policy: "stale-while-revalidate", deferWhenDirty: false });
      } else {
        setDetailBaseline(null);
        setDetailDraft(null);
      }
      setArchiveTargetTrafficId(null);
      setIsArchiveDialogOpen(false);
      return;
    }
    try {
      setError(null);
      await requestJson(`/api/tradsphere/v1/traffic/archive?id=${encodeURIComponent(targetTrafficId)}`, {
        method: "POST",
        headers: requestHeaders,
        body: {},
        successToast: {
          title: "Traffic archived",
          message: "The traffic record was archived.",
        },
      });
      const nextList = trafficList.filter((item) => item.id !== targetTrafficId);
      const nextSelected = nextList[0]?.id || null;
      setTrafficList(nextList);
      setSelectedTrafficId(nextSelected);
      upsertSelectedByAccount(activeAccountCode, nextSelected);
      if (nextSelected) {
        await loadTrafficDetail(nextSelected, { policy: "stale-while-revalidate", deferWhenDirty: false });
      } else {
        setDetailBaseline(null);
        setDetailDraft(null);
      }
      const fetchedAt = Date.now();
      writeBrowserCache(buildTrafficListCacheKey(activeAccountCode), nextList, TRAFFIC_LIST_CACHE_TTL_MS, {
        source: "network",
        fetchedAt,
      });
      setCacheStatus({ source: "network", fetchedAt });
    } catch (archiveError) {
      setError(getTrafficErrorMessage(archiveError, "Unable to archive traffic."));
    } finally {
      setArchiveTargetTrafficId(null);
      setIsArchiveDialogOpen(false);
    }
  }, [activeAccountCode, archiveTargetTrafficId, detailDraft, loadTrafficDetail, requestHeaders, requestJson, trafficList, upsertSelectedByAccount]);

  const handleRemoveTrafficLocally = useCallback(async (trafficIdRaw: string) => {
    const trafficId = asString(trafficIdRaw);
    if (!trafficId) {
      return;
    }
    const nextList = trafficList.filter((item) => item.id !== trafficId);
    const nextSelected = nextList[0]?.id || null;
    setTrafficList(nextList);
    setSelectedTrafficId(nextSelected);
    upsertSelectedByAccount(activeAccountCode, nextSelected);
    if (nextSelected && !isLocalTrafficId(nextSelected)) {
      await loadTrafficDetail(nextSelected, { policy: "stale-while-revalidate", deferWhenDirty: false });
    } else {
      setDetailBaseline(null);
      setDetailDraft(null);
    }
    const fetchedAt = Date.now();
    writeBrowserCache(buildTrafficListCacheKey(activeAccountCode), nextList, TRAFFIC_LIST_CACHE_TTL_MS, {
      source: "network",
      fetchedAt,
    });
    setCacheStatus({ source: "network", fetchedAt });
  }, [activeAccountCode, loadTrafficDetail, trafficList, upsertSelectedByAccount]);

  const handleHardDeleteTraffic = useCallback(async (trafficIdRaw: string) => {
    const trafficId = asString(trafficIdRaw);
    if (!trafficId || !canEditTradsphere || isSaving || isLoadingAccountTraffic || isLoadingDetail) {
      return;
    }
    if (isLocalTrafficId(trafficId)) {
      await handleRemoveTrafficLocally(trafficId);
      toast.success("Draft removed", "Unsaved local traffic draft was removed.");
      return;
    }
    try {
      setError(null);
      await requestJson(`/api/tradsphere/v1/traffic?id=${encodeURIComponent(trafficId)}`, {
        method: "DELETE",
        headers: requestHeaders,
        successToast: {
          title: "Traffic deleted",
          message: "The traffic record was deleted.",
        },
      });
      await handleRemoveTrafficLocally(trafficId);
    } catch {
      await requestJson(`/api/tradsphere/v1/traffic/archive?id=${encodeURIComponent(trafficId)}`, {
        method: "POST",
        headers: requestHeaders,
        body: {},
        successToast: {
          title: "Traffic archived",
          message: "Hard delete is unavailable, so this record was archived.",
        },
      });
      await handleRemoveTrafficLocally(trafficId);
    }
  }, [canEditTradsphere, handleRemoveTrafficLocally, isLoadingAccountTraffic, isLoadingDetail, isSaving, requestHeaders, requestJson, toast]);

  const handleResolveUnsavedDialog = useCallback(async (discardChanges: boolean) => {
    const action = pendingAction;
    setPendingAction(null);
    setIsUnsavedDialogOpen(false);

    if (!discardChanges) {
      return;
    }

    setDetailDraft(cloneDetail(detailBaseline));
    clearDeferredUpdate();

    if (!action) {
      return;
    }
    if (action.type === "account") {
      setSelectedAccountCode(action.accountCode);
      setLoadedAccountCode(action.accountCode);
      await loadAccountTraffic(action.accountCode, "network-first", {
        selectedIdOverride: selectedTrafficByAccount[action.accountCode] || null,
        deferWhenDirty: false,
      });
      setRefreshMessage(null);
      return;
    }
    if (action.type === "traffic") {
      setSelectedTrafficId(action.trafficId);
      upsertSelectedByAccount(activeAccountCode, action.trafficId);
      if (isLocalTrafficId(action.trafficId)) {
        return;
      }
      await loadTrafficDetail(action.trafficId, {
        policy: "stale-while-revalidate",
        deferWhenDirty: false,
      });
      return;
    }
    if (action.type === "refresh") {
      await handleRefresh();
      return;
    }
    if (action.type === "route") {
      action.proceed();
    }
  }, [
    activeAccountCode,
    clearDeferredUpdate,
    detailBaseline,
    handleRefresh,
    loadAccountTraffic,
    loadTrafficDetail,
    pendingAction,
    selectedTrafficByAccount,
    setSelectedAccountCode,
    upsertSelectedByAccount,
  ]);

  const syncStationsFromFlightRange = useCallback(async (params: {
    flightStart: string;
    flightEnd: string;
    trafficId: string;
  }) => {
    const accountCode = asString(activeAccountCode).toUpperCase();
    const flightStart = asString(params.flightStart);
    const flightEnd = asString(params.flightEnd);
    if (!accountCode || !flightStart || !flightEnd) {
      return;
    }

    setIsStationAutoSyncing(true);
    try {
      const query = new URLSearchParams({
        accountCode,
        flightStart,
        flightEnd,
      });
      const payload = await requestJson(`/api/tradsphere/v1/traffic/station-candidates?${query.toString()}`, {
        headers: requestHeaders,
        successToast: false,
        errorToast: false,
      });
      const normalized = normalizeTrafficStationCandidates(payload);
      if (!normalized) {
        toast.info("No station candidates returned");
        return;
      }

      const currentCodes = new Set(
        (detailDraft?.stations ?? [])
          .map((station) => asString(station.stationCode).toUpperCase())
          .filter(Boolean),
      );
      const candidateCodes = normalized.stations
        .map((station) => asString(station.stationCode).toUpperCase())
        .filter(Boolean);
      const stationCandidateByCode = new Map(
        normalized.stations.map((station) => [
          asString(station.stationCode).toUpperCase(),
          station,
        ]),
      );
      const uniqueCandidateCodes = [...new Set(candidateCodes)];
      const autoAddedCodes = uniqueCandidateCodes.filter((code) => !currentCodes.has(code));
      const preservedCodes = uniqueCandidateCodes.filter((code) => currentCodes.has(code));
      const currentStationsByCode = new Map(
        (detailDraft?.stations ?? []).map((station) => [
          asString(station.stationCode).toUpperCase(),
          station,
        ]),
      );
      const updatedExistingCount = preservedCodes.filter((stationCode) => {
        const existing = currentStationsByCode.get(stationCode);
        const candidate = stationCandidateByCode.get(stationCode);
        if (!existing || !candidate) {
          return false;
        }
        return !(
          asString(existing.deliveryMethod ?? "") === asString(candidate.deliveryMethod ?? "")
          && JSON.stringify(existing.contactsSnapshot ?? null) === JSON.stringify(candidate.contactsSnapshot ?? null)
        );
      }).length;

      const stationDrafts: TrafficStation[] = autoAddedCodes.map((stationCode) => {
        const candidate = stationCandidateByCode.get(stationCode);
        const nextId = tempRowIdRef.current;
        tempRowIdRef.current -= 1;
        return {
          id: nextId,
          trafficId: params.trafficId,
          stationCode,
          contactsSnapshot: candidate?.contactsSnapshot ?? null,
          deliveryMethod: candidate?.deliveryMethod ?? null,
          deliveryStatus: "",
          confirmedStatus: "",
          note: null,
          dateCreated: null,
          dateUpdated: null,
        };
      });

      let readyToEmailUpdatedCount = 0;
      updateDraft((current) => {
        if (current.traffic.id !== params.trafficId) {
          return current;
        }
        const shouldAutoMarkReadyToEmail = hasAllFlightFileUrls(current.flights);
        const syncContactEmails = uniqueCandidateCodes.flatMap((stationCode) => {
          const candidate = stationCandidateByCode.get(stationCode);
          return extractPreferredContactEmails(candidate?.contactsSnapshot ?? null);
        });
        let changed = false;
        const nextStations = current.stations.map((station) => {
          const stationCode = asString(station.stationCode).toUpperCase();
          const candidate = stationCandidateByCode.get(stationCode);
          const nextDeliveryMethod = candidate ? (candidate.deliveryMethod ?? null) : station.deliveryMethod;
          const nextContactsSnapshot = candidate ? (candidate.contactsSnapshot ?? null) : station.contactsSnapshot;
          const currentDeliveryStatus = asString(station.deliveryStatus).toLowerCase();
          const nextDeliveryStatus = resolveAutoReadyToEmailStatus(
            currentDeliveryStatus,
            nextDeliveryMethod ?? null,
            shouldAutoMarkReadyToEmail,
          );
          if (nextDeliveryStatus === "ready_to_email" && currentDeliveryStatus !== "ready_to_email") {
            readyToEmailUpdatedCount += 1;
          }
          if (
            asString(station.deliveryMethod ?? "") === asString(nextDeliveryMethod ?? "")
            && JSON.stringify(station.contactsSnapshot ?? null) === JSON.stringify(nextContactsSnapshot)
            && currentDeliveryStatus === nextDeliveryStatus
          ) {
            return station;
          }
          changed = true;
          return {
            ...station,
            deliveryMethod: nextDeliveryMethod,
            contactsSnapshot: nextContactsSnapshot,
            deliveryStatus: nextDeliveryStatus,
          };
        });
        const existingCodes = new Set(
          nextStations
            .map((station) => asString(station.stationCode).toUpperCase())
            .filter(Boolean),
        );
        const rowsToAdd = stationDrafts
          .filter((station) => !existingCodes.has(station.stationCode))
          .map((station) => ({
            ...station,
            deliveryStatus: resolveAutoReadyToEmailStatus(
              station.deliveryStatus,
              station.deliveryMethod,
              shouldAutoMarkReadyToEmail,
            ),
          }));
        if (rowsToAdd.length > 0) {
          changed = true;
          nextStations.push(...rowsToAdd);
        }
        const nextEmail = current.email
          ? {
              ...current.email,
              toEmails: mergeUniqueEmails(current.email.toEmails, syncContactEmails),
            }
          : current.email;
        const emailChanged = Boolean(
          current.email
            && JSON.stringify(nextEmail?.toEmails ?? []) !== JSON.stringify(current.email.toEmails),
        );
        if (!changed && !emailChanged) {
          return current;
        }
        return {
          ...current,
          stations: nextStations,
          email: nextEmail,
        };
      });
      const readyToEmailSuffix = readyToEmailUpdatedCount > 0
        ? ` ${readyToEmailUpdatedCount} station(s) marked Ready to Email.`
        : "";

      if (autoAddedCodes.length > 0) {
        const addedLabel = formatStationCodesPreview(autoAddedCodes);
        if (updatedExistingCount > 0) {
          toast.info(
            "Stations synced from schedule",
            `${autoAddedCodes.length} station(s) added (${addedLabel}). ${updatedExistingCount} existing station(s) had contacts/delivery method refreshed.${readyToEmailSuffix}`,
          );
        } else if (preservedCodes.length > 0) {
          toast.info(
            "Stations auto-added from schedule",
            `${autoAddedCodes.length} station(s) added (${addedLabel}). ${preservedCodes.length} existing station(s) were preserved.${readyToEmailSuffix}`,
          );
        } else {
          toast.info(
            "Stations auto-added from schedule",
            `${autoAddedCodes.length} station(s) added (${addedLabel}).${readyToEmailSuffix}`,
          );
        }
        return;
      }

      if (uniqueCandidateCodes.length === 0) {
        toast.info(
          "No matching schedule stations found",
          "No stations were returned for the selected account and flight date range.",
        );
        return;
      }

      if (updatedExistingCount > 0) {
        toast.info(
          "Stations synced from schedule",
          `${updatedExistingCount} existing station(s) had contacts/delivery method refreshed.${readyToEmailSuffix}`,
        );
      } else {
        toast.info(
          "Existing stations preserved",
          `${preservedCodes.length} matching station(s) already exist and were left unchanged.${readyToEmailSuffix}`,
        );
      }
    } catch (syncError) {
      toast.error(
        "Unable to auto-sync stations",
        getTrafficErrorMessage(syncError, "The flight was saved, but schedule stations could not be loaded."),
      );
    } finally {
      setIsStationAutoSyncing(false);
    }
  }, [activeAccountCode, detailDraft?.stations, requestHeaders, requestJson, toast, updateDraft]);

  const handleManualStationSync = useCallback(() => {
    if (!activeDraft || isStationAutoSyncing) {
      return;
    }
    if (!stationSyncFlightRange) {
      toast.info(
        "No flight date range available",
        "Add at least one flight with a start and end date before syncing stations.",
      );
      return;
    }
    void syncStationsFromFlightRange({
      trafficId: activeDraft.traffic.id,
      flightStart: stationSyncFlightRange.flightStart,
      flightEnd: stationSyncFlightRange.flightEnd,
    });
  }, [activeDraft, isStationAutoSyncing, stationSyncFlightRange, syncStationsFromFlightRange, toast]);

  function resetFlightModalState() {
    setFlightModalError(null);
    setFlightFileUrlError(null);
    setFlightScriptUrlError(null);
    setFlightModalBaseline(null);
    setFlightModalDraft(null);
    setIsFlightDiscardDialogOpen(false);
    setFlightModalMode("create");
  }

  function resetStationModalState() {
    setStationModalError(null);
    setStationModalBaseline(null);
    setStationModalDraft(null);
    setStationLookupName(null);
    setStationLookupError(null);
    setIsStationLookupLoading(false);
    setStationLookupQueryCode("");
    setStationLookupCacheStatus(null);
    setStationLookupRefreshToken(0);
    setIsStationDiscardDialogOpen(false);
    setStationModalMode("create");
  }

  function addFlightDraft() {
    if (!detailDraft || isSentLocked) {
      return;
    }
    const draft = createEmptyFlightDraft(detailDraft.traffic.id);
    setFlightModalMode("create");
    setFlightModalBaseline(draft);
    setFlightModalDraft(draft);
    setFlightModalError(null);
    setIsFlightDiscardDialogOpen(false);
    setIsFlightModalOpen(true);
  }

  function editFlightDraft(flightId: number) {
    if (!detailDraft || isSentLocked) {
      return;
    }
    const target = detailDraft.flights.find((item) => item.id === flightId);
    if (!target) {
      return;
    }
    const cloned = { ...target };
    setFlightModalMode("edit");
    setFlightModalBaseline(cloned);
    setFlightModalDraft(cloned);
    setFlightModalError(null);
    setIsFlightDiscardDialogOpen(false);
    setIsFlightModalOpen(true);
  }

  function removeFlightDraft(flightId: number) {
    if (isSentLocked) {
      return;
    }
    const currentDetail = detailDraft;
    const remainingFlights = (currentDetail?.flights ?? []).filter((item) => item.id !== flightId);
    const nextSyncRange = resolveFlightRangeFromFlights(remainingFlights);
    const shouldConfirmClearStations = Boolean(
      currentDetail
      && !nextSyncRange
      && currentDetail.stations.length > 0,
    );
    if (shouldConfirmClearStations) {
      setPendingFlightDeleteId(flightId);
      setIsLastFlightDeleteConfirmOpen(true);
      return;
    }
    updateDraft((current) => ({
      ...current,
      flights: current.flights.filter((item) => item.id !== flightId),
    }));
    if (currentDetail && nextSyncRange) {
      void syncStationsFromFlightRange({
        trafficId: currentDetail.traffic.id,
        flightStart: nextSyncRange.flightStart,
        flightEnd: nextSyncRange.flightEnd,
      });
      return;
    }
    if (currentDetail && !nextSyncRange) {
      toast.info(
        "Station sync skipped",
        "No flight date range remains after deleting this flight. Existing stations were preserved.",
      );
    }
  }

  function handleResolveLastFlightDelete(shouldClearStations: boolean) {
    const flightId = pendingFlightDeleteId;
    setPendingFlightDeleteId(null);
    setIsLastFlightDeleteConfirmOpen(false);
    if (flightId === null || isSentLocked) {
      return;
    }
    const currentDetail = detailDraft;
    updateDraft((current) => {
      const remainingFlights = current.flights.filter((item) => item.id !== flightId);
      const shouldClear = shouldClearStations && remainingFlights.length === 0;
      const shouldResetDeliveryStatus = !shouldClear && remainingFlights.length === 0;
      return {
        ...current,
        flights: remainingFlights,
        stations: shouldClear
          ? []
          : shouldResetDeliveryStatus
            ? current.stations.map((station) => ({ ...station, deliveryStatus: "" }))
            : current.stations,
      };
    });
    if (currentDetail) {
      const remainingFlights = currentDetail.flights.filter((item) => item.id !== flightId);
      const nextSyncRange = resolveFlightRangeFromFlights(remainingFlights);
      if (nextSyncRange) {
        void syncStationsFromFlightRange({
          trafficId: currentDetail.traffic.id,
          flightStart: nextSyncRange.flightStart,
          flightEnd: nextSyncRange.flightEnd,
        });
        return;
      }
      if (shouldClearStations) {
        toast.info(
          "Last flight removed",
          "No flight date range remains, so all station rows were cleared.",
        );
      } else {
        toast.info(
          "Stations preserved",
          "No flight date range remains after deleting this flight. Existing stations were preserved and delivery status was reset.",
        );
      }
    }
  }

  function saveFlightModal() {
    if (!flightModalDraft || !canSubmitFlightModal) {
      return;
    }
    const validationError = validateFlightModalDraft(flightModalDraft);
    if (validationError) {
      setFlightModalError(validationError);
      return;
    }
    const normalizedFileUrl = normalizeExternalUrl(flightModalDraft.fileUrl);
    const normalizedScriptUrl = normalizeExternalUrl(flightModalDraft.scriptUrl || "");
    const sanitizedFlightDraft: TrafficFlight = {
      ...flightModalDraft,
      fileUrl: normalizedFileUrl,
      scriptUrl: asNullableString(normalizedScriptUrl),
    };

    let nextFlightForSync: { trafficId: string; flightStart: string; flightEnd: string } | null = null;

    if (flightModalMode === "create") {
      const nextTempId = tempRowIdRef.current;
      tempRowIdRef.current -= 1;
      const nextFlight: TrafficFlight = {
        ...sanitizedFlightDraft,
        id: nextTempId,
      };
      nextFlightForSync = {
        trafficId: nextFlight.trafficId,
        flightStart: nextFlight.flightStart,
        flightEnd: nextFlight.flightEnd,
      };
      updateDraft((current) => ({
        ...current,
        flights: [...current.flights, nextFlight],
      }));
    } else if (flightModalBaseline) {
      const hasDateRangeChanged =
        asString(flightModalBaseline.flightStart) !== asString(sanitizedFlightDraft.flightStart)
        || asString(flightModalBaseline.flightEnd) !== asString(sanitizedFlightDraft.flightEnd);
      if (hasDateRangeChanged) {
        nextFlightForSync = {
          trafficId: sanitizedFlightDraft.trafficId,
          flightStart: sanitizedFlightDraft.flightStart,
          flightEnd: sanitizedFlightDraft.flightEnd,
        };
      }
      updateDraft((current) => ({
        ...current,
        flights: current.flights.map((item) => item.id === flightModalBaseline.id ? { ...sanitizedFlightDraft, id: item.id } : item),
      }));
    }

    setIsFlightModalOpen(false);
    resetFlightModalState();
    if (nextFlightForSync) {
      void syncStationsFromFlightRange(nextFlightForSync);
    }
  }

  function handleFlightModalOpenChange(nextOpen: boolean) {
    const allowClose = canModalClose({
      nextOpen,
      isBusy: isSaving,
      hasUnsavedChanges: hasFlightModalChanges,
    });
    if (!allowClose) {
      if (!nextOpen && hasFlightModalChanges) {
        setIsFlightDiscardDialogOpen(true);
      }
      return;
    }
    setIsFlightModalOpen(nextOpen);
    if (!nextOpen) {
      resetFlightModalState();
    }
  }

  function addStationDraft() {
    if (!detailDraft || isSentLocked) {
      return;
    }
    const draft = createEmptyStationDraft(detailDraft.traffic.id);
    setStationModalMode("create");
    setStationModalBaseline(draft);
    setStationModalDraft(draft);
    setStationLookupQueryCode("");
    setStationModalError(null);
    setIsStationDiscardDialogOpen(false);
    setIsStationModalOpen(true);
  }

  function editStationDraft(stationId: number) {
    if (!detailDraft || isSentLocked) {
      return;
    }
    const target = detailDraft.stations.find((item) => item.id === stationId);
    if (!target) {
      return;
    }
    const cloned = { ...target };
    setStationModalMode("edit");
    setStationModalBaseline(cloned);
    setStationModalDraft(cloned);
    setStationLookupQueryCode(asString(cloned.stationCode).toUpperCase());
    setStationModalError(null);
    setIsStationDiscardDialogOpen(false);
    setIsStationModalOpen(true);
  }

  function removeStationDraft(stationId: number) {
    if (isSentLocked) {
      return;
    }
    updateDraft((current) => ({
      ...current,
      stations: current.stations.filter((item) => item.id !== stationId),
    }));
  }

  function saveStationModal() {
    if (!stationModalDraft || !canSubmitStationModal) {
      return;
    }
    const validationError = validateStationModalDraft(stationModalDraft);
    if (validationError) {
      setStationModalError(validationError);
      return;
    }
    const stationContactEmails = extractPreferredContactEmails(stationModalDraft.contactsSnapshot);

    if (stationModalMode === "create") {
      const nextTempId = tempRowIdRef.current;
      tempRowIdRef.current -= 1;
      const nextStation: TrafficStation = {
        ...stationModalDraft,
        id: nextTempId,
        stationCode: asString(stationModalDraft.stationCode).toUpperCase(),
      };
      updateDraft((current) => ({
        ...current,
        stations: [...current.stations, nextStation],
        email: current.email
          ? {
              ...current.email,
              toEmails: mergeUniqueEmails(current.email.toEmails, stationContactEmails),
            }
          : current.email,
      }));
    } else if (stationModalBaseline) {
      updateDraft((current) => ({
        ...current,
        stations: current.stations.map((item) => item.id === stationModalBaseline.id
          ? { ...stationModalDraft, id: item.id, stationCode: asString(stationModalDraft.stationCode).toUpperCase() }
          : item),
        email: current.email
          ? {
              ...current.email,
              toEmails: mergeUniqueEmails(current.email.toEmails, stationContactEmails),
            }
          : current.email,
      }));
    }

    setIsStationModalOpen(false);
    resetStationModalState();
  }

  function handleStationModalOpenChange(nextOpen: boolean) {
    const allowClose = canModalClose({
      nextOpen,
      isBusy: isSaving,
      hasUnsavedChanges: hasStationModalChanges,
    });
    if (!allowClose) {
      if (!nextOpen && hasStationModalChanges) {
        setIsStationDiscardDialogOpen(true);
      }
      return;
    }
    setIsStationModalOpen(nextOpen);
    if (!nextOpen) {
      resetStationModalState();
    }
  }

  const normalizedStationLookupCode = useMemo(
    () => asString(stationModalDraft?.stationCode).toUpperCase(),
    [stationModalDraft?.stationCode],
  );
  const stationMetaCodesKey = useMemo(() => {
    const codes = detailDraft?.stations
      .map((station) => asString(station.stationCode).toUpperCase())
      .filter(Boolean) ?? [];
    const uniqueCodes = [...new Set(codes)];
    uniqueCodes.sort();
    return uniqueCodes.join(",");
  }, [detailDraft?.stations]);
  const commitStationLookupCode = useCallback((rawCode: string) => {
    const nextCode = asString(rawCode).toUpperCase();
    setStationLookupQueryCode(nextCode);
    if (!nextCode) {
      setStationLookupName(null);
      setStationLookupError(null);
      setStationLookupCacheStatus(null);
      setIsStationLookupLoading(false);
    }
  }, []);
  const stationLookupStatusText = useMemo(() => {
    if (!stationLookupQueryCode) {
      return normalizedStationLookupCode
        ? "Press Enter, Tab, or leave the field to load station info."
        : "Enter a station code to load station info.";
    }
    if (isStationLookupLoading && !stationLookupCacheStatus) {
      return "Loading station info...";
    }
    if (!isOnline && stationLookupCacheStatus) {
      return `Offline. Showing cached data from ${formatRelativeTime(stationLookupCacheStatus.fetchedAt)}.`;
    }
    if (stationLookupCacheStatus) {
      return `Data source: ${stationLookupCacheStatus.source}. Last updated ${formatRelativeTime(stationLookupCacheStatus.fetchedAt)}.`;
    }
    return "No cached data yet";
  }, [isOnline, isStationLookupLoading, normalizedStationLookupCode, stationLookupCacheStatus, stationLookupQueryCode]);

  const applyStationLookupData = useCallback((params: {
    stationCode: string;
    stationName: string | null;
    deliveryMethodName: string;
    contactsSnapshot: Record<string, unknown> | null;
  }) => {
    const { stationCode, stationName, deliveryMethodName, contactsSnapshot } = params;
    setStationLookupName(stationName);
    setStationLookupError(null);
    setStationModalDraft((current) => {
      if (!current) {
        return current;
      }
      if (asString(current.stationCode).toUpperCase() !== stationCode) {
        return current;
      }
      return {
        ...current,
        deliveryMethod: deliveryMethodName || current.deliveryMethod,
        contactsSnapshot: contactsSnapshot ?? current.contactsSnapshot,
      };
    });
    updateDraft((current) => {
      let changed = false;
      const nextStations = current.stations.map((item) => {
        if (asString(item.stationCode).toUpperCase() !== stationCode) {
          return item;
        }
        const nextDeliveryMethod = deliveryMethodName || item.deliveryMethod || "";
        const nextContactsSnapshot = contactsSnapshot ?? item.contactsSnapshot;
        if (
          asString(item.deliveryMethod || "") === asString(nextDeliveryMethod)
          && JSON.stringify(item.contactsSnapshot ?? null) === JSON.stringify(nextContactsSnapshot ?? null)
        ) {
          return item;
        }
        changed = true;
        return {
          ...item,
          deliveryMethod: asNullableString(nextDeliveryMethod),
          contactsSnapshot: nextContactsSnapshot,
        };
      });
      if (!changed) {
        return current;
      }
      return {
        ...current,
        stations: nextStations,
      };
    });
  }, [updateDraft]);

  useEffect(() => {
    if (!isStationModalOpen) {
      return;
    }
    if (!stationLookupQueryCode) {
      setStationLookupName(null);
      setStationLookupError(null);
      setIsStationLookupLoading(false);
      setStationLookupCacheStatus(null);
      return;
    }

    const requestId = stationLookupRequestIdRef.current + 1;
    stationLookupRequestIdRef.current = requestId;
    setIsStationLookupLoading(true);
    setStationLookupError(null);

    const timer = window.setTimeout(async () => {
      const lookupCode = stationLookupQueryCode;
      const cacheKey = buildStationLookupCacheKey(lookupCode);
      const cacheSnapshot = readBrowserCacheSnapshot<unknown>(cacheKey);
      const cachedData = isRecord(cacheSnapshot?.data) ? cacheSnapshot.data : null;
      const forceNetworkFetch = stationLookupRefreshToken > 0;
      const shouldFetchFromNetwork = forceNetworkFetch || shouldFetchNetwork("cache-first", cacheSnapshot);
      if (cachedData) {
        applyStationLookupData({
          stationCode: lookupCode,
          stationName: asNullableString(cachedData.name),
          deliveryMethodName: asString(cachedData.deliveryMethod),
          contactsSnapshot: isRecord(cachedData.contactsSnapshot) ? cachedData.contactsSnapshot : null,
        });
        setStationLookupCacheStatus({
          source: "cache",
          fetchedAt: cacheSnapshot?.fetchedAt ?? Date.now(),
        });
      }
      if (!shouldFetchFromNetwork) {
        if (stationLookupRequestIdRef.current === requestId) {
          setIsStationLookupLoading(false);
        }
        if (forceNetworkFetch) {
          setStationLookupRefreshToken(0);
        }
        return;
      }

      try {
        const params = new URLSearchParams();
        params.set("codes", lookupCode);
        params.set("deliveryMethodDetail", "true");
        params.set("contactDetail", "true");
        params.set("includeContacts", "true");
        const payload = await requestJson(`/api/tradsphere/v1/stations?${params.toString()}`, {
          headers: requestHeaders,
          successToast: false,
        });
        if (stationLookupRequestIdRef.current !== requestId) {
          return;
        }

        const list = unwrapData(payload);
        const stationRow = Array.isArray(list) ? list.find(isRecord) : null;
        if (!stationRow) {
          setStationLookupName(null);
          setStationLookupError(`Station ${normalizedStationLookupCode} was not found.`);
          return;
        }

        const stationName = asString(stationRow.name) || null;
        const deliveryMethodRaw = stationRow.deliveryMethod;
        const deliveryMethodName = isRecord(deliveryMethodRaw)
          ? asString(deliveryMethodRaw.name)
          : asString(deliveryMethodRaw);
        const contactsSnapshot = isRecord(stationRow.contacts) ? stationRow.contacts : null;
        applyStationLookupData({
          stationCode: lookupCode,
          stationName,
          deliveryMethodName,
          contactsSnapshot,
        });
        const fetchedAt = Date.now();
        writeBrowserCache(
          cacheKey,
          {
            code: lookupCode,
            name: stationName,
            deliveryMethod: deliveryMethodName,
            contactsSnapshot,
          },
          STATION_LOOKUP_CACHE_TTL_MS,
          {
            source: "network",
            fetchedAt,
          },
        );
        setStationLookupCacheStatus({
          source: "network",
          fetchedAt,
        });
      } catch (lookupError) {
        if (stationLookupRequestIdRef.current !== requestId) {
          return;
        }
        setStationLookupName(null);
        setStationLookupError(getTrafficErrorMessage(lookupError, "Unable to fetch station details."));
      } finally {
        if (stationLookupRequestIdRef.current === requestId) {
          setIsStationLookupLoading(false);
        }
        if (forceNetworkFetch) {
          setStationLookupRefreshToken(0);
        }
      }
    }, 300);

    return () => {
      window.clearTimeout(timer);
    };
  }, [
    applyStationLookupData,
    isStationModalOpen,
    requestHeaders,
    requestJson,
    stationLookupQueryCode,
    stationLookupRefreshToken,
  ]);

  useEffect(() => {
    const uniqueCodes = stationMetaCodesKey
      ? stationMetaCodesKey.split(",").filter(Boolean)
      : [];
    if (!uniqueCodes.length) {
      setStationLookupMetaByCode({});
      return;
    }

    const requestId = stationMetaRequestIdRef.current + 1;
    stationMetaRequestIdRef.current = requestId;

    const cachedMeta: Record<string, StationLookupMeta> = {};
    const localMeta: Record<string, StationLookupMeta> = {};
    const existingMetaByCode = stationLookupMetaByCodeRef.current;
    for (const stationCode of uniqueCodes) {
      const existingMeta = existingMetaByCode[stationCode];
      if (existingMeta) {
        localMeta[stationCode] = existingMeta;
      }
      const cacheSnapshot = readBrowserCacheSnapshot<unknown>(buildStationLookupCacheKey(stationCode));
      const cachedData = isRecord(cacheSnapshot?.data) ? cacheSnapshot.data : null;
      const normalizedCached = normalizeStationLookupMeta(cachedData, stationCode);
      if (!normalizedCached) {
        continue;
      }
      cachedMeta[stationCode] = normalizedCached;
      localMeta[stationCode] = normalizedCached;
    }
    if (Object.keys(localMeta).length > 0) {
      setStationLookupMetaByCode(localMeta);
    }

    const missingCodes = uniqueCodes.filter((stationCode) => !localMeta[stationCode]);
    if (missingCodes.length === 0) {
      return;
    }
    if (!isOnline) {
      return;
    }

    async function loadStationMeta() {
      try {
        const params = new URLSearchParams();
        params.set("codes", missingCodes.join(","));
        params.set("deliveryMethodDetail", "true");
        // Keep this background prefetch lightweight; table rendering only needs station meta.
        params.set("contactDetail", "false");
        params.set("includeContacts", "false");
        const payload = await requestJson(`/api/tradsphere/v1/stations?${params.toString()}`, {
          headers: requestHeaders,
          successToast: false,
          errorToast: false,
        });
        if (stationMetaRequestIdRef.current !== requestId) {
          return;
        }
        const rows = unwrapData(payload);
        if (!Array.isArray(rows)) {
          return;
        }
        const nextMeta: Record<string, StationLookupMeta> = { ...localMeta };
        for (const row of rows) {
          const normalizedMeta = normalizeStationLookupMeta(row);
          if (!normalizedMeta) {
            continue;
          }
          nextMeta[normalizedMeta.code] = normalizedMeta;
          writeBrowserCache(
            buildStationLookupCacheKey(normalizedMeta.code),
            {
              code: normalizedMeta.code,
              name: normalizedMeta.name,
              mediaType: normalizedMeta.mediaType,
              deliveryMethod: normalizedMeta.deliveryMethod?.name || "",
            },
            STATION_LOOKUP_CACHE_TTL_MS,
            {
              source: "network",
              fetchedAt: Date.now(),
            },
          );
        }
        if (Object.keys(nextMeta).length > 0) {
          const visibleMeta: Record<string, StationLookupMeta> = {};
          for (const stationCode of uniqueCodes) {
            const meta = nextMeta[stationCode];
            if (meta) {
              visibleMeta[stationCode] = meta;
            }
          }
          setStationLookupMetaByCode(visibleMeta);
          return;
        }
        if (Object.keys(localMeta).length > 0) {
          setStationLookupMetaByCode(localMeta);
        }
      } catch {
        if (stationMetaRequestIdRef.current !== requestId) {
          return;
        }
        if (Object.keys(localMeta).length > 0) {
          setStationLookupMetaByCode(localMeta);
        }
      }
    }

    void loadStationMeta();
  }, [isOnline, requestHeaders, requestJson, stationMetaCodesKey]);

  const selectedDeliveryStationMeta = useMemo(() => {
    const code = asString(selectedDeliveryStationCode).toUpperCase();
    if (!code) {
      return null;
    }
    return stationLookupMetaByCode[code] ?? null;
  }, [selectedDeliveryStationCode, stationLookupMetaByCode]);

  useEffect(() => {
    stationLookupMetaByCodeRef.current = stationLookupMetaByCode;
  }, [stationLookupMetaByCode]);

  const selectedDeliveryStationLabel = useMemo(() => {
    const code = asString(selectedDeliveryStationCode).toUpperCase();
    if (!code) {
      return "";
    }
    return formatStationDisplayLabel(code, selectedDeliveryStationMeta);
  }, [selectedDeliveryStationCode, selectedDeliveryStationMeta]);

  const sortedStations = useMemo(() => {
    const stations = activeDraft?.stations ?? [];
    if (!stations.length) {
      return stations;
    }
    return [...stations].sort((left, right) => {
      const leftCode = asString(left.stationCode).toUpperCase();
      const rightCode = asString(right.stationCode).toUpperCase();
      const leftMediumRaw = asString(stationLookupMetaByCode[leftCode]?.mediaType).toUpperCase();
      const rightMediumRaw = asString(stationLookupMetaByCode[rightCode]?.mediaType).toUpperCase();
      const leftMedium = leftMediumRaw || "ZZZ";
      const rightMedium = rightMediumRaw || "ZZZ";
      const mediumCompare = leftMedium.localeCompare(rightMedium, "en", {
        sensitivity: "base",
      });
      if (mediumCompare !== 0) {
        return mediumCompare;
      }
      const leftDeliveryMethod = asString(left.deliveryMethod).toUpperCase() || "ZZZ";
      const rightDeliveryMethod = asString(right.deliveryMethod).toUpperCase() || "ZZZ";
      const deliveryMethodCompare = leftDeliveryMethod.localeCompare(rightDeliveryMethod, "en", {
        sensitivity: "base",
      });
      if (deliveryMethodCompare !== 0) {
        return deliveryMethodCompare;
      }
      return leftCode.localeCompare(rightCode, "en", {
        numeric: true,
        sensitivity: "base",
      });
    });
  }, [activeDraft?.stations, stationLookupMetaByCode]);
  const stationContactEmailsById = useMemo<Record<string, string[]>>(() => {
    const output: Record<string, string[]> = {};
    for (const station of sortedStations) {
      output[String(station.id)] = extractPreferredContactEmails(station.contactsSnapshot);
    }
    return output;
  }, [sortedStations]);
  const contactNameByEmail = useMemo(() => {
    const output: Record<string, string> = {};
    const stations = activeDraft?.stations ?? [];
    for (const station of stations) {
      const snapshot = station.contactsSnapshot;
      if (!isRecord(snapshot)) {
        continue;
      }
      for (const bucket of Object.values(snapshot)) {
        if (!Array.isArray(bucket)) {
          continue;
        }
        for (const entry of bucket) {
          if (!isRecord(entry)) {
            continue;
          }
          const email = asString(entry.email).toLowerCase();
          if (!email || output[email]) {
            continue;
          }
          const fullName = asString(entry.fullName || entry.name || entry.contactName);
          if (fullName) {
            output[email] = fullName;
          }
        }
      }
    }
    return output;
  }, [activeDraft?.stations]);

  const handleOpenDeliveryMethodDetail = useCallback((stationCodeRaw: string) => {
    const code = asString(stationCodeRaw).toUpperCase();
    if (!code) {
      return;
    }
    setSelectedDeliveryStationCode(code);
    setIsDeliveryMethodDialogOpen(true);
  }, []);

  const handleCopyStationContacts = useCallback(async (emails: string[]) => {
    if (!emails.length) {
      toast.info("No contact emails available");
      return;
    }
    try {
      await navigator.clipboard.writeText(emails.join(", "));
      toast.success("Copied contact emails", `${emails.length} email${emails.length === 1 ? "" : "s"} copied to clipboard.`);
    } catch {
      toast.error("Unable to copy emails", "Clipboard access is unavailable in this browser.");
    }
  }, [toast]);

  const statusCards = useMemo(() => {
    const counts = {
      draft: 0,
      ready: 0,
      sent: 0,
      confirmed: 0,
    } as Record<string, number>;

    for (const row of trafficList) {
      const key = asString(row.status).toLowerCase();
      if (key in counts) {
        counts[key] += 1;
      }
    }

    return [
      { label: "Draft", value: counts.draft },
      { label: "Ready", value: counts.ready },
      { label: "Sent", value: counts.sent },
      { label: "Confirmed", value: counts.confirmed },
    ];
  }, [trafficList]);

  return (
    <div className="mx-auto flex w-full max-w-[1600px] flex-col gap-5 pb-12 xl:min-h-[calc(100dvh-3.5rem)]">
      <PageBanner
        eyebrow=""
        title="Traffic"
        description="Create and manage account-centered traffic records, flights, stations, and email drafts."
      />

      <SectionCard
        title="Account"
        divider={false}
      >
        <AccountSelector
          selectedAccountCode={selectedAccountCode}
          options={accountSelections}
          isLoadingSelections={isLoadingSelections}
          selectionsError={selectionsError}
          isLoadingAccount={isLoadingAccountTraffic}
          isRefreshingAccount={isRefreshingSelections || isRefreshingAccountTraffic}
          isSavingAccount={isSaving}
          onAccountChange={handleAccountChange}
          onLoad={() => {
            const targetAccountCode = asString(selectedAccountCode).toUpperCase();
            const isSwitchingAccount = Boolean(activeAccountCode && targetAccountCode && targetAccountCode !== activeAccountCode);
            if (hasUnsavedChanges && isSwitchingAccount) {
              setPendingAction({ type: "account", accountCode: targetAccountCode });
              setIsUnsavedDialogOpen(true);
              return;
            }
            if (hasUnsavedChanges) {
              setPendingAction({ type: "refresh" });
              setIsUnsavedDialogOpen(true);
              return;
            }
            void handleLoadFromSelector();
          }}
        />
      </SectionCard>

      {visibleRefreshMessage ? (
        <p className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-2 text-sm text-amber-800">
          {visibleRefreshMessage}
        </p>
      ) : null}

      {error ? (
        <div className="rounded-xl border border-rose-200 bg-rose-50 px-4 py-2 text-sm text-rose-700">
          <div className="flex items-start gap-2">
            <AlertCircle className="mt-0.5 size-4 shrink-0" />
            <p>{error}</p>
          </div>
        </div>
      ) : null}

      <div className="relative grid gap-4 xl:grid-cols-[minmax(18rem,26rem)_minmax(0,1fr)]">
        <SectionCard
          title="Traffic Records"
          description={(
            <span className="flex flex-wrap items-center gap-2">
              {hasUnsavedChanges ? (
                <span className="inline-flex rounded-full border border-amber-200 bg-amber-50 px-2 py-0.5 text-[11px] font-semibold text-amber-800">
                  Unsaved changes
                </span>
              ) : null}
              <span>{trafficList.length} record(s) for {activeAccountCode || "selected account"}</span>
            </span>
          )}
          actions={(
            <ActionIconButton
              icon={<Plus />}
              tooltip="New Traffic"
              aria-label="New Traffic"
              title="New Traffic"
              onClick={() => void handleCreateTraffic()}
              disabled={!activeAccountCode || !canEditTradsphere || isLoadingAccountTraffic || isSaving || hasUnsavedChanges || isSentLocked}
              className="!h-7 !w-7 !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-4 [&_svg]:!w-4 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110"
            />
          )}
          contentClassName="space-y-3"
        >
          <div className="grid grid-cols-2 gap-2">
            {statusCards.map((card) => (
              <div key={card.label} className="rounded-xl border border-blue-100 bg-blue-50/40 px-3 py-2">
                <p className="text-xs text-slate-500">{card.label}</p>
                <p className="text-lg font-semibold text-slate-900">{card.value}</p>
              </div>
            ))}
          </div>

          <div className="max-h-[52vh] space-y-2 overflow-y-auto pr-1">
            {!activeAccountCode ? (
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                Select an account to load traffic records.
              </div>
            ) : trafficList.length === 0 ? (
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                No active traffic records for this account.
              </div>
            ) : (
              trafficList.map((item) => {
                const active = item.id === selectedTrafficId;
                const isDraft = asString(item.status).toLowerCase() === "draft";
                const shouldHardDelete = hasUnsavedChanges || isDraft;
                const displayCampaign = active && activeDraft ? activeDraft.traffic.campaign : item.campaign;
                const displayStatus = active && activeDraft ? activeDraft.traffic.status : item.status;
                return (
                  <div
                    key={item.id}
                    role="button"
                    tabIndex={0}
                    onClick={() => handleSelectTraffic(item.id)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter" || event.key === " ") {
                        event.preventDefault();
                        handleSelectTraffic(item.id);
                      }
                    }}
                    className={[
                      "group w-full rounded-xl border px-3 py-3 text-left transition",
                      active
                        ? "border-blue-300 bg-blue-50/70"
                        : "border-blue-100 bg-white hover:border-blue-200 hover:bg-blue-50/30",
                    ].join(" ")}
                  >
                    <div className="flex items-center justify-between gap-3">
                      <div className="min-w-0">
                        <p className="truncate text-sm font-semibold text-slate-900">{displayCampaign || "Untitled campaign"}</p>
                        <span className={`mt-1 inline-flex rounded-full border px-2 py-0.5 text-[11px] font-semibold ${statusChipClass(displayStatus)}`}>
                          {toTrafficStatusLabel(displayStatus)}
                        </span>
                      </div>
                      <div className="flex items-center">
                        <div className="flex items-center gap-0.5 transition-opacity duration-150 max-md:opacity-100 md:pointer-events-none md:opacity-0 md:group-hover:pointer-events-auto md:group-hover:opacity-100 md:group-focus-within:pointer-events-auto md:group-focus-within:opacity-100">
                          <ActionIconButton
                            icon={shouldHardDelete ? <Trash2 /> : <Archive />}
                            tooltip={shouldHardDelete ? "Delete Traffic" : "Archive Traffic"}
                            aria-label={shouldHardDelete ? "Delete Traffic" : "Archive Traffic"}
                            title={shouldHardDelete ? "Delete Traffic" : "Archive Traffic"}
                            onClick={(event) => {
                              event.stopPropagation();
                              if (shouldHardDelete) {
                                void handleHardDeleteTraffic(item.id);
                                return;
                              }
                              setArchiveTargetTrafficId(item.id);
                              setIsArchiveDialogOpen(true);
                            }}
                            disabled={!canEditTradsphere || isSaving || isLoadingAccountTraffic || isLoadingDetail}
                            className="!h-6 !w-6 !rounded-full !p-0 text-rose-500 hover:!bg-rose-50 hover:!scale-105 hover:text-rose-600 focus-visible:!bg-rose-50 focus-visible:!scale-105 focus-visible:text-rose-600 [&_svg]:!h-3.5 [&_svg]:!w-3.5 [&_svg]:text-rose-500 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110 hover:[&_svg]:text-rose-600 focus-visible:[&_svg]:text-rose-600"
                          />
                        </div>
                      </div>
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </SectionCard>

        <div className="space-y-4">
          {!activeDraft ? (
            <SectionCard title="Traffic Workspace" contentClassName="space-y-3">
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-sm text-slate-600">
                {activeAccountCode
                  ? "Select a traffic record from the left panel to enable details, flights, stations, and email sections."
                  : "Select an account and load traffic records first."}
              </div>
            </SectionCard>
          ) : (
            <>
          <SectionCard
            title="Traffic Details"
            actions={(
              <ActionIconButton
                icon={<Archive />}
                tooltip="Archive Traffic"
                aria-label="Archive Traffic"
                title="Archive Traffic"
                onClick={() => {
                  if (!selectedTrafficId) {
                    return;
                  }
                  setArchiveTargetTrafficId(selectedTrafficId);
                  setIsArchiveDialogOpen(true);
                }}
                disabled={!selectedTrafficId || !canEditTradsphere || isSaving || isSentLocked || hasUnsavedChanges}
                className="!h-7 !w-7 !p-0 text-rose-600 hover:text-rose-700 focus-visible:text-rose-700 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-4 [&_svg]:!w-4 [&_svg]:text-rose-600 hover:[&_svg]:text-rose-700 focus-visible:[&_svg]:text-rose-700"
              />
            )}
            contentClassName="space-y-4"
          >
            {!activeAccountCode ? (
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-sm text-slate-600">
                Select an account to view traffic details.
              </div>
            ) : !activeDraft ? (
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-8 text-sm text-slate-600">
                Select a traffic record from the left panel.
              </div>
            ) : (
              <>
                {isSentLocked ? (
                  <div className="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-2 text-sm text-emerald-700">
                    This traffic is sent and locked. Editing actions are disabled.
                  </div>
                ) : null}

                <div className="grid gap-3 sm:grid-cols-2">
                  <label className="space-y-1 text-sm">
                    <span className="text-slate-600">Campaign</span>
                    <Input
                      value={activeDraft.traffic.campaign}
                      disabled={!canEditTradsphere || isSaving || isSentLocked}
                      onChange={(event) => {
                        const nextCampaign = event.target.value;
                        updateDraft((current) => ({
                          ...current,
                          traffic: {
                            ...current.traffic,
                            campaign: nextCampaign,
                          },
                        }));
                        updateTrafficCardSummary(activeDraft.traffic.id, { campaign: nextCampaign });
                      }}
                    />
                  </label>

                  <label className="space-y-1 text-sm">
                    <span className="text-slate-600">Status</span>
                    <AppDropdown
                      value={activeDraft.traffic.status}
                      onValueChange={(value) => {
                        const nextStatus = value as TrafficStatus;
                        updateDraft((current) => ({
                          ...current,
                          traffic: {
                            ...current.traffic,
                            status: nextStatus,
                          },
                        }));
                        updateTrafficCardSummary(activeDraft.traffic.id, { status: nextStatus });
                      }}
                      options={STATUS_OPTIONS}
                      searchable={false}
                      disabled={!canEditTradsphere || isSaving || isSentLocked}
                    />
                  </label>
                </div>

                <label className="space-y-1 text-sm">
                  <span className="text-slate-600">Note</span>
                  <Textarea
                    value={activeDraft.traffic.note || ""}
                    className="min-h-[88px]"
                    disabled={!canEditTradsphere || isSaving || isSentLocked}
                    onChange={(event) => updateDraft((current) => ({
                      ...current,
                      traffic: {
                        ...current.traffic,
                        note: event.target.value,
                      },
                    }))}
                  />
                </label>
              </>
            )}
          </SectionCard>

          <SectionCard
            title="Flights"
            actions={(
              <ActionIconButton
                icon={<Plus />}
                tooltip="Add Flight"
                aria-label="Add Flight"
                title="Add Flight"
                onClick={addFlightDraft}
                disabled={!canEditTradsphere || isSaving || isSentLocked}
                className="!h-7 !w-7 !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-4 [&_svg]:!w-4 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110"
              />
            )}
            contentClassName="space-y-3"
          >
            {!activeAccountCode ? (
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                Select an account to view flights.
              </div>
            ) : !activeDraft ? (
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                Select a traffic record to manage flights.
              </div>
            ) : (
              <>
                {activeDraft.flights.length > 0 && activeDraft.summary.rotationWarning ? (
                  <div className="rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
                    <div className="flex items-start gap-2">
                      <AlertTriangle className="mt-0.5 size-4 shrink-0" />
                      <p>{activeDraft.summary.rotationWarningMessage || "Total rotation is not 100.00%."}</p>
                    </div>
                  </div>
                ) : null}
                {activeDraft.flights.length === 0 ? (
                  <div className="rounded-lg border border-dashed border-slate-200 bg-white px-3 py-4 text-sm text-slate-600">
                    No flight rows yet.
                  </div>
                ) : (
                  <div className="overflow-x-auto rounded-xl border border-slate-200 bg-white">
                    <table className="min-w-full text-left text-xs">
                      <thead className="bg-slate-50 text-slate-600">
                        <tr>
                          <th className="px-3 py-2 font-semibold">Flight</th>
                          <th className="px-3 py-2 font-semibold">Medium</th>
                          <th className="px-3 py-2 font-semibold">Length</th>
                          <th className="px-3 py-2 font-semibold">ISCI</th>
                          <th className="px-3 py-2 font-semibold">Rotation</th>
                          <th className="px-3 py-2 font-semibold">File</th>
                          <th className="px-3 py-2 font-semibold">Script</th>
                          <th className="px-3 py-2 text-right font-semibold"></th>
                        </tr>
                      </thead>
                      <tbody>
                        {activeDraft.flights.map((flight) => (
                          <tr
                            key={flight.id}
                            onClick={() => editFlightDraft(flight.id)}
                            className="group cursor-pointer border-t border-slate-200 text-slate-700 transition hover:bg-blue-50/40"
                          >
                            <td className="whitespace-nowrap px-3 py-2">{formatFlightRangeForTable(flight.flightStart, flight.flightEnd)}</td>
                            <td className="whitespace-nowrap px-3 py-2">{flight.medium || "-"}</td>
                            <td className="whitespace-nowrap px-3 py-2">{formatLengthValue(flight.length)}</td>
                            <td className="whitespace-nowrap px-3 py-2">{flight.isci || "-"}</td>
                            <td className="whitespace-nowrap px-3 py-2">{formatRotationValue(flight.rotation)}</td>
                            <td className="whitespace-nowrap px-3 py-2">
                              {normalizeExternalUrl(flight.fileUrl) ? (
                                <a
                                  href={normalizeExternalUrl(flight.fileUrl)}
                                  target="_blank"
                                  rel="noreferrer"
                                  onClick={(event) => event.stopPropagation()}
                                  className="inline-flex items-center gap-1 rounded-full border border-emerald-200 bg-emerald-50 px-2 py-0.5 text-[11px] font-medium text-emerald-700 hover:bg-emerald-100"
                                >
                                  <Link2 className="size-3" />
                                  Linked
                                </a>
                              ) : (
                                <span className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-100 px-2 py-0.5 text-[11px] font-medium text-slate-600">
                                  <Unlink className="size-3" />
                                  Missing
                                </span>
                              )}
                            </td>
                            <td className="whitespace-nowrap px-3 py-2">
                              {normalizeExternalUrl(flight.scriptUrl) ? (
                                <a
                                  href={normalizeExternalUrl(flight.scriptUrl)}
                                  target="_blank"
                                  rel="noreferrer"
                                  onClick={(event) => event.stopPropagation()}
                                  className="inline-flex items-center gap-1 rounded-full border border-emerald-200 bg-emerald-50 px-2 py-0.5 text-[11px] font-medium text-emerald-700 hover:bg-emerald-100"
                                >
                                  <Link2 className="size-3" />
                                  Linked
                                </a>
                              ) : (
                                <span className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-100 px-2 py-0.5 text-[11px] font-medium text-slate-600">
                                  <Unlink className="size-3" />
                                  Missing
                                </span>
                              )}
                            </td>
                            <td className="px-3 py-2 text-right">
                              <ActionIconButton
                                icon={<Trash2 />}
                                tooltip="Delete flight row"
                                aria-label="Delete flight row"
                                title="Delete flight row"
                                onClick={(event) => {
                                  event.stopPropagation();
                                  removeFlightDraft(flight.id);
                                }}
                                disabled={!canEditTradsphere || isSaving || isSentLocked}
                                className="!h-6 !w-6 !rounded-full !p-0 text-rose-500 hover:!bg-rose-50 hover:!scale-105 hover:text-rose-600 focus-visible:!bg-rose-50 focus-visible:!scale-105 focus-visible:text-rose-600 [&_svg]:!h-3.5 [&_svg]:!w-3.5"
                              />
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </>
            )}
          </SectionCard>

          <SectionCard
            title="Station"
            actions={(
              <div className="flex items-center gap-1">
                <ActionIconButton
                  icon={isStationAutoSyncing ? <Loader2 className="animate-spin" /> : <RefreshCw />}
                  tooltip="Sync stations from flight date range"
                  aria-label="Sync stations from flight date range"
                  title="Sync stations from flight date range"
                  onClick={handleManualStationSync}
                  disabled={
                    !activeDraft
                    || !stationSyncFlightRange
                    || !canEditTradsphere
                    || isSaving
                    || isSentLocked
                    || isStationAutoSyncing
                    || !isOnline
                  }
                  className="!h-7 !w-7 !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-4 [&_svg]:!w-4 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110"
                />
                <ActionIconButton
                  icon={(isScheduleTimelineLoading || isStationAutoSyncing) ? <Loader2 className="animate-spin" /> : <Table2 />}
                  tooltip="View Schedule Timeline"
                  aria-label="View Schedule Timeline"
                  title="View Schedule Timeline"
                  onClick={() => {
                    if (!timelineAccountCode || !stationSyncFlightRange) {
                      return;
                    }
                    setIsScheduleTimelineModalOpen(true);
                  }}
                  disabled={!timelineAccountCode || !stationSyncFlightRange || isLoadingAccountTraffic || isStationAutoSyncing}
                  className="!h-7 !w-7 !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-4 [&_svg]:!w-4 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110"
                />
                <ActionIconButton
                  icon={<Plus />}
                  tooltip="Add Station"
                  aria-label="Add Station"
                  title="Add Station"
                  onClick={addStationDraft}
                  disabled={!canEditTradsphere || isSaving || isSentLocked}
                  className="!h-7 !w-7 !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-4 [&_svg]:!w-4 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110"
                />
              </div>
            )}
            contentClassName="space-y-3"
          >
            <div className="relative min-h-[5.5rem]">
              {!activeAccountCode ? (
                <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                  Select an account to view stations.
                </div>
              ) : !activeDraft ? (
                <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                  Select a traffic record to manage stations.
                </div>
              ) : activeDraft.stations.length === 0 ? (
                <div className="rounded-lg border border-dashed border-slate-200 bg-white px-3 py-4 text-sm text-slate-600">
                  No station rows yet.
                </div>
              ) : (
                <div className="overflow-x-auto rounded-xl border border-slate-200 bg-white">
                  <table className="min-w-full text-left text-xs">
                    <thead className="bg-slate-50 text-slate-600">
                      <tr>
                        <th className="px-3 py-2 font-semibold">Station</th>
                        <th className="px-3 py-2 font-semibold">Delivery Method</th>
                        <th className="px-3 py-2 font-semibold">Contacts</th>
                        <th className="px-3 py-2 font-semibold">Delivery Status</th>
                        <th className="px-3 py-2 font-semibold">Confirmed Status</th>
                        <th className="px-3 py-2 text-right font-semibold"></th>
                      </tr>
                    </thead>
                    <tbody>
                      {sortedStations.map((station) => (
                        <tr
                          key={station.id}
                          onClick={() => editStationDraft(station.id)}
                          className="group cursor-pointer border-t border-slate-200 text-slate-700 transition hover:bg-blue-50/40"
                        >
                          <td className="whitespace-nowrap px-3 py-2">
                            {formatStationDisplayLabel(station.stationCode, stationLookupMetaByCode[asString(station.stationCode).toUpperCase()])}
                          </td>
                          <td className="whitespace-nowrap px-3 py-2">
                            <button
                              type="button"
                              onClick={(event) => {
                                event.stopPropagation();
                                handleOpenDeliveryMethodDetail(station.stationCode);
                              }}
                              className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-medium transition focus-visible:outline-none focus-visible:ring-2 ${deliveryMethodChipClass(station.deliveryMethod)}`}
                            >
                              {station.deliveryMethod || "View"}
                            </button>
                          </td>
                          <td className="px-3 py-2 min-w-[14rem]">
                            {(() => {
                              const allEmails = stationContactEmailsById[String(station.id)] ?? [];
                              return (
                                <StationContactsCell
                                  emails={allEmails}
                                  onCopy={() => {
                                    void handleCopyStationContacts(allEmails);
                                  }}
                                />
                              );
                            })()}
                          </td>
                          <td className="whitespace-nowrap px-3 py-2">
                            <span className={`inline-flex rounded-full border px-2 py-0.5 text-[11px] font-medium ${stationStatusChipClass(station.deliveryStatus)}`}>
                              {formatStatusOptionLabel(station.deliveryStatus)}
                            </span>
                          </td>
                          <td className="whitespace-nowrap px-3 py-2">
                            <span className={`inline-flex rounded-full border px-2 py-0.5 text-[11px] font-medium ${stationStatusChipClass(station.confirmedStatus)}`}>
                              {formatStatusOptionLabel(station.confirmedStatus)}
                            </span>
                          </td>
                          <td className="px-3 py-2 text-right">
                            <ActionIconButton
                              icon={<Trash2 />}
                              tooltip="Delete station row"
                              aria-label="Delete station row"
                              title="Delete station row"
                              onClick={(event) => {
                                event.stopPropagation();
                                removeStationDraft(station.id);
                              }}
                              disabled={!canEditTradsphere || isSaving || isSentLocked}
                              className="!h-6 !w-6 !rounded-full !p-0 text-rose-500 hover:!bg-rose-50 hover:!scale-105 hover:text-rose-600 focus-visible:!bg-rose-50 focus-visible:!scale-105 focus-visible:text-rose-600 [&_svg]:!h-3.5 [&_svg]:!w-3.5"
                            />
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              {isStationAutoSyncing ? (
                <SectionLoadingOverlay message="Auto-syncing stations from schedule..." />
              ) : null}
            </div>
          </SectionCard>

          <SectionCard
            title={(
              <div className="space-y-1">
                <p>Email Draft</p>
                <span className={`inline-flex rounded-full border px-2 py-0.5 text-[11px] font-medium ${emailStatusChipClass(activeDraft?.email?.sentStatus || "draft")}`}>
                  {formatStatusOptionLabel(activeDraft?.email?.sentStatus || "draft")}
                </span>
              </div>
            )}
            actions={(
              <ActionIconButton
                icon={<Send />}
                tooltip="Mark email as ready"
                aria-label="Mark email as ready"
                title="Mark email as ready"
                onClick={() => {
                  if (!activeDraft) {
                    return;
                  }
                  updateDraft((current) => ({
                    ...current,
                    email: current.email
                      ? {
                          ...current.email,
                          sentStatus: "ready",
                        }
                      : current.email,
                  }));
                }}
                disabled={!canEditTradsphere || isSaving || isSentLocked || !activeDraft}
                className="!h-7 !w-7 !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-4 [&_svg]:!w-4 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110"
              />
            )}
            contentClassName="space-y-3"
          >
            {!activeAccountCode ? (
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                Select an account to view the email draft.
              </div>
            ) : !activeDraft ? (
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                Select a traffic record to manage the email draft.
              </div>
            ) : (
              <>
                <EmailChipsInput
                  value={activeDraft.email?.toEmails || []}
                  placeholder="To emails"
                  disabled={!canEditTradsphere || isSaving || isSentLocked}
                  labelByEmail={contactNameByEmail}
                  onChange={(nextEmails) => updateDraft((current) => ({
                    ...current,
                    email: current.email
                      ? {
                          ...current.email,
                          toEmails: nextEmails,
                        }
                      : current.email,
                  }))}
                />
                <div className="grid gap-2 md:grid-cols-2">
                  <EmailChipsInput
                    value={activeDraft.email?.ccEmails || []}
                    placeholder="CC emails"
                    disabled={!canEditTradsphere || isSaving || isSentLocked}
                    labelByEmail={contactNameByEmail}
                    onChange={(nextEmails) => updateDraft((current) => ({
                      ...current,
                      email: current.email
                        ? {
                            ...current.email,
                            ccEmails: nextEmails,
                          }
                        : current.email,
                    }))}
                  />
                  <EmailChipsInput
                    value={activeDraft.email?.bccEmails || []}
                    placeholder="BCC emails"
                    disabled={!canEditTradsphere || isSaving || isSentLocked}
                    labelByEmail={contactNameByEmail}
                    onChange={(nextEmails) => updateDraft((current) => ({
                      ...current,
                      email: current.email
                        ? {
                            ...current.email,
                            bccEmails: nextEmails,
                          }
                        : current.email,
                    }))}
                  />
                </div>
                <Input
                  value={activeDraft.email?.subject || ""}
                  placeholder="Email subject"
                  disabled={!canEditTradsphere || isSaving || isSentLocked}
                  onChange={(event) => updateDraft((current) => ({
                    ...current,
                    email: current.email
                      ? {
                          ...current.email,
                          subject: event.target.value,
                        }
                      : current.email,
                  }))}
                />
                <Textarea
                  value={activeDraft.email?.body || ""}
                  placeholder="Email body"
                  className="min-h-[110px]"
                  disabled={!canEditTradsphere || isSaving || isSentLocked}
                  onChange={(event) => updateDraft((current) => ({
                    ...current,
                    email: current.email
                      ? {
                          ...current.email,
                          body: event.target.value,
                        }
                      : current.email,
                  }))}
                />
              </>
            )}
          </SectionCard>
            </>
          )}
        </div>

        {(isLoadingAccountTraffic || isLoadingDetail || isSaving) ? (
          <SectionLoadingOverlay message={isSaving ? "Saving traffic changes..." : "Loading traffic data..."} />
        ) : null}
      </div>

      {shouldShowSaveActions ? (
        <div className="flex w-full justify-end">
          <div className="flex flex-wrap items-center gap-2">
            {hasUnsavedChanges ? (
              <Button
                variant="outline"
                onClick={() => handleDiscard()}
                disabled={isSaving || isSentLocked}
              >
                Revert
              </Button>
            ) : null}
            <Button
              onClick={() => void handleSaveAll()}
              disabled={!canSaveChanges}
            >
              {isSaving ? (
                <>
                  <Loader2 className="size-4 animate-spin" />
                  Saving...
                </>
              ) : (
                "Save"
              )}
            </Button>
          </div>
        </div>
      ) : null}

      {cacheStatus ? (
        <div className="pointer-events-none fixed inset-x-0 bottom-[calc(1rem+env(safe-area-inset-bottom))] z-40">
          <div className={`mx-4 sm:mx-6 lg:mr-6 ${sidebarVisuallyExpanded ? "lg:ml-[18.75rem]" : "lg:ml-[6.5rem]"}`}>
            <div className="mx-auto w-full max-w-[1600px]">
              <CacheStatusChip
                text={cacheStatusText}
                onRefresh={() => {
                  if (hasUnsavedChanges) {
                    setPendingAction({ type: "refresh" });
                    setIsUnsavedDialogOpen(true);
                    return;
                  }
                  void handleRefresh();
                }}
                disabled={!activeAccountCode || !isOnline || isSaving || isLoadingAccountTraffic || isLoadingDetail}
                refreshing={isRefreshingAccountTraffic || isLoadingDetail}
                refreshLabel="Refresh traffic data"
                tooltipText={
                  hasUnsavedChanges
                    ? "Save or revert changes before refreshing traffic data."
                    : isOnline
                      ? "Click to refresh traffic data"
                      : "Offline. Reconnect to refresh traffic data."
                }
                containerClassName="pointer-events-auto"
                className="max-w-[min(92vw,40rem)]"
              />
            </div>
          </div>
        </div>
      ) : null}

      <Dialog
        open={isScheduleTimelineModalOpen}
        onOpenChange={(open) => {
          setIsScheduleTimelineModalOpen(open);
          if (!open) {
            setIsScheduleTimelineLoading(false);
          }
        }}
      >
        <DialogContent className="!h-fit !max-h-[95vh] !w-fit !max-w-[95vw] overflow-hidden p-4 sm:p-5 lg:p-6">
          <DialogClose
            className="absolute right-4 top-4 z-20 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            aria-label="Close schedule timeline modal"
          >
            <X className="size-4" />
          </DialogClose>
          <DialogHeader className="space-y-1 pr-12 text-left">
            <DialogTitle>Schedule Timeline</DialogTitle>
            <DialogDescription>
              View account schedule timeline table with cache-first loading.
            </DialogDescription>
          </DialogHeader>
          <div className="mt-3">
            <ScheduleTimelineSection
              accountCode={timelineAccountCode}
              esnums={[]}
              headers={requestHeaders}
              disabled={isLoadingAccountTraffic || isSaving}
              presentation="table-only"
              anchorStartDate={timelineAnchorStart}
              anchorEndDate={timelineAnchorEnd}
              onLoadingChange={setIsScheduleTimelineLoading}
            />
          </div>
        </DialogContent>
      </Dialog>

      <Dialog open={isFlightModalOpen} onOpenChange={handleFlightModalOpenChange}>
        <DialogContent
          onInteractOutside={(event) => {
            if (shouldBlockOutsideClose({ isBusy: isSaving, hasUnsavedChanges: hasFlightModalChanges })) {
              event.preventDefault();
            }
          }}
        >
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none"
            aria-label="Close flight modal"
            disabled={isSaving}
          >
            <X className="size-4" />
          </DialogClose>
          <DialogHeader className="pb-2">
            <DialogTitle>{flightModalMode === "create" ? "Add Flight" : "Edit Flight"}</DialogTitle>
            <DialogDescription>
              Update flight scheduling and delivery references.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3 pt-1">
            <div className="grid gap-3 sm:grid-cols-2">
              <label className="space-y-1 text-sm">
                <span className="text-slate-600">Flight Start</span>
                <DateInputField
                  id="traffic-flight-start"
                  value={flightModalDraft?.flightStart || ""}
                  onChange={(value) => {
                    setFlightModalError(null);
                    setFlightModalDraft((current) => (current ? { ...current, flightStart: value } : current));
                  }}
                  disabled={!canEditTradsphere || isSaving || isSentLocked}
                  label="flight start"
                />
              </label>
              <label className="space-y-1 text-sm">
                <span className="text-slate-600">Flight End</span>
                <DateInputField
                  id="traffic-flight-end"
                  value={flightModalDraft?.flightEnd || ""}
                  onChange={(value) => {
                    setFlightModalError(null);
                    setFlightModalDraft((current) => (current ? { ...current, flightEnd: value } : current));
                  }}
                  disabled={!canEditTradsphere || isSaving || isSentLocked}
                  label="flight end"
                />
              </label>
              <label className="space-y-1 text-sm">
                <span className="text-slate-600">Medium</span>
                <AppDropdown
                  value={flightModalDraft?.medium || "TV"}
                  onValueChange={(value) => {
                    setFlightModalError(null);
                    setFlightModalDraft((current) => (current ? { ...current, medium: value } : current));
                  }}
                  options={FLIGHT_MEDIA_OPTIONS.map((option) => ({ value: option, label: option }))}
                  searchable={false}
                  disabled={!canEditTradsphere || isSaving || isSentLocked}
                />
              </label>
              <label className="space-y-1 text-sm">
                <span className="text-slate-600">Length</span>
                <Input
                  type="number"
                  min={0}
                  step={1}
                  value={String(Math.max(0, Math.trunc(asNumber(flightModalDraft?.length, 0))))}
                  disabled={!canEditTradsphere || isSaving || isSentLocked}
                  onChange={(event) => {
                    setFlightModalError(null);
                    setFlightModalDraft((current) => (current ? { ...current, length: Math.max(0, Math.trunc(asNumber(event.target.value, 0))) } : current));
                  }}
                />
              </label>
              <label className="space-y-1 text-sm">
                <span className="text-slate-600">ISCI</span>
                <Input
                  value={flightModalDraft?.isci || ""}
                  disabled={!canEditTradsphere || isSaving || isSentLocked}
                  onChange={(event) => {
                    setFlightModalError(null);
                    const normalized = event.target.value.replace(/\s+/g, "").toUpperCase();
                    setFlightModalDraft((current) => (current ? { ...current, isci: asNullableString(normalized) } : current));
                  }}
                />
              </label>
              <label className="space-y-1 text-sm">
                <span className="text-slate-600">Rotation</span>
                <Input
                  type="number"
                  min={0}
                  max={100}
                  step={0.01}
                  value={String(asNumber(flightModalDraft?.rotation, 0))}
                  disabled={!canEditTradsphere || isSaving || isSentLocked}
                  onChange={(event) => {
                    setFlightModalError(null);
                    setFlightModalDraft((current) => (current ? { ...current, rotation: asNumber(event.target.value, 0) } : current));
                  }}
                />
              </label>
            </div>
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">File URL</span>
              <Input
                type="url"
                value={flightModalDraft?.fileUrl || ""}
                placeholder="https://..."
                disabled={!canEditTradsphere || isSaving || isSentLocked}
                onChange={(event) => {
                  setFlightModalError(null);
                  setFlightFileUrlError(null);
                  setFlightModalDraft((current) => (current ? { ...current, fileUrl: event.target.value } : current));
                }}
                onBlur={(event) => {
                  const raw = asString(event.target.value);
                  if (!raw) {
                    setFlightFileUrlError(null);
                    return;
                  }
                  if (isStrictHttpUrlInput(raw)) {
                    setFlightFileUrlError(null);
                    return;
                  }
                  setFlightFileUrlError("Invalid URL. Enter a valid http(s) URL.");
                  setFlightModalDraft((current) => (current ? { ...current, fileUrl: "" } : current));
                }}
              />
              {flightFileUrlError ? <p className="text-xs text-rose-600">{flightFileUrlError}</p> : null}
            </label>
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">Script URL</span>
              <Input
                type="url"
                value={flightModalDraft?.scriptUrl || ""}
                placeholder="https://..."
                disabled={!canEditTradsphere || isSaving || isSentLocked}
                onChange={(event) => {
                  setFlightModalError(null);
                  setFlightScriptUrlError(null);
                  setFlightModalDraft((current) => (current ? { ...current, scriptUrl: asNullableString(event.target.value) } : current));
                }}
                onBlur={(event) => {
                  const raw = asString(event.target.value);
                  if (!raw) {
                    setFlightScriptUrlError(null);
                    return;
                  }
                  if (isStrictHttpUrlInput(raw)) {
                    setFlightScriptUrlError(null);
                    return;
                  }
                  setFlightScriptUrlError("Invalid URL. Enter a valid http(s) URL.");
                  setFlightModalDraft((current) => (current ? { ...current, scriptUrl: null } : current));
                }}
              />
              {flightScriptUrlError ? <p className="text-xs text-rose-600">{flightScriptUrlError}</p> : null}
            </label>
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">Note</span>
              <Textarea
                value={flightModalDraft?.note || ""}
                className="min-h-[96px]"
                disabled={!canEditTradsphere || isSaving || isSentLocked}
                onChange={(event) => {
                  setFlightModalError(null);
                  setFlightModalDraft((current) => (current ? { ...current, note: asNullableString(event.target.value) } : current));
                }}
              />
            </label>
          </div>
          {flightModalError ? <p className="text-sm text-rose-600">{flightModalError}</p> : null}
          <DialogFooter>
            {canSubmitFlightModal ? (
              <Button onClick={saveFlightModal}>
                {flightModalMode === "create" ? "Add" : "Save"}
              </Button>
            ) : null}
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={isLastFlightDeleteConfirmOpen}
        onOpenChange={(open) => {
          setIsLastFlightDeleteConfirmOpen(open);
          if (!open) {
            setPendingFlightDeleteId(null);
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Clear Stations?</DialogTitle>
            <DialogDescription>
              Deleting this flight leaves no flight date range. Do you want to clear all station rows?
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => handleResolveLastFlightDelete(false)}>
              Keep Stations
            </Button>
            <Button
              className="bg-rose-600 text-white hover:bg-rose-700"
              onClick={() => handleResolveLastFlightDelete(true)}
            >
              Clear All Stations
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={isStationModalOpen} onOpenChange={handleStationModalOpenChange}>
        <DialogContent
          onInteractOutside={(event) => {
            if (shouldBlockOutsideClose({ isBusy: isSaving, hasUnsavedChanges: hasStationModalChanges })) {
              event.preventDefault();
            }
          }}
        >
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none"
            aria-label="Close station modal"
            disabled={isSaving}
          >
            <X className="size-4" />
          </DialogClose>
          <DialogHeader className="pb-2">
            <DialogTitle>{stationModalMode === "create" ? "Add Station" : "Edit Station"}</DialogTitle>
            <DialogDescription>
              Update delivery workflow and confirmation tracking for this station row.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-6 pt-1">
            <section className="min-w-0 space-y-3">
              <div className="flex min-h-10 items-center justify-between gap-3 border-b border-slate-200 pb-2">
                <h4 className="text-sm font-semibold text-slate-800">Station Info</h4>
                <div aria-hidden className="h-8 w-[56px]" />
              </div>
              <div className="space-y-3">
                <div className="grid gap-3 sm:grid-cols-1">
                <label className="space-y-1 text-sm">
                  <span className="text-sm font-medium leading-5 text-slate-600">Station Code</span>
                  <Input
                    value={stationModalDraft?.stationCode || ""}
                    disabled={!canEditTradsphere || isSaving || isSentLocked}
                    onChange={(event) => {
                      setStationModalError(null);
                      setStationLookupError(null);
                      const normalizedCode = event.target.value.replace(/\s+/g, "").toUpperCase();
                      setStationModalDraft((current) => (current ? { ...current, stationCode: normalizedCode } : current));
                      if (!normalizedCode) {
                        commitStationLookupCode("");
                      }
                    }}
                    onBlur={(event) => {
                      commitStationLookupCode(event.target.value);
                    }}
                    onKeyDown={(event) => {
                      if (event.key === "Enter" || event.key === "Tab") {
                        commitStationLookupCode(event.currentTarget.value);
                      }
                    }}
                  />
                </label>
                </div>

                <div className="space-y-2">
                  {!stationLookupQueryCode ? null : isStationLookupLoading ? (
                    <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-600">
                      <div className="flex items-center gap-2">
                        <Loader2 className="size-4 animate-spin" />
                        <span>Loading contact info...</span>
                      </div>
                    </div>
                  ) : (
                    <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2">
                      <div className="space-y-2">
                        <div className="grid grid-cols-[110px_minmax(0,1fr)] items-start gap-2 text-sm">
                          <p className="text-sm font-medium leading-5 text-slate-600">Station</p>
                          <p className="break-words text-sm leading-5 text-slate-800">
                            {stationLookupName || "-"}
                          </p>
                        </div>
                        <div className="grid grid-cols-[110px_minmax(0,1fr)] items-start gap-2 text-sm">
                          <p className="text-sm font-medium leading-5 text-slate-600">Delivery Method</p>
                          <p className="break-words text-sm leading-5 text-slate-800">
                            {stationModalDraft?.deliveryMethod || "-"}
                          </p>
                        </div>
                        <div className="grid grid-cols-[110px_minmax(0,1fr)] items-start gap-2 text-sm">
                          <p className="text-sm font-medium leading-5 text-slate-600">REP</p>
                          <p className="break-words text-sm leading-5 text-slate-800">
                            {extractContactEmailsFromSnapshot(stationModalDraft?.contactsSnapshot, "REP").join(", ") || "-"}
                          </p>
                        </div>
                        <div className="grid grid-cols-[110px_minmax(0,1fr)] items-start gap-2 text-sm">
                          <p className="text-sm font-medium leading-5 text-slate-600">TRAFFIC</p>
                          <p className="break-words text-sm leading-5 text-slate-800">
                            {extractContactEmailsFromSnapshot(stationModalDraft?.contactsSnapshot, "TRAFFIC").join(", ") || "-"}
                          </p>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </section>

            <section className="min-w-0 space-y-3">
              <div className="flex min-h-10 items-center justify-between gap-3 border-b border-slate-200 pb-2">
                <h4 className="text-sm font-semibold text-slate-800">Status</h4>
                <div aria-hidden className="h-8 w-[56px]" />
              </div>
              <div className="space-y-3">
                <div className="grid gap-3 sm:grid-cols-2">
                <label className="space-y-1 text-sm">
                  <span className="text-sm font-medium leading-5 text-slate-600">Delivery Status</span>
                  <AppDropdown
                    value={stationModalDraft?.deliveryStatus || ""}
                    onValueChange={(value) => {
                      setStationModalError(null);
                      setStationModalDraft((current) => (current ? { ...current, deliveryStatus: value } : current));
                    }}
                    options={[{ value: "", label: "" }, ...DELIVERY_STATUS_OPTIONS.map((option) => ({ value: option, label: formatStatusOptionLabel(option) }))]}
                    searchable={false}
                    disabled={!canEditTradsphere || isSaving || isSentLocked}
                    placeholder=""
                  />
                </label>
                <label className="space-y-1 text-sm">
                  <span className="text-sm font-medium leading-5 text-slate-600">Confirmed Status</span>
                  <AppDropdown
                    value={stationModalDraft?.confirmedStatus || ""}
                    onValueChange={(value) => {
                      setStationModalError(null);
                      setStationModalDraft((current) => (current ? { ...current, confirmedStatus: value } : current));
                    }}
                    options={[{ value: "", label: "" }, ...CONFIRMED_STATUS_OPTIONS.map((option) => ({ value: option, label: formatStatusOptionLabel(option) }))]}
                    searchable={false}
                    disabled={!canEditTradsphere || isSaving || isSentLocked}
                    placeholder=""
                  />
                </label>
                </div>
                <label className="space-y-1 text-sm">
                  <span className="text-sm font-medium leading-5 text-slate-600">Note</span>
                  <Textarea
                    value={stationModalDraft?.note || ""}
                    className="min-h-[96px]"
                    disabled={!canEditTradsphere || isSaving || isSentLocked}
                    onChange={(event) => {
                      setStationModalError(null);
                      setStationModalDraft((current) => (current ? { ...current, note: asNullableString(event.target.value) } : current));
                    }}
                  />
                </label>
              </div>
            </section>
          </div>
          {stationLookupError ? <p className="text-sm text-rose-600">{stationLookupError}</p> : null}
          {stationModalError ? <p className="text-sm text-rose-600">{stationModalError}</p> : null}
          <DialogFooter>
            {canSubmitStationModal ? (
              <Button onClick={saveStationModal}>
                {stationModalMode === "create" ? "Add" : "Save"}
              </Button>
            ) : null}
          </DialogFooter>
          <footer className="shrink-0 border-t border-slate-100 bg-white px-0 pt-2 pb-[calc(1rem+env(safe-area-inset-bottom))]">
            <CacheStatusChip
              text={stationLookupStatusText}
              onRefresh={() => {
                if (!stationLookupQueryCode || isStationLookupLoading) {
                  return;
                }
                setStationLookupRefreshToken((current) => current + 1);
              }}
              disabled={!stationLookupQueryCode || isStationLookupLoading}
              refreshing={isStationLookupLoading}
              refreshLabel="Refresh station info"
              tooltipText={stationLookupQueryCode ? "Click to refresh this data" : "Enter a station code to load station info."}
            />
          </footer>
        </DialogContent>
      </Dialog>

      <Dialog
        open={isDeliveryMethodDialogOpen}
        onOpenChange={(open) => {
          setIsDeliveryMethodDialogOpen(open);
          if (!open) {
            setSelectedDeliveryStationCode(null);
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delivery Method Detail</DialogTitle>
            <DialogDescription>
              {selectedDeliveryStationLabel || "Selected station"}
            </DialogDescription>
          </DialogHeader>
          {selectedDeliveryStationMeta?.deliveryMethod ? (
            <div className="space-y-2 rounded-xl border border-slate-200 bg-slate-50 p-3 text-sm text-slate-700">
              <p><span className="font-semibold text-slate-900">Name:</span> {selectedDeliveryStationMeta.deliveryMethod.name || "-"}</p>
              <p><span className="font-semibold text-slate-900">URL:</span> {selectedDeliveryStationMeta.deliveryMethod.url || "-"}</p>
              <p><span className="font-semibold text-slate-900">Username:</span> {selectedDeliveryStationMeta.deliveryMethod.username || "-"}</p>
              <p><span className="font-semibold text-slate-900">Deadline:</span> {selectedDeliveryStationMeta.deliveryMethod.deadline || "-"}</p>
              <p><span className="font-semibold text-slate-900">Note:</span> {selectedDeliveryStationMeta.deliveryMethod.note || "-"}</p>
            </div>
          ) : (
            <p className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
              No delivery method details available.
            </p>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setIsDeliveryMethodDialogOpen(false)}>
              Close
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <UnsavedChangesDialog
        open={isUnsavedDialogOpen}
        onKeepEditing={() => {
          setPendingAction(null);
          setIsUnsavedDialogOpen(false);
        }}
        onDiscardChanges={() => {
          void handleResolveUnsavedDialog(true);
        }}
      />

      <UnsavedChangesDialog
        open={isFlightDiscardDialogOpen}
        onKeepEditing={() => setIsFlightDiscardDialogOpen(false)}
        onDiscardChanges={() => {
          setIsFlightDiscardDialogOpen(false);
          setIsFlightModalOpen(false);
          resetFlightModalState();
        }}
      />

      <UnsavedChangesDialog
        open={isStationDiscardDialogOpen}
        onKeepEditing={() => setIsStationDiscardDialogOpen(false)}
        onDiscardChanges={() => {
          setIsStationDiscardDialogOpen(false);
          setIsStationModalOpen(false);
          resetStationModalState();
        }}
      />

      <Dialog
        open={isArchiveDialogOpen}
        onOpenChange={(open) => {
          setIsArchiveDialogOpen(open);
          if (!open) {
            setArchiveTargetTrafficId(null);
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Archive traffic record?</DialogTitle>
            <DialogDescription>
              Archived traffic is removed from the active list for this account.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setIsArchiveDialogOpen(false)}>
              Cancel
            </Button>
            <Button
              className="border-rose-700 bg-rose-600 text-white hover:bg-rose-700"
              onClick={() => void handleArchiveTraffic()}
            >
              Archive
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
