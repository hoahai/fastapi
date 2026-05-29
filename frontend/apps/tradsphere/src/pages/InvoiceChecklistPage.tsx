import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ClipboardEvent as ReactClipboardEvent,
  type DragEvent as ReactDragEvent,
  type KeyboardEvent as ReactKeyboardEvent,
} from "react";
import { AlertTriangle, Loader2, Monitor, Paperclip, Plus, Trash2, UploadCloud, X } from "lucide-react";

import { ActionIconButton } from "@/components/dashboard/ActionIconButton";
import { LabeledField } from "@/components/dashboard/FormFieldRow";
import { StationContactCard } from "@/components/dashboard/StationContactCard";
import { StationModal, type StationModalDetailLoadResult } from "@/components/dashboard/StationModal";
import { PageBanner } from "@/components/layout/PageBanner";
import { UnsavedChangesDialog } from "@/components/ui/unsaved-changes-dialog";
import { AppDropdown } from "@/components/ui/app-dropdown";
import { Button } from "@/components/ui/button";
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
import { useApiRequest } from "@/hooks/useApiRequest";
import { useDirtyRefreshGuard } from "@/hooks/useDirtyRefreshGuard";
import { useOnlineStatus } from "@/hooks/useOnlineStatus";
import {
  readBrowserCacheSnapshot,
  removeBrowserCache,
  writeBrowserCache,
} from "@/lib/browserCache";
import { buildAuthHeaders as buildSharedAuthHeaders } from "@shared/api/authHeaders";
import {
  readScopedPageState,
  TRADSPHERE_CACHE_TTL_MS,
  shouldFetchNetwork,
  type CachePolicy,
  writeScopedPageState,
} from "@shared/cache";
import { useAuth } from "@shared/auth/useAuth";
import { shouldProtectFrontendAuth } from "@shared/auth/guards";
import { hasAppEditAccess } from "@shared/auth/permissions";
import { useScopedPersistentState } from "@shared/hooks/useScopedPersistentState";
import { AppPageLayout } from "@shared/components/layout/AppPageLayout";
import { LoadActionArea } from "@shared/components/layout/LoadActionArea";
import { PageCacheFooter } from "@shared/components/layout/PageCacheFooter";
import { SectionCard } from "@shared/components/layout/SectionCard";
import { PageLoadingLayer, SectionLoadingLayer, SectionLoadingOverlay } from "@shared/components/status/LoadingOverlay";
import { PageMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { resolveSharedLoadingContract } from "@shared/components/status/loadingContract";
import {
  buildPeriodValue,
  buildRollingPeriods,
  formatPeriodMonthName,
  formatPeriodQuarterLabel,
  formatPeriodValue,
  parsePeriodInput,
  shiftMonthAnchor,
} from "@shared/utils/periods";
import { useToast } from "@/components/ui/toast";

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type PeriodItem = {
  year: number;
  month: number;
  quarter: number | null;
  value: string;
  label: string;
};

type ChecklistSearchStation = {
  estNum: number;
  stationCode: string;
};

type ChecklistProposalState = "proposed_add" | "proposed_remove" | "changed";

type ChecklistSummary = {
  id: string;
  accountCode: string;
  accountName: string;
  year: number;
  month: number;
  quarter: number | null;
  status: string;
  note: string;
  stationCount: number;
  matchedStationCount?: number;
  expectedStationCount: number;
  mismatchStationCount: number;
  hasScheduleMismatch: boolean;
  searchStations: ChecklistSearchStation[];
  dateUpdated: string | null;
  isLocalDraft?: boolean;
  isGeneratedPreview?: boolean;
  proposalState?: ChecklistProposalState | null;
  proposalLabel?: string | null;
};

type AttachmentItem = {
  id: number;
  noteId: number;
  url: string;
  fileName: string;
  fileType: string;
  mimeType?: string;
  fileSize?: number | null;
  storageProvider?: string;
  providerAssetId?: string;
  providerPublicId?: string;
  providerResourceType?: string;
  accessUrl?: string;
  uploadedBy?: string;
  tenantSlug?: string;
  ownerEntityType?: string;
  ownerEntityId?: string;
  dateCreated: string | null;
  dateUpdated: string | null;
  isLocalDraft?: boolean;
  localFile?: File;
};

type NoteItem = {
  id: number;
  checklistStationId: number;
  checklistYear?: number | null;
  checklistMonth?: number | null;
  amount: number | null;
  note: string;
  dateCreated: string | null;
  dateUpdated: string | null;
  attachments: AttachmentItem[];
  isLocalDraft?: boolean;
};

type StationNoteViewItem = {
  station: StationItem;
  note: NoteItem;
  isExternal?: boolean;
};

type RepContactItem = {
  id: number | null;
  fullName: string;
  email: string;
  office: string;
  mobile: string;
  jobTitle: string;
  company: string;
  primaryContact: boolean;
};

type StationItem = {
  id: number;
  checklistId: string;
  estNum: number;
  stationCode: string;
  stationName: string;
  mediaType: string;
  status: string;
  dateUpdated: string | null;
  inCurrentSchedule: boolean;
  scheduleMismatch: boolean;
  scheduleMismatchReason: string | null;
  repContacts: RepContactItem[];
  notes: NoteItem[];
  isLocalDraft?: boolean;
  isGeneratedPreview?: boolean;
  proposalState?: ChecklistProposalState | null;
  proposalLabel?: string | null;
};

type ChecklistDetail = {
  id: string;
  accountCode: string;
  accountName: string;
  year: number;
  month: number;
  quarter: number | null;
  status: string;
  note: string;
  dateUpdated: string | null;
  expectedStationCount: number;
  mismatchStationCount: number;
  hasScheduleMismatch: boolean;
  stations: StationItem[];
  isLocalDraft?: boolean;
  isGeneratedPreview?: boolean;
  proposalState?: ChecklistProposalState | null;
  proposalLabel?: string | null;
};

type ChecklistMismatchItem = {
  stationRowId: number;
  checklistId: string;
  accountCode: string;
  estNum: number;
  stationCode: string;
  status: string;
  reasonCode: string;
};

type LoadPayload = {
  periods: PeriodItem[];
  selectedPeriod: PeriodItem | null;
  checklists: ChecklistSummary[];
  selectedChecklistId: string | null;
  selectedChecklist: ChecklistDetail | null;
  mismatchStations: ChecklistMismatchItem[];
};

type LoadOptions = {
  policy: CachePolicy;
  periodValue?: string;
  checklistId?: string | null;
  includeSelectedDetail?: boolean;
  deferWhenDirty?: boolean;
  preserveLocalDrafts?: boolean;
};

type AddStationNoteModalSubmitPayload = {
  amount: number | null;
  note: string;
  files: File[];
};

type EditStationNoteModalSubmitPayload = {
  amount: number | null;
  note: string;
  files: File[];
  removedAttachmentIds: number[];
};

type AddChecklistStationModalSubmitPayload = {
  estNum: number;
  stationCode: string;
  status: string;
};

type AddChecklistAccountModalSubmitPayload = {
  accountCode: string;
  status: string;
  note: string;
};

type PendingPeriodSyncPreview = {
  action: "generate" | "update";
  periodValue: string;
  year: number;
  month: number;
  plannedChecklistsCount: number;
  plannedStationsCount: number;
  mismatchStationCount: number;
  plannedChecklists: PeriodSyncPlannedChecklist[];
  plannedStations: PeriodSyncPlannedStation[];
  mismatchStations: ChecklistMismatchItem[];
};

type PeriodSyncPlannedChecklist = {
  accountCode: string;
  year: number;
  month: number;
  accountName?: string;
};

type PeriodSyncPlannedStation = {
  checklistId: string | null;
  accountCode: string;
  estNum: number;
  stationCode: string;
  stationName?: string;
  mediaType?: string;
};

type InvoiceChecklistPageSnapshot = {
  loadedPeriodValue: string | null;
  selectedChecklistId: string | null;
  selectedStationId: number | null;
  checklists: ChecklistSummary[];
  selectedChecklist: ChecklistDetail | null;
  selectedChecklistBaseline: ChecklistDetail | null;
  localChecklistDetailsById: Record<string, ChecklistDetail>;
  localChecklistBaselinesById: Record<string, ChecklistDetail>;
  pendingPeriodSyncPreview: PendingPeriodSyncPreview | null;
  localRemovedScheduledStationsByChecklistId: Record<string, Record<string, PeriodSyncPlannedStation>>;
  draftSearch: string;
  appliedSearch: string;
  checklistStatusDraft: string;
  checklistNoteDraft: string;
  stationStatusDraftById: Record<number, string>;
  cacheStatus: CacheStatus | null;
};

type ChecklistCardSummaryOverride = {
  status: string;
  stationCount: number;
  matchedStationCount: number;
};

type BeforeRouteChangeEventDetail = {
  from: string;
  to: string;
  reason: "push" | "pop";
  proceed: () => void;
};

type PendingUnsavedActionType = "load" | "route";

const LOAD_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.MAIN_LOAD;
const INVOICE_CHECKLIST_PAGE_STATE_CODE = "invoice-checklists";
const PAGE_PERIOD_STATE_KEY = "periodSelectionValue";
const PAGE_PERIOD_CUSTOM_MONTH_STATE_KEY = "customPeriodMonth";
const PAGE_PERIOD_CUSTOM_YEAR_STATE_KEY = "customPeriodYear";
const CHECKLIST_STATUS_OPTIONS_BASE = [
  "Matched All",
  "No Applicable",
];
const CHECKLIST_STATUS_MATCHED_ALL = "Matched All";
const STATION_STATUS_OPTIONS_BASE = [
  "Matched",
  "Matched/PreEmpt",
  "Matched/Buyin",
  "Matched/Pre-Buy",
  "Matched/ManualInv",
  "Matched/BLInv",
  "Matched/InvIssue",
  "Matched/CreditInv",
  "Matched/CalendarBIll",
  "Matched/BroadcastBill",
  "Discrepancy",
  "Different $$",
  "-",
  "Not Received",
  "Wait for Inv",
  "REG. Calendar Inv",
  "BRO. Calendar Inv",
  "Emailing",
  "-",
  "See Note",
];
const PREVIOUS_MONTH_ANCHOR = shiftMonthAnchor(new Date(), -1);
const DEFAULT_PREVIOUS_PERIOD_VALUE = formatPeriodValue(
  PREVIOUS_MONTH_ANCHOR.getFullYear(),
  PREVIOUS_MONTH_ANCHOR.getMonth() + 1,
);
const CUSTOM_PERIOD_OPTION_VALUE = "custom";
const IMAGE_ATTACHMENT_PATTERN = /\.(png|jpe?g|gif|bmp|webp|svg|heic|heif)$/i;
const DEFERRED_REFRESH_MESSAGE = "Latest checklist data loaded in the background. Save or discard local changes to apply it.";
const STATION_NOTES_RECENT_LIMIT = 10;

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

function normalizeProposalState(value: unknown): ChecklistProposalState | null {
  const raw = asString(value);
  if (raw === "proposed_add" || raw === "changed") {
    return raw;
  }
  if (raw === "proposed_remove" || raw === "proposed_delete") {
    return "proposed_remove";
  }
  return null;
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

function isChecklistMissingError(value: unknown): boolean {
  if (!(value instanceof Error)) {
    return false;
  }
  return /^Checklist not found:/i.test(asString(value.message));
}

function normalizeSearchKeyword(value: string): string {
  return asString(value).toLowerCase();
}

function accountMatchesSearch(checklist: ChecklistSummary | ChecklistDetail, keyword: string): boolean {
  if (!keyword) {
    return true;
  }
  const accountCode = asString(checklist.accountCode).toLowerCase();
  const accountName = asString(checklist.accountName).toLowerCase();
  return accountCode.includes(keyword) || accountName.includes(keyword);
}

function stationMatchesSearch(
  station: Pick<StationItem, "estNum" | "stationCode"> | ChecklistSearchStation,
  keyword: string,
): boolean {
  if (!keyword) {
    return true;
  }
  const estNumText = String(station.estNum);
  const stationCode = asString(station.stationCode).toLowerCase();
  return estNumText.includes(keyword) || stationCode.includes(keyword);
}

function filterChecklistsByKeyword(checklistRows: ChecklistSummary[], keyword: string): ChecklistSummary[] {
  if (!keyword) {
    return checklistRows;
  }
  return checklistRows.filter((item) => (
    accountMatchesSearch(item, keyword)
    || item.searchStations.some((station) => stationMatchesSearch(station, keyword))
  ));
}

function filterStationsForChecklist(checklist: ChecklistDetail | null, keyword: string): StationItem[] {
  if (!checklist) {
    return [];
  }
  if (!keyword || accountMatchesSearch(checklist, keyword)) {
    return checklist.stations;
  }
  return checklist.stations.filter((station) => stationMatchesSearch(station, keyword));
}

function formatDollar(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(value);
}

function formatOptionalDollar(value: number | null): string {
  if (value === null) {
    return "No amount";
  }
  return formatDollar(value);
}

function formatDateTime(value: string | null): string {
  const text = asString(value);
  if (!text) {
    return "-";
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    return text;
  }
  return parsed.toLocaleString();
}

function stationNoteSortTimestamp(note: NoteItem): number {
  const updatedAt = Date.parse(asString(note.dateUpdated));
  if (Number.isFinite(updatedAt)) {
    return updatedAt;
  }
  const createdAt = Date.parse(asString(note.dateCreated));
  if (Number.isFinite(createdAt)) {
    return createdAt;
  }
  return Number.NEGATIVE_INFINITY;
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

function monthLabel(month: number): string {
  return formatPeriodMonthName(month);
}

async function copyTextToClipboard(value: string): Promise<void> {
  if (!value) {
    throw new Error("Copy value is empty");
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

async function openAttachmentUrlWithHeaders(url: string, headers: HeadersInit): Promise<void> {
  const targetUrl = asString(url);
  if (!targetUrl) {
    throw new Error("Attachment URL is missing");
  }
  if (targetUrl.startsWith("blob:") || targetUrl.startsWith("data:")) {
    window.open(targetUrl, "_blank", "noopener,noreferrer");
    return;
  }
  if (/^https?:\/\//i.test(targetUrl)) {
    window.open(targetUrl, "_blank", "noopener,noreferrer");
    return;
  }

  const response = await fetch(targetUrl, {
    method: "GET",
    headers,
  });
  if (!response.ok) {
    throw new Error("Unable to open attachment");
  }
  const blob = await response.blob();
  const objectUrl = URL.createObjectURL(blob);
  window.open(objectUrl, "_blank", "noopener,noreferrer");
  window.setTimeout(() => URL.revokeObjectURL(objectUrl), 60_000);
}

function buildStatusOptions(baseValues: string[], currentValue: string, draftValue: string): string[] {
  const values = [""].concat(baseValues);
  if (currentValue && !values.includes(currentValue)) {
    values.push(currentValue);
  }
  if (draftValue && !values.includes(draftValue)) {
    values.push(draftValue);
  }
  return values;
}

function resolveChecklistStatusByStationStatuses(
  stations: StationItem[],
  stationStatusDraftById: Record<number, string>,
): string {
  if (stations.length === 0) {
    return "";
  }
  const everyStationMatched = stations.every((station) => {
    const effectiveStatus = asString(stationStatusDraftById[station.id] ?? station.status ?? "");
    return effectiveStatus.toLowerCase().startsWith("matched");
  });
  return everyStationMatched ? CHECKLIST_STATUS_MATCHED_ALL : "";
}

function buildLoadCacheKey(periodValue: string): string {
  const safe = asString(periodValue) || "latest";
  return `invoice-checklists:load:${safe}:v3`;
}

function buildChecklistDetailCacheKey(periodValue: string, checklistId: string): string {
  const safePeriod = asString(periodValue) || "latest";
  const safeChecklistId = asString(checklistId) || "unknown";
  return `invoice-checklists:detail:${safePeriod}:${safeChecklistId}:v2`;
}

function buildStationMatchedNotesCacheKey(estNum: number, stationCode: string): string {
  const safeEstNum = Number.isFinite(estNum) ? Math.trunc(estNum) : 0;
  const safeStationCode = asString(stationCode).toUpperCase() || "unknown";
  return `invoice-checklists:station-notes:${safeEstNum}:${safeStationCode}:v1`;
}

function buildStationContactsHydrationKey(checklistId: string, stationId: number, stationCode: string): string {
  const safeChecklistId = asString(checklistId) || "unknown";
  const safeStationId = Number.isFinite(stationId) ? Math.trunc(stationId) : 0;
  const safeStationCode = asString(stationCode).toUpperCase() || "unknown";
  return `${safeChecklistId}:${safeStationId}:${safeStationCode}`;
}

function buildPreviewStationId(checklistId: string, estNum: number, stationCode: string): number {
  const base = `${checklistId}:${Math.trunc(estNum)}:${asString(stationCode).toUpperCase()}`;
  let hash = 0;
  for (let index = 0; index < base.length; index += 1) {
    hash = ((hash << 5) - hash + base.charCodeAt(index)) | 0;
  }
  const positive = Math.abs(hash) + 1;
  return -(900_000_000 + positive);
}

function isLocalChecklistId(checklistId: string | null | undefined): boolean {
  const value = asString(checklistId);
  if (!value) {
    return false;
  }
  return value.startsWith("local-checklist-") || value.startsWith("preview-checklist-");
}

function toFallbackChecklistDetail(summary: ChecklistSummary): ChecklistDetail {
  return {
    id: summary.id,
    accountCode: summary.accountCode,
    accountName: summary.accountName,
    year: summary.year,
    month: summary.month,
    quarter: summary.quarter,
    status: summary.status,
    note: summary.note,
    dateUpdated: summary.dateUpdated,
    expectedStationCount: summary.expectedStationCount,
    mismatchStationCount: summary.mismatchStationCount,
    hasScheduleMismatch: summary.hasScheduleMismatch,
    stations: [],
  };
}

function toPeriodItem(value: unknown): PeriodItem | null {
  if (!isRecord(value)) {
    return null;
  }
  const year = asNumber(value.year);
  const month = asNumber(value.month);
  const quarter = asNumber(value.quarter);
  const periodValue = asString(value.value);
  const label = asString(value.label);
  if (year === null || month === null || !periodValue) {
    return null;
  }
  const normalized = buildPeriodValue(Math.trunc(year), Math.trunc(month));
  return {
    year: normalized?.year ?? Math.trunc(year),
    month: normalized?.month ?? Math.trunc(month),
    quarter: quarter === null ? normalized?.quarter ?? null : Math.trunc(quarter),
    value: normalized?.value ?? periodValue,
    label: label || normalized?.label || `${monthLabel(Math.trunc(month))} ${Math.trunc(year)}`,
  };
}

function toAttachment(value: unknown): AttachmentItem | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = asNumber(value.id);
  const noteId = asNumber(value.noteId);
  if (id === null || noteId === null) {
    return null;
  }
  return {
    id: Math.trunc(id),
    noteId: Math.trunc(noteId),
    url: asString(value.url),
    fileName: asString(value.fileName),
    fileType: asString(value.fileType),
    mimeType: asString(value.mimeType),
    fileSize: asNumber(value.fileSize),
    storageProvider: asString(value.storageProvider),
    providerAssetId: asString(value.providerAssetId),
    providerPublicId: asString(value.providerPublicId),
    providerResourceType: asString(value.providerResourceType),
    accessUrl: asString(value.accessUrl),
    uploadedBy: asString(value.uploadedBy),
    tenantSlug: asString(value.tenantSlug),
    ownerEntityType: asString(value.ownerEntityType),
    ownerEntityId: asString(value.ownerEntityId),
    dateCreated: asString(value.dateCreated) || null,
    dateUpdated: asString(value.dateUpdated) || null,
  };
}

function toNote(value: unknown): NoteItem | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = asNumber(value.id);
  const checklistStationId = asNumber(value.checklistStationId);
  const amount = value.amount === null || typeof value.amount === "undefined"
    ? null
    : asNumber(value.amount);
  if (id === null || checklistStationId === null) {
    return null;
  }
  if (value.amount !== null && typeof value.amount !== "undefined" && amount === null) {
    return null;
  }
  const attachmentsRaw = Array.isArray(value.attachments) ? value.attachments : [];
  const attachments = attachmentsRaw.map(toAttachment).filter((item): item is AttachmentItem => item !== null);
  const checklistYearRaw = asNumber(value.checklistYear);
  const checklistMonthRaw = asNumber(value.checklistMonth);
  return {
    id: Math.trunc(id),
    checklistStationId: Math.trunc(checklistStationId),
    checklistYear: checklistYearRaw === null ? null : Math.trunc(checklistYearRaw),
    checklistMonth: checklistMonthRaw === null ? null : Math.trunc(checklistMonthRaw),
    amount: amount === null ? null : amount,
    note: asString(value.note),
    dateCreated: asString(value.dateCreated) || null,
    dateUpdated: asString(value.dateUpdated) || null,
    attachments,
  };
}

function toRepContact(value: unknown): RepContactItem | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = asNumber(value.id);
  return {
    id: id === null ? null : Math.trunc(id),
    fullName: asString(value.fullName),
    email: asString(value.email),
    office: asString(value.office),
    mobile: asString(value.mobile),
    jobTitle: asString(value.jobTitle),
    company: asString(value.company),
    primaryContact: Boolean(value.primaryContact),
  };
}

function toBooleanLike(value: unknown): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    return value !== 0;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "y";
  }
  return false;
}

function buildRepContactFullName(value: Record<string, unknown>): string {
  const explicit = asString(value.fullName || value.name);
  if (explicit) {
    return explicit;
  }
  const firstName = asString(value.firstName);
  const lastName = asString(value.lastName);
  return `${firstName} ${lastName}`.trim();
}

function toRepContactFromStationContactsValue(value: unknown): RepContactItem | null {
  if (typeof value === "string") {
    const email = asString(value).toLowerCase();
    if (!email) {
      return null;
    }
    return {
      id: null,
      fullName: "",
      email,
      office: "",
      mobile: "",
      jobTitle: "",
      company: "",
      primaryContact: false,
    };
  }
  if (!isRecord(value)) {
    return null;
  }
  const id = asNumber(value.id);
  return {
    id: id === null ? null : Math.trunc(id),
    fullName: buildRepContactFullName(value),
    email: asString(value.email).toLowerCase(),
    office: asString(value.office),
    mobile: asString(value.cell || value.mobile),
    jobTitle: asString(value.jobTitle),
    company: asString(value.company),
    primaryContact: toBooleanLike(value.primaryContact),
  };
}

function extractRepContactsFromStationPayloadRow(value: unknown): RepContactItem[] {
  if (!isRecord(value)) {
    return [];
  }
  const explicitRepContactsRaw = Array.isArray(value.repContacts) ? value.repContacts : [];
  const explicitRepContacts = explicitRepContactsRaw
    .map(toRepContact)
    .filter((item): item is RepContactItem => item !== null);
  const contactsByType = isRecord(value.contacts) ? value.contacts : null;
  const repContactsFromGrouped = Array.isArray(contactsByType?.REP)
    ? contactsByType.REP
      .map(toRepContactFromStationContactsValue)
      .filter((item): item is RepContactItem => item !== null)
    : [];
  const source = explicitRepContacts.length > 0 ? explicitRepContacts : repContactsFromGrouped;

  const seen = new Set<string>();
  const output: RepContactItem[] = [];
  for (const contact of source) {
    const fingerprint = `${contact.id ?? ""}::${asString(contact.fullName).toLowerCase()}::${asString(contact.email).toLowerCase()}`;
    if (seen.has(fingerprint)) {
      continue;
    }
    seen.add(fingerprint);
    output.push(contact);
  }
  output.sort((left, right) => (
    Number(right.primaryContact) - Number(left.primaryContact)
    || `${left.fullName} ${left.email}`.localeCompare(`${right.fullName} ${right.email}`)
  ));
  return output;
}

function extractRepContactsFromStationModalDetail(result: StationModalDetailLoadResult): RepContactItem[] {
  const contactsRaw = Array.isArray(result.draft.contacts) ? result.draft.contacts : [];
  const output: RepContactItem[] = [];
  const seen = new Set<string>();
  for (const value of contactsRaw) {
    if (!isRecord(value)) {
      continue;
    }
    const contactType = asString(value.contactType).toUpperCase();
    if (contactType && contactType !== "REP") {
      continue;
    }
    const id = asNumber(value.contactId);
    const fullName = asString(value.fullName) || buildRepContactFullName(value);
    const email = asString(value.email).toLowerCase();
    const contact: RepContactItem = {
      id: id === null ? null : Math.trunc(id),
      fullName,
      email,
      office: asString(value.office),
      mobile: asString(value.cell),
      jobTitle: asString(value.jobTitle),
      company: asString(value.company),
      primaryContact: toBooleanLike(value.primaryContact),
    };
    const fingerprint = `${contact.id ?? ""}::${asString(contact.fullName).toLowerCase()}::${asString(contact.email).toLowerCase()}`;
    if (seen.has(fingerprint)) {
      continue;
    }
    seen.add(fingerprint);
    output.push(contact);
  }
  output.sort((left, right) => (
    Number(right.primaryContact) - Number(left.primaryContact)
    || `${left.fullName} ${left.email}`.localeCompare(`${right.fullName} ${right.email}`)
  ));
  return output;
}

function applyRepContactsToChecklistStationsByCode(
  checklist: ChecklistDetail,
  stationCode: string,
  repContacts: RepContactItem[],
): { checklist: ChecklistDetail; changed: boolean } {
  const normalizedStationCode = asString(stationCode).toUpperCase();
  if (!normalizedStationCode) {
    return { checklist, changed: false };
  }
  let changed = false;
  const nextStations = checklist.stations.map((station) => {
    if (asString(station.stationCode).toUpperCase() !== normalizedStationCode) {
      return station;
    }
    if (areRepContactsEqual(station.repContacts, repContacts)) {
      return station;
    }
    changed = true;
    return {
      ...station,
      repContacts: repContacts.map((contact) => ({ ...contact })),
    };
  });
  if (!changed) {
    return { checklist, changed: false };
  }
  return {
    checklist: {
      ...checklist,
      stations: nextStations,
    },
    changed: true,
  };
}

function areRepContactsEqual(left: RepContactItem[], right: RepContactItem[]): boolean {
  if (left.length !== right.length) {
    return false;
  }
  for (let index = 0; index < left.length; index += 1) {
    const current = left[index];
    const next = right[index];
    if (
      current.id !== next.id
      || current.fullName !== next.fullName
      || current.email !== next.email
      || current.office !== next.office
      || current.mobile !== next.mobile
      || current.jobTitle !== next.jobTitle
      || current.company !== next.company
      || current.primaryContact !== next.primaryContact
    ) {
      return false;
    }
  }
  return true;
}

function toStation(value: unknown): StationItem | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = asNumber(value.id);
  const estNum = asNumber(value.estNum);
  if (id === null || estNum === null) {
    return null;
  }
  const notesRaw = Array.isArray(value.notes) ? value.notes : [];
  const repContactsRaw = Array.isArray(value.repContacts) ? value.repContacts : [];
  return {
    id: Math.trunc(id),
    checklistId: asString(value.checklistId),
    estNum: Math.trunc(estNum),
    stationCode: asString(value.stationCode).toUpperCase(),
    stationName: asString(value.stationName),
    mediaType: asString(value.mediaType).toUpperCase(),
    status: asString(value.status),
    dateUpdated: asString(value.dateUpdated) || null,
    inCurrentSchedule: Boolean(value.inCurrentSchedule),
    scheduleMismatch: Boolean(value.scheduleMismatch),
    scheduleMismatchReason: asString(value.scheduleMismatchReason) || null,
    notes: notesRaw.map(toNote).filter((item): item is NoteItem => item !== null),
    repContacts: repContactsRaw.map(toRepContact).filter((item): item is RepContactItem => item !== null),
    proposalState: normalizeProposalState(value.proposalState),
    proposalLabel: asString(value.proposalLabel) || null,
  };
}

function toChecklistSummary(value: unknown): ChecklistSummary | null {
  if (!isRecord(value)) {
    return null;
  }
  const year = asNumber(value.year);
  const month = asNumber(value.month);
  const quarter = asNumber(value.quarter);
  const stationCount = asNumber(value.stationCount);
  const matchedStationCount = asNumber(value.matchedStationCount);
  const expectedStationCount = asNumber(value.expectedStationCount);
  const mismatchStationCount = asNumber(value.mismatchStationCount);
  const searchStationsRaw = Array.isArray(value.searchStations) ? value.searchStations : [];
  const id = asString(value.id);
  if (!id || year === null || month === null) {
    return null;
  }
  const searchStations = searchStationsRaw.reduce<ChecklistSearchStation[]>((acc, station) => {
    if (!isRecord(station)) {
      return acc;
    }
    const estNum = asNumber(station.estNum);
    const stationCode = asString(station.stationCode).toUpperCase();
    if (estNum === null || !stationCode) {
      return acc;
    }
    acc.push({
      estNum: Math.trunc(estNum),
      stationCode,
    });
    return acc;
  }, []);
  return {
    id,
    accountCode: asString(value.accountCode).toUpperCase(),
    accountName: asString(value.accountName),
    year: Math.trunc(year),
    month: Math.trunc(month),
    quarter: quarter === null ? null : Math.trunc(quarter),
    status: asString(value.status),
    note: asString(value.note),
    stationCount: stationCount === null ? 0 : Math.trunc(stationCount),
    matchedStationCount: matchedStationCount === null ? 0 : Math.trunc(matchedStationCount),
    expectedStationCount: expectedStationCount === null ? 0 : Math.trunc(expectedStationCount),
    mismatchStationCount: mismatchStationCount === null ? 0 : Math.trunc(mismatchStationCount),
    hasScheduleMismatch: Boolean(value.hasScheduleMismatch),
    searchStations,
    dateUpdated: asString(value.dateUpdated) || null,
    proposalState: normalizeProposalState(value.proposalState),
    proposalLabel: asString(value.proposalLabel) || null,
  };
}

function toChecklistDetail(value: unknown): ChecklistDetail | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = asString(value.id);
  const year = asNumber(value.year);
  const month = asNumber(value.month);
  const quarter = asNumber(value.quarter);
  if (!id || year === null || month === null) {
    return null;
  }
  const stationsRaw = Array.isArray(value.stations) ? value.stations : [];
  return {
    id,
    accountCode: asString(value.accountCode).toUpperCase(),
    accountName: asString(value.accountName),
    year: Math.trunc(year),
    month: Math.trunc(month),
    quarter: quarter === null ? null : Math.trunc(quarter),
    status: asString(value.status),
    note: asString(value.note),
    dateUpdated: asString(value.dateUpdated) || null,
    expectedStationCount: Math.trunc(asNumber(value.expectedStationCount) || 0),
    mismatchStationCount: Math.trunc(asNumber(value.mismatchStationCount) || 0),
    hasScheduleMismatch: Boolean(value.hasScheduleMismatch),
    stations: stationsRaw.map(toStation).filter((item): item is StationItem => item !== null),
    proposalState: normalizeProposalState(value.proposalState),
    proposalLabel: asString(value.proposalLabel) || null,
  };
}

function toChecklistMismatch(value: unknown): ChecklistMismatchItem | null {
  if (!isRecord(value)) {
    return null;
  }
  const stationRowId = asNumber(value.stationRowId);
  const estNum = asNumber(value.estNum);
  if (stationRowId === null || estNum === null) {
    return null;
  }
  return {
    stationRowId: Math.trunc(stationRowId),
    checklistId: asString(value.checklistId),
    accountCode: asString(value.accountCode).toUpperCase(),
    estNum: Math.trunc(estNum),
    stationCode: asString(value.stationCode).toUpperCase(),
    status: asString(value.status),
    reasonCode: asString(value.reasonCode),
  };
}

function toPeriodSyncPlannedChecklist(value: unknown): PeriodSyncPlannedChecklist | null {
  if (!isRecord(value)) {
    return null;
  }
  const accountCode = asString(value.accountCode).toUpperCase();
  const year = asNumber(value.year);
  const month = asNumber(value.month);
  if (!accountCode || year === null || month === null) {
    return null;
  }
  return {
    accountCode,
    year: Math.trunc(year),
    month: Math.trunc(month),
    accountName: asString(value.accountName),
  };
}

function toPeriodSyncPlannedStation(value: unknown): PeriodSyncPlannedStation | null {
  if (!isRecord(value)) {
    return null;
  }
  const accountCode = asString(value.accountCode).toUpperCase();
  const estNum = asNumber(value.estNum);
  const stationCode = asString(value.stationCode).toUpperCase();
  if (!accountCode || estNum === null || !stationCode) {
    return null;
  }
  const checklistIdText = asString(value.checklistId) || null;
  return {
    checklistId: checklistIdText,
    accountCode,
    estNum: Math.trunc(estNum),
    stationCode,
    stationName: asString(value.stationName),
    mediaType: asString(value.mediaType),
  };
}

function toPendingPeriodSyncPreview(value: unknown): PendingPeriodSyncPreview | null {
  if (!isRecord(value)) {
    return null;
  }
  const action = asString(value.action).toLowerCase() === "generate" ? "generate" : "update";
  const periodValueRaw = asString(value.periodValue);
  const periodValue = parsePeriodInput(periodValueRaw)?.value || "";
  const year = asNumber(value.year);
  const month = asNumber(value.month);
  if (!periodValue || year === null || month === null) {
    return null;
  }
  const plannedChecklistsRaw = Array.isArray(value.plannedChecklists) ? value.plannedChecklists : [];
  const plannedStationsRaw = Array.isArray(value.plannedStations) ? value.plannedStations : [];
  const mismatchStationsRaw = Array.isArray(value.mismatchStations) ? value.mismatchStations : [];
  const plannedChecklists = plannedChecklistsRaw
    .map(toPeriodSyncPlannedChecklist)
    .filter((item): item is PeriodSyncPlannedChecklist => item !== null);
  const plannedStations = plannedStationsRaw
    .map(toPeriodSyncPlannedStation)
    .filter((item): item is PeriodSyncPlannedStation => item !== null);
  const mismatchStations = mismatchStationsRaw
    .map(toChecklistMismatch)
    .filter((item): item is ChecklistMismatchItem => item !== null);
  const plannedChecklistsCount = Math.trunc(
    asNumber(value.plannedChecklistsCount) ?? asNumber(value.createdChecklistsCount) ?? plannedChecklists.length,
  );
  const plannedStationsCount = Math.trunc(
    asNumber(value.plannedStationsCount) ?? asNumber(value.createdStationsCount) ?? plannedStations.length,
  );
  const mismatchStationCount = Math.trunc(
    asNumber(value.mismatchStationCount) ?? mismatchStations.length,
  );
  return {
    action,
    periodValue,
    year: Math.trunc(year),
    month: Math.trunc(month),
    plannedChecklistsCount,
    plannedStationsCount,
    mismatchStationCount,
    plannedChecklists,
    plannedStations,
    mismatchStations,
  };
}

function normalizeLoadPayload(payload: unknown): LoadPayload {
  const data = unwrapData(payload);
  if (!isRecord(data)) {
    return {
      periods: [],
      selectedPeriod: null,
      checklists: [],
      selectedChecklistId: null,
      selectedChecklist: null,
      mismatchStations: [],
    };
  }

  const periodsRaw = Array.isArray(data.periods) ? data.periods : [];
  const checklistsRaw = Array.isArray(data.checklists) ? data.checklists : [];
  const mismatchStationsRaw = Array.isArray(data.mismatchStations) ? data.mismatchStations : [];
  const selectedPeriod = toPeriodItem(data.selectedPeriod);
  const selectedChecklist = toChecklistDetail(data.selectedChecklist);
  const selectedChecklistId = asString(data.selectedChecklistId) || selectedChecklist?.id || null;

  return {
    periods: periodsRaw.map(toPeriodItem).filter((item): item is PeriodItem => item !== null),
    selectedPeriod,
    checklists: checklistsRaw.map(toChecklistSummary).filter((item): item is ChecklistSummary => item !== null),
    selectedChecklistId,
    selectedChecklist,
    mismatchStations: mismatchStationsRaw.map(toChecklistMismatch).filter((item): item is ChecklistMismatchItem => item !== null),
  };
}

function cloneChecklistDetail(detail: ChecklistDetail | null): ChecklistDetail | null {
  if (!detail) {
    return null;
  }
  return {
    ...detail,
    stations: detail.stations.map((station) => ({
      ...station,
      notes: station.notes.map((note) => ({
        ...note,
        attachments: note.attachments.map((attachment) => ({ ...attachment })),
      })),
      repContacts: station.repContacts.map((contact) => ({ ...contact })),
    })),
  };
}

function mergeIncomingChecklistsWithLocalDrafts(
  incoming: ChecklistSummary[],
  current: ChecklistSummary[],
): ChecklistSummary[] {
  const localDraftRows = current.filter((item) => item.isLocalDraft === true);
  if (localDraftRows.length === 0) {
    return incoming;
  }
  const incomingIdSet = new Set(incoming.map((item) => item.id));
  const merged = incoming.concat(localDraftRows.filter((item) => !incomingIdSet.has(item.id)));
  return merged.sort((left, right) => left.accountCode.localeCompare(right.accountCode));
}

function mergeSavedChecklistSummaryWithBaseline(
  incoming: ChecklistSummary,
  baseline: ChecklistSummary | null,
): ChecklistSummary {
  if (!baseline) {
    return incoming;
  }
  return {
    ...incoming,
    accountName: asString(incoming.accountName) || asString(baseline.accountName),
    searchStations: incoming.searchStations.length > 0 ? incoming.searchStations : baseline.searchStations,
  };
}

function mergeSavedChecklistDetailWithBaseline(
  incoming: ChecklistDetail | null,
  baseline: ChecklistDetail | null,
): ChecklistDetail | null {
  if (!incoming) {
    return baseline ? cloneChecklistDetail(baseline) : null;
  }
  if (!baseline) {
    return incoming;
  }

  const baselineStationById = new Map<number, StationItem>();
  const baselineStationByKey = new Map<string, StationItem>();
  for (const station of baseline.stations) {
    baselineStationById.set(station.id, station);
    const key = `${Math.trunc(station.estNum)}::${asString(station.stationCode).toUpperCase()}`;
    baselineStationByKey.set(key, station);
  }

  const mergedStations = incoming.stations.map((station) => {
    const baselineStation = baselineStationById.get(station.id)
      ?? baselineStationByKey.get(`${Math.trunc(station.estNum)}::${asString(station.stationCode).toUpperCase()}`)
      ?? null;
    if (!baselineStation) {
      return station;
    }
    return {
      ...station,
      stationName: asString(station.stationName) || asString(baselineStation.stationName),
      mediaType: asString(station.mediaType) || asString(baselineStation.mediaType),
    };
  });

  return {
    ...incoming,
    accountName: asString(incoming.accountName) || asString(baseline.accountName),
    stations: mergedStations,
  };
}

type ChecklistNameFallbackMaps = {
  accountNameByCode: Map<string, string>;
  stationMetaByIdentity: Map<string, { stationName: string; mediaType: string }>;
  stationMetaByCode: Map<string, { stationName: string; mediaType: string }>;
};

function buildStationIdentityKey(accountCode: string, estNum: number, stationCode: string): string {
  return `${asString(accountCode).toUpperCase()}::${Math.trunc(estNum)}::${asString(stationCode).toUpperCase()}`;
}

function buildChecklistNameFallbackMaps(
  summaries: ChecklistSummary[],
  details: ChecklistDetail[],
): ChecklistNameFallbackMaps {
  const accountNameByCode = new Map<string, string>();
  const stationMetaByIdentity = new Map<string, { stationName: string; mediaType: string }>();
  const stationMetaByCode = new Map<string, { stationName: string; mediaType: string }>();

  for (const summary of summaries) {
    const accountCode = asString(summary.accountCode).toUpperCase();
    const accountName = asString(summary.accountName);
    if (!accountCode || !accountName || accountNameByCode.get(accountCode)) {
      continue;
    }
    accountNameByCode.set(accountCode, accountName);
  }

  for (const detail of details) {
    const accountCode = asString(detail.accountCode).toUpperCase();
    const accountName = asString(detail.accountName);
    if (accountCode && accountName && !accountNameByCode.get(accountCode)) {
      accountNameByCode.set(accountCode, accountName);
    }

    for (const station of detail.stations) {
      const stationCode = asString(station.stationCode).toUpperCase();
      if (!stationCode) {
        continue;
      }
      const stationName = asString(station.stationName);
      const mediaType = asString(station.mediaType);
      if (accountCode && Number.isFinite(station.estNum) && (stationName || mediaType)) {
        const identityKey = buildStationIdentityKey(accountCode, station.estNum, stationCode);
        if (!stationMetaByIdentity.get(identityKey)) {
          stationMetaByIdentity.set(identityKey, { stationName, mediaType });
        }
      }
      if ((stationName || mediaType) && !stationMetaByCode.get(stationCode)) {
        stationMetaByCode.set(stationCode, { stationName, mediaType });
      }
    }
  }

  return { accountNameByCode, stationMetaByIdentity, stationMetaByCode };
}

function applyLoadPayloadNameFallbacks(
  payload: LoadPayload,
  fallbackMaps: ChecklistNameFallbackMaps,
): LoadPayload {
  const checklists = payload.checklists.map((item) => {
    const accountCode = asString(item.accountCode).toUpperCase();
    return {
      ...item,
      accountName: asString(item.accountName) || fallbackMaps.accountNameByCode.get(accountCode) || "",
    };
  });

  const selectedChecklist = payload.selectedChecklist
    ? (() => {
      const detail = payload.selectedChecklist as ChecklistDetail;
      const accountCode = asString(detail.accountCode).toUpperCase();
      const stations = detail.stations.map((station) => {
        const stationCode = asString(station.stationCode).toUpperCase();
        const identityKey = buildStationIdentityKey(accountCode, station.estNum, stationCode);
        const fallbackMeta = fallbackMaps.stationMetaByIdentity.get(identityKey)
          ?? fallbackMaps.stationMetaByCode.get(stationCode)
          ?? null;
        return {
          ...station,
          stationName: asString(station.stationName) || asString(fallbackMeta?.stationName),
          mediaType: asString(station.mediaType) || asString(fallbackMeta?.mediaType),
        };
      });
      return {
        ...detail,
        accountName: asString(detail.accountName) || fallbackMaps.accountNameByCode.get(accountCode) || "",
        stations,
      };
    })()
    : null;

  return {
    ...payload,
    checklists,
    selectedChecklist,
  };
}

function deriveMismatchMetaFromStations(stations: StationItem[]): {
  mismatchStationCount: number;
  hasScheduleMismatch: boolean;
} {
  const mismatchStationCount = stations.reduce((count, station) => {
    const hasMismatch = station.scheduleMismatch || Boolean(asString(station.scheduleMismatchReason));
    return hasMismatch ? count + 1 : count;
  }, 0);
  return {
    mismatchStationCount,
    hasScheduleMismatch: mismatchStationCount > 0,
  };
}

function applyChecklistCardSummaryOverrides(
  rows: ChecklistSummary[],
  overridesById: Record<string, ChecklistCardSummaryOverride>,
): ChecklistSummary[] {
  if (!rows.length) {
    return rows;
  }
  return rows.map((row) => {
    const override = overridesById[row.id];
    if (!override) {
      return row;
    }
    return {
      ...row,
      status: override.status,
      stationCount: override.stationCount,
      matchedStationCount: override.matchedStationCount,
    };
  });
}

function buildChecklistCardSummaryOverride(
  checklist: ChecklistDetail,
  stationStatusDraftById: Record<number, string>,
): ChecklistCardSummaryOverride {
  const stations = checklist.stations ?? [];
  const stationCount = Math.max(0, stations.length);
  const matchedStationCountRaw = stations.reduce((acc, station) => {
    const effectiveStatus = asString(stationStatusDraftById[station.id] ?? station.status ?? "").toLowerCase();
    return acc + (effectiveStatus.startsWith("matched") ? 1 : 0);
  }, 0);
  const matchedStationCount = Math.max(0, Math.min(stationCount, matchedStationCountRaw));
  const resolvedStatus = resolveChecklistStatusByStationStatuses(stations, stationStatusDraftById);
  return {
    status: resolvedStatus === CHECKLIST_STATUS_MATCHED_ALL ? CHECKLIST_STATUS_MATCHED_ALL : "",
    stationCount,
    matchedStationCount,
  };
}

function normalizePeriodSelectionValue(rawValue: string): string {
  const text = asString(rawValue);
  if (!text) {
    return DEFAULT_PREVIOUS_PERIOD_VALUE;
  }
  if (text === CUSTOM_PERIOD_OPTION_VALUE) {
    return CUSTOM_PERIOD_OPTION_VALUE;
  }
  return parsePeriodInput(text)?.value || DEFAULT_PREVIOUS_PERIOD_VALUE;
}

function normalizeCustomMonthValue(rawValue: string): string {
  const parsed = Number(asString(rawValue));
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 12) {
    return String(PREVIOUS_MONTH_ANCHOR.getMonth() + 1);
  }
  return String(parsed);
}

function normalizeCustomYearValue(rawValue: string): string {
  const parsed = Number(asString(rawValue));
  if (!Number.isInteger(parsed) || parsed < 1900 || parsed > 9999) {
    return String(PREVIOUS_MONTH_ANCHOR.getFullYear());
  }
  return String(parsed);
}

function normalizePageSnapshot(value: unknown): InvoiceChecklistPageSnapshot | null {
  if (!isRecord(value)) {
    return null;
  }

  const loadedPeriodValueRaw = asString(value.loadedPeriodValue);
  const loadedPeriodValue = loadedPeriodValueRaw ? (parsePeriodInput(loadedPeriodValueRaw)?.value || null) : null;
  const selectedChecklistId = asString(value.selectedChecklistId) || null;
  const selectedStationIdValue = asNumber(value.selectedStationId);
  const selectedStationId = selectedStationIdValue === null ? null : Math.trunc(selectedStationIdValue);
  const checklistsRaw = Array.isArray(value.checklists) ? value.checklists : [];
  const selectedChecklist = toChecklistDetail(value.selectedChecklist);
  const selectedChecklistBaseline = toChecklistDetail(value.selectedChecklistBaseline)
    ?? cloneChecklistDetail(selectedChecklist);
  const localChecklistDetailsRaw = isRecord(value.localChecklistDetailsById) ? value.localChecklistDetailsById : {};
  const localChecklistDetailsById = Object.entries(localChecklistDetailsRaw).reduce<Record<string, ChecklistDetail>>(
    (acc, [checklistIdRaw, detailRaw]) => {
      const checklistId = asString(checklistIdRaw);
      const detail = toChecklistDetail(detailRaw);
      if (!checklistId || !detail) {
        return acc;
      }
      acc[checklistId] = detail;
      return acc;
    },
    {},
  );
  const localChecklistBaselinesRaw = isRecord(value.localChecklistBaselinesById) ? value.localChecklistBaselinesById : {};
  const localChecklistBaselinesById = Object.entries(localChecklistBaselinesRaw).reduce<Record<string, ChecklistDetail>>(
    (acc, [checklistIdRaw, detailRaw]) => {
      const checklistId = asString(checklistIdRaw);
      const detail = toChecklistDetail(detailRaw);
      if (!checklistId || !detail) {
        return acc;
      }
      acc[checklistId] = detail;
      return acc;
    },
    {},
  );
  const pendingPeriodSyncPreview = toPendingPeriodSyncPreview(value.pendingPeriodSyncPreview);
  const localRemovedScheduledRaw = isRecord(value.localRemovedScheduledStationsByChecklistId)
    ? value.localRemovedScheduledStationsByChecklistId
    : {};
  const localRemovedScheduledStationsByChecklistId = Object.entries(localRemovedScheduledRaw)
    .reduce<Record<string, Record<string, PeriodSyncPlannedStation>>>((acc, [checklistIdRaw, byIdentityRaw]) => {
      const checklistId = asString(checklistIdRaw);
      if (!checklistId || !isRecord(byIdentityRaw)) {
        return acc;
      }
      const parsedByIdentity = Object.entries(byIdentityRaw).reduce<Record<string, PeriodSyncPlannedStation>>(
        (innerAcc, [identityKeyRaw, rowRaw]) => {
          const identityKey = asString(identityKeyRaw);
          const row = toPeriodSyncPlannedStation(rowRaw);
          if (!identityKey || !row) {
            return innerAcc;
          }
          innerAcc[identityKey] = row;
          return innerAcc;
        },
        {},
      );
      if (Object.keys(parsedByIdentity).length === 0) {
        return acc;
      }
      acc[checklistId] = parsedByIdentity;
      return acc;
    }, {});
  const draftSearch = asString(value.draftSearch);
  const appliedSearch = asString(value.appliedSearch);
  const checklistStatusDraft = asString(value.checklistStatusDraft) || selectedChecklist?.status || "";
  const checklistNoteDraft = asString(value.checklistNoteDraft) || selectedChecklist?.note || "";
  const stationStatusDraftByIdRaw = isRecord(value.stationStatusDraftById) ? value.stationStatusDraftById : {};
  const stationStatusDraftById = Object.entries(stationStatusDraftByIdRaw).reduce<Record<number, string>>(
    (acc, [rawKey, rawValue]) => {
      const stationId = Number(rawKey);
      if (!Number.isInteger(stationId) || stationId <= 0) {
        return acc;
      }
      const statusText = asString(rawValue);
      acc[stationId] = statusText;
      return acc;
    },
    {},
  );
  const cacheStatusRaw = value.cacheStatus;

  let cacheStatus: CacheStatus | null = null;
  if (isRecord(cacheStatusRaw)) {
    const sourceText = asString(cacheStatusRaw.source);
    const fetchedAt = asNumber(cacheStatusRaw.fetchedAt);
    if ((sourceText === "cache" || sourceText === "network") && fetchedAt !== null) {
      cacheStatus = { source: sourceText, fetchedAt: fetchedAt };
    }
  }

  return {
    loadedPeriodValue,
    selectedChecklistId,
    selectedStationId,
    checklists: checklistsRaw.map(toChecklistSummary).filter((item): item is ChecklistSummary => item !== null),
    selectedChecklist,
    selectedChecklistBaseline,
    localChecklistDetailsById,
    localChecklistBaselinesById,
    pendingPeriodSyncPreview,
    localRemovedScheduledStationsByChecklistId,
    draftSearch,
    appliedSearch,
    checklistStatusDraft,
    checklistNoteDraft,
    stationStatusDraftById,
    cacheStatus,
  };
}

export default function InvoiceChecklistPage() {
  const { requestJson } = useApiRequest();
  const auth = useAuth();
  const { isOnline } = useOnlineStatus();
  const toast = useToast();
  const [hasHydratedPageState, setHasHydratedPageState] = useState(false);

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
  const pageStateScope = useMemo(() => {
    const userKey = auth.user?.id || auth.user?.email || "anonymous";
    const tenantSlug = auth.tenantSlug || "default";
    return {
      userKey,
      tenantSlug,
      appCode: "tradsphere",
      pageCode: INVOICE_CHECKLIST_PAGE_STATE_CODE,
    };
  }, [auth.tenantSlug, auth.user?.email, auth.user?.id]);

  const [periodSelectionValue, setPeriodSelectionValue] = useScopedPersistentState<string>(
    {
      appCode: "tradsphere",
      pageCode: INVOICE_CHECKLIST_PAGE_STATE_CODE,
      stateKey: PAGE_PERIOD_STATE_KEY,
    },
    DEFAULT_PREVIOUS_PERIOD_VALUE,
    { validate: (value: unknown): value is string => typeof value === "string" && value.trim().length > 0 },
  );
  const [customPeriodMonth, setCustomPeriodMonth] = useScopedPersistentState<string>(
    {
      appCode: "tradsphere",
      pageCode: INVOICE_CHECKLIST_PAGE_STATE_CODE,
      stateKey: PAGE_PERIOD_CUSTOM_MONTH_STATE_KEY,
    },
    String(PREVIOUS_MONTH_ANCHOR.getMonth() + 1),
    { validate: (value: unknown): value is string => typeof value === "string" && value.trim().length > 0 },
  );
  const [customPeriodYear, setCustomPeriodYear] = useScopedPersistentState<string>(
    {
      appCode: "tradsphere",
      pageCode: INVOICE_CHECKLIST_PAGE_STATE_CODE,
      stateKey: PAGE_PERIOD_CUSTOM_YEAR_STATE_KEY,
    },
    String(PREVIOUS_MONTH_ANCHOR.getFullYear()),
    { validate: (value: unknown): value is string => typeof value === "string" && value.trim().length > 0 },
  );
  const [loadedPeriodValue, setLoadedPeriodValue] = useState<string | null>(null);
  const [checklists, setChecklists] = useState<ChecklistSummary[]>([]);
  const [selectedChecklist, setSelectedChecklist] = useState<ChecklistDetail | null>(null);
  const [selectedChecklistBaseline, setSelectedChecklistBaseline] = useState<ChecklistDetail | null>(null);
  const [localChecklistDetailsById, setLocalChecklistDetailsById] = useState<Record<string, ChecklistDetail>>({});
  const [localChecklistBaselinesById, setLocalChecklistBaselinesById] = useState<Record<string, ChecklistDetail>>({});
  const [localRemovedScheduledStationsByChecklistId, setLocalRemovedScheduledStationsByChecklistId] = useState<
    Record<string, Record<string, PeriodSyncPlannedStation>>
  >({});
  const [deletingChecklistIds, setDeletingChecklistIds] = useState<string[]>([]);
  const [selectedChecklistId, setSelectedChecklistId] = useState<string | null>(null);
  const [selectedStationId, setSelectedStationId] = useState<number | null>(null);
  const [stationMatchedExternalNoteItems, setStationMatchedExternalNoteItems] = useState<StationNoteViewItem[]>([]);
  const [isLoadingSelectedStationNotes, setIsLoadingSelectedStationNotes] = useState(false);
  const [isHydratingSelectedStationContacts, setIsHydratingSelectedStationContacts] = useState(false);
  const [stationPanelRefreshRequestId, setStationPanelRefreshRequestId] = useState(0);
  const [draftSearch, setDraftSearch] = useState("");
  const [appliedSearch, setAppliedSearch] = useState("");

  const [isLoading, setIsLoading] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isChipRefreshing, setIsChipRefreshing] = useState(false);
  const [isChecklistSelectionLoading, setIsChecklistSelectionLoading] = useState(false);
  const [isLoadActionOverlayVisible, setIsLoadActionOverlayVisible] = useState(false);
  const [isSyncingPeriod, setIsSyncingPeriod] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);
  const [pendingPeriodSyncPreview, setPendingPeriodSyncPreview] = useState<PendingPeriodSyncPreview | null>(null);

  const [checklistStatusDraft, setChecklistStatusDraft] = useState("");
  const [checklistNoteDraft, setChecklistNoteDraft] = useState("");
  const [stationStatusDraftById, setStationStatusDraftById] = useState<Record<number, string>>({});
  const [checklistCardSummaryOverridesById, setChecklistCardSummaryOverridesById] = useState<Record<string, ChecklistCardSummaryOverride>>({});
  const [isSavingAllChanges, setIsSavingAllChanges] = useState(false);
  const [isUnsavedDialogOpen, setIsUnsavedDialogOpen] = useState(false);
  const [attachmentPreview, setAttachmentPreview] = useState<{ src: string; name: string } | null>(null);
  const [attachmentModalNoteId, setAttachmentModalNoteId] = useState<number | null>(null);
  const [attachmentImageSrcById, setAttachmentImageSrcById] = useState<Record<number, string>>({});
  const [editingNoteId, setEditingNoteId] = useState<number | null>(null);
  const [deletingNoteId, setDeletingNoteId] = useState<number | null>(null);
  const [deletingStationId, setDeletingStationId] = useState<number | null>(null);
  const [deletingChecklistId, setDeletingChecklistId] = useState<string | null>(null);
  const pendingUnsavedActionTypeRef = useRef<PendingUnsavedActionType | null>(null);
  const pendingUnsavedRouteProceedRef = useRef<(() => void) | null>(null);
  const previewRemovedPlannedStationKeysRef = useRef<Set<string>>(new Set());
  const stationContactsHydrationAttemptedRef = useRef<Set<string>>(new Set());
  const handledStationNotesRefreshRequestIdRef = useRef(0);
  const handledStationContactsRefreshRequestIdRef = useRef(0);
  const selectionBeforeSearchRef = useRef<{ checklistId: string | null; stationId: number | null } | null>(null);
  const pendingStationRestoreAfterChecklistSelectRef = useRef<number | null>(null);
  const generatedAttachmentImageUrlsRef = useRef<string[]>([]);
  const nextLocalIdRef = useRef(-1);
  const [isAddNoteModalOpen, setIsAddNoteModalOpen] = useState(false);
  const [isAddStationModalOpen, setIsAddStationModalOpen] = useState(false);
  const [isAddChecklistAccountModalOpen, setIsAddChecklistAccountModalOpen] = useState(false);
  const [isStationCardModalOpen, setIsStationCardModalOpen] = useState(false);
  const [stationCardModalCode, setStationCardModalCode] = useState<string | null>(null);

  useEffect(() => {
    const restored = readScopedPageState<InvoiceChecklistPageSnapshot>(pageStateScope, (value): value is InvoiceChecklistPageSnapshot => {
      return normalizePageSnapshot(value) !== null;
    });
    const normalized = normalizePageSnapshot(restored);
    if (normalized) {
      setLoadedPeriodValue(normalized.loadedPeriodValue);
      setSelectedChecklistId(normalized.selectedChecklistId);
      setSelectedStationId(normalized.selectedStationId);
      setChecklists(normalized.checklists);
      setSelectedChecklist(normalized.selectedChecklist);
      setSelectedChecklistBaseline(normalized.selectedChecklistBaseline);
      setLocalChecklistDetailsById(normalized.localChecklistDetailsById);
      setLocalChecklistBaselinesById(normalized.localChecklistBaselinesById);
      setPendingPeriodSyncPreview(normalized.pendingPeriodSyncPreview);
      setLocalRemovedScheduledStationsByChecklistId(normalized.localRemovedScheduledStationsByChecklistId);
      setDraftSearch(normalized.draftSearch);
      setAppliedSearch(normalized.appliedSearch);
      setChecklistStatusDraft(normalized.checklistStatusDraft);
      setChecklistNoteDraft(normalized.checklistNoteDraft);
      setStationStatusDraftById(normalized.stationStatusDraftById);
      setCacheStatus(normalized.cacheStatus);
    }
    setHasHydratedPageState(true);
  }, [pageStateScope]);

  useEffect(() => {
    if (!hasHydratedPageState) {
      return;
    }
    const snapshot: InvoiceChecklistPageSnapshot = {
      loadedPeriodValue,
      selectedChecklistId,
      selectedStationId,
      checklists,
      selectedChecklist,
      selectedChecklistBaseline,
      localChecklistDetailsById,
      localChecklistBaselinesById,
      pendingPeriodSyncPreview,
      localRemovedScheduledStationsByChecklistId,
      draftSearch,
      appliedSearch,
      checklistStatusDraft,
      checklistNoteDraft,
      stationStatusDraftById,
      cacheStatus,
    };
    writeScopedPageState(pageStateScope, snapshot);
  }, [
    cacheStatus,
    draftSearch,
    appliedSearch,
    checklistStatusDraft,
    checklistNoteDraft,
    checklists,
    hasHydratedPageState,
    localChecklistBaselinesById,
    localChecklistDetailsById,
    localRemovedScheduledStationsByChecklistId,
    loadedPeriodValue,
    pageStateScope,
    pendingPeriodSyncPreview,
    selectedChecklist,
    selectedChecklistBaseline,
    selectedChecklistId,
    selectedStationId,
    stationStatusDraftById,
  ]);

  useEffect(() => {
    if (!selectedChecklist || !selectedChecklist.isLocalDraft) {
      return;
    }
    setLocalChecklistDetailsById((current) => ({
      ...current,
      [selectedChecklist.id]: cloneChecklistDetail(selectedChecklist) ?? selectedChecklist,
    }));
  }, [selectedChecklist]);

  useEffect(() => {
    stationContactsHydrationAttemptedRef.current.clear();
  }, [loadedPeriodValue]);

  useEffect(() => {
    const pendingStationId = pendingStationRestoreAfterChecklistSelectRef.current;
    if (pendingStationId === null || !selectedChecklist) {
      return;
    }
    pendingStationRestoreAfterChecklistSelectRef.current = null;
    if (!selectedChecklist.stations.some((station) => station.id === pendingStationId)) {
      return;
    }
    setSelectedStationId(pendingStationId);
  }, [selectedChecklist]);

  const normalizedAppliedSearch = useMemo(() => asString(appliedSearch).toLowerCase(), [appliedSearch]);

  const pendingPreviewStationAddsByChecklistId = useMemo(() => {
    const grouped = new Map<string, PeriodSyncPlannedStation[]>();
    if (!pendingPeriodSyncPreview) {
      return grouped;
    }
    for (const item of pendingPeriodSyncPreview.plannedStations) {
      if (!item.checklistId) {
        continue;
      }
      const rows = grouped.get(item.checklistId) ?? [];
      rows.push(item);
      grouped.set(item.checklistId, rows);
    }
    return grouped;
  }, [pendingPeriodSyncPreview]);

  const pendingPreviewStationDeleteIdsByChecklistId = useMemo(() => {
    const grouped = new Map<string, Set<number>>();
    if (!pendingPeriodSyncPreview) {
      return grouped;
    }
    for (const item of pendingPeriodSyncPreview.mismatchStations) {
      const checklistId = asString(item.checklistId);
      if (!checklistId || !Number.isFinite(item.stationRowId) || item.stationRowId <= 0) {
        continue;
      }
      const rows = grouped.get(checklistId) ?? new Set<number>();
      rows.add(Math.trunc(item.stationRowId));
      grouped.set(checklistId, rows);
    }
    for (const checklist of checklists) {
      const checklistId = asString(checklist.id);
      if (!checklistId) {
        continue;
      }
      const detail = (
        selectedChecklist?.id === checklistId
          ? selectedChecklist
          : localChecklistDetailsById[checklistId]
      ) ?? null;
      if (!detail) {
        continue;
      }
      const proposedAddRows = pendingPreviewStationAddsByChecklistId.get(checklistId) ?? [];
      const proposedAddKeySet = new Set<string>(
        proposedAddRows.map((row) => `${Math.trunc(row.estNum)}:${asString(row.stationCode).toUpperCase()}`),
      );
      for (const station of detail.stations) {
        if (station.isLocalDraft !== true) {
          continue;
        }
        const stationKey = `${Math.trunc(station.estNum)}:${asString(station.stationCode).toUpperCase()}`;
        const isProposedAdd = station.isGeneratedPreview === true || proposedAddKeySet.has(stationKey);
        if (isProposedAdd) {
          continue;
        }
        const rows = grouped.get(checklistId) ?? new Set<number>();
        rows.add(Math.trunc(station.id));
        grouped.set(checklistId, rows);
      }
    }
    return grouped;
  }, [checklists, localChecklistDetailsById, pendingPeriodSyncPreview, pendingPreviewStationAddsByChecklistId, selectedChecklist]);

  const checklistsForView = useMemo(() => {
    if (!pendingPeriodSyncPreview) {
      return checklists;
    }
    const addCountByChecklistId = new Map<string, number>();
    for (const [checklistId, rows] of pendingPreviewStationAddsByChecklistId.entries()) {
      addCountByChecklistId.set(checklistId, rows.length);
    }
    const deleteCountByChecklistId = new Map<string, number>();
    for (const [checklistId, rows] of pendingPreviewStationDeleteIdsByChecklistId.entries()) {
      deleteCountByChecklistId.set(checklistId, rows.size);
    }
    return checklists.map((item) => {
      if (item.isGeneratedPreview === true) {
        return {
          ...item,
          proposalState: "proposed_add" as const,
          proposalLabel: "Proposed add",
        };
      }
      const addCount = addCountByChecklistId.get(item.id) ?? 0;
      const deleteCount = deleteCountByChecklistId.get(item.id) ?? 0;
      if (addCount > 0 && deleteCount > 0) {
        return { ...item, proposalState: "changed" as const, proposalLabel: "Changed" };
      }
      if (deleteCount > 0) {
        const isFullDelete = item.expectedStationCount === 0 || (item.stationCount > 0 && deleteCount >= item.stationCount);
        return {
          ...item,
          proposalState: isFullDelete ? ("proposed_remove" as const) : ("changed" as const),
          proposalLabel: isFullDelete ? "Proposed remove" : "Changed",
        };
      }
      if (addCount > 0) {
        return { ...item, proposalState: "proposed_add" as const, proposalLabel: "Proposed add" };
      }
      return { ...item, proposalState: null, proposalLabel: null };
    });
  }, [checklists, pendingPeriodSyncPreview, pendingPreviewStationAddsByChecklistId, pendingPreviewStationDeleteIdsByChecklistId]);

  const filteredChecklists = useMemo(() => {
    return filterChecklistsByKeyword(checklistsForView, normalizedAppliedSearch);
  }, [checklistsForView, normalizedAppliedSearch]);

  const visibleChecklistIds = useMemo(
    () => new Set(filteredChecklists.map((item) => item.id)),
    [filteredChecklists],
  );

  const selectedChecklistForView = useMemo(() => {
    if (!selectedChecklist || !selectedChecklistId) {
      return null;
    }
    if (!visibleChecklistIds.has(selectedChecklistId)) {
      return null;
    }
    if (!pendingPeriodSyncPreview) {
      return selectedChecklist;
    }

    const proposedAddRows = pendingPreviewStationAddsByChecklistId.get(selectedChecklistId) ?? [];
    const proposedDeleteRows = pendingPreviewStationDeleteIdsByChecklistId.get(selectedChecklistId) ?? new Set<number>();
    const proposedAddKeySet = new Set<string>(
      proposedAddRows.map((row) => `${Math.trunc(row.estNum)}:${asString(row.stationCode).toUpperCase()}`),
    );
    const existingKeySet = new Set<string>();
    const stations = selectedChecklist.stations.map((station) => {
      const stationKey = `${Math.trunc(station.estNum)}:${asString(station.stationCode).toUpperCase()}`;
      existingKeySet.add(stationKey);
      if (!proposedDeleteRows.has(station.id)) {
        const isProposedAdd = station.isGeneratedPreview === true || proposedAddKeySet.has(stationKey);
        return {
          ...station,
          proposalState: isProposedAdd ? ("proposed_add" as const) : null,
          proposalLabel: isProposedAdd ? "Proposed add" : null,
        };
      }
      return {
        ...station,
        proposalState: "proposed_remove" as const,
        proposalLabel: "Proposed remove",
      };
    });

    for (const row of proposedAddRows) {
      const stationKey = `${Math.trunc(row.estNum)}:${asString(row.stationCode).toUpperCase()}`;
      if (existingKeySet.has(stationKey)) {
        continue;
      }
      stations.push({
        id: buildPreviewStationId(selectedChecklistId, row.estNum, row.stationCode),
        checklistId: selectedChecklistId,
        estNum: row.estNum,
        stationCode: row.stationCode,
        stationName: asString(row.stationName),
        mediaType: asString(row.mediaType),
        status: "",
        dateUpdated: null,
        inCurrentSchedule: true,
        scheduleMismatch: false,
        scheduleMismatchReason: null,
        repContacts: [],
        notes: [],
        isLocalDraft: true,
        isGeneratedPreview: true,
        proposalState: "proposed_add" as const,
        proposalLabel: "Proposed add",
      });
      existingKeySet.add(stationKey);
    }

    stations.sort((left, right) => (
      left.estNum - right.estNum || left.stationCode.localeCompare(right.stationCode)
    ));

    const hasAdd = proposedAddRows.length > 0;
    const hasDelete = proposedDeleteRows.size > 0;
    const isFullDelete = selectedChecklist.expectedStationCount === 0
      || (selectedChecklist.stations.length > 0 && proposedDeleteRows.size >= selectedChecklist.stations.length);
    const proposalState: ChecklistProposalState | null = hasAdd && hasDelete
      ? "changed"
      : hasDelete
        ? (isFullDelete ? "proposed_remove" : "changed")
        : hasAdd
          ? "proposed_add"
          : selectedChecklist.isGeneratedPreview
            ? "proposed_add"
            : null;
    const proposalLabel = proposalState === "proposed_add"
      ? "Proposed add"
      : proposalState === "proposed_remove"
        ? "Proposed remove"
        : proposalState === "changed"
          ? "Changed"
          : null;
    return {
      ...selectedChecklist,
      stations,
      proposalState,
      proposalLabel,
    };
  }, [
    pendingPeriodSyncPreview,
    pendingPreviewStationAddsByChecklistId,
    pendingPreviewStationDeleteIdsByChecklistId,
    selectedChecklist,
    selectedChecklistId,
    visibleChecklistIds,
  ]);

  const filteredStationsForSelectedChecklist = useMemo(
    () => filterStationsForChecklist(selectedChecklistForView, normalizedAppliedSearch),
    [normalizedAppliedSearch, selectedChecklistForView],
  );

  useEffect(() => {
    if (!selectedChecklist) {
      return;
    }
    const stations = selectedChecklist.stations ?? [];
    const nextChecklistStatus = resolveChecklistStatusByStationStatuses(stations, stationStatusDraftById);
    const targetStatus = nextChecklistStatus === CHECKLIST_STATUS_MATCHED_ALL
      ? CHECKLIST_STATUS_MATCHED_ALL
      : "";
    if (checklistStatusDraft === targetStatus) {
      return;
    }
    setChecklistStatusDraft(targetStatus);
  }, [checklistStatusDraft, selectedChecklist, stationStatusDraftById]);

  useEffect(() => {
    if (!selectedChecklist) {
      return;
    }
    const override = buildChecklistCardSummaryOverride(selectedChecklist, stationStatusDraftById);
    setChecklists((current) => {
      let changed = false;
      const next = current.map((item) => {
        if (item.id !== selectedChecklist.id) {
          return item;
        }
        if (
          asString(item.status) === asString(override.status)
          && Math.trunc(item.stationCount) === Math.trunc(override.stationCount)
          && Math.trunc(item.matchedStationCount ?? 0) === Math.trunc(override.matchedStationCount)
        ) {
          return item;
        }
        changed = true;
        return {
          ...item,
          status: override.status,
          stationCount: override.stationCount,
          matchedStationCount: override.matchedStationCount,
        };
      });
      return changed ? next : current;
    });
    setChecklistCardSummaryOverridesById((current) => {
      const previous = current[selectedChecklist.id];
      if (
        previous
        && asString(previous.status) === asString(override.status)
        && Math.trunc(previous.stationCount) === Math.trunc(override.stationCount)
        && Math.trunc(previous.matchedStationCount) === Math.trunc(override.matchedStationCount)
      ) {
        return current;
      }
      return {
        ...current,
        [selectedChecklist.id]: override,
      };
    });
  }, [selectedChecklist, stationStatusDraftById]);

  const hasDirtyChecklistStatus = useMemo(() => {
    if (!selectedChecklist) {
      return false;
    }
    return checklistStatusDraft !== (selectedChecklist.status || "");
  }, [checklistStatusDraft, selectedChecklist]);

  const hasDirtyChecklistNote = useMemo(() => {
    if (!selectedChecklist) {
      return false;
    }
    return checklistNoteDraft.trim() !== (selectedChecklist.note || "").trim();
  }, [checklistNoteDraft, selectedChecklist]);

  const dirtyStationStatusEntries = useMemo(() => {
    if (!selectedChecklist) {
      return [] as Array<{ stationId: number; status: string }>;
    }
    return selectedChecklist.stations.reduce<Array<{ stationId: number; status: string }>>((acc, station) => {
      if (!(station.id in stationStatusDraftById)) {
        return acc;
      }
      const draftStatus = stationStatusDraftById[station.id] ?? "";
      const baselineStatus = station.status || "";
      if (draftStatus === baselineStatus) {
        return acc;
      }
      acc.push({ stationId: station.id, status: draftStatus });
      return acc;
    }, []);
  }, [selectedChecklist, stationStatusDraftById]);

  const hasPendingLocalNotes = useMemo(() => {
    if (!selectedChecklist) {
      return false;
    }
    return selectedChecklist.stations.some((station) => station.notes.some((note) => note.isLocalDraft === true));
  }, [selectedChecklist]);

  const hasPendingLocalAttachments = useMemo(() => {
    if (!selectedChecklist) {
      return false;
    }
    return selectedChecklist.stations.some((station) => (
      station.notes.some((note) => note.attachments.some((attachment) => attachment.isLocalDraft === true))
    ));
  }, [selectedChecklist]);

  const hasPendingLocalStations = useMemo(() => {
    if (!selectedChecklist) {
      return false;
    }
    return selectedChecklist.stations.some((station) => station.isLocalDraft === true);
  }, [selectedChecklist]);

  const hasPendingLocalChecklists = useMemo(
    () => checklists.some((item) => item.isLocalDraft === true),
    [checklists],
  );

  const deletingChecklistIdSet = useMemo(() => new Set(deletingChecklistIds), [deletingChecklistIds]);
  const isDeletingChecklist = deletingChecklistIds.length > 0;
  const isDeletingSelectedChecklist = Boolean(selectedChecklistId && deletingChecklistIdSet.has(selectedChecklistId));

  const hasDirtyStationRemovals = useMemo(() => {
    if (!selectedChecklist || !selectedChecklistBaseline) {
      return false;
    }
    const currentStationIds = new Set(selectedChecklist.stations.map((station) => station.id));
    return selectedChecklistBaseline.stations.some((station) => !currentStationIds.has(station.id));
  }, [selectedChecklist, selectedChecklistBaseline]);

  const hasDirtyExistingNotes = useMemo(() => {
    if (!selectedChecklist || !selectedChecklistBaseline) {
      return false;
    }
      const baselineById = new Map<number, { amount: number | null; note: string }>();
    for (const station of selectedChecklistBaseline.stations) {
      for (const note of station.notes) {
        if (note.isLocalDraft) {
          continue;
        }
        baselineById.set(note.id, { amount: note.amount, note: note.note.trim() });
      }
    }

    const currentExistingIds = new Set<number>();
    for (const station of selectedChecklist.stations) {
      for (const note of station.notes) {
        if (note.isLocalDraft) {
          continue;
        }
        currentExistingIds.add(note.id);
        const baseline = baselineById.get(note.id);
        if (!baseline) {
          continue;
        }
        if (baseline.amount !== note.amount || baseline.note !== note.note.trim()) {
          return true;
        }
      }
    }

    for (const baselineId of baselineById.keys()) {
      if (!currentExistingIds.has(baselineId)) {
        return true;
      }
    }
    return false;
  }, [selectedChecklist, selectedChecklistBaseline]);

  const hasDirtyStationStatuses = dirtyStationStatusEntries.length > 0;
  const hasDirtyChecklistFields = hasDirtyChecklistStatus || hasDirtyChecklistNote;
  const hasUnsavedChanges = (
    hasPendingLocalChecklists
    || hasDirtyChecklistFields
    || hasDirtyStationStatuses
    || hasDirtyStationRemovals
    || hasPendingLocalStations
    || hasPendingLocalNotes
    || hasPendingLocalAttachments
    || hasDirtyExistingNotes
  );
  const {
    hasDeferredUpdate,
    beginRequest,
    isLatestRequest,
    applyFromRequest,
    clearDeferredUpdate,
  } = useDirtyRefreshGuard(hasUnsavedChanges);

  const normalizedPeriodSelectionValue = useMemo(
    () => normalizePeriodSelectionValue(periodSelectionValue),
    [periodSelectionValue],
  );
  const normalizedCustomMonth = useMemo(
    () => normalizeCustomMonthValue(customPeriodMonth),
    [customPeriodMonth],
  );
  const normalizedCustomYear = useMemo(
    () => normalizeCustomYearValue(customPeriodYear),
    [customPeriodYear],
  );

  const isCustomPeriodSelection = normalizedPeriodSelectionValue === CUSTOM_PERIOD_OPTION_VALUE;
  const customPeriod = useMemo(() => {
    if (!isCustomPeriodSelection) {
      return null;
    }
    return buildPeriodValue(Number(normalizedCustomYear), Number(normalizedCustomMonth));
  }, [isCustomPeriodSelection, normalizedCustomMonth, normalizedCustomYear]);

  const selectedDraftPeriodValue = useMemo(() => {
    if (isCustomPeriodSelection) {
      return customPeriod?.value || "";
    }
    return parsePeriodInput(normalizedPeriodSelectionValue)?.value || "";
  }, [customPeriod?.value, isCustomPeriodSelection, normalizedPeriodSelectionValue]);

  useEffect(() => {
    if (periodSelectionValue !== normalizedPeriodSelectionValue) {
      setPeriodSelectionValue(normalizedPeriodSelectionValue);
    }
  }, [normalizedPeriodSelectionValue, periodSelectionValue, setPeriodSelectionValue]);

  useEffect(() => {
    if (customPeriodMonth !== normalizedCustomMonth) {
      setCustomPeriodMonth(normalizedCustomMonth);
    }
  }, [customPeriodMonth, normalizedCustomMonth, setCustomPeriodMonth]);

  useEffect(() => {
    if (customPeriodYear !== normalizedCustomYear) {
      setCustomPeriodYear(normalizedCustomYear);
    }
  }, [customPeriodYear, normalizedCustomYear, setCustomPeriodYear]);

  const selectedStation = useMemo(() => {
    if (!selectedChecklistForView || selectedStationId === null) {
      return null;
    }
    return filteredStationsForSelectedChecklist.find((item) => item.id === selectedStationId) ?? null;
  }, [filteredStationsForSelectedChecklist, selectedChecklistForView, selectedStationId]);
  const selectedStationCode = useMemo(() => {
    if (!selectedStation) {
      return null;
    }
    const code = asString(selectedStation.stationCode).toUpperCase();
    return code || null;
  }, [selectedStation]);

  useEffect(() => {
    let cancelled = false;
    setIsLoadingSelectedStationNotes(false);
    if (!selectedStation) {
      setStationMatchedExternalNoteItems([]);
      setIsLoadingSelectedStationNotes(false);
      return;
    }

    const estNum = Number.isFinite(selectedStation.estNum) ? Math.trunc(selectedStation.estNum) : null;
    const stationCode = asString(selectedStation.stationCode).toUpperCase();
    if (estNum === null || !stationCode) {
      setStationMatchedExternalNoteItems([]);
      setIsLoadingSelectedStationNotes(false);
      return;
    }
    const isManualRefreshRequest = stationPanelRefreshRequestId > 0
      && stationPanelRefreshRequestId !== handledStationNotesRefreshRequestIdRef.current;
    if (isManualRefreshRequest) {
      handledStationNotesRefreshRequestIdRef.current = stationPanelRefreshRequestId;
    }
    const selectedStationSnapshot = selectedStation;

    const stationById = new Map<number, StationItem>(filteredStationsForSelectedChecklist.map((station) => [station.id, station]));
    const notesCacheKey = buildStationMatchedNotesCacheKey(estNum, stationCode);
    const query = new URLSearchParams();
    query.set("estNum", String(estNum));
    query.set("stationCode", stationCode);
    query.set("includeAttachments", "true");
    query.set("limit", "100");

    const buildStationMatchedItemsFromRows = (rows: unknown[]): StationNoteViewItem[] => {
      const nextItems: StationNoteViewItem[] = [];
      for (const row of rows) {
        if (!isRecord(row)) {
          continue;
        }
        const note = toNote(row);
        if (!note) {
          continue;
        }
        const checklistStationId = Math.trunc(note.checklistStationId);
        const matchedStation = stationById.get(checklistStationId);
        const station = matchedStation ?? {
          id: checklistStationId,
          checklistId: asString(row.checklistId),
          estNum: Math.trunc(asNumber(row.estNum) ?? selectedStationSnapshot.estNum),
          stationCode: asString(row.stationCode).toUpperCase() || stationCode,
          stationName: "",
          mediaType: "",
          status: "",
          dateUpdated: null,
          inCurrentSchedule: false,
          scheduleMismatch: false,
          scheduleMismatchReason: null,
          repContacts: [],
          notes: [],
        };
        nextItems.push({
          station,
          note,
          isExternal: !matchedStation,
        });
      }
      return nextItems;
    };

    async function loadMatchedNotes() {
      // Avoid showing stale notes from the previously selected station while loading.
      setStationMatchedExternalNoteItems([]);
      if (isManualRefreshRequest) {
        removeBrowserCache(notesCacheKey);
      }
      const cachedSnapshot = isManualRefreshRequest ? null : readBrowserCacheSnapshot<unknown[]>(notesCacheKey);
      const cachedRows = Array.isArray(cachedSnapshot?.data) ? cachedSnapshot.data : [];
      if (cachedRows.length > 0) {
        setStationMatchedExternalNoteItems(buildStationMatchedItemsFromRows(cachedRows));
      }

      const shouldFetchNetwork = isManualRefreshRequest || cachedRows.length === 0 || Boolean(cachedSnapshot?.isExpired);
      if (!shouldFetchNetwork) {
        setIsLoadingSelectedStationNotes(false);
        return;
      }
      setIsLoadingSelectedStationNotes(true);

      try {
        const response = await requestJson(`/api/tradsphere/v1/invoice-checklist-notes?${query.toString()}`, {
          headers: requestHeaders,
          successToast: false,
          errorToast: false,
        });
        if (cancelled) {
          return;
        }
        const data = unwrapData(response);
        const rows = Array.isArray(data) ? data : [];
        writeBrowserCache(notesCacheKey, rows, TRADSPHERE_CACHE_TTL_MS.MAIN_LOAD, {
          source: "network",
          fetchedAt: Date.now(),
        });
        setStationMatchedExternalNoteItems(buildStationMatchedItemsFromRows(rows));
      } catch {
        if (!cancelled && cachedRows.length === 0) {
          setStationMatchedExternalNoteItems([]);
        }
      } finally {
        if (!cancelled) {
          setIsLoadingSelectedStationNotes(false);
        }
      }
    }

    void loadMatchedNotes();
    return () => {
      cancelled = true;
    };
  }, [filteredStationsForSelectedChecklist, requestHeaders, requestJson, selectedStation, stationPanelRefreshRequestId]);

  useEffect(() => {
    if (!selectedChecklistForView || !selectedStation) {
      setIsHydratingSelectedStationContacts(false);
      return;
    }
    const selectedChecklistIdText = asString(selectedChecklistForView.id);
    const selectedStationIdValue = Number.isFinite(selectedStation.id) ? Math.trunc(selectedStation.id) : null;
    const selectedStationCodeText = asString(selectedStation.stationCode).toUpperCase();
    if (!selectedChecklistIdText || selectedStationIdValue === null || !selectedStationCodeText) {
      setIsHydratingSelectedStationContacts(false);
      return;
    }
    const hydrationKey = buildStationContactsHydrationKey(
      selectedChecklistIdText,
      selectedStationIdValue,
      selectedStationCodeText,
    );
    const isManualRefreshRequest = stationPanelRefreshRequestId > 0
      && stationPanelRefreshRequestId !== handledStationContactsRefreshRequestIdRef.current;
    if (isManualRefreshRequest) {
      handledStationContactsRefreshRequestIdRef.current = stationPanelRefreshRequestId;
      stationContactsHydrationAttemptedRef.current.delete(hydrationKey);
    }
    if (selectedStation.repContacts.length > 0) {
      setIsHydratingSelectedStationContacts(false);
      return;
    }
    if (stationContactsHydrationAttemptedRef.current.has(hydrationKey)) {
      setIsHydratingSelectedStationContacts(false);
      return;
    }
    let cancelled = false;
    setIsHydratingSelectedStationContacts(true);
    const params = new URLSearchParams();
    params.set("codes", selectedStationCodeText);
    params.set("includeContacts", "true");
    params.set("deliveryMethodDetail", "false");
    params.set("contactDetail", "true");
    void requestJson(`/api/tradsphere/v1/stations?${params.toString()}`, {
      headers: requestHeaders,
      successToast: false,
      errorToast: false,
    })
      .then((payload) => {
        if (cancelled) {
          return;
        }
        const rows = unwrapData(payload);
        if (!Array.isArray(rows)) {
          stationContactsHydrationAttemptedRef.current.add(hydrationKey);
          return;
        }
        stationContactsHydrationAttemptedRef.current.add(hydrationKey);
        const stationRow = rows.find((row) => (
          isRecord(row) && asString(row.code || row.stationCode).toUpperCase() === selectedStationCodeText
        ));
        if (!stationRow) {
          return;
        }
        const hydratedRepContacts = extractRepContactsFromStationPayloadRow(stationRow);
        setSelectedChecklist((current) => {
          if (!current || asString(current.id) !== selectedChecklistIdText) {
            return current;
          }
          let hasChanged = false;
          const nextStations = current.stations.map((station) => {
            if (station.id !== selectedStationIdValue) {
              return station;
            }
            if (areRepContactsEqual(station.repContacts, hydratedRepContacts)) {
              return station;
            }
            hasChanged = true;
            return {
              ...station,
              repContacts: hydratedRepContacts.map((contact) => ({ ...contact })),
            };
          });
          if (!hasChanged) {
            return current;
          }
          const nextChecklist = {
            ...current,
            stations: nextStations,
          };
          setLocalChecklistDetailsById((detailsById) => ({
            ...detailsById,
            [nextChecklist.id]: cloneChecklistDetail(nextChecklist) ?? nextChecklist,
          }));
          return nextChecklist;
        });
      })
      .catch(() => {
        // Keep station-selection UX resilient when contact hydration fails.
      })
      .finally(() => {
        if (!cancelled) {
          setIsHydratingSelectedStationContacts(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [
    requestHeaders,
    requestJson,
    selectedChecklistForView?.id,
    selectedStation?.id,
    selectedStation?.stationCode,
    stationPanelRefreshRequestId,
  ]);

  const visibleStationNoteItems = useMemo<StationNoteViewItem[]>(() => {
    if (!selectedChecklistForView || !selectedStation) {
      return [];
    }
    const selectedStationCode = asString(selectedStation.stationCode).toUpperCase();
    const selectedEstNum = Number.isFinite(selectedStation.estNum) ? Math.trunc(selectedStation.estNum) : null;
    const checklistMatchedItems = filteredStationsForSelectedChecklist
      .filter((station) => {
        const stationCode = asString(station.stationCode).toUpperCase();
        const estNum = Number.isFinite(station.estNum) ? Math.trunc(station.estNum) : null;
        return selectedEstNum !== null && estNum === selectedEstNum && stationCode === selectedStationCode;
      })
      .flatMap((station) => (
      station.notes.map((note) => ({ station, note }))
    ));
    const selectedChecklistIdText = asString(selectedChecklistForView.id);
    const checklistMatchedNoteIdSet = new Set<number>(checklistMatchedItems.map((item) => item.note.id));
    const mergedByNoteId = new Map<number, StationNoteViewItem>();
    for (const item of stationMatchedExternalNoteItems) {
      const itemStationCode = asString(item.station.stationCode).toUpperCase();
      const itemEstNum = Number.isFinite(item.station.estNum) ? Math.trunc(item.station.estNum) : null;
      if (selectedEstNum === null || itemEstNum !== selectedEstNum || itemStationCode !== selectedStationCode) {
        continue;
      }
      const itemChecklistId = asString(item.station.checklistId);
      if (
        itemChecklistId === selectedChecklistIdText
        && !checklistMatchedNoteIdSet.has(item.note.id)
      ) {
        // Respect unsaved local removals/edits for the active checklist.
        continue;
      }
      mergedByNoteId.set(item.note.id, item);
    }
    for (const item of checklistMatchedItems) {
      mergedByNoteId.set(item.note.id, item);
    }
    const allNoteItems = [...mergedByNoteId.values()];
    const sortedRecentNotes = allNoteItems.sort((left, right) => {
      const timestampDelta = stationNoteSortTimestamp(right.note) - stationNoteSortTimestamp(left.note);
      if (timestampDelta !== 0) {
        return timestampDelta;
      }
      return right.note.id - left.note.id;
    });
    const topRecent = sortedRecentNotes.slice(0, STATION_NOTES_RECENT_LIMIT);
    const alreadyContainsSelectedStation = topRecent.some((item) => item.station.id === selectedStation.id);
    if (alreadyContainsSelectedStation) {
      return topRecent;
    }
    const latestSelectedStationNote = sortedRecentNotes.find((item) => item.station.id === selectedStation.id);
    if (!latestSelectedStationNote) {
      return topRecent;
    }
    if (topRecent.length < STATION_NOTES_RECENT_LIMIT) {
      return [...topRecent, latestSelectedStationNote];
    }
    return [...topRecent.slice(0, STATION_NOTES_RECENT_LIMIT - 1), latestSelectedStationNote];
  }, [filteredStationsForSelectedChecklist, selectedChecklistForView, selectedStation, stationMatchedExternalNoteItems]);

  const noteContextById = useMemo(() => {
    const map = new Map<number, StationNoteViewItem>();
    if (!selectedChecklistForView) {
      return map;
    }
    for (const station of filteredStationsForSelectedChecklist) {
      for (const note of station.notes) {
        map.set(note.id, { station, note });
      }
    }
    for (const item of stationMatchedExternalNoteItems) {
      if (!map.has(item.note.id)) {
        map.set(item.note.id, item);
      }
    }
    return map;
  }, [filteredStationsForSelectedChecklist, selectedChecklistForView, stationMatchedExternalNoteItems]);

  const editingNoteContext = useMemo(() => {
    if (editingNoteId === null) {
      return null;
    }
    return noteContextById.get(editingNoteId) ?? null;
  }, [editingNoteId, noteContextById]);
  const editingNote = editingNoteContext?.note ?? null;

  const deletingNoteContext = useMemo(() => {
    if (deletingNoteId === null) {
      return null;
    }
    return noteContextById.get(deletingNoteId) ?? null;
  }, [deletingNoteId, noteContextById]);
  const deletingNote = deletingNoteContext?.note ?? null;

  const deletingStation = useMemo(() => {
    if (!selectedChecklistForView || deletingStationId === null) {
      return null;
    }
    return filteredStationsForSelectedChecklist.find((station) => station.id === deletingStationId) ?? null;
  }, [deletingStationId, filteredStationsForSelectedChecklist, selectedChecklistForView]);

  const deletingChecklist = useMemo(() => {
    if (!deletingChecklistId) {
      return null;
    }
    return checklists.find((item) => item.id === deletingChecklistId) ?? null;
  }, [checklists, deletingChecklistId]);
  const isDeleteTargetChecklistInProgress = Boolean(
    deletingChecklistId && deletingChecklistIdSet.has(deletingChecklistId),
  );

  async function handleOpenAttachment(url: string) {
    try {
      await openAttachmentUrlWithHeaders(url, requestHeaders);
    } catch {
      toast.error("Unable to open attachment", "Please try again.");
    }
  }

  const attachmentModalNoteContext = useMemo(() => {
    if (attachmentModalNoteId === null) {
      return null;
    }
    return noteContextById.get(attachmentModalNoteId) ?? null;
  }, [attachmentModalNoteId, noteContextById]);
  const attachmentModalNote = attachmentModalNoteContext?.note ?? null;

  useEffect(() => {
    const imageAttachments = visibleStationNoteItems
      .flatMap((item) => item.note.attachments)
      .filter((attachment) => isImageAttachment(attachment));

    if (imageAttachments.length === 0) {
      generatedAttachmentImageUrlsRef.current.forEach((url) => URL.revokeObjectURL(url));
      generatedAttachmentImageUrlsRef.current = [];
      setAttachmentImageSrcById({});
      return;
    }

    let cancelled = false;
    const controllers: AbortController[] = [];
    const nextMap: Record<number, string> = {};
    const nextGeneratedUrls: string[] = [];

    async function loadImageSources() {
      await Promise.all(imageAttachments.map(async (attachment) => {
        const accessUrl = asString(attachment.accessUrl);
        const rawUrl = accessUrl || asString(attachment.url);
        if (!rawUrl) {
          return;
        }
        if (/^https?:\/\//i.test(rawUrl)) {
          nextMap[attachment.id] = rawUrl;
          return;
        }
        if (rawUrl.startsWith("blob:") || rawUrl.startsWith("data:")) {
          nextMap[attachment.id] = rawUrl;
          return;
        }
        try {
          const controller = new AbortController();
          controllers.push(controller);
          const response = await fetch(rawUrl, {
            method: "GET",
            headers: requestHeaders,
            signal: controller.signal,
          });
          if (!response.ok) {
            return;
          }
          const blob = await response.blob();
          if (!blob.size) {
            return;
          }
          const objectUrl = URL.createObjectURL(blob);
          nextGeneratedUrls.push(objectUrl);
          nextMap[attachment.id] = objectUrl;
        } catch {
          // Keep fallback tile when preview cannot be resolved.
        }
      }));

      if (cancelled) {
        nextGeneratedUrls.forEach((url) => URL.revokeObjectURL(url));
        return;
      }

      generatedAttachmentImageUrlsRef.current.forEach((url) => URL.revokeObjectURL(url));
      generatedAttachmentImageUrlsRef.current = nextGeneratedUrls;
      setAttachmentImageSrcById(nextMap);
    }

    void loadImageSources();

    return () => {
      cancelled = true;
      controllers.forEach((controller) => controller.abort());
    };
  }, [requestHeaders, visibleStationNoteItems]);

  useEffect(() => {
    return () => {
      generatedAttachmentImageUrlsRef.current.forEach((url) => URL.revokeObjectURL(url));
      generatedAttachmentImageUrlsRef.current = [];
    };
  }, []);

  const checklistStatusOptions = useMemo(() => {
    return buildStatusOptions(CHECKLIST_STATUS_OPTIONS_BASE, selectedChecklist?.status || "", checklistStatusDraft);
  }, [checklistStatusDraft, selectedChecklist?.status]);

  const isPeriodSyncPreviewPending = Boolean(pendingPeriodSyncPreview);

  const canSaveAllChanges = (
    (hasUnsavedChanges || isPeriodSyncPreviewPending)
    && !isSavingAllChanges
    && (
      Boolean(selectedChecklist)
      || hasPendingLocalChecklists
      || isPeriodSyncPreviewPending
    )
  );

  const applyLoadedPayload = useCallback(
    (payload: LoadPayload, options?: { preserveLocalDrafts?: boolean }) => {
      const preserveLocalDrafts = options?.preserveLocalDrafts !== false;
      setChecklists((current) => (
        applyChecklistCardSummaryOverrides(
          preserveLocalDrafts
            ? mergeIncomingChecklistsWithLocalDrafts(payload.checklists, current)
            : payload.checklists,
          preserveLocalDrafts ? checklistCardSummaryOverridesById : {},
        )
      ));
      setSelectedChecklistId(payload.selectedChecklistId || null);
      setSelectedChecklist(payload.selectedChecklist);
      setSelectedChecklistBaseline(cloneChecklistDetail(payload.selectedChecklist));
      setChecklistStatusDraft(payload.selectedChecklist?.status || "");
      setChecklistNoteDraft(payload.selectedChecklist?.note || "");
      if (!preserveLocalDrafts) {
        setStationStatusDraftById({});
        setChecklistCardSummaryOverridesById({});
      }

      const firstStationId = payload.selectedChecklist?.stations[0]?.id ?? null;
      if (
        selectedStationId !== null
        && payload.selectedChecklist?.stations.some((item) => item.id === selectedStationId)
      ) {
        return;
      }
      setSelectedStationId(firstStationId);
    },
    [checklistCardSummaryOverridesById, selectedStationId],
  );

  const applyChecklistSelection = useCallback((detail: ChecklistDetail | null) => {
    setSelectedChecklist(detail);
    setSelectedChecklistBaseline(cloneChecklistDetail(detail));
    setChecklistStatusDraft(detail?.status || "");
    setChecklistNoteDraft(detail?.note || "");
    setSelectedStationId(detail?.stations[0]?.id ?? null);
  }, []);

  const resetLocalDrafts = useCallback(() => {
    setSelectedChecklist(cloneChecklistDetail(selectedChecklistBaseline));
    setChecklistStatusDraft(selectedChecklistBaseline?.status || "");
    setChecklistNoteDraft(selectedChecklistBaseline?.note || "");
    setStationStatusDraftById({});
  }, [selectedChecklistBaseline]);

  useEffect(() => {
    const handleBeforeRouteChange = (event: Event) => {
      const customEvent = event as CustomEvent<BeforeRouteChangeEventDetail>;
      const detail = customEvent.detail;
      if (!detail || typeof detail.to !== "string" || typeof detail.proceed !== "function") {
        return;
      }
      if (detail.to === "/tradsphere/invoice-checklists") {
        return;
      }
      if (!hasUnsavedChanges) {
        return;
      }
      event.preventDefault();
      pendingUnsavedActionTypeRef.current = "route";
      pendingUnsavedRouteProceedRef.current = detail.proceed;
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

  const loadData = useCallback(
    async (options: LoadOptions) => {
      const requestId = beginRequest();
      const targetPeriodValueRaw = asString(options.periodValue)
        || loadedPeriodValue
        || selectedDraftPeriodValue
        || DEFAULT_PREVIOUS_PERIOD_VALUE;
      const targetPeriod = parsePeriodInput(targetPeriodValueRaw);
      if (!targetPeriod) {
        setError("Please select a valid period before loading.");
        setIsLoading(false);
        setIsRefreshing(false);
        return;
      }
      const targetPeriodValue = targetPeriod.value;
      const cacheKey = buildLoadCacheKey(targetPeriodValue);
      const cacheSnapshot = readBrowserCacheSnapshot<LoadPayload>(cacheKey);
      const shouldUseCache = options.policy !== "network-only" && Boolean(cacheSnapshot?.data);
      let shouldFetchFromNetwork = shouldFetchNetwork(options.policy, cacheSnapshot);
      const includeSelectedDetail = options.includeSelectedDetail ?? true;

      if (shouldUseCache && cacheSnapshot?.data) {
        const cachedPayload = normalizeLoadPayload(cacheSnapshot.data);
        if (options.checklistId) {
          const detailCacheSnapshot = readBrowserCacheSnapshot<ChecklistDetail>(
            buildChecklistDetailCacheKey(targetPeriodValue, options.checklistId),
          );
          const detailFromCache = toChecklistDetail(detailCacheSnapshot?.data);
          const payloadHasRequestedChecklist = cachedPayload.selectedChecklist?.id === options.checklistId;
          const selectedSummary = cachedPayload.checklists.find((item) => item.id === options.checklistId) ?? null;
          if (detailFromCache) {
            cachedPayload.selectedChecklistId = options.checklistId;
            cachedPayload.selectedChecklist = detailFromCache;
          } else if (!payloadHasRequestedChecklist) {
            // List cache can be fresh while this checklist detail has never been loaded.
            // Force network fetch so switching accounts does not get stuck on empty fallback detail.
            shouldFetchFromNetwork = true;
            if (selectedSummary) {
              cachedPayload.selectedChecklistId = options.checklistId;
              cachedPayload.selectedChecklist = toFallbackChecklistDetail(selectedSummary);
            }
          } else if (selectedSummary) {
            cachedPayload.selectedChecklistId = options.checklistId;
            cachedPayload.selectedChecklist = toFallbackChecklistDetail(selectedSummary);
          }
        } else if (!includeSelectedDetail && !cachedPayload.selectedChecklist && cachedPayload.selectedChecklistId) {
          const selectedSummary = cachedPayload.checklists.find((item) => item.id === cachedPayload.selectedChecklistId) ?? null;
          if (selectedSummary) {
            cachedPayload.selectedChecklist = toFallbackChecklistDetail(selectedSummary);
          }
        }
        if (isLatestRequest(requestId)) {
          applyLoadedPayload(cachedPayload, {
            preserveLocalDrafts: options.preserveLocalDrafts,
          });
          setLoadedPeriodValue(cachedPayload.selectedPeriod?.value || targetPeriodValue);
          setCacheStatus({ source: "cache", fetchedAt: cacheSnapshot.fetchedAt });
          setError(null);
          setIsLoading(false);
          setIsRefreshing(shouldFetchFromNetwork);
        }
      } else {
        setIsLoading(true);
      }

      if (!shouldFetchFromNetwork) {
        setIsLoading(false);
        setIsRefreshing(false);
        return;
      }

      if (!isOnline) {
        if (!shouldUseCache) {
          setError("You're offline. Checklist data is unavailable until connection is restored.");
          setIsLoading(false);
        }
        setIsRefreshing(false);
        return;
      }

      const params = new URLSearchParams();
      params.set("year", String(targetPeriod.year));
      params.set("month", String(targetPeriod.month));
      if (options.checklistId) {
        params.set("checklistId", options.checklistId);
      }
      if (!includeSelectedDetail) {
        params.set("includeSelectedDetail", "false");
      }

      const url = `/api/tradsphere/v1/ui/invoice-checklists/load${params.toString() ? `?${params.toString()}` : ""}`;
      try {
        const response = await requestJson(url, {
          headers: requestHeaders,
          successToast: false,
          errorToast: false,
        });
        const normalized = normalizeLoadPayload(response);
        const fallbackMaps = buildChecklistNameFallbackMaps(
          checklists,
          [
            ...Object.values(localChecklistDetailsById),
            selectedChecklist,
            selectedChecklistBaseline,
            normalized.selectedChecklist,
          ].filter((item): item is ChecklistDetail => item !== null),
        );
        const normalizedWithNames = applyLoadPayloadNameFallbacks(normalized, fallbackMaps);
        const payloadForUi: LoadPayload = (!includeSelectedDetail && !normalizedWithNames.selectedChecklist && normalizedWithNames.selectedChecklistId)
          ? (() => {
            const selectedSummary = normalizedWithNames.checklists.find(
              (item) => item.id === normalizedWithNames.selectedChecklistId,
            ) ?? null;
            if (!selectedSummary) {
              return normalizedWithNames;
            }
            return {
              ...normalizedWithNames,
              selectedChecklist: toFallbackChecklistDetail(selectedSummary),
            };
          })()
          : normalizedWithNames;
        const fetchedAt = Date.now();

        writeBrowserCache(cacheKey, normalizedWithNames, LOAD_CACHE_TTL_MS, {
          source: "network",
          fetchedAt,
        });
        if (normalizedWithNames.selectedChecklist?.id) {
          writeBrowserCache(
            buildChecklistDetailCacheKey(targetPeriodValue, normalizedWithNames.selectedChecklist.id),
            normalizedWithNames.selectedChecklist,
            LOAD_CACHE_TTL_MS,
            { source: "network", fetchedAt },
          );
        }

        const applied = applyFromRequest(
          requestId,
          () => {
            applyLoadedPayload(payloadForUi, {
              preserveLocalDrafts: options.preserveLocalDrafts,
            });
            setLoadedPeriodValue(payloadForUi.selectedPeriod?.value || targetPeriodValue);
            setCacheStatus({ source: "network", fetchedAt });
            setError(null);
            setRefreshMessage(null);
          },
          { deferWhenDirty: options.deferWhenDirty ?? true },
        );

        if (applied === "deferred") {
          setRefreshMessage(DEFERRED_REFRESH_MESSAGE);
        }
      } catch (loadError) {
        if (options.checklistId && isChecklistMissingError(loadError)) {
          removeBrowserCache(buildChecklistDetailCacheKey(targetPeriodValue, options.checklistId));
          await loadData({
            policy: "network-only",
            periodValue: targetPeriodValue,
            includeSelectedDetail,
            preserveLocalDrafts: false,
            deferWhenDirty: false,
          });
          setRefreshMessage("Selected checklist no longer exists. Loaded latest data.");
          setError(null);
          return;
        }
        if (shouldUseCache) {
          setRefreshMessage("Showing cached results. Could not refresh.");
          setError(null);
        } else {
          const message = loadError instanceof Error && loadError.message.trim()
            ? loadError.message.trim()
            : "Unable to load invoice checklists.";
          setError(message);
        }
      } finally {
        if (isLatestRequest(requestId)) {
          setIsLoading(false);
          setIsRefreshing(false);
        }
      }
    },
    [
      applyFromRequest,
      applyLoadedPayload,
      beginRequest,
      checklists,
      isLatestRequest,
      isOnline,
      localChecklistDetailsById,
      loadedPeriodValue,
      requestHeaders,
      requestJson,
      selectedChecklist,
      selectedChecklistBaseline,
      selectedDraftPeriodValue,
    ],
  );

  useEffect(() => {
    if (!hasDeferredUpdate) {
      setRefreshMessage(null);
    }
  }, [hasDeferredUpdate]);

  useEffect(() => {
    if (refreshMessage !== DEFERRED_REFRESH_MESSAGE) {
      return;
    }
    if (hasUnsavedChanges) {
      return;
    }
    setRefreshMessage(null);
    clearDeferredUpdate();
  }, [clearDeferredUpdate, hasUnsavedChanges, refreshMessage]);

  function updateChecklistInState(updater: (prev: ChecklistDetail) => ChecklistDetail) {
    setSelectedChecklist((current) => {
      if (!current) {
        return current;
      }
      const next = updater(current);
      setLocalChecklistDetailsById((detailsById) => ({
        ...detailsById,
        [next.id]: cloneChecklistDetail(next) ?? next,
      }));
      setLocalChecklistBaselinesById((baselinesById) => {
        if (baselinesById[next.id] || !selectedChecklistBaseline) {
          return baselinesById;
        }
        const baseline = cloneChecklistDetail(selectedChecklistBaseline);
        if (!baseline) {
          return baselinesById;
        }
        return {
          ...baselinesById,
          [next.id]: baseline,
        };
      });
      return next;
    });
  }

  function updateChecklistSummaryInState(checklistId: string, updates: Partial<ChecklistSummary>) {
    setChecklists((current) => current.map((item) => (item.id === checklistId ? { ...item, ...updates } : item)));
  }

  function persistActiveChecklistSummaryToCard() {
    if (!selectedChecklist) {
      return;
    }
    const override = buildChecklistCardSummaryOverride(selectedChecklist, stationStatusDraftById);
    updateChecklistSummaryInState(selectedChecklist.id, override);
    setChecklistCardSummaryOverridesById((current) => ({
      ...current,
      [selectedChecklist.id]: override,
    }));
  }

  async function runLoadForSelectedPeriod() {
    if (!selectedDraftPeriodValue) {
      return;
    }
    const selectedChecklistIdForReload = isLocalChecklistId(selectedChecklistId)
      ? null
      : selectedChecklistId;
    clearDeferredUpdate();
    setRefreshMessage(null);
    setPendingPeriodSyncPreview(null);
    previewRemovedPlannedStationKeysRef.current = new Set();
    setLocalRemovedScheduledStationsByChecklistId({});
    setIsLoadActionOverlayVisible(true);
    try {
      await loadData({
        policy: "network-first",
        periodValue: selectedDraftPeriodValue,
        checklistId: selectedChecklistIdForReload,
        includeSelectedDetail: true,
        deferWhenDirty: false,
        preserveLocalDrafts: false,
      });
    } finally {
      setIsLoadActionOverlayVisible(false);
    }
  }

  function handleLoadClick() {
    if (!hasUnsavedChanges) {
      void runLoadForSelectedPeriod();
      return;
    }
    pendingUnsavedActionTypeRef.current = "load";
    pendingUnsavedRouteProceedRef.current = null;
    setIsUnsavedDialogOpen(true);
  }

  async function runSyncChecklistPeriod() {
    const loadedPeriod = parsePeriodInput(loadedPeriodValue || "");
    if (!loadedPeriod) {
      setError("Load a valid period before generating or updating checklist rows.");
      return;
    }
    if (!selectedDraftPeriodValue || loadedPeriod.value !== selectedDraftPeriodValue) {
      setError("Load the selected period before running Generate/Update.");
      return;
    }

    setIsSyncingPeriod(true);
    try {
      const response = await requestJson("/api/tradsphere/v1/invoice-checklists/sync-period", {
        method: "POST",
        headers: requestHeaders,
        body: {
          year: loadedPeriod.year,
          month: loadedPeriod.month,
          previewOnly: true,
        },
        successToast: false,
        errorToast: false,
      });
      const data = unwrapData(response);
      const payload = isRecord(data) ? data : {};
      const actionKey = asString(payload.action).toLowerCase() === "generate" ? "generate" : "update";
      const action = actionKey === "generate" ? "Generate" : "Update";
      const plannedChecklistsCount = Math.trunc(
        asNumber(payload.plannedChecklistsCount) ?? asNumber(payload.createdChecklistsCount) ?? 0,
      );
      const plannedChecklistsRaw = Array.isArray(payload.plannedChecklists) ? payload.plannedChecklists : [];
      const plannedStationsRaw = Array.isArray(payload.plannedStations) ? payload.plannedStations : [];
      const mismatchStationsRaw = Array.isArray(payload.mismatchStations) ? payload.mismatchStations : [];
      const plannedChecklists = plannedChecklistsRaw.reduce<PeriodSyncPlannedChecklist[]>((acc, item) => {
        if (!isRecord(item)) {
          return acc;
        }
        const accountCode = asString(item.accountCode).toUpperCase();
        const year = asNumber(item.year);
        const month = asNumber(item.month);
        if (!accountCode || year === null || month === null) {
          return acc;
        }
        acc.push({
          accountCode,
          year: Math.trunc(year),
          month: Math.trunc(month),
          accountName: asString(item.accountName),
        });
        return acc;
      }, []);
      const plannedStations = plannedStationsRaw.reduce<PeriodSyncPlannedStation[]>((acc, item) => {
        if (!isRecord(item)) {
          return acc;
        }
        const accountCode = asString(item.accountCode).toUpperCase();
        const estNum = asNumber(item.estNum);
        const stationCode = asString(item.stationCode).toUpperCase();
        if (!accountCode || estNum === null || !stationCode) {
          return acc;
        }
        const checklistIdText = asString(item.checklistId) || null;
        acc.push({
          checklistId: checklistIdText,
          accountCode,
          estNum: Math.trunc(estNum),
          stationCode,
          stationName: asString(item.stationName),
          mediaType: asString(item.mediaType),
        });
        return acc;
      }, []);
      const accountNameByCode = new Map<string, string>();
      for (const row of checklists) {
        const accountCode = asString(row.accountCode).toUpperCase();
        const accountName = asString(row.accountName);
        if (!accountCode || !accountName) {
          continue;
        }
        accountNameByCode.set(accountCode, accountName);
      }
      const stationMetaByCode = new Map<string, { stationName: string; mediaType: string }>();
      const missingAccountCodes = [...new Set(plannedChecklists
        .map((item) => asString(item.accountCode).toUpperCase())
        .filter((code) => code && !accountNameByCode.get(code)))];
      const plannedStationCodes = [...new Set(plannedStations
        .map((item) => asString(item.stationCode).toUpperCase())
        .filter(Boolean))];
      const accountEnrichmentPromise = (missingAccountCodes.length > 0)
        ? (async () => {
          const params = new URLSearchParams();
          params.set("accountCodes", missingAccountCodes.join(","));
          params.set("active", "false");
          try {
            const accountPayload = await requestJson(`/api/tradsphere/v1/accounts?${params.toString()}`, {
              headers: requestHeaders,
              successToast: false,
              errorToast: false,
            });
            const accountRows = unwrapData(accountPayload);
            if (!Array.isArray(accountRows)) {
              return;
            }
            for (const row of accountRows) {
              if (!isRecord(row)) {
                continue;
              }
              const accountCode = asString(row.accountCode).toUpperCase();
              const accountName = asString(row.name || row.accountName);
              if (!accountCode || !accountName || accountNameByCode.get(accountCode)) {
                continue;
              }
              accountNameByCode.set(accountCode, accountName);
            }
          } catch {
            // Keep preview resilient when account directory lookup fails.
          }
        })()
        : Promise.resolve();
      const stationEnrichmentPromise = (plannedStationCodes.length > 0)
        ? (async () => {
          const params = new URLSearchParams();
          params.set("codes", plannedStationCodes.join(","));
          params.set("includeContacts", "false");
          try {
            const stationPayload = await requestJson(`/api/tradsphere/v1/stations?${params.toString()}`, {
              headers: requestHeaders,
              successToast: false,
              errorToast: false,
            });
            const stationRows = unwrapData(stationPayload);
            if (!Array.isArray(stationRows)) {
              return;
            }
            for (const row of stationRows) {
              if (!isRecord(row)) {
                continue;
              }
              const stationCode = asString(row.code || row.stationCode).toUpperCase();
              if (!stationCode || stationMetaByCode.get(stationCode)) {
                continue;
              }
              stationMetaByCode.set(stationCode, {
                stationName: asString(row.name || row.stationName),
                mediaType: asString(row.mediaType),
              });
            }
          } catch {
            // Keep preview resilient when station metadata lookup fails.
          }
        })()
        : Promise.resolve();
      await Promise.all([accountEnrichmentPromise, stationEnrichmentPromise]);
      const enrichedPlannedChecklists = plannedChecklists.map((item) => ({
        ...item,
        accountName: asString(item.accountName) || accountNameByCode.get(item.accountCode) || "",
      }));
      const enrichedPlannedStations: PeriodSyncPlannedStation[] = plannedStations.map((item) => {
        const meta = stationMetaByCode.get(item.stationCode);
        return {
          ...item,
          stationName: asString(item.stationName) || asString(meta?.stationName),
          mediaType: asString(item.mediaType) || asString(meta?.mediaType),
        };
      });
      const plannedStationIdentitySet = new Set<string>(
        enrichedPlannedStations.map((item) => buildStationIdentityKey(item.accountCode, item.estNum, item.stationCode)),
      );
      const locallyRemovedScheduledStations = Object.entries(localRemovedScheduledStationsByChecklistId).flatMap(
        ([checklistId, byIdentity]) => {
          if (!checklists.some((item) => item.id === checklistId)) {
            return [] as PeriodSyncPlannedStation[];
          }
          const selectedDetailForChecklist = (
            selectedChecklist?.id === checklistId ? selectedChecklist : localChecklistDetailsById[checklistId]
          ) ?? null;
          return Object.entries(byIdentity).reduce<PeriodSyncPlannedStation[]>((acc, [identityKey, row]) => {
            const hasInDraft = Boolean(
              selectedDetailForChecklist
              && selectedDetailForChecklist.stations.some((station) => (
                buildStationIdentityKey(
                  selectedDetailForChecklist.accountCode,
                  station.estNum,
                  station.stationCode,
                ) === identityKey
              )),
            );
            if (hasInDraft || plannedStationIdentitySet.has(identityKey)) {
              return acc;
            }
            acc.push(row);
            plannedStationIdentitySet.add(identityKey);
            return acc;
          }, []);
        },
      );
      const effectivePlannedStations = enrichedPlannedStations.concat(locallyRemovedScheduledStations);
      const plannedAddKeysByChecklistId = new Map<string, Set<string>>();
      for (const row of effectivePlannedStations) {
        const checklistId = asString(row.checklistId);
        if (!checklistId) {
          continue;
        }
        const rows = plannedAddKeysByChecklistId.get(checklistId) ?? new Set<string>();
        rows.add(`${Math.trunc(row.estNum)}:${asString(row.stationCode).toUpperCase()}`);
        plannedAddKeysByChecklistId.set(checklistId, rows);
      }
      const localDraftRemoveCandidatesCount = checklists.reduce((acc, checklist) => {
        const checklistId = asString(checklist.id);
        if (!checklistId) {
          return acc;
        }
        const detail = (
          selectedChecklist?.id === checklistId
            ? selectedChecklist
            : localChecklistDetailsById[checklistId]
        ) ?? null;
        if (!detail) {
          return acc;
        }
        const plannedAddKeySet = plannedAddKeysByChecklistId.get(checklistId) ?? new Set<string>();
        const nextCount = detail.stations.reduce((innerAcc, station) => {
          if (station.isLocalDraft !== true) {
            return innerAcc;
          }
          const stationKey = `${Math.trunc(station.estNum)}:${asString(station.stationCode).toUpperCase()}`;
          const isProposedAdd = station.isGeneratedPreview === true || plannedAddKeySet.has(stationKey);
          if (isProposedAdd) {
            return innerAcc;
          }
          return innerAcc + 1;
        }, 0);
        return acc + nextCount;
      }, 0);
      const mismatchStations = mismatchStationsRaw
        .map(toChecklistMismatch)
        .filter((item): item is ChecklistMismatchItem => item !== null);
      const mismatchStationCount = Math.trunc(asNumber(payload.mismatchStationCount) ?? mismatchStations.length);
      const hasActionablePreviewChanges = (
        enrichedPlannedChecklists.length > 0
        || effectivePlannedStations.length > 0
        || mismatchStationCount > 0
        || localDraftRemoveCandidatesCount > 0
      );
      if (!hasActionablePreviewChanges) {
        setPendingPeriodSyncPreview(null);
        previewRemovedPlannedStationKeysRef.current = new Set();
        setError(null);
        toast.success(`${action} preview is up to date`);
        return;
      }

      previewRemovedPlannedStationKeysRef.current = new Set();
      setPendingPeriodSyncPreview({
        action: actionKey,
        periodValue: loadedPeriod.value,
        year: loadedPeriod.year,
        month: loadedPeriod.month,
        plannedChecklistsCount,
        plannedStationsCount: effectivePlannedStations.length,
        mismatchStationCount,
        plannedChecklists: enrichedPlannedChecklists,
        plannedStations: effectivePlannedStations,
        mismatchStations,
      });
      toast.success(`${action} preview ready`);
      setError(null);
      setDraftSearch("");
      setAppliedSearch("");

      const existingAccountCodes = new Set(
        checklists
          .filter((item) => item.isGeneratedPreview !== true)
          .map((item) => asString(item.accountCode).toUpperCase()),
      );
      const localIdByAccount = new Map<string, string>();
      const previewChecklists: ChecklistSummary[] = enrichedPlannedChecklists
        .filter((item) => !existingAccountCodes.has(item.accountCode))
        .map((item) => {
          const localId = `preview-checklist-${Math.abs(nextLocalId())}`;
          localIdByAccount.set(item.accountCode, localId);
          return {
            id: localId,
            accountCode: item.accountCode,
            accountName: asString(item.accountName),
            year: item.year,
            month: item.month,
            quarter: loadedPeriod.quarter,
            status: "",
            note: "",
            stationCount: 0,
            expectedStationCount: 0,
            mismatchStationCount: 0,
            hasScheduleMismatch: false,
            searchStations: [],
            dateUpdated: null,
            isLocalDraft: true,
            isGeneratedPreview: true,
            proposalState: "proposed_add" as const,
            proposalLabel: "Proposed add",
          };
        })
        .sort((left, right) => left.accountCode.localeCompare(right.accountCode));

      if (previewChecklists.length > 0) {
        const stationsByChecklistId = new Map<string, StationItem[]>();
        for (const station of effectivePlannedStations) {
          const localChecklistId = localIdByAccount.get(station.accountCode);
          if (!localChecklistId) {
            continue;
          }
          const rows = stationsByChecklistId.get(localChecklistId) ?? [];
          rows.push({
            id: nextLocalId(),
            checklistId: localChecklistId,
            estNum: station.estNum,
            stationCode: station.stationCode,
            stationName: asString(station.stationName),
            mediaType: asString(station.mediaType),
            status: "",
            dateUpdated: null,
            inCurrentSchedule: true,
            scheduleMismatch: false,
            scheduleMismatchReason: null,
            repContacts: [],
            notes: [],
            isLocalDraft: true,
            isGeneratedPreview: true,
            proposalState: "proposed_add" as const,
            proposalLabel: "Proposed add",
          });
          stationsByChecklistId.set(localChecklistId, rows);
        }

        const previewDetailsById: Record<string, ChecklistDetail> = {};
        const nextPreviewChecklists = previewChecklists.map((summary) => {
          const stations = (stationsByChecklistId.get(summary.id) ?? []).sort((left, right) => (
            left.estNum - right.estNum || left.stationCode.localeCompare(right.stationCode)
          ));
          const searchStations = stations.map((station) => ({
            estNum: station.estNum,
            stationCode: station.stationCode,
          }));
          const nextSummary: ChecklistSummary = {
            ...summary,
            stationCount: stations.length,
            expectedStationCount: stations.length,
            searchStations,
          };
          previewDetailsById[summary.id] = {
            id: summary.id,
            accountCode: summary.accountCode,
            accountName: summary.accountName,
            year: summary.year,
            month: summary.month,
            quarter: summary.quarter,
            status: summary.status,
            note: summary.note,
            dateUpdated: null,
            expectedStationCount: stations.length,
            mismatchStationCount: 0,
            hasScheduleMismatch: false,
            stations,
            isLocalDraft: true,
            isGeneratedPreview: true,
            proposalState: "proposed_add" as const,
            proposalLabel: "Proposed add",
          };
          return nextSummary;
        });

        setChecklists((current) => {
          const nonPreviewRows = current.filter((item) => item.isGeneratedPreview !== true);
          return [...nonPreviewRows, ...nextPreviewChecklists]
            .sort((left, right) => left.accountCode.localeCompare(right.accountCode));
        });
        setLocalChecklistDetailsById((current) => {
          const next: Record<string, ChecklistDetail> = {};
          for (const [key, value] of Object.entries(current)) {
            if (value?.isGeneratedPreview === true) {
              continue;
            }
            next[key] = value;
          }
          return { ...next, ...previewDetailsById };
        });

        if (!selectedChecklistId) {
          const firstChecklist = nextPreviewChecklists[0] ?? null;
          if (firstChecklist) {
            const firstDetail = previewDetailsById[firstChecklist.id] ?? null;
            setSelectedChecklistId(firstChecklist.id);
            setSelectedChecklist(firstDetail);
            setSelectedChecklistBaseline(cloneChecklistDetail(firstDetail));
            setChecklistStatusDraft(firstDetail?.status || "");
            setChecklistNoteDraft(firstDetail?.note || "");
            setStationStatusDraftById({});
            setSelectedStationId(firstDetail?.stations[0]?.id ?? null);
          }
        }
      }
    } catch (syncError) {
      const message = syncError instanceof Error && syncError.message.trim()
        ? syncError.message.trim()
        : "Unable to generate/update checklist rows for this period.";
      setError(message);
      toast.error("Generate/Update failed", message);
    } finally {
      setIsSyncingPeriod(false);
    }
  }

  function handleSyncChecklistPeriod() {
    void runSyncChecklistPeriod();
  }

  async function runChecklistSelect(checklistId: string, options?: { fromUserSelection?: boolean }) {
    const fromUserSelection = options?.fromUserSelection === true;
    setSelectedChecklistId(checklistId);
    if (fromUserSelection && normalizeSearchKeyword(appliedSearch)) {
      selectionBeforeSearchRef.current = {
        checklistId,
        stationId: null,
      };
    }
    const selectedSummary = checklists.find((item) => item.id === checklistId) ?? null;
    const localDetail = localChecklistDetailsById[checklistId];
    if (localDetail) {
      const localBaseline = localChecklistBaselinesById[checklistId] ?? localDetail;
      setSelectedChecklist(localDetail);
      setSelectedChecklistBaseline(cloneChecklistDetail(localBaseline));
      setChecklistStatusDraft(localDetail.status || "");
      setChecklistNoteDraft(localDetail.note || "");
      setSelectedStationId(localDetail.stations[0]?.id ?? null);
      return;
    }
    if (selectedSummary?.isLocalDraft) {
      const fallbackDetail: ChecklistDetail = {
        id: selectedSummary.id,
        accountCode: selectedSummary.accountCode,
        accountName: selectedSummary.accountName,
        year: selectedSummary.year,
        month: selectedSummary.month,
        quarter: selectedSummary.quarter,
        status: selectedSummary.status,
        note: selectedSummary.note,
        dateUpdated: selectedSummary.dateUpdated,
        expectedStationCount: selectedSummary.expectedStationCount,
        mismatchStationCount: selectedSummary.mismatchStationCount,
        hasScheduleMismatch: selectedSummary.hasScheduleMismatch,
        stations: [],
        isLocalDraft: true,
      };
      setSelectedChecklist(fallbackDetail);
      setSelectedChecklistBaseline(cloneChecklistDetail(fallbackDetail));
      setChecklistStatusDraft(fallbackDetail.status || "");
      setChecklistNoteDraft(fallbackDetail.note || "");
      setSelectedStationId(null);
      return;
    }
    const targetPeriodValue = loadedPeriodValue || selectedDraftPeriodValue;
    if (!targetPeriodValue) {
      return;
    }
    if (!isOnline) {
      const detailCacheSnapshot = readBrowserCacheSnapshot<ChecklistDetail>(
        buildChecklistDetailCacheKey(targetPeriodValue, checklistId),
      );
      const detailFromCache = toChecklistDetail(detailCacheSnapshot?.data);
      if (detailFromCache) {
        applyChecklistSelection(detailFromCache);
        setCacheStatus({ source: "cache", fetchedAt: detailCacheSnapshot?.fetchedAt ?? Date.now() });
        setError(null);
        return;
      }
      if (selectedSummary) {
        applyChecklistSelection(toFallbackChecklistDetail(selectedSummary));
        setError(null);
        return;
      }
    }
    if (fromUserSelection) {
      setIsChecklistSelectionLoading(true);
    }
    try {
      await loadData({ policy: "cache-first", periodValue: targetPeriodValue, checklistId });
    } finally {
      if (fromUserSelection) {
        setIsChecklistSelectionLoading(false);
      }
    }
  }

  function handleChecklistSelect(checklistId: string) {
    persistActiveChecklistSummaryToCard();
    if (checklistId === selectedChecklistId) {
      const selectedSummary = checklists.find((item) => item.id === checklistId) ?? null;
      const shouldHydrateFallbackDetail = Boolean(
        selectedChecklist
        && selectedChecklist.id === checklistId
        && selectedChecklist.stations.length === 0
        && selectedSummary
        && !selectedSummary.isLocalDraft
        && (selectedSummary.stationCount > 0 || selectedSummary.mismatchStationCount > 0),
      );
      if (!shouldHydrateFallbackDetail) {
        return;
      }
      void runChecklistSelect(checklistId, { fromUserSelection: true });
      return;
    }
    void runChecklistSelect(checklistId, { fromUserSelection: true });
  }

  function runStationSelect(stationId: number) {
    setSelectedStationId(stationId);
  }

  function handleStationSelect(stationId: number) {
    if (stationId === selectedStationId) {
      return;
    }
    if (normalizeSearchKeyword(appliedSearch) && selectedChecklistId) {
      selectionBeforeSearchRef.current = {
        checklistId: selectedChecklistId,
        stationId,
      };
    }
    runStationSelect(stationId);
  }

  async function handleRefreshFromChip() {
    if (!loadedPeriodValue) {
      return;
    }
    if (isChipRefreshing) {
      return;
    }
    setIsChipRefreshing(true);
    try {
      await loadData({
        policy: "network-only",
        periodValue: loadedPeriodValue,
        checklistId: selectedChecklistId,
      });
      if (selectedStationId !== null) {
        setStationPanelRefreshRequestId((current) => current + 1);
      }
    } finally {
      setIsChipRefreshing(false);
    }
  }

  const handleStationModalDetailLoaded = useCallback((result: StationModalDetailLoadResult) => {
    if (result.source !== "network") {
      return;
    }
    if (!selectedChecklist) {
      return;
    }
    const normalizedStationCode = asString(result.stationCode).toUpperCase();
    if (!normalizedStationCode) {
      return;
    }
    const nextRepContacts = extractRepContactsFromStationModalDetail(result);
    const patched = applyRepContactsToChecklistStationsByCode(
      selectedChecklist,
      normalizedStationCode,
      nextRepContacts,
    );
    if (!patched.changed) {
      return;
    }

    const nextChecklist = patched.checklist;
    setSelectedChecklist(nextChecklist);
    setLocalChecklistDetailsById((detailsById) => ({
      ...detailsById,
      [nextChecklist.id]: cloneChecklistDetail(nextChecklist) ?? nextChecklist,
    }));

    const periodValue = asString(loadedPeriodValue || selectedDraftPeriodValue || "");
    if (!periodValue) {
      return;
    }
    const fetchedAt = Number.isFinite(result.fetchedAt) ? result.fetchedAt : Date.now();
    writeBrowserCache(
      buildChecklistDetailCacheKey(periodValue, nextChecklist.id),
      nextChecklist,
      LOAD_CACHE_TTL_MS,
      { source: "network", fetchedAt },
    );
    const loadCacheKey = buildLoadCacheKey(periodValue);
    const loadCacheSnapshot = readBrowserCacheSnapshot<LoadPayload>(loadCacheKey);
    if (!loadCacheSnapshot?.data) {
      return;
    }
    const normalizedLoadPayload = normalizeLoadPayload(loadCacheSnapshot.data);
    if (
      normalizedLoadPayload.selectedChecklistId !== nextChecklist.id
      && normalizedLoadPayload.selectedChecklist?.id !== nextChecklist.id
    ) {
      return;
    }
    writeBrowserCache(
      loadCacheKey,
      {
        ...normalizedLoadPayload,
        selectedChecklistId: normalizedLoadPayload.selectedChecklistId || nextChecklist.id,
        selectedChecklist: nextChecklist,
      },
      LOAD_CACHE_TTL_MS,
      { source: "network", fetchedAt },
    );
  }, [loadedPeriodValue, selectedChecklist, selectedDraftPeriodValue]);

  function handleChecklistStatusDraftChange(nextStatus: string) {
    setChecklistStatusDraft(nextStatus);
  }

  async function handleCopyRepContact(contact: RepContactItem) {
    const email = asString(contact.email);
    if (!email) {
      toast.error("Could not copy email", "Contact is missing an email address.");
      return;
    }
    try {
      await copyTextToClipboard(email);
      toast.success("Copied email");
    } catch {
      toast.error("Could not copy email");
    }
  }

  function nextLocalId(): number {
    const next = nextLocalIdRef.current;
    nextLocalIdRef.current -= 1;
    return next;
  }

  async function handleSaveAllChanges() {
    if (!canSaveAllChanges) {
      return;
    }

    setIsSavingAllChanges(true);
    try {
      let activeChecklist: ChecklistDetail | null = selectedChecklist ? cloneChecklistDetail(selectedChecklist) : null;
      let targetSelectedChecklistId: string | null = selectedChecklistId;
      const previewGeneratedStationStatusByKey = new Map<string, string>();
      const previewGeneratedStationStatusUpdates: Array<{ stationRowId: number; status: string | null }> = [];
      const previewGeneratedStationDeleteIds: number[] = [];
      const syncedChecklistIdsToInvalidate = new Set<string>();
      if (pendingPeriodSyncPreview) {
        if (selectedChecklistForView) {
          const selectedAccountCode = asString(selectedChecklistForView.accountCode).toUpperCase();
          for (const station of selectedChecklistForView.stations) {
            if (station.isGeneratedPreview !== true) {
              continue;
            }
            const draftStatus = asString(stationStatusDraftById[station.id] ?? station.status ?? "");
            if (!draftStatus) {
              continue;
            }
            const stationCode = asString(station.stationCode).toUpperCase();
            if (!selectedAccountCode || !stationCode) {
              continue;
            }
            const key = `${selectedAccountCode}::${Math.trunc(station.estNum)}::${stationCode}`;
            previewGeneratedStationStatusByKey.set(key, draftStatus);
          }
        }

        const syncResponse = await requestJson("/api/tradsphere/v1/invoice-checklists/sync-period", {
          method: "POST",
          headers: requestHeaders,
          body: {
            year: pendingPeriodSyncPreview.year,
            month: pendingPeriodSyncPreview.month,
            previewOnly: false,
          },
          successToast: false,
          errorToast: false,
        });
        const syncPayload = unwrapData(syncResponse);
        const syncRecord = isRecord(syncPayload) ? syncPayload : {};
        const createdChecklistsRaw = Array.isArray(syncRecord.createdChecklists) ? syncRecord.createdChecklists : [];
        for (const row of createdChecklistsRaw) {
          if (!isRecord(row)) {
            continue;
          }
          const checklistId = asString(row.id);
          if (!checklistId) {
            continue;
          }
          syncedChecklistIdsToInvalidate.add(checklistId);
        }

        const removedPreviewStationKeys = previewRemovedPlannedStationKeysRef.current;
        const createdStationsRaw = Array.isArray(syncRecord.createdStations) ? syncRecord.createdStations : [];
        for (const row of createdStationsRaw) {
          if (!isRecord(row)) {
            continue;
          }
          const checklistId = asString(row.checklistId);
          if (checklistId) {
            syncedChecklistIdsToInvalidate.add(checklistId);
          }
          const stationRowId = asNumber(row.id);
          const accountCode = asString(row.accountCode).toUpperCase();
          const estNum = asNumber(row.estNum);
          const stationCode = asString(row.stationCode).toUpperCase();
          if (stationRowId === null || estNum === null || !accountCode || !stationCode) {
            continue;
          }
          const stationKey = buildStationIdentityKey(accountCode, estNum, stationCode);
          if (removedPreviewStationKeys.has(stationKey)) {
            previewGeneratedStationDeleteIds.push(Math.trunc(stationRowId));
            continue;
          }
          if (previewGeneratedStationStatusByKey.size === 0) {
            continue;
          }
          const status = previewGeneratedStationStatusByKey.get(stationKey);
          if (!status) {
            continue;
          }
          previewGeneratedStationStatusUpdates.push({
            stationRowId: Math.trunc(stationRowId),
            status,
          });
        }
        setPendingPeriodSyncPreview(null);
        previewRemovedPlannedStationKeysRef.current = new Set();
        // Remove temporary preview-only local drafts before reloading persisted rows.
        setChecklists((current) => current.filter((item) => item.isGeneratedPreview !== true));
      setLocalChecklistDetailsById((current) => {
        const next: Record<string, ChecklistDetail> = {};
        for (const [key, value] of Object.entries(current)) {
          if (value?.isGeneratedPreview === true) {
            continue;
          }
          next[key] = value;
        }
        return next;
      });
      setLocalChecklistBaselinesById((current) => {
        const next: Record<string, ChecklistDetail> = {};
        for (const [key, value] of Object.entries(current)) {
          if (value?.isGeneratedPreview === true) {
            continue;
          }
          next[key] = value;
        }
        return next;
      });
        if (activeChecklist?.isGeneratedPreview) {
          activeChecklist = null;
          targetSelectedChecklistId = null;
          setSelectedChecklist(null);
          setSelectedChecklistBaseline(null);
          setSelectedChecklistId(null);
          setSelectedStationId(null);
          setChecklistStatusDraft("");
          setChecklistNoteDraft("");
          setStationStatusDraftById({});
        }
      }

      const selectedPeriod = parsePeriodInput(loadedPeriodValue || selectedDraftPeriodValue || "");
      if (!selectedPeriod) {
        throw new Error("Load or select a valid period before saving.");
      }

      const localChecklistSummaries = checklists.filter(
        (item) => item.isLocalDraft === true && item.isGeneratedPreview !== true,
      );
      const checklistCreates = localChecklistSummaries.map((localSummary) => {
        const isSelectedLocalChecklist = targetSelectedChecklistId === localSummary.id;
        const createStatus = isSelectedLocalChecklist ? checklistStatusDraft : localSummary.status;
        const createNote = isSelectedLocalChecklist ? checklistNoteDraft.trim() : localSummary.note;
        return {
          clientChecklistId: localSummary.id,
          accountCode: localSummary.accountCode,
          year: localSummary.year,
          month: localSummary.month,
          status: createStatus || null,
          note: createNote || null,
        };
      });

      const checklistUpdates: Array<{ checklistId: string; status?: string | null; note?: string | null }> = [];
      if (activeChecklist && hasDirtyChecklistFields) {
        checklistUpdates.push({
          checklistId: activeChecklist.id,
          status: checklistStatusDraft || null,
          note: checklistNoteDraft.trim() || null,
        });
      }

      const baselinePersistedStationIds = new Set(
        (selectedChecklistBaseline?.stations ?? [])
          .filter((station) => !station.isLocalDraft && station.id > 0)
          .map((station) => station.id),
      );
      const stationDeletes: number[] = [];
      const stationCreates: Array<{
        clientStationId: string;
        checklistId?: string;
        checklistClientId?: string;
        estNum: number;
        stationCode: string;
        status: string | null;
      }> = [];
      const stationUpdates: Array<{ stationRowId: number; status: string | null }> = [];
      for (const stationRowId of previewGeneratedStationDeleteIds) {
        if (!Number.isFinite(stationRowId) || stationRowId <= 0 || stationDeletes.includes(stationRowId)) {
          continue;
        }
        stationDeletes.push(stationRowId);
      }

      if (activeChecklist) {
        const currentPersistedStationIds = new Set(
          activeChecklist.stations
            .map((station) => station.id)
            .filter((stationId) => stationId > 0),
        );
        for (const baselineStationId of baselinePersistedStationIds) {
          if (!currentPersistedStationIds.has(baselineStationId)) {
            stationDeletes.push(baselineStationId);
          }
        }

        for (const station of activeChecklist.stations) {
          if (station.isLocalDraft === true) {
            if (station.isGeneratedPreview === true) {
              continue;
            }
            const finalStatus = stationStatusDraftById[station.id] ?? station.status ?? "";
            const localChecklist = isLocalChecklistId(activeChecklist.id);
            stationCreates.push({
              clientStationId: String(station.id),
              checklistId: localChecklist ? undefined : activeChecklist.id,
              checklistClientId: localChecklist ? activeChecklist.id : undefined,
              estNum: station.estNum,
              stationCode: station.stationCode,
              status: finalStatus || null,
            });
            continue;
          }
        }

        const deletedStationIdSet = new Set<number>(stationDeletes);
        for (const update of dirtyStationStatusEntries.filter((entry) => entry.stationId > 0)) {
          if (deletedStationIdSet.has(update.stationId)) {
            continue;
          }
          stationUpdates.push({
            stationRowId: update.stationId,
            status: update.status || null,
          });
        }
        if (previewGeneratedStationStatusUpdates.length > 0) {
          const existingStationUpdateIdSet = new Set<number>(stationUpdates.map((item) => item.stationRowId));
          for (const update of previewGeneratedStationStatusUpdates) {
            if (deletedStationIdSet.has(update.stationRowId) || existingStationUpdateIdSet.has(update.stationRowId)) {
              continue;
            }
            stationUpdates.push(update);
            existingStationUpdateIdSet.add(update.stationRowId);
          }
        }
      }

      const baselineNoteById = new Map<number, NoteItem>();
      for (const station of selectedChecklistBaseline?.stations ?? []) {
        if (stationDeletes.includes(station.id)) {
          continue;
        }
        for (const note of station.notes) {
          if (note.isLocalDraft) {
            continue;
          }
          baselineNoteById.set(note.id, note);
        }
      }

      const noteCreates: Array<{
        clientNoteId: string;
        checklistStationId?: number;
        checklistStationClientId?: string;
        amount: number | null;
        note: string | null;
      }> = [];
      const noteUpdates: Array<{ noteId: number; amount: number | null; note: string | null }> = [];
      const noteDeletes: number[] = [];
      const attachmentDeletes: number[] = [];
      const attachmentUploads: Array<{ sourceNoteId: number; tempAttachmentId: number; file: File }> = [];

      if (activeChecklist) {
        const currentExistingNoteIds = new Set<number>();
        const currentExistingNoteById = new Map<number, { amount: number | null; note: string }>();
        const currentAttachmentIdSetByNoteId = new Map<number, Set<number>>();

        for (const station of activeChecklist.stations) {
          const isLocalStation = station.isLocalDraft === true || station.id <= 0;
          for (const note of station.notes) {
            if (note.isLocalDraft === true) {
              noteCreates.push({
                clientNoteId: String(note.id),
                checklistStationId: isLocalStation ? undefined : station.id,
                checklistStationClientId: isLocalStation ? String(station.id) : undefined,
                amount: note.amount,
                note: note.note?.trim() || null,
              });
            } else {
              currentExistingNoteIds.add(note.id);
              currentExistingNoteById.set(note.id, {
                amount: note.amount,
                note: note.note.trim(),
              });
            }

            const attachmentIdSet = new Set<number>();
            for (const attachment of note.attachments) {
              if (attachment.isLocalDraft) {
                if (attachment.localFile instanceof File) {
                  attachmentUploads.push({
                    sourceNoteId: note.id,
                    tempAttachmentId: attachment.id,
                    file: attachment.localFile,
                  });
                }
              } else if (attachment.id > 0) {
                attachmentIdSet.add(attachment.id);
              }
            }
            if (!note.isLocalDraft) {
              currentAttachmentIdSetByNoteId.set(note.id, attachmentIdSet);
            }
          }
        }

        for (const baselineId of baselineNoteById.keys()) {
          if (!currentExistingNoteIds.has(baselineId)) {
            noteDeletes.push(baselineId);
          }
        }

        for (const [noteId, currentNote] of currentExistingNoteById.entries()) {
          const baseline = baselineNoteById.get(noteId);
          if (!baseline) {
            continue;
          }
          if (baseline.amount !== currentNote.amount || baseline.note.trim() !== currentNote.note) {
            noteUpdates.push({
              noteId,
              amount: currentNote.amount,
              note: currentNote.note || null,
            });
          }
        }

        for (const [noteId, baselineNote] of baselineNoteById.entries()) {
          if (noteDeletes.includes(noteId)) {
            continue;
          }
          const currentIds = currentAttachmentIdSetByNoteId.get(noteId) ?? new Set<number>();
          for (const attachment of baselineNote.attachments) {
            if (attachment.isLocalDraft) {
              continue;
            }
            if (attachment.id > 0 && !currentIds.has(attachment.id)) {
              attachmentDeletes.push(attachment.id);
            }
          }
        }
      }

      const bulkResponse = await requestJson("/api/tradsphere/v1/invoice-checklists/bulk-save", {
        method: "POST",
        headers: requestHeaders,
        body: {
          year: selectedPeriod.year,
          month: selectedPeriod.month,
          selectedChecklistId: targetSelectedChecklistId,
          createChecklists: checklistCreates,
          checklistUpdates,
          deleteChecklistIds: [],
          createStations: stationCreates,
          stationUpdates,
          deleteStationIds: stationDeletes,
          createNotes: noteCreates,
          noteUpdates,
          deleteNoteIds: noteDeletes,
        },
        successToast: false,
      });

      const bulkData = unwrapData(bulkResponse);
      const bulkRecord = isRecord(bulkData) ? bulkData : {};
      const mappings = isRecord(bulkRecord.mappings) ? bulkRecord.mappings : {};
      const checklistIdMappingRaw = isRecord(mappings.checklistIds) ? mappings.checklistIds : {};
      const noteIdMappingRaw = isRecord(mappings.noteIds) ? mappings.noteIds : {};
      const noteIdMap = new Map<string, number>();
      for (const [clientId, serverIdRaw] of Object.entries(noteIdMappingRaw)) {
        const serverId = asNumber(serverIdRaw);
        if (serverId === null) {
          continue;
        }
        noteIdMap.set(String(clientId), Math.trunc(serverId));
      }
      const mappedChecklistIdSet = new Set<string>();
      for (const serverIdRaw of Object.values(checklistIdMappingRaw)) {
        const serverId = asString(serverIdRaw);
        if (!serverId) {
          continue;
        }
        mappedChecklistIdSet.add(serverId);
      }

      const checklistRowsRaw = Array.isArray(bulkRecord.checklists) ? bulkRecord.checklists : [];
      const bulkChecklists = checklistRowsRaw
        .map(toChecklistSummary)
        .filter((item): item is ChecklistSummary => item !== null);
      const selectedChecklistIdFromBulk = asString(bulkRecord.selectedChecklistId) || null;
      const selectedChecklistFromBulk = toChecklistDetail(bulkRecord.selectedChecklist);

      const failedAttachmentDeletes: number[] = [];
      const failedAttachmentUploads: Array<{ resolvedNoteId: number; tempAttachmentId: number; file: File }> = [];

      for (const attachmentId of attachmentDeletes) {
        const query = new URLSearchParams();
        query.set("attachmentId", String(attachmentId));
        try {
          await requestJson(`/api/tradsphere/v1/invoice-note-attachments?${query.toString()}`, {
            method: "DELETE",
            headers: requestHeaders,
            successToast: false,
            errorToast: false,
          });
        } catch {
          failedAttachmentDeletes.push(attachmentId);
        }
      }

      for (const uploadItem of attachmentUploads) {
        const mappedNoteId = noteIdMap.get(String(uploadItem.sourceNoteId));
        const resolvedNoteId = mappedNoteId ?? uploadItem.sourceNoteId;
        if (!Number.isFinite(resolvedNoteId) || resolvedNoteId <= 0) {
          continue;
        }
        const formData = new FormData();
        formData.set("noteId", String(resolvedNoteId));
        formData.set("file", uploadItem.file);
        try {
          await requestJson("/api/tradsphere/v1/invoice-note-attachments/upload", {
            method: "POST",
            headers: requestHeaders,
            body: formData,
            successToast: false,
            errorToast: false,
          });
        } catch {
          failedAttachmentUploads.push({
            resolvedNoteId,
            tempAttachmentId: uploadItem.tempAttachmentId,
            file: uploadItem.file,
          });
        }
      }

      const baselineChecklistSummaryById = new Map<string, ChecklistSummary>();
      for (const row of checklists) {
        baselineChecklistSummaryById.set(row.id, row);
      }
      const summaryList = bulkChecklists.length > 0
        ? bulkChecklists.map((row) => (
          mergeSavedChecklistSummaryWithBaseline(row, baselineChecklistSummaryById.get(row.id) ?? null)
        ))
        : checklists.filter((item) => item.isLocalDraft !== true);

      let selectedChecklistForCache = mergeSavedChecklistDetailWithBaseline(
        selectedChecklistFromBulk,
        selectedChecklist ?? selectedChecklistBaseline,
      );
      if (!selectedChecklistForCache && selectedChecklistIdFromBulk) {
        const selectedSummary = summaryList.find((item) => item.id === selectedChecklistIdFromBulk) ?? null;
        selectedChecklistForCache = selectedSummary ? toFallbackChecklistDetail(selectedSummary) : null;
      }

      const postSaveFallbackMaps = buildChecklistNameFallbackMaps(
        checklists,
        [
          ...Object.values(localChecklistDetailsById),
          selectedChecklist,
          selectedChecklistBaseline,
          activeChecklist,
          selectedChecklistForCache,
        ].filter((item): item is ChecklistDetail => item !== null),
      );
      const postSavePayload = applyLoadPayloadNameFallbacks({
        periods: [],
        selectedPeriod: null,
        checklists: summaryList,
        selectedChecklistId: selectedChecklistIdFromBulk,
        selectedChecklist: selectedChecklistForCache,
        mismatchStations: Array.isArray(bulkRecord.mismatchStations)
          ? bulkRecord.mismatchStations.map(toChecklistMismatch).filter((item): item is ChecklistMismatchItem => item !== null)
          : [],
      }, postSaveFallbackMaps);
      const summaryListWithNames = postSavePayload.checklists;
      selectedChecklistForCache = postSavePayload.selectedChecklist;
      const mismatchStationsFromBulk = postSavePayload.mismatchStations;

      setChecklists(summaryListWithNames);
      setChecklistCardSummaryOverridesById({});
      setLocalChecklistDetailsById({});
      setLocalChecklistBaselinesById({});
      setLocalRemovedScheduledStationsByChecklistId({});
      setPendingPeriodSyncPreview(null);
      previewRemovedPlannedStationKeysRef.current = new Set();
      setSelectedChecklistId(selectedChecklistIdFromBulk);
      applyChecklistSelection(selectedChecklistForCache);

      const savedAt = Date.now();
      setCacheStatus({ source: "network", fetchedAt: savedAt });
      setError(null);
      const periodValue = selectedPeriod.value;
      const loadCacheKey = buildLoadCacheKey(periodValue);
      const selectedPeriodPayload: PeriodItem = {
        year: selectedPeriod.year,
        month: selectedPeriod.month,
        quarter: selectedPeriod.quarter,
        value: selectedPeriod.value,
        label: formatPeriodQuarterLabel(selectedPeriod.year, selectedPeriod.month),
      };
      const loadPayloadForCache: LoadPayload = {
        periods: [],
        selectedPeriod: selectedPeriodPayload,
        checklists: summaryListWithNames,
        selectedChecklistId: selectedChecklistIdFromBulk,
        selectedChecklist: selectedChecklistForCache,
        mismatchStations: mismatchStationsFromBulk,
      };
      writeBrowserCache(loadCacheKey, loadPayloadForCache, LOAD_CACHE_TTL_MS, {
        source: "network",
        fetchedAt: savedAt,
      });

      const deleted = isRecord(bulkRecord.deleted) ? bulkRecord.deleted : {};
      const deletedChecklistIdsRaw = Array.isArray(deleted.checklistIds) ? deleted.checklistIds : [];
      const detailCacheIdsToInvalidate = new Set<string>();
      if (activeChecklist?.id && !isLocalChecklistId(activeChecklist.id)) {
        detailCacheIdsToInvalidate.add(activeChecklist.id);
      }
      if (selectedChecklistIdFromBulk) {
        detailCacheIdsToInvalidate.add(selectedChecklistIdFromBulk);
      }
      for (const checklistId of mappedChecklistIdSet) {
        detailCacheIdsToInvalidate.add(checklistId);
      }
      for (const checklistId of syncedChecklistIdsToInvalidate) {
        detailCacheIdsToInvalidate.add(checklistId);
      }
      for (const checklistIdRaw of deletedChecklistIdsRaw) {
        const checklistId = asString(checklistIdRaw);
        if (!checklistId) {
          continue;
        }
        detailCacheIdsToInvalidate.add(checklistId);
      }
      for (const checklistId of detailCacheIdsToInvalidate) {
        removeBrowserCache(buildChecklistDetailCacheKey(periodValue, checklistId));
      }
      if (selectedChecklistForCache?.id) {
        writeBrowserCache(
          buildChecklistDetailCacheKey(periodValue, selectedChecklistForCache.id),
          selectedChecklistForCache,
          LOAD_CACHE_TTL_MS,
          { source: "network", fetchedAt: savedAt },
        );
      }

      if (failedAttachmentDeletes.length > 0 || failedAttachmentUploads.length > 0) {
        toast.error(
          "Attachment sync incomplete",
          "Checklist changes were saved, but some attachment changes failed. Please review and save again.",
        );
      }

      clearDeferredUpdate();
      setRefreshMessage(null);
      previewRemovedPlannedStationKeysRef.current = new Set();
      return;
    } finally {
      setIsSavingAllChanges(false);
    }
  }

  function handleStationStatusDraftChange(stationId: number, nextStatus: string) {
    const baselineStatus = selectedChecklist?.stations.find((item) => item.id === stationId)?.status || "";
    setStationStatusDraftById((current) => {
      const next = { ...current };
      if (nextStatus === baselineStatus) {
        delete next[stationId];
        return next;
      }
      next[stationId] = nextStatus;
      return next;
    });
  }

  function discardLocalChanges(options?: { reload?: boolean }) {
    const shouldReload = options?.reload ?? true;
    const targetPeriodValue = loadedPeriodValue || selectedDraftPeriodValue;
    const hadDirtyState = hasUnsavedChanges;
    const hadDeferredServerUpdate = hasDeferredUpdate;
    const checklistIdForReload = isLocalChecklistId(selectedChecklistId) ? null : selectedChecklistId;

    resetLocalDrafts();
    setChecklistCardSummaryOverridesById({});
    setLocalChecklistDetailsById({});
    setLocalChecklistBaselinesById({});
    setLocalRemovedScheduledStationsByChecklistId({});
    setChecklists((current) => current.filter((item) => item.isLocalDraft !== true));
    setIsAddChecklistAccountModalOpen(false);
    setPendingPeriodSyncPreview(null);
    previewRemovedPlannedStationKeysRef.current = new Set();
    if (isLocalChecklistId(selectedChecklistId)) {
      setSelectedChecklistId(null);
      setSelectedChecklist(null);
      setSelectedChecklistBaseline(null);
      setSelectedStationId(null);
      setChecklistStatusDraft("");
      setChecklistNoteDraft("");
      setStationStatusDraftById({});
    }
    clearDeferredUpdate();
    setRefreshMessage(null);

    if (shouldReload && targetPeriodValue && (hadDirtyState || hadDeferredServerUpdate)) {
      void loadData({
        policy: "cache-first",
        periodValue: targetPeriodValue,
        checklistId: checklistIdForReload,
        deferWhenDirty: false,
        preserveLocalDrafts: false,
      });
    }
  }

  function handleOpenAddNoteModal() {
    if (!canEditTradsphere || isSavingAllChanges || !selectedStation) {
      return;
    }
    setIsAddNoteModalOpen(true);
  }

  function handleOpenAddStationModal() {
    if (!canEditTradsphere || isSavingAllChanges || !selectedChecklist) {
      return;
    }
    setIsAddStationModalOpen(true);
  }

  function handleAddStationModalSubmit(payload: AddChecklistStationModalSubmitPayload): string | null {
    if (!selectedChecklist) {
      return "Checklist is not selected.";
    }
    const normalizedStationCode = asString(payload.stationCode).toUpperCase();
    const hasDuplicate = selectedChecklist.stations.some((station) => (
      station.estNum === payload.estNum
      && asString(station.stationCode).toUpperCase() === normalizedStationCode
    ));
    if (hasDuplicate) {
      return "This checklist already has the same Est Num + Station Code.";
    }

    const localStation: StationItem = {
      id: nextLocalId(),
      checklistId: selectedChecklist.id,
      estNum: payload.estNum,
      stationCode: normalizedStationCode,
      stationName: "",
      mediaType: "",
      status: payload.status || "",
      dateUpdated: null,
      inCurrentSchedule: false,
      scheduleMismatch: false,
      scheduleMismatchReason: null,
      repContacts: [],
      notes: [],
      isLocalDraft: true,
    };
    const summary = checklists.find((item) => item.id === selectedChecklist.id) ?? null;
    const summarySearchStations = summary?.searchStations ?? [];

    const sortStations = (stations: StationItem[]): StationItem[] => (
      [...stations].sort((left, right) => (
        left.estNum - right.estNum || left.stationCode.localeCompare(right.stationCode)
      ))
    );

    updateChecklistInState((current) => ({
      ...current,
      stations: sortStations(current.stations.concat(localStation)),
    }));
    updateChecklistSummaryInState(selectedChecklist.id, {
      stationCount: selectedChecklist.stations.length + 1,
      searchStations: summarySearchStations.some((station) => (
        station.estNum === localStation.estNum
        && asString(station.stationCode).toUpperCase() === localStation.stationCode
      ))
        ? summarySearchStations
        : summarySearchStations.concat({
          estNum: localStation.estNum,
          stationCode: localStation.stationCode,
        }),
    });
    forgetLocallyRemovedScheduledStation(selectedChecklist, localStation.estNum, localStation.stationCode);
    setSelectedStationId(localStation.id);
    setIsAddStationModalOpen(false);
    toast.success("Station added locally. Click Save to persist changes.");
    return null;
  }

  function handleOpenAddChecklistAccountModal() {
    if (!canEditTradsphere || isSavingAllChanges) {
      return;
    }
    setIsAddChecklistAccountModalOpen(true);
  }

  function handleAddChecklistAccountModalSubmit(payload: AddChecklistAccountModalSubmitPayload): string | null {
    const accountCode = asString(payload.accountCode).toUpperCase();
    if (!accountCode) {
      return "Account Code is required.";
    }
    if (accountCode.length > 10) {
      return "Account Code must be 10 characters or fewer.";
    }
    const hasDuplicate = checklists.some((item) => asString(item.accountCode).toUpperCase() === accountCode);
    if (hasDuplicate) {
      return "A checklist account for this period already exists.";
    }
    const selectedPeriod = parsePeriodInput(loadedPeriodValue || selectedDraftPeriodValue || "");
    if (!selectedPeriod) {
      return "Load or select a valid period first.";
    }

    const localId = `local-checklist-${Math.abs(nextLocalId())}`;
    const localSummary: ChecklistSummary = {
      id: localId,
      accountCode,
      accountName: "",
      year: selectedPeriod.year,
      month: selectedPeriod.month,
      quarter: selectedPeriod.quarter,
      status: payload.status || "",
      note: payload.note || "",
      stationCount: 0,
      expectedStationCount: 0,
      mismatchStationCount: 0,
      hasScheduleMismatch: false,
      searchStations: [],
      dateUpdated: null,
      isLocalDraft: true,
    };
    const localDetail: ChecklistDetail = {
      id: localId,
      accountCode,
      accountName: "",
      year: selectedPeriod.year,
      month: selectedPeriod.month,
      quarter: selectedPeriod.quarter,
      status: payload.status || "",
      note: payload.note || "",
      dateUpdated: null,
      expectedStationCount: 0,
      mismatchStationCount: 0,
      hasScheduleMismatch: false,
      stations: [],
      isLocalDraft: true,
    };

    setChecklists((current) => {
      const next = current.concat(localSummary);
      return [...next].sort((left, right) => left.accountCode.localeCompare(right.accountCode));
    });
    setLocalChecklistDetailsById((current) => ({ ...current, [localId]: localDetail }));
    setSelectedChecklistId(localId);
    setSelectedChecklist(localDetail);
    setSelectedChecklistBaseline(cloneChecklistDetail(localDetail));
    setChecklistStatusDraft(localDetail.status);
    setChecklistNoteDraft(localDetail.note);
    setStationStatusDraftById({});
    setSelectedStationId(null);
    setIsAddChecklistAccountModalOpen(false);
    toast.success("Checklist account added locally. Click Save to persist changes.");
    return null;
  }

  function applyChecklistRemoval(checklistIdRaw: string): { removedSelected: boolean; nextSelected: string | null } | null {
    const checklistId = asString(checklistIdRaw);
    if (!checklistId) {
      return null;
    }
    const previousSelectedId = selectedChecklistId;
    const nextChecklists = checklists.filter((item) => item.id !== checklistId);
    const removedSelected = previousSelectedId === checklistId;
    const nextSelected = removedSelected ? (nextChecklists[0]?.id ?? null) : previousSelectedId;
    const periodValue = asString(loadedPeriodValue || selectedDraftPeriodValue || "");
    if (periodValue) {
      const loadCacheKey = buildLoadCacheKey(periodValue);
      const existingLoadCacheSnapshot = readBrowserCacheSnapshot<LoadPayload>(loadCacheKey);
      const existingLoadCachePayload = normalizeLoadPayload(existingLoadCacheSnapshot?.data);
      const nextSelectedSummary = nextChecklists.find((item) => item.id === nextSelected) ?? null;
      const nextSelectedDetail = (
        selectedChecklist
        && selectedChecklist.id !== checklistId
        && selectedChecklist.id === nextSelected
      )
        ? selectedChecklist
        : (nextSelectedSummary ? toFallbackChecklistDetail(nextSelectedSummary) : null);
      const nextLoadCachePayload: LoadPayload = {
        ...existingLoadCachePayload,
        checklists: nextChecklists,
        selectedChecklistId: nextSelected,
        selectedChecklist: nextSelectedDetail,
        mismatchStations: existingLoadCachePayload.mismatchStations.filter((row) => row.checklistId !== checklistId),
      };
      const fetchedAt = Date.now();
      writeBrowserCache(loadCacheKey, nextLoadCachePayload, LOAD_CACHE_TTL_MS, {
        source: "network",
        fetchedAt,
      });
      removeBrowserCache(buildChecklistDetailCacheKey(periodValue, checklistId));
      if (nextSelectedDetail?.id) {
        writeBrowserCache(
          buildChecklistDetailCacheKey(periodValue, nextSelectedDetail.id),
          nextSelectedDetail,
          LOAD_CACHE_TTL_MS,
          { source: "network", fetchedAt },
        );
      }
    }

    setChecklists(nextChecklists);
    setLocalChecklistDetailsById((current) => {
      const next = { ...current };
      delete next[checklistId];
      return next;
    });
    setLocalChecklistBaselinesById((current) => {
      const next = { ...current };
      delete next[checklistId];
      return next;
    });
    setLocalRemovedScheduledStationsByChecklistId((current) => {
      const next = { ...current };
      delete next[checklistId];
      return next;
    });

    if (!nextSelected) {
      setSelectedChecklistId(null);
      setSelectedChecklist(null);
      setSelectedChecklistBaseline(null);
      setChecklistStatusDraft("");
      setChecklistNoteDraft("");
      setStationStatusDraftById({});
      setSelectedStationId(null);
      return { removedSelected, nextSelected: null };
    }

    const nextSummary = nextChecklists.find((item) => item.id === nextSelected) ?? null;
    setSelectedChecklistId(nextSelected);
    if (nextSummary?.isLocalDraft) {
      const localDetail = localChecklistDetailsById[nextSelected];
      if (localDetail) {
        setSelectedChecklist(localDetail);
        setSelectedChecklistBaseline(cloneChecklistDetail(localDetail));
        setChecklistStatusDraft(localDetail.status || "");
        setChecklistNoteDraft(localDetail.note || "");
        setSelectedStationId(localDetail.stations[0]?.id ?? null);
        return { removedSelected, nextSelected };
      }
    }

    setSelectedChecklist(null);
    setSelectedChecklistBaseline(null);
    setChecklistStatusDraft("");
    setChecklistNoteDraft("");
    setStationStatusDraftById({});
    setSelectedStationId(null);
    return { removedSelected, nextSelected };
  }

  async function handleConfirmDeleteChecklist() {
    if (!deletingChecklistId) {
      return;
    }
    const targetId = deletingChecklistId;
    if (deletingChecklistIdSet.has(targetId)) {
      return;
    }

    const deletingItem = checklists.find((item) => item.id === targetId) ?? null;
    const isLocalDraft = Boolean(deletingItem?.isLocalDraft) || isLocalChecklistId(targetId);

    setDeletingChecklistIds((current) => (current.includes(targetId) ? current : current.concat(targetId)));
    setDeletingChecklistId(null);

    try {
      if (isLocalDraft) {
        const snapshot = applyChecklistRemoval(targetId);
        if (snapshot?.removedSelected && snapshot.nextSelected && !isLocalChecklistId(snapshot.nextSelected)) {
          void runChecklistSelect(snapshot.nextSelected);
        }
        toast.success("Draft removed", "Unsaved local checklist draft was removed.");
        return;
      }

      setError(null);
      const query = new URLSearchParams();
      query.set("checklistId", targetId);
      await requestJson(`/api/tradsphere/v1/invoice-checklists?${query.toString()}`, {
        method: "DELETE",
        headers: requestHeaders,
        successToast: false,
        errorToast: false,
      });

      const snapshot = applyChecklistRemoval(targetId);
      if (snapshot?.removedSelected && snapshot.nextSelected && !isLocalChecklistId(snapshot.nextSelected)) {
        void runChecklistSelect(snapshot.nextSelected);
      }
      toast.success("Checklist account deleted", "Checklist account was removed from this period.");
    } catch (deleteError) {
      if (isChecklistMissingError(deleteError)) {
        const snapshot = applyChecklistRemoval(targetId);
        if (snapshot?.removedSelected && snapshot.nextSelected && !isLocalChecklistId(snapshot.nextSelected)) {
          void runChecklistSelect(snapshot.nextSelected);
        }
        toast.success("Checklist already removed", "Loaded list was stale and has been corrected.");
        return;
      }
      const message = deleteError instanceof Error && deleteError.message.trim()
        ? deleteError.message.trim()
        : "Failed to delete checklist account.";
      setError(message);
      toast.error("Delete failed", message);
    } finally {
      setDeletingChecklistIds((current) => current.filter((id) => id !== targetId));
    }
  }

  function rememberLocallyRemovedScheduledStation(checklist: ChecklistDetail, station: StationItem) {
    if (station.isLocalDraft === true || station.id <= 0 || !station.inCurrentSchedule) {
      return;
    }
    const checklistId = asString(checklist.id);
    if (!checklistId || isLocalChecklistId(checklistId)) {
      return;
    }
    const accountCode = asString(checklist.accountCode).toUpperCase();
    const stationCode = asString(station.stationCode).toUpperCase();
    if (!accountCode || !stationCode || !Number.isFinite(station.estNum)) {
      return;
    }
    const identityKey = buildStationIdentityKey(accountCode, station.estNum, stationCode);
    const plannedRow: PeriodSyncPlannedStation = {
      checklistId,
      accountCode,
      estNum: Math.trunc(station.estNum),
      stationCode,
      stationName: asString(station.stationName),
      mediaType: asString(station.mediaType),
    };
    setLocalRemovedScheduledStationsByChecklistId((current) => {
      const currentByChecklist = current[checklistId] ?? {};
      return {
        ...current,
        [checklistId]: {
          ...currentByChecklist,
          [identityKey]: plannedRow,
        },
      };
    });
  }

  function forgetLocallyRemovedScheduledStation(checklist: ChecklistDetail, estNum: number, stationCodeRaw: string) {
    const checklistId = asString(checklist.id);
    if (!checklistId || isLocalChecklistId(checklistId)) {
      return;
    }
    const accountCode = asString(checklist.accountCode).toUpperCase();
    const stationCode = asString(stationCodeRaw).toUpperCase();
    if (!accountCode || !stationCode || !Number.isFinite(estNum)) {
      return;
    }
    const identityKey = buildStationIdentityKey(accountCode, estNum, stationCode);
    setLocalRemovedScheduledStationsByChecklistId((current) => {
      const currentByChecklist = current[checklistId];
      if (!currentByChecklist || !currentByChecklist[identityKey]) {
        return current;
      }
      const nextByChecklist = { ...currentByChecklist };
      delete nextByChecklist[identityKey];
      if (Object.keys(nextByChecklist).length === 0) {
        const next = { ...current };
        delete next[checklistId];
        return next;
      }
      return {
        ...current,
        [checklistId]: nextByChecklist,
      };
    });
  }

  function handleConfirmDeleteStation() {
    if (!selectedChecklist || deletingStationId === null) {
      return;
    }
    const removingStationId = deletingStationId;
    const removingStationFromView = selectedChecklistForView?.stations.find((station) => station.id === removingStationId) ?? null;
    if (removingStationFromView?.isGeneratedPreview === true && pendingPeriodSyncPreview) {
      const selectedChecklistIdForRemove = asString(selectedChecklist.id);
      const selectedAccountCodeForRemove = asString(selectedChecklist.accountCode).toUpperCase();
      const removingEstNum = Math.trunc(removingStationFromView.estNum);
      const removingStationCode = asString(removingStationFromView.stationCode).toUpperCase();
      const removedStationKey = buildStationIdentityKey(
        selectedAccountCodeForRemove,
        removingEstNum,
        removingStationCode,
      );
      previewRemovedPlannedStationKeysRef.current.add(removedStationKey);

      setPendingPeriodSyncPreview((current) => {
        if (!current) {
          return current;
        }
        const nextPlannedStations = current.plannedStations.filter((row) => {
          const rowAccountCode = asString(row.accountCode).toUpperCase();
          const rowStationCode = asString(row.stationCode).toUpperCase();
          const rowChecklistId = asString(row.checklistId);
          const sameStation = (
            rowAccountCode === selectedAccountCodeForRemove
            && Math.trunc(row.estNum) === removingEstNum
            && rowStationCode === removingStationCode
          );
          if (!sameStation) {
            return true;
          }
          // Existing checklist proposals carry checklistId; generated checklist proposals keep checklistId null.
          if (rowChecklistId) {
            return rowChecklistId !== selectedChecklistIdForRemove;
          }
          return false;
        });
        if (nextPlannedStations.length === current.plannedStations.length) {
          return current;
        }
        return {
          ...current,
          plannedStations: nextPlannedStations,
          plannedStationsCount: nextPlannedStations.length,
        };
      });

      // If this preview row is part of a generated preview checklist detail, also remove it from local detail state.
      const removingStation = selectedChecklist.stations.find((station) => station.id === removingStationId) ?? null;
      if (removingStation) {
        rememberLocallyRemovedScheduledStation(selectedChecklist, removingStation);
        const nextStations = selectedChecklist.stations.filter((station) => station.id !== removingStationId);
        const mismatchMeta = deriveMismatchMetaFromStations(nextStations);
        const summary = checklists.find((item) => item.id === selectedChecklist.id) ?? null;
        const summarySearchStations = summary?.searchStations ?? [];
        updateChecklistInState((current) => ({
          ...current,
          stations: current.stations.filter((station) => station.id !== removingStationId),
          mismatchStationCount: mismatchMeta.mismatchStationCount,
          hasScheduleMismatch: mismatchMeta.hasScheduleMismatch,
        }));
        updateChecklistSummaryInState(selectedChecklist.id, {
          stationCount: Math.max(0, selectedChecklist.stations.length - 1),
          mismatchStationCount: mismatchMeta.mismatchStationCount,
          hasScheduleMismatch: mismatchMeta.hasScheduleMismatch,
          searchStations: summarySearchStations.filter((station) => !(
            station.estNum === removingStation.estNum
            && asString(station.stationCode).toUpperCase() === asString(removingStation.stationCode).toUpperCase()
          )),
        });
      }

      setStationStatusDraftById((current) => {
        const next = { ...current };
        delete next[removingStationId];
        return next;
      });
      if (selectedStationId === removingStationId) {
        const fallbackStationId = selectedChecklistForView?.stations.find((station) => station.id !== removingStationId)?.id ?? null;
        setSelectedStationId(fallbackStationId);
      }
      setDeletingStationId(null);
      return;
    }

    const removingStation = selectedChecklist.stations.find((station) => station.id === removingStationId) ?? null;
    if (removingStation) {
      rememberLocallyRemovedScheduledStation(selectedChecklist, removingStation);
    }
    const nextStations = selectedChecklist.stations.filter((station) => station.id !== removingStationId);
    const mismatchMeta = deriveMismatchMetaFromStations(nextStations);
    const summary = checklists.find((item) => item.id === selectedChecklist.id) ?? null;
    const summarySearchStations = summary?.searchStations ?? [];
    updateChecklistInState((current) => ({
      ...current,
      stations: current.stations.filter((station) => station.id !== removingStationId),
      mismatchStationCount: mismatchMeta.mismatchStationCount,
      hasScheduleMismatch: mismatchMeta.hasScheduleMismatch,
    }));
    updateChecklistSummaryInState(selectedChecklist.id, {
      stationCount: Math.max(0, selectedChecklist.stations.length - 1),
      mismatchStationCount: mismatchMeta.mismatchStationCount,
      hasScheduleMismatch: mismatchMeta.hasScheduleMismatch,
      searchStations: removingStation
        ? summarySearchStations.filter((station) => !(
          station.estNum === removingStation.estNum
          && asString(station.stationCode).toUpperCase() === removingStation.stationCode
        ))
        : summarySearchStations,
    });
    setStationStatusDraftById((current) => {
      const next = { ...current };
      delete next[removingStationId];
      return next;
    });
    if (selectedStationId === removingStationId) {
      const fallbackStationId = selectedChecklist.stations.find((station) => station.id !== removingStationId)?.id ?? null;
      setSelectedStationId(fallbackStationId);
    }
    setDeletingStationId(null);
  }

  function handleAddNoteModalSubmit(payload: AddStationNoteModalSubmitPayload) {
    if (!selectedStation) {
      return;
    }

    const tempNoteId = nextLocalId();
    const attachments: AttachmentItem[] = payload.files.map((file) => ({
      id: nextLocalId(),
      noteId: tempNoteId,
      url: URL.createObjectURL(file),
      fileName: file.name,
      fileType: file.type || "",
      mimeType: file.type || "",
      fileSize: file.size,
      dateCreated: null,
      dateUpdated: null,
      isLocalDraft: true,
      localFile: file,
    }));

    updateChecklistInState((current) => ({
      ...current,
      stations: current.stations.map((station) => {
        if (station.id !== selectedStation.id) {
          return station;
        }
        return {
          ...station,
          notes: [...station.notes, {
            id: tempNoteId,
            checklistStationId: selectedStation.id,
            amount: payload.amount,
            note: payload.note,
            dateCreated: null,
            dateUpdated: null,
            attachments,
            isLocalDraft: true,
          }],
        };
      }),
    }));
    setIsAddNoteModalOpen(false);
  }

  function handleEditNoteModalSubmit(payload: EditStationNoteModalSubmitPayload) {
    if (editingNoteId === null) {
      return;
    }
    const editingContext = noteContextById.get(editingNoteId);
    if (!editingContext) {
      return;
    }
    const targetStationId = editingContext.station.id;
    const localAttachments: AttachmentItem[] = payload.files.map((file) => ({
      id: nextLocalId(),
      noteId: editingNoteId,
      url: URL.createObjectURL(file),
      fileName: file.name,
      fileType: file.type || "",
      mimeType: file.type || "",
      fileSize: file.size,
      dateCreated: null,
      dateUpdated: null,
      isLocalDraft: true,
      localFile: file,
    }));
    const removedAttachmentIdSet = new Set(payload.removedAttachmentIds);
    updateChecklistInState((current) => ({
      ...current,
      stations: current.stations.map((station) => {
        if (station.id !== targetStationId) {
          return station;
        }
        return {
          ...station,
          notes: station.notes.map((note) => (
            note.id === editingNoteId
                ? {
                    ...note,
                    amount: payload.amount,
                    note: payload.note,
                    attachments: note.attachments
                      .filter((attachment) => !removedAttachmentIdSet.has(attachment.id))
                      .concat(localAttachments),
                }
              : note
          )),
        };
      }),
    }));
    setEditingNoteId(null);
  }

  function handleConfirmDeleteNote() {
    if (deletingNoteId === null) {
      return;
    }
    const deletingContext = noteContextById.get(deletingNoteId);
    if (!deletingContext) {
      return;
    }
    const targetStationId = deletingContext.station.id;
    updateChecklistInState((current) => ({
      ...current,
      stations: current.stations.map((station) => {
        if (station.id !== targetStationId) {
          return station;
        }
        return {
          ...station,
          notes: station.notes.filter((note) => note.id !== deletingNoteId),
        };
      }),
    }));
    if (editingNoteId === deletingNoteId) {
      setEditingNoteId(null);
    }
    setDeletingNoteId(null);
  }

  const rollingPeriods = useMemo(() => buildRollingPeriods(2, 2, PREVIOUS_MONTH_ANCHOR), []);

  useEffect(() => {
    if (normalizedPeriodSelectionValue === CUSTOM_PERIOD_OPTION_VALUE) {
      return;
    }
    const existsInRolling = rollingPeriods.some((item) => item.value === normalizedPeriodSelectionValue);
    if (existsInRolling) {
      return;
    }
    const parsed = parsePeriodInput(normalizedPeriodSelectionValue);
    if (parsed) {
      setCustomPeriodMonth(String(parsed.month));
      setCustomPeriodYear(String(parsed.year));
    }
    setPeriodSelectionValue(CUSTOM_PERIOD_OPTION_VALUE);
  }, [
    normalizedPeriodSelectionValue,
    rollingPeriods,
    setCustomPeriodMonth,
    setCustomPeriodYear,
    setPeriodSelectionValue,
  ]);

  const periodOptions = useMemo(() => {
    return rollingPeriods.map((item) => ({
      value: item.value,
      label: formatPeriodQuarterLabel(item.year, item.month),
    })).concat([{ value: CUSTOM_PERIOD_OPTION_VALUE, label: "Custom" }]);
  }, [rollingPeriods]);

  const customMonthOptions = useMemo(() => {
    return Array.from({ length: 12 }, (_, index) => {
      const month = index + 1;
      return {
        value: String(month),
        label: formatPeriodMonthName(month),
      };
    });
  }, []);

  const customYearOptions = useMemo(() => {
    const currentYear = new Date().getFullYear();
    const years: { value: string; label: string }[] = [];
    for (let year = currentYear - 10; year <= currentYear + 10; year += 1) {
      years.push({ value: String(year), label: String(year) });
    }
    return years;
  }, []);

  const handleRestoreLoadedPeriodSelection = useCallback(() => {
    const loaded = parsePeriodInput(loadedPeriodValue || "");
    if (!loaded) {
      return;
    }
    const loadedValue = loaded.value;
    const existsInRolling = rollingPeriods.some((item) => item.value === loadedValue);
    if (existsInRolling) {
      setPeriodSelectionValue(loadedValue);
      return;
    }
    setCustomPeriodMonth(String(loaded.month));
    setCustomPeriodYear(String(loaded.year));
    setPeriodSelectionValue(CUSTOM_PERIOD_OPTION_VALUE);
  }, [
    loadedPeriodValue,
    rollingPeriods,
    setCustomPeriodMonth,
    setCustomPeriodYear,
    setPeriodSelectionValue,
  ]);

  const canLoadSelectedPeriod = selectedDraftPeriodValue.length > 0;
  const loadedPeriod = useMemo(
    () => parsePeriodInput(loadedPeriodValue || ""),
    [loadedPeriodValue],
  );
  const isSelectedPeriodLoaded = Boolean(
    loadedPeriod
    && selectedDraftPeriodValue
    && loadedPeriod.value === selectedDraftPeriodValue,
  );
  const hasPendingPeriodSelectionAfterLoad = Boolean(
    loadedPeriod
    && selectedDraftPeriodValue
    && loadedPeriod.value !== selectedDraftPeriodValue,
  );
  const isLoadedPeriodChecklistEmpty = isSelectedPeriodLoaded && checklists.length === 0;
  const periodActionLabel = "Sync Checklist";
  const canRunPeriodAction = (
    isSelectedPeriodLoaded
    && canEditTradsphere
    && !isLoading
    && !isRefreshing
    && !isSavingAllChanges
    && !isSyncingPeriod
  );
  const shouldShowSaveActions = canEditTradsphere && (hasUnsavedChanges || isSavingAllChanges || Boolean(pendingPeriodSyncPreview));
  const visibleRefreshMessage = useMemo(() => {
    if (!refreshMessage) {
      return null;
    }
    if (refreshMessage === DEFERRED_REFRESH_MESSAGE && !hasUnsavedChanges) {
      return null;
    }
    return refreshMessage;
  }, [hasUnsavedChanges, refreshMessage]);

  const cacheStatusText = isRefreshing
    ? "Refreshing..."
    : !isOnline && cacheStatus
      ? `Offline. Showing cached data from ${formatRelativeTime(cacheStatus.fetchedAt)}.`
      : cacheStatus
        ? `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`
        : "No cached data yet";
  const pageMessages: StackMessage[] = [];
  if (visibleRefreshMessage) {
    pageMessages.push({
      id: "invoice-refresh-message",
      variant: visibleRefreshMessage.toLowerCase().includes("offline") ? "info" : "warning",
      message: visibleRefreshMessage,
    });
  }
  if (error) {
    pageMessages.push({
      id: "invoice-load-error",
      variant: "error",
      message: error,
    });
  }

  const checklistSummaryDescription = normalizedAppliedSearch
    ? `${filteredChecklists.length} of ${checklists.length} checklist(s)`
    : `${checklists.length} checklist(s) in selected period`;
  const loadingContract = resolveSharedLoadingContract(
    {
      pageInitializing: !hasHydratedPageState,
      pageRefreshing: isLoadActionOverlayVisible || isSyncingPeriod,
      cacheChipRefreshing: isChipRefreshing,
    },
    {
      pageInitializing: "Preparing invoice checklist workspace...",
      pageRefreshing: isLoadActionOverlayVisible
        ? "Loading checklist data..."
        : (isLoadedPeriodChecklistEmpty ? "Generating checklist preview..." : "Updating checklist preview..."),
      cacheChipRefreshing: "Loading latest checklist data...",
    },
  );

  const applySearchKeyword = useCallback((rawValue: string) => {
    const normalized = asString(rawValue);
    const normalizedKeyword = normalizeSearchKeyword(normalized);
    const hadActiveSearch = Boolean(normalizeSearchKeyword(appliedSearch));

    const run = async () => {
      if (normalizedKeyword && !hadActiveSearch) {
        selectionBeforeSearchRef.current = {
          checklistId: selectedChecklistId,
          stationId: selectedStationId,
        };
      }
      setDraftSearch(normalized);
      setAppliedSearch(normalized);

      if (!normalizedKeyword && hadActiveSearch) {
        const snapshot = selectionBeforeSearchRef.current;
        selectionBeforeSearchRef.current = null;
        if (snapshot?.checklistId) {
          const hasSnapshotChecklist = checklists.some((item) => item.id === snapshot.checklistId);
          if (hasSnapshotChecklist) {
            pendingStationRestoreAfterChecklistSelectRef.current = snapshot.stationId;
            setSelectedStationId(null);
            setIsAddNoteModalOpen(false);
            setIsAddStationModalOpen(false);
            setEditingNoteId(null);
            setDeletingNoteId(null);
            setDeletingStationId(null);
            setAttachmentModalNoteId(null);
            if (snapshot.checklistId !== selectedChecklistId) {
              await runChecklistSelect(snapshot.checklistId);
              return;
            }
            if (snapshot.stationId !== null && selectedChecklist?.stations.some((station) => station.id === snapshot.stationId)) {
              setSelectedStationId(snapshot.stationId);
            }
            return;
          }
        }
      }

      if (!selectedChecklistId) {
        return;
      }
      const nextFiltered = filterChecklistsByKeyword(checklists, normalizedKeyword);
      const visibleIds = new Set(nextFiltered.map((item) => item.id));
      if (!visibleIds.has(selectedChecklistId)) {
        const firstVisibleChecklist = nextFiltered[0] ?? null;
        if (firstVisibleChecklist) {
          setSelectedStationId(null);
          setIsAddNoteModalOpen(false);
          setIsAddStationModalOpen(false);
          setEditingNoteId(null);
          setDeletingNoteId(null);
          setDeletingStationId(null);
          setAttachmentModalNoteId(null);
          await runChecklistSelect(firstVisibleChecklist.id);
          return;
        }
        setSelectedChecklistId(null);
        setSelectedChecklist(null);
        setSelectedChecklistBaseline(null);
        setChecklistStatusDraft("");
        setChecklistNoteDraft("");
        setStationStatusDraftById({});
        setSelectedStationId(null);
        setIsAddNoteModalOpen(false);
        setIsAddStationModalOpen(false);
        setEditingNoteId(null);
        setDeletingNoteId(null);
        setDeletingStationId(null);
        setAttachmentModalNoteId(null);
        return;
      }

      if (!selectedChecklist || selectedStationId === null) {
        return;
      }
      const nextVisibleStations = filterStationsForChecklist(selectedChecklist, normalizedKeyword);
      const hasSelectedStation = nextVisibleStations.some((station) => station.id === selectedStationId);
      if (hasSelectedStation) {
        return;
      }
      setSelectedStationId(null);
      setIsAddNoteModalOpen(false);
      setEditingNoteId(null);
      setDeletingNoteId(null);
      setDeletingStationId(null);
      setAttachmentModalNoteId(null);
    };

    void run();
  }, [
    appliedSearch,
    checklists,
    runChecklistSelect,
    selectedChecklist,
    selectedChecklistId,
    selectedStationId,
  ]);

  const applySearchFromDraft = useCallback(() => {
    applySearchKeyword(draftSearch);
  }, [applySearchKeyword, draftSearch]);

  function handleSearchInputKeyDown(event: ReactKeyboardEvent<HTMLInputElement>) {
    if (event.key === "Enter") {
      event.preventDefault();
      applySearchFromDraft();
      return;
    }
    if (event.key === "Tab") {
      applySearchFromDraft();
    }
  }

  function handleClearSearch() {
    applySearchKeyword("");
  }

  function handleSearchInputChange(nextValue: string) {
    setDraftSearch(nextValue);
    if (!asString(nextValue)) {
      applySearchKeyword("");
    }
  }

  useEffect(() => {
    const normalizedDraft = asString(draftSearch);
    if (!normalizedDraft) {
      return;
    }
    if (normalizeSearchKeyword(normalizedDraft) === normalizeSearchKeyword(appliedSearch)) {
      return;
    }
    const timer = window.setTimeout(() => {
      applySearchKeyword(normalizedDraft);
    }, 1000);
    return () => {
      window.clearTimeout(timer);
    };
  }, [appliedSearch, applySearchKeyword, draftSearch]);

  return (
    <AppPageLayout
      className="pb-5"
      pageMessages={<PageMessageStack messages={pageMessages} />}
      banner={(
        <PageBanner
          eyebrow="TradSphere"
          title="Invoice Reconciliation Checklist"
          description="Load a billing period, review checklist status by account and station, and track reconciliation notes with proof links."
          gradientVariant="app"
        />
      )}
      footer={cacheStatus ? (
        <PageCacheFooter
          text={cacheStatusText}
          onRefresh={handleRefreshFromChip}
          disabled={!isOnline || !loadedPeriodValue || isRefreshing || isChipRefreshing || isSavingAllChanges || isSyncingPeriod || hasUnsavedChanges || isDeletingChecklist}
          refreshing={isRefreshing || isChipRefreshing || isDeletingChecklist}
          refreshLabel="Refresh invoice checklist"
          tooltipText={
            isDeletingChecklist
              ? "Checklist delete is in progress."
              : hasUnsavedChanges
                ? "Save or discard changes before refreshing checklist data."
                : isOnline
                  ? "Click to refresh selected checklist data"
                  : "Offline. Reconnect to refresh checklist data."
          }
          containerClassName="w-full"
        />
      ) : null}
    >

      <SectionCard title="Period & Load" divider={false} contentClassName="pt-1">
        <LoadActionArea
          controls={(
            <div className={isCustomPeriodSelection ? "space-y-2" : undefined}>
              <AppDropdown
                value={normalizedPeriodSelectionValue}
                onValueChange={setPeriodSelectionValue}
                options={periodOptions}
                searchable={false}
                placeholder=""
                disabled={isLoading || isRefreshing || isSyncingPeriod}
              />
              {isCustomPeriodSelection ? (
                <div className="grid gap-2 sm:grid-cols-2">
                  <AppDropdown
                    value={normalizedCustomMonth}
                    onValueChange={setCustomPeriodMonth}
                    options={customMonthOptions}
                    searchable={false}
                    placeholder=""
                    disabled={isLoading || isRefreshing || isSyncingPeriod}
                  />
                  <AppDropdown
                    value={normalizedCustomYear}
                    onValueChange={setCustomPeriodYear}
                    options={customYearOptions}
                    searchable={false}
                    placeholder=""
                    disabled={isLoading || isRefreshing || isSyncingPeriod}
                  />
                </div>
              ) : null}
            </div>
          )}
          actions={(
            <>
              <Button
                onClick={handleLoadClick}
                disabled={!canLoadSelectedPeriod || isLoading || isRefreshing || isSavingAllChanges || isSyncingPeriod}
              >
                Load
              </Button>
              {isSelectedPeriodLoaded ? (
                <Button
                  variant={isLoadedPeriodChecklistEmpty ? "default" : "outline"}
                  onClick={() => void handleSyncChecklistPeriod()}
                  disabled={!canRunPeriodAction}
                >
                  {isSyncingPeriod ? (
                    <>
                      <Loader2 className="mr-2 size-4 animate-spin" />
                      Syncing...
                    </>
                  ) : periodActionLabel}
                </Button>
              ) : null}
            </>
          )}
        />
      </SectionCard>

      <div className="relative grid gap-4 xl:h-full xl:min-h-0 xl:flex-1 xl:grid-cols-[minmax(0,1fr)_minmax(0,1.3fr)_minmax(0,1.1fr)] xl:grid-rows-[minmax(0,1fr)]">
        <SectionCard
          className="xl:h-full xl:min-h-0"
          contentClassName="xl:min-h-0 xl:overflow-y-auto xl:pr-1"
          title="Checklist Accounts"
          description={checklistSummaryDescription}
          actions={(
            <ActionIconButton
              icon={<Plus />}
              tooltip="Add checklist account"
              onClick={handleOpenAddChecklistAccountModal}
              disabled={!canEditTradsphere || isSavingAllChanges}
              className="!h-7 !w-7 !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-4 [&_svg]:!w-4 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110"
              aria-label="Add checklist account"
            />
          )}
        >
            <div className="mb-3">
              <div className="relative">
                <Input
                  value={draftSearch}
                  onChange={(event) => handleSearchInputChange(event.target.value)}
                  onBlur={applySearchFromDraft}
                  onKeyDown={handleSearchInputKeyDown}
                  placeholder="Filter by account, est num, or station code"
                  className="pr-9 transition !outline-none ![box-shadow:none] !focus:outline-none !focus:ring-0 !focus:ring-offset-0 !focus:border-slate-300 !focus:shadow-none !focus:[box-shadow:none] !focus-visible:outline-none !focus-visible:ring-0 !focus-visible:ring-offset-0 !focus-visible:border-slate-300 !focus-visible:shadow-none !focus-visible:[box-shadow:none]"
                />
                {draftSearch ? (
                  <button
                    type="button"
                    onClick={handleClearSearch}
                    className="absolute right-2 top-1/2 inline-flex size-6 -translate-y-1/2 items-center justify-center rounded-full text-slate-500 transition hover:bg-slate-100 hover:text-slate-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-300"
                    aria-label="Clear search"
                    title="Clear search"
                  >
                    <X className="size-3.5" />
                  </button>
                ) : null}
              </div>
            </div>
            {checklists.length === 0 ? (
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                No checklists found for this period.
              </div>
            ) : filteredChecklists.length === 0 ? (
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                No checklists match your keyword filter.
              </div>
            ) : (
              <div className="space-y-2">
                {filteredChecklists.map((item) => {
                  const active = selectedChecklistId === item.id;
                  const isDeletingCard = deletingChecklistIdSet.has(item.id);
                  const serverStationCount = Math.max(0, Math.trunc(item.stationCount));
                  const serverMatchedStationCount = Math.max(
                    0,
                    Math.min(serverStationCount, Math.trunc(item.matchedStationCount ?? 0)),
                  );
                  const canUseLiveSummary = Boolean(
                    active
                    && selectedChecklistForView
                    && selectedChecklistForView.id === item.id,
                  );
                  const liveStationCount = canUseLiveSummary
                    ? Math.max(0, selectedChecklistForView?.stations.length ?? 0)
                    : serverStationCount;
                  const liveMatchedStationCount = canUseLiveSummary
                    ? (selectedChecklistForView?.stations.reduce((acc, station) => {
                      const effectiveStationStatus = asString(
                        stationStatusDraftById[station.id] ?? station.status ?? "",
                      ).toLowerCase();
                      return acc + (effectiveStationStatus.startsWith("matched") ? 1 : 0);
                    }, 0) ?? 0)
                    : serverMatchedStationCount;
                  const stationCount = liveStationCount;
                  const matchedStationCount = Math.max(
                    0,
                    Math.min(stationCount, liveMatchedStationCount),
                  );
                  const liveMatchedAll = stationCount > 0 && matchedStationCount === stationCount;
                  const effectiveStatus = active
                    ? (
                      liveMatchedAll
                        ? CHECKLIST_STATUS_MATCHED_ALL
                        : (checklistStatusDraft === CHECKLIST_STATUS_MATCHED_ALL ? "" : checklistStatusDraft)
                    )
                    : item.status;
                  const statusText = asString(effectiveStatus);
                  const normalizedStatus = statusText.replace(/\s+/g, "_").toUpperCase();
                  const isMatchedAll = (
                    normalizedStatus === "MATCHED"
                    || normalizedStatus === "MATCHED_ALL"
                    || normalizedStatus === "ALL_MATCHED"
                    || (active && liveMatchedAll)
                  );
                  const isProposalDelete = item.proposalState === "proposed_remove";
                  const isProposalAdd = item.proposalState === "proposed_add";
                  const isProposalChanged = item.proposalState === "changed";
                  const accountTitle = item.accountName
                    ? `${item.accountName} (${item.accountCode})`
                    : item.accountCode;
                  return (
                    <div
                      key={item.id}
                      role="button"
                      aria-disabled={isDeletingCard}
                      tabIndex={0}
                      onClick={() => {
                        if (isDeletingCard) {
                          return;
                        }
                        handleChecklistSelect(item.id);
                      }}
                      onKeyDown={(event) => {
                        if (isDeletingCard) {
                          return;
                        }
                        if (event.key === "Enter" || event.key === " ") {
                          event.preventDefault();
                          handleChecklistSelect(item.id);
                        }
                      }}
                      className={[
                        "group w-full rounded-xl border px-3 py-3 text-left transition",
                        isDeletingCard ? "cursor-not-allowed opacity-70" : "",
                        isProposalDelete
                          ? (active ? "border-rose-300 bg-rose-50/80" : "border-rose-200 bg-rose-50/40 hover:border-rose-300")
                          : isProposalAdd
                            ? (active ? "border-cyan-300 bg-cyan-50/80" : "border-cyan-200 bg-cyan-50/40 hover:border-cyan-300")
                            : isProposalChanged
                              ? (active ? "border-sky-300 bg-sky-50/80" : "border-sky-200 bg-sky-50/40 hover:border-sky-300")
                              : active
                                ? "border-blue-300 bg-blue-50/70"
                                : "border-blue-100 bg-white hover:border-blue-200 hover:bg-blue-50/30",
                      ].join(" ")}
                    >
                      <div className="flex items-center justify-between gap-3">
                        <div className="min-w-0">
                          <p className="truncate text-sm font-semibold text-slate-900">{accountTitle}</p>
                          <div className="mt-1 flex items-center gap-2">
                            {isMatchedAll ? (
                              <span className="inline-flex rounded-full border border-emerald-300 bg-emerald-50 px-2 py-0.5 text-[11px] font-medium text-emerald-700">
                                Match all
                              </span>
                            ) : null}
                            {item.mismatchStationCount > 0 ? (
                              <span className="inline-flex items-center gap-1 rounded-full border border-amber-300 bg-amber-50 px-2 py-0.5 text-[11px] font-medium text-amber-800">
                                <AlertTriangle className="size-3" />
                                {item.mismatchStationCount} stations no schedule
                              </span>
                            ) : null}
                            {item.proposalLabel ? (
                              <span
                                className={[
                                  "inline-flex rounded-full border px-2 py-0.5 text-[11px] font-semibold",
                                  item.proposalState === "proposed_remove"
                                    ? "border-rose-300 bg-rose-100 text-rose-800"
                                    : item.proposalState === "proposed_add"
                                      ? "border-cyan-300 bg-cyan-100 text-cyan-800"
                                      : "border-sky-300 bg-sky-100 text-sky-800",
                                ].join(" ")}
                              >
                                {item.proposalLabel}
                              </span>
                            ) : null}
                            <span className="whitespace-nowrap text-[11px] text-slate-500">
                              {matchedStationCount}/{stationCount} station Matched
                            </span>
                          </div>
                        </div>
                        <div className="flex items-center gap-1.5">
                          {!isMatchedAll && statusText ? (
                            <span className="rounded-full border border-blue-200 bg-white px-2 py-0.5 text-[11px] font-medium text-blue-700">
                              {statusText}
                            </span>
                          ) : null}
                          <div className="flex items-center gap-0.5 transition-opacity duration-150 max-md:opacity-100 md:pointer-events-none md:opacity-0 md:group-hover:pointer-events-auto md:group-hover:opacity-100 md:group-focus-within:pointer-events-auto md:group-focus-within:opacity-100">
                            <ActionIconButton
                              icon={isDeletingCard ? <Loader2 className="animate-spin" /> : <Trash2 />}
                              tooltip={isDeletingCard ? "Deleting checklist account..." : "Delete checklist account"}
                              onClick={(event) => {
                                event.stopPropagation();
                                if (isDeletingCard) {
                                  return;
                                }
                                setDeletingChecklistId(item.id);
                              }}
                              disabled={!canEditTradsphere || isSavingAllChanges || isDeletingCard}
                              className="!h-6 !w-6 !rounded-full !p-0 text-rose-500 hover:!bg-rose-50 hover:!scale-105 hover:text-rose-600 focus-visible:!bg-rose-50 focus-visible:!scale-105 focus-visible:text-rose-600 [&_svg]:!h-3.5 [&_svg]:!w-3.5 [&_svg]:text-rose-500 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110 hover:[&_svg]:text-rose-600 focus-visible:[&_svg]:text-rose-600"
                              aria-label="Delete checklist account"
                            />
                          </div>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
        </SectionCard>

        <div className="relative xl:h-full xl:min-h-0">
        <SectionCard className="xl:h-full xl:min-h-0" title="Checklist & Stations">
            {!selectedChecklistForView ? (
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                Select a checklist account to review details.
              </div>
            ) : (
              <div className="flex h-full min-h-0 flex-col">
                <div className="space-y-4">
                  <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-slate-600">
                    <p className="w-full truncate text-base font-semibold text-slate-900">
                      {selectedChecklistForView.accountName || selectedChecklistForView.accountCode}
                    </p>
                    <span>{formatPeriodQuarterLabel(selectedChecklistForView.year, selectedChecklistForView.month)} - {monthLabel(selectedChecklistForView.month)}</span>
                    {selectedChecklistForView.mismatchStationCount > 0 ? (
                      <span className="inline-flex items-center gap-1 rounded-full border border-amber-300 bg-amber-50 px-2 py-0.5 text-[11px] font-medium text-amber-800">
                        <AlertTriangle className="size-3" />
                        {selectedChecklistForView.mismatchStationCount} stations no schedule
                      </span>
                    ) : null}
                    {selectedChecklistForView.proposalLabel ? (
                      <span
                        className={[
                          "inline-flex rounded-full border px-2 py-0.5 text-[11px] font-semibold",
                          selectedChecklistForView.proposalState === "proposed_remove"
                            ? "border-rose-300 bg-rose-100 text-rose-800"
                            : selectedChecklistForView.proposalState === "proposed_add"
                              ? "border-cyan-300 bg-cyan-100 text-cyan-800"
                              : "border-sky-300 bg-sky-100 text-sky-800",
                        ].join(" ")}
                      >
                        {selectedChecklistForView.proposalLabel}
                      </span>
                    ) : null}
                    {hasUnsavedChanges ? (
                      <span className="rounded-full border border-amber-300 bg-amber-50 px-2 py-0.5 font-medium text-amber-800">
                        Unsaved changes
                      </span>
                    ) : null}
                  </div>

                  <LabeledField label="Checklist Status">
                    <AppDropdown
                      value={checklistStatusDraft}
                      onValueChange={handleChecklistStatusDraftChange}
                      options={checklistStatusOptions.map((value) => ({ value, label: value }))}
                      searchable
                      allowCustomValue
                      placeholder=""
                      disabled={!canEditTradsphere || isSavingAllChanges}
                    />
                  </LabeledField>

                  <LabeledField label="Checklist Note" alignStart>
                    <Textarea
                      value={checklistNoteDraft}
                      onChange={(event) => setChecklistNoteDraft(event.target.value)}
                      placeholder="Add summary note..."
                      disabled={!canEditTradsphere || isSavingAllChanges}
                      className="min-h-[96px] resize-y"
                    />
                  </LabeledField>
                </div>

                <div className="mt-8 flex min-h-0 flex-1 flex-col">
                  <div className="mb-3 flex items-center justify-between gap-2 border-b border-slate-200 pb-2">
                    <p className="text-sm font-semibold text-slate-800">Stations</p>
                    <ActionIconButton
                      icon={<Plus />}
                      tooltip="Add station"
                      onClick={handleOpenAddStationModal}
                      disabled={!canEditTradsphere || !selectedChecklistForView || isSavingAllChanges}
                      className="!h-7 !w-7 !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-4 [&_svg]:!w-4 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110"
                      aria-label="Add station"
                    />
                  </div>
                  {selectedChecklistForView.stations.length === 0 ? (
                    <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                      No stations linked to this checklist yet.
                    </div>
                  ) : filteredStationsForSelectedChecklist.length === 0 ? (
                    <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                      No stations match your keyword filter.
                    </div>
                  ) : (
                    <div className="min-h-0 flex-1 space-y-2 xl:overflow-y-auto xl:pr-1">
                      {filteredStationsForSelectedChecklist.map((station) => {
                        const active = selectedStationId === station.id;
                        const draftStatus = stationStatusDraftById[station.id] ?? station.status ?? "";
                        const isProposalDelete = station.proposalState === "proposed_remove";
                        const isProposalAdd = station.proposalState === "proposed_add";
                        const isProposalChanged = station.proposalState === "changed";
                        const disableStationStatusMutations = (
                          !canEditTradsphere
                          || isSavingAllChanges
                        );
                        const disableStationDelete = !canEditTradsphere || isSavingAllChanges;
                        const stationStatusOptions = buildStatusOptions(
                          STATION_STATUS_OPTIONS_BASE,
                          station.status || "",
                          draftStatus,
                        );
                        return (
                          <button
                            key={station.id}
                            type="button"
                            className={[
                              "group w-full rounded-xl border px-3 py-3 text-left transition",
                              isProposalDelete
                                ? (active ? "border-rose-300 bg-rose-50/80" : "border-rose-200 bg-rose-50/40 hover:border-rose-300")
                                : isProposalAdd
                                  ? (active ? "border-cyan-300 bg-cyan-50/80" : "border-cyan-200 bg-cyan-50/40 hover:border-cyan-300")
                                  : isProposalChanged
                                    ? (active ? "border-sky-300 bg-sky-50/80" : "border-sky-200 bg-sky-50/40 hover:border-sky-300")
                                    : active
                                      ? "border-blue-300 bg-blue-50/70"
                                      : "border-blue-100 bg-white hover:border-blue-200 hover:bg-blue-50/30",
                            ].join(" ")}
                            onClick={() => handleStationSelect(station.id)}
                          >
                            <div className="grid gap-2 md:grid-cols-[minmax(0,1fr)_minmax(0,210px)_auto] md:items-center">
                              <div>
                                <p className="text-sm font-semibold text-slate-900">{formatStationListTitle(station)}</p>
                                {station.scheduleMismatch ? (
                                  <div className="mt-1 inline-flex items-center gap-1 rounded-full border border-amber-300 bg-amber-50 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.08em] text-amber-800">
                                    <AlertTriangle className="size-3" />
                                    Not in current schedule
                                  </div>
                                ) : null}
                                {station.proposalLabel ? (
                                  <div
                                    className={[
                                      "mt-1 inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.08em]",
                                      station.proposalState === "proposed_remove"
                                        ? "border-rose-300 bg-rose-100 text-rose-800"
                                        : station.proposalState === "proposed_add"
                                          ? "border-cyan-300 bg-cyan-100 text-cyan-800"
                                          : "border-sky-300 bg-sky-100 text-sky-800",
                                    ].join(" ")}
                                  >
                                    {station.proposalLabel}
                                  </div>
                                ) : null}
                              </div>
                              <div>
                                <AppDropdown
                                  value={draftStatus}
                                  onValueChange={(value) => handleStationStatusDraftChange(station.id, value)}
                                  options={stationStatusOptions.map((value) => ({ value, label: value }))}
                                  searchable
                                  allowCustomValue
                                  placeholder=""
                                  disabled={disableStationStatusMutations}
                                />
                              </div>
                              <div className="flex items-center justify-end">
                                <div className="flex items-center gap-0.5 transition-opacity duration-150 max-md:opacity-100 md:pointer-events-none md:opacity-0 md:group-hover:pointer-events-auto md:group-hover:opacity-100 md:group-focus-within:pointer-events-auto md:group-focus-within:opacity-100">
                                  <ActionIconButton
                                    icon={<Trash2 />}
                                    tooltip="Delete station"
                                    onClick={(event) => {
                                      event.stopPropagation();
                                      setDeletingStationId(station.id);
                                    }}
                                    disabled={disableStationDelete}
                                    className="!h-6 !w-6 !rounded-full !p-0 text-rose-500 hover:!bg-rose-50 hover:!scale-105 hover:text-rose-600 focus-visible:!bg-rose-50 focus-visible:!scale-105 focus-visible:text-rose-600 [&_svg]:!h-3.5 [&_svg]:!w-3.5 [&_svg]:text-rose-500 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110 hover:[&_svg]:text-rose-600 focus-visible:[&_svg]:text-rose-600"
                                    aria-label="Delete station"
                                  />
                                </div>
                              </div>
                            </div>
                          </button>
                        );
                      })}
                    </div>
                  )}
                </div>
              </div>
            )}
        </SectionCard>
          <SectionLoadingLayer active={isChecklistSelectionLoading} message="Loading checklist stations..." />
          {isDeletingSelectedChecklist ? (
            <SectionLoadingOverlay message="Deleting checklist account..." />
          ) : null}
        </div>

        <div className="relative space-y-4 xl:flex xl:h-full xl:min-h-0 xl:flex-col xl:gap-4 xl:space-y-0">
          <SectionCard
            className="relative xl:max-h-[42%] xl:min-h-[180px] xl:flex-none"
            contentClassName="xl:min-h-0 xl:overflow-y-auto xl:pr-1"
            title="Station Contacts"
            actions={(
              <div className="flex items-center gap-2">
                <ActionIconButton
                  icon={<Monitor />}
                  tooltip="View Station"
                  onClick={() => {
                    if (!selectedStationCode) {
                      return;
                    }
                    setStationCardModalCode(selectedStationCode);
                    setIsStationCardModalOpen(true);
                  }}
                  disabled={!selectedStationCode || isSavingAllChanges}
                  className="!h-7 !w-7 !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-4 [&_svg]:!w-4 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110"
                  aria-label="View Station"
                />
              </div>
            )}
          >
              {!selectedStation ? (
                <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                  Select a station to view contacts.
                </div>
              ) : (
                <div className="space-y-3">
                  <div className="space-y-2">
                    {isHydratingSelectedStationContacts ? (
                      <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                        <div className="flex items-center gap-2">
                          <Loader2 className="size-4 animate-spin text-slate-500" />
                          <span>Loading station contacts...</span>
                        </div>
                      </div>
                    ) : selectedStation.repContacts.length === 0 ? (
                      <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                        No rep contact metadata is currently available for this station.
                      </div>
                    ) : (
                      selectedStation.repContacts.map((contact, index) => (
                        <StationContactCard
                          key={`${contact.id ?? "contact"}:${contact.email || "no-email"}:${index}`}
                          contact={{
                            contactType: "REP",
                            contactId: contact.id,
                            fullName: contact.fullName || null,
                            email: contact.email || null,
                            office: contact.office || null,
                            cell: contact.mobile || null,
                            primaryContact: contact.primaryContact,
                          }}
                          isSubmitting={isSavingAllChanges}
                          isReadOnly={false}
                          onEdit={() => {}}
                          onRemove={() => {}}
                          onCopy={() => {
                            void handleCopyRepContact(contact);
                          }}
                          showEditAction={false}
                          showRemoveAction={false}
                        />
                      ))
                    )}
                  </div>
                </div>
              )}
          </SectionCard>
          <SectionLoadingLayer active={isHydratingSelectedStationContacts} message="Loading station contacts..." />

          <SectionCard
            className="relative xl:min-h-0 xl:flex-1"
            contentClassName="xl:flex xl:min-h-0 xl:flex-1 xl:flex-col"
            title="Station Notes"
            actions={(
              <div className="flex items-center gap-2">
                <ActionIconButton
                  icon={<Plus />}
                  tooltip="Add note"
                  onClick={handleOpenAddNoteModal}
                  disabled={!canEditTradsphere || !selectedStation || isSavingAllChanges}
                  className="!h-7 !w-7 !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-4 [&_svg]:!w-4 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110"
                  aria-label="Add note"
                />
              </div>
            )}
          >
              {!selectedStation ? (
                <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                  Select a station to view notes.
                </div>
              ) : (
                <div className="flex min-h-0 flex-1 flex-col">
                  <div className="min-h-0 flex-1 space-y-3 xl:overflow-y-auto xl:pr-1">
                    {visibleStationNoteItems.length === 0 ? (
                      isLoadingSelectedStationNotes ? (
                        <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                          <div className="flex items-center gap-2">
                            <Loader2 className="size-4 animate-spin text-slate-500" />
                            <span>Loading station notes...</span>
                          </div>
                        </div>
                      ) : (
                        <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                          No notes yet for this checklist.
                        </div>
                      )
                    ) : (
                      visibleStationNoteItems.map(({ note, station }) => {
                        const attachmentCount = note.attachments.length;
                        const isCurrentChecklistNote = station.checklistId === (selectedChecklistForView?.id ?? "");
                        const canOpenEditNote = canEditTradsphere && !isSavingAllChanges && isCurrentChecklistNote;
                        const canDeleteNote = canEditTradsphere && !isSavingAllChanges && isCurrentChecklistNote;
                        const isSelectedChecklistStationNote = station.id === selectedStation.id;
                        return (
                          <div
                            key={note.id}
                            role={canOpenEditNote ? "button" : undefined}
                            tabIndex={canOpenEditNote ? 0 : undefined}
                            onClick={() => {
                              if (!canOpenEditNote) {
                                return;
                              }
                              setEditingNoteId(note.id);
                            }}
                            onKeyDown={(event) => {
                              if (!canOpenEditNote) {
                                return;
                              }
                              if (event.key === "Enter" || event.key === " ") {
                                event.preventDefault();
                                setEditingNoteId(note.id);
                              }
                            }}
                            className={`group rounded-xl border p-3 transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:p-3.5 ${
                              !canOpenEditNote
                                ? "border-slate-300 bg-slate-100"
                                : isSelectedChecklistStationNote
                                ? "border-amber-300/90 bg-amber-50/70 hover:border-amber-400"
                                : "border-blue-100/90 bg-white hover:border-blue-200"
                            }`}
                          >
                            <div className="flex items-start justify-between gap-3">
                              <div className="space-y-1">
                                <div className="inline-flex items-center gap-1.5 rounded-full border border-emerald-300/70 bg-emerald-50 px-2 py-0.5">
                                  <span className="text-[10px] font-medium uppercase tracking-[0.1em] text-emerald-700">Amount</span>
                                  <span className="text-xs font-semibold text-emerald-900">{formatOptionalDollar(note.amount)}</span>
                                </div>
                                {isCurrentChecklistNote ? (
                                  <button
                                    type="button"
                                    className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.08em] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                                      isSelectedChecklistStationNote
                                        ? "border border-amber-300 bg-amber-100/80 text-amber-800"
                                        : "border border-blue-200 bg-blue-50/70 text-blue-700 hover:border-blue-300 hover:bg-blue-100"
                                    }`}
                                    onClick={(event) => {
                                      event.stopPropagation();
                                      runStationSelect(station.id);
                                    }}
                                    title="Switch to this checklist station row"
                                  >
                                    {formatStationNoteLinkLabel(station, note, selectedChecklistForView)}
                                  </button>
                                ) : (
                                  <span className="inline-flex items-center gap-1 rounded-full border border-slate-300 bg-slate-100 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.08em] text-slate-700">
                                    {formatStationNoteLinkLabel(station, note, selectedChecklistForView)}
                                  </span>
                                )}
                              </div>

                              {canDeleteNote ? (
                                <div className="flex items-center gap-0.5 transition-opacity duration-150 max-md:opacity-100 md:pointer-events-none md:opacity-0 md:group-hover:pointer-events-auto md:group-hover:opacity-100 md:group-focus-within:pointer-events-auto md:group-focus-within:opacity-100">
                                  <ActionIconButton
                                    icon={<Trash2 />}
                                    tooltip="Delete note"
                                  onClick={(event) => {
                                    event.stopPropagation();
                                    setDeletingNoteId(note.id);
                                  }}
                                    className="!h-6 !w-6 !rounded-full !p-0 text-rose-500 hover:!bg-rose-50 hover:!scale-105 hover:text-rose-600 focus-visible:!bg-rose-50 focus-visible:!scale-105 focus-visible:text-rose-600 [&_svg]:!h-3.5 [&_svg]:!w-3.5 [&_svg]:text-rose-500 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110 hover:[&_svg]:text-rose-600 focus-visible:[&_svg]:text-rose-600"
                                    aria-label="Delete note"
                                  />
                                </div>
                              ) : null}
                            </div>

                            <p className="mt-2 whitespace-pre-wrap text-sm leading-5 text-slate-800">
                              {note.note || "No note text"}
                            </p>

                            <div className="mt-2.5 border-t border-slate-200/80" />

                            <div className="mt-2 flex items-end justify-between gap-2">
                              <p className="text-[11px] text-slate-400">
                                Updated: {formatDateTime(note.dateUpdated || note.dateCreated || null)}
                              </p>
                              {attachmentCount > 0 ? (
                                <button
                                  type="button"
                                  onClick={(event) => {
                                    event.stopPropagation();
                                    setAttachmentModalNoteId(note.id);
                                  }}
                                  className="inline-flex h-6 min-w-6 items-center justify-center gap-1 rounded-full border border-slate-300 bg-slate-50 px-2 text-[11px] font-semibold text-slate-600 hover:bg-slate-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                                  aria-label={`View ${attachmentCount} attachment${attachmentCount > 1 ? "s" : ""}`}
                                  title={`${attachmentCount} attachment${attachmentCount > 1 ? "s" : ""}`}
                                >
                                  <span className="sr-only">Attachments</span>
                                  <Paperclip className="size-3.5" />
                                  {attachmentCount}
                                </button>
                              ) : null}
                            </div>
                          </div>
                        );
                      })
                    )}
                  </div>
                </div>
              )}
          </SectionCard>
          <SectionLoadingLayer active={isLoadingSelectedStationNotes} message="Loading station notes..." />
          {isDeletingSelectedChecklist ? (
            <SectionLoadingOverlay message="Deleting checklist account..." />
          ) : null}
        </div>

        {hasPendingPeriodSelectionAfterLoad && !(isLoadActionOverlayVisible || isSyncingPeriod || isSavingAllChanges || isChipRefreshing) ? (
          <div className="absolute inset-0 z-20 rounded-2xl bg-slate-900/55 backdrop-blur-[1.5px]">
            <div className="flex h-full w-full items-center justify-center p-4 sm:p-6">
              <button
                type="button"
                onClick={handleRestoreLoadedPeriodSelection}
                className="rounded-full border border-slate-300/80 bg-slate-100/95 px-3 py-1.5 text-xs font-medium text-slate-800 shadow-md transition hover:bg-slate-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-300 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-900"
              >
                Period selection changed. Click to restore previous selection.
              </button>
            </div>
          </div>
        ) : null}

        {isSavingAllChanges ? (
          <SectionLoadingOverlay
            message="Saving checklist changes..."
          />
        ) : null}
      </div>

      {shouldShowSaveActions ? (
        <div className="flex w-full justify-end">
          <div className="flex flex-wrap items-center gap-2">
            {(hasUnsavedChanges || isPeriodSyncPreviewPending) ? (
              <Button variant="outline" onClick={() => discardLocalChanges()} disabled={isSavingAllChanges}>
                Revert
              </Button>
            ) : null}
            <Button onClick={() => void handleSaveAllChanges()} disabled={!canSaveAllChanges}>
              Save
            </Button>
          </div>
        </div>
      ) : null}

      <AddStationNoteDialog
        open={isAddNoteModalOpen}
        onOpenChange={setIsAddNoteModalOpen}
        station={selectedStation}
        disabled={!canEditTradsphere}
        isBusy={isSavingAllChanges}
        onSubmit={handleAddNoteModalSubmit}
        onOpenAttachment={(url) => {
          void handleOpenAttachment(url);
        }}
      />

      <StationModal
        open={isStationCardModalOpen}
        onOpenChange={(nextOpen) => {
          setIsStationCardModalOpen(nextOpen);
          if (!nextOpen) {
            setStationCardModalCode(null);
          }
        }}
        mode="edit"
        canEdit={false}
        modalTitleOverride="Station"
        modalDescriptionOverride="View station basic information and contacts."
        showDeliveryMethodSection={false}
        showContactHeaderActions={false}
        showContactCardActions={false}
        showContactCopyAction
        stationCode={stationCardModalCode}
        headers={requestHeaders}
        onDetailLoaded={handleStationModalDetailLoaded}
      />

      <AddChecklistStationDialog
        open={isAddStationModalOpen}
        onOpenChange={setIsAddStationModalOpen}
        checklist={selectedChecklistForView}
        disabled={!canEditTradsphere}
        isBusy={isSavingAllChanges}
        onSubmit={(payload) => {
          return handleAddStationModalSubmit(payload);
        }}
      />

      <AddChecklistAccountDialog
        open={isAddChecklistAccountModalOpen}
        onOpenChange={setIsAddChecklistAccountModalOpen}
        disabled={!canEditTradsphere}
        isBusy={isSavingAllChanges}
        onSubmit={(payload) => {
          return handleAddChecklistAccountModalSubmit(payload);
        }}
      />

      <Dialog
        open={Boolean(deletingChecklist)}
        onOpenChange={(nextOpen) => {
          if (!nextOpen) {
            setDeletingChecklistId(null);
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete checklist account?</DialogTitle>
            <DialogDescription>
              This removes the checklist account from the selected period immediately.
            </DialogDescription>
          </DialogHeader>
          {deletingChecklist ? (
            <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm text-slate-700">
              <p className="font-semibold text-slate-900">
                {deletingChecklist.accountName
                  ? `${deletingChecklist.accountName} (${deletingChecklist.accountCode})`
                  : deletingChecklist.accountCode}
              </p>
              <p className="mt-1">{formatPeriodQuarterLabel(deletingChecklist.year, deletingChecklist.month)}</p>
            </div>
          ) : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeletingChecklistId(null)} disabled={isDeleteTargetChecklistInProgress}>
              Cancel
            </Button>
            <Button onClick={() => void handleConfirmDeleteChecklist()} disabled={isDeleteTargetChecklistInProgress} className="bg-rose-600 text-white hover:bg-rose-700 border-rose-700">
              {isDeleteTargetChecklistInProgress ? (
                <>
                  <Loader2 className="size-4 animate-spin" />
                  Deleting...
                </>
              ) : "Delete"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <EditStationNoteDialog
        open={Boolean(editingNote)}
        onOpenChange={(nextOpen) => {
          if (!nextOpen) {
            setEditingNoteId(null);
          }
        }}
        station={editingNoteContext?.station ?? selectedStation}
        note={editingNote}
        disabled={!canEditTradsphere}
        isBusy={isSavingAllChanges}
        onSubmit={handleEditNoteModalSubmit}
        onOpenAttachment={(url) => {
          void handleOpenAttachment(url);
        }}
      />

      <Dialog
        open={Boolean(deletingStation)}
        onOpenChange={(nextOpen) => {
          if (!nextOpen) {
            setDeletingStationId(null);
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete station?</DialogTitle>
            <DialogDescription>
              This removes the selected station row from the checklist. Click Save to persist this change.
            </DialogDescription>
          </DialogHeader>
          {deletingStation ? (
            <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm text-slate-700">
              <p className="font-semibold text-slate-900">{deletingStation.estNum} • {deletingStation.stationCode}</p>
              <p className="mt-1">{deletingStation.stationName || "Station name unavailable"}</p>
            </div>
          ) : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeletingStationId(null)}>
              Cancel
            </Button>
            <Button onClick={handleConfirmDeleteStation} className="bg-rose-600 text-white hover:bg-rose-700 border-rose-700">
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={Boolean(deletingNote)}
        onOpenChange={(nextOpen) => {
          if (!nextOpen) {
            setDeletingNoteId(null);
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete note?</DialogTitle>
            <DialogDescription>
              This removes the selected note. Click Save to persist this change.
            </DialogDescription>
          </DialogHeader>
          {deletingNote ? (
            <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm text-slate-700">
              <p className="font-semibold text-slate-900">{formatOptionalDollar(deletingNote.amount)}</p>
              <p className="mt-1 line-clamp-3 whitespace-pre-wrap">{deletingNote.note || "No note text"}</p>
            </div>
          ) : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeletingNoteId(null)}>
              Cancel
            </Button>
            <Button onClick={handleConfirmDeleteNote} className="bg-rose-600 text-white hover:bg-rose-700 border-rose-700">
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <UnsavedChangesDialog
        open={isUnsavedDialogOpen}
        onKeepEditing={() => {
          pendingUnsavedActionTypeRef.current = null;
          pendingUnsavedRouteProceedRef.current = null;
          setIsUnsavedDialogOpen(false);
        }}
        onDiscardChanges={() => {
          setIsUnsavedDialogOpen(false);
          discardLocalChanges({ reload: false });
          const nextActionType = pendingUnsavedActionTypeRef.current;
          const nextRouteProceed = pendingUnsavedRouteProceedRef.current;
          pendingUnsavedActionTypeRef.current = null;
          pendingUnsavedRouteProceedRef.current = null;
          if (nextActionType === "load") {
            void runLoadForSelectedPeriod();
            return;
          }
          if (nextActionType === "route" && nextRouteProceed) {
            nextRouteProceed();
          }
        }}
      />

      <Dialog
        open={Boolean(attachmentModalNote)}
        onOpenChange={(nextOpen) => {
          if (!nextOpen) {
            setAttachmentModalNoteId(null);
          }
        }}
      >
        <DialogContent className="max-w-3xl">
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            aria-label="Close attachments modal"
          >
            <X className="size-4" />
          </DialogClose>
          <DialogHeader>
            <DialogTitle>Attachments</DialogTitle>
            <DialogDescription>
              {attachmentModalNote
                ? `${attachmentModalNote.attachments.length} attachment${attachmentModalNote.attachments.length === 1 ? "" : "s"} for this note`
                : "Attachments"}
            </DialogDescription>
          </DialogHeader>

          <div className="mt-3">
            {!attachmentModalNote || attachmentModalNote.attachments.length === 0 ? (
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                No attachments for this note.
              </div>
            ) : (
              <div className="flex flex-wrap justify-center gap-3">
                {attachmentModalNote.attachments.map((attachment) => {
                  const attachmentName = asString(attachment.fileName) || `Attachment ${attachment.id}`;
                  const attachmentType = asString(attachment.mimeType) || asString(attachment.fileType) || "Unknown type";
                  const attachmentUrl = asString(attachment.accessUrl) || asString(attachment.url);
                  const previewSrc = isImageAttachment(attachment) ? attachmentImageSrcById[attachment.id] : null;
                  const canOpen = Boolean(previewSrc || attachmentUrl);

                  return (
                    <button
                      key={attachment.id}
                      type="button"
                      onClick={() => {
                        if (previewSrc) {
                          setAttachmentPreview({ src: previewSrc, name: attachmentName });
                          return;
                        }
                        if (attachmentUrl) {
                          void handleOpenAttachment(attachmentUrl);
                        }
                      }}
                      disabled={!canOpen}
                      className="w-full max-w-[160px] rounded-lg border border-slate-200 bg-white p-2 text-left transition-colors hover:bg-slate-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-default disabled:opacity-70"
                      aria-label={`Open ${attachmentName}`}
                    >
                      {previewSrc ? (
                        <div className="relative h-28 w-full overflow-hidden rounded-md border border-slate-200 bg-slate-50">
                          <img
                            src={previewSrc}
                            alt={attachmentName}
                            className="h-full w-full object-cover"
                          />
                        </div>
                      ) : (
                        <div className="flex h-28 w-full items-center justify-center rounded-md border border-slate-200 bg-slate-50 text-slate-500">
                          <Paperclip className="size-6" />
                        </div>
                      )}

                      <div className="mt-2 space-y-1">
                        <p className="truncate text-xs font-semibold text-slate-900">{attachmentName}</p>
                        <p className="truncate text-[11px] text-slate-500">{attachmentType}</p>
                      </div>
                    </button>
                  );
                })}
              </div>
            )}
          </div>
        </DialogContent>
      </Dialog>

      <Dialog
        open={Boolean(attachmentPreview)}
        onOpenChange={(nextOpen) => {
          if (!nextOpen) {
            setAttachmentPreview(null);
          }
        }}
      >
        <DialogContent
          className="w-[calc(100vw-2.5rem)] max-w-4xl border-none bg-transparent p-0 shadow-none"
          aria-describedby={undefined}
        >
          <div className="relative flex w-full items-center justify-center overflow-hidden rounded-2xl bg-white px-6 pb-6 pt-14 shadow-2xl sm:px-8 sm:pb-8 sm:pt-16">
            <DialogClose
              className="absolute right-4 top-4 z-10 rounded-md bg-slate-900/85 p-1.5 text-white transition-colors hover:bg-slate-900 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              aria-label="Close attachment preview"
            >
              <X className="size-4" />
            </DialogClose>
            {attachmentPreview ? (
              <img
                src={attachmentPreview.src}
                alt={attachmentPreview.name}
                className="mx-auto block h-auto max-h-[70vh] w-auto max-w-full object-contain"
              />
            ) : null}
          </div>
        </DialogContent>
      </Dialog>
      <PageLoadingLayer
        active={loadingContract.pageOverlayActive}
        message={loadingContract.pageOverlayMessage}
      />

    </AppPageLayout>
  );
}

function formatFileSize(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "0 B";
  }
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  const kb = bytes / 1024;
  if (kb < 1024) {
    return `${kb.toFixed(1)} KB`;
  }
  return `${(kb / 1024).toFixed(1)} MB`;
}

function evaluateAmountExpression(rawValue: string): number | null {
  const normalized = rawValue.replace(/,/g, "").replace(/\$/g, "").trim();
  if (!normalized) {
    return null;
  }
  if (!/^[\d+\-*/().\s]+$/.test(normalized)) {
    return null;
  }
  try {
    const evaluated = Function(`"use strict"; return (${normalized});`)() as unknown;
    if (typeof evaluated !== "number" || !Number.isFinite(evaluated)) {
      return null;
    }
    return Number(evaluated.toFixed(2));
  } catch {
    return null;
  }
}

function formatAddNoteStationSubtitle(station: StationItem | null): string {
  if (!station) {
    return "Create a reconciliation note with optional files.";
  }
  const stationCode = asString(station.stationCode).toUpperCase();
  const stationName = asString(station.stationName) || stationCode || "Station";
  const mediaType = asString(station.mediaType).toUpperCase();
  const isCable = mediaType === "CA" || mediaType === "CABLE";
  const stationLabel = isCable
    ? `ASN ${stationName}${stationCode ? ` (${stationCode})` : ""}`
    : stationName;
  return `${station.estNum} • ${stationLabel}`;
}

function formatAddChecklistStationSubtitle(checklist: ChecklistDetail | null): string {
  if (!checklist) {
    return "Add a station row to this checklist.";
  }
  const accountCode = asString(checklist.accountCode).toUpperCase();
  const accountName = asString(checklist.accountName);
  if (accountName && accountCode) {
    return `${accountName} (${accountCode})`;
  }
  return accountName || accountCode || "Checklist";
}

function formatStationListTitle(station: StationItem): string {
  const estNum = Number.isFinite(station.estNum) ? String(Math.trunc(station.estNum)) : "-";
  const stationCode = asString(station.stationCode).toUpperCase();
  const stationName = asString(station.stationName);
  const mediaType = asString(station.mediaType).toUpperCase();
  const isCable = mediaType === "CA" || mediaType === "CABLE";

  if (stationName && isCable && stationCode) {
    return `${estNum} • ${stationName} (${stationCode})`;
  }
  if (stationName) {
    return `${estNum} • ${stationName}`;
  }
  return `${estNum} • ${stationCode || "Station"}`;
}

function formatStationNoteLinkLabel(
  station: StationItem,
  note: NoteItem,
  checklist: ChecklistDetail | null,
): string {
  const estNum = Number.isFinite(station.estNum) ? String(Math.trunc(station.estNum)) : "-";
  const resolvedMonth = Number.isInteger(note.checklistMonth) ? Math.trunc(note.checklistMonth as number) : checklist?.month;
  const resolvedYear = Number.isInteger(note.checklistYear) ? Math.trunc(note.checklistYear as number) : checklist?.year;
  if (!resolvedMonth || !resolvedYear || resolvedMonth < 1 || resolvedMonth > 12) {
    return estNum;
  }
  const monthText = formatPeriodMonthName(resolvedMonth);
  return `${estNum} • ${monthText} ${resolvedYear}`;
}

function isImageAttachment(attachment: AttachmentItem): boolean {
  const mimeType = asString(attachment.mimeType).toLowerCase();
  if (mimeType.startsWith("image/")) {
    return true;
  }
  const fileType = asString(attachment.fileType).toLowerCase();
  if (fileType.startsWith("image/")) {
    return true;
  }
  const fileName = asString(attachment.fileName);
  if (fileName && IMAGE_ATTACHMENT_PATTERN.test(fileName)) {
    return true;
  }
  const urlWithoutQuery = asString(attachment.url).split("?")[0];
  return IMAGE_ATTACHMENT_PATTERN.test(urlWithoutQuery);
}

function AddChecklistAccountDialog({
  open,
  onOpenChange,
  disabled,
  isBusy,
  onSubmit,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  disabled: boolean;
  isBusy: boolean;
  onSubmit: (payload: AddChecklistAccountModalSubmitPayload) => string | null | Promise<string | null>;
}) {
  const [accountCodeDraft, setAccountCodeDraft] = useState("");
  const [statusDraft, setStatusDraft] = useState("");
  const [noteDraft, setNoteDraft] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);

  useEffect(() => {
    if (!open) {
      setAccountCodeDraft("");
      setStatusDraft("");
      setNoteDraft("");
      setError(null);
      setIsDiscardDialogOpen(false);
      return;
    }
    setError(null);
  }, [open]);

  const accountCode = accountCodeDraft.trim().toUpperCase();
  const noteText = noteDraft.trim();
  const hasUnsavedChanges = accountCodeDraft.trim().length > 0 || statusDraft.length > 0 || noteDraft.trim().length > 0;
  const canSubmit = !disabled && !isBusy && accountCode.length > 0 && accountCode.length <= 10 && noteText.length <= 2048;
  const statusOptions = useMemo(
    () => buildStatusOptions(CHECKLIST_STATUS_OPTIONS_BASE, "", statusDraft).map((value) => ({ value, label: value || "(empty)" })),
    [statusDraft],
  );

  function handleDialogOpenChange(nextOpen: boolean) {
    const allowClose = canModalClose({
      nextOpen,
      isBusy,
      hasUnsavedChanges,
    });
    if (!allowClose) {
      if (!nextOpen && hasUnsavedChanges) {
        setIsDiscardDialogOpen(true);
      }
      return;
    }
    onOpenChange(nextOpen);
  }

  async function handleSubmit() {
    if (!canSubmit) {
      setError("Account Code is required.");
      return;
    }
    const submitError = await onSubmit({
      accountCode,
      status: statusDraft,
      note: noteText,
    });
    if (submitError) {
      setError(submitError);
    }
  }

  return (
    <>
      <Dialog open={open} onOpenChange={handleDialogOpenChange}>
        <DialogContent
          onInteractOutside={(event) => {
            if (shouldBlockOutsideClose({ isBusy, hasUnsavedChanges })) {
              event.preventDefault();
            }
          }}
        >
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none"
            aria-label="Close add checklist account modal"
            disabled={isBusy}
          >
            <X className="size-4" />
          </DialogClose>

          <DialogHeader className="pb-2">
            <DialogTitle>Add Checklist Account</DialogTitle>
            <DialogDescription>
              Add a checklist account row for the selected period.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 pt-1">
            <LabeledField label="Account Code">
              <Input
                value={accountCodeDraft}
                onChange={(event) => {
                  setAccountCodeDraft(event.target.value.toUpperCase());
                  setError(null);
                }}
                placeholder="e.g. TAAA"
                maxLength={10}
                disabled={disabled || isBusy}
              />
            </LabeledField>

            <LabeledField label="Status">
              <AppDropdown
                value={statusDraft}
                onValueChange={(value) => {
                  setStatusDraft(value);
                  setError(null);
                }}
                options={statusOptions}
                searchable={false}
                placeholder=""
                disabled={disabled || isBusy}
              />
            </LabeledField>

            <LabeledField label="Note" alignStart>
              <Textarea
                value={noteDraft}
                onChange={(event) => {
                  setNoteDraft(event.target.value);
                  setError(null);
                }}
                placeholder="Add summary note..."
                className="min-h-[96px] resize-y"
                disabled={disabled || isBusy}
              />
            </LabeledField>
          </div>

          {error ? <p className="text-sm text-rose-600">{error}</p> : null}

          <DialogFooter>
            <Button onClick={() => void handleSubmit()} disabled={!canSubmit}>
              {isBusy ? (
                <>
                  <Loader2 className="size-4 animate-spin" />
                  Adding...
                </>
              ) : (
                "Add Account"
              )}
            </Button>
          </DialogFooter>
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

function AddChecklistStationDialog({
  open,
  onOpenChange,
  checklist,
  disabled,
  isBusy,
  onSubmit,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  checklist: ChecklistDetail | null;
  disabled: boolean;
  isBusy: boolean;
  onSubmit: (payload: AddChecklistStationModalSubmitPayload) => string | null | Promise<string | null>;
}) {
  const [estNumDraft, setEstNumDraft] = useState("");
  const [stationCodeDraft, setStationCodeDraft] = useState("");
  const [statusDraft, setStatusDraft] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);

  useEffect(() => {
    if (!open) {
      setEstNumDraft("");
      setStationCodeDraft("");
      setStatusDraft("");
      setError(null);
      setIsDiscardDialogOpen(false);
      return;
    }
    setError(null);
  }, [open]);

  const parsedEstNum = Number(estNumDraft.trim());
  const normalizedStationCode = stationCodeDraft.trim().toUpperCase();
  const hasUnsavedChanges = estNumDraft.trim().length > 0 || stationCodeDraft.trim().length > 0 || statusDraft.length > 0;
  const estNumValid = Number.isInteger(parsedEstNum) && parsedEstNum > 0;
  const stationCodeValid = normalizedStationCode.length > 0 && normalizedStationCode.length <= 10;
  const canSubmit = !disabled && !isBusy && estNumValid && stationCodeValid && Boolean(checklist);
  const statusOptions = useMemo(
    () => buildStatusOptions(STATION_STATUS_OPTIONS_BASE, "", statusDraft).map((value) => ({ value, label: value || "(empty)" })),
    [statusDraft],
  );

  function handleDialogOpenChange(nextOpen: boolean) {
    const allowClose = canModalClose({
      nextOpen,
      isBusy,
      hasUnsavedChanges,
    });
    if (!allowClose) {
      if (!nextOpen && hasUnsavedChanges) {
        setIsDiscardDialogOpen(true);
      }
      return;
    }
    onOpenChange(nextOpen);
  }

  async function handleSubmit() {
    if (!canSubmit || !checklist) {
      setError("Est Num and Station Code are required.");
      return;
    }
    setError(null);
    try {
      const submitError = await onSubmit({
        estNum: parsedEstNum,
        stationCode: normalizedStationCode,
        status: statusDraft,
      });
      if (submitError) {
        setError(submitError);
      }
    } catch (submitError) {
      const message = submitError instanceof Error && submitError.message.trim()
        ? submitError.message.trim()
        : "Unable to add station.";
      setError(message);
    }
  }

  return (
    <>
      <Dialog open={open} onOpenChange={handleDialogOpenChange}>
        <DialogContent
          onInteractOutside={(event) => {
            if (shouldBlockOutsideClose({ isBusy, hasUnsavedChanges })) {
              event.preventDefault();
            }
          }}
        >
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none"
            aria-label="Close add station modal"
            disabled={isBusy}
          >
            <X className="size-4" />
          </DialogClose>

          <DialogHeader className="pb-2">
            <DialogTitle>Add Station</DialogTitle>
            <DialogDescription>
              {formatAddChecklistStationSubtitle(checklist)}
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 pt-1">
            <LabeledField label="Est Num">
              <Input
                type="number"
                inputMode="numeric"
                value={estNumDraft}
                onChange={(event) => {
                  setEstNumDraft(event.target.value);
                  setError(null);
                }}
                placeholder="e.g. 26001"
                disabled={disabled || isBusy}
              />
            </LabeledField>

            <LabeledField label="Station Code">
              <Input
                value={stationCodeDraft}
                onChange={(event) => {
                  setStationCodeDraft(event.target.value.toUpperCase());
                  setError(null);
                }}
                placeholder="e.g. WXYZT"
                maxLength={10}
                disabled={disabled || isBusy}
              />
            </LabeledField>

            <LabeledField label="Status">
              <AppDropdown
                value={statusDraft}
                onValueChange={(value) => {
                  setStatusDraft(value);
                  setError(null);
                }}
                options={statusOptions}
                searchable={false}
                placeholder=""
                disabled={disabled || isBusy}
              />
            </LabeledField>
          </div>

          {error ? <p className="text-sm text-rose-600">{error}</p> : null}

          <DialogFooter>
            <Button onClick={handleSubmit} disabled={!canSubmit}>
              {isBusy ? (
                <>
                  <Loader2 className="size-4 animate-spin" />
                  Adding...
                </>
              ) : (
                "Add Station"
              )}
            </Button>
          </DialogFooter>
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

function EditStationNoteDialog({
  open,
  onOpenChange,
  station,
  note,
  disabled,
  isBusy,
  onSubmit,
  onOpenAttachment,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  station: StationItem | null;
  note: NoteItem | null;
  disabled: boolean;
  isBusy: boolean;
  onSubmit: (payload: EditStationNoteModalSubmitPayload) => void;
  onOpenAttachment?: (url: string) => void;
}) {
  return (
    <AddStationNoteDialog
      open={open}
      onOpenChange={onOpenChange}
      station={station}
      note={note}
      mode="edit"
      disabled={disabled}
      isBusy={isBusy}
      onSubmitEdit={onSubmit}
      onOpenAttachment={onOpenAttachment}
    />
  );
}

function AddStationNoteDialog({
  open,
  onOpenChange,
  station,
  note,
  mode = "add",
  disabled,
  isBusy,
  onSubmit,
  onSubmitEdit,
  onOpenAttachment,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  station: StationItem | null;
  note?: NoteItem | null;
  mode?: "add" | "edit";
  disabled: boolean;
  isBusy: boolean;
  onSubmit?: (payload: AddStationNoteModalSubmitPayload) => void;
  onSubmitEdit?: (payload: EditStationNoteModalSubmitPayload) => void;
  onOpenAttachment?: (url: string) => void;
}) {
  const toast = useToast();
  const allowedImageMimeTypes = new Set([
    "image/png",
    "image/jpg",
    "image/jpeg",
    "image/webp",
  ]);
  const [amount, setAmount] = useState("");
  const [noteTextDraft, setNoteTextDraft] = useState("");
  const [files, setFiles] = useState<File[]>([]);
  const [removedAttachmentIds, setRemovedAttachmentIds] = useState<number[]>([]);
  const [brokenPreviewByKey, setBrokenPreviewByKey] = useState<Record<string, true>>({});
  const [previewImage, setPreviewImage] = useState<{ src: string; name: string } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);
  const isEditMode = mode === "edit";
  const initialAmount = isEditMode && note && note.amount !== null ? note.amount.toFixed(2) : "";
  const initialNoteText = isEditMode && note ? note.note : "";

  useEffect(() => {
    if (!open) {
      setAmount("");
      setNoteTextDraft("");
      setFiles([]);
      setRemovedAttachmentIds([]);
      setBrokenPreviewByKey({});
      setPreviewImage(null);
      setError(null);
      setIsDiscardDialogOpen(false);
      return;
    }
    if (isEditMode) {
      setAmount(initialAmount);
      setNoteTextDraft(initialNoteText);
      setFiles([]);
      setRemovedAttachmentIds([]);
      setBrokenPreviewByKey({});
      setPreviewImage(null);
      setError(null);
      setIsDiscardDialogOpen(false);
    }
  }, [open, isEditMode, initialAmount, initialNoteText]);

  const amountText = amount.trim();
  const parsedAmount = evaluateAmountExpression(amount);
  const amountProvided = amountText.length > 0;
  const noteText = noteTextDraft.trim();
  const amountValid = !amountProvided || parsedAmount !== null;
  const noteValid = noteText.length <= 2048;
  const hasUnsavedChanges = isEditMode
    ? amount.trim() !== initialAmount || noteTextDraft !== initialNoteText || files.length > 0 || removedAttachmentIds.length > 0
    : Boolean(amountText || noteTextDraft.trim() || files.length > 0);
  const hasAmountValue = amountProvided && parsedAmount !== null;
  const hasRequiredInput = hasAmountValue || noteText.length > 0;
  const hasValidDraft = hasUnsavedChanges && amountValid && noteValid && hasRequiredInput;
  const canSubmit = !disabled && !isBusy && amountValid && noteValid && hasRequiredInput;
  const filePreviews = useMemo(() => files.map((file) => {
    const objectUrl = URL.createObjectURL(file);
    return {
      file,
      objectUrl,
      previewUrl: file.type.startsWith("image/") ? objectUrl : null,
    };
  }), [files]);
  const visibleExistingAttachments = useMemo(() => {
    if (!isEditMode || !note) {
      return [] as AttachmentItem[];
    }
    const removedSet = new Set(removedAttachmentIds);
    return note.attachments.filter((attachment) => !removedSet.has(attachment.id));
  }, [isEditMode, note, removedAttachmentIds]);
  const uploadedAttachmentRows = useMemo(() => {
    const existingRows = visibleExistingAttachments.map((attachment) => {
      const attachmentName = asString(attachment.fileName) || `Attachment ${attachment.id}`;
      const attachmentType = asString(attachment.mimeType) || asString(attachment.fileType) || "Unknown type";
      const attachmentSizeText = typeof attachment.fileSize === "number" && attachment.fileSize > 0
        ? ` • ${formatFileSize(attachment.fileSize)}`
        : "";
      const attachmentUrl = asString(attachment.url);
      const attachmentOpenUrl = asString(attachment.accessUrl) || attachmentUrl;
      return {
        key: `existing:${attachment.id}`,
        label: attachmentName,
        meta: `${attachmentType}${attachmentSizeText}`,
        openUrl: attachmentOpenUrl || null,
        previewSrc: isImageAttachment(attachment) ? (attachmentOpenUrl || null) : null,
        remove: () => removeExistingAttachment(attachment.id),
      };
    });
    const newRows = filePreviews.map(({ file, objectUrl, previewUrl }, index) => ({
      key: `new:${file.name}:${file.size}:${index}`,
      label: file.name,
      meta: `${file.type || "Unknown type"} • ${formatFileSize(file.size)}`,
      openUrl: objectUrl,
      previewSrc: previewUrl,
      remove: () => removeFile(index),
    }));
    return existingRows.concat(newRows);
  }, [filePreviews, visibleExistingAttachments]);

  useEffect(() => {
    return () => {
      filePreviews.forEach((item) => {
        URL.revokeObjectURL(item.objectUrl);
      });
    };
  }, [filePreviews]);

  function handleDialogOpenChange(nextOpen: boolean) {
    const allowClose = canModalClose({
      nextOpen,
      isBusy,
      hasUnsavedChanges,
    });
    if (!allowClose) {
      if (!nextOpen && hasUnsavedChanges) {
        setIsDiscardDialogOpen(true);
      }
      return;
    }
    onOpenChange(nextOpen);
  }

  function handleAddFiles(nextFiles: File[]) {
    if (!nextFiles.length) {
      return;
    }
    const validFiles: File[] = [];
    let hasInvalidFile = false;
    for (const file of nextFiles) {
      const normalizedMimeType = asString(file.type).toLowerCase();
      const normalizedFileName = asString(file.name).toLowerCase();
      const validByMimeType = allowedImageMimeTypes.has(normalizedMimeType);
      const validByExtension = (
        normalizedFileName.endsWith(".png")
        || normalizedFileName.endsWith(".jpg")
        || normalizedFileName.endsWith(".jpeg")
        || normalizedFileName.endsWith(".webp")
      );
      if (validByMimeType || validByExtension) {
        validFiles.push(file);
      } else {
        hasInvalidFile = true;
      }
    }

    if (validFiles.length > 0) {
      setFiles((current) => [...current, ...validFiles]);
    }
    if (hasInvalidFile) {
      toast.error("Only image files are allowed", "PNG, JPG, JPEG, and WEBP only.");
      setError(null);
      return;
    }
    setError(null);
  }

  function handleFileInputChange(fileList: FileList | null) {
    if (!fileList?.length) {
      return;
    }
    handleAddFiles(Array.from(fileList));
  }

  function normalizeClipboardFile(file: File, index: number): File {
    if (file.name) {
      return file;
    }
    const extension = file.type.split("/")[1]?.toLowerCase() || "png";
    const fallbackName = `pasted-image-${Date.now()}-${index + 1}.${extension}`;
    return new File([file], fallbackName, { type: file.type || "image/png" });
  }

  function handleUploadPaste(event: ReactClipboardEvent<HTMLDivElement>) {
    if (disabled || isBusy) {
      return;
    }
    const clipboardItems = Array.from(event.clipboardData?.items ?? []);
    const clipboardFiles = clipboardItems
      .filter((item) => item.kind === "file")
      .map((item) => item.getAsFile())
      .filter((file): file is File => file instanceof File)
      .map((file, index) => normalizeClipboardFile(file, index));
    if (clipboardFiles.length === 0) {
      return;
    }
    event.preventDefault();
    handleAddFiles(clipboardFiles);
  }

  function handleUploadDrop(event: ReactDragEvent<HTMLDivElement>) {
    event.preventDefault();
    event.stopPropagation();
    if (disabled || isBusy) {
      return;
    }
    const droppedFiles = Array.from(event.dataTransfer?.files ?? []);
    if (!droppedFiles.length) {
      return;
    }
    handleAddFiles(droppedFiles);
  }

  function removeFile(index: number) {
    setFiles((current) => current.filter((_, currentIndex) => currentIndex !== index));
  }

  function removeExistingAttachment(attachmentId: number) {
    setRemovedAttachmentIds((current) => (
      current.includes(attachmentId) ? current : current.concat(attachmentId)
    ));
    setError(null);
  }

  function handleSubmit() {
    if (isEditMode) {
      if (!canSubmit || (amountProvided && parsedAmount === null)) {
        setError("Enter amount or note.");
        return;
      }
      onSubmitEdit?.({
        amount: amountProvided ? parsedAmount : null,
        note: noteText,
        files,
        removedAttachmentIds,
      });
      return;
    }
    if (!canSubmit || (amountProvided && parsedAmount === null)) {
      setError("Enter amount or note.");
      return;
    }
    onSubmit?.({
      amount: amountProvided ? parsedAmount : null,
      note: noteText,
      files,
    });
  }

  return (
    <>
      <Dialog open={open} onOpenChange={handleDialogOpenChange}>
        <DialogContent
          onInteractOutside={(event) => {
            if (shouldBlockOutsideClose({ isBusy, hasUnsavedChanges })) {
              event.preventDefault();
            }
          }}
        >
          <DialogClose
            className="absolute right-4 top-4 rounded-md p-1 text-slate-500 transition-colors hover:text-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none"
            aria-label={isEditMode ? "Close edit station note modal" : "Close add station note modal"}
            disabled={isBusy}
          >
            <X className="size-4" />
          </DialogClose>

          <DialogHeader className="pb-2">
            <DialogTitle>{isEditMode ? "Edit Station Note" : "Add Station Note"}</DialogTitle>
            <DialogDescription>
              {formatAddNoteStationSubtitle(station)}
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 pt-1">
            <LabeledField label="Amount">
              <Input
                type="text"
                inputMode="decimal"
                value={amount}
                onChange={(event) => {
                  setAmount(event.target.value);
                  setError(null);
                }}
                onBlur={() => {
                  const evaluated = evaluateAmountExpression(amount);
                  if (evaluated !== null) {
                    setAmount(evaluated.toFixed(2));
                  }
                }}
                disabled={disabled || isBusy}
              />
            </LabeledField>

            <LabeledField alignStart label="Note">
              <Textarea
                value={noteTextDraft}
                onChange={(event) => {
                  setNoteTextDraft(event.target.value);
                  setError(null);
                }}
                placeholder={isEditMode ? "Describe the reconciliation item..." : "Describe the reconciliation item..."}
                className="min-h-[96px]"
                disabled={disabled || isBusy}
              />
            </LabeledField>

            <LabeledField alignStart label="Upload Files">
              <div
                onPaste={handleUploadPaste}
                onDragOver={(event) => {
                  event.preventDefault();
                }}
                onDrop={handleUploadDrop}
              >
              <label className="flex cursor-pointer items-center justify-center gap-2 rounded-lg border border-dashed border-blue-200 bg-blue-50/30 px-3 py-4 text-sm text-slate-700 hover:border-blue-300 hover:bg-blue-50/50">
                <UploadCloud className="size-4 text-blue-600" />
                Select image(s) or paste screenshot
                <input
                  type="file"
                  accept=".png,.jpg,.jpeg,.webp,image/png,image/jpeg,image/webp"
                  multiple
                  className="hidden"
                  onChange={(event) => handleFileInputChange(event.target.files)}
                  disabled={disabled || isBusy}
                />
              </label>
              {uploadedAttachmentRows.length ? (
                <div className="mt-2 space-y-2">
                  {uploadedAttachmentRows.map((row) => (
                    <div key={row.key} className="flex items-center justify-between gap-3 rounded-md border border-slate-200 bg-white px-3 py-2 text-xs">
                      <div className="flex min-w-0 items-center gap-3">
                        {row.previewSrc && !brokenPreviewByKey[row.key] ? (
                          <button
                            type="button"
                            className="flex h-12 w-12 shrink-0 cursor-zoom-in items-center justify-center overflow-hidden rounded-md border border-slate-200 bg-slate-50"
                            onClick={() => setPreviewImage({ src: row.previewSrc!, name: row.label })}
                            disabled={disabled || isBusy}
                            aria-label={`Preview ${row.label}`}
                          >
                            <img
                              src={row.previewSrc!}
                              alt={row.label}
                              className="h-full w-full object-cover"
                              onError={() => {
                                setBrokenPreviewByKey((current) => ({ ...current, [row.key]: true }));
                              }}
                            />
                          </button>
                        ) : row.openUrl ? (
                          <button
                            type="button"
                            className="flex h-12 w-12 shrink-0 items-center justify-center rounded-md border border-slate-200 bg-slate-50 text-slate-500 hover:bg-slate-100"
                            aria-label={`Open ${row.label}`}
                            onClick={() => {
                              if (row.openUrl) {
                                onOpenAttachment?.(row.openUrl);
                              }
                            }}
                            disabled={disabled || isBusy}
                          >
                            <Paperclip className="size-4" aria-hidden="true" />
                          </button>
                        ) : (
                          <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-md border border-slate-200 bg-slate-50 text-slate-500">
                            <Paperclip className="size-4" aria-hidden="true" />
                          </div>
                        )}
                        <div className="min-w-0">
                          <p className="truncate font-medium text-slate-800">{row.label}</p>
                          <p className="text-slate-500">{row.meta}</p>
                        </div>
                      </div>
                      <ActionIconButton
                        icon={<Trash2 />}
                        tooltip="Remove attachment"
                        onClick={row.remove}
                        disabled={disabled || isBusy}
                        className="!h-6 !w-6 !p-0 text-rose-500 hover:text-rose-600 focus-visible:text-rose-600 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-3.5 [&_svg]:!w-3.5 [&_svg]:text-rose-500 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110 hover:[&_svg]:text-rose-600 focus-visible:[&_svg]:text-rose-600"
                        aria-label={`Remove ${row.label}`}
                      />
                    </div>
                  ))}
                </div>
              ) : (
                <p className="mt-2 text-xs text-slate-500">No attachments yet. Paste a screenshot here or choose files.</p>
              )}
              </div>
            </LabeledField>
          </div>

          {error ? <p className="text-sm text-rose-600">{error}</p> : null}

          <DialogFooter>
            {hasValidDraft || isBusy ? (
              <Button onClick={handleSubmit} disabled={!canSubmit}>
                {isBusy ? (
                  <>
                    <Loader2 className="size-4 animate-spin" />
                    Saving...
                  </>
                ) : (
                  isEditMode ? "Apply" : "Add Note"
                )}
              </Button>
            ) : null}
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={Boolean(previewImage)} onOpenChange={(nextOpen) => {
        if (!nextOpen) {
          setPreviewImage(null);
        }
      }}>
        <DialogContent
          className="w-[calc(100vw-2.5rem)] max-w-4xl border-none bg-transparent p-0 shadow-none"
          aria-describedby={undefined}
        >
          <div className="relative flex w-full items-center justify-center overflow-hidden rounded-2xl bg-white px-6 pb-6 pt-14 shadow-2xl sm:px-8 sm:pb-8 sm:pt-16">
            <DialogClose
              className="absolute right-4 top-4 z-10 rounded-md bg-slate-900/85 p-1.5 text-white transition-colors hover:bg-slate-900 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              aria-label="Close image preview"
            >
              <X className="size-4" />
            </DialogClose>
            {previewImage ? (
              <img
                src={previewImage.src}
                alt={previewImage.name}
                className="mx-auto block h-auto max-h-[70vh] w-auto max-w-full object-contain"
              />
            ) : null}
          </div>
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
