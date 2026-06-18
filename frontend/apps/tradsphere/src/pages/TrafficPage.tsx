import { memo, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState, type KeyboardEvent as ReactKeyboardEvent } from "react";
import { AlertTriangle, Archive, Copy, Eye, Link2, Loader2, Pencil, Plus, RefreshCw, Send, Table2, Trash2, Unlink, Unlock, X } from "lucide-react";

import { ActionIconButton } from "@/components/dashboard/ActionIconButton";
import { AccountSelector } from "@/components/dashboard/AccountSelector";
import { ScheduleModal } from "@/components/dashboard/ScheduleModal";
import { ScheduleTimelineSection } from "@/components/dashboard/ScheduleTimelineSection";
import type { EsnumItem } from "@/components/dashboard/types";
import { PageBanner } from "@/components/layout/PageBanner";
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
import { useToast } from "@/components/ui/toast";
import { UnsavedChangesDialog } from "@/components/ui/unsaved-changes-dialog";
import { useApiRequest } from "@/hooks/useApiRequest";
import { useDirtyRefreshGuard } from "@/hooks/useDirtyRefreshGuard";
import { useOnlineStatus } from "@/hooks/useOnlineStatus";
import { usePersistentState } from "@/hooks/usePersistentState";
import { useCommittedTextField } from "@shared/hooks/useCommittedTextField";
import { readBrowserCacheSnapshot, removeBrowserCache, writeBrowserCache } from "@/lib/browserCache";
import {
  buildTrafficEmailHtmlDocument,
  parseTrafficEmailWorkspaceFromBody,
  persistTrafficEmailBody,
  type TrafficEmailDownloadLink,
  type TrafficEmailWorkspacePayload,
} from "@/lib/trafficEmailTemplate";
import { useTradsphereAccountSelections } from "@/hooks/useTradsphereAccountSelections";
import {
  DateInputField,
  FLIGHT_DATE_PICKER_POPOVER_SELECTOR,
} from "@/components/dashboard/FlightDateRangeField";
import { ModalCloseButton, ModalHeaderRow, ModalShell } from "@shared/components";
import { buildAuthHeaders as buildSharedAuthHeaders } from "@shared/api/authHeaders";
import { useAuth } from "@shared/auth/useAuth";
import { shouldProtectFrontendAuth } from "@shared/auth/guards";
import { hasAppEditAccess } from "@shared/auth/permissions";
import {
  readScopedPageState,
  type CachePolicy,
  shouldFetchNetwork,
  TRADSPHERE_CACHE_TTL_MS,
  writeScopedPageState,
} from "@shared/cache";
import { Tooltip } from "@shared/components/actions/Tooltip";
import { TooltipTarget } from "@shared/components/actions/TooltipTarget";
import { AppPageLayout } from "@shared/components/layout/AppPageLayout";
import { PageCacheFooter } from "@shared/components/layout/PageCacheFooter";
import { EmailChipsInput } from "@shared/components/form/EmailChipsInput";
import { RichTextEditor } from "@shared/components/RichTextEditor";
import { SectionCard } from "@shared/components/layout/SectionCard";
import { ModalCacheFooter } from "@shared/components/modal/ModalCacheFooter";
import { PageLoadingLayer, SectionLoadingLayer, SectionLoadingOverlay } from "@shared/components/status/LoadingOverlay";
import { PageMessageStack, SectionMessageStack, type StackMessage } from "@shared/components/status/MessageStack";
import { resolveSharedLoadingContract } from "@shared/components/status/loadingContract";
import { isLikelyEmailAddress, mergeUniqueEmails } from "@shared/utils/email";
import { normalizeRichTextHtml } from "@shared/utils/richText";
import { playSuccessSound, primeSuccessSound } from "@shared/utils/audio";

type CacheStatus = {
  source: "cache" | "network";
  fetchedAt: number;
};

type TrafficStatus = "draft" | "ready" | "sent" | "confirmed" | "archived";
type FlightLanguage = "English" | "Spanish";

type TrafficSummary = {
  id: string;
  accountCode: string;
  campaign: string;
  searchCampaign: string;
  status: TrafficStatus;
  note: string | null;
  dateCreated: string | null;
  dateUpdated: string | null;
  flightCount: number;
  stationCount: number;
  emailSentStatus: string | null;
  emailSentAt: string | null;
  searchIscis: string[];
  searchStations: string[];
  searchEmails: string[];
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
  language: FlightLanguage;
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
  language: string;
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

type FlightStationSyncParams = {
  trafficId: string;
  flightStart: string;
  flightEnd: string;
  estNums?: number[];
  languages?: FlightLanguage[];
  mediums?: string[];
  forceRefreshCandidates?: boolean;
};

type FlightStationSyncDialogSource = "flight_update" | "manual_sync";

type RefreshEmailMergePrompt = {
  trafficId: string;
  emails: string[];
};

type TrafficPageSnapshot = {
  loadedAccountCode: string;
  selectedTrafficId: string | null;
  trafficList: TrafficSummary[];
  detailBaseline: TrafficDetail | null;
  detailDraft: TrafficDetail | null;
  cacheStatus: CacheStatus | null;
  refreshMessage: string | null;
  activeWorkspaceTab: TrafficWorkspaceTab;
  flightStationSyncSelectionsByTrafficId: Record<string, number[]>;
};

type TrafficDraftSession = {
  baseline: TrafficDetail | null;
  draft: TrafficDetail;
};

type UpdateDraftOptions = {
  applyAutoReadyToEmail?: boolean;
};

type OptimisticTrafficRemovalSnapshot = {
  trafficId: string;
  previousList: TrafficSummary[];
  previousSelected: string | null;
  previousDetailBaseline: TrafficDetail | null;
  previousDetailDraft: TrafficDetail | null;
  previousRefreshMessage: string | null;
  nextSelected: string | null;
  removedSelected: boolean;
};

type TrafficStationCandidate = {
  stationCode: string;
  stationName: string | null;
  deliveryMethod: string | null;
  contactsSnapshot: Record<string, unknown> | null;
};

type TrafficStationCandidateSummaryMonth = {
  monthKey: string;
  year: number;
  month: number;
  label: string;
};

type TrafficStationCandidateSummaryCell = {
  monthKey: string;
  year: number;
  month: number;
  label: string;
  hasSchedule: boolean;
  hasSpot: boolean;
  scheduleCount: number;
  totalSpot: number;
  totalGrossText: string;
};

type TrafficStationCandidateSummaryRow = {
  estNum: number;
  stationCode: string;
  stationName: string | null;
  monthCells: TrafficStationCandidateSummaryCell[];
};

type TrafficStationCandidatesSummary = {
  candidateCount: number;
  estNumCount: number;
  months: TrafficStationCandidateSummaryMonth[];
  rows: TrafficStationCandidateSummaryRow[];
};

type TrafficStationCandidatesPayload = {
  accountCode: string;
  flightStart: string;
  flightEnd: string;
  estNums: Array<{
    estNum: number;
    note: string | null;
    medium: string | null;
    stationCount: number;
  }>;
  stations: TrafficStationCandidate[];
  summary: TrafficStationCandidatesSummary;
};

type TrafficStationCandidatesFetchResult = {
  payload: TrafficStationCandidatesPayload;
  cacheStatus: CacheStatus | null;
};

type TrafficWorkspaceTab = "workflow" | "email";
type TrafficRemovalMode = "delete" | "archive";

type TrafficEmailWorkspaceDraft = TrafficEmailWorkspacePayload & {
  bodyTouched: boolean;
  instructionsTouched: boolean;
};

type TrafficListItemProps = {
  item: TrafficSummary;
  active: boolean;
  displayCampaign: string;
  displayStatus: TrafficStatus;
  isDraft: boolean;
  itemHasUnsavedChanges: boolean;
  isDeletingCard: boolean;
  isSelectionLocked: boolean;
  canEditTradsphere: boolean;
  isSaving: boolean;
  isLoadingAccountTraffic: boolean;
  isLoadingDetail: boolean;
  isDuplicatingTraffic: boolean;
  onSelectTraffic: (trafficId: string) => void;
  onDuplicateTraffic: (trafficId: string) => void;
  onOpenTrafficRemovalDialog: (trafficId: string, mode: TrafficRemovalMode) => void;
};

const TRAFFIC_LIST_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.MAIN_LOAD;
const TRAFFIC_DETAIL_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.MAIN_LOAD;
const TRAFFIC_STATION_CANDIDATES_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.SCHEDULE_TIMELINE;
const STATION_LOOKUP_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.STATION_DETAIL;
const SELECTED_ACCOUNT_STORAGE_KEY = "tradsphere.traffic.selectedAccount.v1";
const SELECTED_BY_ACCOUNT_STORAGE_KEY = "tradsphere.traffic.selectedByAccount.v1";
const TRAFFIC_PAGE_STATE_CODE = "traffic";
const DEFERRED_REFRESH_MESSAGE = "Latest traffic data loaded in the background. Save or revert local changes to apply it.";
const DEFAULT_TEST_EMAIL_RECIPIENT = "hai@theautoadagency.com";
const DEFAULT_TRAFFIC_CC_EMAILS = ["hai@theautoadagency.com"];
const STATUS_OPTIONS: Array<{ value: TrafficStatus; label: string }> = [
  { value: "draft", label: "Draft" },
  { value: "ready", label: "Ready" },
  { value: "sent", label: "Sent" },
  { value: "confirmed", label: "Confirmed" },
];
const FLIGHT_MEDIA_OPTIONS = ["TV", "RA", "CA", "OD", "NP", "CINE"];
const FLIGHT_LANGUAGE_OPTIONS: Array<{ value: FlightLanguage; label: string }> = [
  { value: "English", label: "English" },
  { value: "Spanish", label: "Spanish" },
];
const DELIVERY_STATUS_OPTIONS = ["not_started", "needs_manual_upload", "ready_to_email", "sent", "skipped", "issue"];
const CONFIRMED_STATUS_OPTIONS = ["pending", "confirmed", "issue", "not_required"];
const LOCAL_TRAFFIC_ID_PREFIX = "local-traffic:";
const EMAIL_WORKSPACE_TABS: Array<{ value: TrafficWorkspaceTab; label: string }> = [
  { value: "workflow", label: "Traffic Detail" },
  { value: "email", label: "Email" },
];

type TrafficLockBannerKind = "confirmed" | "sent" | "email";

type TrafficLockBannerProps = {
  kind: TrafficLockBannerKind;
  isUnlocking: boolean;
  disabled: boolean;
  onUnlock: () => void;
};

function TrafficLockBanner({
  kind,
  isUnlocking,
  disabled,
  onUnlock,
}: TrafficLockBannerProps) {
  const message = kind === "confirmed"
    ? "This traffic is confirmed and locked. Unblock it to edit again."
    : kind === "sent"
      ? "This traffic is sent and locked. Unblock it to edit again."
      : "This email is sent and locked. Unblock it to edit again.";
  return (
    <div className="flex flex-col gap-3 rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700 sm:flex-row sm:items-center sm:justify-between">
      <p>{message}</p>
      <Button
        variant="outline"
        size="sm"
        disabled={disabled}
        onClick={() => {
          onUnlock();
        }}
        className="shrink-0 border-emerald-200 bg-white text-emerald-700 hover:border-emerald-300 hover:bg-emerald-50 hover:text-emerald-800"
      >
        {isUnlocking ? (
          <>
            <Loader2 className="size-4 animate-spin" />
            Unblocking...
          </>
        ) : (
          <>
            <Unlock className="size-4" />
            Unblock
          </>
        )}
      </Button>
    </div>
  );
}

type TrafficWorkspaceModeTabsProps = {
  activeTab: TrafficWorkspaceTab;
  onChangeTab: (tab: TrafficWorkspaceTab) => void;
};

const TrafficWorkspaceModeTabs = memo(function TrafficWorkspaceModeTabs({
  activeTab,
  onChangeTab,
}: TrafficWorkspaceModeTabsProps) {
  return (
    <div className="flex justify-start">
      <div
        role="tablist"
        aria-label="Traffic workspace mode"
        className="grid w-full grid-cols-2 gap-1.5 rounded-xl border border-blue-200 bg-gradient-to-r from-blue-50/80 via-indigo-50/40 to-violet-50/70 p-1.5 sm:w-auto sm:min-w-[32rem]"
      >
        {EMAIL_WORKSPACE_TABS.map((tab) => {
          const isActive = activeTab === tab.value;
          const tabIcon = tab.value === "workflow" ? <Table2 className="size-3.5" /> : <Send className="size-3.5" />;
          return (
            <button
              key={tab.value}
              id={`traffic-workspace-tab-${tab.value}`}
              role="tab"
              aria-selected={isActive}
              aria-controls={`traffic-workspace-panel-${tab.value}`}
              type="button"
              onClick={() => onChangeTab(tab.value)}
              className={[
                "inline-flex w-full items-center justify-center gap-2 rounded-lg border px-3 py-2 text-xs font-semibold transition duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-300",
                isActive
                  ? "border-blue-200 bg-white text-blue-700 shadow-sm"
                  : "border-transparent bg-transparent text-slate-600 hover:border-blue-100 hover:bg-white/70 hover:text-blue-700",
              ].join(" ")}
            >
              {tabIcon}
              {tab.label}
            </button>
          );
        })}
      </div>
    </div>
  );
});

const TrafficListItemCard = memo(function TrafficListItemCard({
  item,
  active,
  displayCampaign,
  displayStatus,
  isDraft,
  itemHasUnsavedChanges,
  isDeletingCard,
  isSelectionLocked,
  canEditTradsphere,
  isSaving,
  isLoadingAccountTraffic,
  isLoadingDetail,
  isDuplicatingTraffic,
  onSelectTraffic,
  onDuplicateTraffic,
  onOpenTrafficRemovalDialog,
}: TrafficListItemProps) {
  const shouldHardDelete = itemHasUnsavedChanges || isDraft;
  const isLockedTrafficCard = ["sent", "confirmed"].includes(asString(displayStatus).toLowerCase());

  return (
    <div
      role="button"
      aria-disabled={isDeletingCard || isSelectionLocked}
      tabIndex={0}
      onClick={() => {
        if (isDeletingCard || isSelectionLocked) {
          return;
        }
        onSelectTraffic(item.id);
      }}
      onKeyDown={(event) => {
        if (isDeletingCard || isSelectionLocked) {
          return;
        }
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onSelectTraffic(item.id);
        }
      }}
      className={[
        "group w-full rounded-xl border px-3 py-3 text-left transition",
        isDeletingCard || isSelectionLocked ? "cursor-not-allowed opacity-70" : "",
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
          {itemHasUnsavedChanges ? (
            <span className="ml-1 inline-flex rounded-full border border-amber-200 bg-amber-50 px-2 py-0.5 text-[11px] font-semibold text-amber-800">
              Unsaved
            </span>
          ) : null}
        </div>
        <div className="flex items-center">
          <div className="flex items-center gap-0.5 transition-opacity duration-150 max-md:opacity-100 md:pointer-events-none md:opacity-0 md:group-hover:pointer-events-auto md:group-hover:opacity-100 md:group-focus-within:pointer-events-auto md:group-focus-within:opacity-100">
            <ActionIconButton
              icon={isDuplicatingTraffic ? <Loader2 className="animate-spin" /> : <Copy />}
              tooltip={isDuplicatingTraffic ? "Duplicating traffic..." : "Duplicate Traffic"}
              aria-label={isDuplicatingTraffic ? "Duplicating traffic" : "Duplicate Traffic"}
              title={isDuplicatingTraffic ? "Duplicating traffic" : "Duplicate Traffic"}
              onClick={(event) => {
                event.stopPropagation();
                if (isDuplicatingTraffic) {
                  return;
                }
                void onDuplicateTraffic(item.id);
              }}
              className="!h-6 !w-6 !rounded-full !p-0 text-blue-500 hover:!bg-blue-50 hover:!scale-105 hover:text-blue-600 focus-visible:!bg-blue-50 focus-visible:!scale-105 focus-visible:text-blue-600 [&_svg]:!h-3.5 [&_svg]:!w-3.5 [&_svg]:text-blue-500 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110 hover:[&_svg]:text-blue-600 focus-visible:[&_svg]:text-blue-600"
            />
            <ActionIconButton
              icon={isDeletingCard ? <Loader2 className="animate-spin" /> : (shouldHardDelete ? <Trash2 /> : <Archive />)}
              tooltip={isDeletingCard ? "Removing traffic..." : (shouldHardDelete ? "Delete Traffic" : "Archive Traffic")}
              aria-label={isDeletingCard ? "Removing traffic" : (shouldHardDelete ? "Delete Traffic" : "Archive Traffic")}
              title={isDeletingCard ? "Removing traffic" : (shouldHardDelete ? "Delete Traffic" : "Archive Traffic")}
              onClick={(event) => {
                event.stopPropagation();
                if (isDeletingCard) {
                  return;
                }
                if (shouldHardDelete) {
                  onOpenTrafficRemovalDialog(item.id, "delete");
                  return;
                }
                onOpenTrafficRemovalDialog(item.id, "archive");
              }}
              disabled={!canEditTradsphere || isSaving || isDeletingCard || isLoadingAccountTraffic || isLoadingDetail || isLockedTrafficCard}
              className="!h-6 !w-6 !rounded-full !p-0 text-rose-500 hover:!bg-rose-50 hover:!scale-105 hover:text-rose-600 focus-visible:!bg-rose-50 focus-visible:!scale-105 focus-visible:text-rose-600 [&_svg]:!h-3.5 [&_svg]:!w-3.5 [&_svg]:text-rose-500 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110 hover:[&_svg]:text-rose-600 focus-visible:[&_svg]:text-rose-600"
            />
          </div>
        </div>
      </div>
    </div>
  );
});

type TrafficFlightsSectionProps = {
  activeAccountCode: string;
  activeDraft: TrafficDetail | null;
  canEditTradsphere: boolean;
  isSaving: boolean;
  isTrafficEditingLocked: boolean;
  onAddFlight: () => void;
  onEditFlight: (flightId: number) => void;
  onDuplicateFlight: (flightId: number) => void;
  onRemoveFlight: (flightId: number) => void;
};

const TrafficFlightsSection = memo(function TrafficFlightsSection({
  activeAccountCode,
  activeDraft,
  canEditTradsphere,
  isSaving,
  isTrafficEditingLocked,
  onAddFlight,
  onEditFlight,
  onDuplicateFlight,
  onRemoveFlight,
}: TrafficFlightsSectionProps) {
  return (
    <SectionCard
      title="Flights"
      actions={(
        <ActionIconButton
          icon={<Plus />}
          tooltip="Add Flight"
          aria-label="Add Flight"
          title="Add Flight"
          onClick={onAddFlight}
          disabled={!canEditTradsphere || isSaving || isTrafficEditingLocked}
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
                    <th className="px-3 py-2 font-semibold">Language</th>
                    <th className="px-3 py-2 font-semibold">Length</th>
                    <th className="px-3 py-2 font-semibold">ISCI</th>
                    <th className="px-3 py-2 font-semibold">Rotation</th>
                    <th className="px-3 py-2 text-center font-semibold">File</th>
                    <th className="px-3 py-2 text-center font-semibold">Script</th>
                    <th className="px-3 py-2 text-right font-semibold"></th>
                  </tr>
                </thead>
                <tbody>
                  {activeDraft.flights.map((flight) => {
                    const isDraftFlight = flight.id < 0;
                    return (
                      <tr
                        key={flight.id}
                        onClick={() => {
                          onEditFlight(flight.id);
                        }}
                        className={`group cursor-pointer border-t border-slate-200 text-slate-700 transition ${isDraftFlight ? "bg-amber-50/70 hover:bg-amber-100/70" : "hover:bg-blue-50/40"}`}
                      >
                        <td className="whitespace-nowrap px-3 py-2">{formatFlightRangeForTable(flight.flightStart, flight.flightEnd)}</td>
                        <td className="whitespace-nowrap px-3 py-2">{flight.medium || "-"}</td>
                        <td className="whitespace-nowrap px-3 py-2">
                          <span className={`inline-flex rounded-full border px-2 py-0.5 text-[11px] font-medium ${languageChipClass(flight.language)}`}>
                            {normalizeFlightLanguageValue(flight.language)}
                          </span>
                        </td>
                        <td className="whitespace-nowrap px-3 py-2">{formatLengthValue(flight.length)}</td>
                        <td className="whitespace-nowrap px-3 py-2">{flight.isci || "-"}</td>
                        <td className="whitespace-nowrap px-3 py-2">{formatRotationValue(flight.rotation)}</td>
                        <td className="whitespace-nowrap px-3 py-2 text-center">
                          {normalizeExternalUrl(flight.fileUrl) ? (
                            <TooltipTarget text="Click to open file">
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
                            </TooltipTarget>
                          ) : (
                            <span className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-100 px-2 py-0.5 text-[11px] font-medium text-slate-600">
                              <Unlink className="size-3" />
                              Missing
                            </span>
                          )}
                        </td>
                        <td className="whitespace-nowrap px-3 py-2 text-center">
                          {normalizeExternalUrl(flight.scriptUrl) ? (
                            <TooltipTarget text="Click to open script">
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
                            </TooltipTarget>
                          ) : (
                            <span className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-slate-100 px-2 py-0.5 text-[11px] font-medium text-slate-600">
                              <Unlink className="size-3" />
                              Missing
                            </span>
                          )}
                        </td>
                        <td className="px-3 py-2 text-right">
                          <div className="inline-flex items-center gap-0.5">
                            <ActionIconButton
                              icon={<Copy />}
                              tooltip="Duplicate flight row"
                              aria-label="Duplicate flight row"
                              title="Duplicate flight row"
                              onClick={(event) => {
                                event.stopPropagation();
                                onDuplicateFlight(flight.id);
                              }}
                              disabled={!canEditTradsphere || isSaving || isTrafficEditingLocked}
                              className="!h-6 !w-6 !rounded-full !p-0 text-blue-500 hover:!bg-blue-50 hover:!scale-105 hover:text-blue-600 focus-visible:!bg-blue-50 focus-visible:!scale-105 focus-visible:text-blue-600 [&_svg]:!h-3.5 [&_svg]:!w-3.5"
                            />
                            <ActionIconButton
                              icon={<Trash2 />}
                              tooltip="Delete flight row"
                              aria-label="Delete flight row"
                              title="Delete flight row"
                              onClick={(event) => {
                                event.stopPropagation();
                                onRemoveFlight(flight.id);
                              }}
                              disabled={!canEditTradsphere || isSaving || isTrafficEditingLocked}
                              className="!h-6 !w-6 !rounded-full !p-0 text-rose-500 hover:!bg-rose-50 hover:!scale-105 hover:text-rose-600 focus-visible:!bg-rose-50 focus-visible:!scale-105 focus-visible:text-rose-600 [&_svg]:!h-3.5 [&_svg]:!w-3.5"
                            />
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </SectionCard>
  );
});

type TrafficStationsSectionProps = {
  activeAccountCode: string;
  activeDraft: TrafficDetail | null;
  canEditTradsphere: boolean;
  isSaving: boolean;
  isTrafficEditingLocked: boolean;
  isConfirmedLocked: boolean;
  isStationAutoSyncing: boolean;
  isScheduleTimelineLoading: boolean;
  isLoadingAccountTraffic: boolean;
  isOnline: boolean;
  timelineAccountCode: string;
  stationSyncFlightRange: { flightStart: string; flightEnd: string } | null;
  sortedStations: TrafficStation[];
  stationLookupMetaByCode: Record<string, StationLookupMeta>;
  stationContactEmailsById: Record<string, string[]>;
  stationDeliveryStatusRestoreMap: Record<number, string>;
  stationConfirmedStatusRestoreMap: Record<number, string>;
  onManualStationSync: () => void;
  onOpenScheduleTimeline: () => void;
  onAddStation: () => void;
  onEditStation: (stationId: number) => void;
  onOpenDeliveryMethodDetail: (stationCode: string) => void;
  onCopyStationContacts: (emails: string[]) => void;
  onToggleStationDeliveryStatus: (stationId: number) => void;
  onToggleStationConfirmedStatus: (stationId: number) => void;
  onRemoveStation: (stationId: number) => void;
};

const TrafficStationsSection = memo(function TrafficStationsSection({
  activeAccountCode,
  activeDraft,
  canEditTradsphere,
  isSaving,
  isTrafficEditingLocked,
  isConfirmedLocked,
  isStationAutoSyncing,
  isScheduleTimelineLoading,
  isLoadingAccountTraffic,
  isOnline,
  timelineAccountCode,
  stationSyncFlightRange,
  sortedStations,
  stationLookupMetaByCode,
  stationContactEmailsById,
  stationDeliveryStatusRestoreMap,
  stationConfirmedStatusRestoreMap,
  onManualStationSync,
  onOpenScheduleTimeline,
  onAddStation,
  onEditStation,
  onOpenDeliveryMethodDetail,
  onCopyStationContacts,
  onToggleStationDeliveryStatus,
  onToggleStationConfirmedStatus,
  onRemoveStation,
}: TrafficStationsSectionProps) {
  return (
    <SectionCard
      title="Stations"
      actions={(
        <div className="flex items-center gap-1">
          <ActionIconButton
            icon={isStationAutoSyncing ? <Loader2 className="animate-spin" /> : <RefreshCw />}
            tooltip="Sync stations from flight date range"
            aria-label="Sync stations from flight date range"
            title="Sync stations from flight date range"
            onClick={onManualStationSync}
            disabled={
              !activeDraft
              || !stationSyncFlightRange
              || !canEditTradsphere
              || isSaving
              || isStationAutoSyncing
              || isTrafficEditingLocked
              || !isOnline
            }
            className="!h-7 !w-7 !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-4 [&_svg]:!w-4 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110"
          />
          <ActionIconButton
            icon={(isScheduleTimelineLoading || isStationAutoSyncing) ? <Loader2 className="animate-spin" /> : <Table2 />}
            tooltip="View Schedule Timeline"
            aria-label="View Schedule Timeline"
            title="View Schedule Timeline"
            onClick={onOpenScheduleTimeline}
            disabled={!timelineAccountCode || !stationSyncFlightRange || isLoadingAccountTraffic || isStationAutoSyncing || isTrafficEditingLocked}
            className="!h-7 !w-7 !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-4 [&_svg]:!w-4 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110"
          />
          <ActionIconButton
            icon={<Plus />}
            tooltip="Add Station"
            aria-label="Add Station"
            title="Add Station"
            onClick={onAddStation}
            disabled={!canEditTradsphere || isSaving || isTrafficEditingLocked}
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
        ) : sortedStations.length === 0 ? (
          <div className="rounded-lg border border-dashed border-slate-200 bg-white px-3 py-4 text-sm text-slate-600">
            No station rows yet.
          </div>
        ) : (
          <div className="overflow-x-auto rounded-xl border border-slate-200 bg-white">
            <table className="min-w-full text-left text-xs">
              <thead className="bg-slate-50 text-slate-600">
                <tr>
                  <th className="px-3 py-2 font-semibold">Station</th>
                  <th className="px-3 py-2 text-center font-semibold">Language</th>
                  <th className="px-3 py-2 text-center font-semibold">Delivery Method</th>
                  <th className="px-3 py-2 font-semibold">Contacts</th>
                  <th className="px-3 py-2 text-center font-semibold">Delivery Status</th>
                  <th className="px-3 py-2 text-center font-semibold">Confirmed Status</th>
                  <th className="px-3 py-2 text-right font-semibold"></th>
                </tr>
              </thead>
              <tbody>
                {sortedStations.map((station) => (
                  <tr
                    key={station.id}
                    onClick={() => {
                      if (isConfirmedLocked) {
                        return;
                      }
                      onEditStation(station.id);
                    }}
                    className={`group border-t border-slate-200 text-slate-700 transition ${isConfirmedLocked ? "cursor-not-allowed" : "cursor-pointer hover:bg-blue-50/40"}`}
                  >
                    <td className="whitespace-nowrap px-3 py-2">
                      {formatStationDisplayLabel(station.stationCode, stationLookupMetaByCode[asString(station.stationCode).toUpperCase()])}
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 text-center">
                      {(() => {
                        const stationLanguage = stationLookupMetaByCode[asString(station.stationCode).toUpperCase()]?.language || "";
                        if (!stationLanguage) {
                          return "-";
                        }
                        return (
                          <span className={`inline-flex rounded-full border px-2 py-0.5 text-[11px] font-medium ${languageChipClass(stationLanguage)}`}>
                            {stationLanguage}
                          </span>
                        );
                      })()}
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 text-center">
                      <button
                        type="button"
                        onClick={(event) => {
                          event.stopPropagation();
                          if (isTrafficEditingLocked) {
                            return;
                          }
                          onOpenDeliveryMethodDetail(station.stationCode);
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
                              if (isTrafficEditingLocked) {
                                return;
                              }
                              void onCopyStationContacts(allEmails);
                            }}
                          />
                        );
                      })()}
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 text-center">
                      {(() => {
                        const deliveryStatusValue = asString(station.deliveryStatus).toLowerCase();
                        const hasDeliveryRestore = Object.prototype.hasOwnProperty.call(stationDeliveryStatusRestoreMap, station.id);
                        const deliveryStatusInteractive = canEditTradsphere && !isSaving && !isConfirmedLocked && (deliveryStatusValue !== "ready_to_email" || hasDeliveryRestore);
                        const deliveryStatusChip = (
                          <button
                            type="button"
                            onClick={deliveryStatusInteractive ? (event) => {
                              event.stopPropagation();
                              onToggleStationDeliveryStatus(station.id);
                            } : undefined}
                            disabled={!deliveryStatusInteractive}
                            aria-pressed={deliveryStatusValue === "ready_to_email"}
                            aria-disabled={!deliveryStatusInteractive}
                            className={`inline-flex rounded-full border px-2 py-0.5 text-[11px] font-medium transition focus-visible:outline-none focus-visible:ring-2 ${stationStatusChipClass(station.deliveryStatus)} ${deliveryStatusInteractive ? "cursor-pointer hover:brightness-95" : "cursor-default opacity-80"}`}
                          >
                            {formatStatusOptionLabel(station.deliveryStatus)}
                          </button>
                        );

                        if (!deliveryStatusInteractive) {
                          return deliveryStatusChip;
                        }

                        return (
                          <TooltipTarget
                            text={
                              deliveryStatusValue === "ready_to_email"
                                ? "Click to restore the original delivery status."
                                : "Click to mark Ready To Email. Click again to restore the original value."
                            }
                            placement="top"
                          >
                            {deliveryStatusChip}
                          </TooltipTarget>
                        );
                      })()}
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 text-center">
                      {(() => {
                        const confirmedStatusValue = asString(station.confirmedStatus).toLowerCase();
                        const hasConfirmedRestore = Object.prototype.hasOwnProperty.call(stationConfirmedStatusRestoreMap, station.id);
                        const confirmedStatusInteractive = canEditTradsphere && !isSaving && !isConfirmedLocked && (confirmedStatusValue !== "confirmed" || hasConfirmedRestore);
                        const confirmedStatusChip = (
                          <button
                            type="button"
                            onClick={confirmedStatusInteractive ? (event) => {
                              event.stopPropagation();
                              onToggleStationConfirmedStatus(station.id);
                            } : undefined}
                            disabled={!confirmedStatusInteractive}
                            aria-pressed={confirmedStatusValue === "confirmed"}
                            aria-disabled={!confirmedStatusInteractive}
                            className={`inline-flex rounded-full border px-2 py-0.5 text-[11px] font-medium transition focus-visible:outline-none focus-visible:ring-2 ${stationStatusChipClass(station.confirmedStatus)} ${confirmedStatusInteractive ? "cursor-pointer hover:brightness-95" : "cursor-default opacity-80"}`}
                          >
                            {formatStatusOptionLabel(station.confirmedStatus)}
                          </button>
                        );

                        if (!confirmedStatusInteractive) {
                          return confirmedStatusChip;
                        }

                        return (
                          <TooltipTarget
                            text={
                              confirmedStatusValue === "confirmed"
                                ? "Click to restore the original confirmation status."
                                : "Click to mark Confirmed. Click again to restore the original value."
                            }
                            placement="top"
                          >
                            {confirmedStatusChip}
                          </TooltipTarget>
                        );
                      })()}
                    </td>
                    <td className="px-3 py-2 text-right">
                      <ActionIconButton
                        icon={<Trash2 />}
                        tooltip="Delete station row"
                        aria-label="Delete station row"
                        title="Delete station row"
                        onClick={(event) => {
                          event.stopPropagation();
                          onRemoveStation(station.id);
                        }}
                        disabled={!canEditTradsphere || isSaving || isTrafficEditingLocked}
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
  );
});

type TrafficEmailWorkspaceSectionProps = {
  activeAccountCode: string;
  activeDraft: TrafficDetail | null;
  activeEmailWorkspace: TrafficEmailWorkspaceDraft | null;
  canEditTradsphere: boolean;
  isSaving: boolean;
  isEmailLocked: boolean;
  isSendingEmail: boolean;
  isMarkingTrafficSent: boolean;
  isUnlockingTraffic: boolean;
  emailLockBannerKind: TrafficLockBannerKind | null;
  emailWorkspacePrimaryActionLabel: string;
  contactNameByEmail: Record<string, string>;
  onOpenEmailPreviewModal: () => void;
  onUnlockTraffic: () => void;
  onOpenDownloadLinkNoteModal: (rowIndex: number) => void;
  onCommitEmailWorkspace: (workspace: TrafficEmailWorkspaceDraft) => void;
  onUpdateDraft: (updater: (current: TrafficDetail) => TrafficDetail) => void;
};

const TrafficEmailWorkspaceSection = memo(function TrafficEmailWorkspaceSection({
  activeAccountCode,
  activeDraft,
  activeEmailWorkspace,
  canEditTradsphere,
  isSaving,
  isEmailLocked,
  isSendingEmail,
  isMarkingTrafficSent,
  isUnlockingTraffic,
  emailLockBannerKind,
  emailWorkspacePrimaryActionLabel,
  contactNameByEmail,
  onOpenEmailPreviewModal,
  onUnlockTraffic,
  onOpenDownloadLinkNoteModal,
  onCommitEmailWorkspace,
  onUpdateDraft,
}: TrafficEmailWorkspaceSectionProps) {
  return (
    <SectionCard
      id="traffic-workspace-panel-email"
      title={(
        <div className="space-y-1">
          <p>Email Workspace</p>
          <span className={`inline-flex rounded-full border px-2 py-0.5 text-[11px] font-medium ${emailStatusChipClass(activeDraft?.email?.sentStatus || "draft")}`}>
            {formatStatusOptionLabel(activeDraft?.email?.sentStatus || "draft")}
          </span>
        </div>
      )}
      actions={(
        <div className="flex items-center gap-1">
          <ActionIconButton
            icon={isSendingEmail ? <Loader2 className="animate-spin" /> : (isEmailLocked ? <Eye /> : <Send />)}
            tooltip={isSendingEmail ? "Sending email..." : emailWorkspacePrimaryActionLabel}
            aria-label={isSendingEmail ? "Sending email" : emailWorkspacePrimaryActionLabel}
            title={isSendingEmail ? "Sending email..." : emailWorkspacePrimaryActionLabel}
            onClick={onOpenEmailPreviewModal}
            disabled={
              !canEditTradsphere
              || !activeDraft
              || !activeDraft.email
              || !activeEmailWorkspace
              || isSaving
              || isSendingEmail
              || isMarkingTrafficSent
            }
            className="!h-7 !w-7 !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-4 [&_svg]:!w-4 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110"
          />
        </div>
      )}
      contentClassName="space-y-4"
    >
      {!activeAccountCode ? (
        <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
          Select an account to view the email draft.
        </div>
      ) : !activeDraft ? (
        <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
          Select a traffic record to manage the email draft.
        </div>
      ) : !activeEmailWorkspace ? (
        <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
          Email workspace is unavailable for this traffic record.
        </div>
      ) : (
        <>
          <div className="rounded-xl border border-blue-100 bg-blue-50/40 p-3">
            {emailLockBannerKind ? (
              <>
                <TrafficLockBanner
                  kind={emailLockBannerKind}
                  isUnlocking={isUnlockingTraffic}
                  disabled={!canEditTradsphere || isSaving || isUnlockingTraffic}
                  onUnlock={() => {
                    onUnlockTraffic();
                  }}
                />
                <div className="h-3" />
              </>
            ) : null}
            <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">Recipients</p>
            <p className="mt-1 text-xs text-slate-500">Manage recipients and draft subject.</p>
            <div className="mt-3 space-y-2">
              <EmailChipsInput
                value={activeDraft.email?.toEmails || []}
                placeholder="To emails"
                disabled={!canEditTradsphere || isSaving || isEmailLocked}
                labelByEmail={contactNameByEmail}
                onChange={(nextEmails) => {
                  if (!activeDraft.email) {
                    return;
                  }
                  onUpdateDraft((current) => ({
                    ...current,
                    email: current.email
                      ? {
                          ...current.email,
                          toEmails: nextEmails,
                        }
                      : current.email,
                  }));
                }}
              />
              <div className="grid gap-2 md:grid-cols-2">
                <EmailChipsInput
                  value={activeDraft.email?.ccEmails || []}
                  placeholder="CC emails"
                  disabled={!canEditTradsphere || isSaving || isEmailLocked}
                  labelByEmail={contactNameByEmail}
                  onChange={(nextEmails) => {
                    if (!activeDraft.email) {
                      return;
                    }
                    onUpdateDraft((current) => ({
                      ...current,
                      email: current.email
                        ? {
                            ...current.email,
                            ccEmails: nextEmails,
                          }
                        : current.email,
                    }));
                  }}
                />
                <EmailChipsInput
                  value={activeDraft.email?.bccEmails || []}
                  placeholder="BCC emails"
                  disabled={!canEditTradsphere || isSaving || isEmailLocked}
                  labelByEmail={contactNameByEmail}
                  onChange={(nextEmails) => {
                    if (!activeDraft.email) {
                      return;
                    }
                    onUpdateDraft((current) => ({
                      ...current,
                      email: current.email
                        ? {
                            ...current.email,
                            bccEmails: nextEmails,
                          }
                        : current.email,
                    }));
                  }}
                />
              </div>
              <Input
                value={activeDraft.email?.subject || ""}
                placeholder="Email subject"
                disabled={!canEditTradsphere || isSaving || isEmailLocked}
                onChange={(event) => {
                  if (!activeDraft.email) {
                    return;
                  }
                  onUpdateDraft((current) => ({
                    ...current,
                    email: current.email
                      ? {
                          ...current.email,
                          subject: event.target.value,
                        }
                      : current.email,
                  }));
                }}
              />
            </div>
          </div>

          <div className="rounded-xl border border-slate-200 bg-white p-3">
            <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">Body Content</p>
            <p className="mt-1 text-xs text-slate-500">Main message shown in the email body. Use the toolbar to format text.</p>
            <RichTextEditor
              value={activeEmailWorkspace.bodyContent}
              placeholder="Email body"
              className="mt-3"
              editorMinHeight="130px"
              disabled={!canEditTradsphere || isSaving || isEmailLocked}
              onChange={(nextValue) => {
                onCommitEmailWorkspace({
                  ...activeEmailWorkspace,
                  bodyContent: nextValue,
                  bodyTouched: true,
                });
              }}
            />
          </div>

          <div className="rounded-xl border border-slate-200 bg-white p-3">
            <div>
              <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">Download Links</p>
              <p className="mt-1 text-xs text-slate-500">
                ISCI, File URL, and Script URL are auto-synced from Flights. To update these values, edit the corresponding flight row.
              </p>
            </div>
            <div className="mt-3">
              {activeEmailWorkspace.downloadLinks.length === 0 ? (
                <div className="rounded-lg border border-dashed border-slate-200 bg-slate-50 px-3 py-3 text-xs text-slate-500">
                  No flights found. Add flight rows to auto-generate this section.
                </div>
              ) : (
                <div className="overflow-x-auto rounded-lg border border-slate-200">
                  <table className="min-w-full text-left text-xs">
                    <thead className="bg-slate-50 text-slate-600">
                      <tr>
                        <th className="px-3 py-2 font-semibold">ISCI</th>
                        <th className="px-3 py-2 font-semibold">File URL</th>
                        <th className="px-3 py-2 font-semibold">Script URL</th>
                        <th className="px-3 py-2 font-semibold">Note</th>
                      </tr>
                    </thead>
                    <tbody>
                      {activeEmailWorkspace.downloadLinks.map((item, index) => {
                        const fileUrl = normalizeExternalUrl(item.fileUrl);
                        const scriptUrl = normalizeExternalUrl(item.scriptUrl);
                        const notePreview = formatDownloadLinkNotePreview(item.note);
                        const hasNote = Boolean(notePreview);
                        return (
                          <tr key={`download-link-${item.flightId ?? index}`} className="border-t border-slate-200 text-slate-700">
                            <td className="whitespace-nowrap px-3 py-2.5 align-middle">{asString(item.isci) || "—"}</td>
                            <td className="px-3 py-2.5 align-middle">
                              {fileUrl ? (
                                <a
                                  href={fileUrl}
                                  target="_blank"
                                  rel="noreferrer"
                                  className="text-blue-700 hover:text-blue-800 hover:underline"
                                >
                                  Open file
                                </a>
                              ) : (
                                <span className="text-slate-400">Missing</span>
                              )}
                            </td>
                            <td className="px-3 py-2.5 align-middle">
                              {scriptUrl ? (
                                <a
                                  href={scriptUrl}
                                  target="_blank"
                                  rel="noreferrer"
                                  className="text-blue-700 hover:text-blue-800 hover:underline"
                                >
                                  Open script
                                </a>
                              ) : (
                                <span className="text-slate-400">Missing</span>
                              )}
                            </td>
                            <td className="px-3 py-2.5 align-middle">
                              <div className="flex items-center gap-2">
                                <div className="min-w-0 flex-1 whitespace-pre-wrap break-words text-xs leading-5 text-slate-700">
                                  {hasNote ? asString(item.note).trim() : <span className="text-slate-400">—</span>}
                                </div>
                                <ActionIconButton
                                  icon={<Pencil />}
                                  tooltip={hasNote ? "Edit note" : "Add note"}
                                  aria-label={hasNote ? "Edit note" : "Add note"}
                                  title={hasNote ? `Edit note: ${notePreview}` : "Add note"}
                                  onClick={() => onOpenDownloadLinkNoteModal(index)}
                                  disabled={!canEditTradsphere || isSaving || isEmailLocked}
                                  className="!h-8 !w-8 !shrink-0 !rounded-full !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-3.5 [&_svg]:!w-3.5"
                                />
                              </div>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>

          <div className="rounded-xl border border-slate-200 bg-white p-3">
            <p className="text-xs font-semibold uppercase tracking-wide text-slate-600">Traffic Instructions</p>
            <p className="mt-1 text-xs text-slate-500">Example: Air ASAP, Expired date, and airing guidance. Use the toolbar to format text.</p>
            <RichTextEditor
              value={activeEmailWorkspace.instructionsContent}
              placeholder="Air ASAP"
              className="mt-3"
              editorMinHeight="120px"
              disabled={!canEditTradsphere || isSaving || isEmailLocked}
              onChange={(nextValue) => {
                onCommitEmailWorkspace({
                  ...activeEmailWorkspace,
                  instructionsContent: nextValue,
                  instructionsTouched: true,
                });
              }}
            />
          </div>
        </>
      )}
    </SectionCard>
  );
});

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

function normalizeSearchTokenList(
  rawValues: unknown[],
  options?: {
    toUpper?: boolean;
    toLower?: boolean;
  },
): string[] {
  const pending: string[] = [];

  const pushRaw = (value: unknown) => {
    if (value === null || value === undefined) {
      return;
    }
    if (Array.isArray(value)) {
      for (const entry of value) {
        pushRaw(entry);
      }
      return;
    }
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (!trimmed) {
        return;
      }
      if (trimmed.startsWith("[")) {
        try {
          const parsed = JSON.parse(trimmed);
          if (Array.isArray(parsed)) {
            pushRaw(parsed);
            return;
          }
        } catch {
          // Fallback to plain-text token splitting.
        }
      }
      pending.push(trimmed);
      return;
    }
    pending.push(asString(value));
  };

  for (const value of rawValues) {
    pushRaw(value);
  }

  const seen = new Set<string>();
  const output: string[] = [];
  for (const raw of pending) {
    for (const token of raw.split(/\|\||[;,\n]+/g)) {
      let normalized = asString(token);
      if (!normalized) {
        continue;
      }
      if (options?.toUpper) {
        normalized = normalized.toUpperCase();
      } else if (options?.toLower) {
        normalized = normalized.toLowerCase();
      }
      if (!normalized || seen.has(normalized)) {
        continue;
      }
      seen.add(normalized);
      output.push(normalized);
    }
  }
  return output;
}

function normalizeSearchKeyword(value: string): string {
  return asString(value).toLowerCase();
}

function areStringArraysEqual(left: string[], right: string[]): boolean {
  if (left === right) {
    return true;
  }
  if (left.length !== right.length) {
    return false;
  }
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) {
      return false;
    }
  }
  return true;
}

function areRotationSummariesEqual(left: RotationSummary, right: RotationSummary): boolean {
  if (left === right) {
    return true;
  }
  if (
    left.totalRotation !== right.totalRotation
    || left.rotationWarning !== right.rotationWarning
    || left.rotationWarningMessage !== right.rotationWarningMessage
    || left.warnings.length !== right.warnings.length
  ) {
    return false;
  }
  for (let index = 0; index < left.warnings.length; index += 1) {
    const leftWarning = left.warnings[index];
    const rightWarning = right.warnings[index];
    if (leftWarning.code !== rightWarning.code || leftWarning.message !== rightWarning.message) {
      return false;
    }
  }
  return true;
}

function areNormalizedFlightsEqual(left: Record<string, unknown> | null, right: Record<string, unknown> | null): boolean {
  if (left === right) {
    return true;
  }
  if (!left || !right) {
    return false;
  }
  return (
    left.flightStart === right.flightStart
    && left.flightEnd === right.flightEnd
    && left.medium === right.medium
    && left.language === right.language
    && left.length === right.length
    && left.isci === right.isci
    && left.rotation === right.rotation
    && left.fileUrl === right.fileUrl
    && left.scriptUrl === right.scriptUrl
    && left.note === right.note
  );
}

function areNormalizedStationsEqual(left: Record<string, unknown> | null, right: Record<string, unknown> | null): boolean {
  if (left === right) {
    return true;
  }
  if (!left || !right) {
    return false;
  }
  return (
    left.stationCode === right.stationCode
    && left.deliveryStatus === right.deliveryStatus
    && left.confirmedStatus === right.confirmedStatus
    && left.note === right.note
  );
}

function trafficMatchesSearch(item: TrafficSummary, keyword: string): boolean {
  if (!keyword) {
    return true;
  }
  if (item.searchCampaign.includes(keyword)) {
    return true;
  }
  if (item.searchIscis.some((isci) => isci.includes(keyword))) {
    return true;
  }
  if (item.searchStations.some((station) => station.includes(keyword))) {
    return true;
  }
  if (item.searchEmails.some((email) => email.includes(keyword))) {
    return true;
  }
  return false;
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

function formatBroadcastMonthLabel(year: number, month: number): string {
  const monthNames = [
    "",
    "JAN",
    "FEB",
    "MAR",
    "APR",
    "MAY",
    "JUN",
    "JUL",
    "AUG",
    "SEP",
    "OCT",
    "NOV",
    "DEC",
  ];
  if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) {
    return "";
  }
  return `${monthNames[month]}'${String(Math.trunc(year)).slice(-2)}`;
}

function formatSpotTooltipText(spotCount: number, grossText: string): string {
  const count = Math.max(0, Math.trunc(spotCount));
  const label = count === 1 ? "spot" : "spots";
  const amount = asString(grossText) || "$0.00";
  return `${count} ${label} (${amount})`;
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

function normalizeTrafficSummary(raw: unknown): TrafficSummary | null {
  if (!isRecord(raw)) {
    return null;
  }

  const id = asString(raw.id);
  if (!id) {
    return null;
  }

  return {
    id,
    accountCode: asString(raw.accountCode).toUpperCase(),
    campaign: asString(raw.campaign),
    searchCampaign: asString(raw.searchCampaign || raw.campaign).toLowerCase(),
    status: (asString(raw.status).toLowerCase() as TrafficStatus) || "draft",
    note: asNullableString(raw.note),
    dateCreated: asNullableString(raw.dateCreated),
    dateUpdated: asNullableString(raw.dateUpdated),
    flightCount: Math.max(0, Math.trunc(asNumber(raw.flightCount, 0))),
    stationCount: Math.max(0, Math.trunc(asNumber(raw.stationCount, 0))),
    emailSentStatus: asNullableString(raw.emailSentStatus),
    emailSentAt: asNullableString(raw.emailSentAt),
    searchIscis: normalizeSearchTokenList(
      [raw.searchIscis, raw.isciSearch],
      { toUpper: true },
    ),
    searchStations: normalizeSearchTokenList(
      [raw.searchStations, raw.stationCodeSearch, raw.stationNameSearch],
      { toUpper: true },
    ),
    searchEmails: normalizeSearchTokenList(
      [raw.searchEmails, raw.searchToEmails, raw.searchCcEmails, raw.searchBccEmails],
      { toLower: true },
    ),
    summary: normalizeRotationSummary(raw.summary),
  };
}

function normalizeTrafficSummaryList(payload: unknown): TrafficSummary[] {
  const data = unwrapData(payload);
  if (!Array.isArray(data)) {
    return [];
  }

  return data
    .map((item) => normalizeTrafficSummary(item))
    .filter((row): row is TrafficSummary => Boolean(row));
}

function normalizeTrafficListItemFromBulkSave(payload: unknown): TrafficSummary | null {
  const data = unwrapData(payload);
  if (!isRecord(data)) {
    return null;
  }
  if ("trafficListItem" in data) {
    return normalizeTrafficSummary(data.trafficListItem);
  }
  if ("trafficSummary" in data) {
    return normalizeTrafficSummary(data.trafficSummary);
  }
  return normalizeTrafficSummary(data);
}

function normalizeTrafficEmail(raw: unknown): TrafficEmail | null {
  if (!isRecord(raw)) {
    return null;
  }
  return {
    id: Math.trunc(asNumber(raw.id, 0)),
    trafficId: asString(raw.trafficId),
    toEmails: asStringArray(raw.toEmails),
    ccEmails: asStringArray(raw.ccEmails),
    bccEmails: asStringArray(raw.bccEmails),
    subject: asString(raw.subject),
    body: typeof raw.body === "string" ? raw.body : asString(raw.body),
    sentStatus: asString(raw.sentStatus).toLowerCase() || "draft",
    sentAt: asNullableString(raw.sentAt),
    sentByUserId: asNullableString(raw.sentByUserId),
    smtpMessageId: asNullableString(raw.smtpMessageId),
    lastSendAttemptAt: asNullableString(raw.lastSendAttemptAt),
    lastSendError: asNullableString(raw.lastSendError),
    dateCreated: asNullableString(raw.dateCreated),
    dateUpdated: asNullableString(raw.dateUpdated),
  };
}

function normalizeTrafficStationCandidateSummaryMonth(raw: unknown): TrafficStationCandidateSummaryMonth | null {
  if (!isRecord(raw)) {
    return null;
  }
  const year = Math.max(0, Math.trunc(asNumber(raw.year, 0)));
  const month = Math.max(0, Math.trunc(asNumber(raw.month, 0)));
  const monthKey = asString(raw.monthKey);
  const label = asString(raw.label) || formatBroadcastMonthLabel(year, month);
  if (!monthKey || !label || year < 1000 || month < 1 || month > 12) {
    return null;
  }
  return {
    monthKey,
    year,
    month,
    label,
  };
}

function normalizeTrafficStationCandidateSummaryCell(raw: unknown): TrafficStationCandidateSummaryCell | null {
  if (!isRecord(raw)) {
    return null;
  }
  const year = Math.max(0, Math.trunc(asNumber(raw.year, 0)));
  const month = Math.max(0, Math.trunc(asNumber(raw.month, 0)));
  const monthKey = asString(raw.monthKey);
  const label = asString(raw.label) || formatBroadcastMonthLabel(year, month);
  if (!monthKey || !label || year < 1000 || month < 1 || month > 12) {
    return null;
  }
  return {
    monthKey,
    year,
    month,
    label,
    hasSchedule: Boolean(raw.hasSchedule),
    hasSpot: Boolean(raw.hasSpot),
    scheduleCount: Math.max(0, Math.trunc(asNumber(raw.scheduleCount, 0))),
    totalSpot: Math.max(0, Math.trunc(asNumber(raw.totalSpot, 0))),
    totalGrossText: asString(raw.totalGrossText) || "$0.00",
  };
}

function normalizeTrafficStationCandidateSummaryRow(raw: unknown): TrafficStationCandidateSummaryRow | null {
  if (!isRecord(raw)) {
    return null;
  }
  const estNum = Math.trunc(asNumber(raw.estNum, -1));
  const stationCode = asString(raw.stationCode).toUpperCase();
  if (!Number.isFinite(estNum) || estNum < 0 || !stationCode) {
    return null;
  }
  const monthCells = Array.isArray(raw.monthCells)
    ? raw.monthCells
      .map((cell) => normalizeTrafficStationCandidateSummaryCell(cell))
      .filter((cell): cell is TrafficStationCandidateSummaryCell => Boolean(cell))
    : [];
  return {
    estNum,
    stationCode,
    stationName: asNullableString(raw.stationName),
    monthCells,
  };
}

function normalizeTrafficStationCandidatesSummary(raw: unknown): TrafficStationCandidatesSummary {
  if (!isRecord(raw)) {
    return {
      candidateCount: 0,
      estNumCount: 0,
      months: [],
      rows: [],
    };
  }
  const monthsRaw = Array.isArray(raw.months) ? raw.months : [];
  const rowsRaw = Array.isArray(raw.rows) ? raw.rows : [];
  return {
    candidateCount: Math.max(0, Math.trunc(asNumber(raw.candidateCount, 0))),
    estNumCount: Math.max(0, Math.trunc(asNumber(raw.estNumCount, 0))),
    months: monthsRaw
      .map((item) => normalizeTrafficStationCandidateSummaryMonth(item))
      .filter((item): item is TrafficStationCandidateSummaryMonth => Boolean(item)),
    rows: rowsRaw
      .map((item) => normalizeTrafficStationCandidateSummaryRow(item))
      .filter((item): item is TrafficStationCandidateSummaryRow => Boolean(item)),
  };
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
  const compareFlightCreatedAt = (left: { dateCreated: string | null; id: number }, right: { dateCreated: string | null; id: number }) => {
    const leftCreated = asString(left.dateCreated);
    const rightCreated = asString(right.dateCreated);
    if (leftCreated && rightCreated && leftCreated !== rightCreated) {
      return leftCreated.localeCompare(rightCreated);
    }
    if (leftCreated && !rightCreated) {
      return -1;
    }
    if (!leftCreated && rightCreated) {
      return 1;
    }
    return left.id - right.id;
  };

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
        language: normalizeFlightLanguageValue(item.language),
        length: Math.max(0, Math.trunc(asNumber(item.length, 0))),
        isci: asNullableString(item.isci),
        rotation: asNumber(item.rotation, 100),
        fileUrl: asString(item.fileUrl),
        scriptUrl: asNullableString(item.scriptUrl),
        note: asNullableString(item.note),
        dateCreated: asNullableString(item.dateCreated),
        dateUpdated: asNullableString(item.dateUpdated),
      }))
      .sort(compareFlightCreatedAt),
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
    email: normalizeTrafficEmail(emailRaw)
      || {
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
  const estNumsRaw = Array.isArray(data.estNums) ? data.estNums : [];
  const stations = stationsRaw
    .filter(isRecord)
    .map((item) => ({
      stationCode: asString(item.stationCode).toUpperCase(),
      stationName: asNullableString(item.stationName),
      deliveryMethod: asNullableString(item.deliveryMethod),
      contactsSnapshot: isRecord(item.contactsSnapshot) ? item.contactsSnapshot : null,
    }))
    .filter((item) => Boolean(item.stationCode));
  const estNums = estNumsRaw
    .filter(isRecord)
    .map((item) => ({
      estNum: Math.trunc(asNumber(item.estNum, -1)),
      note: asNullableString(item.note),
      medium: asNullableString(asString(item.medium).toUpperCase()),
      stationCount: Math.max(0, Math.trunc(asNumber(item.stationCount, 0))),
    }))
    .filter((item) => Number.isFinite(item.estNum) && item.estNum >= 0);
  return {
    accountCode,
    flightStart,
    flightEnd,
    estNums,
    stations,
    summary: normalizeTrafficStationCandidatesSummary(data.summary),
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
    language: asString(raw.language),
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

function isStationLookupMetaComplete(meta: StationLookupMeta | null | undefined): boolean {
  if (!meta) {
    return false;
  }
  return Boolean(asString(meta.name) && asString(meta.mediaType));
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
        language: normalizeFlightLanguageValue(item.language),
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

function normalizeEstNumList(values: number[] | undefined): number[] {
  if (!Array.isArray(values)) {
    return [];
  }
  return [...new Set(values
    .map((item) => Math.trunc(asNumber(item, Number.NaN)))
    .filter((item) => Number.isFinite(item) && item >= 0))].sort((a, b) => a - b);
}

function normalizeFlightLanguageValue(value: unknown): FlightLanguage {
  const normalized = asString(value).toLowerCase();
  return normalized === "spanish" ? "Spanish" : "English";
}

function normalizeFlightLanguageList(values: unknown[] | undefined): FlightLanguage[] {
  if (!Array.isArray(values)) {
    return [];
  }
  return [...new Set(values.map((item) => normalizeFlightLanguageValue(item)))];
}

function normalizeFlightMediumList(values: unknown[] | undefined): string[] {
  if (!Array.isArray(values)) {
    return [];
  }
  const allowed = new Set(FLIGHT_MEDIA_OPTIONS.map((item) => asString(item).toUpperCase()));
  const dedupe = new Set<string>();
  const output: string[] = [];
  for (const raw of values) {
    const normalized = asString(raw).toUpperCase();
    if (!normalized || !allowed.has(normalized) || dedupe.has(normalized)) {
      continue;
    }
    dedupe.add(normalized);
    output.push(normalized);
  }
  return output;
}

function canonicalFlightMediumForCandidates(value: unknown): string {
  const normalized = asString(value).toUpperCase();
  if (normalized === "TV" || normalized === "CA") {
    return "TV_CA";
  }
  return normalized;
}

function expandFlightMediumsForCandidateFilter(values: string[]): string[] {
  const normalized = normalizeFlightMediumList(values);
  const output = new Set<string>();
  const includesTvOrCa = normalized.includes("TV") || normalized.includes("CA");
  for (const medium of normalized) {
    if (medium === "TV" || medium === "CA") {
      continue;
    }
    output.add(medium);
  }
  if (includesTvOrCa) {
    output.add("TV");
    output.add("CA");
  }
  return [...output];
}

function hasIsoDateRangeOverlap(
  rangeStart: string,
  rangeEnd: string,
  candidateStart: string,
  candidateEnd: string,
): boolean {
  if (!rangeStart || !rangeEnd || !candidateStart || !candidateEnd) {
    return false;
  }
  return candidateStart <= rangeEnd && candidateEnd >= rangeStart;
}

function resolveFlightLanguagesFromFlights(
  flights: TrafficFlight[],
  rangeStart?: string,
  rangeEnd?: string,
): FlightLanguage[] {
  const targetStart = asString(rangeStart);
  const targetEnd = asString(rangeEnd);
  const dedupe = new Set<FlightLanguage>();
  for (const flight of flights) {
    const flightStart = asString(flight.flightStart);
    const flightEnd = asString(flight.flightEnd);
    if (targetStart && targetEnd) {
      if (!hasIsoDateRangeOverlap(targetStart, targetEnd, flightStart, flightEnd)) {
        continue;
      }
    }
    dedupe.add(normalizeFlightLanguageValue(flight.language));
  }
  return [...dedupe];
}

function resolveFlightMediumsFromFlights(
  flights: TrafficFlight[],
  rangeStart?: string,
  rangeEnd?: string,
): string[] {
  const targetStart = asString(rangeStart);
  const targetEnd = asString(rangeEnd);
  const allowed = new Set(FLIGHT_MEDIA_OPTIONS.map((item) => asString(item).toUpperCase()));
  const dedupe = new Set<string>();
  for (const flight of flights) {
    const flightStart = asString(flight.flightStart);
    const flightEnd = asString(flight.flightEnd);
    if (targetStart && targetEnd) {
      if (!hasIsoDateRangeOverlap(targetStart, targetEnd, flightStart, flightEnd)) {
        continue;
      }
    }
    const medium = asString(flight.medium).toUpperCase();
    if (!medium || !allowed.has(medium)) {
      continue;
    }
    dedupe.add(medium);
  }
  return [...dedupe];
}

function buildTrafficStationCandidatesCacheKey(params: {
  accountCode: string;
  flightStart: string;
  flightEnd: string;
  estNums?: number[];
  languages?: FlightLanguage[];
  mediums?: string[];
}): string {
  const accountCode = asString(params.accountCode).toUpperCase() || "UNKNOWN";
  const flightStart = asString(params.flightStart) || "missing";
  const flightEnd = asString(params.flightEnd) || "missing";
  const estNums = normalizeEstNumList(params.estNums);
  const languages = normalizeFlightLanguageList(params.languages);
  const mediums = normalizeFlightMediumList(params.mediums);
  const estNumsKey = estNums.length > 0 ? estNums.join(",") : "*";
  const languagesKey = languages.length > 0 ? languages.join(",") : "*";
  const mediumsKey = mediums.length > 0
    ? [...new Set(mediums.map((medium) => canonicalFlightMediumForCandidates(medium)))].sort().join(",")
    : "*";
  return `traffic:station-candidates:${accountCode}:${flightStart}:${flightEnd}:estNums=${estNumsKey}:languages=${languagesKey}:mediums=${mediumsKey}:v3`;
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

function buildListSummaryFromDetail(
  detail: TrafficDetail,
  stationMetaByCode?: Record<string, StationLookupMeta>,
): TrafficSummary {
  const searchIscis = [...new Set(
    detail.flights
      .map((flight) => asString(flight.isci).toUpperCase())
      .filter(Boolean),
  )];
  const searchStations = [...new Set(
    detail.stations
      .flatMap((station) => {
        const stationCode = asString(station.stationCode).toUpperCase();
        const stationName = stationCode
          ? asString(stationMetaByCode?.[stationCode]?.name).toUpperCase()
          : "";
        return [stationCode, stationName];
      })
      .filter(Boolean),
  )];
  const searchEmails = [...new Set(
    [
      ...(detail.email?.toEmails ?? []),
      ...(detail.email?.ccEmails ?? []),
      ...(detail.email?.bccEmails ?? []),
    ]
      .map((email) => asString(email).toLowerCase())
      .filter(Boolean),
  )];
  return {
    id: detail.traffic.id,
    accountCode: detail.traffic.accountCode,
    campaign: detail.traffic.campaign,
    searchCampaign: asString(detail.traffic.campaign).toLowerCase(),
    status: detail.traffic.status,
    note: detail.traffic.note,
    dateCreated: detail.traffic.dateCreated,
    dateUpdated: detail.traffic.dateUpdated,
    flightCount: detail.summary.flightCount,
    stationCount: detail.summary.stationCount,
    emailSentStatus: detail.email?.sentStatus || null,
    emailSentAt: detail.email?.sentAt || null,
    searchIscis,
    searchStations,
    searchEmails,
    summary: {
      totalRotation: detail.summary.totalRotation,
      rotationWarning: detail.summary.rotationWarning,
      rotationWarningMessage: detail.summary.rotationWarningMessage,
      warnings: detail.summary.warnings,
    },
  };
}

function upsertTrafficSummaryInList(
  currentList: TrafficSummary[],
  summary: TrafficSummary,
  sourceDraftId: string | null,
  trafficId: string,
  isLocalDraft: boolean,
): TrafficSummary[] {
  const normalizedSourceDraftId = asString(sourceDraftId);
  const normalizedTrafficId = asString(trafficId);
  let changed = false;
  const nextList = currentList.map((item) => {
    if (item.id !== normalizedSourceDraftId && item.id !== normalizedTrafficId) {
      return item;
    }
    changed = true;
    return summary;
  });
  if (!changed && isLocalDraft) {
    return [summary, ...currentList];
  }
  return changed ? nextList : currentList;
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

function languageChipClass(language: string): string {
  const normalized = asString(language).toLowerCase();
  if (normalized === "english") {
    return "border-sky-200 bg-sky-50 text-sky-700";
  }
  if (normalized === "spanish") {
    return "border-amber-200 bg-amber-50 text-amber-700";
  }
  return "border-slate-200 bg-slate-100 text-slate-700";
}

function isGoogleDriveDeliveryMethod(value: string | null): boolean {
  return asString(value).toLowerCase().includes("google drive");
}

function hasFlightsForDelivery(flights: TrafficFlight[]): boolean {
  if (flights.length === 0) {
    return false;
  }
  return flights.every((flight) => Boolean(normalizeExternalUrl(flight.fileUrl)));
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
  const hasCompleteFlightAssets = hasFlightsForDelivery(detail.flights);
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

function buildIsciPlaceholder(accountCode: string, flightStart: string): string {
  const normalizedAccountCode = asString(accountCode).toUpperCase().replace(/\s+/g, "");
  const parsed = /^(\d{4})-(\d{2})-\d{2}$/.exec(asString(flightStart));
  const fallbackToday = new Date();
  const yearPart = parsed
    ? parsed[1].slice(-2)
    : String(fallbackToday.getFullYear()).slice(-2);
  const monthPart = parsed
    ? parsed[2]
    : String(fallbackToday.getMonth() + 1).padStart(2, "0");
  return `${normalizedAccountCode}${yearPart}${monthPart}11EH`;
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
    language: normalizeFlightLanguageValue(flight.language),
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

function buildStationEmailRecipients(stations: TrafficStation[]): string[] {
  const stationEmails = stations.flatMap((station) => extractPreferredContactEmails(station.contactsSnapshot));
  return mergeUniqueEmails([], stationEmails);
}

function buildStationSyncFingerprint(stations: TrafficStation[]): string {
  const normalized = stations
    .map((station) => normalizeStationForCompare(station))
    .filter((item): item is Record<string, unknown> => Boolean(item))
    .sort((left, right) => {
      const leftCode = asString(left.stationCode).toUpperCase();
      const rightCode = asString(right.stationCode).toUpperCase();
      if (leftCode !== rightCode) {
        return leftCode.localeCompare(rightCode);
      }
      const leftDelivery = asString(left.deliveryMethod).toUpperCase();
      const rightDelivery = asString(right.deliveryMethod).toUpperCase();
      if (leftDelivery !== rightDelivery) {
        return leftDelivery.localeCompare(rightDelivery);
      }
      return JSON.stringify(left).localeCompare(JSON.stringify(right));
    });
  return JSON.stringify(normalized);
}

function syncTrafficEmailRecipientsFromStations(previousDetail: TrafficDetail, nextDetail: TrafficDetail): TrafficDetail {
  if (!nextDetail.email) {
    return nextDetail;
  }
  const previousFingerprint = buildStationSyncFingerprint(previousDetail.stations);
  const nextFingerprint = buildStationSyncFingerprint(nextDetail.stations);
  if (previousFingerprint === nextFingerprint) {
    return nextDetail;
  }
  const nextToEmails = buildStationEmailRecipients(nextDetail.stations);
  if (areStringArraysEqual(nextToEmails, nextDetail.email.toEmails)) {
    return nextDetail;
  }
  return {
    ...nextDetail,
    email: {
      ...nextDetail.email,
      toEmails: nextToEmails,
    },
  };
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

function formatDownloadLinkNotePreview(noteValue: string): string {
  const firstLine = asString(noteValue)
    .replace(/\r\n/g, "\n")
    .split("\n")
    .map((line) => line.trim())
    .find(Boolean);
  if (!firstLine) {
    return "";
  }
  if (firstLine.length <= 48) {
    return firstLine;
  }
  return `${firstLine.slice(0, 45)}...`;
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
  const activeWorkspaceTabRaw = asString(payload.activeWorkspaceTab).toLowerCase();
  const activeWorkspaceTab: TrafficWorkspaceTab = activeWorkspaceTabRaw === "email" ? "email" : "workflow";
  const rawSelectionsByTrafficId = isRecord(payload.flightStationSyncSelectionsByTrafficId)
    ? payload.flightStationSyncSelectionsByTrafficId
    : null;
  const flightStationSyncSelectionsByTrafficId: Record<string, number[]> = {};
  if (rawSelectionsByTrafficId) {
    for (const [trafficIdRaw, rawValues] of Object.entries(rawSelectionsByTrafficId)) {
      const trafficId = asString(trafficIdRaw);
      if (!trafficId) {
        continue;
      }
      const normalizedValues = normalizeEstNumList(Array.isArray(rawValues) ? rawValues : []);
      if (normalizedValues.length === 0) {
        continue;
      }
      flightStationSyncSelectionsByTrafficId[trafficId] = normalizedValues;
    }
  }

  return {
    loadedAccountCode: asString(payload.loadedAccountCode).toUpperCase(),
    selectedTrafficId: asString(payload.selectedTrafficId) || null,
    trafficList: normalizeTrafficSummaryList(payload.trafficList),
    detailBaseline: normalizeTrafficDetail(payload.detailBaseline),
    detailDraft: normalizeTrafficDetail(payload.detailDraft),
    cacheStatus,
    refreshMessage: asNullableString(payload.refreshMessage),
    activeWorkspaceTab,
    flightStationSyncSelectionsByTrafficId,
  };
}

function createEmptyFlightDraft(trafficId: string): TrafficFlight {
  return {
    id: 0,
    trafficId,
    flightStart: getTodayIsoDate(),
    flightEnd: getEndOfMonthIsoDate(),
    medium: "TV",
    language: "English",
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

function normalizeStrictHttpUrlInput(value: string | null | undefined): string {
  const normalized = normalizeExternalUrl(value);
  if (!normalized) {
    return "";
  }
  return isStrictHttpUrlInput(normalized) ? normalized : "";
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
  if (fileUrl && !normalizeStrictHttpUrlInput(fileUrl)) {
    return "File URL must be a valid http(s) URL (for example, https://example.com/file).";
  }
  const scriptUrl = asString(flight.scriptUrl || "");
  if (scriptUrl && !normalizeStrictHttpUrlInput(scriptUrl)) {
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

function hasTrafficDetailChanges(baseline: TrafficDetail | null, draft: TrafficDetail | null): boolean {
  return buildFingerprint(baseline) !== buildFingerprint(draft);
}

function resolveTrafficEmailExpiryLabel(flights: TrafficFlight[]): string {
  const dates = flights
    .map((flight) => asString(flight.flightEnd))
    .filter((value) => /^(\d{4})-(\d{2})-(\d{2})$/.test(value));
  if (!dates.length) {
    return "";
  }
  const latest = [...dates].sort().at(-1) || "";
  const formatted = formatIsoDateForTable(latest);
  return formatted === "-" ? "" : formatted;
}

function mergeTrafficEmailDownloadLinksByFlight(
  flights: TrafficFlight[],
  previousLinks: TrafficEmailDownloadLink[] | null | undefined,
): TrafficEmailDownloadLink[] {
  const previousByFlightId = new Map<number, TrafficEmailDownloadLink>();
  const previousByFallbackKey = new Map<string, TrafficEmailDownloadLink>();
  for (const item of previousLinks ?? []) {
    if (typeof item.flightId === "number" && Number.isFinite(item.flightId)) {
      previousByFlightId.set(item.flightId, item);
    }
    const fallbackKey = `${asString(item.isci)}::${asString(item.fileUrl)}::${asString(item.scriptUrl)}`;
    if (fallbackKey !== "::") {
      previousByFallbackKey.set(fallbackKey, item);
    }
  }

  return flights.map((flight) => {
    const flightId = Number.isFinite(flight.id) ? flight.id : null;
    const fallbackKey = `${asString(flight.isci || "")}::${asString(flight.fileUrl || "")}::${asString(flight.scriptUrl || "")}`;
    const previous = (flightId !== null ? previousByFlightId.get(flightId) : undefined) || previousByFallbackKey.get(fallbackKey);
    return {
      flightId,
      isci: asString(flight.isci || ""),
      fileUrl: asString(flight.fileUrl || ""),
      scriptUrl: asString(flight.scriptUrl || ""),
      note: asString(previous?.note || ""),
    };
  });
}

function buildDefaultTrafficInstructionsText(flights: TrafficFlight[]): string {
  const expiryLabel = resolveTrafficEmailExpiryLabel(flights);
  return [
    "Air ASAP",
    expiryLabel ? `Expired: ${expiryLabel}` : "Expired: (set expiration date)",
    "Keep airing after expiration or until the new commercial is sent.",
  ].join("\n");
}

function buildTrafficEmailWorkspaceFromDetail(detail: TrafficDetail): TrafficEmailWorkspaceDraft {
  const parsedBody = parseTrafficEmailWorkspaceFromBody(asString(detail.email?.body || ""));

  return {
    bodyContent: parsedBody.workspace?.bodyContent ?? normalizeRichTextHtml(parsedBody.bodyFallbackText),
    instructionsContent: parsedBody.workspace?.instructionsContent ?? normalizeRichTextHtml(buildDefaultTrafficInstructionsText(detail.flights)),
    downloadLinks: mergeTrafficEmailDownloadLinksByFlight(detail.flights, parsedBody.workspace?.downloadLinks),
    bodyTouched: false,
    instructionsTouched: false,
  };
}

function TrafficStationCandidateSummaryTable({
  summary,
}: {
  summary: TrafficStationCandidatesSummary;
}) {
  if (summary.months.length === 0 || summary.rows.length === 0) {
    return null;
  }

  return (
    <div className="w-full space-y-2 rounded-md border border-slate-200 bg-slate-50/80 p-2">
      <div className="flex items-center justify-between gap-2 text-[11px] text-slate-600">
        <span>
          {summary.rows.length} station row(s) across {summary.months.length} broadcast month(s)
        </span>
        <span>Has schedule / spot by broadcast month</span>
      </div>
      <div className="max-h-72 w-full overflow-auto rounded-md border border-slate-200 bg-white">
        <table className="w-full min-w-full border-collapse text-xs">
          <thead className="sticky top-0 z-10 bg-slate-100">
            <tr className="text-slate-700">
              <th className="border-b border-r border-slate-200 bg-slate-100 px-3 py-2 text-left font-semibold">
                Estnum
              </th>
              <th className="border-b border-r border-slate-200 bg-slate-100 px-3 py-2 text-left font-semibold">
                Station
              </th>
              {summary.months.map((month) => (
                <th
                  key={month.monthKey}
                  className="min-w-[96px] border-b border-r border-slate-200 px-3 py-2 text-center font-semibold last:border-r-0"
                >
                  {month.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {summary.rows.map((row) => (
              <tr key={`${row.estNum}:${row.stationCode}`} className="odd:bg-white even:bg-slate-50">
                <td className="border-b border-r border-slate-200 px-3 py-2 font-mono text-slate-700">
                  {row.estNum}
                </td>
                <td className="border-b border-r border-slate-200 px-3 py-2">
                  <div className="font-medium text-slate-800">
                    {row.stationName || row.stationCode}
                  </div>
                  {row.stationName && row.stationName !== row.stationCode ? (
                    <div className="text-[11px] text-slate-500">{row.stationCode}</div>
                  ) : null}
                </td>
                {row.monthCells.map((cell) => {
                  const hasMatch = cell.hasSchedule || cell.hasSpot;
                  const title = formatSpotTooltipText(cell.totalSpot, cell.totalGrossText);
                  return (
                    <td key={cell.monthKey} className="border-b border-r border-slate-200 px-3 py-2 text-center last:border-r-0">
                      <TooltipTarget text={title}>
                        <span
                          className={[
                            "inline-flex items-center justify-center font-semibold",
                            hasMatch ? "text-blue-600" : "text-slate-300",
                          ].join(" ")}
                          aria-label={title}
                        >
                          {hasMatch ? "✓" : "—"}
                        </span>
                      </TooltipTarget>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
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
    refreshSelections,
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
  const accountNameByCodeRef = useRef<Record<string, string>>(accountNameByCode);
  useEffect(() => {
    accountNameByCodeRef.current = accountNameByCode;
  }, [accountNameByCode]);

  const [trafficList, setTrafficList] = useState<TrafficSummary[]>([]);
  const [draftTrafficSearch, setDraftTrafficSearch] = useState("");
  const [appliedTrafficSearch, setAppliedTrafficSearch] = useState("");
  const [selectedTrafficId, setSelectedTrafficId] = useState<string | null>(null);
  const [detailBaseline, setDetailBaseline] = useState<TrafficDetail | null>(null);
  const [detailDraft, setDetailDraft] = useState<TrafficDetail | null>(null);
  const [draftSessionsByTrafficId, setDraftSessionsByTrafficId] = useState<Record<string, TrafficDraftSession>>({});

  const [isLoadingAccountTraffic, setIsLoadingAccountTraffic] = useState(false);
  const [isRefreshingAccountTraffic, setIsRefreshingAccountTraffic] = useState(false);
  const [isLoadingDetail, setIsLoadingDetail] = useState(false);
  const [isRefreshingDetail, setIsRefreshingDetail] = useState(false);
  const isDetailBusy = isLoadingDetail || isRefreshingDetail;
  const [isLoadActionOverlayVisible, setIsLoadActionOverlayVisible] = useState(false);
  const [isSelectorRefreshOverlayVisible, setIsSelectorRefreshOverlayVisible] = useState(false);
  const [isChipRefreshOverlayVisible, setIsChipRefreshOverlayVisible] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [isSendingEmail, setIsSendingEmail] = useState(false);
  const [isSendingTestEmail, setIsSendingTestEmail] = useState(false);
  const [isMarkingTrafficSent, setIsMarkingTrafficSent] = useState(false);
  const [isUnlockingTraffic, setIsUnlockingTraffic] = useState(false);
  const [isTrafficConfirmDialogOpen, setIsTrafficConfirmDialogOpen] = useState(false);
  const [trafficConfirmTargetTrafficId, setTrafficConfirmTargetTrafficId] = useState<string | null>(null);
  const [isSendEmailConfirmationOpen, setIsSendEmailConfirmationOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [refreshMessage, setRefreshMessage] = useState<string | null>(null);
  const [cacheStatus, setCacheStatus] = useState<CacheStatus | null>(null);
  const [pendingAction, setPendingAction] = useState<PendingAction>(null);
  const [isUnsavedDialogOpen, setIsUnsavedDialogOpen] = useState(false);
  const [isArchiveDialogOpen, setIsArchiveDialogOpen] = useState(false);
  const [archiveTargetTrafficId, setArchiveTargetTrafficId] = useState<string | null>(null);
  const [archiveDialogMode, setArchiveDialogMode] = useState<TrafficRemovalMode>("archive");
  const [deletingTrafficIds, setDeletingTrafficIds] = useState<string[]>([]);
  const [activeWorkspaceTab, setActiveWorkspaceTab] = useState<TrafficWorkspaceTab>("workflow");
  const [isEmailPreviewOpen, setIsEmailPreviewOpen] = useState(false);
  const [isTestEmailModalOpen, setIsTestEmailModalOpen] = useState(false);
  const [testEmailTo, setTestEmailTo] = useState<string[]>([DEFAULT_TEST_EMAIL_RECIPIENT]);
  const [emailWorkspaceByTrafficId, setEmailWorkspaceByTrafficId] = useState<Record<string, TrafficEmailWorkspaceDraft>>({});
  const [isDownloadLinkNoteModalOpen, setIsDownloadLinkNoteModalOpen] = useState(false);
  const [activeDownloadLinkNoteIndex, setActiveDownloadLinkNoteIndex] = useState<number | null>(null);
  const [downloadLinkNoteDraft, setDownloadLinkNoteDraft] = useState("");
  const [isDownloadLinkNoteDiscardDialogOpen, setIsDownloadLinkNoteDiscardDialogOpen] = useState(false);
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
  const [isFlightStationSyncSelectOpen, setIsFlightStationSyncSelectOpen] = useState(false);
  const [pendingFlightStationSync, setPendingFlightStationSync] = useState<FlightStationSyncParams | null>(null);
  const [flightStationSyncCandidates, setFlightStationSyncCandidates] = useState<TrafficStationCandidatesPayload | null>(null);
  const [selectedFlightStationSyncEstNums, setSelectedFlightStationSyncEstNums] = useState<number[]>([]);
  const [flightStationSyncSelectionsByTrafficId, setFlightStationSyncSelectionsByTrafficId] = useState<Record<string, number[]>>({});
  const [isFlightStationSyncCandidatesLoading, setIsFlightStationSyncCandidatesLoading] = useState(false);
  const [flightStationSyncCacheStatus, setFlightStationSyncCacheStatus] = useState<CacheStatus | null>(null);
  const [flightStationSyncDialogSource, setFlightStationSyncDialogSource] = useState<FlightStationSyncDialogSource>("flight_update");
  const [isSyncMissingStationsDialogOpen, setIsSyncMissingStationsDialogOpen] = useState(false);
  const [pendingSyncMissingStationsCodes, setPendingSyncMissingStationsCodes] = useState<string[]>([]);
  const [selectedSyncMissingStationsCodes, setSelectedSyncMissingStationsCodes] = useState<string[]>([]);
  const [flightSyncPreviewEstnum, setFlightSyncPreviewEstnum] = useState<EsnumItem | null>(null);
  const [isFlightSyncPreviewModalOpen, setIsFlightSyncPreviewModalOpen] = useState(false);
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
  const [stationMetaRefreshToken, setStationMetaRefreshToken] = useState(0);
  const [duplicatingTrafficId, setDuplicatingTrafficId] = useState<string | null>(null);
  const [isRefreshEmailMergeDialogOpen, setIsRefreshEmailMergeDialogOpen] = useState(false);
  const [pendingRefreshEmailMerge, setPendingRefreshEmailMerge] = useState<RefreshEmailMergePrompt | null>(null);
  const [isStationDiscardDialogOpen, setIsStationDiscardDialogOpen] = useState(false);
  const [stationLookupMetaByCode, setStationLookupMetaByCode] = useState<Record<string, StationLookupMeta>>({});
  const [isDeliveryMethodDialogOpen, setIsDeliveryMethodDialogOpen] = useState(false);
  const [selectedDeliveryStationCode, setSelectedDeliveryStationCode] = useState<string | null>(null);
  const [loadedAccountCode, setLoadedAccountCode] = useState("");

  const listRequestIdRef = useRef(0);
  const detailRequestIdRef = useRef(0);
  const tempRowIdRef = useRef(-1);
  const stationLookupRequestIdRef = useRef(0);
  const stationMetaRequestIdRef = useRef(0);
  const syncMissingStationsResolverRef = useRef<((stationCodesToRemove: string[]) => void) | null>(null);
  const stationLookupMetaByCodeRef = useRef<Record<string, StationLookupMeta>>({});
  const trafficListRef = useRef<TrafficSummary[]>(trafficList);
  const selectedTrafficIdRef = useRef<string | null>(selectedTrafficId);
  const detailBaselineRef = useRef<TrafficDetail | null>(detailBaseline);
  const detailDraftRef = useRef<TrafficDetail | null>(detailDraft);
  const refreshMessageRef = useRef<string | null>(refreshMessage);
  const activeAccountCodeRef = useRef<string>("");
  const flightFileUrlInputRef = useRef<HTMLInputElement | null>(null);
  const flightScriptUrlInputRef = useRef<HTMLInputElement | null>(null);
  const flightNoteTextareaRef = useRef<HTMLTextAreaElement | null>(null);
  const downloadLinkNoteTextareaRef = useRef<HTMLTextAreaElement | null>(null);
  const stationDeliveryStatusFieldRef = useRef<HTMLDivElement | null>(null);
  const stationConfirmedStatusFieldRef = useRef<HTMLDivElement | null>(null);
  const stationNoteTextareaRef = useRef<HTMLTextAreaElement | null>(null);
  const stationDeliveryStatusRestoreRef = useRef<Record<number, string>>({});
  const stationConfirmedStatusRestoreRef = useRef<Record<number, string>>({});

  const flightScriptUrlField = useCommittedTextField<HTMLInputElement>(
    flightModalDraft?.scriptUrl ?? "",
    (value) => {
      setFlightModalError(null);
      setFlightScriptUrlError(null);
      setFlightModalDraft((current) => (current ? { ...current, scriptUrl: value } : current));
    },
    {
      onCommit: (normalizedValue, rawValue) => {
        if (!asString(rawValue)) {
          setFlightScriptUrlError(null);
          setFlightModalDraft((current) => (current ? { ...current, scriptUrl: null } : current));
          return;
        }
        const normalizedUrl = normalizeStrictHttpUrlInput(normalizedValue);
        if (normalizedUrl) {
          setFlightScriptUrlError(null);
          setFlightModalDraft((current) => (current ? { ...current, scriptUrl: normalizedUrl } : current));
          return;
        }
        setFlightScriptUrlError("Invalid URL. Enter a valid http(s) URL.");
        setFlightModalDraft((current) => (current ? { ...current, scriptUrl: null } : current));
      },
    },
  );
  const flightNoteField = useCommittedTextField<HTMLTextAreaElement>(
    flightModalDraft?.note ?? "",
    (value) => {
      setFlightModalError(null);
      setFlightModalDraft((current) => (current ? { ...current, note: value } : current));
    },
  );
  const stationNoteField = useCommittedTextField<HTMLTextAreaElement>(
    stationModalDraft?.note ?? "",
    (value) => {
      setStationModalError(null);
      setStationModalDraft((current) => (current ? { ...current, note: value } : current));
    },
  );

  const hasUnsavedChanges = useMemo(() => hasTrafficDetailChanges(detailBaseline, detailDraft), [detailBaseline, detailDraft]);
  const hasQueuedUnsavedChanges = useMemo(() => {
    return Object.values(draftSessionsByTrafficId).some((session) => hasTrafficDetailChanges(session.baseline, session.draft));
  }, [draftSessionsByTrafficId]);
  const hasAnyUnsavedChanges = hasUnsavedChanges || hasQueuedUnsavedChanges;
  const unsavedTrafficIds = useMemo(() => {
    const ids = new Set<string>();
    for (const [trafficId, session] of Object.entries(draftSessionsByTrafficId)) {
      if (hasTrafficDetailChanges(session.baseline, session.draft)) {
        ids.add(trafficId);
      }
    }
    if (hasUnsavedChanges) {
      const activeTrafficId = asString(detailDraft?.traffic.id);
      if (activeTrafficId) {
        ids.add(activeTrafficId);
      }
    }
    return ids;
  }, [detailDraft?.traffic.id, draftSessionsByTrafficId, hasUnsavedChanges]);
  const unsavedTrafficCount = unsavedTrafficIds.size;

  const {
    hasDeferredUpdate,
    applyOrDefer,
    clearDeferredUpdate,
  } = useDirtyRefreshGuard(hasUnsavedChanges);

  const activeAccountCode = asString(loadedAccountCode).toUpperCase();
  const selectedAccountCodeNormalized = asString(selectedAccountCode).toUpperCase();
  const selectedAccountIsLoaded = Boolean(activeAccountCode && selectedAccountCodeNormalized === activeAccountCode);
  const hasPendingAccountSelectionAfterLoad = Boolean(
    activeAccountCode
    && selectedAccountCodeNormalized
    && selectedAccountCodeNormalized !== activeAccountCode,
  );
  useEffect(() => {
    trafficListRef.current = trafficList;
    selectedTrafficIdRef.current = selectedTrafficId;
    detailBaselineRef.current = detailBaseline;
    detailDraftRef.current = detailDraft;
    refreshMessageRef.current = refreshMessage;
    activeAccountCodeRef.current = activeAccountCode;
  }, [activeAccountCode, detailBaseline, detailDraft, refreshMessage, selectedTrafficId, trafficList]);

  useEffect(() => {
    stationDeliveryStatusRestoreRef.current = {};
    stationConfirmedStatusRestoreRef.current = {};
  }, [selectedTrafficId]);

  const activeDraft = detailDraft;
  const flightModalIsciPlaceholder = useMemo(() => {
    const accountCode = asString(activeDraft?.traffic.accountCode || loadedAccountCode).toUpperCase();
    const flightStart = asString(flightModalDraft?.flightStart || getTodayIsoDate());
    return buildIsciPlaceholder(accountCode, flightStart);
  }, [activeDraft?.traffic.accountCode, flightModalDraft?.flightStart, loadedAccountCode]);
  const applyFlightModalIsciPlaceholder = useCallback((): boolean => {
    const normalizedPlaceholder = asString(flightModalIsciPlaceholder).replace(/\s+/g, "").toUpperCase();
    if (!normalizedPlaceholder) {
      return false;
    }
    let applied = false;
    setFlightModalDraft((current) => {
      if (!current || asString(current.isci)) {
        return current;
      }
      applied = true;
      return {
        ...current,
        isci: normalizedPlaceholder,
      };
    });
    if (applied) {
      setFlightModalError(null);
    }
    return applied;
  }, [flightModalIsciPlaceholder]);
  const focusStationDeliveryStatusField = useCallback(() => {
    stationDeliveryStatusFieldRef.current?.querySelector<HTMLButtonElement>("button[role='combobox']")?.focus();
  }, []);
  const focusStationConfirmedStatusField = useCallback(() => {
    stationConfirmedStatusFieldRef.current?.querySelector<HTMLButtonElement>("button[role='combobox']")?.focus();
  }, []);
  const handleStationStatusFieldAdvance = useCallback((event: ReactKeyboardEvent<HTMLElement>, target: "confirmed" | "note") => {
    if (event.key !== "Enter" && event.key !== "Tab") {
      return;
    }
    if (event.key === "Tab" && event.shiftKey) {
      return;
    }
    const source = event.target;
    if (!(source instanceof HTMLElement) || source.getAttribute("role") !== "combobox") {
      return;
    }
    if (source.getAttribute("aria-expanded") === "true") {
      return;
    }
    event.preventDefault();
    if (target === "confirmed") {
      focusStationConfirmedStatusField();
      return;
    }
    stationNoteTextareaRef.current?.focus();
  }, [focusStationConfirmedStatusField]);
  const deletingTrafficIdSet = useMemo(() => new Set(deletingTrafficIds), [deletingTrafficIds]);
  const isDeletingTraffic = deletingTrafficIds.length > 0;
  const isDeletingSelectedTraffic = Boolean(
    selectedTrafficId
    && deletingTrafficIdSet.has(selectedTrafficId)
  );
  const isArchiveTargetDeleting = Boolean(
    archiveTargetTrafficId
    && deletingTrafficIdSet.has(archiveTargetTrafficId),
  );
  const timelineAccountCode = asString(activeDraft?.traffic.accountCode).toUpperCase();
  const persistedTrafficStatus = asString(detailBaseline?.traffic.status).toLowerCase();
  const currentTrafficStatus = asString(detailDraft?.traffic.status || detailBaseline?.traffic.status).toLowerCase();
  const isSentLocked = currentTrafficStatus === "sent";
  const isConfirmedLocked = currentTrafficStatus === "confirmed";
  const isConfirmedSavedLocked = persistedTrafficStatus === "confirmed";
  const isEmailSentLocked = asString(activeDraft?.email?.sentStatus || detailBaseline?.email?.sentStatus).toLowerCase() === "sent";
  const isEmailLocked = isEmailSentLocked || isSentLocked || isConfirmedLocked;
  const isTrafficEditingLocked = isSentLocked || isConfirmedLocked;
  const isFlightModalReadOnly = isTrafficEditingLocked;
  const emailWorkspacePrimaryActionLabel = isEmailLocked ? "Preview email" : "Send email";
  const trafficLockBannerKind = isConfirmedLocked
    ? "confirmed"
    : isSentLocked
      ? "sent"
      : null;
  const emailLockBannerKind = isConfirmedLocked
    ? "confirmed"
    : isSentLocked
      ? "sent"
      : isEmailSentLocked
        ? "email"
        : null;
  const stationSyncFlightRange = useMemo(() => {
    return resolveFlightRangeFromFlights(activeDraft?.flights ?? []);
  }, [activeDraft?.flights]);
  const timelineAnchorStart = stationSyncFlightRange?.flightStart ?? null;
  const timelineAnchorEnd = stationSyncFlightRange?.flightEnd ?? null;
  const shouldShowSaveActions = canEditTradsphere && (hasAnyUnsavedChanges || isSaving);
  const hasFlightModalChanges = useMemo(() => {
    return !areNormalizedFlightsEqual(
      normalizeFlightForCompare(flightModalBaseline),
      normalizeFlightForCompare(flightModalDraft),
    );
  }, [flightModalBaseline, flightModalDraft]);
  const hasStationModalChanges = useMemo(() => {
    const baseline = stationModalBaseline
      ? {
          stationCode: asString(stationModalBaseline.stationCode).toUpperCase(),
          deliveryStatus: asString(stationModalBaseline.deliveryStatus).toLowerCase(),
          confirmedStatus: asString(stationModalBaseline.confirmedStatus).toLowerCase(),
          note: typeof stationModalBaseline.note === "string" ? stationModalBaseline.note : asString(stationModalBaseline.note || ""),
        }
      : null;
    const draft = stationModalDraft
      ? {
          stationCode: asString(stationModalDraft.stationCode).toUpperCase(),
          deliveryStatus: asString(stationModalDraft.deliveryStatus).toLowerCase(),
          confirmedStatus: asString(stationModalDraft.confirmedStatus).toLowerCase(),
          note: typeof stationModalDraft.note === "string" ? stationModalDraft.note : asString(stationModalDraft.note || ""),
        }
      : null;
    return !areNormalizedStationsEqual(baseline, draft);
  }, [stationModalBaseline, stationModalDraft]);
  const canSaveFlightModal = Boolean(
    canEditTradsphere
      && !isSaving
      && !isTrafficEditingLocked
      && flightModalDraft
      && hasFlightModalChanges,
  );
  const flightModalValidationError = useMemo(() => validateFlightModalDraft(flightModalDraft), [flightModalDraft]);
  const canSubmitFlightModal = Boolean(canSaveFlightModal && !flightModalValidationError);
  const canSaveStationModal = Boolean(
    canEditTradsphere
      && !isSaving
      && !isConfirmedLocked
      && stationModalDraft
      && hasStationModalChanges,
  );
  const stationModalValidationError = useMemo(() => validateStationModalDraft(stationModalDraft), [stationModalDraft]);
  const canSubmitStationModal = Boolean(canSaveStationModal && !stationModalValidationError);
  const stationModalFooterActions = (
    <>
      {canEditTradsphere && hasStationModalChanges ? (
        <Button
          variant="outline"
          onClick={revertStationModalChanges}
          disabled={isSaving}
        >
          Revert
        </Button>
      ) : null}
      {canSubmitStationModal ? (
        <Button onClick={saveStationModal}>
          {stationModalMode === "create" ? "Add" : "Save"}
        </Button>
      ) : null}
    </>
  );

  const canSaveChanges = Boolean(
    canEditTradsphere
      && hasAnyUnsavedChanges
      && !isSaving
      && !isSendingEmail
      && !isMarkingTrafficSent
      && !isDeletingTraffic
    && !isLoadingAccountTraffic
    && !isDetailBusy,
  );
  const activeTrafficId = asString(activeDraft?.traffic.id);
  const activeEmailWorkspace = useMemo(() => {
    if (!activeDraft || !activeTrafficId) {
      return null;
    }
    const existing = emailWorkspaceByTrafficId[activeTrafficId];
    if (existing) {
      return existing;
    }
    return buildTrafficEmailWorkspaceFromDetail(activeDraft);
  }, [activeDraft, activeTrafficId, emailWorkspaceByTrafficId]);
  const activeEmailPreviewHtml = useMemo(() => {
    if (!activeDraft?.email || !activeEmailWorkspace) {
      return "";
    }
    const accountCode = asString(activeDraft.traffic.accountCode).toUpperCase();
    const accountLabel = accountNameByCode[accountCode] || accountCode || "Tradsphere";
    return buildTrafficEmailHtmlDocument({
      subject: asString(activeDraft.email.subject),
      accountLabel,
      campaignLabel: asString(activeDraft.traffic.campaign),
      statusLabel: formatStatusOptionLabel(activeDraft.traffic.status),
      bodyContent: activeEmailWorkspace.bodyContent,
      instructionsContent: activeEmailWorkspace.instructionsContent,
      downloadLinks: activeEmailWorkspace.downloadLinks,
    });
  }, [accountNameByCode, activeDraft, activeEmailWorkspace]);
  const activeDownloadLinkNoteEntry = useMemo(() => {
    if (!activeEmailWorkspace || activeDownloadLinkNoteIndex === null) {
      return null;
    }
    return activeEmailWorkspace.downloadLinks[activeDownloadLinkNoteIndex] ?? null;
  }, [activeDownloadLinkNoteIndex, activeEmailWorkspace]);
  const hasDownloadLinkNoteChanges = useMemo(() => {
    if (!activeDownloadLinkNoteEntry) {
      return false;
    }
    return asString(downloadLinkNoteDraft).trim() !== asString(activeDownloadLinkNoteEntry.note || "").trim();
  }, [activeDownloadLinkNoteEntry, downloadLinkNoteDraft]);
  const canSaveDownloadLinkNote = Boolean(
    canEditTradsphere
      && !isSaving
      && !isEmailLocked
      && activeDownloadLinkNoteEntry
      && hasDownloadLinkNoteChanges,
  );
  const canSendTrafficEmail = useMemo(() => {
    if (!canEditTradsphere || isSaving || isSendingEmail || isMarkingTrafficSent || isEmailLocked) {
      return false;
    }
    if (!activeDraft?.email || !activeEmailWorkspace || !activeTrafficId) {
      return false;
    }
    return true;
  }, [
    activeDraft,
    activeEmailWorkspace,
    activeTrafficId,
    canEditTradsphere,
    isSaving,
    isSendingEmail,
    isMarkingTrafficSent,
    isEmailLocked,
  ]);
  const canOpenTrafficTestEmailModal = useMemo(() => {
    if (!canEditTradsphere || isSaving || isSendingEmail || isSendingTestEmail) {
      return false;
    }
    if (!activeDraft?.email || !activeEmailWorkspace || !activeTrafficId) {
      return false;
    }
    return true;
  }, [
    activeDraft,
    activeEmailWorkspace,
    activeTrafficId,
    canEditTradsphere,
    isSaving,
    isSendingEmail,
    isSendingTestEmail,
  ]);
  const canSendTrafficTestEmail = useMemo(() => {
    if (!canEditTradsphere || isSaving || isSendingTestEmail) {
      return false;
    }
    if (!activeDraft?.email || !activeEmailWorkspace || !activeTrafficId) {
      return false;
    }
    return testEmailTo.length === 1 && isLikelyEmailAddress(testEmailTo[0] ?? "");
  }, [
    activeDraft,
    activeEmailWorkspace,
    activeTrafficId,
    canEditTradsphere,
    isSaving,
    isSendingTestEmail,
    testEmailTo,
  ]);
  const activeTrafficNeedsSaveBeforeSending = useMemo(() => {
    if (!detailDraft || !detailBaseline) {
      return false;
    }
    const trafficId = asString(detailDraft.traffic.id);
    if (!trafficId) {
      return false;
    }
    return isLocalTrafficId(trafficId) || hasTrafficDetailChanges(detailBaseline, detailDraft);
  }, [detailBaseline, detailDraft]);
  const sendTrafficEmailDisabledReason = useMemo(() => {
    if (!activeDraft?.email || !activeEmailWorkspace || !activeTrafficId) {
      return "Open a traffic record with an email draft first.";
    }
    if (isSendingTestEmail) {
      return "Wait for the test email to finish sending before sending.";
    }
    if (isSaving) {
      return "Save traffic changes first.";
    }
    if (isSendingEmail) {
      return "Email is currently sending.";
    }
    if (isMarkingTrafficSent) {
      return "Traffic is being marked sent.";
    }
    return "Send email is unavailable right now.";
  }, [
    activeDraft?.email,
    activeEmailWorkspace,
    activeTrafficId,
    isMarkingTrafficSent,
    isSaving,
    isSendingEmail,
    isSendingTestEmail,
  ]);
  const sendTrafficTestEmailDisabledReason = useMemo(() => {
    if (!activeDraft?.email || !activeEmailWorkspace || !activeTrafficId) {
      return "Open a traffic record with an email draft first.";
    }
    if (isSendingEmail) {
      return "Wait for the main email send to finish before sending a test email.";
    }
    if (isSaving) {
      return "Save traffic changes first.";
    }
    if (isSendingTestEmail) {
      return "Test email is currently sending.";
    }
    return "Send test email is unavailable right now.";
  }, [
    activeDraft?.email,
    activeEmailWorkspace,
    activeTrafficId,
    isSaving,
    isSendingEmail,
    isSendingTestEmail,
  ]);

  const cacheStatusText = useMemo(() => {
    if (isDeletingTraffic) {
      return "Removing traffic...";
    }
    if (isRefreshingAccountTraffic || isDetailBusy) {
      return "Refreshing...";
    }
    if (!isOnline && cacheStatus) {
      return `Offline. Showing cached data from ${formatRelativeTime(cacheStatus.fetchedAt)}.`;
    }
    if (cacheStatus) {
      return `Data source: ${cacheStatus.source}. Last updated ${formatRelativeTime(cacheStatus.fetchedAt)}.`;
    }
    return "No cached data yet";
  }, [cacheStatus, isDeletingTraffic, isDetailBusy, isOnline, isRefreshingAccountTraffic]);

  const visibleRefreshMessage = useMemo(() => {
    if (refreshMessage === DEFERRED_REFRESH_MESSAGE && !hasUnsavedChanges) {
      return null;
    }
    return refreshMessage;
  }, [hasUnsavedChanges, refreshMessage]);
  const pageMessages: StackMessage[] = [];
  if (visibleRefreshMessage) {
    pageMessages.push({
      id: "traffic-refresh-message",
      variant: visibleRefreshMessage.toLowerCase().includes("offline") ? "info" : "warning",
      message: visibleRefreshMessage,
    });
  }
  if (error) {
    pageMessages.push({
      id: "traffic-load-error",
      variant: "error",
      message: error,
    });
  }

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

  const stashCurrentDraftSession = useCallback(() => {
    const activeTrafficId = asString(detailDraft?.traffic.id);
    if (!activeTrafficId || !detailDraft) {
      return;
    }
    const hasChanges = hasTrafficDetailChanges(detailBaseline, detailDraft);
    setDraftSessionsByTrafficId((current) => {
      if (!hasChanges) {
        if (!(activeTrafficId in current)) {
          return current;
        }
        const next = { ...current };
        delete next[activeTrafficId];
        return next;
      }
      return {
        ...current,
        [activeTrafficId]: {
          baseline: cloneDetail(detailBaseline),
          draft: cloneDetail(detailDraft) as TrafficDetail,
        },
      };
    });
  }, [detailBaseline, detailDraft]);

  const restoreDraftSession = useCallback((trafficIdRaw: string): boolean => {
    const trafficId = asString(trafficIdRaw);
    if (!trafficId) {
      return false;
    }
    const session = draftSessionsByTrafficId[trafficId];
    if (!session) {
      return false;
    }
    setDetailBaseline(cloneDetail(session.baseline));
    setDetailDraft(cloneDetail(session.draft));
    setDraftSessionsByTrafficId((current) => {
      if (!(trafficId in current)) {
        return current;
      }
      const next = { ...current };
      delete next[trafficId];
      return next;
    });
    return true;
  }, [draftSessionsByTrafficId]);

  const applyDetailState = useCallback((detail: TrafficDetail | null) => {
    const cloned = applyDefaultTrafficEmailSubject(cloneDetail(detail), accountNameByCodeRef.current);
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
      setDraftSessionsByTrafficId((current) => {
        if (!(cloned.traffic.id in current)) {
          return current;
        }
        const next = { ...current };
        delete next[cloned.traffic.id];
        return next;
      });
    }
  }, [upsertSelectedByAccount]);

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
      setIsLoadingDetail(false);
      setIsRefreshingDetail(false);
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
      setIsRefreshingDetail(shouldFetchFromNetwork);
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
      setIsRefreshingDetail(false);
    }

    if (!shouldFetchFromNetwork) {
      setIsLoadingDetail(false);
      setIsRefreshingDetail(false);
      return;
    }

    if (!isOnline) {
      if (!shouldUseCache) {
        setError("You're offline. Traffic detail is unavailable until connection is restored.");
      } else {
        setRefreshMessage("You're offline. Showing cached traffic.");
      }
      setIsLoadingDetail(false);
      setIsRefreshingDetail(false);
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
        setIsRefreshingDetail(false);
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
        setIsRefreshingDetail(false);
      }
    }
  }, [applyDetailState, applyOrDefer, isOnline, requestHeaders, requestJson]);

  const requestStationCandidatesWithCache = useCallback(async (params: {
    accountCode: string;
    flightStart: string;
    flightEnd: string;
    estNums?: number[];
    languages?: FlightLanguage[];
    mediums?: string[];
    preferCache?: boolean;
  }): Promise<TrafficStationCandidatesFetchResult | null> => {
    const accountCode = asString(params.accountCode).toUpperCase();
    const flightStart = asString(params.flightStart);
    const flightEnd = asString(params.flightEnd);
    const estNums = normalizeEstNumList(params.estNums);
    const languages = normalizeFlightLanguageList(params.languages);
    const mediums = normalizeFlightMediumList(params.mediums);
    const mediumFilters = expandFlightMediumsForCandidateFilter(mediums);
    if (!accountCode || !flightStart || !flightEnd) {
      return null;
    }

    const cacheKey = buildTrafficStationCandidatesCacheKey({
      accountCode,
      flightStart,
      flightEnd,
      estNums,
      languages,
      mediums: mediumFilters,
    });
    const cacheSnapshot = readBrowserCacheSnapshot<TrafficStationCandidatesPayload>(cacheKey);
    const cached = cacheSnapshot
      ? normalizeTrafficStationCandidates({ data: cacheSnapshot.data })
      : null;
    if (params.preferCache && cached) {
      return {
        payload: cached,
        cacheStatus: {
          source: "cache",
          fetchedAt: cacheSnapshot?.fetchedAt ?? Date.now(),
        },
      };
    }

    const query = new URLSearchParams({
      accountCode,
      flightStart,
      flightEnd,
    });
    if (estNums.length > 0) {
      query.set("estNums", estNums.join(","));
    }
    if (languages.length > 0) {
      query.set("languages", languages.join(","));
    }
    if (mediumFilters.length > 0) {
      query.set("mediaTypes", mediumFilters.join(","));
    }

    try {
      const payload = await requestJson(`/api/tradsphere/v1/traffic/station-candidates?${query.toString()}`, {
        headers: requestHeaders,
        successToast: false,
        errorToast: false,
      });
      const normalized = normalizeTrafficStationCandidates(payload);
      if (normalized) {
        const fetchedAt = Date.now();
        writeBrowserCache(cacheKey, normalized, TRAFFIC_STATION_CANDIDATES_CACHE_TTL_MS, {
          source: "network",
          fetchedAt,
        });
        return {
          payload: normalized,
          cacheStatus: {
            source: "network",
            fetchedAt,
          },
        };
      }
      return cached
        ? {
            payload: cached,
            cacheStatus: {
              source: "cache",
              fetchedAt: cacheSnapshot?.fetchedAt ?? Date.now(),
            },
          }
        : null;
    } catch (error) {
      if (cached) {
        return {
          payload: cached,
          cacheStatus: {
            source: "cache",
            fetchedAt: cacheSnapshot?.fetchedAt ?? Date.now(),
          },
        };
      }
      throw error;
    }
  }, [requestHeaders, requestJson]);

  const loadFlightStationSyncCandidates = useCallback(async (
    params: {
      trafficId: string;
      accountCode: string;
      flightStart: string;
      flightEnd: string;
      estNums?: number[];
      languages?: FlightLanguage[];
      mediums?: string[];
      forceRefreshCandidates?: boolean;
    },
    options: {
      preferCache: boolean;
      preserveSelection?: number[];
    },
  ): Promise<TrafficStationCandidatesPayload | null> => {
    setIsFlightStationSyncCandidatesLoading(true);
    try {
      const result = await requestStationCandidatesWithCache({
        accountCode: params.accountCode,
        flightStart: params.flightStart,
        flightEnd: params.flightEnd,
        estNums: params.estNums,
        languages: params.languages,
        mediums: params.mediums,
        preferCache: options.preferCache,
      });
      if (!result) {
        return null;
      }
      setFlightStationSyncCandidates(result.payload);
      setFlightStationSyncCacheStatus(result.cacheStatus);
      const availableEstNums = normalizeEstNumList(result.payload.estNums.map((item) => item.estNum));
      const preserved = normalizeEstNumList(options.preserveSelection ?? []);
      const nextSelection = preserved.filter((value) => availableEstNums.includes(value));
      setSelectedFlightStationSyncEstNums(nextSelection.length > 0 ? nextSelection : availableEstNums);
      return result.payload;
    } finally {
      setIsFlightStationSyncCandidatesLoading(false);
    }
  }, [requestStationCandidatesWithCache]);

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
      setActiveWorkspaceTab(normalized.activeWorkspaceTab);
      setFlightStationSyncSelectionsByTrafficId(normalized.flightStationSyncSelectionsByTrafficId);
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
      activeWorkspaceTab,
      flightStationSyncSelectionsByTrafficId,
    };
    writeScopedPageState(pageStateScope, snapshot);
  }, [
    activeWorkspaceTab,
    cacheStatus,
    detailBaseline,
    detailDraft,
    hasHydratedPageState,
    loadedAccountCode,
    pageStateScope,
    refreshMessage,
    selectedTrafficId,
    flightStationSyncSelectionsByTrafficId,
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
      if (!hasAnyUnsavedChanges) {
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
  }, [hasAnyUnsavedChanges]);

  useEffect(() => {
    if (!hasAnyUnsavedChanges) {
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
  }, [hasAnyUnsavedChanges]);

  const updateDraft = useCallback((updater: (current: TrafficDetail) => TrafficDetail, options?: UpdateDraftOptions) => {
    setDetailDraft((current) => {
      if (!current) {
        return current;
      }
      const previousDetail = cloneDetail(current) as TrafficDetail;
      const nextDetail = updater(cloneDetail(current) as TrafficDetail);
      const withAutoSyncedSubject = syncAutoTrafficEmailSubject(previousDetail, nextDetail, accountNameByCode);
      const withAutoReadyToEmail = options?.applyAutoReadyToEmail === true
        ? applyAutoReadyToEmailFromFlights(withAutoSyncedSubject)
        : withAutoSyncedSubject;
      const withDefaultSubject = applyDefaultTrafficEmailSubject(withAutoReadyToEmail, accountNameByCode);
      const withStationSyncedToEmails = syncTrafficEmailRecipientsFromStations(previousDetail, withDefaultSubject as TrafficDetail);
      return computeSummary(withStationSyncedToEmails as TrafficDetail);
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
          searchCampaign: patch.campaign !== undefined ? asString(patch.campaign).toLowerCase() : item.searchCampaign,
          status: patch.status ?? item.status,
        }
      : item));
  }, []);

  const applyTrafficStatusToLoadedState = useCallback((trafficIdRaw: string, nextStatus: TrafficStatus) => {
    const trafficId = asString(trafficIdRaw);
    if (!trafficId) {
      return;
    }
    const currentDetail = detailDraftRef.current ?? detailBaselineRef.current;
    const nextDetail = currentDetail && asString(currentDetail.traffic.id) === trafficId
      ? computeSummary({
          ...currentDetail,
          traffic: {
            ...currentDetail.traffic,
            status: nextStatus,
          },
        })
      : null;

    const applyStatus = (current: TrafficDetail | null): TrafficDetail | null => {
      if (!current || asString(current.traffic.id) !== trafficId) {
        return current;
      }
      return computeSummary({
        ...current,
        traffic: {
          ...current.traffic,
          status: nextStatus,
        },
      });
    };

    setDetailBaseline((current) => applyStatus(current));
    setDetailDraft((current) => applyStatus(current));
    setDraftSessionsByTrafficId((current) => {
      const session = current[trafficId];
      if (!session) {
        return current;
      }
      return {
        ...current,
        [trafficId]: {
          baseline: session.baseline
            ? computeSummary({
                ...session.baseline,
                traffic: {
                  ...session.baseline.traffic,
                  status: nextStatus,
                },
              })
            : session.baseline,
          draft: computeSummary({
            ...session.draft,
            traffic: {
              ...session.draft.traffic,
              status: nextStatus,
            },
          }),
        },
      };
    });
    if (nextDetail) {
      writeBrowserCache(buildTrafficDetailCacheKey(trafficId), nextDetail, TRAFFIC_DETAIL_CACHE_TTL_MS, {
        source: "network",
        fetchedAt: Date.now(),
      });
      setTrafficList((current) => {
        let changed = false;
        const nextSummary = buildListSummaryFromDetail(nextDetail, stationLookupMetaByCode);
        const nextList = current.map((item) => {
          if (item.id !== trafficId) {
            return item;
          }
          changed = true;
          return nextSummary;
        });
        return changed ? nextList : current;
      });
    }
    updateTrafficCardSummary(trafficId, { status: nextStatus });
  }, [computeSummary, stationLookupMetaByCode, updateTrafficCardSummary]);

  const handleToggleStationConfirmedStatus = useCallback((stationId: number) => {
    if (!canEditTradsphere || isSaving || isConfirmedLocked) {
      return;
    }
    let nextStations: TrafficStation[] | null = null;
    updateDraft((current) => {
      const target = current.stations.find((item) => item.id === stationId);
      if (!target) {
        return current;
      }
      const currentConfirmedStatus = asString(target.confirmedStatus).toLowerCase();
      const baselineStation = detailBaselineRef.current?.stations.find((item) => item.id === stationId) ?? null;
      const baselineConfirmedStatus = asString(baselineStation?.confirmedStatus).toLowerCase();
      const nextStationsDraft = current.stations.map((item) => {
        if (item.id !== stationId) {
          return item;
        }
        if (currentConfirmedStatus === "confirmed") {
          const restoreStatus = stationConfirmedStatusRestoreRef.current[stationId]
            ?? (baselineStation ? baselineStation.confirmedStatus : target.confirmedStatus);
          delete stationConfirmedStatusRestoreRef.current[stationId];
          return {
            ...item,
            confirmedStatus: restoreStatus,
          };
        }
        stationConfirmedStatusRestoreRef.current[stationId] = asString(target.confirmedStatus).toLowerCase()
          || baselineConfirmedStatus
          || "";
        return {
          ...item,
          confirmedStatus: "confirmed",
        };
      });
      nextStations = nextStationsDraft;
      return {
        ...current,
        stations: nextStationsDraft,
      };
    });
    if (nextStations) {
      maybePromptConfirmTrafficStatus(nextStations);
    }
  }, [canEditTradsphere, isConfirmedLocked, isSaving, maybePromptConfirmTrafficStatus, updateDraft]);

  const handleToggleStationDeliveryStatus = useCallback((stationId: number) => {
    if (!canEditTradsphere || isSaving || isConfirmedLocked) {
      return;
    }
    updateDraft((current) => {
      const target = current.stations.find((item) => item.id === stationId);
      if (!target) {
        return current;
      }
      const currentDeliveryStatus = asString(target.deliveryStatus).toLowerCase();
      const baselineStation = detailBaselineRef.current?.stations.find((item) => item.id === stationId) ?? null;
      const baselineDeliveryStatus = asString(baselineStation?.deliveryStatus).toLowerCase();
      const nextStations = current.stations.map((item) => {
        if (item.id !== stationId) {
          return item;
        }
        if (currentDeliveryStatus === "ready_to_email") {
          const restoreStatus = stationDeliveryStatusRestoreRef.current[stationId]
            ?? (baselineStation ? baselineStation.deliveryStatus : target.deliveryStatus)
            ?? "";
          delete stationDeliveryStatusRestoreRef.current[stationId];
          return {
            ...item,
            deliveryStatus: restoreStatus,
          };
        }
        stationDeliveryStatusRestoreRef.current[stationId] = asString(target.deliveryStatus)
          || baselineDeliveryStatus
          || "";
        return {
          ...item,
          deliveryStatus: "ready_to_email",
        };
      });
      return {
        ...current,
        stations: nextStations,
      };
    });
  }, [canEditTradsphere, isConfirmedLocked, isSaving, updateDraft]);

  useEffect(() => {
    if (!activeDraft || !activeTrafficId) {
      return;
    }
    setEmailWorkspaceByTrafficId((current) => {
      const existing = current[activeTrafficId];
      const defaults = buildTrafficEmailWorkspaceFromDetail(activeDraft);
      if (!existing) {
        return {
          ...current,
          [activeTrafficId]: defaults,
        };
      }

      let changed = false;
      let next = existing;
      if (!existing.bodyTouched && defaults.bodyContent !== existing.bodyContent) {
        next = {
          ...next,
          bodyContent: defaults.bodyContent,
        };
        changed = true;
      }
      if (!existing.instructionsTouched && defaults.instructionsContent !== existing.instructionsContent) {
        next = {
          ...next,
          instructionsContent: defaults.instructionsContent,
        };
        changed = true;
      }
      const defaultLinksFingerprint = JSON.stringify(defaults.downloadLinks);
      const existingLinksFingerprint = JSON.stringify(existing.downloadLinks);
      if (defaultLinksFingerprint !== existingLinksFingerprint) {
        next = {
          ...next,
          downloadLinks: defaults.downloadLinks,
        };
        changed = true;
      }

      if (!changed) {
        return current;
      }
      return {
        ...current,
        [activeTrafficId]: next,
      };
    });
  }, [activeDraft, activeTrafficId]);

  const commitEmailWorkspace = useCallback((nextWorkspace: TrafficEmailWorkspaceDraft) => {
    if (!activeDraft?.email || !activeTrafficId) {
      return;
    }
    const accountCode = asString(activeDraft.traffic.accountCode).toUpperCase();
    const accountLabel = accountNameByCode[accountCode] || accountCode || "Tradsphere";
    const nextBodyValue = persistTrafficEmailBody({
      workspace: {
        bodyContent: nextWorkspace.bodyContent,
        instructionsContent: nextWorkspace.instructionsContent,
        downloadLinks: nextWorkspace.downloadLinks,
      },
      subject: asString(activeDraft.email.subject),
      accountLabel,
      campaignLabel: asString(activeDraft.traffic.campaign),
      statusLabel: formatStatusOptionLabel(activeDraft.traffic.status),
    });

    setEmailWorkspaceByTrafficId((current) => ({
      ...current,
      [activeTrafficId]: nextWorkspace,
    }));
    updateDraft((current) => ({
      ...current,
      email: current.email
        ? {
            ...current.email,
            body: nextBodyValue,
          }
        : current.email,
    }));
  }, [accountNameByCode, activeDraft, activeTrafficId, updateDraft]);

  const refreshEmailInstructionsFromFlights = useCallback((nextFlights: TrafficFlight[], options?: { force?: boolean }) => {
    const activeEmailDraft = activeDraft?.email;
    if (!activeEmailDraft || !activeTrafficId) {
      return;
    }

    const nextInstructionsContent = normalizeRichTextHtml(buildDefaultTrafficInstructionsText(nextFlights));
    const accountCode = asString(activeDraft.traffic.accountCode).toUpperCase();
    const accountLabel = accountNameByCode[accountCode] || accountCode || "Tradsphere";

    setEmailWorkspaceByTrafficId((current) => {
      const existing = current[activeTrafficId];
      if (!existing || existing.instructionsContent === nextInstructionsContent) {
        return current;
      }
      if (!options?.force && existing.instructionsTouched) {
        return current;
      }
      const nextWorkspace: TrafficEmailWorkspaceDraft = {
        ...existing,
        instructionsContent: nextInstructionsContent,
      };
      const nextBodyValue = persistTrafficEmailBody({
        workspace: {
          bodyContent: nextWorkspace.bodyContent,
          instructionsContent: nextWorkspace.instructionsContent,
          downloadLinks: nextWorkspace.downloadLinks,
        },
        subject: asString(activeEmailDraft.subject),
        accountLabel,
        campaignLabel: asString(activeDraft.traffic.campaign),
        statusLabel: formatStatusOptionLabel(activeDraft.traffic.status),
      });
      updateDraft((currentDraft) => ({
        ...currentDraft,
        email: currentDraft.email
          ? {
              ...currentDraft.email,
              body: nextBodyValue,
            }
          : currentDraft.email,
      }));
      return {
        ...current,
        [activeTrafficId]: nextWorkspace,
      };
    });
  }, [accountNameByCode, activeDraft, activeTrafficId, updateDraft]);

  const handleOpenDownloadLinkNoteModal = useCallback((rowIndex: number) => {
    const row = activeEmailWorkspace?.downloadLinks[rowIndex];
    if (!row) {
      return;
    }
    setActiveDownloadLinkNoteIndex(rowIndex);
    setDownloadLinkNoteDraft(asString(row.note || ""));
    setIsDownloadLinkNoteDiscardDialogOpen(false);
    setIsDownloadLinkNoteModalOpen(true);
  }, [activeEmailWorkspace]);

  const closeDownloadLinkNoteModal = useCallback(() => {
    setIsDownloadLinkNoteModalOpen(false);
    setActiveDownloadLinkNoteIndex(null);
    setDownloadLinkNoteDraft("");
    setIsDownloadLinkNoteDiscardDialogOpen(false);
  }, []);

  const commitDownloadLinkNote = useCallback(() => {
    if (!activeEmailWorkspace || activeDownloadLinkNoteIndex === null) {
      return false;
    }
    const currentNote = asString(activeEmailWorkspace.downloadLinks[activeDownloadLinkNoteIndex]?.note || "");
    if (currentNote.trim() === asString(downloadLinkNoteDraft).trim()) {
      return false;
    }

    const nextLinks = activeEmailWorkspace.downloadLinks.map((entry, entryIndex) => entryIndex === activeDownloadLinkNoteIndex
      ? { ...entry, note: downloadLinkNoteDraft }
      : entry);
    commitEmailWorkspace({
      ...activeEmailWorkspace,
      downloadLinks: nextLinks,
    });
    return true;
  }, [activeDownloadLinkNoteIndex, activeEmailWorkspace, commitEmailWorkspace, downloadLinkNoteDraft]);

  const handleDownloadLinkNoteModalOpenChange = useCallback((open: boolean) => {
    if (open) {
      setIsDownloadLinkNoteModalOpen(true);
      return;
    }
    const allowClose = canModalClose({
      nextOpen: open,
      isBusy: isSaving || isEmailLocked,
      hasUnsavedChanges: hasDownloadLinkNoteChanges,
    });
    if (!allowClose) {
      if (hasDownloadLinkNoteChanges && !isSaving && !isEmailLocked) {
        setIsDownloadLinkNoteDiscardDialogOpen(true);
      }
      return;
    }
    closeDownloadLinkNoteModal();
  }, [closeDownloadLinkNoteModal, hasDownloadLinkNoteChanges, isEmailLocked, isSaving]);

  useEffect(() => {
    if (activeDraft) {
      return;
    }
    setIsEmailPreviewOpen(false);
    setIsDownloadLinkNoteModalOpen(false);
    setActiveDownloadLinkNoteIndex(null);
    setDownloadLinkNoteDraft("");
    setIsDownloadLinkNoteDiscardDialogOpen(false);
  }, [activeDraft]);

  useEffect(() => {
    if (!isDownloadLinkNoteModalOpen) {
      return;
    }
    if (activeDownloadLinkNoteEntry) {
      return;
    }
    setIsDownloadLinkNoteModalOpen(false);
    setActiveDownloadLinkNoteIndex(null);
    setDownloadLinkNoteDraft("");
  }, [activeDownloadLinkNoteEntry, isDownloadLinkNoteModalOpen]);

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
      const nextSummary = buildListSummaryFromDetail(activeDraft, stationLookupMetaByCode);
      const nextList = current.map((item) => {
        if (item.id !== trafficId) {
          return item;
        }
        const shouldUpdate = (
          item.campaign !== nextSummary.campaign
          || item.searchCampaign !== nextSummary.searchCampaign
          || item.status !== nextSummary.status
          || item.flightCount !== nextSummary.flightCount
          || item.stationCount !== nextSummary.stationCount
          || item.emailSentStatus !== nextSummary.emailSentStatus
          || item.emailSentAt !== nextSummary.emailSentAt
          || !areStringArraysEqual(item.searchIscis, nextSummary.searchIscis)
          || !areStringArraysEqual(item.searchStations, nextSummary.searchStations)
          || !areStringArraysEqual(item.searchEmails, nextSummary.searchEmails)
          || !areRotationSummariesEqual(item.summary, nextSummary.summary)
        );
        if (!shouldUpdate) {
          return item;
        }
        changed = true;
        return {
          ...item,
          campaign: nextSummary.campaign,
          status: nextSummary.status,
          flightCount: nextSummary.flightCount,
          stationCount: nextSummary.stationCount,
          emailSentStatus: nextSummary.emailSentStatus,
          emailSentAt: nextSummary.emailSentAt,
          searchIscis: nextSummary.searchIscis,
          searchStations: nextSummary.searchStations,
          searchEmails: nextSummary.searchEmails,
          summary: nextSummary.summary,
        };
      });
      return changed ? nextList : current;
    });
  }, [activeDraft, stationLookupMetaByCode]);

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

  const buildBulkSaveRequestForDetail = useCallback((draftDetail: TrafficDetail, baselineDetail: TrafficDetail | null) => {
    const draftTrafficId = asString(draftDetail.traffic.id);
    const isLocalDraft = isLocalTrafficId(draftDetail.traffic.id);
    const trafficChanged = Boolean(
      !isLocalDraft
      && baselineDetail
      && (
        draftDetail.traffic.campaign !== baselineDetail.traffic.campaign
        || draftDetail.traffic.status !== baselineDetail.traffic.status
        || (draftDetail.traffic.note || "") !== (baselineDetail.traffic.note || "")
      ),
    );

    const flightCreates: Array<Record<string, unknown>> = [];
    const flightUpdates: Array<Record<string, unknown>> = [];
    const flightDeletes: number[] = [];
    const stationCreates: Array<Record<string, unknown>> = [];
    const stationUpdates: Array<Record<string, unknown>> = [];
    const stationDeletes: number[] = [];

    const baselineFlightsById = new Map((baselineDetail?.flights ?? []).map((item) => [item.id, item]));
    const draftFlightsById = new Map(draftDetail.flights.map((item) => [item.id, item]));
    const baselineStationsById = new Map((baselineDetail?.stations ?? []).map((item) => [item.id, item]));
    const draftStationsById = new Map(draftDetail.stations.map((item) => [item.id, item]));

    for (const baselineFlight of (baselineDetail?.flights ?? [])) {
      if (baselineFlight.id > 0 && !draftFlightsById.has(baselineFlight.id)) {
        flightDeletes.push(baselineFlight.id);
      }
    }

    for (const draftFlight of draftDetail.flights) {
      const flightPayload = {
        flightStart: draftFlight.flightStart,
        flightEnd: draftFlight.flightEnd,
        medium: draftFlight.medium,
        language: normalizeFlightLanguageValue(draftFlight.language),
        length: draftFlight.length,
        isci: draftFlight.isci,
        rotation: draftFlight.rotation,
        fileUrl: draftFlight.fileUrl,
        scriptUrl: draftFlight.scriptUrl,
        note: draftFlight.note,
      };
      if (draftFlight.id <= 0) {
        flightCreates.push(flightPayload);
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
      flightUpdates.push({
        id: draftFlight.id,
        ...flightPayload,
      });
    }

    for (const baselineStation of (baselineDetail?.stations ?? [])) {
      if (baselineStation.id > 0 && !draftStationsById.has(baselineStation.id)) {
        stationDeletes.push(baselineStation.id);
      }
    }

    for (const draftStation of draftDetail.stations) {
      const stationPayload = {
        stationCode: draftStation.stationCode,
        contactsSnapshot: draftStation.contactsSnapshot,
        deliveryMethod: draftStation.deliveryMethod,
        deliveryStatus: draftStation.deliveryStatus,
        confirmedStatus: draftStation.confirmedStatus,
        note: draftStation.note,
      };
      if (draftStation.id <= 0) {
        stationCreates.push(stationPayload);
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
      stationUpdates.push({
        id: draftStation.id,
        ...stationPayload,
      });
    }

    const normalizeEmailPayload = (email: TrafficEmail | null) => {
      if (!email) {
        return null;
      }
      return {
        toEmails: asStringArray(email.toEmails).map((entry) => entry.toLowerCase()),
        ccEmails: asStringArray(email.ccEmails).map((entry) => entry.toLowerCase()),
        bccEmails: asStringArray(email.bccEmails).map((entry) => entry.toLowerCase()),
        subject: asString(email.subject),
        body: asString(email.body),
        sentStatus: asString(email.sentStatus).toLowerCase() || "draft",
      };
    };

    const shouldUpsertEmail = (() => {
      if (!draftDetail.email) {
        return false;
      }

      const nextEmail = normalizeEmailPayload(draftDetail.email);
      if (!nextEmail) {
        return false;
      }

      const defaultSubjectCandidates = buildTrafficEmailSubjectCandidates(
        draftDetail,
        accountNameByCode,
      );
      const hasDefaultOrEmptySubject = !nextEmail.subject
        || defaultSubjectCandidates.includes(nextEmail.subject);

      const hasMeaningfulContent = nextEmail.toEmails.length > 0
        || nextEmail.ccEmails.length > 0
        || nextEmail.bccEmails.length > 0
        || Boolean(nextEmail.body)
        || nextEmail.sentStatus !== "draft"
        || !hasDefaultOrEmptySubject;

      if (isLocalDraft && !hasMeaningfulContent) {
        return false;
      }

      const baselineEmail = normalizeEmailPayload(baselineDetail?.email ?? null);
      if (!baselineEmail) {
        return hasMeaningfulContent;
      }

      return JSON.stringify(baselineEmail) !== JSON.stringify(nextEmail);
    })();

    return {
      isLocalDraft,
      draftTrafficId,
      body: {
        trafficId: isLocalDraft ? null : draftDetail.traffic.id,
        traffic: {
          accountCode: draftDetail.traffic.accountCode,
          campaign: draftDetail.traffic.campaign,
          status: draftDetail.traffic.status,
          note: draftDetail.traffic.note,
        },
        updateTraffic: trafficChanged,
        flightCreates,
        flightUpdates,
        flightDeletes,
        stationCreates,
        stationUpdates,
        stationDeletes,
        upsertEmail: shouldUpsertEmail,
        email: shouldUpsertEmail && draftDetail.email
          ? {
              toEmails: draftDetail.email.toEmails,
              ccEmails: draftDetail.email.ccEmails,
              bccEmails: draftDetail.email.bccEmails,
              subject: draftDetail.email.subject,
              body: draftDetail.email.body,
              sentStatus: draftDetail.email.sentStatus,
            }
          : undefined,
      },
    };
  }, [accountNameByCode]);

  const saveTrafficDetailForEmail = useCallback(async (draftDetail: TrafficDetail, baselineDetail: TrafficDetail | null) => {
    const validationError = validateForSave(draftDetail);
    if (validationError) {
      setError(validationError);
      throw new Error(validationError);
    }

    setIsSaving(true);
    setError(null);

    try {
      const requestBody = buildBulkSaveRequestForDetail(draftDetail, baselineDetail);
      const bulkSavePayload = await requestJson("/api/tradsphere/v1/traffic/bulk-save", {
        method: "POST",
        headers: requestHeaders,
        body: requestBody.body,
        successToast: false,
      });

      const bulkSaveData = isRecord(unwrapData(bulkSavePayload)) ? unwrapData(bulkSavePayload) : null;
      const bulkDetail = normalizeTrafficDetail({
        data: isRecord(bulkSaveData) && isRecord(bulkSaveData.detail)
          ? bulkSaveData.detail
          : unwrapData(bulkSavePayload),
      });
      const trafficId = asString(
        (isRecord(bulkSaveData) ? bulkSaveData.trafficId : "")
        || bulkDetail?.traffic.id
        || (requestBody.isLocalDraft ? "" : requestBody.draftTrafficId),
      );
      if (!bulkDetail || !trafficId) {
        throw new Error("Failed to save traffic record.");
      }

      const computedDetail = computeSummary(bulkDetail);
      const sourceDraftId = requestBody.draftTrafficId;
      const fetchedAt = Date.now();
      writeBrowserCache(buildTrafficDetailCacheKey(trafficId), computedDetail, TRAFFIC_DETAIL_CACHE_TTL_MS, {
        source: "network",
        fetchedAt,
      });
      if (requestBody.isLocalDraft && sourceDraftId && sourceDraftId !== trafficId) {
        removeBrowserCache(buildTrafficDetailCacheKey(sourceDraftId));
      }

      const nextSummary = normalizeTrafficListItemFromBulkSave(bulkSavePayload)
        ?? buildListSummaryFromDetail(computedDetail, stationLookupMetaByCode);
      setTrafficList((current) => {
        let changed = false;
        const nextList = current.map((item) => {
          if (item.id !== sourceDraftId && item.id !== trafficId) {
            return item;
          }
          changed = true;
          return nextSummary;
        });
        if (!changed && requestBody.isLocalDraft) {
          return [nextSummary, ...current];
        }
        return changed ? nextList : current;
      });

      if (requestBody.isLocalDraft && sourceDraftId && sourceDraftId !== trafficId) {
        setEmailWorkspaceByTrafficId((current) => {
          const existing = current[sourceDraftId];
          if (!existing) {
            return current;
          }
          const next = { ...current };
          delete next[sourceDraftId];
          if (!(trafficId in next)) {
            next[trafficId] = existing;
          }
          return next;
        });
        setFlightStationSyncSelectionsByTrafficId((current) => {
          const existing = current[sourceDraftId];
          if (!existing) {
            return current;
          }
          const next = { ...current };
          delete next[sourceDraftId];
          if (!(trafficId in next)) {
            next[trafficId] = normalizeEstNumList(existing);
          }
          return next;
        });
      }

      setDraftSessionsByTrafficId((current) => {
        const next = { ...current };
        let changed = false;
        if (sourceDraftId in next) {
          delete next[sourceDraftId];
          changed = true;
        }
        if (trafficId in next) {
          delete next[trafficId];
          changed = true;
        }
        return changed ? next : current;
      });

      setDetailBaseline((current) => {
        if (!current) {
          return current;
        }
        const currentTrafficId = asString(current.traffic.id);
        if (currentTrafficId !== sourceDraftId && currentTrafficId !== trafficId) {
          return current;
        }
        return cloneDetail(computedDetail);
      });
      setDetailDraft((current) => {
        if (!current) {
          return current;
        }
        const currentTrafficId = asString(current.traffic.id);
        if (currentTrafficId !== sourceDraftId && currentTrafficId !== trafficId) {
          return current;
        }
        return cloneDetail(computedDetail);
      });
      setSelectedTrafficId((current) => {
        if (current === sourceDraftId || current === trafficId) {
          return trafficId;
        }
        return current;
      });
      upsertSelectedByAccount(activeAccountCode, trafficId);
      setCacheStatus({ source: "network", fetchedAt });

      return {
        trafficId,
        detail: computedDetail,
      };
    } catch (saveError) {
      throw saveError;
    } finally {
      setIsSaving(false);
    }
  }, [
    activeAccountCode,
    buildBulkSaveRequestForDetail,
    computeSummary,
    requestHeaders,
    requestJson,
    stationLookupMetaByCode,
    upsertSelectedByAccount,
    validateForSave,
  ]);

  const handleSaveAll = useCallback(async () => {
    if (!canSaveChanges) {
      return;
    }
    stashCurrentDraftSession();
    void primeSuccessSound();

    const targetsById = new Map<string, { baseline: TrafficDetail | null; draft: TrafficDetail }>();
    for (const [trafficId, session] of Object.entries(draftSessionsByTrafficId)) {
      if (!hasTrafficDetailChanges(session.baseline, session.draft)) {
        continue;
      }
      targetsById.set(trafficId, {
        baseline: cloneDetail(session.baseline),
        draft: cloneDetail(session.draft) as TrafficDetail,
      });
    }
    if (detailDraft && hasUnsavedChanges) {
      const activeTrafficId = asString(detailDraft.traffic.id);
      if (activeTrafficId) {
        targetsById.set(activeTrafficId, {
          baseline: cloneDetail(detailBaseline),
          draft: cloneDetail(detailDraft) as TrafficDetail,
        });
      }
    }
    const saveTargets = [...targetsById.values()];
    if (saveTargets.length === 0) {
      return;
    }
    for (const target of saveTargets) {
      const validationError = validateForSave(target.draft);
      if (validationError) {
        setError(validationError);
        return;
      }
    }

    setIsSaving(true);
    setError(null);

    try {
      const savedTrafficIdByDraftId: Record<string, string> = {};
      const savedTrafficCacheWrites: Array<{
        cacheKey: string;
        detail: TrafficDetail;
        oldCacheKey?: string;
      }> = [];
      const savedTrafficListWrites: Array<{
        trafficId: string;
        sourceDraftId: string;
        summary: TrafficSummary;
        isLocalDraft: boolean;
      }> = [];
      for (const target of saveTargets) {
        const requestBody = buildBulkSaveRequestForDetail(target.draft, target.baseline);
        const bulkSavePayload = await requestJson("/api/tradsphere/v1/traffic/bulk-save", {
          method: "POST",
          headers: requestHeaders,
          body: requestBody.body,
          successToast: false,
        });

        const bulkSaveData = isRecord(unwrapData(bulkSavePayload)) ? unwrapData(bulkSavePayload) : null;
        const bulkDetail = normalizeTrafficDetail({
          data: isRecord(bulkSaveData) && isRecord(bulkSaveData.detail)
            ? bulkSaveData.detail
            : unwrapData(bulkSavePayload),
        });
        const trafficId = asString(
          (isRecord(bulkSaveData) ? bulkSaveData.trafficId : "")
          || bulkDetail?.traffic.id
          || (requestBody.isLocalDraft ? "" : requestBody.draftTrafficId),
        );
        if (!trafficId) {
          throw new Error("Failed to save traffic record.");
        }
        savedTrafficIdByDraftId[requestBody.draftTrafficId] = trafficId;

        if (bulkDetail) {
          const computedDetail = computeSummary(bulkDetail);
          const nextSummary = normalizeTrafficListItemFromBulkSave(bulkSavePayload)
            ?? buildListSummaryFromDetail(computedDetail, stationLookupMetaByCode);
          savedTrafficCacheWrites.push({
            cacheKey: buildTrafficDetailCacheKey(trafficId),
            detail: computedDetail,
            oldCacheKey: requestBody.isLocalDraft && requestBody.draftTrafficId && requestBody.draftTrafficId !== trafficId
              ? buildTrafficDetailCacheKey(requestBody.draftTrafficId)
              : undefined,
          });
          savedTrafficListWrites.push({
            trafficId,
            sourceDraftId: requestBody.draftTrafficId,
            summary: nextSummary,
            isLocalDraft: requestBody.isLocalDraft,
          });
        }

        if (requestBody.isLocalDraft && requestBody.draftTrafficId && requestBody.draftTrafficId !== trafficId) {
          setEmailWorkspaceByTrafficId((current) => {
            const existing = current[requestBody.draftTrafficId];
            if (!existing) {
              return current;
            }
            const next = { ...current };
            delete next[requestBody.draftTrafficId];
            if (!(trafficId in next)) {
              next[trafficId] = existing;
            }
            return next;
          });
          setFlightStationSyncSelectionsByTrafficId((current) => {
            const existing = current[requestBody.draftTrafficId];
            if (!existing) {
              return current;
            }
            const next = { ...current };
            delete next[requestBody.draftTrafficId];
            if (!(trafficId in next)) {
              next[trafficId] = normalizeEstNumList(existing);
            }
            return next;
          });
        }
      }

      setDraftSessionsByTrafficId({});
      stationDeliveryStatusRestoreRef.current = {};
      stationConfirmedStatusRestoreRef.current = {};

      const selectedOverride = asString(savedTrafficIdByDraftId[asString(selectedTrafficId)] || selectedTrafficId) || null;
      const nextSelectedId = selectedOverride || asString(savedTrafficIdByDraftId[asString(detailDraft?.traffic.id)]) || null;
      if (nextSelectedId) {
        setSelectedTrafficId(nextSelectedId);
        upsertSelectedByAccount(activeAccountCode, nextSelectedId);
      }

      let nextTrafficList = trafficList;
      let nextSelectedSavedDetail: TrafficDetail | null = null;
      for (const saved of savedTrafficListWrites) {
        nextTrafficList = upsertTrafficSummaryInList(
          nextTrafficList,
          saved.summary,
          saved.sourceDraftId,
          saved.trafficId,
          saved.isLocalDraft,
        );
        if (nextSelectedId && (saved.sourceDraftId === nextSelectedId || saved.trafficId === nextSelectedId)) {
          const matchingCacheWrite = savedTrafficCacheWrites.find((entry) => entry.cacheKey === buildTrafficDetailCacheKey(saved.trafficId));
          if (matchingCacheWrite) {
            nextSelectedSavedDetail = cloneDetail(matchingCacheWrite.detail);
          }
        }
      }
      if (savedTrafficListWrites.length > 0) {
        const fetchedAt = Date.now();
        setTrafficList(nextTrafficList);
        setCacheStatus({ source: "network", fetchedAt });
        if (activeAccountCode) {
          writeBrowserCache(buildTrafficListCacheKey(activeAccountCode), nextTrafficList, TRAFFIC_LIST_CACHE_TTL_MS, {
            source: "network",
            fetchedAt,
          });
        }
      }
      if (nextSelectedSavedDetail) {
        setDetailBaseline(cloneDetail(nextSelectedSavedDetail));
        setDetailDraft(cloneDetail(nextSelectedSavedDetail));
      }

      for (const cacheWrite of savedTrafficCacheWrites) {
        writeBrowserCache(cacheWrite.cacheKey, cacheWrite.detail, TRAFFIC_DETAIL_CACHE_TTL_MS, {
          source: "network",
          fetchedAt: Date.now(),
        });
        if (cacheWrite.oldCacheKey) {
          removeBrowserCache(cacheWrite.oldCacheKey);
        }
      }

      setRefreshMessage(null);
      clearDeferredUpdate();
      void playSuccessSound();
      toast.success(
        "Traffic saved",
        `${saveTargets.length} traffic record(s) saved.`,
      );
    } catch (saveError) {
      setError(getTrafficErrorMessage(saveError, "Failed to save traffic changes."));
    } finally {
      setIsSaving(false);
    }
  }, [
    activeAccountCode,
    buildBulkSaveRequestForDetail,
    canSaveChanges,
    clearDeferredUpdate,
    detailDraft,
    draftSessionsByTrafficId,
    hasUnsavedChanges,
    requestHeaders,
    requestJson,
    selectedTrafficId,
    stashCurrentDraftSession,
    stationLookupMetaByCode,
    trafficList,
    toast,
    upsertSelectedByAccount,
    validateForSave,
  ]);

  const handleSaveShortcut = useCallback((event: KeyboardEvent) => {
    const key = asString(event.key).toLowerCase();
    if (!(event.metaKey || event.ctrlKey) || key !== "s") {
      return;
    }

    event.preventDefault();
    event.stopPropagation();

    if (!canSaveChanges) {
      return;
    }

    void handleSaveAll();
  }, [canSaveChanges, handleSaveAll]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      handleSaveShortcut(event);
    };

    window.addEventListener("keydown", onKeyDown, true);
    return () => {
      window.removeEventListener("keydown", onKeyDown, true);
    };
  }, [handleSaveShortcut]);

  const resolveDiscardAllUnsavedState = useCallback(() => {
    const activeDraftId = asString(detailDraft?.traffic.id);
    const activeHasUnsavedChanges = hasTrafficDetailChanges(detailBaseline, detailDraft);
    const unsavedBaselineByTrafficId: Record<string, TrafficDetail | null> = {};

    for (const [trafficId, session] of Object.entries(draftSessionsByTrafficId)) {
      unsavedBaselineByTrafficId[trafficId] = cloneDetail(session.baseline);
    }
    if (activeDraftId && activeHasUnsavedChanges) {
      unsavedBaselineByTrafficId[activeDraftId] = cloneDetail(detailBaseline);
    }

    const localDraftIdsToDrop = Object.entries(unsavedBaselineByTrafficId)
      .filter(([trafficId, baseline]) => !baseline && isLocalTrafficId(trafficId))
      .map(([trafficId]) => trafficId);
    const localDraftIdSet = new Set(localDraftIdsToDrop);
    const nextFlightStationSyncSelectionsByTrafficId = Object.fromEntries(
      Object.entries(flightStationSyncSelectionsByTrafficId)
        .filter(([trafficId]) => !localDraftIdSet.has(trafficId)),
    );

    const nextList = trafficList
      .filter((item) => !localDraftIdSet.has(asString(item.id)))
      .map((item) => {
        const baseline = unsavedBaselineByTrafficId[asString(item.id)];
        if (!baseline) {
          return item;
        }
        return buildListSummaryFromDetail(baseline, stationLookupMetaByCode);
      });

    const selectedIdExists = Boolean(
      selectedTrafficId
      && nextList.some((item) => item.id === selectedTrafficId),
    );
    const nextSelectedId = selectedIdExists
      ? selectedTrafficId
      : (nextList[0]?.id || null);

    const shouldDropActiveLocalDraft = localDraftIdSet.has(activeDraftId);
    const nextDetailBaseline = shouldDropActiveLocalDraft
      ? null
      : cloneDetail(detailBaseline);
    const nextDetailDraft = shouldDropActiveLocalDraft
      ? null
      : cloneDetail(detailBaseline);

    return {
      nextList,
      nextSelectedId,
      nextDetailBaseline,
      nextDetailDraft,
      localDraftIdsToDrop,
      nextFlightStationSyncSelectionsByTrafficId,
    };
  }, [
    detailBaseline,
    detailDraft,
    draftSessionsByTrafficId,
    flightStationSyncSelectionsByTrafficId,
    selectedTrafficId,
    stationLookupMetaByCode,
    trafficList,
  ]);

  const handleDiscard = useCallback(() => {
    const {
      nextList,
      nextSelectedId,
      nextDetailBaseline,
      nextDetailDraft,
      localDraftIdsToDrop,
      nextFlightStationSyncSelectionsByTrafficId,
    } = resolveDiscardAllUnsavedState();

    setDraftSessionsByTrafficId({});
    setTrafficList(nextList);
    setSelectedTrafficId(nextSelectedId);
    upsertSelectedByAccount(activeAccountCode, nextSelectedId);
    setDetailBaseline(nextDetailBaseline);
    setDetailDraft(nextDetailDraft);
    setFlightStationSyncSelectionsByTrafficId(nextFlightStationSyncSelectionsByTrafficId);
    if (localDraftIdsToDrop.length > 0) {
      setEmailWorkspaceByTrafficId((current) => {
        const next = { ...current };
        let changed = false;
        for (const trafficId of localDraftIdsToDrop) {
          if (!(trafficId in next)) {
            continue;
          }
          delete next[trafficId];
          changed = true;
        }
        return changed ? next : current;
      });
    }
    setRefreshMessage(null);
    clearDeferredUpdate();
    if (nextSelectedId && !isLocalTrafficId(nextSelectedId)) {
      void loadTrafficDetail(nextSelectedId, { policy: "stale-while-revalidate", deferWhenDirty: false });
    }
  }, [
    activeAccountCode,
    clearDeferredUpdate,
    loadTrafficDetail,
    resolveDiscardAllUnsavedState,
    setDraftSessionsByTrafficId,
    setEmailWorkspaceByTrafficId,
    upsertSelectedByAccount,
  ]);

  const refreshStationRowsFromMaster = useCallback(async (params?: {
    trafficId?: string | null;
    stationCodes?: string[];
    autoMergeEmails?: boolean;
  }) => {
    if (!isOnline) {
      return;
    }
    const targetTrafficId = asString(params?.trafficId || selectedTrafficId || detailDraft?.traffic.id);
    if (!targetTrafficId || isLocalTrafficId(targetTrafficId)) {
      return;
    }
    const requestedCodes = Array.isArray(params?.stationCodes) ? params?.stationCodes : [];
    const stationCodes = [...new Set(
      requestedCodes
        .map((code) => asString(code).toUpperCase())
        .filter(Boolean),
    )];
    if (stationCodes.length === 0) {
      return;
    }

    const query = new URLSearchParams();
    query.set("codes", stationCodes.join(","));
    query.set("deliveryMethodDetail", "true");
    query.set("contactDetail", "true");
    query.set("includeContacts", "true");

    try {
      const payload = await requestJson(`/api/tradsphere/v1/stations?${query.toString()}`, {
        headers: requestHeaders,
        successToast: false,
        errorToast: false,
      });
      const rows = unwrapData(payload);
      if (!Array.isArray(rows)) {
        return;
      }

      const refreshedByCode = new Map<string, {
        stationName: string | null;
        deliveryMethod: string | null;
        contactsSnapshot: Record<string, unknown> | null;
      }>();
      const nextLookupMetaByCode: Record<string, StationLookupMeta> = {};

      for (const row of rows) {
        if (!isRecord(row)) {
          continue;
        }
        const stationCode = asString(row.code || row.stationCode).toUpperCase();
        if (!stationCode) {
          continue;
        }
        const deliveryMethodRaw = row.deliveryMethod;
        const deliveryMethodName = isRecord(deliveryMethodRaw)
          ? asString(deliveryMethodRaw.name)
          : asString(deliveryMethodRaw);
        const contactsSnapshot = isRecord(row.contacts) ? row.contacts : null;
        const normalizedMeta = normalizeStationLookupMeta(row, stationCode);
        if (normalizedMeta) {
          nextLookupMetaByCode[stationCode] = normalizedMeta;
          writeBrowserCache(
            buildStationLookupCacheKey(stationCode),
            {
              code: normalizedMeta.code,
              name: normalizedMeta.name,
              mediaType: normalizedMeta.mediaType,
              language: normalizedMeta.language,
              deliveryMethod: normalizedMeta.deliveryMethod?.name || "",
              contactsSnapshot,
            },
            STATION_LOOKUP_CACHE_TTL_MS,
            {
              source: "network",
              fetchedAt: Date.now(),
            },
          );
        }
        refreshedByCode.set(stationCode, {
          stationName: asNullableString(row.name),
          deliveryMethod: asNullableString(deliveryMethodName),
          contactsSnapshot,
        });
      }

      if (refreshedByCode.size === 0) {
        return;
      }

      if (Object.keys(nextLookupMetaByCode).length > 0) {
        setStationLookupMetaByCode((current) => ({
          ...current,
          ...nextLookupMetaByCode,
        }));
      }

      const applyStationRefreshToDetail = (detail: TrafficDetail | null): TrafficDetail | null => {
        if (!detail || detail.traffic.id !== targetTrafficId) {
          return detail;
        }
        let changed = false;
        const nextStations = detail.stations.map((station) => {
          const stationCode = asString(station.stationCode).toUpperCase();
          const refreshed = refreshedByCode.get(stationCode);
          if (!refreshed) {
            return station;
          }
          const nextDeliveryMethod = refreshed.deliveryMethod ?? station.deliveryMethod;
          const nextContactsSnapshot = refreshed.contactsSnapshot ?? station.contactsSnapshot;
          if (
            asString(station.deliveryMethod ?? "") === asString(nextDeliveryMethod ?? "")
            && JSON.stringify(station.contactsSnapshot ?? null) === JSON.stringify(nextContactsSnapshot ?? null)
          ) {
            return station;
          }
          changed = true;
          return {
            ...station,
            deliveryMethod: asNullableString(nextDeliveryMethod),
            contactsSnapshot: nextContactsSnapshot,
          };
        });
        if (!changed) {
          return detail;
        }
        return computeSummary({
          ...detail,
          stations: nextStations,
        });
      };

      const activeDetailForPrompt = detailDraft && detailDraft.traffic.id === targetTrafficId
        ? applyStationRefreshToDetail(detailDraft)
        : null;
      let autoSyncToEmails = false;
      if (activeDetailForPrompt?.email) {
        const refreshContactEmails = activeDetailForPrompt.stations.flatMap((station) => (
          extractPreferredContactEmails(station.contactsSnapshot)
        ));
        const mergedToEmails = mergeUniqueEmails(activeDetailForPrompt.email.toEmails, refreshContactEmails);
        const currentToEmailSet = new Set(activeDetailForPrompt.email.toEmails.map((email) => asString(email).toLowerCase()));
        const addedToEmails = mergedToEmails.filter((email) => !currentToEmailSet.has(email));
        if (addedToEmails.length > 0) {
          if (params?.autoMergeEmails) {
            autoSyncToEmails = true;
          } else {
            setPendingRefreshEmailMerge({
              trafficId: targetTrafficId,
              emails: addedToEmails,
            });
            setIsRefreshEmailMergeDialogOpen(true);
          }
        }
      }

      const applyAutoEmailMerge = (detail: TrafficDetail | null): TrafficDetail | null => {
        if (!detail || detail.traffic.id !== targetTrafficId || !detail.email || !autoSyncToEmails) {
          return detail;
        }
        const nextToEmails = buildStationEmailRecipients(detail.stations);
        if (areStringArraysEqual(nextToEmails, detail.email.toEmails)) {
          return detail;
        }
        return {
          ...detail,
          email: {
            ...detail.email,
            toEmails: nextToEmails,
          },
        };
      };
      const applyStationRefreshWithOptionalEmailMerge = (detail: TrafficDetail | null): TrafficDetail | null => {
        const refreshed = applyStationRefreshToDetail(detail);
        return applyAutoEmailMerge(refreshed);
      };

      setDetailBaseline((current) => applyStationRefreshWithOptionalEmailMerge(current));
      setDetailDraft((current) => applyStationRefreshWithOptionalEmailMerge(current));
      setStationModalDraft((current) => {
        if (!current || asString(current.trafficId) !== targetTrafficId) {
          return current;
        }
        const stationCode = asString(current.stationCode).toUpperCase();
        const refreshed = refreshedByCode.get(stationCode);
        if (!refreshed) {
          return current;
        }
        return {
          ...current,
          deliveryMethod: refreshed.deliveryMethod ?? current.deliveryMethod,
          contactsSnapshot: refreshed.contactsSnapshot ?? current.contactsSnapshot,
        };
      });
      if (stationLookupQueryCode) {
        const refreshed = refreshedByCode.get(stationLookupQueryCode);
        if (refreshed) {
          setStationLookupName(refreshed.stationName);
          setStationLookupError(null);
        }
      }
    } catch {
      // Keep refresh resilient: traffic refresh already completed even if this follow-up fetch fails.
    }
  }, [detailDraft, isOnline, requestHeaders, requestJson, selectedTrafficId, stationLookupQueryCode]);

  const handleRefresh = useCallback(async () => {
    if (!activeAccountCode) {
      return;
    }
    const targetTrafficId = asString(selectedTrafficId || activeDraft?.traffic.id) || null;
    const targetStationCodes = (activeDraft?.stations ?? [])
      .map((station) => asString(station.stationCode).toUpperCase())
      .filter(Boolean);
    setStationMetaRefreshToken((current) => current + 1);
    if (stationLookupQueryCode) {
      setStationLookupRefreshToken((current) => current + 1);
    }
    await refreshSelections();
    await loadAccountTraffic(activeAccountCode, "network-first", {
      selectedIdOverride: selectedTrafficId,
      deferWhenDirty: false,
    });
    await refreshStationRowsFromMaster({
      trafficId: targetTrafficId,
      stationCodes: targetStationCodes,
    });
  }, [activeAccountCode, activeDraft?.stations, activeDraft?.traffic.id, loadAccountTraffic, refreshSelections, refreshStationRowsFromMaster, selectedTrafficId, stationLookupQueryCode]);

  const handleRefreshFromChip = useCallback(async () => {
    setIsChipRefreshOverlayVisible(true);
    try {
      await handleRefresh();
    } finally {
      setIsChipRefreshOverlayVisible(false);
    }
  }, [handleRefresh]);

  const resolveSyncMissingStationsDialog = useCallback((stationCodesToRemove: string[]) => {
    const resolver = syncMissingStationsResolverRef.current;
    syncMissingStationsResolverRef.current = null;
    setIsSyncMissingStationsDialogOpen(false);
    setPendingSyncMissingStationsCodes([]);
    setSelectedSyncMissingStationsCodes([]);
    if (resolver) {
      resolver(stationCodesToRemove);
    }
  }, []);

  const promptSyncMissingStationsDecision = useCallback((missingCodes: string[]): Promise<string[]> => {
    const normalized = [...new Set(
      missingCodes.map((code) => asString(code).toUpperCase()).filter(Boolean),
    )];
    if (normalized.length === 0) {
      return Promise.resolve([]);
    }
    if (syncMissingStationsResolverRef.current) {
      syncMissingStationsResolverRef.current([]);
      syncMissingStationsResolverRef.current = null;
    }
    setPendingSyncMissingStationsCodes(normalized);
    setSelectedSyncMissingStationsCodes(normalized);
    setIsSyncMissingStationsDialogOpen(true);
    return new Promise<string[]>((resolve) => {
      syncMissingStationsResolverRef.current = resolve;
    });
  }, []);

  const toggleSyncMissingStationSelection = useCallback((stationCodeRaw: string) => {
    const stationCode = asString(stationCodeRaw).toUpperCase();
    if (!stationCode) {
      return;
    }
    setSelectedSyncMissingStationsCodes((current) => {
      if (current.includes(stationCode)) {
        return current.filter((code) => code !== stationCode);
      }
      return [...current, stationCode];
    });
  }, []);

  const handleSelectAllSyncMissingStations = useCallback(() => {
    setSelectedSyncMissingStationsCodes([...pendingSyncMissingStationsCodes]);
  }, [pendingSyncMissingStationsCodes]);

  const handleClearSyncMissingStations = useCallback(() => {
    setSelectedSyncMissingStationsCodes([]);
  }, []);

  const handleConfirmSyncMissingStationsRemoval = useCallback(() => {
    const selectedSet = new Set(
      selectedSyncMissingStationsCodes
        .map((code) => asString(code).toUpperCase())
        .filter(Boolean),
    );
    const normalizedSelection = pendingSyncMissingStationsCodes.filter((code) => selectedSet.has(code));
    resolveSyncMissingStationsDialog(normalizedSelection);
  }, [pendingSyncMissingStationsCodes, resolveSyncMissingStationsDialog, selectedSyncMissingStationsCodes]);

  const handleResolveRefreshEmailMerge = useCallback((shouldMerge: boolean) => {
    const pending = pendingRefreshEmailMerge;
    setIsRefreshEmailMergeDialogOpen(false);
    setPendingRefreshEmailMerge(null);
    if (!shouldMerge || !pending) {
      return;
    }
    const applyEmailMerge = (detail: TrafficDetail | null): TrafficDetail | null => {
      if (!detail || detail.traffic.id !== pending.trafficId || !detail.email) {
        return detail;
      }
      const nextToEmails = mergeUniqueEmails(detail.email.toEmails, pending.emails);
      if (areStringArraysEqual(nextToEmails, detail.email.toEmails)) {
        return detail;
      }
      return {
        ...detail,
        email: {
          ...detail.email,
          toEmails: nextToEmails,
        },
      };
    };
    setDetailBaseline((current) => applyEmailMerge(current));
    setDetailDraft((current) => applyEmailMerge(current));
    toast.info(
      "Email recipients updated",
      `${pending.emails.length} contact email(s) added to To.`,
    );
  }, [pendingRefreshEmailMerge, toast]);

  const handleLoadFromSelector = useCallback(async () => {
    const targetAccountCode = asString(selectedAccountCode).toUpperCase();
    if (!targetAccountCode) {
      return;
    }
    const cacheSnapshot = readBrowserCacheSnapshot<TrafficSummary[]>(buildTrafficListCacheKey(targetAccountCode));
    const hasCachedList = normalizeTrafficSummaryList(cacheSnapshot?.data).length > 0;
    if (!hasCachedList) {
      setIsLoadActionOverlayVisible(true);
    }
    try {
      setLoadedAccountCode(targetAccountCode);
      await loadAccountTraffic(targetAccountCode, "stale-while-revalidate", {
        selectedIdOverride: selectedTrafficByAccount[targetAccountCode] || null,
        deferWhenDirty: false,
      });
    } finally {
      if (!hasCachedList) {
        setIsLoadActionOverlayVisible(false);
      }
    }
  }, [loadAccountTraffic, selectedAccountCode, selectedTrafficByAccount]);

  const handleRefreshFromSelector = useCallback(async () => {
    if (!activeAccountCode) {
      return;
    }
    setIsSelectorRefreshOverlayVisible(true);
    try {
      await handleRefresh();
    } finally {
      setIsSelectorRefreshOverlayVisible(false);
    }
  }, [activeAccountCode, handleRefresh]);

  const handleAccountChange = useCallback((nextAccountCodeRaw: string) => {
    const nextAccountCode = asString(nextAccountCodeRaw).toUpperCase();
    if (nextAccountCode === asString(selectedAccountCode).toUpperCase()) {
      return;
    }
    setSelectedAccountCode(nextAccountCode);
    setRefreshMessage(null);
  }, [selectedAccountCode, setSelectedAccountCode]);

  const handleRestoreLoadedAccountSelection = useCallback(() => {
    if (!activeAccountCode) {
      return;
    }
    setSelectedAccountCode(activeAccountCode);
    setRefreshMessage(null);
  }, [activeAccountCode, setSelectedAccountCode]);

  const handleSelectTraffic = useCallback((trafficIdRaw: string) => {
    const trafficId = asString(trafficIdRaw);
    if (!trafficId || trafficId === selectedTrafficId) {
      return;
    }
    stashCurrentDraftSession();
    setSelectedTrafficId(trafficId);
    upsertSelectedByAccount(activeAccountCode, trafficId);
    if (restoreDraftSession(trafficId)) {
      return;
    }
    if (isLocalTrafficId(trafficId)) {
      return;
    }
    void loadTrafficDetail(trafficId, { policy: "stale-while-revalidate", deferWhenDirty: false });
  }, [activeAccountCode, loadTrafficDetail, restoreDraftSession, selectedTrafficId, stashCurrentDraftSession, upsertSelectedByAccount]);

  const handleCreateTraffic = useCallback(async () => {
    if (!activeAccountCode || !canEditTradsphere || isSaving) {
      return;
    }
    stashCurrentDraftSession();
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
        ccEmails: [...DEFAULT_TRAFFIC_CC_EMAILS],
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
    stashCurrentDraftSession,
    toast,
    trafficList,
    upsertSelectedByAccount,
  ]);

  const buildDuplicatedTrafficDraft = useCallback((source: TrafficDetail): TrafficDetail => {
    const nextTrafficId = buildLocalTrafficId();
    const nextFlights = source.flights.map((flight) => {
      const nextId = tempRowIdRef.current;
      tempRowIdRef.current -= 1;
      return {
        ...flight,
        id: nextId,
        trafficId: nextTrafficId,
        dateCreated: null,
        dateUpdated: null,
      };
    });
    const nextStations = source.stations.map((station) => {
      const nextId = tempRowIdRef.current;
      tempRowIdRef.current -= 1;
      return {
        ...station,
        id: nextId,
        trafficId: nextTrafficId,
        dateCreated: null,
        dateUpdated: null,
      };
    });
    const nextEmail = source.email
      ? {
          ...source.email,
          id: 0,
          trafficId: nextTrafficId,
          sentStatus: "draft",
          sentAt: null,
          sentByUserId: null,
          smtpMessageId: null,
          lastSendAttemptAt: null,
          lastSendError: null,
          dateCreated: null,
          dateUpdated: null,
        }
      : null;

    return computeSummary({
      traffic: {
        ...source.traffic,
        id: nextTrafficId,
        status: "draft",
        dateCreated: null,
        dateUpdated: null,
      },
      flights: nextFlights,
      stations: nextStations,
      email: nextEmail,
      summary: {
        totalRotation: 0,
        rotationWarning: false,
        rotationWarningMessage: null,
        warnings: [],
        flightCount: 0,
        stationCount: 0,
      },
    });
  }, []);

  const resolveTrafficDetailForDuplicate = useCallback(async (trafficIdRaw: string): Promise<TrafficDetail | null> => {
    const trafficId = asString(trafficIdRaw);
    if (!trafficId) {
      return null;
    }
    if (activeDraft && asString(activeDraft.traffic.id) === trafficId) {
      return cloneDetail(activeDraft);
    }

    const session = draftSessionsByTrafficId[trafficId];
    if (session?.draft) {
      return cloneDetail(session.draft);
    }

    const cachedSnapshot = readBrowserCacheSnapshot<TrafficDetail>(buildTrafficDetailCacheKey(trafficId));
    const cachedDetail = normalizeTrafficDetail(cachedSnapshot?.data);
    if (cachedDetail) {
      return cachedDetail;
    }

    if (!isOnline) {
      return null;
    }

    const payload = await requestJson(`/api/tradsphere/v1/traffic?id=${encodeURIComponent(trafficId)}`, {
      headers: requestHeaders,
      successToast: false,
      errorToast: false,
    });
    return normalizeTrafficDetail(payload);
  }, [activeDraft, draftSessionsByTrafficId, isOnline, requestHeaders, requestJson]);

  const handleDuplicateTraffic = useCallback(async (trafficIdRaw: string) => {
    const trafficId = asString(trafficIdRaw);
    if (!trafficId || !canEditTradsphere || isSaving || isLoadingAccountTraffic || isDetailBusy) {
      return;
    }
    if (duplicatingTrafficId) {
      return;
    }

    setDuplicatingTrafficId(trafficId);
    try {
      const sourceDetail = await resolveTrafficDetailForDuplicate(trafficId);
      if (!sourceDetail) {
        throw new Error("Traffic detail is unavailable for duplication.");
      }

      stashCurrentDraftSession();

      const duplicatedDraft = buildDuplicatedTrafficDraft(sourceDetail);
      const nextList = [buildListSummaryFromDetail(duplicatedDraft, stationLookupMetaByCode), ...trafficList];

      setTrafficList(nextList);
      setSelectedTrafficId(duplicatedDraft.traffic.id);
      upsertSelectedByAccount(activeAccountCode, duplicatedDraft.traffic.id);
      setDetailBaseline(null);
      setDetailDraft(duplicatedDraft);
      setRefreshMessage(null);
      setError(null);
      toast.success("Draft duplicated", "A local copy of the traffic record was created.");
    } catch (duplicateError) {
      const message = getTrafficErrorMessage(duplicateError, "Failed to duplicate traffic.");
      toast.error("Unable to duplicate traffic", message);
    } finally {
      setDuplicatingTrafficId(null);
    }
  }, [
    activeAccountCode,
    buildDuplicatedTrafficDraft,
    canEditTradsphere,
    duplicatingTrafficId,
    isLoadingAccountTraffic,
    isDetailBusy,
    isSaving,
    resolveTrafficDetailForDuplicate,
    stashCurrentDraftSession,
    stationLookupMetaByCode,
    toast,
    trafficList,
    upsertSelectedByAccount,
  ]);

  const applyOptimisticTrafficRemoval = useCallback((trafficIdRaw: string): OptimisticTrafficRemovalSnapshot | null => {
    const trafficId = asString(trafficIdRaw);
    if (!trafficId) {
      return null;
    }
    const previousList = trafficListRef.current;
    const previousSelected = selectedTrafficIdRef.current;
    const previousDetailBaseline = cloneDetail(detailBaselineRef.current);
    const previousDetailDraft = cloneDetail(detailDraftRef.current);
    const previousRefreshMessage = refreshMessageRef.current;

    const nextList = previousList.filter((item) => item.id !== trafficId);
    const removedSelected = previousSelected === trafficId;
    const selectedStillExists = Boolean(previousSelected && nextList.some((item) => item.id === previousSelected));
    const nextSelected = removedSelected
      ? (nextList[0]?.id || null)
      : (selectedStillExists ? previousSelected : (nextList[0]?.id || null));

    setTrafficList(nextList);
    trafficListRef.current = nextList;
    setSelectedTrafficId(nextSelected);
    selectedTrafficIdRef.current = nextSelected;
    upsertSelectedByAccount(activeAccountCodeRef.current, nextSelected);
    setDraftSessionsByTrafficId((current) => {
      if (!(trafficId in current)) {
        return current;
      }
      const next = { ...current };
      delete next[trafficId];
      return next;
    });
    if (removedSelected) {
      setDetailBaseline(null);
      setDetailDraft(null);
      detailBaselineRef.current = null;
      detailDraftRef.current = null;
    }
    setRefreshMessage(null);
    refreshMessageRef.current = null;

    const fetchedAt = Date.now();
    writeBrowserCache(buildTrafficListCacheKey(activeAccountCodeRef.current), nextList, TRAFFIC_LIST_CACHE_TTL_MS, {
      source: "network",
      fetchedAt,
    });
    setCacheStatus({ source: "network", fetchedAt });

    return {
      trafficId,
      previousList,
      previousSelected,
      previousDetailBaseline,
      previousDetailDraft,
      previousRefreshMessage,
      nextSelected,
      removedSelected,
    };
  }, [upsertSelectedByAccount]);

  const handleRemoveTrafficLocally = useCallback(async (trafficIdRaw: string) => {
    const snapshot = applyOptimisticTrafficRemoval(trafficIdRaw);
    if (!snapshot) {
      return;
    }
    if (snapshot.removedSelected && snapshot.nextSelected && !isLocalTrafficId(snapshot.nextSelected)) {
      await loadTrafficDetail(snapshot.nextSelected, { policy: "stale-while-revalidate", deferWhenDirty: false });
    }
  }, [applyOptimisticTrafficRemoval, loadTrafficDetail]);

  const openTrafficRemovalDialog = useCallback((trafficIdRaw: string, mode: TrafficRemovalMode) => {
    const trafficId = asString(trafficIdRaw);
    if (!trafficId || !canEditTradsphere || isSaving || isLoadingAccountTraffic || isDetailBusy) {
      return;
    }
    if (deletingTrafficIdSet.has(trafficId)) {
      return;
    }
    setArchiveTargetTrafficId(trafficId);
    setArchiveDialogMode(mode);
    setIsArchiveDialogOpen(true);
  }, [canEditTradsphere, deletingTrafficIdSet, isDetailBusy, isLoadingAccountTraffic, isSaving]);

  const handleArchiveTraffic = useCallback(async () => {
    const targetTrafficId = asString(archiveTargetTrafficId) || asString(detailDraft?.traffic.id);
    if (!targetTrafficId || !canEditTradsphere || isSaving || isLoadingAccountTraffic || isDetailBusy) {
      return;
    }
    if (deletingTrafficIdSet.has(targetTrafficId)) {
      return;
    }
    const removalMode = archiveDialogMode;
    setDeletingTrafficIds((current) => (current.includes(targetTrafficId) ? current : [...current, targetTrafficId]));
    setArchiveTargetTrafficId(null);
    setIsArchiveDialogOpen(false);
    if (isLocalTrafficId(targetTrafficId)) {
      await handleRemoveTrafficLocally(targetTrafficId);
      toast.success("Draft removed", "Unsaved local traffic draft was removed.");
      setDeletingTrafficIds((current) => current.filter((id) => id !== targetTrafficId));
      return;
    }

    try {
      setError(null);
      await requestJson(`/api/tradsphere/v1/traffic/archive?id=${encodeURIComponent(targetTrafficId)}`, {
        method: "POST",
        headers: requestHeaders,
        body: {},
        successToast: {
          title: removalMode === "delete" ? "Traffic removed" : "Traffic archived",
          message: removalMode === "delete"
            ? "The traffic record was removed from the active list."
            : "The traffic record was archived.",
        },
      });
      const snapshot = applyOptimisticTrafficRemoval(targetTrafficId);
      if (!snapshot) {
        return;
      }
      if (snapshot.removedSelected && snapshot.nextSelected && !isLocalTrafficId(snapshot.nextSelected)) {
        await loadTrafficDetail(snapshot.nextSelected, { policy: "stale-while-revalidate", deferWhenDirty: false });
      }
    } catch (archiveError) {
      setError(getTrafficErrorMessage(
        archiveError,
        removalMode === "delete"
          ? "Failed to delete traffic."
          : "Unable to archive traffic.",
      ));
    } finally {
      setDeletingTrafficIds((current) => current.filter((id) => id !== targetTrafficId));
      setArchiveDialogMode("archive");
      setArchiveTargetTrafficId(null);
      setIsArchiveDialogOpen(false);
    }
  }, [
    applyOptimisticTrafficRemoval,
    archiveDialogMode,
    archiveTargetTrafficId,
    canEditTradsphere,
    deletingTrafficIdSet,
    detailDraft,
    handleRemoveTrafficLocally,
    isLoadingAccountTraffic,
    isDetailBusy,
    isSaving,
    loadTrafficDetail,
    requestHeaders,
    requestJson,
    toast,
  ]);

  const handleResolveUnsavedDialog = useCallback(async (discardChanges: boolean) => {
    const action = pendingAction;
    setPendingAction(null);
    setIsUnsavedDialogOpen(false);

    if (!discardChanges) {
      return;
    }

    const {
      nextList,
      nextSelectedId,
      nextDetailBaseline,
      nextDetailDraft,
      localDraftIdsToDrop,
      nextFlightStationSyncSelectionsByTrafficId,
    } = resolveDiscardAllUnsavedState();

    setDraftSessionsByTrafficId({});
    setTrafficList(nextList);
    setSelectedTrafficId(nextSelectedId);
    upsertSelectedByAccount(activeAccountCode, nextSelectedId);
    setDetailBaseline(nextDetailBaseline);
    setDetailDraft(nextDetailDraft);
    setFlightStationSyncSelectionsByTrafficId(nextFlightStationSyncSelectionsByTrafficId);
    if (localDraftIdsToDrop.length > 0) {
      setEmailWorkspaceByTrafficId((current) => {
        const next = { ...current };
        let changed = false;
        for (const trafficId of localDraftIdsToDrop) {
          if (!(trafficId in next)) {
            continue;
          }
          delete next[trafficId];
          changed = true;
        }
        return changed ? next : current;
      });
    }
    setRefreshMessage(null);
    clearDeferredUpdate();

    if (!action) {
      if (nextSelectedId && !isLocalTrafficId(nextSelectedId)) {
        void loadTrafficDetail(nextSelectedId, {
          policy: "stale-while-revalidate",
          deferWhenDirty: false,
        });
      }
      return;
    }
    if (action.type === "account") {
      setSelectedAccountCode(action.accountCode);
      setLoadedAccountCode(action.accountCode);
      await loadAccountTraffic(action.accountCode, "stale-while-revalidate", {
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
      await handleRefreshFromSelector();
      return;
    }
    if (action.type === "route") {
      const snapshot: TrafficPageSnapshot = {
        loadedAccountCode,
        selectedTrafficId: nextSelectedId,
        trafficList: nextList,
        detailBaseline: nextDetailBaseline,
        detailDraft: nextDetailDraft,
        cacheStatus,
        refreshMessage: null,
        activeWorkspaceTab,
        flightStationSyncSelectionsByTrafficId: nextFlightStationSyncSelectionsByTrafficId,
      };
      writeScopedPageState(pageStateScope, snapshot);
      action.proceed();
    }
  }, [
    activeWorkspaceTab,
    activeAccountCode,
    cacheStatus,
    clearDeferredUpdate,
    handleRefresh,
    handleRefreshFromChip,
    loadedAccountCode,
    loadAccountTraffic,
    loadTrafficDetail,
    pageStateScope,
    pendingAction,
    selectedTrafficByAccount,
    resolveDiscardAllUnsavedState,
    setSelectedAccountCode,
    setEmailWorkspaceByTrafficId,
    upsertSelectedByAccount,
  ]);

  const syncStationsFromFlightRange = useCallback(async (params: FlightStationSyncParams) => {
    const accountCode = asString(activeAccountCode).toUpperCase();
    const flightStart = asString(params.flightStart);
    const flightEnd = asString(params.flightEnd);
    const estNums = normalizeEstNumList(params.estNums);
    const requestedLanguages = normalizeFlightLanguageList(params.languages);
    const requestedMediums = normalizeFlightMediumList(params.mediums);
    const fallbackLanguages = resolveFlightLanguagesFromFlights(detailDraft?.flights ?? [], flightStart, flightEnd);
    const fallbackMediums = resolveFlightMediumsFromFlights(detailDraft?.flights ?? [], flightStart, flightEnd);
    const languages = requestedLanguages.length > 0
      ? requestedLanguages
      : fallbackLanguages;
    const mediums = requestedMediums.length > 0
      ? requestedMediums
      : fallbackMediums;
    if (!accountCode || !flightStart || !flightEnd) {
      return;
    }

    setIsStationAutoSyncing(true);
    try {
      const normalizedResult = await requestStationCandidatesWithCache({
        accountCode,
        flightStart,
        flightEnd,
        estNums,
        languages,
        mediums,
        preferCache: true,
      });
      const normalized = normalizedResult?.payload ?? null;
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
      const uniqueCandidateCodeSet = new Set(uniqueCandidateCodes);
      const autoAddedCodes = uniqueCandidateCodes.filter((code) => !currentCodes.has(code));
      const preservedCodes = uniqueCandidateCodes.filter((code) => currentCodes.has(code));
      const missingExistingCodes = [...currentCodes].filter((code) => !uniqueCandidateCodeSet.has(code));
      const selectedMissingStationsToRemove = missingExistingCodes.length > 0
        ? await promptSyncMissingStationsDecision(missingExistingCodes)
        : [];
      const missingExistingCodeSet = new Set(selectedMissingStationsToRemove);
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
      let removedMissingCount = 0;
      updateDraft((current) => {
        if (current.traffic.id !== params.trafficId) {
          return current;
        }
        const shouldAutoMarkReadyToEmail = hasFlightsForDelivery(current.flights);
        const syncContactEmails = uniqueCandidateCodes.flatMap((stationCode) => {
          const candidate = stationCandidateByCode.get(stationCode);
          return extractPreferredContactEmails(candidate?.contactsSnapshot ?? null);
        });
        let changed = false;
        const baseStations = selectedMissingStationsToRemove.length > 0
          ? current.stations.filter((station) => {
            const stationCode = asString(station.stationCode).toUpperCase();
            if (!missingExistingCodeSet.has(stationCode)) {
              return true;
            }
            removedMissingCount += 1;
            changed = true;
            return false;
          })
          : current.stations;
        const nextStations = baseStations.map((station) => {
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
          .filter((station) => !existingCodes.has(station.stationCode));
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
      const stationCodesForMasterRefresh = [...new Set([...currentCodes, ...uniqueCandidateCodes])];
      if (stationCodesForMasterRefresh.length > 0) {
        void refreshStationRowsFromMaster({
          trafficId: params.trafficId,
          stationCodes: stationCodesForMasterRefresh,
          autoMergeEmails: true,
        });
      }

      const removedMissingSuffix = removedMissingCount > 0
        ? ` ${removedMissingCount} station(s) removed because they are no longer in synced schedule results.`
        : "";
      const keptMissingCount = Math.max(0, missingExistingCodes.length - removedMissingCount);
      const keptMissingSuffix = keptMissingCount > 0
        ? ` ${keptMissingCount} non-matching existing station(s) were kept.`
        : "";
      const readyToEmailSuffix = readyToEmailUpdatedCount > 0
        ? ` ${readyToEmailUpdatedCount} station(s) marked Ready to Email.`
        : "";

      if (autoAddedCodes.length > 0) {
        const addedLabel = formatStationCodesPreview(autoAddedCodes);
        if (updatedExistingCount > 0) {
          toast.info(
            "Stations synced from schedule",
            `${autoAddedCodes.length} station(s) added (${addedLabel}). ${updatedExistingCount} existing station(s) had contacts/delivery method refreshed.${removedMissingSuffix}${keptMissingSuffix}${readyToEmailSuffix}`,
          );
        } else if (preservedCodes.length > 0) {
          toast.info(
            "Stations auto-added from schedule",
            `${autoAddedCodes.length} station(s) added (${addedLabel}). ${preservedCodes.length} existing station(s) were preserved.${removedMissingSuffix}${keptMissingSuffix}${readyToEmailSuffix}`,
          );
        } else {
          toast.info(
            "Stations auto-added from schedule",
            `${autoAddedCodes.length} station(s) added (${addedLabel}).${removedMissingSuffix}${keptMissingSuffix}${readyToEmailSuffix}`,
          );
        }
        return;
      }

      if (uniqueCandidateCodes.length === 0) {
        const noMatchDetail = removedMissingCount > 0
          ? `No stations were returned for the selected account and flight date range.${removedMissingSuffix}${readyToEmailSuffix}`
          : `No stations were returned for the selected account and flight date range.${keptMissingSuffix}${readyToEmailSuffix}`;
        toast.info(
          "No matching schedule stations found",
          noMatchDetail,
        );
        return;
      }

      if (updatedExistingCount > 0) {
        toast.info(
          "Stations synced from schedule",
          `${updatedExistingCount} existing station(s) had contacts/delivery method refreshed.${removedMissingSuffix}${keptMissingSuffix}${readyToEmailSuffix}`,
        );
      } else {
        toast.info(
          "Existing stations preserved",
          `${preservedCodes.length} matching station(s) already exist and were left unchanged.${removedMissingSuffix}${keptMissingSuffix}${readyToEmailSuffix}`,
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
  }, [activeAccountCode, detailDraft?.flights, detailDraft?.stations, promptSyncMissingStationsDecision, refreshStationRowsFromMaster, requestStationCandidatesWithCache, toast, updateDraft]);

  const openStationSyncSelectDialog = useCallback((params: FlightStationSyncParams, source: FlightStationSyncDialogSource) => {
    const accountCode = asString(activeAccountCode).toUpperCase();
    if (!accountCode) {
      return;
    }
    setFlightStationSyncDialogSource(source);
    setPendingFlightStationSync(params);
    setFlightStationSyncCandidates(null);
    setFlightStationSyncCacheStatus(null);
    setSelectedFlightStationSyncEstNums(
      normalizeEstNumList(flightStationSyncSelectionsByTrafficId[params.trafficId]),
    );
    setIsFlightStationSyncSelectOpen(true);
    const requestedLanguages = normalizeFlightLanguageList(params.languages);
    const requestedMediums = normalizeFlightMediumList(params.mediums);
    const fallbackLanguages = resolveFlightLanguagesFromFlights(detailDraft?.flights ?? [], params.flightStart, params.flightEnd);
    const fallbackMediums = resolveFlightMediumsFromFlights(detailDraft?.flights ?? [], params.flightStart, params.flightEnd);
    const languages = requestedLanguages.length > 0
      ? requestedLanguages
      : fallbackLanguages;
    const mediums = requestedMediums.length > 0
      ? requestedMediums
      : fallbackMediums;
    const shouldForceNetworkCandidates = source === "flight_update" && Boolean(params.forceRefreshCandidates);
    const preferCacheForDialog = !shouldForceNetworkCandidates;
    void (async () => {
      try {
        const payload = await loadFlightStationSyncCandidates(
          {
            trafficId: params.trafficId,
            accountCode,
            flightStart: params.flightStart,
            flightEnd: params.flightEnd,
            languages,
            mediums,
            forceRefreshCandidates: params.forceRefreshCandidates,
          },
          {
            preferCache: preferCacheForDialog,
            preserveSelection: flightStationSyncSelectionsByTrafficId[params.trafficId],
          },
        );
        if (!payload) {
          toast.error(
            "Unable to load estimate numbers",
            "No estimate-number candidates were returned for station sync.",
          );
          setPendingFlightStationSync(null);
          setIsFlightStationSyncSelectOpen(false);
        }
      } catch (loadError) {
        toast.error(
          "Unable to load estimate numbers",
          getTrafficErrorMessage(loadError, "Estimate-number candidates could not be loaded."),
        );
        setPendingFlightStationSync(null);
        setIsFlightStationSyncSelectOpen(false);
      }
    })();
  }, [activeAccountCode, detailDraft?.flights, flightStationSyncSelectionsByTrafficId, loadFlightStationSyncCandidates, toast]);

  const flightStationSyncCacheStatusText = useMemo(() => {
    if (isFlightStationSyncCandidatesLoading && !flightStationSyncCacheStatus) {
      return "Loading matching estimate numbers...";
    }
    if (!pendingFlightStationSync) {
      return "No estimate-number candidates loaded.";
    }
    if (!isOnline && flightStationSyncCacheStatus) {
      return `Offline. Showing cached data from ${formatRelativeTime(flightStationSyncCacheStatus.fetchedAt)}.`;
    }
    if (isFlightStationSyncCandidatesLoading && flightStationSyncCacheStatus) {
      return `Refreshing estimate numbers from ${formatRelativeTime(flightStationSyncCacheStatus.fetchedAt)}.`;
    }
    if (flightStationSyncCacheStatus) {
      return `Data source: ${flightStationSyncCacheStatus.source}. Last updated ${formatRelativeTime(flightStationSyncCacheStatus.fetchedAt)}.`;
    }
    return "No cached data yet";
  }, [flightStationSyncCacheStatus, isFlightStationSyncCandidatesLoading, isOnline, pendingFlightStationSync]);

  const handleRefreshFlightStationSyncCandidates = useCallback(() => {
    if (!pendingFlightStationSync || isFlightStationSyncCandidatesLoading) {
      return;
    }
    void loadFlightStationSyncCandidates(
      {
        trafficId: pendingFlightStationSync.trafficId,
        accountCode: activeAccountCode,
        flightStart: pendingFlightStationSync.flightStart,
        flightEnd: pendingFlightStationSync.flightEnd,
        estNums: pendingFlightStationSync.estNums,
        languages: pendingFlightStationSync.languages,
        mediums: pendingFlightStationSync.mediums,
        forceRefreshCandidates: true,
      },
      {
        preferCache: false,
        preserveSelection: selectedFlightStationSyncEstNums,
      },
    ).catch((loadError) => {
      toast.error(
        "Unable to refresh estimate numbers",
        getTrafficErrorMessage(loadError, "Estimate-number candidates could not be refreshed."),
      );
    });
  }, [activeAccountCode, isFlightStationSyncCandidatesLoading, loadFlightStationSyncCandidates, pendingFlightStationSync, selectedFlightStationSyncEstNums, toast]);

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
    openStationSyncSelectDialog({
      trafficId: activeDraft.traffic.id,
      flightStart: stationSyncFlightRange.flightStart,
      flightEnd: stationSyncFlightRange.flightEnd,
      languages: resolveFlightLanguagesFromFlights(
        activeDraft.flights,
        stationSyncFlightRange.flightStart,
        stationSyncFlightRange.flightEnd,
      ),
      mediums: resolveFlightMediumsFromFlights(
        activeDraft.flights,
        stationSyncFlightRange.flightStart,
        stationSyncFlightRange.flightEnd,
      ),
    }, "manual_sync");
  }, [activeDraft, isStationAutoSyncing, openStationSyncSelectDialog, stationSyncFlightRange, toast]);

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
    if (!detailDraft) {
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
    if (!detailDraft) {
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

  function duplicateFlightDraft(flightId: number) {
    if (!detailDraft) {
      return;
    }
    const targetIndex = detailDraft.flights.findIndex((item) => item.id === flightId);
    if (targetIndex < 0) {
      return;
    }
    const nextTempId = tempRowIdRef.current;
    tempRowIdRef.current -= 1;
    const duplicatedFlight: TrafficFlight = {
      ...detailDraft.flights[targetIndex],
      id: nextTempId,
      dateCreated: null,
      dateUpdated: null,
    };
    const nextFlights = [...detailDraft.flights, duplicatedFlight];
    updateDraft((current) => {
      return {
        ...current,
        flights: nextFlights,
      };
    }, { applyAutoReadyToEmail: true });
    refreshEmailInstructionsFromFlights(nextFlights, { force: true });
  }

  function removeFlightDraft(flightId: number) {
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
    }), { applyAutoReadyToEmail: true });
    refreshEmailInstructionsFromFlights(remainingFlights, { force: true });
    if (currentDetail && nextSyncRange) {
      const remainingLanguages = resolveFlightLanguagesFromFlights(
        remainingFlights,
        nextSyncRange.flightStart,
        nextSyncRange.flightEnd,
      );
      const remainingMediums = resolveFlightMediumsFromFlights(
        remainingFlights,
        nextSyncRange.flightStart,
        nextSyncRange.flightEnd,
      );
      void syncStationsFromFlightRange({
        trafficId: currentDetail.traffic.id,
        flightStart: nextSyncRange.flightStart,
        flightEnd: nextSyncRange.flightEnd,
        languages: remainingLanguages,
        mediums: remainingMediums,
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
    if (flightId === null) {
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
    }, { applyAutoReadyToEmail: true });
    refreshEmailInstructionsFromFlights(
      currentDetail?.flights.filter((item) => item.id !== flightId) ?? [],
      { force: true },
    );
    if (currentDetail) {
      const remainingFlights = currentDetail.flights.filter((item) => item.id !== flightId);
      const nextSyncRange = resolveFlightRangeFromFlights(remainingFlights);
      if (nextSyncRange) {
        const remainingLanguages = resolveFlightLanguagesFromFlights(
          remainingFlights,
          nextSyncRange.flightStart,
          nextSyncRange.flightEnd,
        );
        const remainingMediums = resolveFlightMediumsFromFlights(
          remainingFlights,
          nextSyncRange.flightStart,
          nextSyncRange.flightEnd,
        );
        void syncStationsFromFlightRange({
          trafficId: currentDetail.traffic.id,
          flightStart: nextSyncRange.flightStart,
          flightEnd: nextSyncRange.flightEnd,
          languages: remainingLanguages,
          mediums: remainingMediums,
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
    const currentFlights = detailDraft?.flights ?? [];

    let nextFlightForSync: {
      trafficId: string;
      flightStart: string;
      flightEnd: string;
      languages: FlightLanguage[];
      mediums: string[];
      forceRefreshCandidates?: boolean;
    } | null = null;

    if (flightModalMode === "create") {
      const nextTempId = tempRowIdRef.current;
      tempRowIdRef.current -= 1;
      const nextFlight: TrafficFlight = {
        ...sanitizedFlightDraft,
        id: nextTempId,
      };
      const nextFlights = [...currentFlights, nextFlight];
      nextFlightForSync = {
        trafficId: nextFlight.trafficId,
        flightStart: nextFlight.flightStart,
        flightEnd: nextFlight.flightEnd,
        languages: [normalizeFlightLanguageValue(nextFlight.language)],
        mediums: [asString(nextFlight.medium).toUpperCase()],
        forceRefreshCandidates: true,
      };
      updateDraft((current) => ({
        ...current,
        flights: nextFlights,
      }), { applyAutoReadyToEmail: true });
      refreshEmailInstructionsFromFlights(nextFlights, { force: true });
    } else if (flightModalBaseline) {
      const shouldForceRefreshCandidates = (
        asString(flightModalBaseline.flightStart) !== asString(sanitizedFlightDraft.flightStart)
        || asString(flightModalBaseline.flightEnd) !== asString(sanitizedFlightDraft.flightEnd)
        || normalizeFlightLanguageValue(flightModalBaseline.language) !== normalizeFlightLanguageValue(sanitizedFlightDraft.language)
        || canonicalFlightMediumForCandidates(flightModalBaseline.medium) !== canonicalFlightMediumForCandidates(sanitizedFlightDraft.medium)
      );
      const nextFlights = currentFlights.map((item) => (
        item.id === flightModalBaseline.id ? { ...sanitizedFlightDraft, id: item.id } : item
      ));
      nextFlightForSync = {
        trafficId: sanitizedFlightDraft.trafficId,
        flightStart: sanitizedFlightDraft.flightStart,
        flightEnd: sanitizedFlightDraft.flightEnd,
        languages: [normalizeFlightLanguageValue(sanitizedFlightDraft.language)],
        mediums: [asString(sanitizedFlightDraft.medium).toUpperCase()],
        forceRefreshCandidates: shouldForceRefreshCandidates,
      };
      updateDraft((current) => ({
        ...current,
        flights: nextFlights,
      }), { applyAutoReadyToEmail: true });
      refreshEmailInstructionsFromFlights(nextFlights, { force: true });
    }

    setIsFlightModalOpen(false);
    resetFlightModalState();
    if (nextFlightForSync) {
      openStationSyncSelectDialog(nextFlightForSync, "flight_update");
    }
  }

  function revertFlightModalChanges() {
    if (!flightModalBaseline || !hasFlightModalChanges || isSaving || isFlightModalReadOnly) {
      return;
    }
    setFlightModalDraft({ ...flightModalBaseline });
    setFlightModalError(null);
    setFlightFileUrlError(null);
    setFlightScriptUrlError(null);
  }

  function handleResolveFlightStationSyncSelection(shouldSyncStations: boolean) {
    const pending = pendingFlightStationSync;
    const selectedEstNums = [...selectedFlightStationSyncEstNums];
    setIsFlightStationSyncSelectOpen(false);
    setPendingFlightStationSync(null);
    setFlightStationSyncCandidates(null);
    setSelectedFlightStationSyncEstNums([]);
    setIsFlightStationSyncCandidatesLoading(false);
    if (!pending || !shouldSyncStations || selectedEstNums.length === 0) {
      return;
    }
    setFlightStationSyncSelectionsByTrafficId((current) => ({
      ...current,
      [pending.trafficId]: normalizeEstNumList(selectedEstNums),
    }));
    void syncStationsFromFlightRange({
      ...pending,
      estNums: selectedEstNums,
    });
  }

  function toggleFlightStationSyncEstNum(estNum: number) {
    setSelectedFlightStationSyncEstNums((current) => {
      const normalized = Math.max(0, Math.trunc(asNumber(estNum, 0)));
      if (current.includes(normalized)) {
        return current.filter((item) => item !== normalized);
      }
      return [...current, normalized].sort((a, b) => a - b);
    });
  }

  function handleOpenFlightSyncSchedulePreview(estNum: number, note: string | null) {
    setFlightSyncPreviewEstnum({
      estnum: Math.max(0, Math.trunc(asNumber(estNum, 0))),
      name: String(estNum),
      hasSchedule: true,
      note: asNullableString(note),
    });
    setIsFlightSyncPreviewModalOpen(true);
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

  const addStationDraft = useCallback(() => {
    if (!detailDraft) {
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
  }, [detailDraft]);

  const editStationDraft = useCallback((stationId: number) => {
    if (!detailDraft) {
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
  }, [detailDraft]);

  const removeStationDraft = useCallback((stationId: number) => {
    updateDraft((current) => ({
      ...current,
      stations: current.stations.filter((item) => item.id !== stationId),
    }));
  }, [updateDraft]);

  function maybePromptConfirmTrafficStatus(nextStations: TrafficStation[]) {
    if (!detailDraft) {
      return;
    }
    const trafficStatus = asString(detailDraft.traffic.status).toLowerCase();
    if (trafficStatus === "confirmed") {
      return;
    }
    if (nextStations.length === 0) {
      return;
    }
    const allConfirmed = nextStations.every((station) => asString(station.confirmedStatus).toLowerCase() === "confirmed");
    if (!allConfirmed) {
      return;
    }
    setTrafficConfirmTargetTrafficId(detailDraft.traffic.id);
    setIsTrafficConfirmDialogOpen(true);
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

    let nextStations: TrafficStation[] | null = null;
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
      if (detailDraft) {
        nextStations = [...detailDraft.stations, nextStation];
      }
    } else if (stationModalBaseline) {
      const updatedStation = {
        ...stationModalDraft,
        id: stationModalBaseline.id,
        stationCode: asString(stationModalDraft.stationCode).toUpperCase(),
      };
      updateDraft((current) => ({
        ...current,
        stations: current.stations.map((item) => item.id === stationModalBaseline.id
          ? updatedStation
          : item),
        email: current.email
          ? {
              ...current.email,
              toEmails: mergeUniqueEmails(current.email.toEmails, stationContactEmails),
            }
          : current.email,
      }));
      if (detailDraft) {
        nextStations = detailDraft.stations.map((item) => item.id === stationModalBaseline.id ? updatedStation : item);
      }
    }

    setIsStationModalOpen(false);
    resetStationModalState();
    if (nextStations) {
      maybePromptConfirmTrafficStatus(nextStations);
    }
  }

  function revertStationModalChanges() {
    if (!stationModalBaseline || !hasStationModalChanges || isSaving) {
      return;
    }
    setStationModalDraft({ ...stationModalBaseline });
    setStationModalError(null);
    setStationLookupError(null);
    setStationLookupName(null);
    setStationLookupQueryCode(asString(stationModalBaseline.stationCode).toUpperCase());
    setStationLookupCacheStatus(null);
    setStationLookupRefreshToken(0);
    setIsStationLookupLoading(false);
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
      }, { applyAutoReadyToEmail: true });
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
      const cachedContactsSnapshot = isRecord(cachedData?.contactsSnapshot) ? cachedData.contactsSnapshot : null;
      const hasCachedContactsSnapshot =
        cachedData !== null
        && Object.prototype.hasOwnProperty.call(cachedData, "contactsSnapshot")
        && (cachedData.contactsSnapshot === null || isRecord(cachedData.contactsSnapshot));
      const forceNetworkFetch = stationLookupRefreshToken > 0;
      const shouldFetchFromNetwork =
        forceNetworkFetch ||
        !hasCachedContactsSnapshot ||
        shouldFetchNetwork("cache-first", cacheSnapshot);
      if (cachedData) {
        const cachedMeta = normalizeStationLookupMeta(cachedData, lookupCode);
        if (cachedMeta) {
          setStationLookupMetaByCode((current) => ({
            ...current,
            [cachedMeta.code]: cachedMeta,
          }));
        }
        applyStationLookupData({
          stationCode: lookupCode,
          stationName: asNullableString(cachedData.name),
          deliveryMethodName: asString(cachedData.deliveryMethod),
          contactsSnapshot: cachedContactsSnapshot,
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
        const stationMediaType = asString(stationRow.mediaType).toUpperCase();
        const stationLanguage = asString(stationRow.language);
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
            mediaType: stationMediaType,
            language: stationLanguage,
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
    const forceNetworkFetch = stationMetaRefreshToken > 0;
    const uniqueCodes = stationMetaCodesKey
      ? stationMetaCodesKey.split(",").filter(Boolean)
      : [];
    if (!uniqueCodes.length) {
      setStationLookupMetaByCode({});
      if (forceNetworkFetch) {
        setStationMetaRefreshToken(0);
      }
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

    const missingCodes = forceNetworkFetch
      ? uniqueCodes
      : uniqueCodes.filter((stationCode) => !isStationLookupMetaComplete(localMeta[stationCode]));
    if (missingCodes.length === 0) {
      if (forceNetworkFetch) {
        setStationMetaRefreshToken(0);
      }
      return;
    }
    if (!isOnline) {
      if (forceNetworkFetch) {
        setStationMetaRefreshToken(0);
      }
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
          const cacheSnapshot = readBrowserCacheSnapshot<unknown>(buildStationLookupCacheKey(normalizedMeta.code));
          const cachedData = isRecord(cacheSnapshot?.data) ? cacheSnapshot.data : null;
          writeBrowserCache(
            buildStationLookupCacheKey(normalizedMeta.code),
            {
              ...(cachedData ?? {}),
              code: normalizedMeta.code,
              name: normalizedMeta.name,
              mediaType: normalizedMeta.mediaType,
              language: normalizedMeta.language,
              deliveryMethod: normalizedMeta.deliveryMethod?.name || asString(cachedData?.deliveryMethod),
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
      } finally {
        if (forceNetworkFetch && stationMetaRequestIdRef.current === requestId) {
          setStationMetaRefreshToken(0);
        }
      }
    }

    void loadStationMeta();
  }, [isOnline, requestHeaders, requestJson, stationMetaCodesKey, stationMetaRefreshToken]);

  const selectedDeliveryStationMeta = useMemo(() => {
    const code = asString(selectedDeliveryStationCode).toUpperCase();
    if (!code) {
      return null;
    }
    return stationLookupMetaByCode[code] ?? null;
  }, [selectedDeliveryStationCode, stationLookupMetaByCode]);
  const selectedStationLookupMeta = useMemo(() => {
    if (!normalizedStationLookupCode) {
      return null;
    }
    return stationLookupMetaByCode[normalizedStationLookupCode] ?? null;
  }, [normalizedStationLookupCode, stationLookupMetaByCode]);

  useEffect(() => {
    stationLookupMetaByCodeRef.current = stationLookupMetaByCode;
  }, [stationLookupMetaByCode]);

  useEffect(() => () => {
    if (syncMissingStationsResolverRef.current) {
      syncMissingStationsResolverRef.current([]);
      syncMissingStationsResolverRef.current = null;
    }
  }, []);

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

  const handleOpenScheduleTimelineModal = useCallback(() => {
    if (!timelineAccountCode || !stationSyncFlightRange) {
      return;
    }
    setIsScheduleTimelineModalOpen(true);
  }, [stationSyncFlightRange, timelineAccountCode]);

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

  const handleCopyEmailHtml = useCallback(async () => {
    const html = activeEmailPreviewHtml;
    if (!html) {
      toast.info("No email HTML available");
      return;
    }
    try {
      if (!navigator?.clipboard) {
        throw new Error("Clipboard API unavailable");
      }
      if (typeof ClipboardItem !== "undefined" && typeof navigator.clipboard.write === "function") {
        const htmlBlob = new Blob([html], { type: "text/html" });
        const textBlob = new Blob([html], { type: "text/plain" });
        await navigator.clipboard.write([
          new ClipboardItem({
            "text/html": htmlBlob,
            "text/plain": textBlob,
          }),
        ]);
      } else {
        await navigator.clipboard.writeText(html);
      }
      toast.success("Copied email HTML", "Email HTML is ready to paste into Gmail.");
    } catch {
      toast.error("Unable to copy HTML", "Clipboard access is unavailable in this browser.");
    }
  }, [activeEmailPreviewHtml, toast]);

  const markTrafficStatusSentById = useCallback(async (trafficId: string) => {
    if (!trafficId || isLocalTrafficId(trafficId)) {
      return false;
    }

    setIsMarkingTrafficSent(true);
    setError(null);
    try {
      const nextStatus: TrafficStatus = "sent";
      const payload = await requestJson(`/api/tradsphere/v1/traffic?id=${encodeURIComponent(trafficId)}`, {
        method: "PUT",
        headers: requestHeaders,
        body: { status: nextStatus },
        successToast: false,
      });
      const data = unwrapData(payload);
      const persistedStatus = asString(isRecord(data) ? data.status : "").toLowerCase() || nextStatus;
      applyTrafficStatusToLoadedState(trafficId, persistedStatus as TrafficStatus);
      return true;
    } catch (statusError) {
      const message = getTrafficErrorMessage(statusError, "Failed to update traffic status.");
      setError(message);
      toast.error("Status update failed", message);
      return false;
    } finally {
      setIsMarkingTrafficSent(false);
    }
  }, [applyTrafficStatusToLoadedState, requestHeaders, requestJson, toast]);

  const sendTrafficEmailForDetail = useCallback(async (
    draftDetail: TrafficDetail,
    trafficId: string,
    options?: { markSentAfterSend?: boolean },
  ) => {
    if (!canEditTradsphere || isSaving || isSendingEmail || isMarkingTrafficSent || isEmailLocked) {
      return;
    }
    if (!draftDetail.email || !activeEmailWorkspace || !trafficId) {
      toast.info("Select a traffic record", "Open a traffic record with an email draft before sending.");
      return;
    }
    const toEmails = asStringArray(draftDetail.email.toEmails).map((item) => item.toLowerCase());
    const ccEmails = asStringArray(draftDetail.email.ccEmails).map((item) => item.toLowerCase());
    const bccEmails = asStringArray(draftDetail.email.bccEmails).map((item) => item.toLowerCase());
    const subject = asString(draftDetail.email.subject);
    if (toEmails.length === 0) {
      toast.error("Missing recipients", "Add at least one To email before sending.");
      return;
    }
    if (!subject) {
      toast.error("Missing subject", "Email subject is required before sending.");
      return;
    }

    const accountCode = asString(draftDetail.traffic.accountCode).toUpperCase();
    const accountLabel = accountNameByCode[accountCode] || accountCode || "Tradsphere";
    const body = persistTrafficEmailBody({
      workspace: {
        bodyContent: activeEmailWorkspace.bodyContent,
        instructionsContent: activeEmailWorkspace.instructionsContent,
        downloadLinks: activeEmailWorkspace.downloadLinks,
      },
      subject,
      accountLabel,
      campaignLabel: asString(draftDetail.traffic.campaign),
      statusLabel: formatStatusOptionLabel(draftDetail.traffic.status),
    });
    if (!asString(body)) {
      toast.error("Missing email body", "Email body is required before sending.");
      return;
    }

    setIsSendingEmail(true);
    const shouldMarkSent = Boolean(options?.markSentAfterSend)
      && asString(draftDetail.traffic.status).toLowerCase() !== "sent";
    if (shouldMarkSent) {
      setIsMarkingTrafficSent(true);
    }
    setError(null);
    try {
      const responsePayload = await requestJson(
        `/api/tradsphere/v1/traffic/email/send?trafficId=${encodeURIComponent(trafficId)}`,
        {
          method: "POST",
          headers: requestHeaders,
          body: {
            toEmails,
            ccEmails,
            bccEmails,
            subject,
            body,
            markSentAfterSend: shouldMarkSent,
          },
          successToast: false,
        },
      );
      const responseData = unwrapData(responsePayload);
      const nextDetail = normalizeTrafficDetail(
        isRecord(responseData) && isRecord(responseData.detail)
          ? responseData.detail
          : responseData,
      );
      if (nextDetail) {
        const computedDetail = computeSummary(nextDetail);
        const nextSummary = buildListSummaryFromDetail(computedDetail, stationLookupMetaByCode);
        let nextTrafficList = trafficList;
        setTrafficList((current) => {
          const next = upsertTrafficSummaryInList(current, nextSummary, trafficId, trafficId, false);
          nextTrafficList = next;
          return next;
        });
        if (activeAccountCode) {
          writeBrowserCache(buildTrafficListCacheKey(activeAccountCode), nextTrafficList, TRAFFIC_LIST_CACHE_TTL_MS, {
            source: "network",
            fetchedAt: Date.now(),
          });
        }
        setDetailBaseline((current) => {
          if (!current || asString(current.traffic.id) !== trafficId) {
            return current;
          }
          return cloneDetail(computedDetail);
        });
        setDetailDraft((current) => {
          if (!current || asString(current.traffic.id) !== trafficId) {
            return current;
          }
          return cloneDetail(computedDetail);
        });
        setDraftSessionsByTrafficId((current) => {
          const session = current[trafficId];
          if (!session) {
            return current;
          }
          return {
            ...current,
            [trafficId]: {
              baseline: session.baseline
                ? (cloneDetail(computedDetail) as TrafficDetail)
                : session.baseline,
              draft: cloneDetail(computedDetail) as TrafficDetail,
            },
          };
        });
      } else {
        const nextEmail = normalizeTrafficEmail(
          isRecord(responseData) && isRecord(responseData.email)
            ? responseData.email
            : responseData,
        );
        if (!nextEmail) {
          throw new Error("Invalid email send response.");
        }

        setDetailBaseline((current) => {
          if (!current || asString(current.traffic.id) !== trafficId) {
            return current;
          }
          return computeSummary({
            ...current,
            email: nextEmail,
          });
        });
        setDetailDraft((current) => {
          if (!current || asString(current.traffic.id) !== trafficId) {
            return current;
          }
          return computeSummary({
            ...current,
            email: nextEmail,
          });
        });
        setDraftSessionsByTrafficId((current) => {
          const session = current[trafficId];
          if (!session) {
            return current;
          }
          return {
            ...current,
            [trafficId]: {
              baseline: session.baseline
                ? computeSummary({
                    ...session.baseline,
                    email: nextEmail,
                  })
                : session.baseline,
              draft: computeSummary({
                ...session.draft,
                email: nextEmail,
              }),
            },
          };
        });
        if (shouldMarkSent) {
          const markedSent = await markTrafficStatusSentById(trafficId);
          toast.success(
            "Email sent",
            markedSent
              ? "Traffic email was sent and the status was marked as Sent."
              : "Traffic email was sent successfully.",
          );
          setIsEmailPreviewOpen(false);
          return;
        }
      }
      toast.success(
        "Email sent",
        shouldMarkSent
          ? "Traffic email was sent and the status was marked as Sent."
          : "Traffic email was sent successfully.",
      );
      setIsEmailPreviewOpen(false);
    } catch (sendError) {
      const message = getTrafficErrorMessage(sendError, "Failed to send email.");
      const attemptAt = new Date().toISOString();
      const applyFailedSendState = (current: TrafficDetail | null): TrafficDetail | null => {
        if (!current || asString(current.traffic.id) !== trafficId || !current.email) {
          return current;
        }
        return computeSummary({
          ...current,
          email: {
            ...current.email,
            sentStatus: "failed",
            sentAt: null,
            lastSendAttemptAt: attemptAt,
            lastSendError: message,
          },
        });
      };
      setDetailBaseline((current) => applyFailedSendState(current));
      setDetailDraft((current) => applyFailedSendState(current));
      setError(message);
      toast.error("Send failed", message);
    } finally {
      setIsSendingEmail(false);
      if (shouldMarkSent) {
        setIsMarkingTrafficSent(false);
      }
    }
  }, [
    activeEmailWorkspace,
    activeAccountCode,
    accountNameByCode,
    canEditTradsphere,
    buildListSummaryFromDetail,
    isEmailLocked,
    isSaving,
    isSendingEmail,
    isMarkingTrafficSent,
    markTrafficStatusSentById,
    stationLookupMetaByCode,
    requestHeaders,
    requestJson,
    trafficList,
    toast,
    upsertTrafficSummaryInList,
  ]);

  const handleSendTrafficEmail = useCallback(() => {
    if (!canEditTradsphere || isSaving || isSendingEmail || isMarkingTrafficSent || isEmailLocked) {
      return;
    }
    if (!activeDraft?.email || !activeEmailWorkspace || !activeTrafficId) {
      toast.info("Select a traffic record", "Open a traffic record with an email draft before sending.");
      return;
    }
    setIsSendEmailConfirmationOpen(true);
  }, [
    activeDraft,
    activeEmailWorkspace,
    activeTrafficId,
    canEditTradsphere,
    isEmailLocked,
    isMarkingTrafficSent,
    isSaving,
    isSendingEmail,
    sendTrafficEmailForDetail,
    toast,
  ]);

  const handleConfirmSendTrafficEmail = useCallback(async (markSentAfterSend: boolean) => {
    setIsSendEmailConfirmationOpen(false);
    if (!detailDraft || !activeDraft?.email || !activeEmailWorkspace || !activeTrafficId) {
      return;
    }
    if (activeTrafficNeedsSaveBeforeSending) {
      const saved = await saveTrafficDetailForEmail(detailDraft, detailBaseline);
      await sendTrafficEmailForDetail(saved.detail, saved.trafficId, { markSentAfterSend });
      return;
    }
    await sendTrafficEmailForDetail(detailDraft, activeTrafficId, { markSentAfterSend });
  }, [
    activeDraft?.email,
    activeEmailWorkspace,
    activeTrafficNeedsSaveBeforeSending,
    activeTrafficId,
    detailBaseline,
    detailDraft,
    saveTrafficDetailForEmail,
    sendTrafficEmailForDetail,
  ]);

  const handleOpenTestEmailModal = useCallback(() => {
    if (!canOpenTrafficTestEmailModal) {
      return;
    }
    setTestEmailTo([DEFAULT_TEST_EMAIL_RECIPIENT]);
    setIsTestEmailModalOpen(true);
  }, [canOpenTrafficTestEmailModal]);

  const handleSendTrafficTestEmail = useCallback(async () => {
    if (!canSendTrafficTestEmail || !activeDraft?.email || !activeEmailWorkspace || !activeTrafficId) {
      return;
    }

    const toEmail = asString(testEmailTo[0]).trim().toLowerCase();
    if (!isLikelyEmailAddress(toEmail)) {
      toast.error("Invalid test email", "Enter a valid email address for the test recipient.");
      return;
    }

    const subject = asString(activeDraft.email.subject);
    if (!subject) {
      toast.error("Missing subject", "Email subject is required before sending the test copy.");
      return;
    }

    const accountCode = asString(activeDraft.traffic.accountCode).toUpperCase();
    const accountLabel = accountNameByCode[accountCode] || accountCode || "Tradsphere";
    const body = persistTrafficEmailBody({
      workspace: {
        bodyContent: activeEmailWorkspace.bodyContent,
        instructionsContent: activeEmailWorkspace.instructionsContent,
        downloadLinks: activeEmailWorkspace.downloadLinks,
      },
      subject,
      accountLabel,
      campaignLabel: asString(activeDraft.traffic.campaign),
      statusLabel: formatStatusOptionLabel(activeDraft.traffic.status),
    });
    if (!asString(body)) {
      toast.error("Missing email body", "Email body is required before sending the test copy.");
      return;
    }

    setIsSendingTestEmail(true);
    setError(null);
    try {
      await requestJson(
        `/api/tradsphere/v1/traffic/email/send-test?trafficId=${encodeURIComponent(activeTrafficId)}`,
        {
          method: "POST",
          headers: requestHeaders,
          body: {
            toEmail,
            subject,
            body,
          },
          successToast: false,
        },
      );
      setIsTestEmailModalOpen(false);
      toast.success("Test email sent", `A test copy was sent to ${toEmail}.`);
    } catch (sendError) {
      const message = getTrafficErrorMessage(sendError, "Failed to send test email.");
      setError(message);
      toast.error("Test send failed", message);
    } finally {
      setIsSendingTestEmail(false);
    }
  }, [
    accountNameByCode,
    activeDraft,
    activeEmailWorkspace,
    activeTrafficId,
    canSendTrafficTestEmail,
    requestHeaders,
    requestJson,
    testEmailTo,
    toast,
  ]);

  const handleOpenEmailPreviewModal = useCallback(() => {
    if (!activeDraft || !activeDraft.email || !activeEmailWorkspace) {
      return;
    }
    setIsTestEmailModalOpen(false);
    setIsEmailPreviewOpen(true);
  }, [activeDraft, activeEmailWorkspace]);

  const handleUnlockTraffic = useCallback(async () => {
    const trafficId = asString(activeTrafficId);
    if (
      !trafficId
      || !activeDraft
      || !activeDraft.email
      || !activeEmailWorkspace
      || !isEmailLocked
      || isLocalTrafficId(trafficId)
    ) {
      return;
    }

    setIsUnlockingTraffic(true);
    setError(null);
    try {
      const payload = await requestJson("/api/tradsphere/v1/traffic/bulk-save", {
        method: "POST",
        headers: requestHeaders,
        body: {
          trafficId,
          traffic: {
            accountCode: activeDraft.traffic.accountCode,
            campaign: activeDraft.traffic.campaign,
            status: "ready",
            note: activeDraft.traffic.note,
          },
          updateTraffic: true,
          flightCreates: [],
          flightUpdates: [],
          flightDeletes: [],
          stationCreates: [],
          stationUpdates: [],
          stationDeletes: [],
          upsertEmail: true,
          email: {
            toEmails: activeDraft.email.toEmails,
            ccEmails: activeDraft.email.ccEmails,
            bccEmails: activeDraft.email.bccEmails,
            subject: activeDraft.email.subject,
            body: activeDraft.email.body,
            sentStatus: "ready",
          },
        },
        successToast: false,
      });
      const data = unwrapData(payload);
      const nextDetail = normalizeTrafficDetail({
        data: isRecord(data) && isRecord(data.detail) ? data.detail : data,
      });
      if (!nextDetail) {
        throw new Error("Invalid unlock response.");
      }
      const computedDetail = computeSummary(nextDetail);
      setDetailBaseline((current) => {
        if (!current || asString(current.traffic.id) !== trafficId) {
          return current;
        }
        return computedDetail;
      });
      setDetailDraft((current) => {
        if (!current || asString(current.traffic.id) !== trafficId) {
          return current;
        }
        return computedDetail;
      });
      setDraftSessionsByTrafficId((current) => {
        const session = current[trafficId];
        if (!session) {
          return current;
        }
        return {
          ...current,
          [trafficId]: {
            baseline: session.baseline
              ? computedDetail
              : session.baseline,
            draft: computedDetail,
          },
        };
      });
      updateTrafficCardSummary(trafficId, { status: "ready" });
      toast.success("Traffic unlocked", "Traffic email and traffic status have been set back to Ready.");
    } catch (statusError) {
      const message = getTrafficErrorMessage(statusError, "Failed to unlock traffic.");
      setError(message);
      toast.error("Unlock failed", message);
    } finally {
      setIsUnlockingTraffic(false);
    }
  }, [activeDraft, activeEmailWorkspace, activeTrafficId, computeSummary, isEmailLocked, requestHeaders, requestJson, toast, updateTrafficCardSummary]);

  const handlePromptConfirmTrafficStatus = useCallback((shouldConfirm: boolean) => {
    const trafficId = asString(trafficConfirmTargetTrafficId);
    setIsTrafficConfirmDialogOpen(false);
    setTrafficConfirmTargetTrafficId(null);
    if (!shouldConfirm || !trafficId || !detailDraft || asString(detailDraft.traffic.id) !== trafficId) {
      return;
    }
    updateDraft((current) => ({
      ...current,
      traffic: {
        ...current.traffic,
        status: "confirmed",
      },
    }));
    updateTrafficCardSummary(trafficId, { status: "confirmed" });
    toast.info(
      "Traffic status prepared",
      "Traffic status has been set to Confirmed in the draft. Save to lock the record.",
    );
  }, [detailDraft, toast, trafficConfirmTargetTrafficId, updateDraft, updateTrafficCardSummary]);

  const normalizedAppliedTrafficSearch = useMemo(
    () => normalizeSearchKeyword(appliedTrafficSearch),
    [appliedTrafficSearch],
  );
  const filteredTrafficList = useMemo(
    () => trafficList.filter((item) => trafficMatchesSearch(item, normalizedAppliedTrafficSearch)),
    [trafficList, normalizedAppliedTrafficSearch],
  );

  const applyTrafficSearchKeyword = useCallback((rawValue: string) => {
    const normalized = asString(rawValue);
    setDraftTrafficSearch(normalized);
    setAppliedTrafficSearch(normalized);
  }, []);

  const applyTrafficSearchFromDraft = useCallback(() => {
    applyTrafficSearchKeyword(draftTrafficSearch);
  }, [applyTrafficSearchKeyword, draftTrafficSearch]);

  function handleTrafficSearchInputKeyDown(event: ReactKeyboardEvent<HTMLInputElement>) {
    if (event.key === "Enter") {
      event.preventDefault();
      applyTrafficSearchFromDraft();
      return;
    }
    if (event.key === "Tab") {
      applyTrafficSearchFromDraft();
    }
  }

  function handleTrafficSearchInputChange(nextValue: string) {
    setDraftTrafficSearch(nextValue);
    if (!asString(nextValue)) {
      applyTrafficSearchKeyword("");
    }
  }

  function handleClearTrafficSearch() {
    applyTrafficSearchKeyword("");
  }

  useEffect(() => {
    const normalizedDraft = asString(draftTrafficSearch);
    if (!normalizedDraft) {
      return;
    }
    if (normalizeSearchKeyword(normalizedDraft) === normalizeSearchKeyword(appliedTrafficSearch)) {
      return;
    }
    const timer = window.setTimeout(() => {
      applyTrafficSearchKeyword(normalizedDraft);
    }, 1000);
    return () => {
      window.clearTimeout(timer);
    };
  }, [appliedTrafficSearch, applyTrafficSearchKeyword, draftTrafficSearch]);

  const isGridActionOverlayVisible = isSaving || isSendingEmail;
  const sectionOverlayMessage = isSaving
    ? "Saving traffic changes..."
    : isSendingEmail
      ? "Sending email..."
      : isSelectorRefreshOverlayVisible
        ? "Refreshing traffic data..."
        : isRefreshingDetail
          ? "Refreshing traffic detail..."
          : isLoadingDetail
            ? "Loading traffic detail..."
            : "Loading...";
  const detailOverlayMessage = isRefreshingDetail
    ? "Refreshing traffic detail..."
    : "Loading traffic detail...";
  const loadingContract = resolveSharedLoadingContract(
    {
      pageInitializing: !hasHydratedPageState,
      pageRefreshing: isLoadActionOverlayVisible,
      cacheChipRefreshing: isChipRefreshOverlayVisible,
      sectionLoading: isGridActionOverlayVisible || isSelectorRefreshOverlayVisible,
    },
    {
      pageInitializing: "Preparing traffic workspace...",
      pageRefreshing: "Loading traffic data...",
      cacheChipRefreshing: "Loading latest traffic data...",
      sectionLoading: sectionOverlayMessage,
    },
  );

  return (
    <AppPageLayout
      className="pb-5"
      pageMessages={<PageMessageStack messages={pageMessages} />}
      banner={(
        <PageBanner
          eyebrow="TradSphere"
          title="Traffic"
          description="Create and manage account-centered traffic records, flights, stations, and email drafts."
        />
      )}
      footer={(cacheStatus || isDeletingTraffic) ? (
        <PageCacheFooter
          text={cacheStatusText}
          onRefresh={() => {
            if (hasAnyUnsavedChanges) {
              setPendingAction({ type: "refresh" });
              setIsUnsavedDialogOpen(true);
              return;
            }
            void handleRefreshFromChip();
          }}
          disabled={
            !activeAccountCode ||
            !isOnline ||
            isSaving ||
            isDeletingTraffic ||
            isLoadingAccountTraffic ||
            isRefreshingSelections ||
            isChipRefreshOverlayVisible ||
            isDetailBusy
          }
          refreshing={
            isDeletingTraffic ||
            isRefreshingSelections ||
            isRefreshingAccountTraffic ||
            isChipRefreshOverlayVisible ||
            isDetailBusy
          }
          refreshLabel="Refresh traffic data"
          tooltipText={
            isDeletingTraffic
              ? "Delete/archive is in progress."
              : hasAnyUnsavedChanges
                ? "Save or revert changes before refreshing traffic data."
                : isOnline
                  ? "Click to refresh traffic data"
                  : "Offline. Reconnect to refresh traffic data."
          }
          containerClassName="w-full"
        />
      ) : null}
    >

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
          loadButtonLabel={selectedAccountIsLoaded ? "Refresh" : "Load"}
          onAccountChange={handleAccountChange}
          onLoad={() => {
            const targetAccountCode = asString(selectedAccountCode).toUpperCase();
            const isSwitchingAccount = Boolean(activeAccountCode && targetAccountCode && targetAccountCode !== activeAccountCode);
            if (hasAnyUnsavedChanges && isSwitchingAccount) {
              setPendingAction({ type: "account", accountCode: targetAccountCode });
              setIsUnsavedDialogOpen(true);
              return;
            }
            if (hasAnyUnsavedChanges) {
              setPendingAction({ type: "refresh" });
              setIsUnsavedDialogOpen(true);
              return;
            }
            if (targetAccountCode && targetAccountCode === activeAccountCode) {
              void handleRefreshFromSelector();
              return;
            }
            void handleLoadFromSelector();
          }}
        />
      </SectionCard>

      <div className="relative grid gap-4 xl:grid-cols-[minmax(18rem,26rem)_minmax(0,1fr)]">
        <SectionCard
          title="Traffic Records"
          description={(
            <span className="flex flex-wrap items-center gap-2">
              {hasAnyUnsavedChanges ? (
                <span className="inline-flex rounded-full border border-amber-200 bg-amber-50 px-2 py-0.5 text-[11px] font-semibold text-amber-800">
                  Unsaved changes ({unsavedTrafficCount})
                </span>
              ) : null}
              <span>
                {normalizedAppliedTrafficSearch
                  ? `${filteredTrafficList.length} of ${trafficList.length} record(s) for ${activeAccountCode || "selected account"}`
                  : `${trafficList.length} record(s) for ${activeAccountCode || "selected account"}`}
              </span>
            </span>
          )}
          actions={(
            <ActionIconButton
              icon={<Plus />}
              tooltip="New Traffic"
              aria-label="New Traffic"
              title="New Traffic"
              onClick={() => void handleCreateTraffic()}
              disabled={!activeAccountCode || !canEditTradsphere || isLoadingAccountTraffic || isSaving}
              className="!h-7 !w-7 !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-4 [&_svg]:!w-4 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110"
            />
          )}
          contentClassName="space-y-3"
        >
          <div className="relative">
            <Input
              value={draftTrafficSearch}
              onChange={(event) => handleTrafficSearchInputChange(event.target.value)}
              onBlur={applyTrafficSearchFromDraft}
              onKeyDown={handleTrafficSearchInputKeyDown}
              placeholder="Filter by ISCI, campaign, station, or email"
              className="pr-9 transition !outline-none ![box-shadow:none] !focus:outline-none !focus:ring-0 !focus:ring-offset-0 !focus:border-slate-300 !focus:shadow-none !focus:[box-shadow:none] !focus-visible:outline-none !focus-visible:ring-0 !focus-visible:ring-offset-0 !focus-visible:border-slate-300 !focus-visible:shadow-none !focus-visible:[box-shadow:none]"
            />
            {draftTrafficSearch ? (
              <TooltipTarget text="Clear traffic search">
                <button
                  type="button"
                  onClick={handleClearTrafficSearch}
                  className="absolute right-2 top-1/2 inline-flex size-6 -translate-y-1/2 items-center justify-center rounded-full text-slate-500 transition hover:bg-slate-100 hover:text-slate-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-300"
                  aria-label="Clear traffic search"
                >
                  <X className="size-3.5" />
                </button>
              </TooltipTarget>
            ) : null}
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
            ) : filteredTrafficList.length === 0 ? (
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-600">
                No traffic records match your keyword filter.
              </div>
            ) : (
              filteredTrafficList.map((item) => {
                const active = item.id === selectedTrafficId;
                const activeDraftMatchesRow = active && activeDraft?.traffic.id === item.id;
                const displayCampaign = activeDraftMatchesRow ? activeDraft.traffic.campaign : item.campaign;
                const displayStatus = activeDraftMatchesRow ? activeDraft.traffic.status : item.status;
                return (
                  <TrafficListItemCard
                    key={item.id}
                    item={item}
                    active={active}
                    displayCampaign={displayCampaign}
                    displayStatus={displayStatus}
                    isDraft={asString(item.status).toLowerCase() === "draft"}
                    itemHasUnsavedChanges={unsavedTrafficIds.has(item.id)}
                    isDeletingCard={deletingTrafficIdSet.has(item.id)}
                    isSelectionLocked={active && isDetailBusy}
                    canEditTradsphere={canEditTradsphere}
                    isSaving={isSaving}
                    isLoadingAccountTraffic={isLoadingAccountTraffic}
                    isLoadingDetail={isDetailBusy}
                    isDuplicatingTraffic={duplicatingTrafficId === item.id}
                    onSelectTraffic={handleSelectTraffic}
                    onDuplicateTraffic={handleDuplicateTraffic}
                    onOpenTrafficRemovalDialog={openTrafficRemovalDialog}
                  />
                );
              })
            )}
          </div>
        </SectionCard>

        <div className="relative space-y-4">
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
          <TrafficWorkspaceModeTabs activeTab={activeWorkspaceTab} onChangeTab={setActiveWorkspaceTab} />

          {activeWorkspaceTab === "workflow" ? (
            <div className="relative space-y-4">
              <div className="space-y-4">
                <SectionCard
                  id="traffic-workspace-panel-workflow"
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
                        openTrafficRemovalDialog(selectedTrafficId, "archive");
                      }}
                      disabled={!selectedTrafficId || !canEditTradsphere || isSaving || isDeletingSelectedTraffic || hasUnsavedChanges || isTrafficEditingLocked}
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
                      {trafficLockBannerKind ? (
                        <TrafficLockBanner
                          kind={trafficLockBannerKind}
                          isUnlocking={isUnlockingTraffic}
                          disabled={!canEditTradsphere || isSaving || isUnlockingTraffic}
                          onUnlock={() => {
                            void handleUnlockTraffic();
                          }}
                        />
                      ) : null}

                      <div className="grid gap-3 sm:grid-cols-2">
                        <label className="space-y-1 text-sm">
                          <span className="text-slate-600">Campaign</span>
                          <Input
                            value={activeDraft.traffic.campaign}
                            disabled={!canEditTradsphere || isSaving || isTrafficEditingLocked}
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
                            disabled={!canEditTradsphere || isSaving || isConfirmedSavedLocked}
                          />
                        </label>
                      </div>

                      <label className="space-y-1 text-sm">
                        <span className="text-slate-600">Note</span>
                        <Textarea
                          value={activeDraft.traffic.note || ""}
                          className="min-h-[88px]"
                          disabled={!canEditTradsphere || isSaving || isTrafficEditingLocked}
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

                <TrafficFlightsSection
                  activeAccountCode={activeAccountCode}
                  activeDraft={activeDraft}
                  canEditTradsphere={canEditTradsphere}
                  isSaving={isSaving}
                  isTrafficEditingLocked={isTrafficEditingLocked}
                  onAddFlight={addFlightDraft}
                  onEditFlight={editFlightDraft}
                  onDuplicateFlight={duplicateFlightDraft}
                  onRemoveFlight={removeFlightDraft}
                />

                <TrafficStationsSection
                  activeAccountCode={activeAccountCode}
                  activeDraft={activeDraft}
                  canEditTradsphere={canEditTradsphere}
                  isSaving={isSaving}
                  isTrafficEditingLocked={isTrafficEditingLocked}
                  isConfirmedLocked={isConfirmedLocked}
                  isStationAutoSyncing={isStationAutoSyncing}
                  isScheduleTimelineLoading={isScheduleTimelineLoading}
                  isLoadingAccountTraffic={isLoadingAccountTraffic}
                  isOnline={isOnline}
                  timelineAccountCode={timelineAccountCode}
                  stationSyncFlightRange={stationSyncFlightRange}
                  sortedStations={sortedStations}
                  stationLookupMetaByCode={stationLookupMetaByCode}
                  stationContactEmailsById={stationContactEmailsById}
                  stationDeliveryStatusRestoreMap={stationDeliveryStatusRestoreRef.current}
                  stationConfirmedStatusRestoreMap={stationConfirmedStatusRestoreRef.current}
                  onManualStationSync={handleManualStationSync}
                  onOpenScheduleTimeline={handleOpenScheduleTimelineModal}
                  onAddStation={addStationDraft}
                  onEditStation={editStationDraft}
                  onOpenDeliveryMethodDetail={handleOpenDeliveryMethodDetail}
                  onCopyStationContacts={handleCopyStationContacts}
                  onToggleStationDeliveryStatus={handleToggleStationDeliveryStatus}
                  onToggleStationConfirmedStatus={handleToggleStationConfirmedStatus}
                  onRemoveStation={removeStationDraft}
                />
              </div>
            </div>
          ) : null}

          {activeWorkspaceTab === "email" ? (
            <div className="relative">
              <TrafficEmailWorkspaceSection
                activeAccountCode={activeAccountCode}
                activeDraft={activeDraft}
                activeEmailWorkspace={activeEmailWorkspace}
                canEditTradsphere={canEditTradsphere}
                isSaving={isSaving}
                isEmailLocked={isEmailLocked}
                isSendingEmail={isSendingEmail}
                isMarkingTrafficSent={isMarkingTrafficSent}
                isUnlockingTraffic={isUnlockingTraffic}
                emailLockBannerKind={emailLockBannerKind}
                emailWorkspacePrimaryActionLabel={emailWorkspacePrimaryActionLabel}
                contactNameByEmail={contactNameByEmail}
                onOpenEmailPreviewModal={handleOpenEmailPreviewModal}
                onUnlockTraffic={() => {
                  void handleUnlockTraffic();
                }}
                onOpenDownloadLinkNoteModal={handleOpenDownloadLinkNoteModal}
                onCommitEmailWorkspace={commitEmailWorkspace}
                onUpdateDraft={updateDraft}
              />
            </div>
          ) : null}
            </>
          )}
          <SectionLoadingLayer
            active={isDetailBusy}
            message={detailOverlayMessage}
          />
          {isDeletingSelectedTraffic ? (
            <SectionLoadingOverlay message="Deleting traffic..." />
          ) : null}
        </div>

        {hasPendingAccountSelectionAfterLoad && !(isLoadActionOverlayVisible || isChipRefreshOverlayVisible || isSaving || isSendingEmail) ? (
          <div className="absolute inset-0 z-20 rounded-2xl bg-slate-900/55 backdrop-blur-[1.5px]">
            <div className="flex h-full w-full items-center justify-center p-4 sm:p-6">
              <button
                type="button"
                onClick={handleRestoreLoadedAccountSelection}
                className="rounded-full border border-slate-300/80 bg-slate-100/95 px-3 py-1.5 text-xs font-medium text-slate-800 shadow-md transition hover:bg-slate-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-300 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-900"
              >
                Account selection changed. Click to restore previous selection.
              </button>
            </div>
          </div>
        ) : null}

        <SectionLoadingLayer
          active={loadingContract.sectionOverlayActive}
          message={loadingContract.sectionOverlayMessage}
        />
      </div>

      {shouldShowSaveActions ? (
        <div className="flex w-full justify-end">
          <div className="flex flex-wrap items-center gap-2">
            {hasAnyUnsavedChanges ? (
                <Button
                  variant="outline"
                  onClick={() => handleDiscard()}
                  disabled={isSaving || isSendingEmail || isMarkingTrafficSent || isDeletingSelectedTraffic}
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
          <ModalHeaderRow
            actions={(
              <DialogClose asChild aria-label="Close schedule timeline modal">
                <ModalCloseButton icon={<X className="size-4" />} />
              </DialogClose>
            )}
          >
            <DialogHeader className="space-y-1 text-left">
              <DialogTitle>Schedule Timeline</DialogTitle>
              <DialogDescription>
                View account schedule timeline table with cache-first loading.
              </DialogDescription>
            </DialogHeader>
          </ModalHeaderRow>
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

      <Dialog
        open={isEmailPreviewOpen}
        onOpenChange={(open) => {
          setIsEmailPreviewOpen(open);
          if (!open) {
            setIsTestEmailModalOpen(false);
          }
        }}
      >
        <DialogContent className="!h-[92vh] !max-h-[92vh] !w-[min(96vw,1160px)] !max-w-[1160px] overflow-hidden p-0">
          <DialogHeader className="border-b border-slate-200 px-5 py-4 pr-12">
            <div className="flex items-center justify-between gap-2">
              <DialogTitle>Email Preview</DialogTitle>
              <div className="flex items-center gap-2">
                <ActionIconButton
                  icon={<Copy />}
                  tooltip="Copy email HTML"
                  aria-label="Copy email HTML"
                  title="Copy email HTML"
                  onClick={() => {
                    void handleCopyEmailHtml();
                  }}
                  disabled={!activeEmailPreviewHtml}
                  className="!h-7 !w-7 !p-0 hover:!scale-105 focus-visible:!scale-105 [&_svg]:!h-4 [&_svg]:!w-4 [&_svg]:transition-transform [&_svg]:duration-150 hover:[&_svg]:scale-110 focus-visible:[&_svg]:scale-110"
                />
                <Button
                  variant="secondary"
                  onClick={() => {
                    handleOpenTestEmailModal();
                  }}
                  disabled={!canOpenTrafficTestEmailModal}
                >
                  {isSendingTestEmail ? (
                    <>
                      <Loader2 className="size-4 animate-spin" />
                      Sending Test...
                    </>
                  ) : (
                    "Send Test"
                  )}
                </Button>
                {!isEmailLocked ? (
                  !canSendTrafficEmail || isSendingTestEmail ? (
                    <TooltipTarget text={sendTrafficEmailDisabledReason}>
                      <span className="inline-flex">
                        <Button
                          onClick={() => {
                            void handleSendTrafficEmail();
                          }}
                          disabled
                        >
                          {isSendingEmail ? (
                            <>
                              <Loader2 className="size-4 animate-spin" />
                              Sending...
                            </>
                          ) : (
                            <>
                              <Send className="size-4" />
                              Send
                            </>
                          )}
                        </Button>
                      </span>
                    </TooltipTarget>
                  ) : (
                    <Button
                      onClick={() => {
                        void handleSendTrafficEmail();
                      }}
                    >
                      {isSendingEmail ? (
                        <>
                          <Loader2 className="size-4 animate-spin" />
                          Sending...
                        </>
                      ) : (
                        <>
                          <Send className="size-4" />
                          Send
                        </>
                      )}
                    </Button>
                  )
                ) : null}
                <DialogClose asChild aria-label="Close email preview modal">
                  <ModalCloseButton icon={<X className="size-4" />} />
                </DialogClose>
              </div>
            </div>
            <DialogDescription>
              Review recipients and final HTML before sending.
            </DialogDescription>
          </DialogHeader>
          <div className="border-b border-slate-200 bg-white px-5 py-4">
            {!activeDraft?.email ? (
              <div className="rounded-lg border border-dashed border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-600">
                Email draft is unavailable for this traffic record.
              </div>
            ) : (
              <div className="space-y-2">
                {emailLockBannerKind ? (
                  <TrafficLockBanner
                    kind={emailLockBannerKind}
                    isUnlocking={isUnlockingTraffic}
                    disabled={!canEditTradsphere || isSaving || isUnlockingTraffic}
                    onUnlock={() => {
                      void handleUnlockTraffic();
                    }}
                  />
                ) : null}
                <EmailChipsInput
                  value={activeDraft.email.toEmails || []}
                  placeholder="To emails"
                  disabled={!canEditTradsphere || isSaving || isSendingEmail || isMarkingTrafficSent || isEmailLocked}
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
                    value={activeDraft.email.ccEmails || []}
                    placeholder="CC emails"
                    disabled={!canEditTradsphere || isSaving || isSendingEmail || isMarkingTrafficSent || isEmailLocked}
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
                    value={activeDraft.email.bccEmails || []}
                    placeholder="BCC emails"
                    disabled={!canEditTradsphere || isSaving || isSendingEmail || isMarkingTrafficSent || isEmailLocked}
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
              </div>
            )}
            {activeTrafficId && isLocalTrafficId(activeTrafficId) ? (
              <p className="mt-2 text-xs text-amber-700">
                This traffic draft will be saved first when you send email.
              </p>
            ) : null}
          </div>
          <div className="h-[calc(92vh-16.5rem)] overflow-y-auto bg-slate-100 p-4">
            <div className="mx-auto h-full w-full max-w-[980px] overflow-hidden rounded-xl border border-blue-100 bg-white shadow-sm">
              <iframe
                title="Traffic email preview"
                srcDoc={activeEmailPreviewHtml}
                className="h-full min-h-[920px] w-full border-0 bg-white"
              />
            </div>
          </div>
        </DialogContent>
      </Dialog>

      <Dialog
        open={isSendEmailConfirmationOpen}
        onOpenChange={(open) => {
          if (isSaving || isSendingEmail || isMarkingTrafficSent) {
            return;
          }
          setIsSendEmailConfirmationOpen(open);
        }}
      >
        <DialogContent className="max-w-lg">
          <ModalHeaderRow
            actions={(
              <DialogClose asChild aria-label="Close send traffic email dialog" disabled={isSaving || isSendingEmail || isMarkingTrafficSent}>
                <ModalCloseButton icon={<X className="size-4" />} />
              </DialogClose>
            )}
          >
            <DialogHeader>
              <DialogTitle>Send traffic email?</DialogTitle>
              <DialogDescription>
                Choose whether a successful send should also mark the traffic status as Sent.
              </DialogDescription>
            </DialogHeader>
          </ModalHeaderRow>
          {activeTrafficNeedsSaveBeforeSending ? (
            <p className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-900">
              Any unsaved changes in the selected traffic will be saved before the email is sent.
            </p>
          ) : null}
          <DialogFooter>
            <Button
              variant="secondary"
              onClick={() => {
                void handleConfirmSendTrafficEmail(false);
              }}
              disabled={isSaving || isSendingEmail}
            >
              {isSaving || isSendingEmail ? (
                <>
                  <Loader2 className="size-4 animate-spin" />
                  Sending...
                </>
              ) : (
                "Send without marking"
              )}
            </Button>
            <Button
              onClick={() => {
                void handleConfirmSendTrafficEmail(true);
              }}
              disabled={isSaving || isSendingEmail}
            >
              {isMarkingTrafficSent ? (
                <>
                  <Loader2 className="size-4 animate-spin" />
                  Sending...
                </>
              ) : (
                "Mark as Sent and Send"
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={isTestEmailModalOpen}
        onOpenChange={(open) => {
          setIsTestEmailModalOpen(open);
        }}
        >
          <DialogContent className="flex max-h-[90vh] max-w-lg flex-col overflow-hidden rounded-xl bg-white p-6">
          <ModalShell busy={isSendingTestEmail} busyMessage="Sending test email..." className="min-h-0 flex-1">
            <ModalHeaderRow
              actions={(
                <DialogClose asChild aria-label="Close test email modal">
                  <ModalCloseButton icon={<X className="size-4" />} />
                </DialogClose>
              )}
            >
              <DialogHeader>
                <DialogTitle>Send Test Email</DialogTitle>
                <DialogDescription>
                  Send a one-off copy to a single recipient. The message includes a visible test-email notice.
                </DialogDescription>
              </DialogHeader>
            </ModalHeaderRow>
            <div className="space-y-2">
              <label className="space-y-1 text-sm">
                <span className="text-slate-600">Test recipient</span>
                <EmailChipsInput
                  value={testEmailTo}
                  onChange={(nextEmails) => setTestEmailTo(nextEmails.slice(0, 1))}
                  placeholder="test@example.com"
                  disabled={!canOpenTrafficTestEmailModal || isSendingTestEmail}
                  labelByEmail={contactNameByEmail}
                  autoFocus
                />
              </label>
              <p className="text-xs text-slate-500">
                This does not change the saved traffic email state.
              </p>
            </div>
            <DialogFooter>
              <Button
                variant="outline"
                onClick={() => setIsTestEmailModalOpen(false)}
                disabled={isSendingTestEmail}
              >
                Cancel
              </Button>
              {!canSendTrafficTestEmail ? (
                <TooltipTarget text={sendTrafficTestEmailDisabledReason}>
                  <span className="inline-flex">
                    <Button
                      onClick={() => {
                        void handleSendTrafficTestEmail();
                      }}
                      disabled
                    >
                      {isSendingTestEmail ? (
                        <>
                          <Loader2 className="size-4 animate-spin" />
                          Sending...
                        </>
                      ) : (
                        "Send Test Email"
                      )}
                    </Button>
                  </span>
                </TooltipTarget>
              ) : (
                <Button
                  onClick={() => {
                    void handleSendTrafficTestEmail();
                  }}
                >
                  {isSendingTestEmail ? (
                    <>
                      <Loader2 className="size-4 animate-spin" />
                      Sending...
                    </>
                  ) : (
                    "Send Test Email"
                  )}
                </Button>
              )}
            </DialogFooter>
          </ModalShell>
        </DialogContent>
      </Dialog>

      <Dialog
        open={isDownloadLinkNoteModalOpen}
        onOpenChange={handleDownloadLinkNoteModalOpenChange}
      >
        <DialogContent
          className="flex max-h-[90vh] max-w-xl flex-col overflow-hidden rounded-xl bg-white p-6"
          onOpenAutoFocus={(event) => {
            event.preventDefault();
            requestAnimationFrame(() => {
              const element = downloadLinkNoteTextareaRef.current;
              if (!element) {
                return;
              }
              element.focus();
              element.select();
            });
          }}
          onInteractOutside={(event) => {
            if (shouldBlockOutsideClose({ isBusy: isSaving || isEmailLocked, hasUnsavedChanges: hasDownloadLinkNoteChanges })) {
              event.preventDefault();
            }
          }}
        >
          <ModalShell busy={isSaving} busyMessage="Saving download note..." className="min-h-0 flex-1">
            <ModalHeaderRow
              actions={(
                <DialogClose asChild aria-label="Close download note modal">
                  <ModalCloseButton icon={<X className="size-4" />} />
                </DialogClose>
              )}
            >
              <DialogHeader>
                <DialogTitle>{activeDownloadLinkNoteEntry?.note ? "Edit Download Note" : "Add Download Note"}</DialogTitle>
                <DialogDescription>
                  {activeDownloadLinkNoteEntry
                    ? `ISCI: ${asString(activeDownloadLinkNoteEntry.isci) || "-"}. This note appears in the Download Links email table.`
                    : "Add a short note for this flight download item."}
                </DialogDescription>
              </DialogHeader>
            </ModalHeaderRow>
            <div className="space-y-2">
              <label className="space-y-1 text-sm">
                <span className="text-slate-600">Note</span>
                <Textarea
                  ref={downloadLinkNoteTextareaRef}
                  value={downloadLinkNoteDraft}
                  placeholder="Add a custom note for this flight download item"
                  className="min-h-[130px]"
                  disabled={!canEditTradsphere || isSaving || isEmailLocked}
                  onChange={(event) => setDownloadLinkNoteDraft(event.target.value)}
                />
              </label>
            </div>
            <DialogFooter className="gap-2">
              {canEditTradsphere && hasDownloadLinkNoteChanges ? (
                <Button
                  variant="outline"
                  onClick={() => {
                    setDownloadLinkNoteDraft(asString(activeDownloadLinkNoteEntry?.note || ""));
                  }}
                  disabled={isSaving || isEmailLocked}
                >
                  Revert
                </Button>
              ) : null}
              {canSaveDownloadLinkNote ? (
                <Button
                  onClick={() => {
                    if (!commitDownloadLinkNote()) {
                      return;
                    }
                    closeDownloadLinkNoteModal();
                  }}
                >
                  Save Changes
                </Button>
              ) : null}
            </DialogFooter>
          </ModalShell>
        </DialogContent>
      </Dialog>

      <UnsavedChangesDialog
        open={isDownloadLinkNoteDiscardDialogOpen}
        onKeepEditing={() => {
          setIsDownloadLinkNoteDiscardDialogOpen(false);
        }}
        onDiscardChanges={() => {
          setIsDownloadLinkNoteDiscardDialogOpen(false);
          closeDownloadLinkNoteModal();
        }}
      />

      <Dialog open={isFlightModalOpen} onOpenChange={handleFlightModalOpenChange}>
        <DialogContent
          className="flex max-h-[90vh] flex-col overflow-hidden rounded-xl bg-white p-6"
          onInteractOutside={(event) => {
            const target = event.target;
            if (target instanceof Element && target.closest(FLIGHT_DATE_PICKER_POPOVER_SELECTOR)) {
              event.preventDefault();
              return;
            }
            if (shouldBlockOutsideClose({ isBusy: isSaving, hasUnsavedChanges: hasFlightModalChanges })) {
              event.preventDefault();
            }
          }}
        >
          <ModalShell busy={isSaving} busyMessage="Saving flight..." className="min-h-0 flex-1">
            <ModalHeaderRow
              actions={(
                <DialogClose asChild aria-label="Close flight modal">
                  <ModalCloseButton icon={<X className="size-4" />} />
                </DialogClose>
              )}
            >
              <DialogHeader className="pb-2">
                <DialogTitle>{flightModalMode === "create" ? "Add Flight" : "Edit Flight"}</DialogTitle>
                <DialogDescription>
                  Update flight scheduling and delivery references.
                </DialogDescription>
              </DialogHeader>
            </ModalHeaderRow>
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
                  disabled={!canEditTradsphere || isSaving || isFlightModalReadOnly}
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
                  disabled={!canEditTradsphere || isSaving || isFlightModalReadOnly}
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
                  disabled={!canEditTradsphere || isSaving || isFlightModalReadOnly}
                />
              </label>
              <label className="space-y-1 text-sm">
                <span className="text-slate-600">Language</span>
                <AppDropdown
                  value={normalizeFlightLanguageValue(flightModalDraft?.language)}
                  onValueChange={(value) => {
                    setFlightModalError(null);
                    setFlightModalDraft((current) => (current
                      ? { ...current, language: normalizeFlightLanguageValue(value) }
                      : current));
                  }}
                  options={FLIGHT_LANGUAGE_OPTIONS}
                  searchable={false}
                  disabled={!canEditTradsphere || isSaving || isFlightModalReadOnly}
                />
              </label>
              <label className="space-y-1 text-sm">
                <span className="text-slate-600">Length</span>
                <Input
                  type="number"
                  min={0}
                  step={1}
                  value={String(Math.max(0, Math.trunc(asNumber(flightModalDraft?.length, 0))))}
                  disabled={!canEditTradsphere || isSaving || isFlightModalReadOnly}
                  onChange={(event) => {
                    setFlightModalError(null);
                    setFlightModalDraft((current) => (current ? { ...current, length: Math.max(0, Math.trunc(asNumber(event.target.value, 0))) } : current));
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
                  disabled={!canEditTradsphere || isSaving || isFlightModalReadOnly}
                  onChange={(event) => {
                    setFlightModalError(null);
                    setFlightModalDraft((current) => (current ? { ...current, rotation: asNumber(event.target.value, 0) } : current));
                  }}
                />
              </label>
              <label className="space-y-1 text-sm">
                <span className="text-slate-600">ISCI</span>
                <Input
                  value={flightModalDraft?.isci || ""}
                  placeholder={flightModalIsciPlaceholder}
                  disabled={!canEditTradsphere || isSaving || isFlightModalReadOnly}
                  onKeyDown={(event) => {
                    if (event.key !== "Enter" && event.key !== "Tab") {
                      return;
                    }
                    const wasApplied = applyFlightModalIsciPlaceholder();
                    if (event.key === "Enter" && wasApplied) {
                      event.preventDefault();
                    }
                  }}
                  onDoubleClick={() => {
                    applyFlightModalIsciPlaceholder();
                  }}
                  onChange={(event) => {
                    setFlightModalError(null);
                    const normalized = event.target.value.replace(/\s+/g, "").toUpperCase();
                    setFlightModalDraft((current) => (current ? { ...current, isci: asNullableString(normalized) } : current));
                  }}
                />
              </label>
            </div>
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">File URL</span>
              <div className="relative">
                <Input
                  ref={flightFileUrlInputRef}
                  type="url"
                  value={flightModalDraft?.fileUrl || ""}
                  placeholder="https://..."
                  className="pr-9"
                  disabled={!canEditTradsphere || isSaving || isFlightModalReadOnly}
                  onChange={(event) => {
                    setFlightModalError(null);
                    setFlightFileUrlError(null);
                    setFlightModalDraft((current) => (current ? { ...current, fileUrl: event.target.value } : current));
                  }}
                  onKeyDown={(event) => {
                    if (event.key !== "Enter") {
                      return;
                    }
                    event.preventDefault();
                    flightScriptUrlInputRef.current?.focus();
                  }}
                  onBlur={(event) => {
                    const raw = asString(event.target.value);
                    if (!raw) {
                      setFlightFileUrlError(null);
                      return;
                    }
                    const normalized = normalizeStrictHttpUrlInput(raw);
                    if (normalized) {
                      setFlightFileUrlError(null);
                      setFlightModalDraft((current) => (current ? { ...current, fileUrl: normalized } : current));
                      return;
                    }
                    setFlightFileUrlError("Invalid URL. Enter a valid http(s) URL.");
                    setFlightModalDraft((current) => (current ? { ...current, fileUrl: "" } : current));
                  }}
                />
                {asString(flightModalDraft?.fileUrl) ? (
                  <TooltipTarget text="Clear file URL">
                    <button
                      type="button"
                      onClick={() => {
                        setFlightModalError(null);
                        setFlightFileUrlError(null);
                        setFlightModalDraft((current) => (current ? { ...current, fileUrl: "" } : current));
                      }}
                      disabled={!canEditTradsphere || isSaving || isFlightModalReadOnly}
                      className="absolute right-2 top-1/2 inline-flex size-6 -translate-y-1/2 items-center justify-center rounded-full text-slate-500 transition hover:bg-slate-100 hover:text-slate-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-300 disabled:cursor-not-allowed disabled:opacity-50"
                      aria-label="Clear file URL"
                    >
                      <X className="size-3.5" />
                    </button>
                  </TooltipTarget>
                ) : null}
              </div>
              {flightFileUrlError ? <p className="text-xs text-rose-600">{flightFileUrlError}</p> : null}
            </label>
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">Script URL</span>
              <div className="relative">
                <Input
                  ref={flightScriptUrlInputRef}
                  type="url"
                  value={flightScriptUrlField.value}
                  placeholder="https://..."
                  className="pr-9"
                  disabled={!canEditTradsphere || isSaving || isFlightModalReadOnly}
                  onChange={flightScriptUrlField.onChange}
                  onKeyDown={(event) => {
                    if (event.key !== "Enter") {
                      return;
                    }
                    event.preventDefault();
                    flightNoteTextareaRef.current?.focus();
                  }}
                  onBlur={flightScriptUrlField.onBlur}
                />
                {asString(flightScriptUrlField.value) ? (
                  <TooltipTarget text="Clear script URL">
                    <button
                      type="button"
                      onClick={() => {
                        setFlightModalError(null);
                        setFlightScriptUrlError(null);
                        setFlightModalDraft((current) => (current ? { ...current, scriptUrl: null } : current));
                      }}
                      disabled={!canEditTradsphere || isSaving || isFlightModalReadOnly}
                      className="absolute right-2 top-1/2 inline-flex size-6 -translate-y-1/2 items-center justify-center rounded-full text-slate-500 transition hover:bg-slate-100 hover:text-slate-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-300 disabled:cursor-not-allowed disabled:opacity-50"
                      aria-label="Clear script URL"
                    >
                      <X className="size-3.5" />
                    </button>
                  </TooltipTarget>
                ) : null}
              </div>
              {flightScriptUrlError ? <p className="text-xs text-rose-600">{flightScriptUrlError}</p> : null}
            </label>
            <label className="space-y-1 text-sm">
              <span className="text-slate-600">Note</span>
              <Textarea
                ref={flightNoteTextareaRef}
                value={flightNoteField.value}
                className="min-h-[96px]"
                disabled={!canEditTradsphere || isSaving || isFlightModalReadOnly}
                onChange={flightNoteField.onChange}
                onBlur={flightNoteField.onBlur}
              />
            </label>
          </div>
          {flightModalError ? <p className="text-sm text-rose-600">{flightModalError}</p> : null}
            <DialogFooter className="gap-2">
              {canEditTradsphere && hasFlightModalChanges ? (
                <Button variant="outline" onClick={revertFlightModalChanges} disabled={isSaving || isFlightModalReadOnly}>
                  Revert
                </Button>
              ) : null}
              {canSubmitFlightModal ? (
                <Button onClick={saveFlightModal}>
                  {flightModalMode === "create" ? "Add" : "Save"}
                </Button>
              ) : null}
            </DialogFooter>
          </ModalShell>
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

      <Dialog
        open={isSyncMissingStationsDialogOpen}
        onOpenChange={(open) => {
          if (!open) {
            resolveSyncMissingStationsDialog([]);
            return;
          }
          setIsSyncMissingStationsDialogOpen(true);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Remove Non-Matching Stations?</DialogTitle>
            <DialogDescription>
              {pendingSyncMissingStationsCodes.length} existing station(s) are not in the synced schedule result.
              Select which station(s) to remove from this traffic record.
            </DialogDescription>
          </DialogHeader>
          {pendingSyncMissingStationsCodes.length > 0 ? (
            <div className="space-y-2">
              <div className="flex items-center justify-between gap-2 text-xs text-slate-600">
                <span>{selectedSyncMissingStationsCodes.length} selected</span>
                <div className="flex items-center gap-2">
                  <Button
                    type="button"
                    variant="outline"
                    className="h-7 px-2 text-xs"
                    onClick={handleSelectAllSyncMissingStations}
                  >
                    Select All
                  </Button>
                  <Button
                    type="button"
                    variant="outline"
                    className="h-7 px-2 text-xs"
                    onClick={handleClearSyncMissingStations}
                  >
                    Clear
                  </Button>
                </div>
              </div>
              <div className="max-h-52 space-y-2 overflow-y-auto rounded-md border border-slate-200 bg-slate-50 p-2">
                {pendingSyncMissingStationsCodes.map((stationCode) => {
                  const checked = selectedSyncMissingStationsCodes.includes(stationCode);
                  return (
                    <label
                      key={stationCode}
                      className={[
                        "flex cursor-pointer items-center gap-2 rounded-md border px-2 py-2 text-sm",
                        checked ? "border-rose-300 bg-rose-50 text-rose-900" : "border-slate-200 bg-white text-slate-700",
                      ].join(" ")}
                    >
                      <input
                        type="checkbox"
                        className="h-4 w-4 rounded border-slate-300"
                        checked={checked}
                        onChange={() => {
                          toggleSyncMissingStationSelection(stationCode);
                        }}
                      />
                      <span className="font-medium">{stationCode}</span>
                    </label>
                  );
                })}
              </div>
            </div>
          ) : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => resolveSyncMissingStationsDialog([])}>
              Keep Existing
            </Button>
            <Button
              className="bg-rose-600 text-white hover:bg-rose-700"
              onClick={handleConfirmSyncMissingStationsRemoval}
            >
              Remove Selected ({selectedSyncMissingStationsCodes.length})
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={isFlightStationSyncSelectOpen}
        onOpenChange={(open) => {
          setIsFlightStationSyncSelectOpen(open);
          if (!open) {
            setPendingFlightStationSync(null);
            setFlightStationSyncCandidates(null);
            setSelectedFlightStationSyncEstNums([]);
            setIsFlightStationSyncCandidatesLoading(false);
            setFlightStationSyncDialogSource("flight_update");
          }
        }}
      >
        <DialogContent>
          <ModalHeaderRow
            actions={(
              <DialogClose asChild aria-label="Close station sync modal">
                <ModalCloseButton icon={<X className="size-4" />} />
              </DialogClose>
            )}
          >
            <DialogHeader>
              <DialogTitle>Select Estimate Numbers To Sync Stations</DialogTitle>
              <DialogDescription>
                {flightStationSyncDialogSource === "manual_sync"
                  ? "Select estimate numbers, then sync stations from matching schedules."
                  : "This flight update changed the date range. Select estimate numbers, then sync stations from matching schedules."}
              </DialogDescription>
            </DialogHeader>
          </ModalHeaderRow>
          <div className="space-y-3">
            {isFlightStationSyncCandidatesLoading ? (
              <div className="flex items-center gap-2 rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
                <Loader2 className="size-4 animate-spin" />
                <span>Loading matching estimate numbers...</span>
              </div>
            ) : (flightStationSyncCandidates?.estNums.length ?? 0) > 0 ? (
              <>
                {flightStationSyncCandidates?.summary ? (
                  <TrafficStationCandidateSummaryTable summary={flightStationSyncCandidates.summary} />
                ) : null}
                <div className="max-h-72 space-y-2 overflow-y-auto rounded-md border border-slate-200 p-2">
                  {(flightStationSyncCandidates?.estNums ?? []).map((item) => {
                    const checked = selectedFlightStationSyncEstNums.includes(item.estNum);
                    return (
                      <div
                        key={item.estNum}
                        className={[
                          "flex items-start justify-between gap-2 rounded-md border px-2 py-2",
                          checked
                            ? "border-blue-300 bg-blue-50"
                            : "border-slate-200 bg-white",
                        ].join(" ")}
                      >
                        <label className="flex min-w-0 flex-1 cursor-pointer items-start gap-2">
                          <input
                            type="checkbox"
                            className="mt-0.5 h-4 w-4 rounded border-slate-300"
                            checked={checked}
                            onChange={() => {
                              toggleFlightStationSyncEstNum(item.estNum);
                            }}
                          />
                          <span className="min-w-0 text-sm text-slate-800">
                            <span className="font-semibold">EstNum {item.estNum}</span>
                            <span className="ml-2 text-xs text-slate-500">
                              {item.stationCount} station(s)
                            </span>
                            {item.medium ? (
                              <span className="ml-2 inline-flex rounded-full border border-slate-200 bg-slate-100 px-2 py-0.5 text-[11px] font-medium text-slate-700">
                                {item.medium}
                              </span>
                            ) : null}
                            {item.note ? (
                              <span className="block truncate text-xs text-slate-600">
                                {item.note}
                              </span>
                            ) : null}
                          </span>
                        </label>
                        <Button
                          type="button"
                          variant="outline"
                          className="h-7 px-2 text-xs"
                          onClick={() => {
                            handleOpenFlightSyncSchedulePreview(item.estNum, item.note);
                          }}
                        >
                          Preview
                        </Button>
                      </div>
                      );
                    })}
                </div>
                <div className="text-xs text-slate-600">
                  {(flightStationSyncCandidates?.estNums.length ?? 0)} estimate number(s) found in this flight range.
                </div>
              </>
          ) : (
            <p className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
              No estimate numbers found for this flight range. Sync is skipped.
            </p>
          )}
          </div>
          <ModalCacheFooter
            text={flightStationSyncCacheStatusText}
            onRefresh={handleRefreshFlightStationSyncCandidates}
            disabled={!pendingFlightStationSync || isFlightStationSyncCandidatesLoading}
            refreshing={isFlightStationSyncCandidatesLoading}
            refreshLabel="Refresh estimate numbers"
            tooltipText={
              !pendingFlightStationSync
                ? "Load a flight date range before refreshing estimate numbers."
                : isOnline
                  ? "Click to refresh estimate numbers"
                  : "Offline. Reconnect to refresh estimate numbers."
            }
            actions={(
              <>
                <Button variant="outline" onClick={() => handleResolveFlightStationSyncSelection(false)}>
                  Skip Sync
                </Button>
                <Button
                  className="bg-slate-900 text-white hover:bg-slate-800"
                  disabled={isFlightStationSyncCandidatesLoading || selectedFlightStationSyncEstNums.length === 0}
                  onClick={() => handleResolveFlightStationSyncSelection(true)}
                >
                  Sync Selected ({selectedFlightStationSyncEstNums.length})
                </Button>
              </>
            )}
          />
        </DialogContent>
      </Dialog>

      <ScheduleModal
        open={isFlightSyncPreviewModalOpen}
        onOpenChange={(open) => {
          setIsFlightSyncPreviewModalOpen(open);
          if (!open) {
            setFlightSyncPreviewEstnum(null);
          }
        }}
        selectedEstnum={flightSyncPreviewEstnum}
        accountCode={activeAccountCode}
        billingType={null}
        headers={requestHeaders}
      />

      <Dialog open={isStationModalOpen} onOpenChange={handleStationModalOpenChange}>
        <DialogContent
          className="flex max-h-[90vh] flex-col overflow-hidden rounded-xl bg-white p-6"
          onInteractOutside={(event) => {
            if (shouldBlockOutsideClose({ isBusy: isSaving, hasUnsavedChanges: hasStationModalChanges })) {
              event.preventDefault();
            }
          }}
        >
          <ModalShell busy={isSaving} busyMessage="Saving station..." className="min-h-0 flex-1">
            <ModalHeaderRow
              actions={(
                <DialogClose asChild aria-label="Close station modal">
                  <ModalCloseButton icon={<X className="size-4" />} />
                </DialogClose>
              )}
            >
              <DialogHeader className="pb-2">
                <DialogTitle>{stationModalMode === "create" ? "Add Station" : "Edit Station"}</DialogTitle>
                <DialogDescription>
                  Update delivery workflow and confirmation tracking for this station row.
                </DialogDescription>
              </DialogHeader>
            </ModalHeaderRow>
            <SectionMessageStack
              className="pt-1"
              messages={[
                ...(stationLookupError
                  ? [{ id: "traffic-station-lookup-error", variant: "error" as const, message: stationLookupError }]
                  : []),
                ...(stationModalError
                  ? [{ id: "traffic-station-modal-error", variant: "error" as const, message: stationModalError }]
                  : []),
              ]}
            />
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
                    disabled={!canEditTradsphere || isSaving || isTrafficEditingLocked}
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
                        if (event.key === "Enter" || !event.shiftKey) {
                          event.preventDefault();
                          if (isSentLocked) {
                            focusStationConfirmedStatusField();
                          } else {
                            focusStationDeliveryStatusField();
                          }
                        }
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
                          <p className="text-sm font-medium leading-5 text-slate-600">Language</p>
                          <p className="break-words text-sm leading-5 text-slate-800">
                            {selectedStationLookupMeta?.language || "-"}
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
                  <div
                    ref={stationDeliveryStatusFieldRef}
                    onKeyDownCapture={(event) => {
                      handleStationStatusFieldAdvance(event, "confirmed");
                    }}
                  >
                      <AppDropdown
                        value={stationModalDraft?.deliveryStatus || ""}
                        onValueChange={(value) => {
                          setStationModalError(null);
                          setStationModalDraft((current) => (current ? { ...current, deliveryStatus: value } : current));
                        }}
                        options={[{ value: "", label: "" }, ...DELIVERY_STATUS_OPTIONS.map((option) => ({ value: option, label: formatStatusOptionLabel(option) }))]}
                        searchable={false}
                        disabled={!canEditTradsphere || isSaving || isTrafficEditingLocked}
                        placeholder=""
                      />
                    </div>
                  </label>
                  <label className="space-y-1 text-sm">
                  <span className="text-sm font-medium leading-5 text-slate-600">Confirmed Status</span>
                  <div
                    ref={stationConfirmedStatusFieldRef}
                    onKeyDownCapture={(event) => {
                      handleStationStatusFieldAdvance(event, "note");
                    }}
                  >
                      <AppDropdown
                        value={stationModalDraft?.confirmedStatus || ""}
                        onValueChange={(value) => {
                          setStationModalError(null);
                          setStationModalDraft((current) => (current ? { ...current, confirmedStatus: value } : current));
                        }}
                        options={[{ value: "", label: "" }, ...CONFIRMED_STATUS_OPTIONS.map((option) => ({ value: option, label: formatStatusOptionLabel(option) }))]}
                        searchable={false}
                        disabled={!canEditTradsphere || isSaving || isConfirmedLocked}
                        placeholder=""
                      />
                    </div>
                  </label>
                </div>
                <label className="space-y-1 text-sm">
                  <span className="text-sm font-medium leading-5 text-slate-600">Note</span>
                  <Textarea
                    ref={stationNoteTextareaRef}
                    value={stationNoteField.value}
                    className="min-h-[96px]"
                    disabled={!canEditTradsphere || isSaving || isConfirmedLocked}
                    onChange={stationNoteField.onChange}
                    onBlur={stationNoteField.onBlur}
                  />
                </label>
              </div>
            </section>
          </div>
            <ModalCacheFooter
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
              actions={canEditTradsphere && (hasStationModalChanges || canSubmitStationModal) ? stationModalFooterActions : null}
            />
          </ModalShell>
        </DialogContent>
      </Dialog>

      <Dialog
        open={isTrafficConfirmDialogOpen}
        onOpenChange={(open) => {
          setIsTrafficConfirmDialogOpen(open);
          if (!open) {
            setTrafficConfirmTargetTrafficId(null);
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Mark traffic as Confirmed?</DialogTitle>
            <DialogDescription>
              All station confirmed statuses are now Confirmed. Do you want to change the traffic status to Confirmed?
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => handlePromptConfirmTrafficStatus(false)}
            >
              Not now
            </Button>
            <Button
              onClick={() => handlePromptConfirmTrafficStatus(true)}
            >
              Mark Confirmed
            </Button>
          </DialogFooter>
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
        open={isRefreshEmailMergeDialogOpen}
        onOpenChange={(open) => {
          setIsRefreshEmailMergeDialogOpen(open);
          if (!open) {
            setPendingRefreshEmailMerge(null);
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add updated contact emails to To?</DialogTitle>
            <DialogDescription>
              Refresh found new station contact email(s). Add them to the traffic To list?
            </DialogDescription>
          </DialogHeader>
          <div className="max-h-48 overflow-y-auto rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
            {(pendingRefreshEmailMerge?.emails ?? []).join(", ")}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => handleResolveRefreshEmailMerge(false)}>
              Keep current To
            </Button>
            <Button onClick={() => handleResolveRefreshEmailMerge(true)}>
              Add to To ({pendingRefreshEmailMerge?.emails.length ?? 0})
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={isArchiveDialogOpen}
        onOpenChange={(open) => {
          if (isArchiveTargetDeleting) {
            return;
          }
          setIsArchiveDialogOpen(open);
          if (!open) {
            setArchiveTargetTrafficId(null);
            setArchiveDialogMode("archive");
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{archiveDialogMode === "delete" ? "Delete traffic record?" : "Archive traffic record?"}</DialogTitle>
            <DialogDescription>
              {archiveDialogMode === "delete"
                ? "This action removes the traffic from the active list."
                : "Archived traffic is removed from the active list for this account."}
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" disabled={isArchiveTargetDeleting} onClick={() => setIsArchiveDialogOpen(false)}>
              Cancel
            </Button>
            <Button
              className="border-rose-700 bg-rose-600 text-white hover:bg-rose-700"
              disabled={isArchiveTargetDeleting}
              onClick={() => void handleArchiveTraffic()}
            >
              {isArchiveTargetDeleting ? (
                <>
                  <Loader2 className="size-4 animate-spin" />
                  Processing...
                </>
              ) : (
                archiveDialogMode === "delete" ? "Delete" : "Archive"
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <PageLoadingLayer
        active={loadingContract.pageOverlayActive}
        message={loadingContract.pageOverlayMessage}
      />
    </AppPageLayout>
  );
}
