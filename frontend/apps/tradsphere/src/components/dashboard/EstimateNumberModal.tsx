import { useEffect, useMemo, useRef, useState } from "react";
import { Loader2, X } from "lucide-react";

import {
  TRADSPHERE_BROADCAST_TIMEZONE,
  getBroadcastMonthRange,
  getBroadcastQuarterRange,
  getBroadcastYearRange,
} from "@/lib/broadcastCalendar";
import { AppDropdown, type AppDropdownOption } from "@/components/ui/app-dropdown";
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
import { Textarea } from "@/components/ui/textarea";
import { canModalClose, shouldBlockOutsideClose } from "@/components/ui/modal-close-guard";
import { UnsavedChangesDialog } from "@/components/ui/unsaved-changes-dialog";
import { useApiRequest, type ApiRequestOptions } from "@/hooks/useApiRequest";
import { readBrowserCacheSnapshot, writeBrowserCache } from "@/lib/browserCache";
import { TRADSPHERE_CACHE_TTL_MS } from "@shared/cache";
import { ModalCacheFooter, ModalCloseButton, ModalHeaderRow, ModalShell } from "@shared/components";
import { useCommittedTextField } from "@shared/hooks/useCommittedTextField";

import { FlightDateRangeField, FLIGHT_DATE_PICKER_POPOVER_SELECTOR } from "./FlightDateRangeField";
import { type FlightRangePresetState } from "./FlightRangeSelector";
import { LabeledField, ReadOnlyValue } from "./FormFieldRow";

const EST_NUM_MAX_UNSIGNED_INT = 4294967295;
const MEDIA_TYPE_VALUES = ["TV", "RA", "CA", "OD", "NP", "CINE", "OTT"] as const;
const MEDIA_TYPE_OPTIONS = MEDIA_TYPE_VALUES.map((value) => ({ value, label: value }));
const CREATE_EST_NUM_URL = "/api/tradsphere/v1/estNums";
const UPDATE_EST_NUM_URL = "/api/tradsphere/v1/estNums";
const ESTNUM_DETAIL_CACHE_TTL_MS = TRADSPHERE_CACHE_TTL_MS.ESTNUM_DETAIL;
const CHICAGO_DATE_PARTS_FORMATTER = new Intl.DateTimeFormat("en-US", {
  timeZone: TRADSPHERE_BROADCAST_TIMEZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});

export type EstimateNumberModalMode = "create" | "edit";

export type EstimateNumberModalData = {
  estNum?: number;
  accountCode?: string;
  flightStart?: string;
  flightEnd?: string;
  mediaType?: string;
  buyer?: string;
  note?: string | null;
};

export type EstimateNumberModalSaveResult = {
  mode: EstimateNumberModalMode;
  estNum: number;
  accountCode: string;
  flightStart: string;
  flightEnd: string;
  mediaType: string;
  buyer: string;
  note: string | null;
};

interface EstimateNumberModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  mode: EstimateNumberModalMode;
  canEdit?: boolean;
  initialData?: EstimateNumberModalData | null;
  accountCode: string;
  accountName?: string;
  accountOptions?: AppDropdownOption[];
  headers: HeadersInit;
  onSuccess?: (result: EstimateNumberModalSaveResult) => Promise<void> | void;
}

type EstNumFormState = {
  estNum: string;
  accountCode: string;
  flightStart: string;
  flightEnd: string;
  mediaType: string;
  buyer: string;
  note: string;
};

type ComparableFormState = {
  estNum: string;
  accountCode: string;
  flightStart: string;
  flightEnd: string;
  mediaType: string;
  buyer: string;
  note: string;
};

type ParsedIsoDate = {
  year: number;
  month: number;
  day: number;
};


function buildInitialFormState(
  accountCode: string,
  mode: EstimateNumberModalMode,
  initialData?: EstimateNumberModalData | null,
): EstNumFormState {
  const normalizedAccountCode =
    asString(initialData?.accountCode).toUpperCase() || accountCode.trim().toUpperCase();

  return {
    estNum:
      initialData?.estNum !== undefined && initialData?.estNum !== null
        ? String(initialData.estNum)
        : "",
    accountCode: normalizedAccountCode,
    flightStart: asString(initialData?.flightStart),
    flightEnd: asString(initialData?.flightEnd),
    mediaType:
      asString(initialData?.mediaType).toUpperCase() ||
      (mode === "create" ? MEDIA_TYPE_VALUES[0] : ""),
    buyer: asString(initialData?.buyer) || (mode === "create" ? "Elyse" : ""),
    note: asString(initialData?.note),
  };
}

function parseIsoDate(value: string): ParsedIsoDate | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value.trim());
  if (!match) {
    return null;
  }

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return null;
  }
  if (month < 1 || month > 12) {
    return null;
  }

  const maxDay = new Date(Date.UTC(year, month, 0)).getUTCDate();
  if (day < 1 || day > maxDay) {
    return null;
  }

  return { year, month, day };
}

function getTodayInChicago(): ParsedIsoDate {
  const parts = CHICAGO_DATE_PARTS_FORMATTER.formatToParts(new Date());
  const values = parts.reduce<Record<string, string>>((output, part) => {
    output[part.type] = part.value;
    return output;
  }, {});

  const year = Number(values.year);
  const month = Number(values.month);
  const day = Number(values.day);

  return {
    year: Number.isFinite(year) ? year : 1970,
    month: Number.isFinite(month) ? month : 1,
    day: Number.isFinite(day) ? day : 1,
  };
}

function toQuarter(month: number): string {
  return String(Math.floor((month - 1) / 3) + 1);
}

function buildDefaultQuarterPreset(month: number, year: number): FlightRangePresetState {
  return {
    rangeType: "QUARTER",
    rangeValue: toQuarter(month),
    year: String(year),
  };
}

function getRangeFromPreset(preset: FlightRangePresetState): { flightStart: string; flightEnd: string } | null {
  const year = Number(preset.year);
  if (!Number.isInteger(year)) {
    return null;
  }

  if (preset.rangeType === "MONTH") {
    const month = Number(preset.rangeValue);
    if (!Number.isInteger(month)) {
      return null;
    }
    return getBroadcastMonthRange(month, year);
  }

  if (preset.rangeType === "QUARTER") {
    const quarter = Number(preset.rangeValue);
    if (!Number.isInteger(quarter) || quarter < 1 || quarter > 4) {
      return null;
    }
    return getBroadcastQuarterRange(quarter as 1 | 2 | 3 | 4, year);
  }

  if (preset.rangeType === "YEAR") {
    return getBroadcastYearRange(year);
  }

  return null;
}

function resolvePresetFromFlightRange(
  flightStart: string,
  flightEnd: string,
): FlightRangePresetState | null {
  const parsedStart = parseIsoDate(flightStart);
  const parsedEnd = parseIsoDate(flightEnd);
  if (!parsedStart || !parsedEnd) {
    return null;
  }

  const candidateYears = new Set<number>([
    parsedStart.year - 1,
    parsedStart.year,
    parsedStart.year + 1,
    parsedEnd.year - 1,
    parsedEnd.year,
    parsedEnd.year + 1,
  ]);

  for (const year of candidateYears) {
    if (!Number.isInteger(year)) {
      continue;
    }

    for (let month = 1; month <= 12; month += 1) {
      const range = getBroadcastMonthRange(month, year);
      if (range.flightStart === flightStart && range.flightEnd === flightEnd) {
        return {
          rangeType: "MONTH",
          rangeValue: String(month),
          year: String(year),
        };
      }
    }

    for (let quarter = 1; quarter <= 4; quarter += 1) {
      const range = getBroadcastQuarterRange(quarter as 1 | 2 | 3 | 4, year);
      if (range.flightStart === flightStart && range.flightEnd === flightEnd) {
        return {
          rangeType: "QUARTER",
          rangeValue: String(quarter),
          year: String(year),
        };
      }
    }

    const yearRange = getBroadcastYearRange(year);
    if (yearRange.flightStart === flightStart && yearRange.flightEnd === flightEnd) {
      return {
        rangeType: "YEAR",
        rangeValue: "",
        year: String(year),
      };
    }
  }

  return {
    rangeType: "CUSTOM",
    rangeValue: "",
    year: String(parsedStart.year),
  };
}


function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asString(value: unknown): string {
  if (typeof value === "string") {
    return value.trim();
  }
  if (typeof value === "number") {
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

function unwrapData(payload: unknown): unknown {
  if (isRecord(payload) && "data" in payload) {
    return payload.data;
  }
  return payload;
}

function toTitleCasePreservingSpaces(value: string): string {
  return value.replace(/\S+/g, (token) => {
    if (!token) {
      return token;
    }
    return token.charAt(0).toUpperCase() + token.slice(1).toLowerCase();
  });
}

function validateForm(form: EstNumFormState): string | null {
  const estNumText = form.estNum.trim();
  if (!estNumText) {
    return "EstNum is required.";
  }

  const estNum = Number(estNumText);
  if (!Number.isInteger(estNum) || estNum < 0 || estNum > EST_NUM_MAX_UNSIGNED_INT) {
    return "EstNum must be an unsigned integer.";
  }

  if (!form.accountCode.trim()) {
    return "Account code is required.";
  }

  if (!form.flightStart || !form.flightEnd) {
    return "Flight dates are required.";
  }

  const flightRangeError = getFlightRangeValidationError(form);
  if (flightRangeError) {
    return flightRangeError;
  }

  if (!form.mediaType.trim()) {
    return "Media type is required.";
  }

  const buyer = form.buyer.trim();
  if (!buyer) {
    return "Buyer is required.";
  }
  if (buyer.length > 36) {
    return "Buyer must be 36 characters or fewer.";
  }

  if (form.note.trim().length > 2048) {
    return "Note must be 2048 characters or fewer.";
  }

  return null;
}

function getFlightRangeValidationError(form: EstNumFormState): string | null {
  if (!form.flightStart || !form.flightEnd) {
    return null;
  }
  if (form.flightStart > form.flightEnd) {
    return "Flight start date must be on or before flight end date.";
  }
  return null;
}

function toComparableFormState(form: EstNumFormState): ComparableFormState {
  return {
    estNum: form.estNum.trim(),
    accountCode: form.accountCode.trim().toUpperCase(),
    flightStart: form.flightStart.trim(),
    flightEnd: form.flightEnd.trim(),
    mediaType: form.mediaType.trim().toUpperCase(),
    buyer: form.buyer.trim(),
    note: form.note.trim(),
  };
}

function hasFormChanges(current: EstNumFormState, original: EstNumFormState | null): boolean {
  if (!original) {
    return false;
  }

  const currentComparable = toComparableFormState(current);
  const originalComparable = toComparableFormState(original);
  return (
    currentComparable.estNum !== originalComparable.estNum ||
    currentComparable.accountCode !== originalComparable.accountCode ||
    currentComparable.flightStart !== originalComparable.flightStart ||
    currentComparable.flightEnd !== originalComparable.flightEnd ||
    currentComparable.mediaType !== originalComparable.mediaType ||
    currentComparable.buyer !== originalComparable.buyer ||
    currentComparable.note !== originalComparable.note
  );
}

function parseEstNumRow(payload: unknown, targetEstNum: number): EstimateNumberModalData | null {
  const data = unwrapData(payload);
  if (!Array.isArray(data)) {
    return null;
  }

  const matched = data.find((item) => {
    if (!isRecord(item)) {
      return false;
    }
    const estNum = asNumber(item.estNum);
    return estNum !== null && estNum === targetEstNum;
  });

  if (!isRecord(matched)) {
    return null;
  }

  const estNum = asNumber(matched.estNum);
  if (estNum === null) {
    return null;
  }

  return {
    estNum,
    accountCode: asString(matched.accountCode).toUpperCase(),
    flightStart: asString(matched.flightStart),
    flightEnd: asString(matched.flightEnd),
    mediaType: asString(matched.mediaType).toUpperCase(),
    buyer: asString(matched.buyer),
    note: asString(matched.note),
  };
}

function canUseInitialDataForEdit(initialData?: EstimateNumberModalData | null): boolean {
  if (!initialData) {
    return false;
  }

  const estNum = asNumber(initialData.estNum);
  const accountCode = asString(initialData.accountCode).toUpperCase();
  const flightStart = asString(initialData.flightStart);
  const flightEnd = asString(initialData.flightEnd);
  const mediaType = asString(initialData.mediaType).toUpperCase();
  const buyer = asString(initialData.buyer);

  const hasIsoDates =
    /^\d{4}-\d{2}-\d{2}$/.test(flightStart) &&
    /^\d{4}-\d{2}-\d{2}$/.test(flightEnd);

  return (
    estNum !== null &&
    estNum >= 0 &&
    estNum <= EST_NUM_MAX_UNSIGNED_INT &&
    Boolean(accountCode) &&
    hasIsoDates &&
    Boolean(mediaType) &&
    Boolean(buyer)
  );
}

async function fetchEstNumDetail(
  requestJson: (url: string, options?: ApiRequestOptions) => Promise<unknown>,
  estNum: number,
  accountCode: string,
  headers: HeadersInit,
  forceFresh = false,
): Promise<{ detail: EstimateNumberModalData | null; source: "cache" | "network" | null; fetchedAt: number | null }> {
  const normalizedAccountCode = accountCode.trim().toUpperCase();
  const detailCacheKey = `estnum-detail:${normalizedAccountCode}:${estNum}`;
  const cachedDetail = readBrowserCacheSnapshot<EstimateNumberModalData>(detailCacheKey);
  if (!forceFresh && cachedDetail && !cachedDetail.isExpired) {
    return {
      detail: cachedDetail.data,
      source: "cache",
      fetchedAt: cachedDetail.fetchedAt,
    };
  }

  const query = new URLSearchParams();
  query.set("estNum", String(estNum));
  if (normalizedAccountCode) {
    query.set("accountCode", normalizedAccountCode);
  }

  const payload = await requestJson(`/api/tradsphere/v1/estNums?${query.toString()}`, {
    headers,
    errorToast: false,
  });
  const parsed = parseEstNumRow(payload, estNum);
  const fetchedAt = Date.now();
  if (parsed) {
    writeBrowserCache(detailCacheKey, parsed, ESTNUM_DETAIL_CACHE_TTL_MS, { source: "network", fetchedAt });
  }
  return {
    detail: parsed,
    source: "network",
    fetchedAt,
  };
}

function RequiredMark() {
  return <span className="ml-1 text-rose-600">*</span>;
}

export function EstimateNumberModal({
  open,
  onOpenChange,
  mode,
  canEdit = true,
  initialData,
  accountCode,
  accountName,
  accountOptions = [],
  headers,
  onSuccess,
}: EstimateNumberModalProps) {
  const { requestJson } = useApiRequest();
  const chicagoToday = useMemo(() => getTodayInChicago(), []);
  const [form, setForm] = useState<EstNumFormState>(() =>
    buildInitialFormState(accountCode, mode, initialData),
  );
  const [originalForm, setOriginalForm] = useState<EstNumFormState | null>(null);
  const [flightRangePreset, setFlightRangePreset] = useState<FlightRangePresetState>(() =>
    buildDefaultQuarterPreset(chicagoToday.month, chicagoToday.year),
  );
  const [hasAutoSeededDefaultDates, setHasAutoSeededDefaultDates] = useState(false);
  const [flightRangeError, setFlightRangeError] = useState<string | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [isLoadingDetail, setIsLoadingDetail] = useState(false);
  const [isRefreshingDetail, setIsRefreshingDetail] = useState(false);
  const [hasAttemptedDetailLoad, setHasAttemptedDetailLoad] = useState(false);
  const [detailRefreshToken, setDetailRefreshToken] = useState(0);
  const [detailCacheStatus, setDetailCacheStatus] = useState<{ source: "cache" | "network"; fetchedAt: number } | null>(null);
  const [hasDeferredDetailUpdate, setHasDeferredDetailUpdate] = useState(false);
  const handledDetailRefreshTokenRef = useRef(0);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);
  const buyerField = useCommittedTextField<HTMLInputElement>(
    form.buyer,
    (value) => updateForm("buyer", value),
    { normalizeOnBlur: toTitleCasePreservingSpaces },
  );
  const noteField = useCommittedTextField<HTMLTextAreaElement>(
    form.note,
    (value) => updateForm("note", value),
  );

  useEffect(() => {
    if (!open) {
      setHasDeferredDetailUpdate(false);
      setOriginalForm(null);
      setHasAutoSeededDefaultDates(false);
      setSubmitError(null);
      setDetailError(null);
      setIsDiscardDialogOpen(false);
      setIsLoadingDetail(false);
      setIsRefreshingDetail(false);
      setHasAttemptedDetailLoad(false);
      setDetailCacheStatus(null);
      setFlightRangeError(null);
      return;
    }

    setSubmitError(null);
    setDetailError(null);
    setIsDiscardDialogOpen(false);
    setFlightRangeError(null);
    setHasAutoSeededDefaultDates(false);
    const isManualRefresh = detailRefreshToken !== handledDetailRefreshTokenRef.current;
    if (isManualRefresh) {
      handledDetailRefreshTokenRef.current = detailRefreshToken;
    }
    setIsRefreshingDetail(isManualRefresh);

    const initialForm = buildInitialFormState(accountCode, mode, initialData);
    const parsedStart = parseIsoDate(initialForm.flightStart);
    const defaultPreset = buildDefaultQuarterPreset(
      parsedStart?.month ?? chicagoToday.month,
      parsedStart?.year ?? chicagoToday.year,
    );
    setFlightRangePreset(defaultPreset);

    if (mode === "create") {
      let seededInitialForm = initialForm;
      if (!initialForm.flightStart || !initialForm.flightEnd) {
        const defaultRange = getRangeFromPreset(defaultPreset);
        if (defaultRange) {
          seededInitialForm = {
            ...initialForm,
            flightStart: initialForm.flightStart || defaultRange.flightStart,
            flightEnd: initialForm.flightEnd || defaultRange.flightEnd,
          };
        }
      }
      setForm(seededInitialForm);
      setOriginalForm(seededInitialForm);
      return;
    }

    // For search-result edits we often already have a full row payload.
    // Reuse it directly and skip refetch unless user explicitly requests refresh.
    if (!isManualRefresh && canUseInitialDataForEdit(initialData)) {
      setForm(initialForm);
      setOriginalForm(initialForm);
      setHasAttemptedDetailLoad(true);
      setIsLoadingDetail(false);
      setIsRefreshingDetail(false);
      setDetailCacheStatus({
        source: "cache",
        fetchedAt: Date.now(),
      });
      const estNum = asNumber(initialData?.estNum);
      const normalizedAccountCode = asString(initialData?.accountCode).toUpperCase();
      if (estNum !== null && normalizedAccountCode) {
        writeBrowserCache(
          `estnum-detail:${normalizedAccountCode}:${estNum}`,
          {
            estNum,
            accountCode: normalizedAccountCode,
            flightStart: asString(initialData?.flightStart),
            flightEnd: asString(initialData?.flightEnd),
            mediaType: asString(initialData?.mediaType).toUpperCase(),
            buyer: asString(initialData?.buyer),
            note: asString(initialData?.note) || null,
          },
          ESTNUM_DETAIL_CACHE_TTL_MS,
          { source: "cache", fetchedAt: Date.now() },
        );
      }
      return;
    }

    // Fallback for partial/stale payloads: fetch detail in edit mode.
    setForm(initialForm);
    setOriginalForm(null);
    setHasAttemptedDetailLoad(false);
    const estNum = initialData?.estNum;
    if (estNum === undefined || estNum === null) {
      setDetailError("Missing EstNum identifier for edit mode.");
      setHasAttemptedDetailLoad(true);
      return;
    }

    let isMounted = true;
    setIsLoadingDetail(true);

    void fetchEstNumDetail(requestJson, estNum, accountCode, headers, isManualRefresh)
      .then((detailResult) => {
        if (!isMounted || !detailResult.detail) {
          if (isMounted) {
            setDetailError("Unable to load estimate details.");
            setHasAttemptedDetailLoad(true);
          }
          return;
        }
        if (hasUnsavedChangesRef.current) {
          if (detailResult.source && detailResult.fetchedAt) {
            setDetailCacheStatus({
              source: detailResult.source,
              fetchedAt: detailResult.fetchedAt,
            });
          }
          setHasDeferredDetailUpdate(true);
          setHasAttemptedDetailLoad(true);
          return;
        }
        setHasDeferredDetailUpdate(false);
        if (detailResult.source && detailResult.fetchedAt) {
          setDetailCacheStatus({
            source: detailResult.source,
            fetchedAt: detailResult.fetchedAt,
          });
        }
        const loadedForm = buildInitialFormState(accountCode, mode, detailResult.detail);
        setForm(loadedForm);
        setOriginalForm(loadedForm);
        setHasAutoSeededDefaultDates(false);
        {
          const parsedStart = parseIsoDate(loadedForm.flightStart);
          const defaultPreset = buildDefaultQuarterPreset(
            parsedStart?.month ?? chicagoToday.month,
            parsedStart?.year ?? chicagoToday.year,
          );
          setFlightRangePreset(defaultPreset);

          if (!loadedForm.flightStart || !loadedForm.flightEnd) {
            const defaultRange = getRangeFromPreset(defaultPreset);
            if (defaultRange) {
              setHasAutoSeededDefaultDates(true);
              setForm((current) => ({
                ...current,
                flightStart: current.flightStart || defaultRange.flightStart,
                flightEnd: current.flightEnd || defaultRange.flightEnd,
              }));
            }
          }
        }
        setHasAttemptedDetailLoad(true);
      })
      .catch((error) => {
        if (!isMounted) {
          return;
        }
        setDetailError(error instanceof Error ? error.message : "Unable to load estimate details.");
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
  }, [accountCode, chicagoToday.month, chicagoToday.year, detailRefreshToken, headers, initialData, mode, open]);

  function updateForm<K extends keyof EstNumFormState>(field: K, value: EstNumFormState[K]) {
    setForm((current) => ({ ...current, [field]: value }));
    setSubmitError(null);
  }

  function updateFlightDateManually(field: "flightStart" | "flightEnd", value: string) {
    const nextForm = { ...form, [field]: value };
    setForm(nextForm);
    setFlightRangeError(null);
    setSubmitError(null);
    const resolvedPreset = resolvePresetFromFlightRange(nextForm.flightStart, nextForm.flightEnd);
    if (resolvedPreset) {
      setFlightRangePreset(resolvedPreset);
      return;
    }
    setFlightRangePreset((current) =>
      current.rangeType === "CUSTOM" ? current : { ...current, rangeType: "CUSTOM", rangeValue: "" },
    );
  }

  function applyFlightRangePreset(range: { flightStart: string; flightEnd: string }) {
    if (range.flightStart > range.flightEnd) {
      setFlightRangeError("Preset range is invalid. Please choose a different preset.");
      return;
    }
    setFlightRangeError(null);
    setSubmitError(null);
    setForm((current) => ({
      ...current,
      flightStart: range.flightStart,
      flightEnd: range.flightEnd,
    }));
  }

  function handleRevertChanges() {
    if (!hasUnsavedChanges || isSubmitting || isReadOnly || !originalForm) {
      return;
    }

    const parsedStart = parseIsoDate(originalForm.flightStart);
    const resetPreset = buildDefaultQuarterPreset(
      parsedStart?.month ?? chicagoToday.month,
      parsedStart?.year ?? chicagoToday.year,
    );

    setForm(originalForm);
    setFlightRangePreset(resetPreset);
    setHasAutoSeededDefaultDates(false);
    setFlightRangeError(null);
    setSubmitError(null);
  }

  const isEditMode = mode === "edit";
  const isReadOnly = !canEdit;
  const hasUnsavedChanges =
    hasFormChanges(form, originalForm) || (isEditMode && hasAutoSeededDefaultDates);
  const hasUnsavedChangesRef = useRef(hasUnsavedChanges);
  const isDetailReady = !isEditMode || (hasAttemptedDetailLoad && !isLoadingDetail && !detailError && !!originalForm);

  useEffect(() => {
    hasUnsavedChangesRef.current = hasUnsavedChanges;
  }, [hasUnsavedChanges]);

  useEffect(() => {
    if (!open || hasUnsavedChanges || !hasDeferredDetailUpdate || isLoadingDetail) {
      return;
    }
    setHasDeferredDetailUpdate(false);
    setDetailRefreshToken((current) => current + 1);
  }, [hasDeferredDetailUpdate, hasUnsavedChanges, isLoadingDetail, open]);

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
    if (isSubmitting || isReadOnly) {
      return;
    }

    const validationError = validateForm(form);
    if (validationError) {
      setSubmitError(validationError);
      return;
    }

    if (mode === "edit" && (!isDetailReady || !hasUnsavedChanges)) {
      return;
    }

    setSubmitError(null);
    setIsSubmitting(true);

    try {
      const estNum = Number(form.estNum.trim());

      if (mode === "create") {
        const createPayload = {
          estNum,
          accountCode: form.accountCode.trim().toUpperCase(),
          flightStart: form.flightStart,
          flightEnd: form.flightEnd,
          mediaType: form.mediaType.trim().toUpperCase(),
          buyer: form.buyer.trim(),
          note: form.note.trim(),
        };

        await requestJson(CREATE_EST_NUM_URL, {
          method: "POST",
          headers,
          body: createPayload,
          successToast: {
            title: "Estimate created",
            message: `EstNum ${estNum} was created successfully.`,
          },
          errorToast: {
            title: "Create failed",
          },
        });
      } else {
        const updatePayload = {
          estNum,
          accountCode: form.accountCode.trim().toUpperCase(),
          flightStart: form.flightStart,
          flightEnd: form.flightEnd,
          mediaType: form.mediaType.trim().toUpperCase(),
          buyer: form.buyer.trim(),
          note: form.note.trim(),
        };

        await requestJson(UPDATE_EST_NUM_URL, {
          method: "PUT",
          headers,
          body: updatePayload,
          successToast: {
            title: "Estimate updated",
            message: `EstNum ${estNum} was updated successfully.`,
          },
          errorToast: {
            title: "Update failed",
          },
        });
      }

      await onSuccess?.({
        mode,
        estNum,
        accountCode: form.accountCode.trim().toUpperCase(),
        flightStart: form.flightStart,
        flightEnd: form.flightEnd,
        mediaType: form.mediaType.trim().toUpperCase(),
        buyer: form.buyer.trim(),
        note: form.note.trim() || null,
      });
      writeBrowserCache(
        `estnum-detail:${form.accountCode.trim().toUpperCase()}:${estNum}`,
        {
          estNum,
          accountCode: form.accountCode.trim().toUpperCase(),
          flightStart: form.flightStart,
          flightEnd: form.flightEnd,
          mediaType: form.mediaType.trim().toUpperCase(),
          buyer: form.buyer.trim(),
          note: form.note.trim() || null,
        },
        ESTNUM_DETAIL_CACHE_TTL_MS,
        { source: "network" },
      );
      setDetailCacheStatus({
        source: "network",
        fetchedAt: Date.now(),
      });
      onOpenChange(false);
    } catch (error) {
      const errorMessage =
        error instanceof Error
          ? error.message
          : mode === "create"
            ? "Unable to create estimate number."
            : "Unable to update estimate number.";
      setSubmitError(
        errorMessage,
      );
    } finally {
      setIsSubmitting(false);
    }
  }

  const modalTitle = isEditMode ? "Edit Estimate Number" : "Estimate Number";
  const submitLabel = isEditMode ? "Save Changes" : "Create";
  const description = isEditMode
    ? isReadOnly
      ? "View the selected estimate number."
      : "Update the selected estimate number."
    : isReadOnly
      ? "View estimate number details."
      : "Create a new estimate number for the selected TradSphere account.";
  const formValidationError = isDetailReady ? validateForm(form) : null;
  const flightDateValidationError = isDetailReady ? getFlightRangeValidationError(form) : null;
  const flightErrorMessage = flightRangeError || flightDateValidationError;
  const detailStatusText = isEditMode
    ? isLoadingDetail
      ? isRefreshingDetail
        ? "Refreshing..."
        : "Loading..."
      : detailCacheStatus
        ? `Data source: ${detailCacheStatus.source}. Last updated ${formatRelativeTime(detailCacheStatus.fetchedAt)}.`
        : "No cached data yet"
    : null;
  const canSubmit =
    isDetailReady &&
    !formValidationError &&
    (!isEditMode || hasUnsavedChanges) &&
    canEdit;
  const shouldShowSubmitButton = canEdit && (canSubmit || isSubmitting);
  const footerActions = (
    <>
      {canEdit && hasUnsavedChanges ? (
        <Button variant="outline" onClick={handleRevertChanges} disabled={isSubmitting || !originalForm}>
          Revert
        </Button>
      ) : null}
      {shouldShowSubmitButton ? (
        <Button onClick={handleSubmit} disabled={isSubmitting}>
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
    </>
  );
  const shouldShowFooterActions = Boolean(canEdit && (hasUnsavedChanges || shouldShowSubmitButton));

  return (
    <Dialog open={open} onOpenChange={handleDialogOpenChange}>
      <DialogContent
        className="max-w-[620px] rounded-xl bg-white p-6"
        onPointerDownOutside={(event) => {
          const target = event.target;
          if (target instanceof Element && target.closest(FLIGHT_DATE_PICKER_POPOVER_SELECTOR)) {
            event.preventDefault();
          }
        }}
        onFocusOutside={(event) => {
          const target = event.target;
          if (target instanceof Element && target.closest(FLIGHT_DATE_PICKER_POPOVER_SELECTOR)) {
            event.preventDefault();
          }
        }}
        onInteractOutside={(event) => {
          const target = event.target;
          if (target instanceof Element && target.closest(FLIGHT_DATE_PICKER_POPOVER_SELECTOR)) {
            event.preventDefault();
            return;
          }
          if (
            shouldBlockOutsideClose({
              isBusy: isSubmitting,
              hasUnsavedChanges,
            })
          ) {
            event.preventDefault();
          }
        }}
        >
          <ModalShell busy={isSubmitting} busyMessage="Saving estimate number..." className="min-h-0 flex-1">
            <ModalHeaderRow
              actions={(
                <DialogClose asChild aria-label="Close estimate number modal">
                  <ModalCloseButton icon={<X className="size-4" />} />
                </DialogClose>
              )}
            >
              <DialogHeader>
                <DialogTitle>{modalTitle}</DialogTitle>
                <DialogDescription>{description}</DialogDescription>
              </DialogHeader>
            </ModalHeaderRow>

          {isEditMode && !isDetailReady ? (
            <div className="mt-8 flex min-h-52 flex-col items-center justify-center gap-3 text-center">
              {detailError ? (
                <p className="text-sm text-amber-700">{detailError}</p>
              ) : (
                <>
                  <Loader2 className="size-5 animate-spin text-slate-500" />
                  <p className="text-sm text-slate-600">Loading estimate number...</p>
                </>
              )}
            </div>
          ) : (
            <div className="mt-4 space-y-4">
            <LabeledField
              label={
                <>
                  Account<RequiredMark />
                </>
              }
            >
              {isEditMode ? (
                <ReadOnlyValue value={accountName?.trim() || form.accountCode} />
              ) : (
                <AppDropdown
                  ariaLabel="Account"
                  value={form.accountCode}
                  options={accountOptions}
                  onValueChange={(value) => updateForm("accountCode", value.trim().toUpperCase())}
                  placeholder="Select account"
                  disabled={isSubmitting || isReadOnly}
                  className="w-full"
                  emptyText="No accounts found."
                />
              )}
            </LabeledField>

            <LabeledField
              label={
                <>
                  EstNum<RequiredMark />
                </>
              }
            >
              {isEditMode ? (
                <ReadOnlyValue value={form.estNum} />
              ) : (
                <Input
                  id="estnum-number"
                  type="number"
                  min={0}
                  max={EST_NUM_MAX_UNSIGNED_INT}
                  step={1}
                  value={form.estNum}
                  onChange={(event) => updateForm("estNum", event.target.value)}
                  placeholder="e.g. 26001"
                  disabled={isSubmitting || isReadOnly}
                />
              )}
            </LabeledField>

            <LabeledField
              label={
                <>
                  Media Type<RequiredMark />
                </>
              }
            >
              <AppDropdown
                ariaLabel="Media type"
                value={form.mediaType}
                options={MEDIA_TYPE_OPTIONS}
                onValueChange={(value) => updateForm("mediaType", value)}
                placeholder="Select media type"
                disabled={isSubmitting || isReadOnly}
                searchable={false}
                className="w-full"
                emptyText="No media type found."
              />
            </LabeledField>

            <LabeledField
              label={
                <>
                  Buyer<RequiredMark />
                </>
              }
            >
              <Input
                id="estnum-buyer"
                value={buyerField.value}
                onChange={buyerField.onChange}
                onBlur={buyerField.onBlur}
                placeholder="Buyer name"
                maxLength={36}
                disabled={isSubmitting || isReadOnly}
              />
            </LabeledField>

            <LabeledField
              label={
                <>
                  Flight Dates<RequiredMark />
                </>
              }
            >
              <FlightDateRangeField
                flightStart={form.flightStart}
                flightEnd={form.flightEnd}
                onFlightStartChange={(value) => updateFlightDateManually("flightStart", value)}
                onFlightEndChange={(value) => updateFlightDateManually("flightEnd", value)}
                flightRangePreset={flightRangePreset}
                onFlightRangePresetChange={setFlightRangePreset}
                onApplyFlightRangePreset={applyFlightRangePreset}
                onFlightRangeError={setFlightRangeError}
                defaultMonth={chicagoToday.month}
                defaultYear={chicagoToday.year}
                disabled={isSubmitting || isReadOnly}
              />
            </LabeledField>

            {flightErrorMessage ? <p className="ml-[126px] text-sm text-rose-600">{flightErrorMessage}</p> : null}

            <LabeledField label="Note" alignStart>
              <Textarea
                id="estnum-note"
                value={noteField.value}
                onChange={noteField.onChange}
                onBlur={noteField.onBlur}
                placeholder="Optional note"
                maxLength={2048}
                disabled={isSubmitting || isReadOnly}
              />
            </LabeledField>
          </div>
        )}

          {submitError ? <p className="mt-2 text-sm text-rose-600">{submitError}</p> : null}

          {detailStatusText ? (
            <ModalCacheFooter
              text={detailStatusText}
              onRefresh={() => {
                if (!isLoadingDetail && !isSubmitting && isEditMode && !hasUnsavedChanges) {
                  setDetailRefreshToken((current) => current + 1);
                }
              }}
              disabled={!isEditMode || isLoadingDetail || isSubmitting || hasUnsavedChanges}
              refreshing={isRefreshingDetail}
              refreshLabel="Refresh estimate detail"
              tooltipText={
                hasUnsavedChanges
                  ? "Save or discard your edits before refreshing estimate detail."
                  : "Click to refresh this data"
              }
              actions={shouldShowFooterActions ? footerActions : null}
            />
          ) : shouldShowSubmitButton ? (
            <DialogFooter>
              <Button onClick={handleSubmit} disabled={isSubmitting}>
                {isSubmitting ? (
                  <>
                    <Loader2 className="size-4 animate-spin" />
                    Saving...
                  </>
                ) : (
                  submitLabel
                )}
              </Button>
            </DialogFooter>
          ) : null}
          {hasDeferredDetailUpdate ? (
            <p className="mt-2 text-sm text-amber-700">
              Newer estimate data is available and will apply after your current edits are saved or discarded.
            </p>
          ) : null}
        </ModalShell>
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
