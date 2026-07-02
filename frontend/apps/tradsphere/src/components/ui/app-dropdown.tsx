import { createPortal } from "react-dom";
import { Fragment, type CSSProperties, type KeyboardEvent, useEffect, useMemo, useRef, useState } from "react";
import { Check, ChevronsUpDown, X } from "lucide-react";

import { cn } from "@/lib/utils";

import { Button } from "./button";
import { Spinner } from "./spinner";

export type AppDropdownOption = {
  value: string;
  label: string;
  keywords?: string;
  muted?: boolean;
  groupLabel?: string;
};

function formatOptionLabel(option: AppDropdownOption): string {
  return option.muted ? `${option.label} (inactive)` : option.label;
}

interface AppDropdownProps {
  value: string;
  options: AppDropdownOption[];
  onValueChange: (value: string) => void;
  values?: string[];
  onValuesChange?: (values: string[]) => void;
  multiple?: boolean;
  placeholder?: string;
  ariaLabel?: string;
  disabled?: boolean;
  loading?: boolean;
  searchable?: boolean;
  allowCustomValue?: boolean;
  emptyText?: string;
  className?: string;
  size?: "default" | "sm";
}

export function AppDropdown({
  value,
  options,
  onValueChange,
  values,
  onValuesChange,
  multiple = false,
  placeholder = "Select option",
  ariaLabel,
  disabled = false,
  loading = false,
  searchable = true,
  allowCustomValue = false,
  emptyText = "No option found.",
  className,
  size = "default",
}: AppDropdownProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const triggerRef = useRef<HTMLSpanElement | null>(null);
  const menuRef = useRef<HTMLDivElement | null>(null);
  const [isOpen, setIsOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [highlightedIndex, setHighlightedIndex] = useState(0);
  const [menuStyle, setMenuStyle] = useState<CSSProperties | null>(null);
  const [panelMaxHeight, setPanelMaxHeight] = useState<number>(320);
  const portalTarget = containerRef.current?.closest("[role='dialog']") as HTMLElement | null;

  const selectedOption = options.find((option) => option.value === value) ?? null;
  const selectedValues = useMemo(() => {
    if (!multiple) {
      return [];
    }
    const seen = new Set<string>();
    const normalized: string[] = [];
    for (const raw of values ?? []) {
      const candidate = String(raw || "").trim();
      if (!candidate || seen.has(candidate)) {
        continue;
      }
      seen.add(candidate);
      normalized.push(candidate);
    }
    return normalized;
  }, [multiple, values]);

  const selectedOptions = useMemo(() => {
    if (!multiple) {
      return [];
    }
    const selectedSet = new Set(selectedValues);
    return options.filter((option) => selectedSet.has(option.value));
  }, [multiple, options, selectedValues]);

  const filteredOptions = useMemo(() => {
    if (!searchable) {
      return options;
    }

    const normalizedQuery = query.trim().toLowerCase();
    if (!normalizedQuery) {
      return options;
    }

    return options.filter((option) =>
      [option.label, option.value, option.keywords ?? ""].join(" ").toLowerCase().includes(normalizedQuery),
    );
  }, [options, query, searchable]);

  const normalizedQuery = query.trim();
  const normalizedQueryLower = normalizedQuery.toLowerCase();
  const hasExactMatch = normalizedQueryLower.length > 0 && options.some((option) => (
    option.value.trim().toLowerCase() === normalizedQueryLower
    || option.label.trim().toLowerCase() === normalizedQueryLower
  ));
  const customOption = allowCustomValue && normalizedQuery && !hasExactMatch
    ? {
        value: normalizedQuery,
        label: `Use "${normalizedQuery}"`,
      }
    : null;
  const filteredOptionsWithCustom = customOption
    ? filteredOptions.concat(customOption)
    : filteredOptions;
  const displayedOptions = useMemo(() => {
    if (!multiple) {
      return filteredOptionsWithCustom;
    }

    const selectedSet = new Set(selectedValues);
    return filteredOptionsWithCustom
      .map((option, index) => ({
        option,
        index,
        isSelected: selectedSet.has(option.value),
      }))
      .sort((left, right) => {
        if (left.isSelected !== right.isSelected) {
          return left.isSelected ? -1 : 1;
        }
        return left.index - right.index;
      })
      .map(({ option }) => option);
  }, [filteredOptionsWithCustom, multiple, selectedValues]);

  useEffect(() => {
    if (!isOpen) {
      setQuery("");
      setHighlightedIndex(0);
    }
  }, [isOpen]);

  useEffect(() => {
    const handleOutsideClick = (event: MouseEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) {
        return;
      }
      if (containerRef.current?.contains(target)) {
        return;
      }
      if (menuRef.current?.contains(target)) {
        return;
      }
      if (!containerRef.current?.contains(target)) {
        setIsOpen(false);
      }
    };

    document.addEventListener("mousedown", handleOutsideClick);
    return () => {
      document.removeEventListener("mousedown", handleOutsideClick);
    };
  }, []);

  function selectValue(nextValue: string) {
    if (!multiple) {
      onValueChange(nextValue);
      setIsOpen(false);
      return;
    }

    const selectedSet = new Set(selectedValues);
    if (selectedSet.has(nextValue)) {
      selectedSet.delete(nextValue);
    } else {
      selectedSet.add(nextValue);
    }
    onValuesChange?.(Array.from(selectedSet));
    setHighlightedIndex(0);
  }

  function updateMenuPosition() {
    const trigger = triggerRef.current;
    if (!trigger) {
      return;
    }

    const rect = trigger.getBoundingClientRect();
    const viewportPadding = 12;
    const minPanelHeight = 220;
    const maxPanelHeight = 440;
    const availableBelow = window.innerHeight - rect.bottom - viewportPadding;
    const availableAbove = rect.top - viewportPadding;
    const preferAbove = availableBelow < minPanelHeight && availableAbove > availableBelow;
    const resolvedMaxHeight = Math.max(
      minPanelHeight,
      Math.min(maxPanelHeight, (preferAbove ? availableAbove : availableBelow) - 8),
    );

    setPanelMaxHeight(resolvedMaxHeight);
    setMenuStyle({
      position: "fixed",
      left: rect.left,
      top: preferAbove ? rect.top - 8 : rect.bottom + 8,
      width: rect.width,
      zIndex: 90,
      transform: preferAbove ? "translateY(-100%)" : "none",
    });
  }

  useEffect(() => {
    if (!isOpen) {
      setMenuStyle(null);
      return;
    }

    updateMenuPosition();
    const handlePositionUpdate = () => {
      updateMenuPosition();
    };

    window.addEventListener("resize", handlePositionUpdate);
    window.addEventListener("scroll", handlePositionUpdate, true);
    return () => {
      window.removeEventListener("resize", handlePositionUpdate);
      window.removeEventListener("scroll", handlePositionUpdate, true);
    };
  }, [isOpen]);

  function handleListKeyDown(event: KeyboardEvent<HTMLElement>) {
    if (!displayedOptions.length) {
      if (event.key === "Escape") {
        event.preventDefault();
        setIsOpen(false);
      }
      return;
    }

    if (event.key === "ArrowDown") {
      event.preventDefault();
      setHighlightedIndex((current) => Math.min(current + 1, displayedOptions.length - 1));
      return;
    }

    if (event.key === "ArrowUp") {
      event.preventDefault();
      setHighlightedIndex((current) => Math.max(current - 1, 0));
      return;
    }

    if (event.key === "Enter") {
      event.preventDefault();
      const highlighted = displayedOptions[highlightedIndex];
      if (highlighted) {
        selectValue(highlighted.value);
      }
      return;
    }

    if (event.key === "Escape") {
      event.preventDefault();
      setIsOpen(false);
    }
  }

  const isCompact = size === "sm";
  const canClearSelections = multiple && selectedValues.length > 0;
  const triggerLabel = multiple
    ? selectedOptions.length === 0
      ? placeholder
      : selectedOptions.length <= 2
        ? selectedOptions.map((option) => formatOptionLabel(option)).join(", ")
        : `${formatOptionLabel(selectedOptions[0] as AppDropdownOption)}, +${selectedOptions.length - 1}`
    : (selectedOption
      ? (selectedOption.label.trim() ? formatOptionLabel(selectedOption) : placeholder)
      : (value || placeholder));
  const isTriggerMuted = !multiple && Boolean(selectedOption?.muted);
  const panelSurfaceClasses =
    "relative isolate overflow-hidden rounded-lg border border-slate-200 bg-white/28 text-slate-900 shadow-xl ring-1 ring-slate-200/60 backdrop-blur-[32px] backdrop-saturate-150";
  const panelSheenClasses =
    "pointer-events-none absolute inset-0 bg-[linear-gradient(135deg,rgba(255,255,255,0.92)_0%,rgba(239,246,255,0.74)_22%,rgba(248,250,252,0.58)_55%,rgba(236,242,255,0.46)_100%)]";
  const selectedGradientClasses =
    "border-white/60 bg-[linear-gradient(135deg,rgba(255,255,255,0.78)_0%,rgba(239,246,255,0.66)_100%)] text-slate-900 shadow-[inset_0_1px_0_rgba(255,255,255,0.9),0_12px_26px_-20px_rgba(15,23,42,0.18)] backdrop-blur-xl";
  const selectedGradientHighlightedClasses =
    "border-white/72 bg-[linear-gradient(135deg,rgba(255,255,255,0.88)_0%,rgba(239,246,255,0.76)_100%)]";

  return (
    <div ref={containerRef} data-app-dropdown-root="true" className={cn("group relative w-full", className)}>
      <span ref={triggerRef} className="inline-flex w-full">
        <Button
          type="button"
          variant="outline"
          role="combobox"
          aria-label={ariaLabel}
          aria-expanded={isOpen}
          disabled={disabled || loading}
          className={cn(
            "w-full justify-start rounded-lg border border-input bg-white/96 font-medium text-slate-800 shadow-[0_8px_20px_-18px_rgba(30,64,175,0.45)] backdrop-blur-sm transition-[border-color,box-shadow,background-color,transform] hover:!translate-y-0 hover:!scale-100 hover:border-blue-200 hover:bg-white active:!translate-y-0 active:!scale-100 focus-visible:translate-y-0 focus-visible:bg-white disabled:border-input disabled:bg-slate-100/80 disabled:text-slate-500 disabled:shadow-none",
            isOpen && "border-blue-300 bg-white shadow-[inset_0_0_0_1px_rgba(59,130,246,0.12)]",
            isCompact ? "h-8 px-2.5 py-1.5 text-xs" : "h-10 px-3 py-2 text-sm",
          )}
          onClick={() => setIsOpen((current) => !current)}
        >
          <span className={cn("min-w-0 flex-1 truncate text-left", isTriggerMuted && "text-slate-400")}>{triggerLabel}</span>
          <span className="ml-1.5 flex shrink-0 items-center gap-1">
            {canClearSelections ? (
              <span
                role="button"
                tabIndex={0}
                aria-label="Clear selections"
                title="Clear selections"
                className={cn(
                  "rounded-sm p-0.5 text-slate-500 opacity-0 transition-opacity hover:text-slate-800 group-hover:opacity-100 group-focus-within:opacity-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-300 focus-visible:ring-offset-2",
                  isCompact && "p-0",
                )}
                onMouseDown={(event) => {
                  event.preventDefault();
                  event.stopPropagation();
                }}
                onClick={(event) => {
                  event.preventDefault();
                  event.stopPropagation();
                  onValuesChange?.([]);
                  setHighlightedIndex(0);
                }}
                onKeyDown={(event) => {
                  if (event.key !== "Enter" && event.key !== " ") {
                    return;
                  }
                  event.preventDefault();
                  event.stopPropagation();
                  onValuesChange?.([]);
                  setHighlightedIndex(0);
                }}
              >
                <X className={cn("size-3", isCompact && "size-2.5")} />
              </span>
            ) : null}
            {loading ? (
              <Spinner className={cn("shrink-0", isCompact ? "size-3.5" : "size-4")} />
            ) : (
              <ChevronsUpDown className={cn("shrink-0 opacity-50", isCompact ? "size-3.5" : "size-3.5")} />
            )}
          </span>
        </Button>
      </span>

      {isOpen && menuStyle && typeof document !== "undefined"
        ? createPortal(
            <div
              ref={menuRef}
              data-app-dropdown-menu="true"
              style={menuStyle}
              className={cn("pointer-events-auto", panelSurfaceClasses)}
            >
              <div aria-hidden="true" className={panelSheenClasses} />
              <div className="relative z-10 p-2">
                {searchable ? (
                  <input
                    type="text"
                    className={cn(
                      "mb-2 w-full rounded-lg border border-white/45 bg-white/62 text-slate-900 outline-none shadow-[inset_0_1px_0_rgba(255,255,255,0.9),0_8px_18px_-14px_rgba(15,23,42,0.16)] transition-[border-color,box-shadow,background-color] placeholder:text-slate-500 hover:border-blue-200/60 hover:bg-white/72 focus:border-blue-300 focus:bg-white/80 focus:shadow-[inset_0_0_0_1px_rgba(59,130,246,0.1),0_10px_20px_-16px_rgba(15,23,42,0.16)]",
                      isCompact ? "h-8 px-2.5 text-xs" : "h-9 px-3 text-sm",
                    )}
                    placeholder="Search..."
                    value={query}
                    onChange={(event) => {
                      setQuery(event.target.value);
                      setHighlightedIndex(0);
                    }}
                    onKeyDown={handleListKeyDown}
                    autoFocus
                  />
                ) : null}

                <div
                  className="overflow-auto"
                  style={{
                    maxHeight: searchable ? Math.max(140, panelMaxHeight - 74) : Math.max(160, panelMaxHeight - 16),
                  }}
                  onKeyDown={handleListKeyDown}
                >
                  {!displayedOptions.length ? (
                    <p className={cn("px-2 py-3 text-slate-600", isCompact ? "text-xs" : "text-sm")}>
                      {emptyText}
                    </p>
                  ) : (
                    <ul>
                      {displayedOptions.map((option, index) => {
                        const isSelected = multiple
                          ? selectedValues.includes(option.value)
                          : option.value === value;
                        const isHighlighted = index === highlightedIndex;
                        const isMuted = Boolean(option.muted);
                        const groupLabel = option.groupLabel?.trim();
                        const previousGroupLabel = displayedOptions[index - 1]?.groupLabel?.trim();
                        const shouldRenderGroupLabel = Boolean(groupLabel) && previousGroupLabel !== groupLabel;
                        return (
                          <Fragment key={`${option.value}:${index}`}>
                            {shouldRenderGroupLabel ? (
                              <li aria-hidden="true" className="px-2 pb-1 pt-2">
                                <div className="flex items-center gap-2">
                                  <span className="shrink-0 text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">
                                    {groupLabel}
                                  </span>
                                  <span className="h-px flex-1 bg-slate-200/80" />
                                </div>
                              </li>
                            ) : null}
                            <li>
                              <button
                                type="button"
                                className={cn(
                                  "flex w-full items-center justify-between rounded-lg border border-transparent px-2 text-left text-slate-900 transition-[background-color,border-color,box-shadow,color,transform] hover:-translate-y-px hover:border-white/50 hover:bg-white/40 hover:shadow-[0_8px_18px_-16px_rgba(15,23,42,0.16)]",
                                  isCompact ? "py-1.5 text-xs" : "py-2 text-sm",
                                  isSelected && selectedGradientClasses,
                                  isSelected && isHighlighted && selectedGradientHighlightedClasses,
                                  isHighlighted && !isSelected && "border-white/68 bg-white/44 text-slate-950",
                                )}
                                onMouseEnter={() => setHighlightedIndex(index)}
                                onClick={() => selectValue(option.value)}
                              >
                                <span
                                  className={cn(
                                    "truncate",
                                    isSelected && "rounded-md bg-white/36 px-1.5 py-0.5 shadow-[inset_0_1px_0_rgba(255,255,255,0.86)]",
                                    isMuted && !isSelected && "text-slate-500",
                                  )}
                                >
                                  {option.label}
                                </span>
                                <div className="ml-2 flex items-center gap-1.5">
                                  {isMuted ? (
                                    <span
                                      className={cn(
                                        "shrink-0 rounded-full border px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-[0.12em]",
                                        isSelected
                                          ? "border-white/42 bg-white/48 text-slate-700"
                                          : "border-white/60 bg-white/62 text-slate-500",
                                      )}
                                    >
                                      Inactive
                                    </span>
                                  ) : null}
                                  <Check
                                    className={cn(
                                      isSelected ? "text-sky-600" : "text-slate-500",
                                      isCompact ? "size-3.5" : "size-4",
                                      isSelected ? "opacity-100" : "opacity-0",
                                    )}
                                  />
                                </div>
                              </button>
                            </li>
                          </Fragment>
                        );
                      })}
                    </ul>
                  )}
                </div>
              </div>
            </div>,
            portalTarget ?? document.body,
          )
        : null}
    </div>
  );
}
