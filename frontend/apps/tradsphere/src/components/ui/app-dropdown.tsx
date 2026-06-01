import { createPortal } from "react-dom";
import { type CSSProperties, type KeyboardEvent, useEffect, useMemo, useRef, useState } from "react";
import { Check, ChevronsUpDown } from "lucide-react";

import { cn } from "@/lib/utils";

import { Button } from "./button";
import { Spinner } from "./spinner";

export type AppDropdownOption = {
  value: string;
  label: string;
  keywords?: string;
  muted?: boolean;
};

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
    if (!filteredOptionsWithCustom.length) {
      if (event.key === "Escape") {
        event.preventDefault();
        setIsOpen(false);
      }
      return;
    }

    if (event.key === "ArrowDown") {
      event.preventDefault();
      setHighlightedIndex((current) => Math.min(current + 1, filteredOptionsWithCustom.length - 1));
      return;
    }

    if (event.key === "ArrowUp") {
      event.preventDefault();
      setHighlightedIndex((current) => Math.max(current - 1, 0));
      return;
    }

    if (event.key === "Enter") {
      event.preventDefault();
      const highlighted = filteredOptionsWithCustom[highlightedIndex];
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
  const triggerLabel = multiple
    ? selectedOptions.length === 0
      ? placeholder
      : selectedOptions.length <= 2
        ? selectedOptions.map((option) => option.label).join(", ")
        : `${selectedOptions[0]?.label || ""}, +${selectedOptions.length - 1}`
    : (selectedOption?.label || value || placeholder);
  const isTriggerMuted = !multiple && Boolean(selectedOption?.muted);

  return (
    <div ref={containerRef} data-app-dropdown-root="true" className={cn("relative w-full", className)}>
      <span ref={triggerRef} className="inline-flex w-full">
        <Button
          type="button"
          variant="outline"
          role="combobox"
          aria-label={ariaLabel}
          aria-expanded={isOpen}
          disabled={disabled || loading}
          className={cn(
            "w-full justify-between rounded-lg border-blue-100/95 bg-white/90 font-medium text-slate-800 shadow-[0_16px_34px_-24px_rgba(59,130,246,0.5)] backdrop-blur-sm transition-[border-color,box-shadow,background-color,transform] hover:border-blue-200 hover:bg-white/95 hover:-translate-y-0.5 focus-visible:translate-y-0 disabled:border-input disabled:bg-slate-100/80 disabled:text-slate-500 disabled:shadow-none",
            isOpen && "border-blue-200 bg-white/95 shadow-[0_20px_40px_-24px_rgba(59,130,246,0.52)]",
            isCompact ? "h-8 px-2.5 py-1.5 text-xs" : "h-10 px-3 py-2 text-sm",
          )}
          onClick={() => setIsOpen((current) => !current)}
        >
          <span className={cn("truncate text-left", isTriggerMuted && "text-slate-400")}>{triggerLabel}</span>
          {loading ? (
            <Spinner className={cn("ml-2 shrink-0", isCompact ? "size-3.5" : "size-4")} />
          ) : (
            <ChevronsUpDown className={cn("ml-2 shrink-0 opacity-50", isCompact ? "size-3.5" : "size-4")} />
          )}
        </Button>
      </span>

      {isOpen && menuStyle && typeof document !== "undefined"
        ? createPortal(
        <div
          ref={menuRef}
          data-app-dropdown-menu="true"
          style={menuStyle}
          className="pointer-events-auto rounded-xl border border-blue-100/95 bg-white/88 p-2 shadow-[0_16px_34px_-22px_rgba(59,130,246,0.5)] backdrop-blur-[14px]"
        >
          {searchable ? (
            <input
              type="text"
              className={cn(
                "mb-2 w-full rounded-lg border border-blue-100/85 bg-white/76 outline-none transition-[border-color,box-shadow,background-color] focus:border-blue-300 focus:bg-white/92 focus:ring-2 focus:ring-ring focus:ring-offset-1",
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
            {!filteredOptionsWithCustom.length ? (
              <p className={cn("px-2 py-3 text-slate-500", isCompact ? "text-xs" : "text-sm")}>
                {emptyText}
              </p>
            ) : (
              <ul>
                {filteredOptionsWithCustom.map((option, index) => {
                  const isSelected = multiple
                    ? selectedValues.includes(option.value)
                    : option.value === value;
                  const isHighlighted = index === highlightedIndex;
                  const isMuted = Boolean(option.muted);
                  return (
                    <li key={`${option.value}:${index}`}>
                      <button
                        type="button"
                        className={cn(
                          "flex w-full items-center justify-between rounded-lg border border-transparent px-2 text-left text-slate-800 transition-[background-color,border-color,box-shadow,color] hover:border-blue-300/60 hover:bg-blue-200/46",
                          isCompact ? "py-1.5 text-xs" : "py-2 text-sm",
                          isSelected && "border-blue-600/80 bg-blue-600/58 text-slate-950 shadow-[inset_0_1px_0_rgba(255,255,255,0.45),inset_0_0_0_1px_rgba(30,64,175,0.22),0_12px_22px_-16px_rgba(30,64,175,0.75)]",
                          isSelected && isHighlighted && "border-blue-700/85 bg-blue-700/62",
                          isSelected && !isHighlighted && "hover:border-blue-700/85 hover:bg-blue-700/60",
                          isHighlighted && !isSelected && "border-blue-300/70 bg-blue-300/50 text-slate-900",
                        )}
                        onMouseEnter={() => setHighlightedIndex(index)}
                        onClick={() => selectValue(option.value)}
                      >
                        <span
                          className={cn(
                            "truncate",
                            isSelected && "rounded-md bg-white/30 px-1.5 py-0.5 shadow-[inset_0_1px_0_rgba(255,255,255,0.35)]",
                            isMuted && "text-slate-400",
                          )}
                        >
                          {option.label}
                        </span>
                        <Check
                          className={cn(
                            isSelected ? "text-blue-700" : "text-blue-600",
                            isCompact ? "size-3.5" : "size-4",
                            isSelected ? "opacity-100" : "opacity-0",
                          )}
                        />
                      </button>
                    </li>
                  );
                })}
              </ul>
            )}
          </div>
        </div>,
        document.body,
      ) : null}
    </div>
  );
}
