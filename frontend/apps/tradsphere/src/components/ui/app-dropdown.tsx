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
};

interface AppDropdownProps {
  value: string;
  options: AppDropdownOption[];
  onValueChange: (value: string) => void;
  placeholder?: string;
  ariaLabel?: string;
  disabled?: boolean;
  loading?: boolean;
  searchable?: boolean;
  emptyText?: string;
  className?: string;
  size?: "default" | "sm";
}

export function AppDropdown({
  value,
  options,
  onValueChange,
  placeholder = "Select option",
  ariaLabel,
  disabled = false,
  loading = false,
  searchable = true,
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
    onValueChange(nextValue);
    setIsOpen(false);
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
    if (!filteredOptions.length) {
      if (event.key === "Escape") {
        event.preventDefault();
        setIsOpen(false);
      }
      return;
    }

    if (event.key === "ArrowDown") {
      event.preventDefault();
      setHighlightedIndex((current) => Math.min(current + 1, filteredOptions.length - 1));
      return;
    }

    if (event.key === "ArrowUp") {
      event.preventDefault();
      setHighlightedIndex((current) => Math.max(current - 1, 0));
      return;
    }

    if (event.key === "Enter") {
      event.preventDefault();
      const highlighted = filteredOptions[highlightedIndex];
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
            "w-full justify-between rounded-md border-input bg-white font-normal",
            isCompact ? "h-8 px-2.5 py-1.5 text-xs" : "h-10 px-3 py-2 text-sm",
          )}
          onClick={() => setIsOpen((current) => !current)}
        >
          <span className="truncate text-left">{selectedOption?.label || placeholder}</span>
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
          className="pointer-events-auto rounded-md border border-input bg-white p-2 shadow-lg"
        >
          {searchable ? (
            <input
              type="text"
              className={cn(
                "mb-2 w-full rounded-md border border-input bg-white outline-none focus:ring-2 focus:ring-ring",
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
            {!filteredOptions.length ? (
              <p className={cn("px-2 py-3 text-slate-500", isCompact ? "text-xs" : "text-sm")}>
                {emptyText}
              </p>
            ) : (
              <ul>
                {filteredOptions.map((option, index) => {
                  const isSelected = option.value === value;
                  const isHighlighted = index === highlightedIndex;
                  return (
                    <li key={option.value}>
                      <button
                        type="button"
                        className={cn(
                          "flex w-full items-center justify-between rounded-md px-2 text-left",
                          isCompact ? "py-1.5 text-xs" : "py-2 text-sm",
                          isHighlighted && "bg-blue-50",
                          !isHighlighted && "hover:bg-slate-100",
                        )}
                        onMouseEnter={() => setHighlightedIndex(index)}
                        onClick={() => selectValue(option.value)}
                      >
                        <span className="truncate">{option.label}</span>
                        <Check
                          className={cn(
                            "text-blue-600",
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
