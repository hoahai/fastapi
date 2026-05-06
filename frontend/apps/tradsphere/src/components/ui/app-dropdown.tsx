import { type KeyboardEvent, useEffect, useMemo, useRef, useState } from "react";
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
}: AppDropdownProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [isOpen, setIsOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [highlightedIndex, setHighlightedIndex] = useState(0);

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

  return (
    <div ref={containerRef} className={cn("relative w-full", className)}>
      <Button
        type="button"
        variant="outline"
        role="combobox"
        aria-label={ariaLabel}
        aria-expanded={isOpen}
        disabled={disabled || loading}
        className="h-10 w-full justify-between rounded-md border-input bg-white px-3 py-2 text-sm font-normal"
        onClick={() => setIsOpen((current) => !current)}
      >
        <span className="truncate text-left">{selectedOption?.label || placeholder}</span>
        {loading ? <Spinner className="ml-2 size-4" /> : <ChevronsUpDown className="ml-2 size-4 shrink-0 opacity-50" />}
      </Button>

      {isOpen ? (
        <div className="absolute z-50 mt-2 w-full rounded-md border border-input bg-white p-2 shadow-lg">
          {searchable ? (
            <input
              type="text"
              className="mb-2 h-9 w-full rounded-md border border-input bg-white px-3 text-sm outline-none focus:ring-2 focus:ring-ring"
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

          <div className="max-h-60 overflow-auto" onKeyDown={handleListKeyDown}>
            {!filteredOptions.length ? (
              <p className="px-2 py-3 text-sm text-slate-500">{emptyText}</p>
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
                          "flex w-full items-center justify-between rounded-md px-2 py-2 text-left text-sm",
                          isHighlighted && "bg-blue-50",
                          !isHighlighted && "hover:bg-slate-100",
                        )}
                        onMouseEnter={() => setHighlightedIndex(index)}
                        onClick={() => selectValue(option.value)}
                      >
                        <span className="truncate">{option.label}</span>
                        <Check className={cn("size-4 text-blue-600", isSelected ? "opacity-100" : "opacity-0")} />
                      </button>
                    </li>
                  );
                })}
              </ul>
            )}
          </div>
        </div>
      ) : null}
    </div>
  );
}
