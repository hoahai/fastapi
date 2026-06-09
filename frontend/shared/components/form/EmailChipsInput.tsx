import { useCallback, useMemo, useState } from "react";
import { X } from "lucide-react";

import { splitEmailList, mergeUniqueEmails } from "../../utils/email";
import { TooltipTarget } from "../actions/TooltipTarget";
import { cn } from "../utils/cn";

export type EmailChipsInputProps = {
  value: string[];
  onChange: (nextEmails: string[]) => void;
  placeholder: string;
  disabled: boolean;
  labelByEmail: Record<string, string>;
  autoFocus?: boolean;
  className?: string;
};

export function EmailChipsInput({
  value,
  onChange,
  placeholder,
  disabled,
  labelByEmail,
  autoFocus,
  className,
}: EmailChipsInputProps) {
  const [draftValue, setDraftValue] = useState("");
  const [allChipsSelected, setAllChipsSelected] = useState(false);
  const allEmailsText = useMemo(
    () => value.map((entry) => String(entry).trim().toLowerCase()).filter(Boolean).join(", "),
    [value],
  );

  const commitDraft = useCallback(() => {
    const parsed = splitEmailList(draftValue);
    if (parsed.length > 0) {
      onChange(mergeUniqueEmails(value, parsed));
    }
    setDraftValue("");
    setAllChipsSelected(false);
  }, [draftValue, onChange, value]);

  return (
    <div
      className={cn(
        "flex min-h-10 w-full flex-wrap items-center gap-1 rounded-lg border border-input bg-white px-3.5 py-2 text-sm text-slate-800 shadow-[0_8px_20px_-18px_rgba(30,64,175,0.45)] transition-[border-color,box-shadow,background-color] hover:border-blue-200 hover:bg-white focus-within:border-blue-300 focus-within:bg-white focus-within:outline-none focus-within:shadow-[inset_0_0_0_1px_rgba(59,130,246,0.12)]",
        disabled ? "cursor-not-allowed bg-slate-100/80 opacity-60" : null,
        className,
      )}
    >
      {value.map((email) => {
        const normalizedEmail = String(email).trim().toLowerCase();
        const displayLabel = String(labelByEmail[normalizedEmail] ?? normalizedEmail);
        return (
          <TooltipTarget
            key={normalizedEmail}
            text={displayLabel === normalizedEmail ? normalizedEmail : `${displayLabel} <${normalizedEmail}>`}
          >
            <span
              className={cn(
                "inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs font-medium",
                allChipsSelected
                  ? "border-blue-300 bg-blue-100 text-blue-800"
                  : "border-slate-200 bg-slate-100 text-slate-700",
              )}
            >
              <span className="max-w-[14rem] truncate">{displayLabel}</span>
              {!disabled ? (
                <button
                  type="button"
                  onClick={() => onChange(value.filter((entry) => String(entry).trim().toLowerCase() !== normalizedEmail))}
                  className="inline-flex size-4 items-center justify-center rounded-full text-slate-500 transition hover:bg-slate-200 hover:text-slate-700 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                  aria-label={`Remove ${normalizedEmail}`}
                >
                  <X className="size-3" />
                </button>
              ) : null}
            </span>
          </TooltipTarget>
        );
      })}
      <input
        value={draftValue}
        autoFocus={autoFocus}
        onChange={(event) => {
          setDraftValue(event.target.value);
          if (allChipsSelected) {
            setAllChipsSelected(false);
          }
        }}
        onBlur={commitDraft}
        onCopy={(event) => {
          if (!allChipsSelected || !allEmailsText) {
            return;
          }
          event.preventDefault();
          event.clipboardData.setData("text/plain", allEmailsText);
        }}
        onPaste={(event) => {
          const pasted = event.clipboardData.getData("text");
          if (!/[;,\n]/.test(pasted)) {
            return;
          }
          event.preventDefault();
          if (allChipsSelected) {
            setAllChipsSelected(false);
          }
          const parsed = splitEmailList(pasted);
          if (parsed.length > 0) {
            onChange(mergeUniqueEmails(value, parsed));
          }
        }}
        onKeyDown={(event) => {
          const isSelectAllShortcut = (event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "a";
          if (isSelectAllShortcut && allEmailsText) {
            event.preventDefault();
            setAllChipsSelected(true);
            return;
          }
          if (event.key === "Enter" || event.key === "Tab" || event.key === "," || event.key === ";") {
            event.preventDefault();
            commitDraft();
            return;
          }
          if (event.key === "Backspace" && !draftValue && value.length > 0) {
            event.preventDefault();
            if (allChipsSelected) {
              setAllChipsSelected(false);
            }
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
