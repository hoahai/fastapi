import { useEffect, useLayoutEffect, useMemo, useRef, useState, type ClipboardEvent as ReactClipboardEvent } from "react";
import { Bold, Italic, List, ListOrdered, RemoveFormatting, Underline } from "lucide-react";

import { normalizeRichTextHtml } from "@shared/utils/richText";

import { cn } from "@shared/components/utils/cn";

type RichTextCommand = "bold" | "italic" | "underline" | "insertUnorderedList" | "insertOrderedList";

type RichTextEditorProps = {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  disabled?: boolean;
  className?: string;
  editorClassName?: string;
  editorMinHeight?: string;
  ariaLabel?: string;
};

const COMMANDS: Array<{
  label: string;
  icon: typeof Bold;
  command: RichTextCommand | "removeFormat";
}> = [
  { label: "Bold", icon: Bold, command: "bold" },
  { label: "Italic", icon: Italic, command: "italic" },
  { label: "Underline", icon: Underline, command: "underline" },
  { label: "Bulleted list", icon: List, command: "insertUnorderedList" },
  { label: "Numbered list", icon: ListOrdered, command: "insertOrderedList" },
  { label: "Clear formatting", icon: RemoveFormatting, command: "removeFormat" },
];

function emitEditorHtml(editor: HTMLDivElement, onChange: (value: string) => void): void {
  onChange(normalizeRichTextHtml(editor.innerHTML));
}

export function RichTextEditor({
  value,
  onChange,
  placeholder = "Write here...",
  disabled = false,
  className,
  editorClassName,
  editorMinHeight,
  ariaLabel,
}: RichTextEditorProps) {
  const editorRef = useRef<HTMLDivElement | null>(null);
  const [isFocused, setIsFocused] = useState(false);
  const [selectionState, setSelectionState] = useState({
    bold: false,
    italic: false,
    underline: false,
    unorderedList: false,
    orderedList: false,
  });

  const normalizedValue = useMemo(() => normalizeRichTextHtml(value), [value]);
  const isEmpty = !normalizedValue.trim();

  useLayoutEffect(() => {
    const editor = editorRef.current;
    if (!editor) {
      return;
    }

    if (editor.innerHTML !== normalizedValue) {
      editor.innerHTML = normalizedValue;
    }
  }, [normalizedValue]);

  useEffect(() => {
    const handleSelectionChange = () => {
      const editor = editorRef.current;
      const selection = window.getSelection();
      if (!editor || !selection || selection.rangeCount === 0 || !editor.contains(selection.anchorNode)) {
        return;
      }

      setSelectionState({
        bold: document.queryCommandState("bold"),
        italic: document.queryCommandState("italic"),
        underline: document.queryCommandState("underline"),
        unorderedList: document.queryCommandState("insertUnorderedList"),
        orderedList: document.queryCommandState("insertOrderedList"),
      });
    };

    document.addEventListener("selectionchange", handleSelectionChange);
    return () => document.removeEventListener("selectionchange", handleSelectionChange);
  }, []);

  const runCommand = (command: RichTextCommand | "removeFormat") => {
    const editor = editorRef.current;
    if (!editor || disabled) {
      return;
    }

    editor.focus();
    document.execCommand("styleWithCSS", false, "false");
    document.execCommand(command, false);
    emitEditorHtml(editor, onChange);
  };

  const handleInput = () => {
    const editor = editorRef.current;
    if (!editor || disabled) {
      return;
    }
    emitEditorHtml(editor, onChange);
  };

  const handleBlur = () => {
    const editor = editorRef.current;
    if (!editor) {
      return;
    }
    const sanitized = normalizeRichTextHtml(editor.innerHTML);
    if (editor.innerHTML !== sanitized) {
      editor.innerHTML = sanitized;
    }
    onChange(sanitized);
    setIsFocused(false);
  };

  const handlePaste = (event: ReactClipboardEvent<HTMLDivElement>) => {
    if (disabled) {
      event.preventDefault();
      return;
    }

    event.preventDefault();
    const plainText = event.clipboardData.getData("text/plain");
    const htmlText = event.clipboardData.getData("text/html");
    const fallbackText = htmlText
      ? (() => {
          const temp = document.createElement("div");
          temp.innerHTML = htmlText;
          return temp.textContent || "";
        })()
      : "";
    const nextText = plainText || fallbackText;
    if (!nextText) {
      return;
    }

    document.execCommand("insertText", false, nextText);
    handleInput();
  };

  const editorFrameClassName = cn(
    "relative min-h-[120px] rounded-lg border border-slate-200 bg-white/96 shadow-[0_8px_20px_-18px_rgba(30,64,175,0.45)] transition-[border-color,box-shadow,background-color] focus-within:border-blue-300 focus-within:bg-white focus-within:shadow-[inset_0_0_0_1px_rgba(59,130,246,0.12)]",
    disabled && "cursor-not-allowed bg-slate-100/80 opacity-60",
    className,
  );

  return (
    <div className={editorFrameClassName}>
      <div className="flex flex-wrap items-center gap-1 border-b border-slate-200/80 px-2 py-2">
        {COMMANDS.map((item) => {
          const Icon = item.icon;
          const active = item.command === "bold"
            ? selectionState.bold
            : item.command === "italic"
              ? selectionState.italic
              : item.command === "underline"
                ? selectionState.underline
                : item.command === "insertUnorderedList"
                  ? selectionState.unorderedList
                  : item.command === "insertOrderedList"
                    ? selectionState.orderedList
                    : false;

          return (
            <button
              key={item.label}
              type="button"
              aria-label={item.label}
              title={item.label}
              disabled={disabled}
              aria-pressed={active}
              onMouseDown={(event) => event.preventDefault()}
              onClick={() => runCommand(item.command)}
              className={cn(
                "inline-flex h-8 items-center justify-center gap-1.5 rounded-md border px-2.5 text-xs font-semibold transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50",
                active
                  ? "border-blue-200 bg-blue-50 text-blue-700"
                  : "border-transparent bg-transparent text-slate-600 hover:border-slate-100 hover:bg-slate-50/80 hover:text-slate-800",
              )}
            >
              <Icon className="size-3.5" />
            </button>
          );
        })}
      </div>

      <div className="relative">
        {isEmpty && !isFocused ? (
          <div className="pointer-events-none absolute left-3.5 top-3 text-sm text-slate-400">
            {placeholder}
          </div>
        ) : null}
        <div
          ref={editorRef}
          role="textbox"
          aria-multiline="true"
          aria-label={ariaLabel || placeholder}
          contentEditable={!disabled}
          suppressContentEditableWarning
          style={editorMinHeight ? { minHeight: editorMinHeight } : undefined}
          className={cn(
            "px-3.5 py-3 text-sm leading-6 text-slate-800 outline-none",
            editorClassName,
          )}
          onFocus={() => setIsFocused(true)}
          onBlur={handleBlur}
          onInput={handleInput}
          onPaste={handlePaste}
        />
      </div>
    </div>
  );
}
