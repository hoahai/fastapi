import { useEffect, useMemo, useRef, useState, type ClipboardEvent as ReactClipboardEvent, type DragEvent as ReactDragEvent, type ReactNode } from "react";
import { Paperclip, Trash2, UploadCloud } from "lucide-react";

import { cn } from "../utils/cn";

export type ImageUploadFieldItem = {
  key: string;
  label: string;
  meta?: ReactNode;
  previewSrc?: string | null;
  openUrl?: string | null;
  placeholder?: ReactNode;
  previewAriaLabel?: string;
  openAriaLabel?: string;
  removeAriaLabel?: string;
};

export type ImageUploadFieldProps = {
  buttonLabel: ReactNode;
  helperText?: ReactNode;
  statusText?: ReactNode;
  errorText?: ReactNode;
  accept?: string;
  multiple?: boolean;
  disabled?: boolean;
  isBusy?: boolean;
  allowPaste?: boolean;
  allowDrop?: boolean;
  className?: string;
  buttonClassName?: string;
  itemListClassName?: string;
  items?: ImageUploadFieldItem[];
  onFilesSelected: (files: File[]) => void | Promise<void>;
  onPreviewItem?: (item: ImageUploadFieldItem) => void;
  onOpenItem?: (item: ImageUploadFieldItem) => void;
  onRemoveItem?: (item: ImageUploadFieldItem) => void;
};

const DEFAULT_ACCEPT = ".png,.jpg,.jpeg,.webp,image/png,image/jpeg,image/webp";

function normalizeClipboardFile(file: File, index: number): File {
  if (file.name) {
    return file;
  }
  const extension = file.type.split("/")[1]?.toLowerCase() || "png";
  const fallbackName = `pasted-image-${Date.now()}-${index + 1}.${extension}`;
  return new File([file], fallbackName, { type: file.type || "image/png" });
}

export function ImageUploadField({
  buttonLabel,
  helperText,
  statusText,
  errorText,
  accept = DEFAULT_ACCEPT,
  multiple = false,
  disabled = false,
  isBusy = false,
  allowPaste = true,
  allowDrop = true,
  className,
  buttonClassName,
  itemListClassName,
  items = [],
  onFilesSelected,
  onPreviewItem,
  onOpenItem,
  onRemoveItem,
}: ImageUploadFieldProps) {
  const [brokenPreviewByKey, setBrokenPreviewByKey] = useState<Record<string, true>>({});
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const itemSignature = useMemo(() => items.map((item) => item.key).join("|"), [items]);

  useEffect(() => {
    setBrokenPreviewByKey({});
  }, [itemSignature]);

  function openPicker() {
    if (disabled || isBusy) {
      return;
    }
    if (fileInputRef.current) {
      fileInputRef.current.value = "";
      fileInputRef.current.click();
    }
  }

  function handleFiles(nextFiles: File[]) {
    const selectedFiles = multiple ? nextFiles : nextFiles.slice(0, 1);
    if (selectedFiles.length === 0) {
      return;
    }
    void onFilesSelected(selectedFiles);
  }

  function handleFileInputChange(fileList: FileList | null) {
    const nextFiles = Array.from(fileList ?? []);
    if (fileInputRef.current) {
      fileInputRef.current.value = "";
    }
    handleFiles(nextFiles);
  }

  function handlePaste(event: ReactClipboardEvent<HTMLDivElement>) {
    if (!allowPaste || disabled || isBusy) {
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
    handleFiles(clipboardFiles);
  }

  function handleDrop(event: ReactDragEvent<HTMLDivElement>) {
    if (!allowDrop || disabled || isBusy) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    const droppedFiles = Array.from(event.dataTransfer?.files ?? []);
    if (droppedFiles.length === 0) {
      return;
    }
    handleFiles(droppedFiles);
  }

  return (
    <div
      className={cn("space-y-3", className)}
      onPaste={handlePaste}
      onDragOver={(event) => {
        if (allowDrop) {
          event.preventDefault();
        }
      }}
      onDrop={handleDrop}
    >
      <input
        ref={fileInputRef}
        type="file"
        accept={accept}
        multiple={multiple}
        className="hidden"
        onChange={(event) => handleFileInputChange(event.target.files)}
        disabled={disabled || isBusy}
      />

      <button
        type="button"
        className={cn(
          "flex w-full items-center justify-center gap-2 rounded-lg border border-dashed px-3 py-4 text-sm transition",
          buttonClassName,
          disabled || isBusy
            ? "cursor-not-allowed border-blue-100 bg-blue-50/20 text-slate-400"
            : "cursor-pointer border-blue-200 bg-blue-50/30 text-slate-700 hover:border-blue-300 hover:bg-blue-50/50",
        )}
        onClick={openPicker}
        disabled={disabled || isBusy}
      >
        <UploadCloud className="size-4 text-blue-600" />
        {buttonLabel}
      </button>

      {items.length > 0 ? (
        <div className={cn("space-y-2", itemListClassName)}>
          {items.map((item) => {
            const hasPreview = Boolean(item.previewSrc) && !brokenPreviewByKey[item.key];
            const previewAriaLabel = item.previewAriaLabel || `Preview ${item.label}`;
            const openAriaLabel = item.openAriaLabel || `Open ${item.label}`;
            const removeAriaLabel = item.removeAriaLabel || `Remove ${item.label}`;
            return (
              <div key={item.key} className="flex items-center justify-between gap-3 rounded-md border border-slate-200 bg-white px-3 py-2 text-xs">
                <div className="flex min-w-0 items-center gap-3">
                  {hasPreview ? (
                    <button
                      type="button"
                      className="flex h-12 w-12 shrink-0 cursor-zoom-in items-center justify-center overflow-hidden rounded-md border border-slate-200 bg-slate-50"
                      onClick={() => {
                        onPreviewItem?.(item);
                      }}
                      disabled={disabled || isBusy}
                      aria-label={previewAriaLabel}
                    >
                      <img
                        src={item.previewSrc || undefined}
                        alt={item.label}
                        className="h-full w-full object-cover"
                        onError={() => {
                          setBrokenPreviewByKey((current) => ({ ...current, [item.key]: true }));
                        }}
                      />
                    </button>
                  ) : item.openUrl ? (
                    <button
                      type="button"
                      className="flex h-12 w-12 shrink-0 items-center justify-center rounded-md border border-slate-200 bg-slate-50 text-slate-500 hover:bg-slate-100"
                      aria-label={openAriaLabel}
                      onClick={() => {
                        onOpenItem?.(item);
                      }}
                      disabled={disabled || isBusy}
                    >
                      <Paperclip className="size-4" aria-hidden="true" />
                    </button>
                  ) : (
                    <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-md border border-slate-200 bg-slate-50 text-slate-500">
                      {item.placeholder ?? <Paperclip className="size-4" aria-hidden="true" />}
                    </div>
                  )}
                  <div className="min-w-0">
                    <p className="truncate font-medium text-slate-800">{item.label}</p>
                    {item.meta ? <p className="text-slate-500">{item.meta}</p> : null}
                  </div>
                </div>
                {onRemoveItem ? (
                  <button
                    type="button"
                    className="inline-flex size-6 items-center justify-center rounded-md text-slate-500 transition hover:bg-slate-100 hover:text-slate-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-70"
                    onClick={() => {
                      onRemoveItem(item);
                    }}
                    disabled={disabled || isBusy}
                    aria-label={removeAriaLabel}
                  >
                    <Trash2 className="size-4" aria-hidden="true" />
                  </button>
                ) : null}
              </div>
            );
          })}
        </div>
      ) : null}

      {helperText ? <p className="text-xs text-slate-500">{helperText}</p> : null}
      {statusText ? <p className="text-xs text-slate-500">{statusText}</p> : null}
      {errorText ? <p className="text-sm text-rose-600">{errorText}</p> : null}
    </div>
  );
}
