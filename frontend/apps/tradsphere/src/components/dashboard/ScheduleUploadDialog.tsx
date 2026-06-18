import { useEffect, useRef, useState, type ChangeEvent, type DragEvent } from "react";
import { FileText, Loader2, UploadCloud, X } from "lucide-react";

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
import { canModalClose, shouldBlockOutsideClose } from "@/components/ui/modal-close-guard";
import { UnsavedChangesDialog } from "@/components/ui/unsaved-changes-dialog";
import { useApiRequest } from "@/hooks/useApiRequest";
import { cn } from "@/lib/utils";
import { ModalCloseButton, ModalHeaderRow, ModalShell } from "@shared/components";

interface ScheduleUploadDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  uploadUrl: string;
  headers: HeadersInit;
  onUploadSuccess?: (fileName: string) => void;
}

const ACCEPTED_FILE_EXTENSION = ".txt";

function isValidTextFile(file: File): boolean {
  return file.name.toLowerCase().endsWith(ACCEPTED_FILE_EXTENSION);
}

export function ScheduleUploadDialog({
  open,
  onOpenChange,
  uploadUrl,
  headers,
  onUploadSuccess,
}: ScheduleUploadDialogProps) {
  const { requestJson } = useApiRequest();
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [validationError, setValidationError] = useState<string | null>(null);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [isUploading, setIsUploading] = useState(false);
  const [isDragActive, setIsDragActive] = useState(false);
  const [isDiscardDialogOpen, setIsDiscardDialogOpen] = useState(false);
  const canUpload = Boolean(selectedFile) && !isUploading;
  const shouldShowUploadButton = canUpload || isUploading;

  useEffect(() => {
    if (!open && !isUploading) {
      setSelectedFile(null);
      setValidationError(null);
      setUploadError(null);
      setIsDragActive(false);
      setIsDiscardDialogOpen(false);
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
    }
  }, [open, isUploading]);

  function handleDialogOpenChange(nextOpen: boolean) {
    const hasUnsavedChanges = Boolean(selectedFile);
    const allowClose = canModalClose({
      nextOpen,
      isBusy: isUploading,
      hasUnsavedChanges,
    });
    if (!allowClose) {
      if (!nextOpen && hasUnsavedChanges && !isUploading) {
        setIsDiscardDialogOpen(true);
      }
      return;
    }
    onOpenChange(nextOpen);
  }

  function setFileFromList(fileList: FileList | null) {
    setValidationError(null);
    setUploadError(null);

    if (!fileList || fileList.length === 0) {
      setSelectedFile(null);
      return;
    }

    if (fileList.length > 1) {
      setSelectedFile(null);
      setValidationError("Only 1 file is allowed.");
      return;
    }

    const candidate = fileList[0];
    if (!isValidTextFile(candidate)) {
      setSelectedFile(null);
      setValidationError("Invalid file type. Please upload a .txt file.");
      return;
    }

    setSelectedFile(candidate);
  }

  function handleInputChange(event: ChangeEvent<HTMLInputElement>) {
    setFileFromList(event.target.files);
  }

  function handleDragOver(event: DragEvent<HTMLDivElement>) {
    event.preventDefault();
    event.stopPropagation();
    if (!isUploading) {
      setIsDragActive(true);
    }
  }

  function handleDragLeave(event: DragEvent<HTMLDivElement>) {
    event.preventDefault();
    event.stopPropagation();
    setIsDragActive(false);
  }

  function handleDrop(event: DragEvent<HTMLDivElement>) {
    event.preventDefault();
    event.stopPropagation();
    if (isUploading) {
      return;
    }
    setIsDragActive(false);
    setFileFromList(event.dataTransfer.files);
  }

  async function handleUpload() {
    if (!selectedFile || isUploading) {
      return;
    }

    setIsUploading(true);
    setValidationError(null);
    setUploadError(null);

    try {
      const formData = new FormData();
      formData.append("file", selectedFile);

      await requestJson(uploadUrl, {
        method: "POST",
        headers,
        body: formData,
        successToast: {
          title: "Upload completed",
          message: `Schedule file "${selectedFile.name}" uploaded successfully.`,
        },
        errorToast: {
          title: "Upload failed",
        },
      });

      const uploadedFileName = selectedFile.name;
      onUploadSuccess?.(uploadedFileName);
      onOpenChange(false);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Upload failed.";
      setUploadError(errorMessage);
    } finally {
      setIsUploading(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={handleDialogOpenChange}>
      <DialogContent
        className="flex max-h-[90vh] max-w-xl flex-col overflow-hidden rounded-xl bg-white p-6"
        onInteractOutside={(event) => {
          if (
            shouldBlockOutsideClose({
              isBusy: isUploading,
              hasUnsavedChanges: Boolean(selectedFile),
            })
          ) {
            event.preventDefault();
          }
        }}
      >
        <ModalShell busy={isUploading} busyMessage="Uploading schedule..." className="min-h-0 flex-1">
          <ModalHeaderRow
            actions={(
              <DialogClose asChild aria-label="Close upload schedule modal">
                <ModalCloseButton icon={<X className="size-4" />} />
              </DialogClose>
            )}
          >
            <DialogHeader>
              <DialogTitle>Upload STRATA Schedule File</DialogTitle>
              <DialogDescription>Drag and drop a .txt schedule file, or browse from your device.</DialogDescription>
            </DialogHeader>
          </ModalHeaderRow>

          <div
            role="button"
            tabIndex={0}
            className={cn(
              "mt-4 rounded-lg border border-dashed p-5 text-center transition-colors",
              isDragActive ? "border-blue-500 bg-blue-50/70" : "border-blue-200 bg-blue-50/30",
              isUploading ? "cursor-not-allowed opacity-70" : "cursor-pointer hover:border-blue-400 hover:bg-blue-50",
            )}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
            onClick={() => {
              if (!isUploading) {
                fileInputRef.current?.click();
              }
            }}
            onKeyDown={(event) => {
              if (isUploading) {
                return;
              }
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                fileInputRef.current?.click();
              }
            }}
            aria-disabled={isUploading}
          >
            <input
              ref={fileInputRef}
              type="file"
              accept=".txt,text/plain"
              className="hidden"
              onChange={handleInputChange}
              disabled={isUploading}
            />
            <UploadCloud className="mx-auto size-8 text-blue-500" aria-hidden="true" />
            <p className="mt-2 text-sm font-medium text-slate-700">Drop a .txt file here or click to select</p>
            <p className="mt-1 text-xs text-slate-500">Only 1 file is allowed.</p>
          </div>

          {selectedFile ? (
            <div className="mt-4 rounded-md border border-blue-100 bg-blue-50/60 px-3 py-2 text-sm text-slate-700">
              <p className="flex items-center gap-2 font-medium">
                <FileText className="size-4 text-blue-600" aria-hidden="true" />
                Selected file: {selectedFile.name}
              </p>
            </div>
          ) : null}

          {validationError ? <p className="mt-3 text-sm text-rose-600">{validationError}</p> : null}
          {uploadError ? <p className="mt-2 text-sm text-rose-600">{uploadError}</p> : null}

          {shouldShowUploadButton ? (
            <DialogFooter>
              <Button onClick={handleUpload} disabled={isUploading}>
                {isUploading ? (
                  <>
                    <Loader2 className="size-4 animate-spin" aria-hidden="true" />
                    Uploading...
                  </>
                ) : (
                  "Upload"
                )}
              </Button>
            </DialogFooter>
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
