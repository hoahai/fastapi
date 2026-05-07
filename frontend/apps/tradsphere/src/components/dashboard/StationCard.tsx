import { useEffect, useMemo, useRef, useState } from "react";
import { Copy } from "lucide-react";

import { EntityItemCard } from "@/components/dashboard/EntityItemCard";
import { FloatingActionMenu, type FloatingActionMenuItem } from "@/components/dashboard/FloatingActionMenu";
import { useToast } from "@/components/ui/toast";
import { cn } from "@/lib/utils";

import type { StationItem } from "./types";

interface StationCardProps {
  station: StationItem;
  onClick?: () => void;
  disabled?: boolean;
}

export function StationCard({ station, onClick, disabled = false }: StationCardProps) {
  const toast = useToast();
  const anchorRef = useRef<HTMLDivElement | null>(null);
  const closeTimerRef = useRef<number | null>(null);
  const [isCardHovered, setIsCardHovered] = useState(false);
  const [isMenuHovered, setIsMenuHovered] = useState(false);
  const [isFocusWithin, setIsFocusWithin] = useState(false);

  const resolvedContact = useMemo(() => {
    const contacts = station.repContacts ?? [];
    const withEmail = contacts.find((contact) => (contact.email ?? "").trim());
    const fallback = contacts[0];
    const selected = withEmail ?? fallback;
    const fullName = selected?.fullName?.trim() || "";
    const email = selected?.email?.trim() || "";
    const copyValue = email ? (fullName ? `${fullName} <${email}>` : email) : "";
    return {
      fullName,
      email,
      copyValue,
      canCopy: Boolean(copyValue),
    };
  }, [station.repContacts]);

  function clearCloseTimer() {
    if (closeTimerRef.current !== null) {
      window.clearTimeout(closeTimerRef.current);
      closeTimerRef.current = null;
    }
  }

  function queueMenuClose() {
    clearCloseTimer();
    closeTimerRef.current = window.setTimeout(() => {
      setIsCardHovered(false);
      setIsMenuHovered(false);
      setIsFocusWithin(false);
    }, 110);
  }

  useEffect(() => {
    return () => {
      if (closeTimerRef.current !== null) {
        window.clearTimeout(closeTimerRef.current);
      }
    };
  }, []);

  async function copyContactToClipboard(value: string): Promise<void> {
    if (!value) {
      throw new Error("Contact value is empty");
    }

    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(value);
      return;
    }

    const textArea = document.createElement("textarea");
    textArea.value = value;
    textArea.setAttribute("readonly", "");
    textArea.style.position = "fixed";
    textArea.style.left = "-9999px";
    textArea.style.top = "0";
    document.body.appendChild(textArea);
    textArea.select();
    try {
      const didCopy = document.execCommand("copy");
      if (!didCopy) {
        throw new Error("execCommand copy failed");
      }
    } finally {
      document.body.removeChild(textArea);
    }
  }

  const hasCopyContact = resolvedContact.canCopy;
  const isMenuOpen = hasCopyContact && (isCardHovered || isMenuHovered || isFocusWithin);
  const menuItems = useMemo<FloatingActionMenuItem[]>(() => {
    if (!resolvedContact.canCopy) {
      return [];
    }
    return [
      {
        key: "copy-contact",
        label: "Copy contact",
        icon: <Copy className="size-3.5" />,
        onSelect: () => {
          void copyContactToClipboard(resolvedContact.copyValue)
            .then(() => {
              toast.success("Copied contact");
            })
            .catch(() => {
              toast.error("Could not copy contact");
            });
        },
      },
    ];
  }, [resolvedContact.canCopy, resolvedContact.copyValue, toast]);

  return (
    <>
      <div
        ref={anchorRef}
        className="shrink-0"
        onMouseEnter={() => {
          clearCloseTimer();
          setIsCardHovered(true);
        }}
        onMouseLeave={() => {
          setIsCardHovered(false);
          if (!isMenuHovered && !isFocusWithin) {
            queueMenuClose();
          }
        }}
        onFocusCapture={() => {
          clearCloseTimer();
          setIsFocusWithin(true);
        }}
        onBlurCapture={() => {
          setIsFocusWithin(false);
          if (!isCardHovered && !isMenuHovered) {
            queueMenuClose();
          }
        }}
      >
        <EntityItemCard
          rootAs={onClick ? "button" : "article"}
          rootClassName="min-w-52 p-5"
          onClick={onClick}
          disabled={disabled}
          circleClassName="mb-4 text-xl font-bold tracking-tight"
          circleContent={station.code}
          title={resolvedContact.fullName}
          subtitle={resolvedContact.email}
          subtitleClassName={cn("text-sm", resolvedContact.email ? "text-slate-500" : "text-slate-400")}
        />
      </div>
      {hasCopyContact ? (
        <FloatingActionMenu
          open={isMenuOpen}
          anchorRef={anchorRef}
          belowOffsetY={-24}
          items={menuItems}
          onClose={() => {
            clearCloseTimer();
            setIsCardHovered(false);
            setIsMenuHovered(false);
            setIsFocusWithin(false);
          }}
          onPointerEnter={() => {
            clearCloseTimer();
            setIsMenuHovered(true);
          }}
          onPointerLeave={() => {
            setIsMenuHovered(false);
            if (!isCardHovered && !isFocusWithin) {
              queueMenuClose();
            }
          }}
        />
      ) : null}
    </>
  );
}
