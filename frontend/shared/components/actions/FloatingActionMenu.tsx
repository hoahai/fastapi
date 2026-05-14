import {
  type ReactNode,
  type RefObject,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { createPortal } from "react-dom";

import { cn } from "../utils/cn";

export type FloatingActionMenuItem = {
  key: string;
  label: string;
  onSelect: () => void;
  icon?: ReactNode;
  disabled?: boolean;
  variant?: "default" | "danger";
};

interface FloatingActionMenuProps {
  open: boolean;
  anchorRef: RefObject<HTMLElement | null>;
  belowOffsetY?: number;
  align?: "center" | "left";
  items: FloatingActionMenuItem[];
  onClose: () => void;
  onPointerEnter?: () => void;
  onPointerLeave?: () => void;
}

type MenuPosition = {
  top: number;
  left: number;
  placement: "below" | "above";
};

const MENU_WIDTH = 196;
const ITEM_HEIGHT = 34;
const MENU_PADDING = 8;
const MENU_GAP = 6;
const VIEWPORT_PADDING = 8;

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

export function FloatingActionMenu({
  open,
  anchorRef,
  belowOffsetY = 0,
  align = "center",
  items,
  onClose,
  onPointerEnter,
  onPointerLeave,
}: FloatingActionMenuProps) {
  const menuRef = useRef<HTMLDivElement | null>(null);
  const [position, setPosition] = useState<MenuPosition | null>(null);

  const estimatedMenuHeight = useMemo(() => {
    return items.length * ITEM_HEIGHT + MENU_PADDING * 2;
  }, [items.length]);

  useLayoutEffect(() => {
    if (!open) {
      setPosition(null);
      return;
    }

    const anchor = anchorRef.current;
    if (!anchor) {
      setPosition(null);
      return;
    }

    const rect = anchor.getBoundingClientRect();
    const spaceBelow = window.innerHeight - rect.bottom;
    const spaceAbove = rect.top;

    let placement: MenuPosition["placement"] = "below";
    if (spaceBelow < estimatedMenuHeight + MENU_GAP + VIEWPORT_PADDING) {
      placement = "above";
    }
    if (placement === "above" && spaceAbove < estimatedMenuHeight + MENU_GAP + VIEWPORT_PADDING) {
      placement = "below";
    }

    const left = align === "left"
      ? rect.left
      : rect.left + rect.width / 2 - MENU_WIDTH / 2;
    const top =
      placement === "above"
        ? rect.top - estimatedMenuHeight - MENU_GAP
        : rect.bottom + MENU_GAP + belowOffsetY;

    const maxLeft = window.innerWidth - MENU_WIDTH - VIEWPORT_PADDING;
    const minLeft = VIEWPORT_PADDING;
    const maxTop = window.innerHeight - estimatedMenuHeight - VIEWPORT_PADDING;
    const minTop = VIEWPORT_PADDING;

    setPosition({
      placement,
      left: clamp(left, minLeft, maxLeft),
      top: clamp(top, minTop, maxTop),
    });
  }, [align, anchorRef, belowOffsetY, estimatedMenuHeight, open]);

  useEffect(() => {
    if (!open) {
      return;
    }

    const updatePosition = () => {
      const anchor = anchorRef.current;
      if (!anchor) {
        return;
      }
      const rect = anchor.getBoundingClientRect();
      setPosition((current) => {
        if (!current) {
          return current;
        }
        const left = align === "left"
          ? rect.left
          : rect.left + rect.width / 2 - MENU_WIDTH / 2;
        const top =
          current.placement === "above"
            ? rect.top - estimatedMenuHeight - MENU_GAP
            : rect.bottom + MENU_GAP + belowOffsetY;

        const maxLeft = window.innerWidth - MENU_WIDTH - VIEWPORT_PADDING;
        const minLeft = VIEWPORT_PADDING;
        const maxTop = window.innerHeight - estimatedMenuHeight - VIEWPORT_PADDING;
        const minTop = VIEWPORT_PADDING;
        return {
          ...current,
          left: clamp(left, minLeft, maxLeft),
          top: clamp(top, minTop, maxTop),
        };
      });
    };

    const handlePointerDown = (event: MouseEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) {
        return;
      }
      if (anchorRef.current?.contains(target)) {
        return;
      }
      if (menuRef.current?.contains(target)) {
        return;
      }
      onClose();
    };

    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onClose();
      }
    };

    window.addEventListener("resize", updatePosition);
    window.addEventListener("scroll", updatePosition, true);
    document.addEventListener("mousedown", handlePointerDown);
    document.addEventListener("keydown", handleEscape);

    return () => {
      window.removeEventListener("resize", updatePosition);
      window.removeEventListener("scroll", updatePosition, true);
      document.removeEventListener("mousedown", handlePointerDown);
      document.removeEventListener("keydown", handleEscape);
    };
  }, [align, anchorRef, belowOffsetY, estimatedMenuHeight, onClose, open]);

  if (!open || !position || !items.length) {
    return null;
  }

  return createPortal(
    <div
      className="pointer-events-none fixed inset-0 z-[120]"
      aria-hidden={false}
      onClick={(event) => {
        event.stopPropagation();
      }}
    >
      <div
        ref={menuRef}
        className="pointer-events-auto"
        style={{
          position: "fixed",
          top: position.top,
          left: position.left,
          width: MENU_WIDTH,
        }}
        onMouseEnter={onPointerEnter}
        onMouseLeave={onPointerLeave}
        onClick={(event) => {
          event.stopPropagation();
        }}
        onMouseDown={(event) => {
          event.stopPropagation();
        }}
      >
        <div className="animate-in slide-in-from-top-1 fade-in-0 rounded-xl border border-slate-200 bg-white p-1.5 shadow-[0_14px_35px_rgba(15,23,42,0.16)] duration-150">
          {items.map((item) => (
            <button
              key={item.key}
              type="button"
              className={cn(
                "flex w-full items-center justify-start rounded-lg px-2.5 py-2 text-left text-sm font-medium transition-colors",
                "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-300 focus-visible:ring-offset-2",
                item.disabled && "cursor-not-allowed opacity-50",
                !item.disabled && item.variant === "danger" && "text-rose-600 hover:bg-rose-50",
                !item.disabled && item.variant !== "danger" && "text-slate-700 hover:bg-slate-100",
              )}
              disabled={item.disabled}
              onClick={(event) => {
                event.stopPropagation();
                if (item.disabled) {
                  return;
                }
                item.onSelect();
                onClose();
              }}
            >
              <span className="inline-flex w-full items-center justify-start gap-2">
                {item.icon ? (
                  <span
                    aria-hidden
                    className="inline-flex size-4 items-center justify-center"
                  >
                    {item.icon}
                  </span>
                ) : null}
                <span>{item.label}</span>
              </span>
            </button>
          ))}
        </div>
      </div>
    </div>,
    document.body,
  );
}
