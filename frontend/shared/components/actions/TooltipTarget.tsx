import {
  Children,
  cloneElement,
  isValidElement,
  type MouseEvent,
  type FocusEvent,
  type ReactElement,
  type Ref,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import { Tooltip, type TooltipPlacement } from "./Tooltip";

type TooltipTargetProps = {
  text?: string | null;
  children: ReactElement;
  openDelayMs?: number;
  offsetY?: number;
  placement?: TooltipPlacement;
  followCursor?: boolean;
  cursorOffsetX?: number;
  cursorOffsetY?: number;
  className?: string;
};

function setRefValue<T>(ref: Ref<T> | undefined, value: T) {
  if (!ref) {
    return;
  }
  if (typeof ref === "function") {
    ref(value);
    return;
  }
  (ref as { current: T }).current = value;
}

export function TooltipTarget({
  text,
  children,
  openDelayMs = 0,
  offsetY,
  placement,
  followCursor,
  cursorOffsetX,
  cursorOffsetY,
  className,
}: TooltipTargetProps) {
  const child = Children.only(children);
  const normalizedText = (text ?? "").trim();
  const enabled = normalizedText.length > 0;
  const anchorRef = useRef<HTMLElement | null>(null);
  const openDelayRef = useRef<number | null>(null);
  const [open, setOpen] = useState(false);

  const childProps = (isValidElement(child) ? child.props : {}) as Record<string, unknown>;

  const clearOpenDelay = () => {
    if (openDelayRef.current === null) {
      return;
    }
    window.clearTimeout(openDelayRef.current);
    openDelayRef.current = null;
  };

  useEffect(() => () => {
    clearOpenDelay();
  }, []);

  const renderedChild = useMemo(() => {
    if (!isValidElement(child)) {
      return child;
    }

    const originalOnMouseEnter = childProps.onMouseEnter as ((event: MouseEvent<HTMLElement>) => void) | undefined;
    const originalOnMouseLeave = childProps.onMouseLeave as ((event: MouseEvent<HTMLElement>) => void) | undefined;
    const originalOnFocus = childProps.onFocus as ((event: FocusEvent<HTMLElement>) => void) | undefined;
    const originalOnBlur = childProps.onBlur as ((event: FocusEvent<HTMLElement>) => void) | undefined;

    const mergedProps: Record<string, unknown> = {
      ...childProps,
      title: undefined,
      ref: (node: HTMLElement | null) => {
        anchorRef.current = node;
        setRefValue((child as ReactElement & { ref?: Ref<HTMLElement | null> }).ref, node);
      },
      onMouseEnter: (event: MouseEvent<HTMLElement>) => {
        originalOnMouseEnter?.(event);
        if (!enabled) {
          return;
        }
        clearOpenDelay();
        if (openDelayMs <= 0) {
          setOpen(true);
          return;
        }
        openDelayRef.current = window.setTimeout(() => {
          setOpen(true);
          openDelayRef.current = null;
        }, openDelayMs);
      },
      onMouseLeave: (event: MouseEvent<HTMLElement>) => {
        originalOnMouseLeave?.(event);
        clearOpenDelay();
        setOpen(false);
      },
      onFocus: (event: FocusEvent<HTMLElement>) => {
        originalOnFocus?.(event);
        if (enabled) {
          setOpen(true);
        }
      },
      onBlur: (event: FocusEvent<HTMLElement>) => {
        originalOnBlur?.(event);
        setOpen(false);
      },
    };

    return cloneElement(child, mergedProps);
  }, [child, childProps, enabled, openDelayMs]);

  return (
    <>
      {renderedChild}
      {enabled ? (
        <Tooltip
          open={open}
          anchorRef={anchorRef}
          text={normalizedText}
          offsetY={offsetY}
          placement={placement}
          followCursor={followCursor}
          cursorOffsetX={cursorOffsetX}
          cursorOffsetY={cursorOffsetY}
          className={className}
        />
      ) : null}
    </>
  );
}
