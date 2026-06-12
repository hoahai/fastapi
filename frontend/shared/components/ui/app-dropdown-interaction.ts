export function isAppDropdownInteractionTarget(target: EventTarget | null): boolean {
  if (!(target instanceof Element)) {
    return false;
  }

  return Boolean(target.closest("[data-app-dropdown-root='true'], [data-app-dropdown-menu='true']"));
}

export function isAppDropdownInteractionEvent(event: { target: EventTarget | null; detail?: unknown }): boolean {
  const detail = event.detail;
  if (!detail || typeof detail !== "object") {
    return isAppDropdownInteractionTarget(event.target);
  }

  const originalEvent = (detail as { originalEvent?: unknown }).originalEvent;
  const originalTarget =
    originalEvent && typeof originalEvent === "object"
      ? (originalEvent as { target?: EventTarget | null }).target ?? null
      : null;

  return isAppDropdownInteractionTarget(originalTarget) || isAppDropdownInteractionTarget(event.target);
}
