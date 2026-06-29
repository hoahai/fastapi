import type { CSSProperties } from "react";

type RgbColor = {
  r: number;
  g: number;
  b: number;
};

export type FundsphereDepartmentColorStyles = {
  headerStyle: CSSProperties;
  cardStyle: CSSProperties;
  chipStyle: CSSProperties;
};

export type FundsphereDepartmentColorInput = {
  color?: string | null;
  identity?: string | null;
};

function normalizeHexColor(value: string | null | undefined): string | null {
  const trimmed = String(value ?? "").trim();
  if (!trimmed) {
    return null;
  }
  const normalized = trimmed.startsWith("#") ? trimmed.slice(1) : trimmed;
  if (/^[0-9a-fA-F]{6}$/.test(normalized)) {
    return `#${normalized.toUpperCase()}`;
  }
  if (/^[0-9a-fA-F]{3}$/.test(normalized)) {
    return `#${normalized
      .split("")
      .map((part) => `${part}${part}`)
      .join("")
      .toUpperCase()}`;
  }
  return null;
}

function hexToRgb(value: string): RgbColor | null {
  const normalized = normalizeHexColor(value);
  if (!normalized) {
    return null;
  }
  const parsed = Number.parseInt(normalized.slice(1), 16);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return {
    r: (parsed >> 16) & 255,
    g: (parsed >> 8) & 255,
    b: parsed & 255,
  };
}

function toRgba(color: RgbColor, alpha: number): string {
  return `rgba(${color.r}, ${color.g}, ${color.b}, ${alpha})`;
}

function darken(color: RgbColor, amount: number): RgbColor {
  const factor = Math.max(0, Math.min(1, 1 - amount));
  return {
    r: Math.round(color.r * factor),
    g: Math.round(color.g * factor),
    b: Math.round(color.b * factor),
  };
}

const FALLBACK_PALETTE: string[] = [
  "#4F84D6",
  "#D98B47",
  "#56A16D",
  "#D9677F",
  "#9A6BD8",
  "#3AA3A2",
  "#D0A13E",
  "#6678D8",
  "#D65D5D",
  "#85965A",
  "#4FADC8",
  "#B65CA5",
];

function hashToHue(identity: string): number {
  let hash = 0;
  for (let index = 0; index < identity.length; index += 1) {
    hash = (hash * 31 + identity.charCodeAt(index)) % 3600;
  }
  return (hash % 360 + 360) % 360;
}

function resolveDepartmentRgb(input: FundsphereDepartmentColorInput): RgbColor | null {
  const identity = String(input.identity ?? "").trim();
  if (!identity) {
    return hexToRgb(input.color ?? "");
  }
  const fallback = FALLBACK_PALETTE[hashToHue(identity) % FALLBACK_PALETTE.length];
  return hexToRgb(fallback);
}

export function buildFundsphereDepartmentColorStyles(
  input: FundsphereDepartmentColorInput,
): FundsphereDepartmentColorStyles {
  const rgb = resolveDepartmentRgb(input);
  if (!rgb) {
    return {
      headerStyle: {},
      cardStyle: {},
      chipStyle: {},
    };
  }

  const chipForeground = darken(rgb, 0.34);
  const headerForeground = darken(rgb, 0.42);
  const cardBorder = darken(rgb, 0.16);
  const cardShadow = darken(rgb, 0.22);

  return {
    headerStyle: {
      borderColor: toRgba(rgb, 0.22),
      borderLeftColor: toRgba(rgb, 0.72),
      borderLeftStyle: "solid",
      borderLeftWidth: 6,
      backgroundColor: toRgba(rgb, 0.075),
      backgroundImage: `linear-gradient(90deg, ${toRgba(rgb, 0.22)} 0%, rgba(255, 255, 255, 0.965) 68%)`,
      color: `rgb(${headerForeground.r} ${headerForeground.g} ${headerForeground.b})`,
    },
    cardStyle: {
      borderColor: toRgba(cardBorder, 0.42),
      backgroundColor: toRgba(rgb, 0.018),
      backgroundImage: `radial-gradient(circle at 0% 0%, ${toRgba(rgb, 0.04)} 0%, rgba(255, 255, 255, 0.996) 56%, ${toRgba(rgb, 0.018)} 100%)`,
      boxShadow: `0 18px 30px -24px ${toRgba(cardShadow, 0.16)}`,
    },
    chipStyle: {
      borderColor: toRgba(rgb, 0.26),
      backgroundColor: toRgba(rgb, 0.11),
      color: `rgb(${chipForeground.r} ${chipForeground.g} ${chipForeground.b})`,
    },
  };
}
