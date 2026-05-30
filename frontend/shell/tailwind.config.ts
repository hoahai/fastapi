import type { Config } from "tailwindcss";

const config: Config = {
  darkMode: ["class"],
  content: [
    "./index.html",
    "./src/**/*.{ts,tsx}",
    "../apps/tradsphere/src/**/*.{ts,tsx}",
    "../apps/home/src/**/*.{ts,tsx}",
    "../apps/shiftzy/src/**/*.{ts,tsx}",
    "../apps/leavesphere/src/**/*.{ts,tsx}",
    "../shared/**/*.{ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        border: "hsl(223 30% 84%)",
        input: "hsl(223 30% 84%)",
        ring: "hsl(226 85% 58%)",
        background: "hsl(221 45% 97%)",
        foreground: "hsl(224 42% 14%)",
        primary: {
          DEFAULT: "hsl(224 82% 56%)",
          foreground: "hsl(210 40% 98%)",
        },
        secondary: {
          DEFAULT: "hsl(223 86% 95%)",
          foreground: "hsl(224 42% 14%)",
        },
        muted: {
          DEFAULT: "hsl(223 40% 94%)",
          foreground: "hsl(219 20% 40%)",
        },
        card: {
          DEFAULT: "hsl(0 0% 100%)",
          foreground: "hsl(224 42% 14%)",
        },
      },
      boxShadow: {
        soft: "0 28px 54px -34px rgba(37, 69, 150, 0.48)",
      },
      borderRadius: {
        xl: "1.1rem",
        "2xl": "1.45rem",
      },
    },
  },
  plugins: [],
};

export default config;
