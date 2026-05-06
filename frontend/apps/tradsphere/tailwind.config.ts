import type { Config } from "tailwindcss";

const config: Config = {
  darkMode: ["class"],
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        border: "hsl(214 32% 90%)",
        input: "hsl(214 32% 90%)",
        ring: "hsl(218 92% 56%)",
        background: "hsl(210 29% 98%)",
        foreground: "hsl(221 39% 11%)",
        primary: {
          DEFAULT: "hsl(218 92% 56%)",
          foreground: "hsl(210 40% 98%)",
        },
        secondary: {
          DEFAULT: "hsl(214 80% 95%)",
          foreground: "hsl(221 39% 11%)",
        },
        muted: {
          DEFAULT: "hsl(214 50% 96%)",
          foreground: "hsl(215 20% 38%)",
        },
        card: {
          DEFAULT: "hsl(0 0% 100%)",
          foreground: "hsl(221 39% 11%)",
        },
      },
      boxShadow: {
        soft: "0 10px 30px -18px rgba(30, 64, 175, 0.35)",
      },
      borderRadius: {
        xl: "1rem",
        "2xl": "1.25rem",
      },
    },
  },
  plugins: [],
};

export default config;
