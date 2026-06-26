import path from "node:path";
import react from "@vitejs/plugin-react";

const shellRoot = __dirname;
const shellNodeModules = path.resolve(shellRoot, "./node_modules");

export default {
  root: shellRoot,
  base: "/",
  envDir: path.resolve(shellRoot, "../../etc"),
  plugins: [react()],
  resolve: {
    alias: {
      // Keep legacy @ semantics during host cutover to avoid broad import churn.
      "@": path.resolve(shellRoot, "../apps/tradsphere/src"),
      "@shell": path.resolve(shellRoot, "./src"),
      "@tradsphere": path.resolve(shellRoot, "../apps/tradsphere/src"),
      "@home": path.resolve(shellRoot, "../apps/home/src"),
      "@shiftzy": path.resolve(shellRoot, "../apps/shiftzy/src"),
      "@leavesphere": path.resolve(shellRoot, "../apps/leavesphere/src"),
      "@fundsphere": path.resolve(shellRoot, "../apps/fundsphere/src"),
      "@shared": path.resolve(shellRoot, "../shared"),
      react: path.resolve(shellNodeModules, "react"),
      "react-dom": path.resolve(shellNodeModules, "react-dom"),
      "lucide-react": path.resolve(shellNodeModules, "lucide-react"),
      "framer-motion": path.resolve(shellNodeModules, "framer-motion"),
      "@radix-ui/react-dialog": path.resolve(shellNodeModules, "@radix-ui/react-dialog"),
      "@tanstack/react-table": path.resolve(shellNodeModules, "@tanstack/react-table"),
      recharts: path.resolve(shellNodeModules, "recharts"),
    },
  },
  build: {
    outDir: path.resolve(shellRoot, "../apps/tradsphere/dist"),
    emptyOutDir: true,
  },
};
