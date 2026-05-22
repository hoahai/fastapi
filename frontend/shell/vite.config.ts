import path from "node:path";
import react from "../apps/tradsphere/node_modules/@vitejs/plugin-react/dist/index.js";

const shellRoot = __dirname;
const tradsphereNodeModules = path.resolve(shellRoot, "../apps/tradsphere/node_modules");

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
      "@shared": path.resolve(shellRoot, "../shared"),
      react: path.resolve(tradsphereNodeModules, "react"),
      "react-dom": path.resolve(tradsphereNodeModules, "react-dom"),
      "lucide-react": path.resolve(tradsphereNodeModules, "lucide-react"),
      "framer-motion": path.resolve(tradsphereNodeModules, "framer-motion"),
    },
  },
  build: {
    outDir: path.resolve(shellRoot, "../apps/tradsphere/dist"),
    emptyOutDir: true,
  },
};
