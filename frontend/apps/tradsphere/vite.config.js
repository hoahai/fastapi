import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";
export default defineConfig({
    base: "/fe/",
    plugins: [react()],
    resolve: {
        alias: {
            "@": path.resolve(__dirname, "./src"),
            "@home": path.resolve(__dirname, "../home/src"),
            "@shared": path.resolve(__dirname, "../../shared"),
            "lucide-react": path.resolve(__dirname, "./node_modules/lucide-react"),
        },
    },
});
