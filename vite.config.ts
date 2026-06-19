import { resolve } from "node:path";
import { defineConfig } from "vite";

export default defineConfig(({ command }) => {
  return {
    server: {
      port: 3000,
      strictPort: true,
    },
    resolve: {
      alias: {
        peerix: resolve(__dirname, "./src/index.js"),
      },
    },
    build: {
      lib: {
        entry: "src/index.ts",
        name: "peerix",
        formats: ["umd", "es"],
        fileName: (format) => {
          return format === "umd" ? "peerix.umd.js" : "peerix.esm.js";
        },
      },
      target: "es2020",
      minify: command === "build",
      sourcemap: true,
    },
  };
});
