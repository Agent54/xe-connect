import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";

export default defineConfig({
  base: "/_xe/ui/",
  plugins: [svelte()],
  server: {
    proxy: {
      "/_xe/diagnostics": "http://127.0.0.1:5198",
      "/_xe/test": "http://127.0.0.1:5198",
      "/_xe/invite": "http://127.0.0.1:5198",
      "/_xe/pair": "http://127.0.0.1:5198",
      "/_xe/unpair": "http://127.0.0.1:5198",
    },
  },
});
