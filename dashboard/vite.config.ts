import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The dashboard calls the FastAPI service at http://localhost:8000 (CORS enabled).
// Override with VITE_API_BASE if the API runs elsewhere.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
  },
});
