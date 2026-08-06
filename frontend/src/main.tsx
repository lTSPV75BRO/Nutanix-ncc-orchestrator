import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter } from "react-router";
import App from "./App";
import "antd/dist/reset.css";
import "./styles.css";
import { AppThemeProvider } from "./theme";

// Sensible defaults so page navigation feels instant:
//   - 30 s staleTime → no thrashing refetch when the same page is mounted again
//   - 5 min gcTime   → cached responses survive tab switches
//   - retry: 1       → quick fail on real errors instead of 3×exp-backoff (which
//                      can stretch failures into ~7 s of perceived latency)
//   - no refetchOnWindowFocus by default — the components that genuinely need
//     live data set their own refetchInterval explicitly (e.g. header health,
//     active run polling).
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30_000,
      gcTime: 5 * 60_000,
      retry: 1,
      retryDelay: (attempt) => Math.min(1000 * 2 ** attempt, 5000),
      refetchOnWindowFocus: false,
      refetchOnReconnect: false,
    },
  },
});

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <AppThemeProvider>
      <QueryClientProvider client={queryClient}>
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </QueryClientProvider>
    </AppThemeProvider>
  </React.StrictMode>,
);
