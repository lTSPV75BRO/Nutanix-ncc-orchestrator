import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter } from "react-router-dom";
import { ConfigProvider, theme as antdTheme } from "antd";
import App from "./App";
import "antd/dist/reset.css";
import "./styles.css";

const queryClient = new QueryClient();

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ConfigProvider
      theme={{
        algorithm: antdTheme.darkAlgorithm,
        token: {
          colorPrimary: "#8b5cf6",
          colorInfo: "#38bdf8",
          colorSuccess: "#22c55e",
          colorWarning: "#f59e0b",
          colorError: "#f43f5e",
          colorBgBase: "#070b1b",
          colorBgContainer: "#111831",
          colorText: "#e2e8f0",
          colorBorder: "#2a355c",
          borderRadius: 10,
        },
        components: {
          Card: {
            headerBg: "#141d3a",
          },
          Table: {
            headerBg: "#182349",
            rowHoverBg: "#1a2856",
          },
        },
      }}
    >
      <QueryClientProvider client={queryClient}>
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </QueryClientProvider>
    </ConfigProvider>
  </React.StrictMode>,
);
