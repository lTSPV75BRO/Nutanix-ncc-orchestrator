import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { App as AntApp, ConfigProvider, theme as antdTheme } from "antd";
import type { ThemeConfig } from "antd";
import { useLocalStorageState } from "./hooks/useLocalStorageState";
import { NotifyBridge } from "./notify";

export type AppThemeKey = "dark" | "light" | "it-pro";
export type AppThemeSelection = AppThemeKey | "auto";

type ThemeContextValue = {
  selectedTheme: AppThemeSelection;
  theme: AppThemeKey;
  setTheme: (next: AppThemeSelection) => void;
};

const ThemeContext = createContext<ThemeContextValue | null>(null);

export const THEME_OPTIONS: Array<{ value: AppThemeSelection; label: string }> = [
  { value: "auto", label: "Auto (System)" },
  { value: "dark", label: "Dark" },
  { value: "light", label: "Light" },
  { value: "it-pro", label: "IT Professional" },
];

function toAntThemeConfig(theme: AppThemeKey): ThemeConfig {
  if (theme === "light") {
    // Cleaner, neutral light: warm-tinted off-white surface, white containers,
    // softer slate borders. Replaces the previous cool blue-tinted slate which
    // looked washed-out on bright displays and didn't read as a neutral
    // "professional product" backdrop.
    return {
      algorithm: antdTheme.defaultAlgorithm,
      token: {
        colorPrimary: "#2563eb",
        colorInfo: "#0ea5e9",
        colorSuccess: "#16a34a",
        colorWarning: "#d97706",
        colorError: "#dc2626",
        colorBgBase: "#fafafa",
        colorBgContainer: "#ffffff",
        colorBgLayout: "#f4f4f5",
        colorText: "#18181b",
        colorTextSecondary: "#52525b",
        colorBorder: "#e4e4e7",
        colorBorderSecondary: "#f4f4f5",
        borderRadius: 10,
      },
      components: {
        Card: { headerBg: "#fafafa" },
        Table: { headerBg: "#f4f4f5", rowHoverBg: "#f8fafc" },
        Layout: { bodyBg: "#f4f4f5", headerBg: "#ffffff" },
      },
    };
  }
  if (theme === "it-pro") {
    return {
      algorithm: antdTheme.darkAlgorithm,
      token: {
        colorPrimary: "#14b8a6",
        colorInfo: "#0284c7",
        colorSuccess: "#22c55e",
        colorWarning: "#a16207",
        colorError: "#ef4444",
        colorBgBase: "#0b1220",
        colorBgContainer: "#152238",
        colorText: "#dbeafe",
        colorBorder: "#334155",
        borderRadius: 6,
      },
      components: {
        Card: { headerBg: "#1e293b" },
        Table: { headerBg: "#223047", rowHoverBg: "#1f3b57" },
      },
    };
  }
  // Dark: near-black neutral surface with purple accent. Previous palette was
  // navy (#070b1b) which looked blue rather than dark. Now the base reads as
  // true black, container as deep charcoal — like VS Code Dark+ / IntelliJ
  // Darcula — while keeping the purple primary for brand continuity.
  return {
    algorithm: antdTheme.darkAlgorithm,
    token: {
      colorPrimary: "#8b5cf6",
      colorInfo: "#38bdf8",
      colorSuccess: "#22c55e",
      colorWarning: "#f59e0b",
      colorError: "#f43f5e",
      colorBgBase: "#0a0a0a",
      colorBgContainer: "#141414",
      colorBgLayout: "#0a0a0a",
      colorText: "#ededed",
      colorTextSecondary: "#a1a1aa",
      colorBorder: "#262626",
      colorBorderSecondary: "#1f1f1f",
      borderRadius: 10,
    },
    components: {
      Card: { headerBg: "#171717" },
      Table: { headerBg: "#1a1a1a", rowHoverBg: "#1f1f1f" },
      Layout: { bodyBg: "#0a0a0a", headerBg: "#0f0f0f" },
    },
  };
}

function normalizeThemeSelection(raw: string): AppThemeSelection {
  const trimmed = (raw || "").trim();
  if (trimmed === "auto" || trimmed === "light" || trimmed === "it-pro" || trimmed === "dark") {
    return trimmed;
  }
  // Merge old "night" into dark and retire comic.
  return "dark";
}

function getSystemTheme(): AppThemeKey {
  if (typeof window === "undefined" || typeof window.matchMedia !== "function") {
    return "dark";
  }
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

export function AppThemeProvider({ children }: { children: ReactNode }) {
  const [storedTheme, setStoredTheme] = useLocalStorageState<string>("app.theme", "dark");
  const selectedTheme = normalizeThemeSelection(storedTheme);
  const [systemTheme, setSystemTheme] = useState<AppThemeKey>(getSystemTheme());

  useEffect(() => {
    if (storedTheme !== selectedTheme) {
      setStoredTheme(selectedTheme);
    }
  }, [storedTheme, selectedTheme, setStoredTheme]);

  useEffect(() => {
    if (typeof window === "undefined" || typeof window.matchMedia !== "function") return;
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    const apply = () => setSystemTheme(media.matches ? "dark" : "light");
    apply();
    media.addEventListener("change", apply);
    return () => media.removeEventListener("change", apply);
  }, [setSystemTheme]);

  const theme: AppThemeKey = selectedTheme === "auto" ? systemTheme : selectedTheme;

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
  }, [theme]);

  const setTheme = (next: AppThemeSelection) => setStoredTheme(next);

  return (
    <ThemeContext.Provider value={{ selectedTheme, theme, setTheme }}>
      <ConfigProvider theme={toAntThemeConfig(theme)}>
        <AntApp notification={{ placement: "topRight", duration: 4.5 }} component={false}>
          <NotifyBridge />
          {children}
        </AntApp>
      </ConfigProvider>
    </ThemeContext.Provider>
  );
}

export function useAppTheme() {
  const ctx = useContext(ThemeContext);
  if (!ctx) throw new Error("useAppTheme must be used within AppThemeProvider");
  return ctx;
}
