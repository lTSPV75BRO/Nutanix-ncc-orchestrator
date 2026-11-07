import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import { App as AntApp, ConfigProvider, theme as antdTheme } from "antd";
import type { ThemeConfig } from "antd";
import { useLocalStorageState } from "./hooks/useLocalStorageState";
import { NotifyBridge } from "./notify";
import { ArcSpinner, BarsLoader } from "./components/Loaders";
import { InboxOutlined } from "@ant-design/icons";

export type AppThemeKey = "dark" | "light" | "it-pro";
export type AppThemeSelection = AppThemeKey | "auto";

type ThemeContextValue = {
  selectedTheme: AppThemeSelection;
  theme: AppThemeKey;
  setTheme: (next: AppThemeSelection) => void;
};

const ThemeContext = createContext<ThemeContextValue | null>(null);

export const THEME_OPTIONS: Array<{ value: AppThemeSelection; label: string }> = [
  { value: "auto", label: "Match system" },
  { value: "dark", label: "Dark" },
  { value: "light", label: "Light" },
  { value: "it-pro", label: "IT Professional" },
];

// One design system for every theme (docs/ui/redesign-plan.md): the same
// purple accent, radii, type scale and component shapes; only the surfaces,
// text greys and status shades differ per theme.
const SHARED_TOKENS = {
  fontFamily: "var(--app-font-family)",
  fontSize: 14,
  fontSizeSM: 12,
  fontSizeLG: 16,
  fontSizeHeading1: 30,
  fontSizeHeading2: 24,
  fontSizeHeading3: 20,
  fontSizeHeading4: 16,
  fontSizeHeading5: 14,
  fontWeightStrong: 600,
  borderRadius: 8,
  borderRadiusSM: 6,
  borderRadiusLG: 12,
  borderRadiusXS: 4,
  controlHeight: 32,
  boxShadow: "none",
  boxShadowSecondary: "0 8px 24px rgba(0, 0, 0, 0.18)",
} as const;

function sharedComponents(surface: string, raised: string, border: string): ThemeConfig["components"] {
  return {
    Card: { headerBg: surface, headerFontSize: 16, headerFontSizeSM: 14, headerHeight: 52, headerHeightSM: 44 },
    Table: { headerBg: raised, rowHoverBg: raised, headerSplitColor: "transparent", borderColor: border, cellFontSize: 14 },
    Layout: { bodyBg: "transparent", headerBg: surface },
    Tabs: { titleFontSize: 14, horizontalItemGutter: 24 },
    Tag: { defaultBg: raised },
    Alert: { withDescriptionPadding: "12px 16px", withDescriptionIconSize: 16, defaultPadding: "8px 12px" },
    Typography: { titleMarginBottom: 8, titleMarginTop: 0 },
    Modal: { titleFontSize: 16 },
    Drawer: { footerPaddingBlock: 12 },
    Statistic: { titleFontSize: 13, contentFontSize: 24 },
  };
}

function toAntThemeConfig(theme: AppThemeKey): ThemeConfig {
  if (theme === "light") {
    return {
      algorithm: antdTheme.defaultAlgorithm,
      token: {
        ...SHARED_TOKENS,
        colorPrimary: "#7c3aed",
        colorLink: "#6d28d9",
        colorInfo: "#0284c7",
        colorSuccess: "#16a34a",
        colorWarning: "#d97706",
        colorError: "#dc2626",
        colorBgBase: "#ffffff",
        colorBgContainer: "#ffffff",
        colorBgLayout: "#f4f4f5",
        colorText: "#18181b",
        colorTextSecondary: "#52525b",
        // antd's default muted greys fall short of WCAG AA (4.5:1) on white.
        colorTextDescription: "#52525b",
        colorTextPlaceholder: "#6b7280",
        colorBorder: "#e4e4e7",
        colorBorderSecondary: "#f4f4f5",
      },
      components: sharedComponents("#ffffff", "#fafafa", "#e4e4e7"),
    };
  }
  if (theme === "it-pro") {
    return {
      algorithm: antdTheme.darkAlgorithm,
      token: {
        ...SHARED_TOKENS,
        colorPrimary: "#8b5cf6",
        colorLink: "#c4b5fd",
        colorInfo: "#38bdf8",
        colorSuccess: "#22c55e",
        colorWarning: "#f59e0b",
        colorError: "#ef4444",
        colorBgBase: "#0b1220",
        colorBgContainer: "#111b2e",
        colorBgLayout: "#0b1220",
        colorText: "#dbeafe",
        colorTextSecondary: "#b6c9e6",
        colorTextDescription: "#a8bcd9",
        colorTextPlaceholder: "#90a6c6",
        colorBorder: "#24324a",
        colorBorderSecondary: "#1b2840",
      },
      components: sharedComponents("#111b2e", "#172540", "#24324a"),
    };
  }
  return {
    algorithm: antdTheme.darkAlgorithm,
    token: {
      ...SHARED_TOKENS,
      colorPrimary: "#8b5cf6",
      colorLink: "#c4b5fd",
      colorInfo: "#38bdf8",
      colorSuccess: "#22c55e",
      colorWarning: "#f59e0b",
      colorError: "#f43f5e",
      colorBgBase: "#0a0a0a",
      colorBgContainer: "#111111",
      colorBgLayout: "#0a0a0a",
      colorText: "#ededed",
      colorTextSecondary: "#a1a1aa",
      // antd's dark muted greys (~rgba .45/.25) fall short of WCAG AA.
      colorTextDescription: "#a1a1aa",
      colorTextPlaceholder: "#8a8a93",
      colorBorder: "#262626",
      colorBorderSecondary: "#1f1f1f",
    },
    components: sharedComponents("#111111", "#1a1a1a", "#262626"),
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
      <ConfigProvider
        theme={toAntThemeConfig(theme)}
        spin={{ indicator: <BarsLoader /> }}
        button={{ loadingIcon: <ArcSpinner /> }}
        empty={{ image: <span className="ui-empty-icon"><InboxOutlined /></span> }}
      >
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
