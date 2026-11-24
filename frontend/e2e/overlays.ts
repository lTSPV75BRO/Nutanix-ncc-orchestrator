import { expect, type Locator, type Page } from "@playwright/test";
import { open, settled } from "./helpers";

/**
 * Things that open on top of a page. Each one says how to open it, where its
 * surface is and which control opened it, so the scans, the Escape test and
 * the focus-return test all use the same definition.
 */
export type Overlay = {
  name: string;
  /** Opens it and returns the control that opened it. */
  open: (page: Page) => Promise<Locator>;
  /** The surface that must be visible once open. */
  surface: (page: Page) => Locator;
  /** Escape closes it (menus and dialogs); toasts close themselves. */
  closesWithEscape: boolean;
  /** Must focus return to the opener after Escape? */
  focusReturns: boolean;
};

const dialog = (page: Page) => page.locator(".ant-modal-wrap:visible [role=dialog]");

/** The user menu popup (the sidebar is a role=menu too, so match on the popup). */
const userMenuSurface = (page: Page) => page.locator(".ant-dropdown:visible").filter({ hasText: "Signed in as" });

async function userMenu(page: Page): Promise<Locator> {
  const btn = page.locator("button.header-user-btn");
  // Opened with the keyboard, as someone without a mouse would.
  await btn.focus();
  await page.keyboard.press("Enter");
  await expect(userMenuSurface(page)).toBeVisible();
  return btn;
}

export const OVERLAYS: Overlay[] = [
  {
    name: "findings drawer",
    open: async (page) => {
      await open(page, "/findings");
      const trigger = page.locator("main").getByRole("button", { name: /vg_space_usage_check/ }).first();
      await trigger.click();
      return trigger;
    },
    surface: (page) => page.locator(".ant-drawer-content-wrapper:visible [role=dialog], .ant-drawer-section:visible").first(),
    closesWithEscape: true,
    focusReturns: true,
  },
  {
    name: "personal tokens dialog",
    open: async (page) => {
      await open(page, "/overview");
      const btn = await userMenu(page);
      await page.getByRole("menuitem", { name: "Personal access tokens" }).click();
      return btn;
    },
    surface: (page) => dialog(page).filter({ hasText: "Personal access tokens" }),
    closesWithEscape: true,
    focusReturns: true,
  },
  {
    name: "confirm modal (restore a backup)",
    open: async (page) => {
      await open(page, "/system/updates");
      await page.getByRole("tab", { name: /Backups/ }).click();
      await settled(page);
      const trigger = page.locator(".ant-table").getByRole("button", { name: /Restore/ }).first();
      await trigger.click();
      return trigger;
    },
    surface: (page) => dialog(page),
    closesWithEscape: true,
    focusReturns: true,
  },
  {
    name: "user menu",
    open: async (page) => {
      await open(page, "/overview");
      return userMenu(page);
    },
    surface: (page) => userMenuSurface(page),
    closesWithEscape: true,
    focusReturns: true,
  },
  {
    name: "notification toast",
    open: async (page) => {
      await open(page, "/findings");
      const trigger = page.getByRole("button", { name: "Copy link" });
      await trigger.click();
      return trigger;
    },
    surface: (page) => page.locator(".ant-notification-notice").first(),
    closesWithEscape: false,
    focusReturns: false,
  },
];
