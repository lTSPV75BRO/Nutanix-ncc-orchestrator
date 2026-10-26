import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { LoginPage, setLoginNotice } from "./LoginPage";
import { isEncryptedBackupName } from "../features/backup/RestoreBackupDialog";

vi.mock("../notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

type Reply = { status: number; body: unknown };

function mockFetch(route: (url: string, init?: RequestInit) => Reply | undefined) {
  globalThis.fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const r = route(url, init) ?? { status: 200, body: { success: true, data: { status: "ok", version: "2.2.1" } } };
    return {
      ok: r.status >= 200 && r.status < 300,
      status: r.status,
      statusText: "",
      headers: new Headers({ "content-type": "application/json" }),
      json: async () => r.body,
      text: async () => JSON.stringify(r.body),
    } as Response;
  }) as unknown as typeof fetch;
}

function renderLogin(props: Partial<Parameters<typeof LoginPage>[0]> = {}) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <LoginPage localEnabled samlEnabled={false} bootstrapPending={false} onSuccess={vi.fn()} {...props} />
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  mockFetch(() => undefined);
  window.sessionStorage.clear();
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("LoginPage", () => {
  // Regression: the form was rendered only with local accounts, so a
  // directory-only (AD/LDAP) install offered no way to sign in.
  it("shows the password form for directory-only installs", () => {
    renderLogin({ localEnabled: false, ldapEnabled: true });
    expect(screen.getByLabelText("Username")).toBeInTheDocument();
    expect(screen.getByText(/Directory users/)).toBeInTheDocument();
  });

  it("warns when Caps Lock is on", () => {
    renderLogin();
    const pw = screen.getByLabelText("Password");
    fireEvent.keyUp(pw, { key: "A", modifierCapsLock: true });
    expect(screen.getByText("Caps Lock is on")).toBeInTheDocument();
  });

  it("shows a lockout as a warning with the server's message", async () => {
    mockFetch((url) =>
      url.includes("/auth/login")
        ? { status: 429, body: { success: false, error: "too many failed sign-in attempts; try again in about 60s." } }
        : undefined,
    );
    renderLogin();
    fireEvent.change(screen.getByLabelText("Username"), { target: { value: "admin" } });
    fireEvent.change(screen.getByLabelText("Password"), { target: { value: "nope" } });
    fireEvent.click(screen.getByRole("button", { name: /Sign in/ }));
    expect(await screen.findByText("Too many attempts")).toBeInTheDocument();
    expect(screen.getByText(/try again in about 60s/)).toBeInTheDocument();
  });

  it("shows a one-shot notice left by the app (e.g. session ended)", () => {
    setLoginNotice({ type: "warning", text: "Your session ended." });
    renderLogin();
    expect(screen.getByText("Your session ended.")).toBeInTheDocument();
    cleanup();
    renderLogin();
    expect(screen.queryByText("Your session ended.")).not.toBeInTheDocument();
  });

  it("offers restore on a new install and accepts encrypted backups with a passphrase", async () => {
    renderLogin({ bootstrapPending: true });
    fireEvent.click(screen.getByRole("button", { name: /Restore from a backup/ }));
    expect(await screen.findByLabelText("Backup passphrase")).toBeInTheDocument();
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    expect(input.accept).toContain(".enc");
  });
});

describe("isEncryptedBackupName", () => {
  it("recognises sealed archives", () => {
    expect(isEncryptedBackupName("manual-20261026T000000Z.tar.gz.enc")).toBe(true);
    expect(isEncryptedBackupName("ncc-backup.tar.gz")).toBe(false);
  });
});

describe("LoginPage sign-in", () => {
  it("calls onSuccess after a successful login", async () => {
    mockFetch((url) => (url.includes("/auth/login") ? { status: 200, body: { success: true, data: { username: "admin" } } } : undefined));
    const onSuccess = vi.fn();
    renderLogin({ onSuccess });
    fireEvent.change(screen.getByLabelText("Username"), { target: { value: "admin" } });
    fireEvent.change(screen.getByLabelText("Password"), { target: { value: "right-password" } });
    fireEvent.click(screen.getByRole("button", { name: /Sign in/ }));
    await waitFor(() => expect(onSuccess).toHaveBeenCalled());
  });
});
