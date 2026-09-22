import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  API_CREDENTIALS,
  api,
  ApiError,
  resetUnauthorizedRedirect,
  setOnUnauthorized,
} from "./client";

function jsonResponse(status: number, body: unknown): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: status === 401 ? "Unauthorized" : "OK",
    headers: new Headers({ "content-type": "application/json" }),
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as Response;
}

describe("api client credentials and 401 interceptor", () => {
  const originalFetch = globalThis.fetch;
  const originalCookie = Object.getOwnPropertyDescriptor(Document.prototype, "cookie");

  beforeEach(() => {
    resetUnauthorizedRedirect();
    setOnUnauthorized(undefined);
    document.cookie = "";
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    resetUnauthorizedRedirect();
    setOnUnauthorized(undefined);
    if (originalCookie) {
      Object.defineProperty(document, "cookie", originalCookie);
    }
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("sends credentials: include on every request", async () => {
    const fetchMock = vi.fn(async () => jsonResponse(200, { success: true, data: { status: "ok" } }));
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    await api.health();

    expect(API_CREDENTIALS).toBe("include");
    expect(fetchMock).toHaveBeenCalledOnce();
    const call = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    const init = call[1];
    expect(init.credentials).toBe("include");
    expect((init.headers as Record<string, string>)["X-Requested-With"]).toBe("ncc-ui");
  });

  it("attaches the CSRF header on mutating requests when ncc_csrf is set", async () => {
    document.cookie = "ncc_csrf=csrf-test-token";
    const fetchMock = vi.fn(async () =>
      jsonResponse(201, {
        success: true,
        data: { id: "tok1", name: "ci", token: "ncc_pat_abc", role: "admin" },
      }),
    );
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    await api.createToken({ name: "ci", expires_in_days: 90 });

    const call = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    const init = call[1];
    expect(call[0]).toBe("/api/v1/users/me/pats");
    expect(init.method).toBe("POST");
    expect((init.headers as Record<string, string>)["X-CSRF-Token"]).toBe("csrf-test-token");
  });

  it("redirects to /login once when a protected endpoint returns 401", async () => {
    const onUnauthorized = vi.fn();
    setOnUnauthorized(onUnauthorized);
    globalThis.fetch = vi.fn(async () =>
      jsonResponse(401, { error: "Unauthorized" }),
    ) as unknown as typeof fetch;

    await expect(api.listTokens()).rejects.toBeInstanceOf(ApiError);
    await expect(api.listTokens()).rejects.toBeInstanceOf(ApiError);

    expect(onUnauthorized).toHaveBeenCalledOnce();
  });

  it("does not redirect on 401 from login, me, forgot-password, or health", async () => {
    const onUnauthorized = vi.fn();
    setOnUnauthorized(onUnauthorized);
    globalThis.fetch = vi.fn(async () =>
      jsonResponse(401, { error: "Unauthorized" }),
    ) as unknown as typeof fetch;

    await expect(api.login("a", "b")).rejects.toBeInstanceOf(ApiError);
    await expect(api.me()).rejects.toBeInstanceOf(ApiError);
    await expect(api.forgotPassword("a")).rejects.toBeInstanceOf(ApiError);
    await expect(api.health()).rejects.toBeInstanceOf(ApiError);

    expect(onUnauthorized).not.toHaveBeenCalled();
  });

  it("does not redirect when the browser is already on /login", async () => {
    const onUnauthorized = vi.fn();
    setOnUnauthorized(onUnauthorized);
    vi.stubGlobal("location", { pathname: "/login", assign: vi.fn() });
    globalThis.fetch = vi.fn(async () =>
      jsonResponse(401, { error: "Unauthorized" }),
    ) as unknown as typeof fetch;

    await expect(api.listTokens()).rejects.toBeInstanceOf(ApiError);
    expect(onUnauthorized).not.toHaveBeenCalled();
  });

  it("lists, mints, and revokes PATs against /api/v1/users/me/pats", async () => {
    const fetchMock = vi.fn(async (path: string, init?: RequestInit) => {
      const method = (init?.method ?? "GET").toUpperCase();
      if (method === "GET" && path === "/api/v1/users/me/pats") {
        return jsonResponse(200, {
          success: true,
          data: { tokens: [{ id: "abc", name: "ci", role: "operator", created_at: "2026-01-01T00:00:00Z" }] },
        });
      }
      if (method === "DELETE" && path === "/api/v1/users/me/pats/abc") {
        return jsonResponse(200, { success: true, message: "token revoked" });
      }
      return jsonResponse(404, { success: false, error: "not found" });
    });
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const listed = await api.listTokens();
    expect(listed.tokens).toHaveLength(1);
    expect(listed.tokens[0].name).toBe("ci");

    await api.revokeToken("abc");
    const revokeCall = fetchMock.mock.calls[1] as unknown as [string, RequestInit];
    expect(revokeCall[0]).toBe("/api/v1/users/me/pats/abc");
    expect(revokeCall[1].method).toBe("DELETE");
  });
});
