import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { LDAPConfig } from "../../api/types";
import { AccessSection } from "./AccessSection";
import { ldapFormValues, ldapProfilePayload } from "./ldapProfiles";

vi.mock("../../notify", () => ({
  notify: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn(), loading: vi.fn(), close: vi.fn() },
  notifyError: vi.fn(),
  NotifyBridge: () => null,
}));

const STORED: LDAPConfig = {
  enabled: false,
  managed_by: "runtime",
  url: "ldaps://dc1.corp.example.com:636",
  base_dn: "DC=corp,DC=example,DC=com",
  has_bind_password: true,
  upn_suffixes: ["corp.example.com"],
  netbios_names: ["CORP"],
  profiles: [
    {
      name: "emea",
      upn_suffixes: ["emea.corp.example.com"],
      netbios_names: ["EMEA"],
      url: "ldaps://dc1.emea.corp.example.com:636",
      base_dn: "DC=emea,DC=corp,DC=example,DC=com",
      has_bind_password: true,
      has_ca_cert: true,
      default_role: "operator",
    },
  ],
};

describe("ldapProfiles helpers", () => {
  it("seeds previous_name and blank secrets from the stored config", () => {
    const v = ldapFormValues(STORED);
    expect(v.profiles[0]).toMatchObject({ name: "emea", previous_name: "emea", bind_password: "", ca_cert_pem: "" });
    expect(v.netbios_names).toEqual(["CORP"]);
    // A config from before profiles existed has none.
    expect(ldapFormValues({ enabled: true, managed_by: "runtime", url: "u", base_dn: "b" }).profiles).toEqual([]);
  });

  it("omits blank secrets and sends previous_name only on rename", () => {
    const kept = ldapProfilePayload({ ...ldapFormValues(STORED).profiles[0] });
    expect(kept).not.toHaveProperty("bind_password");
    expect(kept).not.toHaveProperty("ca_cert_pem");
    expect(kept).not.toHaveProperty("previous_name");
    expect(kept.upn_suffixes).toEqual(["emea.corp.example.com"]);

    const renamed = ldapProfilePayload({
      ...ldapFormValues(STORED).profiles[0],
      name: " europe ",
      bind_password: "s3cret ",
      upn_suffixes: [" emea.corp.example.com ", ""],
    });
    expect(renamed).toMatchObject({ name: "europe", previous_name: "emea", bind_password: "s3cret " });
    expect(renamed.upn_suffixes).toEqual(["emea.corp.example.com"]);
  });
});

describe("Sign-in LDAP card with directory profiles", () => {
  afterEach(() => cleanup());

  function renderSignIn() {
    const puts: Record<string, unknown>[] = [];
    globalThis.fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      let data: unknown = {};
      if (url.includes("/api/v1/settings/ldap") && init?.method === "PUT") {
        puts.push(JSON.parse(String(init.body)));
      } else if (url.endsWith("/api/v1/settings/ldap")) {
        data = STORED;
      }
      return {
        ok: true,
        status: 200,
        statusText: "OK",
        headers: new Headers({ "content-type": "application/json" }),
        json: async () => ({ success: true, data }),
      };
    }) as unknown as typeof fetch;
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={qc}>
        <MemoryRouter initialEntries={["/settings/access?focus=ldap"]}>
          <AccessSection />
        </MemoryRouter>
      </QueryClientProvider>,
    );
    return puts;
  }

  it("shows stored profiles and saves them back with the default directory", async () => {
    const puts = renderSignIn();
    await screen.findByText("Other domains or forests");
    await waitFor(() => expect(screen.getByDisplayValue("ldaps://dc1.emea.corp.example.com:636")).toBeInTheDocument());
    expect(await screen.findByText("A CA cert is stored; leave blank to keep it.")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /Save LDAP configuration/, hidden: true }));
    await waitFor(() => expect(puts).toHaveLength(1));
    const body = puts[0];
    expect(body.netbios_names).toEqual(["CORP"]);
    expect(body).not.toHaveProperty("bind_password");
    const profiles = body.profiles as Record<string, unknown>[];
    expect(profiles).toHaveLength(1);
    expect(profiles[0]).toMatchObject({ name: "emea", upn_suffixes: ["emea.corp.example.com"], default_role: "operator" });
    expect(profiles[0]).not.toHaveProperty("bind_password");
  });

  it("adds a directory card that must be filled in before saving", async () => {
    const puts = renderSignIn();
    await waitFor(() => expect(screen.getByDisplayValue("ldaps://dc1.emea.corp.example.com:636")).toBeInTheDocument());
    fireEvent.click(screen.getByRole("button", { name: /Add directory/, hidden: true }));
    expect(await screen.findByText("New directory")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /Save LDAP configuration/, hidden: true }));
    expect(await screen.findByText("Name this directory")).toBeInTheDocument();
    expect(puts).toHaveLength(0);
  });
});
