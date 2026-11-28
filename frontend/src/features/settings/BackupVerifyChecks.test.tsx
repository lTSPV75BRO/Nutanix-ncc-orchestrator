import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import type { BackupVerifyReport } from "../../api/types";
import { BackupVerifyChecks, backupVerifyReportFrom } from "./BackupVerifyChecks";

const report: BackupVerifyReport = {
  archive: "manual-x.tar.gz",
  encrypted: false,
  backup_version: "3.0.0",
  running_version: "2.4.0",
  files: 4,
  bytes: 100,
  secrets: [".ncc-api-token"],
  checks: [
    { name: "Archive", status: "pass", message: "Every entry is readable." },
    { name: "Contents", status: "warn", message: "No checksums." },
    { name: "Version", status: "fail", message: "Different major versions are not compatible." },
  ],
  ok: false,
};

describe("BackupVerifyChecks", () => {
  afterEach(() => cleanup());

  it("shows one badge per check and the failure summary", () => {
    render(<BackupVerifyChecks report={report} />);
    const rows = screen.getByRole("list", { name: "Backup checks" }).querySelectorAll("li");
    expect(rows).toHaveLength(3);
    expect(rows[0].querySelector(".ui-badge.tone-ok")).toHaveTextContent("Pass");
    expect(rows[1].querySelector(".ui-badge.tone-warn")).toHaveTextContent("Warn");
    expect(rows[2].querySelector(".ui-badge.tone-fail")).toHaveTextContent("Fail");
    expect(screen.getByText("1 check failed. Don't restore this backup.")).toBeInTheDocument();
    expect(screen.getByText(/Made by version 3.0.0/)).toBeInTheDocument();
  });

  it("reads the report from response or error data", () => {
    expect(backupVerifyReportFrom({ report })).toBe(report);
    expect(backupVerifyReportFrom({ output: "Archive OK" })).toBeNull();
    expect(backupVerifyReportFrom(undefined)).toBeNull();
  });
});
