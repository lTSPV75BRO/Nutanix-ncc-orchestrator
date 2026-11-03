import { describe, expect, it } from "vitest";
import { describeCron, describeEvery, kubernetesCronFor, nextCronRuns, parseCron } from "./cron";

describe("cron helper", () => {
  it("validates fields", () => {
    expect(typeof parseCron("15 */4 * * *")).toBe("object");
    expect(typeof parseCron("@daily")).toBe("object");
    expect(parseCron("*/5 * * *")).toMatch(/five fields/);
    expect(parseCron("61 * * * *")).toMatch(/Minute must be between 0 and 59/);
    expect(parseCron("0 0 * * MON")).toMatch(/Invalid day of week/);
  });

  it("describes common shapes", () => {
    expect(describeCron("*/15 * * * *")).toBe("Every 15 minutes");
    expect(describeCron("0 */4 * * *")).toBe("Every 4 hours");
    expect(describeCron("15 */4 * * *")).toBe("Every 4 hours at :15 past");
    expect(describeCron("30 2 * * *")).toBe("Every day at 02:30");
    expect(describeCron("0 2 * * 1-5")).toBe("Weekdays at 02:00");
    expect(describeCron("0 0 * * 0")).toBe("Every Sunday at 00:00");
    expect(describeCron("0 6 1 * *")).toBe("Monthly on day 1 at 06:00");
    expect(describeCron("@hourly")).toBe("Every hour, on the hour");
    expect(describeCron("5 1,13 * 3 *")).toBe("Custom schedule");
  });

  it("lists the next runs in UTC", () => {
    const from = new Date("2026-11-03T09:20:00Z"); // a Tuesday
    expect(nextCronRuns("0 */4 * * *", from, 3, true).map((d) => d.toISOString())).toEqual([
      "2026-11-03T12:00:00.000Z",
      "2026-11-03T16:00:00.000Z",
      "2026-11-03T20:00:00.000Z",
    ]);
    // Weekdays only: Friday then Monday.
    expect(nextCronRuns("0 2 * * 1-5", new Date("2026-11-06T03:00:00Z"), 2, true).map((d) => d.toISOString())).toEqual([
      "2026-11-09T02:00:00.000Z",
      "2026-11-10T02:00:00.000Z",
    ]);
    // Day-of-month and day-of-week both restricted: either matches.
    expect(nextCronRuns("0 0 1 * 0", new Date("2026-11-01T00:30:00Z"), 1, true)[0].toISOString()).toBe("2026-11-08T00:00:00.000Z");
    expect(nextCronRuns("not cron", from, 3, true)).toEqual([]);
  });

  it("mirrors the server's Kubernetes interval rules", () => {
    expect(kubernetesCronFor("15m")).toEqual({ cron: "*/15 * * * *" });
    expect(kubernetesCronFor("1h")).toEqual({ cron: "0 * * * *" });
    expect(kubernetesCronFor("24h")).toEqual({ cron: "0 0 * * *" });
    expect(kubernetesCronFor("7d")).toEqual({ cron: "0 0 * * 0" });
    expect(kubernetesCronFor("45m")).toHaveProperty("error");
    expect(kubernetesCronFor("5h")).toHaveProperty("error");
    expect(kubernetesCronFor("2d")).toHaveProperty("error");
  });

  it("describes intervals", () => {
    expect(describeEvery("1h")).toBe("Every hour");
    expect(describeEvery("4h")).toBe("Every 4 hours");
    expect(describeEvery("1d")).toBe("Every day");
  });
});
