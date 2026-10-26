import { describe, expect, it } from "vitest";
import { buildClusterNameMap, displayClusterName, healthPillValue, resolveClusterName } from "./report";

describe("cluster identity", () => {
  it("prefers a name or IP over a UUID on the same row", () => {
    const uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    expect(displayClusterName({ cluster: uuid, cluster_ip: "10.1.2.3", cluster_name: "prod-east" })).toBe("prod-east");
    expect(displayClusterName({ cluster: `{${uuid}}`, cluster_ip: "10.1.2.3" })).toBe("10.1.2.3");
  });

  it("resolves a braced UUID through the cluster map", () => {
    const uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    const map = buildClusterNameMap({
      aggRows: [{ cluster: uuid, cluster_name: "prod-east", cluster_ip: "10.1.2.3" }],
    });
    expect(resolveClusterName(`{${uuid}}`, map)).toBe("prod-east");
    expect(resolveClusterName("https://10.1.2.3:9440", map)).toBe("prod-east");
  });
});

describe("healthPillValue", () => {
  it("distinguishes no data from a 0% score", () => {
    expect(healthPillValue(0, 0)).toBeNull();
    expect(healthPillValue(Number.NaN, 50)).toBeNull();
    expect(healthPillValue(120, 0)).toBe("0.0%");
    expect(healthPillValue(120, 97.456)).toBe("97.5%");
  });
});
