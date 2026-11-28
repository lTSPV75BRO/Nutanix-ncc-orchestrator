import { describe, expect, it } from "vitest";
import { previewDetail } from "./ClusterTable";

describe("previewDetail", () => {
  it("drops the KB boilerplate and its raw URL (the KB has its own badge)", () => {
    expect(
      previewDetail(
        "NGT Client Cert is already expired for VM : vm-app-01 before 21 days Refer to KB 10075 (http://portal.nutanix.com/kb/10075) for details on ngt_client_cert_expiry_check",
      ),
    ).toBe("NGT Client Cert is already expired for VM : vm-app-01 before 21 days");
  });

  it("keeps text after the KB reference", () => {
    expect(previewDetail("Disk low. Refer to KB 4600 (https://portal.nutanix.com/kb/4600) for details on x_check. Node z impacted")).toBe(
      "Disk low. Node z impacted",
    );
  });

  it("leaves ordinary text alone and handles empty", () => {
    expect(previewDetail("Volume Group usage is high")).toBe("Volume Group usage is high");
    expect(previewDetail("")).toBe("—");
    expect(previewDetail("Refer to KB 1 (http://x/kb/1) for details on a_b")).toBe("—");
  });
});
