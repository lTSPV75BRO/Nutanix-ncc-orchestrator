import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { SaveBar } from "./SaveBar";
import { middleEllipsis } from "./KeyValueList";

describe("middleEllipsis", () => {
  it("keeps short text and both ends of long paths", () => {
    expect(middleEllipsis("config.yaml")).toBe("config.yaml");
    const long = "C:\\Users\\me\\AppData\\Local\\Temp\\claude\\scratchpad\\smoke\\root\\config.yaml";
    const out = middleEllipsis(long, 40);
    expect(out).toHaveLength(40);
    expect(out.startsWith("C:\\Users")).toBe(true);
    expect(out.endsWith("root\\config.yaml")).toBe(true);
  });
});

describe("SaveBar", () => {
  afterEach(() => cleanup());

  it("is hidden while there is nothing to save", () => {
    render(<SaveBar dirty={false} onSave={() => {}} />);
    expect(screen.queryByRole("region", { name: "Unsaved changes" })).toBeNull();
  });

  it("offers Discard and Save when dirty", () => {
    const onSave = vi.fn();
    const onDiscard = vi.fn();
    render(<SaveBar dirty onSave={onSave} onDiscard={onDiscard} message="2 fields changed" />);
    expect(screen.getByText("2 fields changed")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Discard" }));
    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    expect(onDiscard).toHaveBeenCalledOnce();
    expect(onSave).toHaveBeenCalledOnce();
  });

  it("can block saving, e.g. on a validation error", () => {
    render(<SaveBar dirty onSave={() => {}} saveDisabled />);
    expect(screen.getByRole("button", { name: "Save" })).toBeDisabled();
  });
});
