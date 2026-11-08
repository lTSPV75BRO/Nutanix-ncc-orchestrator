import { useState } from "react";
import { Button, Modal } from "antd";
import { BookOutlined, CopyOutlined, GithubOutlined, TagOutlined } from "@ant-design/icons";
import { notify } from "../notify";

export type AboutBuild = {
  version?: string;
  buildDate?: string;
  stream?: string;
  goVersion?: string;
  os?: string;
  arch?: string;
};

const REPO = "https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator";

// Kept in step with LICENSE at the repository root (AboutDialog.test.tsx
// compares them).
const LICENSE_HOLDER = "Copyright (c) 2025-2026 Prajwal Vernekar";
const MIT_TEXT = `Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.`;

/** One line with everything support needs to identify the build. */
function buildLine(b: AboutBuild): string {
  return [
    `NCC Orchestrator ${b.version || "unknown"}`,
    b.buildDate && b.buildDate !== "unknown" ? `built ${b.buildDate}` : "",
    b.stream ? `stream ${b.stream}` : "",
    b.goVersion ? b.goVersion : "",
    b.os || b.arch ? [b.os, b.arch].filter(Boolean).join("/") : "",
  ]
    .filter(Boolean)
    .join(" · ");
}

/**
 * About NCC Orchestrator: what it is, the build that is running, the license
 * and where the project lives. Opened from the user menu.
 */
export function AboutDialog({ open, onClose, build }: { open: boolean; onClose: () => void; build: AboutBuild }) {
  const [showLicense, setShowLicense] = useState(false);
  const shortVersion = (build.version || "").split("-")[0];

  const close = () => {
    setShowLicense(false);
    onClose();
  };

  const copyBuild = async () => {
    try {
      await navigator.clipboard.writeText(buildLine(build));
      notify.success("Version details copied");
    } catch {
      notify.info({ message: "Version details", description: buildLine(build), duration: 8 });
    }
  };

  const facts: { label: string; value: string }[] = [
    { label: "Version", value: build.version || "Unknown" },
    { label: "Built", value: build.buildDate && build.buildDate !== "unknown" ? build.buildDate : "Unknown" },
    ...(build.stream ? [{ label: "Release stream", value: build.stream }] : []),
    ...(build.goVersion ? [{ label: "Go", value: build.goVersion }] : []),
    ...(build.os || build.arch ? [{ label: "Platform", value: [build.os, build.arch].filter(Boolean).join(" / ") }] : []),
  ];

  return (
    <Modal
      open={open}
      title="About NCC Orchestrator"
      onCancel={close}
      centered
      width={480}
      className="about-dialog"
      destroyOnHidden
      footer={
        <Button type="primary" onClick={close}>
          Close
        </Button>
      }
    >
      <div className="about-head">
        <img src="/logo.svg" alt="" className="about-logo" />
        <h2 className="about-title">NCC Orchestrator</h2>
        <p className="about-subtitle">
          {shortVersion ? <span className="brand-version">v{shortVersion}</span> : null}
          <span>Cluster health checks at fleet scale</span>
        </p>
      </div>

      <p className="about-text">
        Runs Nutanix Cluster Check (NCC) on your Prism Element clusters in parallel, gathers every finding in one place,
        and tracks how each alert is handled from run to run.
      </p>

      <section className="about-section" aria-labelledby="about-build">
        <div className="about-section-head">
          <h3 id="about-build">This installation</h3>
          <Button size="small" type="link" icon={<CopyOutlined />} onClick={() => void copyBuild()}>
            Copy details
          </Button>
        </div>
        <dl className="about-facts">
          {facts.map((f) => (
            <div key={f.label} className="about-fact">
              <dt>{f.label}</dt>
              <dd>{f.value}</dd>
            </div>
          ))}
        </dl>
      </section>

      <section className="about-section" aria-labelledby="about-license">
        <div className="about-section-head">
          <h3 id="about-license">License</h3>
          <Button size="small" type="link" onClick={() => setShowLicense((v) => !v)} aria-expanded={showLicense}>
            {showLicense ? "Hide license" : "View license"}
          </Button>
        </div>
        <p className="about-text">MIT License. {LICENSE_HOLDER}.</p>
        {showLicense ? (
          <pre className="about-license" tabIndex={0} aria-label="MIT License text">
            {`MIT License\n\n${LICENSE_HOLDER}\n\n${MIT_TEXT}`}
          </pre>
        ) : null}
      </section>

      <div className="about-links">
        <Button size="small" type="link" icon={<GithubOutlined />} href={REPO} target="_blank" rel="noopener noreferrer">
          Project
        </Button>
        <Button size="small" type="link" icon={<TagOutlined />} href={`${REPO}/releases`} target="_blank" rel="noopener noreferrer">
          Releases
        </Button>
        <Button size="small" type="link" icon={<BookOutlined />} href={`${REPO}#readme`} target="_blank" rel="noopener noreferrer">
          Documentation
        </Button>
      </div>

      <p className="about-fineprint">
        Independent open-source project. Nutanix, Prism and NCC are trademarks of Nutanix, Inc.
      </p>
    </Modal>
  );
}
