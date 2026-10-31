import { useState } from "react";
import { Button, Modal } from "antd";
import { BookOutlined, CopyOutlined, GithubOutlined, TagOutlined } from "@ant-design/icons";
import { KeyValueList } from "../ui";
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

// Kept in step with LICENSE at the repository root.
const LICENSE_HOLDER = "Copyright (c) 2025 Prajwal Vernekar";
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
 * and where the project lives. Opened from the product name in the sidebar.
 */
export function AboutDialog({ open, onClose, build }: { open: boolean; onClose: () => void; build: AboutBuild }) {
  const [showLicense, setShowLicense] = useState(false);
  const shortVersion = (build.version || "").split("-")[0];

  const copyBuild = async () => {
    try {
      await navigator.clipboard.writeText(buildLine(build));
      notify.success("Version details copied");
    } catch {
      notify.info({ message: "Version details", description: buildLine(build), duration: 8 });
    }
  };

  return (
    <Modal
      open={open}
      onCancel={() => {
        setShowLicense(false);
        onClose();
      }}
      footer={null}
      width={560}
      title={null}
      className="about-dialog"
      destroyOnHidden
    >
      <div className="about-head">
        <img src="/logo.svg" alt="" className="about-logo" />
        <div>
          <h2 className="about-title">NCC Orchestrator</h2>
          <p className="about-subtitle">
            {shortVersion ? <span className="brand-version">v{shortVersion}</span> : null}
            <span>Cluster health checks at fleet scale</span>
          </p>
        </div>
      </div>

      <p className="about-text">
        Runs Nutanix Cluster Check (NCC) on your Prism Element clusters in parallel, gathers every finding in one place,
        and keeps track of how each alert is handled from run to run, with schedules, notifications and history.
      </p>

      <section className="about-section" aria-labelledby="about-build">
        <div className="about-section-head">
          <h3 id="about-build">This installation</h3>
          <Button size="small" type="text" icon={<CopyOutlined />} onClick={() => void copyBuild()}>
            Copy
          </Button>
        </div>
        <KeyValueList
          items={[
            { label: "Version", value: build.version || "Unknown" },
            { label: "Built", value: build.buildDate && build.buildDate !== "unknown" ? build.buildDate : "Unknown" },
            ...(build.stream ? [{ label: "Release stream", value: build.stream }] : []),
            ...(build.goVersion ? [{ label: "Go", value: build.goVersion }] : []),
            ...(build.os || build.arch ? [{ label: "Platform", value: [build.os, build.arch].filter(Boolean).join(" / ") }] : []),
          ]}
        />
      </section>

      <section className="about-section" aria-labelledby="about-license">
        <div className="about-section-head">
          <h3 id="about-license">License</h3>
          <Button size="small" type="text" onClick={() => setShowLicense((v) => !v)} aria-expanded={showLicense}>
            {showLicense ? "Hide license" : "View license"}
          </Button>
        </div>
        <p className="about-text">
          MIT License. {LICENSE_HOLDER}. Free to use, copy, modify and distribute, provided the copyright and license
          notice are kept.
        </p>
        {showLicense ? (
          <pre className="about-license" tabIndex={0} aria-label="MIT License text">
            {`MIT License\n\n${LICENSE_HOLDER}\n\n${MIT_TEXT}`}
          </pre>
        ) : null}
      </section>

      <div className="about-links">
        <Button icon={<GithubOutlined />} href={REPO} target="_blank" rel="noopener noreferrer">
          Project
        </Button>
        <Button icon={<TagOutlined />} href={`${REPO}/releases`} target="_blank" rel="noopener noreferrer">
          Releases
        </Button>
        <Button icon={<BookOutlined />} href={`${REPO}#readme`} target="_blank" rel="noopener noreferrer">
          Documentation
        </Button>
      </div>

      <p className="about-fineprint">Nutanix, Prism and NCC are trademarks of Nutanix, Inc.</p>
    </Modal>
  );
}
