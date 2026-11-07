// Renders the download table for the latest release (see release.js).
// Shows only what the release checklist in CHANGELOG.md allows users to
// install: ncc-v2-stack-* archives, standalone ncc-orchestrator-* binaries and
// checksums.txt (plus the example config, notes and SBOMs as "other files").
// Standalone ncc-api-server-* / ncc-ui-server-* binaries are never listed.
(function () {
  "use strict";
  var RELEASES = "https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator/releases";
  var OS_LABEL = { linux: "Linux", darwin: "macOS", windows: "Windows" };
  var ARCH_LABEL = { amd64: "x86-64 (amd64)", arm64: "ARM64" };
  var STACK = /^ncc-v2-stack-(linux|darwin|windows)-(amd64|arm64)\.(tar\.gz|zip)$/;
  var CLI = /^ncc-orchestrator-(linux|darwin|windows)-(amd64|arm64)(\.exe)?$/;
  var NEVER = /^ncc-(api|ui)-server-/;

  var filesEl = document.getElementById("files");
  var metaEl = document.getElementById("release-meta");
  var noteEl = document.getElementById("source-note");
  var tabs = Array.prototype.slice.call(document.querySelectorAll('[role="tab"]'));
  var panels = Array.prototype.slice.call(document.querySelectorAll(".os-panel"));
  var release = null;
  var current = detectOS();

  function detectOS() {
    var p = ((navigator.userAgentData && navigator.userAgentData.platform) || navigator.platform || navigator.userAgent || "").toLowerCase();
    if (p.indexOf("win") >= 0) return "windows";
    if (p.indexOf("mac") >= 0 || p.indexOf("iphone") >= 0 || p.indexOf("ipad") >= 0) return "darwin";
    return "linux";
  }
  // Browsers do not reliably report the CPU; Macs sold since 2020 are Apple
  // silicon, and most Linux and Windows servers are x86-64.
  function preferredArch(os) {
    return os === "darwin" ? "arm64" : "amd64";
  }

  function el(tag, attrs, children) {
    var n = document.createElement(tag);
    if (attrs) for (var k in attrs) {
      if (k === "text") n.textContent = attrs[k];
      else if (k === "class") n.className = attrs[k];
      else n.setAttribute(k, attrs[k]);
    }
    (children || []).forEach(function (c) { if (c) n.appendChild(typeof c === "string" ? document.createTextNode(c) : c); });
    return n;
  }
  function size(bytes) {
    if (!bytes && bytes !== 0) return "";
    if (bytes < 1024) return bytes + " B";
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
    return (bytes / 1024 / 1024).toFixed(1) + " MB";
  }
  function sha(asset) {
    var d = asset.digest || "";
    return d.indexOf("sha256:") === 0 ? d.slice(7) : "";
  }
  function copyButton(text, label) {
    var b = el("button", { type: "button", class: "copy", "aria-label": label, text: "Copy" });
    b.addEventListener("click", function () {
      var done = function () { b.textContent = "Copied"; setTimeout(function () { b.textContent = "Copy"; }, 1500); };
      if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(text).then(done, function () {});
      else { var t = el("textarea"); t.value = text; document.body.appendChild(t); t.select(); try { document.execCommand("copy"); done(); } catch (e) {} t.remove(); }
    });
    return b;
  }

  function classify(assets) {
    var out = { stack: [], cli: [], checksums: null, other: [] };
    assets.forEach(function (a) {
      var m;
      if (NEVER.test(a.name)) return;
      if ((m = STACK.exec(a.name))) out.stack.push({ a: a, os: m[1], arch: m[2] });
      else if ((m = CLI.exec(a.name))) out.cli.push({ a: a, os: m[1], arch: m[2] });
      else if (a.name === "checksums.txt") out.checksums = a;
      else if (/^example_config\.yaml$|^RELEASE_NOTES_.*\.md$|^release-attestation\.json$|^bom-.*\.cdx\.json$/.test(a.name)) out.other.push({ a: a });
    });
    var archOrder = function (x, y) { return x.arch === y.arch ? 0 : x.arch === preferredArch(x.os) ? -1 : 1; };
    out.stack.sort(archOrder);
    out.cli.sort(archOrder);
    return out;
  }

  function fileRow(item, kind, desc, recommended) {
    var a = item.a;
    var hash = sha(a);
    var fileCell = el("td", { class: "file" }, [
      el("a", { href: a.browser_download_url, text: a.name }),
      recommended ? el("span", { class: "badge", text: "Recommended" }) : null,
      desc ? el("small", { text: desc }) : null,
      hash ? el("div", { class: "sha" }, [el("span", { text: "SHA-256 " + hash }), copyButton(hash, "Copy SHA-256 of " + a.name)]) : null,
    ]);
    return el("tr", null, [
      fileCell,
      el("td", { class: "num hide-sm", text: kind }),
      el("td", { class: "num", text: size(a.size) }),
      el("td", { class: "act" }, [el("a", { class: "btn btn-sm", href: a.browser_download_url, "aria-label": "Download " + a.name, text: "Download" })]),
    ]);
  }

  function table(rows) {
    var t = el("table", { class: "dl-table" }, [
      el("thead", null, [el("tr", null, [el("th", { text: "File" }), el("th", { class: "hide-sm", text: "Type" }), el("th", { text: "Size" }), el("th", { text: "" })])]),
      el("tbody", null, rows),
    ]);
    return el("div", { class: "table-scroll" }, [t]);
  }

  function render() {
    tabs.forEach(function (t) { t.setAttribute("aria-selected", String(t.dataset.os === current)); t.tabIndex = t.dataset.os === current ? 0 : -1; });
    panels.forEach(function (p) { p.hidden = p.dataset.os !== current; });
    filesEl.setAttribute("aria-labelledby", "tab-" + current);
    if (!release) return;

    var c = classify(release.assets);
    var rows = [];
    var pref = preferredArch(current);
    c.stack.filter(function (x) { return x.os === current; }).forEach(function (x) {
      rows.push(fileRow(x, "Full stack", "Dashboard, API and runner for " + OS_LABEL[x.os] + " " + ARCH_LABEL[x.arch] + ": bin/, frontend-dist/ and example_config.yaml", x.arch === pref));
    });
    c.cli.filter(function (x) { return x.os === current; }).forEach(function (x) {
      rows.push(fileRow(x, "CLI only", "Standalone runner for " + OS_LABEL[x.os] + " " + ARCH_LABEL[x.arch] + " (scans, reports, scheduling; no dashboard)", false));
    });
    if (c.checksums) rows.push(fileRow({ a: c.checksums }, "Checksums", "SHA-256 of every file in this release", false));

    filesEl.textContent = "";
    if (rows.length === 0) {
      filesEl.appendChild(el("p", { class: "loading" }, ["No " + OS_LABEL[current] + " files in " + release.tag_name + ". ", el("a", { href: release.html_url || RELEASES, text: "See the release on GitHub" }), "."]));
    } else {
      filesEl.appendChild(table(rows));
    }
    if (c.other.length) {
      var details = el("details", { class: "more" }, [
        el("summary", { text: "Other files: example config, release notes, SBOMs, attestation (" + c.other.length + ")" }),
        el("div", null, [table(c.other.map(function (x) {
          var kind = /\.ya?ml$/.test(x.a.name) ? "Config" : /\.md$/.test(x.a.name) ? "Notes" : /bom-/.test(x.a.name) ? "SBOM" : "Provenance";
          return fileRow(x, kind, "", false);
        }))]),
      ]);
      filesEl.appendChild(details);
    }
  }

  function renderMeta(rel) {
    metaEl.textContent = "";
    if (!rel) {
      metaEl.appendChild(el("span", null, ["Could not load the release list. ", el("a", { href: RELEASES, text: "Open the releases on GitHub" }), "."]));
      filesEl.textContent = "";
      filesEl.appendChild(el("p", { class: "loading" }, ["GitHub did not answer (it allows 60 requests per hour from one address). ", el("a", { href: RELEASES + "/latest", text: "Download the latest release from GitHub" }), " instead."]));
      return;
    }
    var title = document.getElementById("files-title");
    if (title) title.textContent = "Release files · " + rel.tag_name;
    var date = rel.published_at ? new Date(rel.published_at).toLocaleDateString(undefined, { year: "numeric", month: "long", day: "numeric" }) : "";
    metaEl.appendChild(el("span", { class: "chip", text: "LATEST" }));
    metaEl.appendChild(el("strong", { text: rel.tag_name }));
    if (date) metaEl.appendChild(el("span", { text: "Published " + date }));
    metaEl.appendChild(el("a", { href: rel.html_url || RELEASES, text: "Release notes" }));
    metaEl.appendChild(el("a", { href: RELEASES, text: "All releases" }));
    Array.prototype.forEach.call(document.querySelectorAll("[data-tag]"), function (n) { n.textContent = rel.tag_name; });
    noteEl.textContent = rel._source === "snapshot"
      ? "Showing the release list saved when this site was last published; GitHub's live API was not reachable."
      : "";
  }

  tabs.forEach(function (t, i) {
    t.addEventListener("click", function () { current = t.dataset.os; render(); });
    t.addEventListener("keydown", function (e) {
      var d = e.key === "ArrowRight" ? 1 : e.key === "ArrowLeft" ? -1 : 0;
      if (!d) return;
      var n = tabs[(i + d + tabs.length) % tabs.length];
      n.focus(); n.click();
    });
  });

  render();
  window.nccRelease.then(function (rel) {
    release = rel;
    renderMeta(rel);
    render();
  });
})();
