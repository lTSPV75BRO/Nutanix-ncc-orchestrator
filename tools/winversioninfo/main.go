// Build-only helper that generates Windows VERSIONINFO resource files
// (.syso) for the three Go binaries shipped in the v2 stack. Linking
// these resources into the .exe surfaces product / copyright /
// version metadata in the Windows file Properties dialog and in PE
// inspection tools like sigcheck.exe — giving administrators a clear
// "this binary is the NCC Orchestrator v2 stack, version 2.0.2,
// from <project repository>" signal without any code-signing
// certificate involvement.
//
// This project is an independent open-source tool (MIT licensed). It
// is NOT an official Nutanix product or distribution; the embedded
// metadata reflects the project itself and its upstream repository.
//
// Why generated, not committed: the .syso files are arch-specific
// resource blobs and they embed the VERSION number, so generating
// them at build time from binaryGO.txt's $VERSION keeps the embedded
// metadata in lock-step with the rest of the release artifacts.
//
// Usage (driven from binaryGO.txt before a Windows GOOS=windows build):
//
//	go run ./tools/winversioninfo \
//	  --version=2.0.2 \
//	  --git-rev=abcdef1 \
//	  --output-dir=. \
//	  --arch=amd64
//
// Produces:
//
//	./resource_windows_amd64.syso             (orchestrator)
//	./cmd/ncc-api-server/resource_windows_amd64.syso
//	./cmd/ncc-ui-server/resource_windows_amd64.syso
//
// Go's build system auto-picks up *_windows_<arch>.syso from a package
// directory and links it into the resulting .exe.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/josephspurrier/goversioninfo"
)

// Embedded metadata constants. These intentionally do NOT name a
// corporate vendor — this project is an independent open-source tool
// (see README + LICENSE). The companyName field surfaces in the
// Windows file Properties dialog as the string most user-facing,
// so we set it to the project name itself.
const (
	companyName    = "ncc-orchestrator (open-source project)"
	productName    = "NCC Orchestrator"
	copyrightLine  = "(c) 2025-2026 Prajwal Vernekar and contributors. MIT licensed; see LICENSE."
	trademarksLine = "NCC and Nutanix are trademarks of their respective owners; this project is not affiliated with or endorsed by Nutanix, Inc."
	supportSiteURL = "https://github.com/lTSPV75BRO/Nutanix-ncc-orchestrator"
)

type binarySpec struct {
	exeBaseName     string // e.g. "ncc-orchestrator.exe"
	internalName    string // shown in Properties dialog
	fileDescription string
	pkgRelDir       string // "" for repo root, "cmd/ncc-api-server" for sub-mains
}

var binaries = []binarySpec{
	{
		exeBaseName:     "ncc-orchestrator.exe",
		internalName:    "ncc-orchestrator",
		fileDescription: "NCC Orchestrator (CLI + v2 lifecycle manager)",
		pkgRelDir:       ".",
	},
	{
		exeBaseName:     "ncc-api-server.exe",
		internalName:    "ncc-api-server",
		fileDescription: "NCC Orchestrator API Server (REST control plane)",
		pkgRelDir:       "cmd/ncc-api-server",
	},
	{
		exeBaseName:     "ncc-ui-server.exe",
		internalName:    "ncc-ui-server",
		fileDescription: "NCC Orchestrator UI Server (static + reverse proxy)",
		pkgRelDir:       "cmd/ncc-ui-server",
	},
}

func main() {
	var (
		version   string
		gitRev    string
		outputDir string
		arch      string
	)
	flag.StringVar(&version, "version", "0.0.0", "Product version (e.g. 2.0.2)")
	flag.StringVar(&gitRev, "git-rev", "", "Short git revision (optional)")
	flag.StringVar(&outputDir, "output-dir", ".", "Repo root used as base for package paths")
	flag.StringVar(&arch, "arch", "amd64", "Target windows arch: amd64 or arm64 (matches the Go toolchain naming)")
	flag.Parse()

	if arch != "amd64" && arch != "arm64" && arch != "386" {
		log.Fatalf("unsupported arch %q (want amd64 / arm64 / 386)", arch)
	}

	major, minor, patch := splitVersion(version)
	productVersion := version
	if gitRev != "" {
		productVersion = version + "+" + gitRev
	}

	for _, b := range binaries {
		vi := goversioninfo.VersionInfo{
			FixedFileInfo: goversioninfo.FixedFileInfo{
				FileVersion: goversioninfo.FileVersion{
					Major: major, Minor: minor, Patch: patch, Build: 0,
				},
				ProductVersion: goversioninfo.FileVersion{
					Major: major, Minor: minor, Patch: patch, Build: 0,
				},
				FileFlagsMask: "3f",
				FileFlags:     "00",
				FileOS:        "040004", // VOS_NT_WINDOWS32
				FileType:      "01",     // VFT_APP
				FileSubType:   "00",
			},
			StringFileInfo: goversioninfo.StringFileInfo{
				CompanyName:      companyName,
				FileDescription:  b.fileDescription,
				FileVersion:      productVersion,
				InternalName:     b.internalName,
				LegalCopyright:   copyrightLine,
				LegalTrademarks:  trademarksLine,
				OriginalFilename: b.exeBaseName,
				ProductName:      productName,
				ProductVersion:   productVersion,
				Comments:         "Independent open-source project (MIT). Source: " + supportSiteURL,
			},
			VarFileInfo: goversioninfo.VarFileInfo{
				Translation: goversioninfo.Translation{
					LangID:    goversioninfo.LngUSEnglish,
					CharsetID: goversioninfo.CsUnicode,
				},
			},
		}
		vi.Build()
		vi.Walk()
		dst := filepath.Join(outputDir, b.pkgRelDir,
			"resource_windows_"+arch+".syso")
		if err := vi.WriteSyso(dst, archGoTarget(arch)); err != nil {
			log.Fatalf("write %s: %v", dst, err)
		}
		fmt.Fprintf(os.Stderr, "wrote %s (%s %s)\n", dst, b.exeBaseName, productVersion)
	}
}

// archGoTarget maps our -arch CLI value to the GOARCH name the
// goversioninfo library expects.
func archGoTarget(arch string) string {
	switch arch {
	case "amd64":
		return "amd64"
	case "arm64":
		return "arm64"
	case "386":
		return "386"
	}
	return "amd64"
}

// splitVersion parses "2.0.2" into (2, 0, 2). Anything past the patch
// component is ignored. Suffixes like "-rc1" are stripped before
// parsing so a -rc tag still produces a valid VERSIONINFO triple.
func splitVersion(v string) (int, int, int) {
	v = strings.TrimPrefix(v, "v")
	if i := strings.IndexAny(v, "-+"); i >= 0 {
		v = v[:i]
	}
	parts := strings.Split(v, ".")
	out := []int{0, 0, 0}
	for i := 0; i < 3 && i < len(parts); i++ {
		n, err := strconv.Atoi(parts[i])
		if err != nil {
			continue
		}
		out[i] = n
	}
	return out[0], out[1], out[2]
}
