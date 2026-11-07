# Project website

Source of the public NCC Orchestrator website, deployed to GitHub Pages by
[`.github/workflows/pages.yml`](../.github/workflows/pages.yml). Once Pages is
on, it is served at `https://ltspv75bro.github.io/Nutanix-ncc-orchestrator/`.

| Path | Source | What it is |
| --- | --- | --- |
| `/` | `index.html` | Landing page: what the tool does, features, screenshots, quick start, links to the docs |
| `/download.html` | `download.html`, `assets/downloads.js` | Downloads for the latest release, install steps per OS, checksum and provenance verification |
| `/demo/` | `frontend/` built with `frontend/vite.demo.config.ts` | The real dashboard, read-only, against sample data (`frontend/src/demo/`) |
| `/data/latest-release.json` | written by the workflow | Snapshot of the latest release, used if the GitHub API is unreachable |

## Why this layout

- **`site/`, not `docs/`.** `docs/` holds the Markdown documentation people read
  on GitHub; mixing a website into it (or using Pages' "deploy from `/docs`"
  mode) would publish those files too and tie the site to a branch folder.
  `site/` holds only the static pages; the workflow assembles the final site.
- **No site generator.** Two pages of plain HTML, CSS and a little JavaScript.
  Nothing to install or update, and nothing for Dependabot to chase.
- **The demo is the real frontend.** `vite.demo.config.ts` imports
  `src/demo/install.ts` first, which answers `/api/*` from fixtures typed
  against `src/api/types.ts` (so `tsc -b` catches fixture drift), refuses
  every change with a "read-only demo" message, and switches to hash routing so
  deep links work from a sub-folder. No app source file is changed.

## Downloads stay current without edits

`download.html` asks the GitHub REST API for the latest release when the page
loads, so a new release appears as soon as it is published. If the API does not
answer (it allows 60 unauthenticated requests per hour per IP), the page uses
`data/latest-release.json`, which the workflow refreshes on every deploy and
after every successful `Release` workflow run. It lists `ncc-v2-stack-*`
archives, standalone `ncc-orchestrator-*` binaries and `checksums.txt` (plus
the example config, notes, SBOMs and attestation under "Other files"), with
each file's SHA-256 from GitHub. Standalone `ncc-api-server-*` /
`ncc-ui-server-*` binaries are never listed (release checklist in
`CHANGELOG.md`), and the workflow fails if a page links one.

## Hosting: GitHub Pages

Free for public repositories, no account or secret beyond the repository
itself, HTTPS included, and the deploy runs in the same Actions setup as CI and
releases. Cloudflare Pages or Netlify would also work (both have free tiers and
build from GitHub), but they add a second service, its own access tokens and a
second place to look when a deploy fails, and nothing here needs their extras
(edge functions, form handling, preview URLs per PR).

### Turn it on (one time)

1. Merge this to `main` (the workflow deploys from the default branch only).
2. GitHub → **Settings → Pages → Build and deployment → Source: GitHub Actions**.
3. **Actions → Website → Run workflow** (or push a change under `site/` or
   `frontend/`). The deploy job prints the URL.
4. Optional: add the URL to the repository's **About → Website** field and the
   README.

A custom domain can be set later under Settings → Pages; add a `CNAME` file to
`site/` with the domain.

## Work on it locally

Static pages: open `site/index.html` through any static server (the release
fetch needs `http://`, not `file://`), for example:

```bash
python -m http.server 8000 --directory site
```

The demo (from `frontend/`):

```bash
npm run build:demo -- --outDir ../site/demo   # do not commit site/demo
```

or `npx vite --config vite.demo.config.ts` for a dev server with hot reload.

## Design

The pages use the app's design system (`frontend/src/styles.css`): the same
font stack, colour tokens, spacing scale, radii, panels, stat tiles, buttons
and segmented control. Dark is the default, as in the app; the header toggle
switches to light and stores the choice under the app's own `app.theme` key,
so the site and the demo (same origin) always show the same theme. When the
app's tokens change, update the matching variables at the top of
`assets/style.css`.

Screenshots in `assets/screens/` are 1440×900 captures of the demo, one per
theme (`*.png` dark, `*-light.png` light); `assets/site.js` shows the one that
matches the current theme. Re-take them after visible UI changes, for example
with headless Chrome or Edge (`--headless=new --force-prefers-reduced-motion
--window-size=1440,900 --screenshot=overview.png <demo URL>`), using a browser
profile where `localStorage["app.theme"]` is `"light"` for the light set.
