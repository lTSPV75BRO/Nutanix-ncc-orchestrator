# Unslop plan for the app pages

Audit of every page in the demo build (dark theme, 1440×900, signed in as
admin), with the shared stylesheet measured. Nothing is changed yet. The
sign-in page is done and is left out.

Method: unslop by Damir Sokolovsky ([@heydamir](https://x.com/heydamir)), CC BY 4.0.

## Verdict

**Light slop.** The app already has a design system (`docs/ui/DESIGN_SYSTEM.md`),
quiet dark surfaces, one icon set (Ant Design outlined), one font (Inter),
a sensible sidebar (36 px rows, 4 px apart) and no emoji, gradient text or
glowing cards. What is left is a handful of habits repeated on every page.
So this is **polish mode**: fix what the audit found, keep the palette, the
violet accent, the logo, the layout and the order of everything.

Most fixes live in `frontend/src/styles.css` and `frontend/src/theme.tsx`,
so one change fixes many pages.

## Direction

- **Product:** runs Nutanix NCC health checks across many clusters and turns
  the results into findings for the people who keep those clusters running.
- **Character:** exact, steady, quietly nocturnal.
- **World:** the operations desk on a night shift: rack rows, status lights,
  a checklist being worked through.
- **Moments:** the result itself. On Overview the "Checks by result" bars and
  the cluster list, on Insights the results bar, on a cluster page its open
  findings. Colour lives there and nowhere else.
- **Signature:** the status dot (pass, warning, fail, error, not checked),
  used everywhere a result appears, in place of the coloured pills.

Kept on purpose: the violet accent and logo, the flowing wave and wash on the
sign-in page, Inter, the sidebar structure, the three themes.

## Cross-app fixes (do once, every page benefits)

| # | Sign | Where | Fix |
|---|------|-------|-----|
| 1 | Too many 600/700 weights: 66 × `font-weight: 600`, 1 × 700 | `styles.css`, `theme.tsx` (`fontWeightStrong: 600`), table headers, top bar title, Trigger button | 500 for titles, table headers, buttons and labels; 600 only for the big stat figures |
| 2 | Uppercase tracked labels | sidebar groups "SETTINGS", "SYSTEM" (`.ui-sidebar-nav .ant-menu-item-group-title`); config groups "SOURCE", "CLUSTER LIST" (`.config-group-title`); findings table header "SEVERITY", "CLUSTER" (`.alerts-table` thead); `.integration-step-where`; `SecretsMigrationModal.tsx` | Sentence case, no letter spacing, muted colour |
| 3 | Coloured shadow on primary buttons: antd's `0 2px 0 rgba(147,100,254,.3)` | every primary button | `primaryShadow: "none"` in the theme |
| 4 | Trigger button differs from every other button: 36 px tall, radius 10, weight 600, 0.2 px tracking | top bar | Same as the other primary buttons: 32 px, radius 8, weight 500 |
| 5 | A violet icon tile in front of every card title and the page title | "Checks by result", "Clusters", "Recent runs", "History", "Open findings", "Sources", "Runner", "Software updates", "Where to send", "Readiness", "Results", "Local accounts", "API" and the top bar | Drop the tile; keep a 16 px icon in the muted text colour, or no icon. The accent is then free for the selection and the primary action |
| 6 | Every stat figure coloured (green 93.2 %, red 26, red 7/8, orange 3, green 14) | stat cards on Overview, Insights, Run details, Cluster, Health & repair, System status, Schedule | Figures in the primary text colour; the meaning goes into a status dot and a change with what it compares to, e.g. "26 · 1 more than the previous run" |
| 7 | Three vocabularies for the same results | Overview "Fail / Error / Warning", Findings "FAIL / WARN / ERR", clusters "Failing / Warnings / Healthy" | One set: Fail, Error, Warning, Info, Pass, Not checked; a cluster is Failing, Warnings or Healthy |
| 8 | Severity as tinted, bordered pills, upper case in tables | Findings summary bar (FAIL 7, WARN 18, ERR 1, INFO 23 with coloured borders), severity column, Insights, Cluster page, Logs level | Status dot plus the word in sentence case; counts in quiet tabular figures |
| 9 | Badges as coloured pills | "v2.4.0" (violet) and "Admin" (orange) in the chrome, "Beta" on Features, "Fresh", "Partial · exit 3", "Moderate / Easy / Advanced" on Integrations | Version as muted mono text, role as muted text after the name, status as dot plus text; one quiet "Beta" text label |
| 10 | Off-grid spacing: `6` (39×), `10` (35×), `14` (11×), `18` (9×), `22`, `38`, `7`, `9`, `5` | `styles.css` | Nearest 4 px step (4, 8, 12, 16, 20, 24, 36, 40); 2 px only inside small controls |
| 11 | Icons at 12, 14, 15 and 16 px | across pages | 16 px, 14 px only inside dense table rows |
| 12 | Single-sentence subtitles and captions ending with a period | page subtitles ("Which clusters to check, how to sign in to them, and how runs behave."), card descriptions, alerts | No period on single sentences; paragraphs keep theirs |
| 13 | Numbers in mixed form | "3,227" next to "3276"; percentages with and without decimals | One format (thousands separator, one decimal for rates) and `tabular-nums` in tables and stats |

Motion is fine: the only looping animations are loaders and the Trigger
pulse while a run is actually running, which is live. The wave is your choice.

## Page by page

**Overview**
- Regression and policy-gate banners stack two full-width coloured boxes above
  the stats. Merge them into one quiet "Needs attention" strip with a dot per
  item; keep the colour on the dot.
- Stat row: four equal cards. Keep them, but neutral figures and a comparison
  line (fix 6).
- "Checks by result": Pass dominates, so Fail, Error and Warning bars are
  slivers. Scale the bars to the problems (as the sign-in preview did) and show
  Pass as a number only. This is the page's moment.
- Clusters list: status pills become dots plus text; right-align the counts in
  one column.

**Findings**
- Summary bar of bordered pills (fix 8) and an "UNKNOWN 0" that is always 0:
  hide zero counts.
- Search placeholder "Search findings (e.g. cluster:10.1)" is cut off at this
  width: "Search findings", with the syntax in a help tooltip.
- Table header in upper case (fix 2); severity tags (fix 8); the KB column uses
  violet pills for every row, which makes violet the loudest colour on the page:
  plain link text "KB 4600".
- Six view toggles (Table, Focused, By check, List, Comfortable, Full page) in
  one row: group them into one segmented control and one density menu.

**Insights**
- Stat cards (fix 6). "Clusters at risk 4/7" in red and "Health score" in green.
- "What this run is saying": five rows each with a coloured icon; use the status
  dot and let the text carry it.
- Top checks table: KB links wrap onto two lines ("KB / 4600"); keep them on one
  line.

**Integrations**
- Three numbered step cards and six guide cards each have an icon tile and a
  difficulty pill (Moderate, Easy, Advanced); make difficulty and time one
  muted line ("Moderate · about 20 minutes").
- "Search e.g. Splunk" → "Search".

**Runs**
- Clean table. Status pills (fix 8); "Partial · exit 3" → "Partial (exit 3)" as
  text with a dot. Right-align Duration and Clusters.

**Run details**
- Stat cards (fix 6); "Re-run 1 failed cluster" is good.
- Cluster table results "3 fail · 5 warn · 5 info": the same format as Overview.

**Cluster page**
- Stat cards (fix 6); "Health 87" in green.
- "Failing checks" badge in the header plus three buttons; fine, but the badge
  becomes a dot plus text.
- "About this cluster" has the copy icon; good.

**Clusters & config**
- Upper-case group labels (fix 2).
- "Flat layout" / "Nested layout" is a tag next to the config picker; make it
  muted text.
- Cluster chips in the field are violet-free already; good.

**Schedule**
- Four stat cards with "Custom", "Soon", "42m ago", "Succeeded": these are
  words, not numbers, at 30 px. Show them as a small definition list, or make
  "Next run" the one large value with its time.

**Notifications**
- Each channel card has an icon tile (fix 5); toggles fine.
- "Delivering" and "Failing" status pills (fix 8).

**Access**
- Roles shown as orange, blue and grey pills inside sentences ("Roles: admin
  full access…"): plain text in the sentence, quiet text in the table.
- "Delete" buttons are red outlined in every row; keep them as text buttons in
  the danger colour on hover only.

**Features**
- Fine. "Beta" pills (fix 9).

**System status**
- "Run activity": seven full-height saturated bars that all show "1" (equal
  heights say nothing). Make them thin and scaled to the number of runs, grey
  for success and red for failed, with the day label in muted mono. This is the
  page's moment.
- Platform "linux/amd64" set at the stat size; use the normal text size.

**Health & repair**
- Stat cards (fix 6); "Readiness" rows use pills "Ready" / "Not ready": dot plus
  text.

**Updates & backups**
- Fine. The "In-app updates are not available" box: one info alert is enough;
  remove the bold heading inside it.

**Logs**
- Level pills (Warning, Info, Error, Debug) and a coloured left border per row:
  keep the left border (it scans well), make the level a muted word.
- Monospace message lines run to 10 px gaps; set the row rhythm to 8.

**Audit log**
- Clean. "Succeeded" / "Failed" pills (fix 8).

**Developer**
- "Sensitive request headers/body are not persisted in browser storage."
  alert takes a full-width box; make it a muted hint under the form.

## Order of work

**Status (2026-11-27):** steps 1 to 5 are done. In step 4 the Run activity
chart keeps its original full-width bars (the owner's choice); only its
figures are neutral now. Steps 1 to 3: Not done from them: the
comparison line on stat cards ("1 more than the previous run") needs data from
the previous run on each page, and the role, Beta and difficulty pills on the
Access and Integrations pages are left for the page-by-page pass.

1. **Theme and tokens** (fixes 1, 3, 4, 10, 11, 13): `theme.tsx`, `styles.css`.
   Small diff, visible everywhere. Check the three themes.
2. **Labels and badges** (fixes 2, 7, 8, 9, 12): a shared `StatusDot` + text
   component replaces the severity and status tags; one wording table.
3. **Card headers and stats** (fixes 5, 6): one change to the shared card
   header and stat components.
4. **Page moments**: Overview "Checks by result", System status "Run activity",
   Findings toolbar, Schedule stats.
5. **Copy pass** page by page (subtitles, placeholders, alerts).

After each step: `npm test`, `npm run test:a11y`, screenshots of Overview,
Findings and System status in dark, light and midnight themes.

## Questions before starting

- Keep the coloured stat figures (fix 6) or move to neutral figures with dots?
- Status pills → dot plus text everywhere (fix 8), including Logs and Access?
- Do all of it, or only steps 1 to 3 for 2.4.0 and the rest later?
