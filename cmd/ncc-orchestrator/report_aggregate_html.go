package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

func writeAggregatedHTMLSingle(fs FS, outDir string, rows []AggBlock, perCluster []struct{ Cluster, HTML, CSV string }, cfg Config) error {
	if err := fs.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", outDir, err)
	}
	path := filepath.Join(outDir, "index.html")
	abs, _ := filepath.Abs(path)
	const tmpl = `<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>NCC Aggregated Report</title>
	<link rel="icon" type="image/svg+xml" href="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMzIiIGhlaWdodD0iMzIiIHZpZXdCb3g9IjAgMCAzMiAzMiIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHJlY3QgeD0iMiIgeT0iMiIgd2lkdGg9IjI4IiBoZWlnaHQ9IjI4IiByeD0iOCIgZmlsbD0iIzBCMTIyMCIvPgo8cGF0aCBkPSJNOSAyM1Y5SDEyTDIwIDE5VjlIMjNWMjNIMjBMMTIgMTNWMjNIOVoiIGZpbGw9IiMyMkM1NUUiLz4KPGNpcmNsZSBjeD0iMjQiIGN5PSI4IiByPSIyIiBmaWxsPSIjMzhCREY4Ii8+CjxjaXJjbGUgY3g9IjgiIGN5PSIyNCIgcj0iMiIgZmlsbD0iI0Y1OUUwQiIvPgo8L3N2Zz4=">
	<style>
	:root {
	  --bg: #0f172a;
	  --card: #111827;
	  --text: #e5e7eb;
	  --muted: #9ca3af;
	  --accent: #2563eb;
	  --row1: #0b1224;
	  --row2: #0e1630;
	  --border: #1f2937;
	  --fail: #ef4444;
	  --warn: #f59e0b;
	  --info: #3b82f6;
	  --details: #aaa;
	  --err:  #94a3b8;
	}
	* { box-sizing: border-box; }
	html, body { height: 100%; }
	body {
	  margin: 0;
	  font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
	  background: linear-gradient(180deg,#0b1224,#0e1630);
	  color: var(--text);
    background-color: var(--row1);
	}
	.container { max-width: 1400px; margin: 0 auto; padding: 7px 12px; }
	.header { display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; margin-bottom: 16px; flex-wrap: wrap; }
	.title { flex: 1; min-width: 0; }
	.title h1 { margin: 0; font-size: 22px; font-weight: 700; }
	.title .sub { color: var(--muted); font-size: 12px; margin-top: 4px; }
	.header-actions { flex-shrink: 0; }
	.controls { display: flex; flex-wrap: wrap; gap: 12px; align-items: center; margin: 12px 0 18px 0; }
	.control { background: #0d152b; border: 1px solid var(--border); border-radius: 10px; padding: 10px 12px; display: flex; gap: 8px; align-items: center; }
	.control label { font-size: 12px; color: var(--muted); margin-right: 6px; }
	input[type="text"] { background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 8px 10px; border-radius: 8px; outline: none; width: 280px; }
	select, button { background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 8px 10px; border-radius: 8px; outline: none; }
	button:hover { border-color: var(--accent); cursor: pointer; }
	.badge { display:inline-flex; align-items:center; gap:6px; padding: 6px 10px; border-radius: 999px; background:#0a1123; border:1px solid var(--border); user-select:none; }
	.badge .dot { width: 8px; height: 8px; border-radius: 999px; display:inline-block; }
	.dot.fail{ background: var(--fail); } .dot.warn{ background: var(--warn); }
	.dot.info{ background: var(--info); } .dot.err{ background: var(--err); }
	.legend { display:flex; gap:8px; flex-wrap: wrap; }
	.card { padding: 12px; }
	
	 
	.summary { display:grid; grid-template-columns: repeat(5, 1fr); gap:12px; margin: 16px 0; }
	.sum-item { background: #0a1123; border: 1px solid var(--border); border-radius: 10px; padding: 10px; }
	.sum-item .label { font-size: 12px; color: var(--muted); }
	.sum-item .count { font-size: 18px; font-weight: 700; margin-top: 6px; }
	.progress { height: 6px; border-radius: 999px; background: #0d152b; margin-top: 8px; overflow: hidden; border:1px solid var(--border); }
	.progress > span { display:block; height:100%; }
	.progress.fail > span { background: var(--fail); } .progress.warn > span { background: var(--warn); }
	.progress.err  > span { background: var(--err); }  .progress.info > span { background: var(--info); }
	
.scroll {
  overflow-x: auto;
  overflow-y: hidden;
  -webkit-overflow-scrolling: touch; /* smooth mobile scroll */
  border-radius: 12px;
  border: 1px solid var(--border);
  margin: 0 -12px 16px -12px; /* full width minus padding */
  padding: 12px;
}

@media (max-width: 1024px) {
  .scroll {
    margin: 0 -8px 16px -8px;
    padding: 8px;
  }
}


	.scroll::-webkit-scrollbar { height: 10px; }
	.scroll::-webkit-scrollbar-thumb { background: #22304d; border-radius: 8px; }
	.scroll::-webkit-scrollbar-track { background: #0a1123; }
	
	 
	table { width: 100%; border-collapse: collapse; table-layout: fixed; }
	thead th {
	  position: sticky; top: 0; z-index: 1;
	  background: #0d152b; border-bottom: 1px solid var(--border);
	  padding: 10px; text-align: left; font-size: 12px; color: var(--muted);
	}
	tbody td { padding: 10px; border-bottom: 1px solid var(--border); vertical-align: top; }
	thead th, tbody td { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
	
	tbody tr:nth-child(odd) { background: var(--row1); }
	tbody tr:nth-child(even){ background: var(--row2); }
	
	td .severity { padding: 2px 8px; border-radius: 999px; font-size: 12px; }
	.sev-FAIL { background: #2b0d0d; color: var(--fail); border: 1px solid #4c1d1d; }
	.sev-WARN { background: #2b1f0d; color: var(--warn); border: 1px solid #4a3112; }
	.sev-INFO { background: #0c1f35; color: var(--info); border: 1px solid #173e6d; }
	.sev-ERR  { background: #1b2130; color: var(--err);  border: 1px solid #2c354a; }
	
	small.mono { color: var(--muted); font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
	.highlight { background: #3b82f655; }
	
	 
	th.col-cname, td.col-cname { width: 190px; }
	th.col-cluster, td.col-cluster   { width: 140px; }
	th.col-sev,     td.col-sev       { width: 96px; }
	th.col-title,   td.col-title     { width: 240px; }
	th.col-kb,      td.col-kb        { width: 110px; }
	th.col-detail,  td.col-detail    { width: 640px; }
	th.col-actions, td.col-actions   { width: 220px; }
	
	td.col-detail { white-space: normal; overflow: visible; }
	.detail-full { color: var(--details); font-size: 13px; line-height: 1.35; }
	
	 
	tbody tr.selected { outline: 2px solid var(--accent); outline-offset: -2px; }
	.actions { white-space: nowrap; display: inline-flex; gap: 6px; flex-wrap: wrap; }
	.actions button { background:#0a1123; border:1px solid var(--border); color:var(--text); padding:6px 8px; border-radius:8px; }
	.actions button:hover { border-color: var(--accent); cursor:pointer; }
	
	 
	a { color: #93c5fd; text-decoration: none; }
	a:hover { text-decoration: underline; color: #bfdbfe; }
	a:visited { color: #a5b4fc; }
	a[href^="http"]::after {
	  content: "↗";
	  font-size: 11px;
	  margin-left: 4px;
	  color: #64748b;
	}
	   
.control input[type="checkbox"] {
  position: absolute;
  opacity: 0;
  cursor: pointer;
  height: 0;
  width: 0;
}


.control span {
  display: flex;
  align-items: center;
  justify-content: center;
  position: relative;
  padding-left: 24px;
  min-height: 16px;  
  cursor: pointer;
  color: var(--muted);
}


.control span::before {
  content: "";
  position: absolute;
  top: 50%;
  left: 0;
  transform: translateY(-50%);  
  height: 16px;
  width: 16px;
  background-color: #0a1123;
  border: 1px solid var(--border);
  border-radius: 4px;
  box-sizing: border-box;  
}


.control span::after {
  content: "";
  width: 9px;
  height: 9px;
  background-color: var(--muted);
  position: absolute;
  top: 50%;
  left: 8px;  
  transform: translate(-50%, -50%) scale(0);  
  transition: transform 0.2s ease-in-out;
  border-radius: 2px;
}


.control input[type="checkbox"]:checked ~ span::after {
  transform: translate(-50%, -50%) scale(1);
}


 
.control span:hover::before {
  border-color: var(--accent);
}


 
.control input[type="checkbox"]:focus + span::before {
  outline: 2px solid var(--accent);
}

.cluster-meta {
  font-size: 10px;
  color: var(--muted);
  margin-top: 2px;
}

.cluster-wrapper {
  display: flex;
  gap: 8px;
  align-items: center;
  flex: 1;
}

.cluster-display {
  flex: 1;
  background: #0a1123;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 10px 14px;
  min-height: 20px;
  cursor: pointer;
  position: relative;
  font-size: 13px;
}

.cluster-display:hover {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.05);
}

.cluster-placeholder {
  color: var(--text);
}

.cluster-toggle {
  background: #0a1123;
  border: 1px solid var(--border);
  color: var(--text);
  padding: 8px 12px;
  border-radius: 8px;
  font-size: 12px;
  cursor: pointer;
  min-width: 70px;
}

.cluster-toggle:hover {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.1);
}

.cluster-modal {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0,0,0,0.8);
  z-index: 10000;
  display: flex;
  align-items: center;
  justify-content: center;
}

.cluster-modal-content {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 24px;
  max-width: 500px;
  width: 90%;
  max-height: 80vh;  /* ✅ Fixed height */
  overflow-y: auto;  /* ✅ Scroll whole modal */
}

.cluster-list {
  max-height: 400px;        /* ✅ Scroll container */
  overflow-y: auto;         /* ✅ Vertical scroll */
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 8px;
  margin-bottom: 16px;
  background: #0a1123;
}

.cluster-list::-webkit-scrollbar {
  width: 8px;
}

.cluster-list::-webkit-scrollbar-track {
  background: #0a1123;
  border-radius: 4px;
}

.cluster-list::-webkit-scrollbar-thumb {
  background: #334155;
  border-radius: 4px;
}

.cluster-list::-webkit-scrollbar-thumb:hover {
  background: var(--accent);
}


.cluster-item {
  display: flex;
  align-items: center;
  padding: 12px;
  background: #0a1123;
  border: 1px solid var(--border);
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s;
  margin-bottom: 4px;  /* ✅ Spacing */
}

.cluster-item:hover {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.1);
}

.cluster-item.active {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.2);
  color: var(--accent);
}
.cluster-load-more {
  width: 100%;
  padding: 10px;
  background: #0a1123;
  border: 1px solid var(--border);
  color: var(--accent);
  border-radius: 8px;
  cursor: pointer;
  font-size: 13px;
}
.cluster-load-more:hover { border-color: var(--accent); background: rgba(37,99,235,0.1); }

.modal-buttons {
  display: flex;
  gap: 12px;
  justify-content: flex-end;
  margin-top: 20px;
  padding-top: 16px;
  border-top: 1px solid var(--border);
}

.cluster-status-wrapper {
  display: flex;
  gap: 8px;
  align-items: center;
  flex: 1;
}

.cluster-status {
  flex: 1;
  background: #0a1123;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 10px 16px;
  font-size: 13px;
  color: var(--text);
  cursor: pointer;
  position: relative;
  transition: all 0.2s ease;
  min-height: 20px;
  display: flex;
  align-items: center;
}

.cluster-status:hover {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.1);
}

.cluster-status::after {
  content: "▼";
  margin-left: 8px;
  color: var(--muted);
  font-size: 11px;
}

.cluster-status-empty {
  color: var(--muted);
}

.cluster-edit-btn {
  background: #0a1123;
  border: 1px solid var(--border);
  color: var(--text);
  padding: 10px 14px;
  border-radius: 8px;
  font-size: 12px;
  cursor: pointer;
  white-space: nowrap;
  transition: all 0.2s ease;
}

.cluster-edit-btn:hover {
  border-color: var(--accent);
  background: rgba(37, 99, 235, 0.1);
}



.tooltip {
  position: absolute;
  z-index: 1000;
  background: var(--card);
  color: var(--text);
  padding: 12px;
  border: 1px solid var(--border);
  border-radius: 8px;
  font-size: 13px;
  line-height: 1.4;
  max-width: 450px;
  box-shadow: 0 8px 24px rgba(0,0,0,0.4);
  white-space: pre-wrap;
  pointer-events: none;
}

.cluster-expand-btn {
  background: #0a1123 !important;
  border: 1px solid var(--border) !important;
  color: var(--accent) !important;
  padding: 8px 12px !important;
  border-radius: 6px !important;
  font-size: 12px !important;
  cursor: pointer !important;
  margin-top: 8px !important;
  width: 100% !important;
  text-align: left !important;
}

.cluster-expand-btn:hover {
  background: rgba(37,99,235,0.1) !important;
}

.full-cluster-table table th,
.full-cluster-table table td {
  padding: 6px 4px !important;
  text-align: right !important;
}

.full-cluster-table table th:first-child,
.full-cluster-table table td:first-child {
  text-align: left !important;
}


.compact-cluster-table {
  width: 100%;
  font-size: 12px;
  margin-top: 12px;
}

.compact-cluster-table th,
.compact-cluster-table td {
  padding: 6px 4px;
  text-align: right;
}

.compact-cluster-table th:first-child,
.compact-cluster-table td:first-child {
  text-align: left;
}

.compact-cluster-table tr:hover {
  background: rgba(37, 99, 235, 0.05);
}

.full-cluster-table {
  display: none;
  margin-top: 12px;
  max-height: 300px;
  overflow-y: auto;
}

.full-cluster-table.show {
  display: block;
}

#detailsToggle {
  background: #0a1123;
  border: 1px solid var(--border);
  color: var(--accent);
  padding: 6px 12px;
  border-radius: 6px;
  font-size: 12px;
  cursor: pointer;
  margin-top: 8px;
}

#detailsToggle:hover {
  background: rgba(37, 99, 235, 0.1);
}

.expand-clusters-btn {
  width: 100% !important;
  background: #0a1123 !important;
  border: 1px solid var(--border) !important;
  color: var(--accent) !important;
  padding: 10px 12px !important;
  border-radius: 8px !important;
  font-size: 13px !important;
  cursor: pointer !important;
  margin-top: 12px !important;
}

.expand-clusters-btn:hover {
  background: rgba(37,99,235,0.15) !important;
  border-color: var(--accent) !important;
}

.compact-cluster-table a,
.full-cluster-table a {
  color: #93c5fd !important;
  text-decoration: none !important;
}

.compact-cluster-table a:hover,
.full-cluster-table a:hover {
  text-decoration: underline !important;
  color: #bfdbfe !important;
}


@media (max-width: 768px) {
  .controls {
    flex-direction: column !important;
    gap: 8px;
    align-items: stretch;
  }
  
  .control {
    flex: 1;
    min-width: 0; /* allow shrinking */
  }
  
  .control input[type="text"] {
    width: 100%;
    font-size: 16px; /* mobile zoom fix */
  }
}

@media (max-width: 900px) {
  th.col-kb, td.col-kb,
  th.col-actions, td.col-actions {
    display: none; /* Hide KB/Actions on small screens */
  }
  
  th.col-detail, td.col-detail {
    min-width: 200px; /* Priority for details */
  }
}

@media (max-width: 600px) {
  th.col-cluster, td.col-cluster {
    display: none; /* Hide IP on tiny screens */
  }
  
  th.col-cname, td.col-cname {
    width: 140px !important; /* Fixed width for cluster names */
  }
}

/* 6. PRINT STYLES (clean PDF export) */
@media print {
  .controls, .actions, .summary, footer {
    display: none !important;
  }
  
  .scroll {
    overflow: visible !important;
    border: none !important;
    margin: 0 !important;
  }
  
  table {
    font-size: 9px !important;
    width: 100% !important;
  }
  
  th, td {
    padding: 4px 2px !important;
    border-bottom: 1px solid #ccc !important;
  }
}

/* 7. MOBILE SUMMARY CARDS (stack vertically) */
@media (max-width: 768px) {
  .summary {
    grid-template-columns: 1fr !important;
    gap: 8px;
    margin: 12px 0;
  }
}

/* 8. TOUCH TARGETS (min 44px for mobile) */
button, .cluster-display, .cluster-toggle {
  min-height: 44px;
  min-width: 44px;
}

@media (hover: none) and (pointer: coarse) {
  .actions button {
    padding: 12px 16px; /* bigger touch targets */
    font-size: 14px;
  }
}

*:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.skip-link { position: absolute; top: -40px; left: 8px; background: var(--accent); color: #fff; padding: 8px 12px; border-radius: 6px; z-index: 100; text-decoration: none; font-size: 14px; }
.skip-link:focus { top: 8px; }
.report-footer { font-size: 0.8125rem; color: var(--muted); margin-top: 16px; padding: 12px 0; border-top: 1px solid var(--border); }
.report-meta-line { margin-top: 8px; line-height: 1.5; word-break: break-word; }
.sum-item.clickable { cursor: pointer; transition: background 0.15s, border-color 0.15s; }
.sum-item.clickable:hover { background: #0d152b; border-color: var(--accent); }
.sum-item.clickable:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.th-sort { cursor: pointer; user-select: none; }
.th-sort:hover { color: var(--text); }
.th-sort .sort-arrow { font-size: 10px; margin-left: 4px; opacity: 0.7; }
.table-info { font-size: 12px; color: var(--muted); margin-bottom: 8px; }
.pagination { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 10px; margin-bottom: 12px; padding: 8px 10px; border: 1px solid var(--border); border-radius: 10px; background: #0a1123; }
.pagination button { background: #0f172a; border: 1px solid var(--border); color: var(--text); padding: 6px 10px; border-radius: 6px; cursor: pointer; font-size: 12px; min-width: 38px; transition: border-color 0.15s, background 0.15s; }
.pagination button:hover:not(:disabled) { border-color: var(--accent); }
.pagination button:disabled { opacity: 0.5; cursor: not-allowed; }
.pagination-info { font-size: 12px; color: var(--muted); }
.pagination-left { display: inline-flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.pagination-right { display: inline-flex; flex-wrap: wrap; gap: 10px; align-items: center; justify-content: flex-end; }
.pagination-pages { display: inline-flex; flex-wrap: wrap; gap: 6px; align-items: center; }
.pagination-page.active { border-color: var(--accent); background: rgba(37,99,235,0.18); color: #bfdbfe; }
.pagination-ellipsis { color: var(--muted); padding: 0 2px; font-size: 12px; }
.pagination-size { display: flex; align-items: center; gap: 6px; font-size: 12px; color: var(--muted); }
.pagination-size select { background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 4px 8px; border-radius: 6px; font-size: 12px; }
.pagination-jump { display: inline-flex; align-items: center; gap: 6px; font-size: 12px; color: var(--muted); }
.pagination-jump input { width: 62px; background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 4px 8px; border-radius: 6px; font-size: 12px; }
.empty-state { text-align: center; padding: 48px 16px; color: var(--muted); }
.empty-state p { margin: 0 0 12px 0; font-size: 14px; }
.per-cluster-btn { font-size: 12px; padding: 8px 14px; background: transparent; border: 1px solid var(--border); border-radius: 8px; color: var(--muted); cursor: pointer; display: inline-flex; align-items: center; gap: 6px; transition: border-color 0.15s, color 0.15s; }
.per-cluster-btn:hover { border-color: var(--accent); color: #93c5fd; }
.per-cluster-btn::after { content: "↗"; font-size: 11px; opacity: 0.8; }
.per-cluster-links { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.per-cluster-links a { font-size: 12px; padding: 4px 10px; background: #0a1123; border: 1px solid var(--border); border-radius: 6px; color: #93c5fd; text-decoration: none; }
.per-cluster-links a:hover { border-color: var(--accent); background: rgba(37,99,235,0.1); }
.row-badges { display:flex; gap:6px; flex-wrap:wrap; margin-top:6px; }
.ux-tag { font-size: 11px; padding: 2px 8px; border-radius: 999px; border: 1px solid var(--border); background:#0a1123; color: var(--muted); }
.ux-tag.tag-new { border-color: #7f1d1d; color: #fecaca; background: #3b1111; }
.ux-tag.tag-resolved { border-color: #14532d; color: #bbf7d0; background: #0f2d1d; }
.ux-tag.tag-changed { border-color: #1e3a8a; color: #bfdbfe; background: #10244f; }
.ux-tag.tag-flaky { border-color: #92400e; color: #fde68a; background: #3a2609; }
.insights { display:grid; grid-template-columns: repeat(3, 1fr); gap:12px; margin: 0 0 16px 0; }
.insights .sum-item .meta { font-size: 11px; color: var(--muted); margin-top: 6px; line-height: 1.4; max-height: 84px; overflow: auto; }
.meta { font-size: 12px; color: var(--muted); margin-top: 6px; line-height: 1.4; }
.health-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap:10px; margin-top: 10px; }
.health-card { border: 1px solid var(--border); background: #0a1123; border-radius: 10px; padding: 10px; }
.health-top { display:flex; justify-content: space-between; align-items: center; gap: 10px; }
.health-name { font-size: 13px; color: var(--text); font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.health-score { font-size: 12px; font-weight: 700; border-radius: 999px; padding: 3px 8px; border: 1px solid transparent; }
.health-score.good { color: #86efac; border-color: #14532d; background: #0f2d1d; }
.health-score.warn { color: #fde68a; border-color: #854d0e; background: #36250b; }
.health-score.bad { color: #fca5a5; border-color: #7f1d1d; background: #3b1111; }
.health-trend { margin-top: 8px; color: var(--muted); font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }
.health-bar { margin-top: 8px; height: 6px; background: #111827; border: 1px solid var(--border); border-radius: 999px; overflow: hidden; }
.health-bar > span { display: block; height: 100%; background: linear-gradient(90deg, #ef4444 0%, #f59e0b 45%, #22c55e 100%); }

@media (max-width: 768px) {
  .insights { grid-template-columns: 1fr !important; gap: 8px; }
  .pagination { padding: 8px; }
  .pagination-left, .pagination-right { width: 100%; }
  .pagination-right { justify-content: flex-start; }
}

	</style>
	<script>

	const AGG = {{.JSON}};
	let AGG_ROWS = AGG;
	const AGG_DATA_URL = "{{.AggDataURL}}";
	var CLUSTER_LINKS = {{.ClusterLinksJSON}};
	const DIFF_FLAGS = {{.DiffFlagsJSON}};
	const FLAKY_KEYS = {{.FlakyKeysJSON}};
	const POLICY_VIOLATIONS = {{.PolicyViolationsJSON}};
	const RUN_SUMMARY = {{.RunSummaryJSON}};
	const HEALTH_TRENDS = {{.HealthTrendsJSON}};
	const ARTIFACT_LINKS = {{.ArtifactLinksJSON}};
	const REPORT_META = {{.ReportMetaJSON}};

	let state = {
  sortKey: "severity",
  sortDir: "asc",
  filterSev: new Set(["FAIL","WARN","ERR","INFO"]),
  filterClusters: new Set(),
  search: "",
  showClusterModal: false,
  allClusters: [],
  pageSize: 100,
  currentPage: 0,
  filteredRows: [],
  compareMode: "all",
  clusterModalSearch: "",
  clusterListVisible: 100,
  showPerClusterModal: false,
  perClusterSearch: "",
  perClusterListVisible: 50
};
	
	const sevRank = { FAIL: 1, WARN: 2, ERR: 3, INFO: 4 };
	let selIndex = -1;

async function init() {
  if (AGG_ROWS.length === 0 && AGG_DATA_URL) {
    try {
      const resp = await fetch(AGG_DATA_URL, { cache: "no-store" });
      if (!resp.ok) throw new Error("HTTP " + resp.status);
      AGG_ROWS = await resp.json();
    } catch (e) {
      console.error("failed to load aggregated data sidecar", e);
    }
  }
  initClusters();
  renderMetadata();
  renderHealthWidget();
  renderArtifactLinks();
  updateAndRender();
  document.addEventListener("keydown", onKey);
  var statusEl = document.getElementById('clusterStatus');
  if (statusEl) {
    statusEl.onclick = toggleClusterFilter;
    statusEl.onkeydown = function(e) { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggleClusterFilter(); } };
  }
}

function rowKeyFor(cluster, check) {
  return ((cluster || "").trim().toLowerCase() + "||" + (check || "").trim().toLowerCase());
}

function renderInsights(rows) {
  var policyViolations = Array.isArray(POLICY_VIOLATIONS) ? POLICY_VIOLATIONS : [];
  var changed = 0;
  var flaky = 0;
  (rows || []).forEach(function(r) {
    var k = rowKeyFor(r.cluster, r.check);
    var d = DIFF_FLAGS[k];
    if (d && (d.new_fail || d.resolved_fail || d.severity_changed)) changed++;
    if (FLAKY_KEYS[k]) flaky++;
  });
  var changedEl = document.getElementById("changedChecksCount");
  if (changedEl) changedEl.textContent = String(changed);
  var flakyEl = document.getElementById("flakyChecksCount");
  if (flakyEl) flakyEl.textContent = String(flaky);
  var policyStatus = document.getElementById("policyStatus");
  var policyMeta = document.getElementById("policyMeta");
  if (policyStatus) {
    if (policyViolations.length > 0) {
      policyStatus.textContent = "FAILED";
      policyStatus.style.color = "var(--fail)";
    } else {
      policyStatus.textContent = "PASS/NOT_CONFIGURED";
      policyStatus.style.color = "#86efac";
    }
  }
  if (policyMeta) {
    if (policyViolations.length > 0) {
      policyMeta.innerHTML = policyViolations.map(function(v) { return "• " + escapeHtml(v); }).join("<br>");
    } else {
      policyMeta.textContent = "No policy gate violations file for this run.";
    }
  }
}

function sparkline(values) {
  if (!values || !values.length) return "";
  var blocks = ["▁","▂","▃","▄","▅","▆","▇","█"];
  return values.map(function(v) {
    var n = Math.max(0, Math.min(100, Number(v) || 0));
    var idx = Math.round((n / 100) * (blocks.length - 1));
    return blocks[idx];
  }).join("");
}

function renderMetadata() {
  var meta = document.getElementById("reportMetaFooter");
  if (!meta) return;
  var parts = [];
  if (REPORT_META.version) parts.push("Version: " + REPORT_META.version);
  if (REPORT_META.git_revision) parts.push("Git: " + REPORT_META.git_revision);
  if (REPORT_META.stream) parts.push("Stream: " + REPORT_META.stream);
  if (REPORT_META.build_date) parts.push("Build: " + REPORT_META.build_date);
  if (RUN_SUMMARY && RUN_SUMMARY.duration_s) parts.push("Run duration: " + Number(RUN_SUMMARY.duration_s).toFixed(1) + "s");
  if (REPORT_META.quiet_hours || REPORT_META.maintenance_windows) {
    var q = REPORT_META.quiet_hours ? ("quiet-hours=" + REPORT_META.quiet_hours) : "";
    var m = REPORT_META.maintenance_windows ? ("maintenance=" + REPORT_META.maintenance_windows) : "";
    parts.push([q, m].filter(Boolean).join(" "));
  } else {
    parts.push("Quiet-hours/maintenance: not configured");
  }
  meta.textContent = parts.join(" | ");
}

function renderHealthWidget() {
  var root = document.getElementById("healthWidget");
  if (!root) return;
  var clusters = (RUN_SUMMARY && RUN_SUMMARY.clusters) ? RUN_SUMMARY.clusters.slice() : [];
  // Keep health widget aligned with currently selected cluster filters.
  if (state && state.filterClusters) {
    var selectedAddresses = new Set();
    (AGG_ROWS || []).forEach(function(r) {
      var clusterLabel = displayClusterName(r);
      if (state.filterClusters.has(clusterLabel)) {
        selectedAddresses.add((r.cluster || "").trim());
      }
    });
    clusters = clusters.filter(function(c) {
      return selectedAddresses.has((c.address || "").trim());
    });
  }
  if (!clusters.length) {
    root.innerHTML = '<div class="meta">No per-cluster health data available for selected clusters.</div>';
    return;
  }
  clusters.sort(function(a, b) {
    var sa = Math.max(0, Math.min(100, Number(a.health_score || 0)));
    var sb = Math.max(0, Math.min(100, Number(b.health_score || 0)));
    if (sa !== sb) return sa - sb; // worst first
    return (a.address || "").localeCompare(b.address || "");
  });
  var visible = clusters.slice(0, 10);
  var rows = visible.map(function(c) {
    var k = (c.address || "").toLowerCase();
    var trend = HEALTH_TRENDS[k] || [];
    var score = Math.max(0, Math.min(100, Number(c.health_score || 0)));
    var cls = score >= 90 ? "good" : (score >= 75 ? "warn" : "bad");
    var trendText = trend.length ? sparkline(trend) : "n/a";
    return '<div class="health-card">' +
      '<div class="health-top">' +
        '<div class="health-name" title="' + escapeHtml(c.address || "") + '">' + escapeHtml(c.address || "-") + '</div>' +
        '<div class="health-score ' + cls + '">' + score.toFixed(0) + '/100</div>' +
      '</div>' +
      '<div class="health-trend">Trend: ' + escapeHtml(trendText) + '</div>' +
      '<div class="health-bar"><span style="width:' + score.toFixed(0) + '%"></span></div>' +
    '</div>';
  });
  var note = clusters.length > 10
    ? '<div class="meta">Showing 10 worst clusters out of ' + clusters.length + '.</div>'
    : '<div class="meta">Showing all ' + clusters.length + ' clusters.</div>';
  root.innerHTML = note + '<div class="health-grid">' + rows.join("") + '</div>';
}

function renderArtifactLinks() {
  var root = document.getElementById("artifactLinks");
  if (!root) return;
  var links = [];
  Object.keys(ARTIFACT_LINKS || {}).forEach(function(name) {
    var href = ARTIFACT_LINKS[name];
    if (!href) return;
    links.push('<a href="' + escapeHtml(href) + '" target="_blank" rel="noopener">' + escapeHtml(name) + '</a>');
  });
  root.innerHTML = links.length ? links.join("") : '<span class="meta">No artifact links available.</span>';
}

function setCompareMode(v) {
  state.compareMode = v || "all";
  updateAndRender();
}

function parseSearchQuery(raw) {
  var out = { terms: [], sev: null, cluster: null, changed: null, flaky: null };
  if (!raw) return out;
  raw.split(/\s+/).forEach(function(tok) {
    if (!tok) return;
    var m = tok.match(/^([^:]+):(.*)$/);
    if (!m) { out.terms.push(tok.toLowerCase()); return; }
    var key = m[1].toLowerCase();
    var val = m[2].toLowerCase();
    if (key === "sev" || key === "severity") out.sev = val.toUpperCase();
    else if (key === "cluster") out.cluster = val;
    else if (key === "changed") out.changed = (val === "true" || val === "1" || val === "yes");
    else if (key === "flaky") out.flaky = (val === "true" || val === "1" || val === "yes");
    else out.terms.push(tok.toLowerCase());
  });
  return out;
}

function displayClusterName(r) {
  var name = (r && r.clusterName ? String(r.clusterName).trim() : "");
  if (name) return name;
  var version = (r && r.clusterVersion ? String(r.clusterVersion).trim().toLowerCase() : "");
  if (version.indexOf("pc.") === 0) return "Prism Central";
  return (r && r.cluster ? String(r.cluster) : "-");
}

function getSelectedClusterAddresses() {
  var selectedAddresses = new Set();
  (AGG_ROWS || []).forEach(function(r) {
    var label = displayClusterName(r);
    if (state.filterClusters.has(label)) {
      selectedAddresses.add((r.cluster || "").trim());
    }
  });
  return selectedAddresses;
}

function getClusterFilteredRows() {
  return (AGG_ROWS || []).filter(function(r) {
    return state.filterClusters.has(displayClusterName(r));
  });
}

function initClusters() {
  const clusters = Array.from(new Set(AGG_ROWS.map(function(r) { 
    return displayClusterName(r);
  }))).sort();
  console.log("Found clusters:", clusters); 
  state.allClusters = clusters;
  state.filterClusters = new Set(clusters);
  updateClusterStatus();
}

function updateClusterStatus() {
  const statusEl = document.getElementById('clusterStatus');
  if (!statusEl) return;
  const count = state.filterClusters.size;
  const total = state.allClusters.length || 0;
  let text, className;
  if (count === 0) {
    text = 'No clusters selected';
    className = 'cluster-status cluster-status-empty';
  } else if (count === total && total > 0) {
    text = total > 1 ? 'All clusters (' + total + ')' : 'All clusters selected';
    className = 'cluster-status';
  } else {
    if (total > 8) {
      text = count + ' of ' + total + ' clusters selected';
    } else {
      const names = Array.from(state.filterClusters).slice(0, 2);
      text = names.join(', ') + (count > 2 ? ' +' + (count - 2) : '') + ' (' + count + '/' + total + ')';
    }
    className = 'cluster-status';
  }
  statusEl.textContent = text;
  statusEl.className = className;
}


function toggleCluster(name) {
  if (state.filterClusters.has(name)) {
    state.filterClusters.delete(name);
  } else {
    state.filterClusters.add(name);
  }
  renderClusterList();
  updateClusterStatus();
  updateAndRender();
}

function selectAllClusters() {
  state.filterClusters = new Set(state.allClusters);
  renderClusterList();
  updateClusterStatus();
  updateAndRender();
}

function clearAllClusters() {
  state.filterClusters.clear();
  renderClusterList();
  updateClusterStatus();
  updateAndRender();
}

function toggleClusterFilter() {
  state.showClusterModal = !state.showClusterModal;
  if (state.showClusterModal) {
    showClusterModal();
  } else {
    hideClusterModal();
  }
}

function showClusterModal() {
  if (document.getElementById('clusterModal')) return;
  state.clusterModalSearch = "";
  state.clusterListVisible = 100;
  const modal = document.createElement('div');
  modal.id = 'clusterModal';
  modal.className = 'cluster-modal';
  modal.innerHTML = [
    '<div class="cluster-modal-content">',
      '<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; flex-wrap: wrap; gap: 8px;">',
        '<h3 style="margin: 0; font-size: 18px;">Select Clusters</h3>',
        '<button type="button" onclick="toggleClusterFilter()" style="background: none; border: 1px solid var(--border); color: var(--text); padding: 6px 12px; border-radius: 6px; cursor: pointer;" aria-label="Close">✕</button>',
      '</div>',
      '<div style="margin-bottom: 12px;"><input type="text" id="clusterModalSearch" placeholder="Filter clusters..." style="width:100%; background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 8px 10px; border-radius: 8px; font-size: 13px;" aria-label="Filter cluster list"></div>',
      '<div class="cluster-list" id="clusterList"></div>',
      '<div id="clusterListMore" style="margin-top: 8px;"></div>',
      '<div class="modal-buttons">',
        '<button type="button" onclick="selectAllClusters()" style="padding: 8px 16px; border-radius: 6px; border: 1px solid var(--border); background: #0a1123; color: var(--text);">Select All</button>',
        '<button type="button" onclick="clearAllClusters()" style="padding: 8px 16px; border-radius: 6px; border: 1px solid var(--border); background: #0a1123; color: var(--text);">Clear All</button>',
        '<button type="button" onclick="toggleClusterFilter()" style="padding: 8px 16px; border-radius: 6px; background: var(--accent); color: white; border: none;">Done</button>',
      '</div>',
    '</div>'
  ].join('');
  document.body.appendChild(modal);
  var searchEl = document.getElementById('clusterModalSearch');
  if (searchEl) {
    searchEl.oninput = function() { state.clusterModalSearch = this.value.trim(); renderClusterList(); };
    searchEl.onkeydown = function(e) { if (e.key === 'Escape') { this.value = ''; state.clusterModalSearch = ''; renderClusterList(); this.focus(); } };
  }
  renderClusterList();
}

function hideClusterModal() {
  const modal = document.getElementById('clusterModal');
  if (modal) modal.remove();
}
function showPerClusterModal() {
  if (typeof CLUSTER_LINKS === 'undefined' || !CLUSTER_LINKS.length) return;
  if (document.getElementById('perClusterModal')) return;
  state.perClusterSearch = "";
  state.perClusterListVisible = 50;
  var modal = document.createElement('div');
  modal.id = 'perClusterModal';
  modal.className = 'cluster-modal';
  modal.innerHTML = [
    '<div class="cluster-modal-content" style="max-width: 560px;">',
      '<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">',
        '<h3 style="margin: 0; font-size: 18px;">Per-cluster reports</h3>',
        '<button type="button" onclick="hidePerClusterModal()" style="background: none; border: 1px solid var(--border); color: var(--text); padding: 6px 12px; border-radius: 6px; cursor: pointer;" aria-label="Close">✕</button>',
      '</div>',
      '<div style="margin-bottom: 12px;"><input type="text" id="perClusterSearch" placeholder="Filter by cluster name or IP..." style="width:100%; background: #0a1123; border: 1px solid var(--border); color: var(--text); padding: 8px 10px; border-radius: 8px; font-size: 13px;" aria-label="Filter per-cluster links"></div>',
      '<div class="cluster-list" id="perClusterList"></div>',
      '<div id="perClusterListMore" style="margin-top: 8px;"></div>',
    '</div>'
  ].join('');
  document.body.appendChild(modal);
  var searchEl = document.getElementById('perClusterSearch');
  if (searchEl) {
    searchEl.oninput = function() { state.perClusterSearch = this.value.trim(); renderPerClusterList(); };
    searchEl.onkeydown = function(e) { if (e.key === 'Escape') { this.value = ''; state.perClusterSearch = ''; renderPerClusterList(); this.focus(); } };
    searchEl.focus();
  }
  state.showPerClusterModal = true;
  renderPerClusterList();
}
function hidePerClusterModal() {
  var modal = document.getElementById('perClusterModal');
  if (modal) modal.remove();
  state.showPerClusterModal = false;
}
function renderPerClusterList() {
  var list = document.getElementById('perClusterList');
  var moreEl = document.getElementById('perClusterListMore');
  if (!list || typeof CLUSTER_LINKS === 'undefined') return;
  var needle = state.perClusterSearch.toLowerCase();
  var filtered = needle ? CLUSTER_LINKS.filter(function(item) { return (item.Cluster || '').toLowerCase().indexOf(needle) !== -1; }) : CLUSTER_LINKS.slice();
  var visible = filtered.slice(0, state.perClusterListVisible);
  var items = visible.map(function(item) {
    var name = escapeHtml(item.Cluster || '');
    var href = escapeHtml(item.HTML || '#');
    return '<a href="' + href + '" target="_blank" rel="noopener" class="cluster-item" style="display:block; text-decoration: none; color: inherit;">' + name + '</a>';
  });
  list.innerHTML = items.join('');
  if (moreEl) {
    var remaining = filtered.length - state.perClusterListVisible;
    if (remaining > 0) {
      moreEl.innerHTML = '<button type="button" class="cluster-load-more" onclick="state.perClusterListVisible += 50; renderPerClusterList();">Show more (' + remaining + ' remaining)</button>';
      moreEl.style.display = 'block';
    } else {
      moreEl.innerHTML = '';
      moreEl.style.display = 'none';
    }
  }
}

function renderClusterList() {
  const list = document.getElementById('clusterList');
  const moreEl = document.getElementById('clusterListMore');
  if (!list) return;
  var needle = state.clusterModalSearch.toLowerCase();
  var filtered = needle ? state.allClusters.filter(function(name) { return name.toLowerCase().indexOf(needle) !== -1; }) : state.allClusters.slice();
  var visible = filtered.slice(0, state.clusterListVisible);
  var items = visible.map(function(name) {
    var active = state.filterClusters.has(name) ? 'active' : '';
    var safeName = escapeHtml(name);
    var safeJs = jsStrEsc(name);
    return '<div class="cluster-item ' + active + '" onclick="toggleCluster(\'' + safeJs + '\')">' + safeName + '</div>';
  });
  list.innerHTML = items.join('');
  if (moreEl) {
    var remaining = filtered.length - state.clusterListVisible;
    if (remaining > 0) {
      moreEl.innerHTML = '<button type="button" class="cluster-load-more" onclick="state.clusterListVisible += 100; renderClusterList();">Show more (' + remaining + ' remaining)</button>';
      moreEl.style.display = 'block';
    } else {
      moreEl.innerHTML = '';
      moreEl.style.display = 'none';
    }
  }
}
	
	function filterBySev(sev) {
	  if (sev === null) {
	    state.filterSev = new Set(["FAIL","WARN","ERR","INFO"]);
	  } else {
	    state.filterSev = new Set([sev]);
	  }
	  document.querySelectorAll('.control input[type="checkbox"]').forEach(function(cb) {
	    var m = (cb.getAttribute('onchange') || '').match(/'([^']+)'/);
	    if (m) cb.checked = state.filterSev.has(m[1]);
	  });
	  updateAndRender();
	}
	function clearFilters() {
	  state.search = "";
	  var sb = document.getElementById("searchBox");
	  if (sb) sb.value = "";
	  state.filterClusters = new Set(state.allClusters);
	  state.filterSev = new Set(["FAIL","WARN","ERR","INFO"]);
	  document.querySelectorAll('.control input[type="checkbox"]').forEach(function(cb) { cb.checked = true; });
	  renderClusterList();
	  updateClusterStatus();
	  updateAndRender();
	  if (state.showClusterModal) { state.showClusterModal = false; hideClusterModal(); }
	}
	function setSev(checked, sev) {
	  if (checked) state.filterSev.add(sev); else state.filterSev.delete(sev);
	  updateAndRender();
	}
	
	function onSearch(inp) {
	  state.search = inp.value.trim();
	  updateAndRender();
	}
	
	let debounceTimer;
	function onSearchDebounced(inp) {
	  clearTimeout(debounceTimer);
	  debounceTimer = setTimeout(() => onSearch(inp), 150);
	}
	
	function sortBy(key) {
	  if (state.sortKey === key) state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
	  else { state.sortKey = key; state.sortDir = "asc"; }
	  updateSortIndicators();
	  updateAndRender();
	}
	function updateSortIndicators() {
	  document.querySelectorAll('.th-sort .sort-arrow').forEach(function(el) {
	    var key = el.id ? el.id.replace('arrow-','') : '';
	    if (key === 'clusterName') key = 'clusterName';
	    if (key === 'cluster') key = 'cluster';
	    if (key === 'severity') key = 'severity';
	    if (key === 'check') key = 'check';
	    el.textContent = state.sortKey === key ? (state.sortDir === 'asc' ? ' ↑' : ' ↓') : '';
	  });
	  var keys = ['clusterName','cluster','severity','check'];
	  keys.forEach(function(key) {
	    var el = document.getElementById('arrow-' + key);
	    if (el) el.textContent = state.sortKey === key ? (state.sortDir === 'asc' ? ' ↑' : ' ↓') : '';
	  });
	}
	
function filterData() {
  const q = parseSearchQuery(state.search);
  return AGG_ROWS.filter(r => {
    if (!state.filterSev.has(r.severity)) return false;
    const clusterId = displayClusterName(r);
    if (!state.filterClusters.has(clusterId)) return false;
    const k = rowKeyFor(r.cluster, r.check);
    const d = DIFF_FLAGS[k] || {};
    const isChanged = !!(d.new_fail || d.resolved_fail || d.severity_changed);
    const isFlaky = !!FLAKY_KEYS[k];

    if (state.compareMode === "changed" && !isChanged) return false;
    if (state.compareMode === "flaky" && !isFlaky) return false;

    if (q.sev && r.severity !== q.sev) return false;
    if (q.cluster && clusterId.toLowerCase().indexOf(q.cluster) === -1) return false;
    if (q.changed !== null && isChanged !== q.changed) return false;
    if (q.flaky !== null && isFlaky !== q.flaky) return false;

    if (!q.terms.length) return true;
    const hay = (clusterId + " " + r.severity + " " + r.check + " " + r.detail).toLowerCase();
    return q.terms.every(function(t){ return hay.indexOf(t) !== -1; });
  });
}


	function sortData(rows) {
	  const k = state.sortKey, dir = state.sortDir;
	  const mul = dir === "asc" ? 1 : -1;
	  rows.sort((a,b) => {
		let av = a[k], bv = b[k];
    if (k === "clusterName") {
      av = displayClusterName(a);
      bv = displayClusterName(b);
    }
		if (k === "severity") { av = sevRank[av] || 99; bv = sevRank[bv] || 99; }
		return (av > bv ? 1 : av < bv ? -1 : 0) * mul;
	  });
	  return rows;
	}
	
function updateCounts(rows) {
  const total = rows.length;
  const cnt = { FAIL:0, WARN:0, ERR:0, INFO:0 };
  rows.forEach(r => { if (cnt[r.severity] !== undefined) cnt[r.severity]++; });

  // Update main summary counts ONLY
  document.getElementById("countTotal").textContent = total;
  document.getElementById("countFail").textContent = cnt.FAIL;
  document.getElementById("countWarn").textContent = cnt.WARN;
  document.getElementById("countErr").textContent  = cnt.ERR;
  document.getElementById("countInfo").textContent = cnt.INFO;

  const pct = {};
  Object.keys(cnt).forEach(k => pct[k] = total ? Math.round(cnt[k]*100/total) : 0);
  document.getElementById("barFail").style.width = pct.FAIL + "%";
  document.getElementById("barWarn").style.width = pct.WARN + "%";
  document.getElementById("barErr").style.width  = pct.ERR  + "%";
  document.getElementById("barInfo").style.width = pct.INFO + "%";

  // const pc = document.getElementById("perCluster"); pc.innerHTML = "";
}

	
	function extractKB(detail) {
	  const text = detail || "";
	  const re = /(https?:\/\/[^\s)]+portal\.nutanix\.com\/kb\/\d+|https?:\/\/[^\s)]+)/i;
	  const m = text.match(re);
	  return m ? m[0] : "";
	}
	function kbLabel(url) {
	  if (!url) return "";
	  const m = url.match(/\/kb\/(\d+)\b/i);
	  return m ? ('KB-' + m[1]) : 'KB';
	}
	
	function escapeHtml(s) {
	  return (s || "").toString()
		.replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;")
		.replaceAll('"',"&quot;").replaceAll("'","&#39;");
	}
	function jsStrEsc(s) {
	  return (s || "").toString()
		.replace(/\\/g, "\\\\").replace(/'/g, "\\'").replace(/\r/g, " ").replace(/\n/g, " ");
	}
	
	function highlight(text, needle) {
	  if (!needle) return escapeHtml(text);
	  const re = new RegExp("(" + needle.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\$&") + ")", "ig");
	  return escapeHtml(text).replace(re, '<span class="highlight">$1</span>');
	}
	
	function formatCheckTitle(s) {
 	 s = s || "";
  	return s.replace(/^detailed information for\s*/i, "").replace(/:$/, "");
	}

	function jsEscape(s) {
	  return (s || "").toString()
		.replaceAll("\\", "\\\\").replaceAll("\n", "\\n").replaceAll("\r", " ")
		.replaceAll("'", "\\'").replaceAll("\"", "\\\"");
	}
	
	async function copyText(text) {
	  try { await navigator.clipboard.writeText(text); }
	  catch {
		const ta = document.createElement("textarea");
		ta.value = text; document.body.appendChild(ta);
		ta.select(); document.execCommand("copy");
		document.body.removeChild(ta);
	  }
	}

	
  function renderTable(rows) {
  const tbody = document.getElementById("tbody");
  tbody.innerHTML = "";
  const needle = state.search;
  const frag = document.createDocumentFragment();
  
  rows.forEach((r, idx) => {
    const tr = document.createElement("tr");
    tr.setAttribute("tabindex", "0");
    tr.dataset.index = idx.toString();
    
    const detailEsc = (r.detail || "").replaceAll("\\n","<br>");
    const nameLine = escapeHtml(displayClusterName(r));
    const verLine = escapeHtml(r.clusterVersion || "");
    const nccLine = escapeHtml(r.nccVersion || "");
    const checkTitle = formatCheckTitle(r.check || "");
    const kb = extractKB(r.detail);
    const kbCell = kb ? ('<a href="' + kb + '" target="_blank" rel="noopener">' + kbLabel(kb) + '</a>') : '';
    const clusterUrl = 'https://' + encodeURIComponent(r.cluster) + ':9440';
    const rowText = (r.cluster + " " + r.severity + " " + r.check + " " + (r.detail || "")).trim();
    const actHTML = '<div class="actions">' +
      '<button onclick="copyText(\'' + jsEscape(rowText) + '\')">Copy row</button>' +
      '<button onclick="copyText(\'' + jsEscape(r.detail || "") + '\')">Copy detail</button>' +
      '</div>';
    
    
    const nameCell = '<td class="col-cname">' +
      '<div class="mono" style="color: var(--text); font-weight: 600;">' + highlight(nameLine, needle) + '</div>' +
      '<div class="cluster-meta mono">' +
      (verLine ? ('<div>Version: ' + highlight(verLine, needle) + '</div>') : '') +
      (nccLine ? ('<div>NCC: ' + highlight(nccLine, needle) + '</div>') : '') +
      '</div></td>';
    
    const clusterCell = '<td class="col-cluster">' +
      '<small class="mono has-tooltip" data-fulltext="' + escapeHtml(r.cluster) + '">' +
      '<a href="' + clusterUrl + '" target="_blank" rel="noopener">' + highlight(r.cluster, needle) + '</a>' +
      '</small></td>';
    
    const titleCell = '<td class="col-title">' +
      '<small class="mono has-tooltip" data-fulltext="' + escapeHtml(checkTitle) + '">' +
      highlight(checkTitle, needle) +
      '</small>' +
      (function() {
        var k = rowKeyFor(r.cluster, r.check);
        var d = DIFF_FLAGS[k] || {};
        var tags = [];
        if (d.new_fail) tags.push('<span class="ux-tag tag-new">+new FAIL</span>');
        if (d.resolved_fail) tags.push('<span class="ux-tag tag-resolved">resolved FAIL</span>');
        if (d.severity_changed) tags.push('<span class="ux-tag tag-changed">severity changed</span>');
        if (FLAKY_KEYS[k]) tags.push('<span class="ux-tag tag-flaky">flaky</span>');
        return tags.length ? ('<div class="row-badges">' + tags.join('') + '</div>') : '';
      })() +
      '</td>';
    
    const detailCell = '<td class="col-detail">' +
      '<div class="detail-full has-tooltip" data-fulltext="' + jsEscape(r.detail || "") + '"' +
      ' style="max-height: 60px; overflow-y: auto;">' +
      highlight(detailEsc, needle) +
      '</div></td>';
    
    tr.innerHTML = nameCell + clusterCell +
      '<td class="col-sev"><span class="severity sev-' + r.severity + '">' + r.severity + '</span></td>' +
      titleCell + '<td class="col-kb">' + kbCell + '</td>' + detailCell +
      '<td class="col-actions">' + actHTML + '</td>';
    
    tr.addEventListener("focus", () => selectRow(tr));
    frag.appendChild(tr);
  });
  
  tbody.appendChild(frag);
  setTimeout(initTooltips, 0);
}


let tooltip = null;
let tooltipTimeout = null;

function showTooltip(event, text) {
  
  if (tooltipTimeout) clearTimeout(tooltipTimeout);
  
  if (!text || text.length < 20) return; 
  
  
  if (tooltip) {
    tooltip.remove();
    tooltip = null;
  }
  
  tooltip = document.createElement('div');
  tooltip.className = 'tooltip';
  tooltip.textContent = text;
  document.body.appendChild(tooltip);
  
  
  const rect = event.target.getBoundingClientRect();
  const tooltipRect = tooltip.getBoundingClientRect();
  
  
  let top = event.clientY - tooltipRect.height - 10;
  let left = event.clientX - tooltipRect.width / 2;
  
  
  if (top < 10) top = event.clientY + 20;
  if (left < 10) left = 10;
  if (left + tooltipRect.width > window.innerWidth - 10) {
    left = window.innerWidth - tooltipRect.width - 10;
  }
  
  tooltip.style.position = 'fixed';
  tooltip.style.left = left + 'px';
  tooltip.style.top = top + 'px';
}

function hideTooltip() {
  if (tooltipTimeout) clearTimeout(tooltipTimeout);
  tooltipTimeout = setTimeout(() => {
    if (tooltip) {
      tooltip.remove();
      tooltip = null;
    }
  }, 50); 
}

function initTooltips() {
  
  document.querySelectorAll('.has-tooltip').forEach(el => {
    el.removeEventListener('mouseenter', el._ttEnter);
    el.removeEventListener('mouseleave', el._ttLeave);
    el.removeEventListener('mousemove', el._ttMove);
  });
  
  
  document.querySelectorAll('.has-tooltip').forEach(el => {
    const text = el.dataset.fulltext || '';
    if (text.length < 20) return; 
    
    el._ttEnter = (e) => showTooltip(e, text);
    el._ttLeave = hideTooltip;
    el._ttMove = (e) => {
      if (tooltip && text.length >= 20) {
        showTooltip(e, text); 
      }
    };
    
    el.addEventListener('mouseenter', el._ttEnter);
    el.addEventListener('mouseleave', el._ttLeave);
    el.addEventListener('mousemove', el._ttMove);
  });
}
	function selectRow(tr) {
	  const tbody = document.getElementById("tbody");
	  Array.from(tbody.querySelectorAll("tr.selected")).forEach(x => x.classList.remove("selected"));
	  tr.classList.add("selected");
	  selIndex = parseInt(tr.dataset.index || "-1", 10);
	}
	
	function focusRow(i) {
	  const rows = document.querySelectorAll("#tbody tr");
	  if (!rows.length) return;
	  if (i < 0) i = 0;
	  if (i >= rows.length) i = rows.length - 1;
	  selIndex = i;
	  const tr = rows[i];
	  tr.focus({preventScroll:false});
	  selectRow(tr);
	  tr.scrollIntoView({block:"nearest", inline:"nearest"});
	}
	
	function onKey(e) {
	  const k = e.key;
	  if (k === "/") {
		e.preventDefault();
		const sb = document.getElementById("searchBox");
		if (sb) { sb.focus(); sb.select(); }
		return;
	  }
	  if (k === "Escape") {
		if (state.showPerClusterModal) {
		  state.showPerClusterModal = false;
		  hidePerClusterModal();
		  return;
		}
		if (state.showClusterModal) {
		  state.showClusterModal = false;
		  hideClusterModal();
		  return;
		}
		if (state.search) {
		  state.search = ""; var sb = document.getElementById("searchBox"); if (sb) sb.value = "";
		  updateAndRender();
		}
		return;
	  }
	  if (k === "ArrowDown") { e.preventDefault(); focusRow(selIndex + 1); return; }
	  if (k === "ArrowUp")   { e.preventDefault(); focusRow(selIndex - 1); return; }
	}
	
	function updateAndRender() {
	  let rows = filterData();
	  var clusterScopedRows = getClusterFilteredRows();
	  renderInsights(clusterScopedRows);
	  
	  const total = rows.length;
	  const cnt = { FAIL:0, WARN:0, ERR:0, INFO:0 };
	  rows.forEach(r => { if (cnt[r.severity] !== undefined) cnt[r.severity]++; });
	  document.getElementById("countTotal").textContent = total;
	  document.getElementById("countFail").textContent = cnt.FAIL;
	  document.getElementById("countWarn").textContent = cnt.WARN;
	  document.getElementById("countErr").textContent  = cnt.ERR;
	  document.getElementById("countInfo").textContent = cnt.INFO;
	  const pct = {};
	  Object.keys(cnt).forEach(k => pct[k] = total ? Math.round(cnt[k]*100/total) : 0);
	  document.getElementById("barFail").style.width = pct.FAIL + "%";
	  document.getElementById("barWarn").style.width = pct.WARN + "%";
	  document.getElementById("barErr").style.width  = pct.ERR  + "%";
	  document.getElementById("barInfo").style.width = pct.INFO + "%";
	
	  
	  updateCounts(rows);
	  renderHealthWidget();
	  rows = sortData(rows.slice());
	  state.filteredRows = rows;
	  var totalRows = rows.length;
	  var totalPages = state.pageSize > 0 ? Math.max(1, Math.ceil(totalRows / state.pageSize)) : 1;
	  if (state.currentPage >= totalPages) state.currentPage = Math.max(0, totalPages - 1);
	  var pageStart = state.currentPage * state.pageSize;
	  var pageEnd = Math.min(pageStart + state.pageSize, totalRows);
	  var pageRows = totalRows > 0 ? rows.slice(pageStart, pageEnd) : [];
	  var infoEl = document.getElementById("tableInfo");
	  var emptyEl = document.getElementById("emptyState");
	  var tableEl = document.getElementById("main-table");
	  var paginationEl = document.getElementById("pagination");
	  if (infoEl) {
	    if (totalRows === 0) infoEl.textContent = "Showing 0 rows";
	    else if (totalRows <= state.pageSize) infoEl.textContent = "Showing " + totalRows + " row" + (totalRows === 1 ? "" : "s");
	    else infoEl.textContent = "Showing " + (pageStart + 1) + "–" + pageEnd + " of " + totalRows + " rows";
	  }
	  if (emptyEl && tableEl) {
	    if (totalRows === 0) { tableEl.style.display = "none"; emptyEl.style.display = "block"; if (paginationEl) paginationEl.style.display = "none"; }
	    else { tableEl.style.display = ""; emptyEl.style.display = "none"; if (paginationEl) paginationEl.style.display = "flex"; }
	  }
	  if (paginationEl && totalRows > 0) {
	    var pageButtons = buildPageButtons(totalPages, state.currentPage);
	    paginationEl.innerHTML =
	      '<div class="pagination-left">' +
	      '<button type="button" onclick="goToPage(0)" ' + (state.currentPage === 0 ? 'disabled' : '') + ' aria-label="First page">First</button>' +
	      '<button type="button" onclick="goToPage(' + (state.currentPage - 1) + ')" ' + (state.currentPage === 0 ? 'disabled' : '') + ' aria-label="Previous page">Prev</button>' +
	      '<span class="pagination-pages">' + pageButtons + '</span>' +
	      '<button type="button" onclick="goToPage(' + (state.currentPage + 1) + ')" ' + (state.currentPage >= totalPages - 1 ? 'disabled' : '') + ' aria-label="Next page">Next</button>' +
	      '<button type="button" onclick="goToPage(' + (totalPages - 1) + ')" ' + (state.currentPage >= totalPages - 1 ? 'disabled' : '') + ' aria-label="Last page">Last</button>' +
	      '<span class="pagination-info">Page ' + (state.currentPage + 1) + ' of ' + totalPages + '</span>' +
	      '</div>' +
	      '<div class="pagination-right">' +
	      '<label class="pagination-jump"><span>Go</span><input id="pageJumpInput" type="number" min="1" max="' + totalPages + '" value="' + (state.currentPage + 1) + '" onkeydown="if(event.key===&#39;Enter&#39;){goToPageInput(this.value,' + totalPages + ')}" aria-label="Go to page number"><button type="button" onclick="goToPageInput(document.getElementById(&#39;pageJumpInput&#39;).value,' + totalPages + ')">Go</button></label>' +
	      '<label class="pagination-size"><span>Rows</span><select id="pageSizeSelect" onchange="setPageSize(parseInt(this.value,10))">' +
	      (state.pageSize === 50 ? '<option value="50" selected>50</option>' : '<option value="50">50</option>') +
	      (state.pageSize === 100 ? '<option value="100" selected>100</option>' : '<option value="100">100</option>') +
	      (state.pageSize === 200 ? '<option value="200" selected>200</option>' : '<option value="200">200</option>') +
	      (state.pageSize === 500 ? '<option value="500" selected>500</option>' : '<option value="500">500</option>') +
	      '</select></label>' +
	      '</div>';
	  }
	  updateSortIndicators();
	  var fc = document.getElementById("footerClusterCount");
	  if (fc) fc.textContent = "Clusters: " + state.filterClusters.size + "/" + state.allClusters.length;
	  renderTable(pageRows);
	}
	function goToPage(p) {
	  state.currentPage = Math.max(0, Math.min(p, Math.ceil(state.filteredRows.length / state.pageSize) - 1));
	  updateAndRender();
	}
	function setPageSize(n) {
	  state.pageSize = n;
	  state.currentPage = 0;
	  updateAndRender();
	}
	function buildPageButtons(totalPages, currentPage) {
	  if (totalPages <= 1) return '<button type="button" class="pagination-page active" aria-current="page">1</button>';
	  var pages = [0];
	  var start = Math.max(1, currentPage - 1);
	  var end = Math.min(totalPages - 2, currentPage + 1);
	  if (start > 1) pages.push("...");
	  for (var i = start; i <= end; i++) pages.push(i);
	  if (end < totalPages - 2) pages.push("...");
	  pages.push(totalPages - 1);
	  var html = "";
	  for (var p = 0; p < pages.length; p++) {
	    var item = pages[p];
	    if (item === "...") {
	      html += '<span class="pagination-ellipsis" aria-hidden="true">...</span>';
	    } else {
	      html += '<button type="button" class="pagination-page ' + (item === currentPage ? 'active' : '') + '" onclick="goToPage(' + item + ')" ' + (item === currentPage ? 'aria-current="page"' : '') + ' aria-label="Page ' + (item + 1) + '">' + (item + 1) + '</button>';
	    }
	  }
	  return html;
	}
	function goToPageInput(raw, totalPages) {
	  var n = parseInt(raw, 10);
	  if (isNaN(n)) return;
	  if (n < 1) n = 1;
	  if (n > totalPages) n = totalPages;
	  goToPage(n - 1);
	}
	
	function downloadCSV() {
		const rows = filterData();
		const headers = ["Cluster","Severity","NCC Alert Title","Detail"];
		const lines = [headers.join(",")];
		rows.forEach(r => {
		  const title = formatCheckTitle(r.check || "");
		  const row = [r.cluster, r.severity, title, r.detail || ""].map(v => {
		    const s = (v ?? "").toString().replaceAll('"','""').replaceAll("\r"," ").replaceAll("\n","\\n");
		    return '"' + s + '"';
		  }).join(",");
		  lines.push(row);
		});
	  const blob = new Blob([lines.join("\n")], {type: "text/csv;charset=utf-8;"});
	  triggerDownload(blob, "aggregated_filtered.csv");
	}
	
	function downloadJSON() {
	  const rows = filterData();
	  const blob = new Blob([JSON.stringify(rows, null, 2)], {type: "application/json;charset=utf-8;"});
	  triggerDownload(blob, "aggregated_filtered.json");
	}
	
	function triggerDownload(blob, name) {
	  const a = document.createElement("a");
	  a.href = URL.createObjectURL(blob);
	  a.download = name;
	  document.body.appendChild(a);
	  a.click();
	  document.body.removeChild(a);
	}
	</script>
	</head>
	<body onload="init()">
	<a href="#main-table" class="skip-link">Skip to table</a>
	<div class="container">
	  <div class="header">
		<div class="title">
		  <h1>NCC Aggregated Report</h1>
		  <div class="sub">Generated at {{.GeneratedAt}}{{if .Clusters}} · {{len .Clusters}} cluster{{if eq (len .Clusters) 1}}{{else}}s{{end}}{{end}}</div>
		</div>
		{{if .Clusters}}
		<div class="header-actions">
		  <button type="button" class="per-cluster-btn" onclick="showPerClusterModal()" aria-label="Open per-cluster report links">Per-cluster reports ({{len .Clusters}})</button>
		</div>
		{{end}}
	  </div>
	
	  <div class="controls">
		<div class="control">
		  <label for="searchBox">Search</label>
		  <input id="searchBox" type="text" placeholder="Type to filter... (tokens: sev:FAIL cluster:10.1 changed:true flaky:true)" oninput="onSearchDebounced(this)" aria-label="Filter rows by search text or tokens" />
		</div>
		<div class="control">
		  <label>Severity</label>
		  <label><input type="checkbox" checked onchange="setSev(this.checked,'FAIL')"> <span style="color: var(--fail);">FAIL</span></label>
		  <label><input type="checkbox" checked onchange="setSev(this.checked,'WARN')"> <span style="color: var(--warn);">WARN</span></label>
		  <label><input type="checkbox" checked onchange="setSev(this.checked,'ERR')"> <span style="color: var(--err);">ERR</span></label>
		  <label><input type="checkbox" checked onchange="setSev(this.checked,'INFO')"> <span style="color: var(--info);">INFO</span></label>
		</div>
<div class="control">
  <label for="clusterStatus">Clusters</label>
  <div class="cluster-status-wrapper">
    <div id="clusterStatus" class="cluster-status" role="button" tabindex="0" aria-label="Select clusters to filter" aria-haspopup="dialog">All clusters selected</div>
  </div>
</div>
		<div class="control">
		  <label for="compareMode">Compare</label>
		  <select id="compareMode" onchange="setCompareMode(this.value)" aria-label="Comparison mode">
		    <option value="all">All rows</option>
		    <option value="changed">Changed only</option>
		    <option value="flaky">Flaky only</option>
		  </select>
</div>
		<div class="control">
		  <button type="button" onclick="downloadCSV()" aria-label="Export filtered rows as CSV">Export CSV</button>
		  <button type="button" onclick="downloadJSON()" aria-label="Export filtered rows as JSON">Export JSON</button>
		</div>
		<div class="control" style="min-width: 280px;">
		  <label>Artifacts</label>
		  <div class="per-cluster-links" id="artifactLinks" aria-label="Artifact quick links"></div>
		</div>
	  </div>
	
	  <div class="summary">
		<div class="sum-item clickable" id="sumTotal" role="button" tabindex="0" onclick="filterBySev(null)" onkeydown="if(event.key==='Enter'||event.key===' ') { event.preventDefault(); filterBySev(null); }" aria-label="Show all severities">
		  <div class="label">Total</div>
		  <div class="count" id="countTotal">0</div>
		</div>
		<div class="sum-item clickable" id="sumFail" role="button" tabindex="0" onclick="filterBySev('FAIL')" onkeydown="if(event.key==='Enter'||event.key===' ') { event.preventDefault(); filterBySev('FAIL'); }" aria-label="Show only FAIL">
		  <div class="label">FAIL</div>
		  <div class="count" id="countFail">0</div>
		  <div class="progress fail"><span id="barFail" style="width:0%"></span></div>
		</div>
		<div class="sum-item clickable" id="sumWarn" role="button" tabindex="0" onclick="filterBySev('WARN')" onkeydown="if(event.key==='Enter'||event.key===' ') { event.preventDefault(); filterBySev('WARN'); }" aria-label="Show only WARN">
		  <div class="label">WARN</div>
		  <div class="count" id="countWarn">0</div>
		  <div class="progress warn"><span id="barWarn" style="width:0%"></span></div>
		</div>
		<div class="sum-item clickable" id="sumErr" role="button" tabindex="0" onclick="filterBySev('ERR')" onkeydown="if(event.key==='Enter'||event.key===' ') { event.preventDefault(); filterBySev('ERR'); }" aria-label="Show only ERR">
		  <div class="label">ERR</div>
		  <div class="count" id="countErr">0</div>
		  <div class="progress err"><span id="barErr" style="width:0%"></span></div>
		</div>
		<div class="sum-item clickable" id="sumInfo" role="button" tabindex="0" onclick="filterBySev('INFO')" onkeydown="if(event.key==='Enter'||event.key===' ') { event.preventDefault(); filterBySev('INFO'); }" aria-label="Show only INFO">
		  <div class="label">INFO</div>
		  <div class="count" id="countInfo">0</div>
		  <div class="progress info"><span id="barInfo" style="width:0%"></span></div>
		</div>
	  </div>
	
	  <div class="insights">
		<div class="sum-item">
		  <div class="label">Policy Gates</div>
		  <div class="count" id="policyStatus">-</div>
		  <div class="meta" id="policyMeta"></div>
		</div>
		<div class="sum-item">
		  <div class="label">Changed checks</div>
		  <div class="count" id="changedChecksCount">0</div>
		  <div class="meta">Rows with diff markers (+new FAIL, resolved FAIL, or severity change).</div>
		</div>
		<div class="sum-item">
		  <div class="label">Flaky checks</div>
		  <div class="count" id="flakyChecksCount">0</div>
		  <div class="meta">Rows flagged as severity-oscillating in flaky analysis.</div>
		</div>
	  </div>

	  <div class="card" style="margin-bottom: 12px;">
		<div class="label">Cluster health (score + trend)</div>
		<div class="meta">Sparkline is built from recent run-history snapshots when available.</div>
		<div id="healthWidget" style="margin-top:8px;"></div>
	  </div>

	  <div class="table-info" id="tableInfo" aria-live="polite">Showing 0 rows</div>
	  <div id="pagination" class="pagination" style="display:none;"></div>
	  <div class="card">
		<div class="scroll">
		  <table id="main-table" aria-label="NCC aggregated findings table">
			<thead>
			  <tr>
			  	<th class="col-cname th-sort" data-sort="clusterName" onclick="sortBy('clusterName')">Cluster Name <span class="sort-arrow" id="arrow-clusterName"></span></th>
				<th class="col-cluster th-sort" data-sort="cluster" onclick="sortBy('cluster')">Cluster <span class="sort-arrow" id="arrow-cluster"></span></th>
				<th class="col-sev th-sort" data-sort="severity" onclick="sortBy('severity')">Severity <span class="sort-arrow" id="arrow-severity"></span></th>
				<th class="col-title th-sort" data-sort="check" onclick="sortBy('check')">NCC Alert Title <span class="sort-arrow" id="arrow-check"></span></th>
				<th class="col-kb">KB</th>
				<th class="col-detail">Detail</th>
				<th class="col-actions">Actions</th>
			  </tr>
			</thead>
			<tbody id="tbody"></tbody>
		  </table>
		  <div id="emptyState" class="empty-state" style="display:none;">
		    <p>No rows match your filters.</p>
		    <button type="button" onclick="clearFilters()">Clear filters</button>
		  </div>
		</div>
	  </div>
	
     <footer class="report-footer">
    <strong>Keyboard:</strong> / focus search · ↑/↓ move row · Esc clear search or close modal. <span id="footerClusterCount"></span><br>
    <strong>Search tokens:</strong> <code>sev:FAIL</code> <code>cluster:10.1.1.5</code> <code>changed:true</code> <code>flaky:true</code>
    <div class="report-meta-line" id="reportMetaFooter" aria-live="polite"></div>
</footer>
	</div>
	</body>
	</html>`

	// Build data for template with embedded JSON
	type tmplRow struct {
		Cluster        string `json:"cluster"`
		Severity       string `json:"severity"`
		Check          string `json:"check"`
		Detail         string `json:"detail"`
		ClusterName    string `json:"clusterName"`
		ClusterVersion string `json:"clusterVersion"`
		NCCVersion     string `json:"nccVersion"`
	}

	aggRows := make([]tmplRow, 0, len(rows))
	for _, r := range rows {
		aggRows = append(aggRows, tmplRow(r))
	}

	const maxEmbeddedAggRows = 20000
	aggDataURL := ""
	jsonBytes, err := json.Marshal(aggRows)
	if err != nil {
		return fmt.Errorf("marshal agg json: %w", err)
	}
	if len(aggRows) > maxEmbeddedAggRows {
		sidecarPath := filepath.Join(outDir, "aggregated-data.json")
		if err := fs.WriteFile(sidecarPath, jsonBytes, 0644); err != nil {
			return fmt.Errorf("write aggregated data sidecar: %w", err)
		}
		aggDataURL = filepath.Base(sidecarPath)
		jsonBytes = []byte("[]")
	}

	clusterLinksJSON, _ := json.Marshal(perCluster)
	if clusterLinksJSON == nil {
		clusterLinksJSON = []byte("[]")
	}

	type diffFlags struct {
		NewFail         bool `json:"new_fail"`
		ResolvedFail    bool `json:"resolved_fail"`
		SeverityChanged bool `json:"severity_changed"`
	}
	rowKey := func(cluster, check string) string {
		return strings.ToLower(strings.TrimSpace(cluster)) + "||" + strings.ToLower(strings.TrimSpace(check))
	}
	diffFlagMap := map[string]diffFlags{}
	diffPath := filepath.Join(outDir, "drilldown-diff.json")
	if b, err := os.ReadFile(diffPath); err == nil {
		var d DrillDownDiffJSON
		if err := json.Unmarshal(b, &d); err == nil {
			for _, c := range d.Clusters {
				for _, name := range c.NewFailures {
					k := rowKey(c.Address, name)
					v := diffFlagMap[k]
					v.NewFail = true
					diffFlagMap[k] = v
				}
				for _, name := range c.ResolvedFailures {
					k := rowKey(c.Address, name)
					v := diffFlagMap[k]
					v.ResolvedFail = true
					diffFlagMap[k] = v
				}
				for _, ch := range c.SeverityChanges {
					k := rowKey(c.Address, ch.CheckName)
					v := diffFlagMap[k]
					v.SeverityChanged = true
					diffFlagMap[k] = v
				}
			}
		}
	}
	diffFlagsJSON, _ := json.Marshal(diffFlagMap)
	if diffFlagsJSON == nil {
		diffFlagsJSON = []byte("{}")
	}

	flakyKeys := map[string]bool{}
	flakyPath := filepath.Join(outDir, "flaky-checks.json")
	if b, err := os.ReadFile(flakyPath); err == nil {
		var f FlakyChecksJSON
		if err := json.Unmarshal(b, &f); err == nil {
			for _, c := range f.Checks {
				flakyKeys[rowKey(c.Cluster, c.CheckName)] = true
			}
		}
	}
	flakyKeysJSON, _ := json.Marshal(flakyKeys)
	if flakyKeysJSON == nil {
		flakyKeysJSON = []byte("{}")
	}

	policyViolations := []string{}
	policyPath := filepath.Join(outDir, "policy-gates.txt")
	if b, err := os.ReadFile(policyPath); err == nil {
		for _, ln := range strings.Split(string(b), "\n") {
			ln = strings.TrimSpace(ln)
			if ln != "" {
				policyViolations = append(policyViolations, ln)
			}
		}
	}
	policyViolationsJSON, _ := json.Marshal(policyViolations)
	if policyViolationsJSON == nil {
		policyViolationsJSON = []byte("[]")
	}

	var runSummary RunSummaryJSON
	if b, err := os.ReadFile(filepath.Join(outDir, "run-summary.json")); err == nil {
		_ = json.Unmarshal(b, &runSummary)
	}
	runSummaryJSON, _ := json.Marshal(runSummary)
	if runSummaryJSON == nil {
		runSummaryJSON = []byte("{}")
	}

	historyDir := strings.TrimSpace(cfg.RunHistoryDir)
	if historyDir == "" {
		historyDir = filepath.Join(outDir, "runs")
	}
	healthTrends := map[string][]int{}
	if dirs, err := listHistoryRunDirs(historyDir); err == nil && len(dirs) > 0 {
		limit := 20
		if len(dirs) > limit {
			dirs = dirs[:limit]
		}
		// oldest -> newest
		for i, j := 0, len(dirs)-1; i < j; i, j = i+1, j-1 {
			dirs[i], dirs[j] = dirs[j], dirs[i]
		}
		for _, d := range dirs {
			p := filepath.Join(historyDir, d, "run-summary.json")
			b, err := os.ReadFile(p)
			if err != nil {
				continue
			}
			var s RunSummaryJSON
			if err := json.Unmarshal(b, &s); err != nil {
				continue
			}
			for _, c := range s.Clusters {
				key := strings.ToLower(strings.TrimSpace(c.Address))
				if key == "" {
					continue
				}
				healthTrends[key] = append(healthTrends[key], c.HealthScore)
			}
		}
	}
	for _, c := range runSummary.Clusters {
		key := strings.ToLower(strings.TrimSpace(c.Address))
		if key == "" {
			continue
		}
		healthTrends[key] = append(healthTrends[key], c.HealthScore)
	}
	healthTrendsJSON, _ := json.Marshal(healthTrends)
	if healthTrendsJSON == nil {
		healthTrendsJSON = []byte("{}")
	}

	artifactLinks := map[string]string{}
	for _, name := range []string{"run-summary.json", "slo-dashboard.json", "drilldown-diff.json", "flaky-checks.json", "checks-snapshot.json", "regression-summary.json"} {
		if _, err := os.Stat(filepath.Join(outDir, name)); err == nil {
			artifactLinks[name] = name
		}
	}
	artifactLinksJSON, _ := json.Marshal(artifactLinks)
	if artifactLinksJSON == nil {
		artifactLinksJSON = []byte("{}")
	}

	reportMeta := map[string]string{
		"version":             Version,
		"build_date":          BuildDate,
		"stream":              Stream,
		"quiet_hours":         strings.TrimSpace(cfg.QuietHours),
		"maintenance_windows": strings.Join(cfg.MaintenanceWindows, ","),
	}
	if hn, err := os.Hostname(); err == nil && strings.TrimSpace(hn) != "" {
		reportMeta["hostname"] = hn
	}
	schedulerSource := strings.TrimSpace(os.Getenv("NCC_SCHEDULER_SOURCE"))
	if schedulerSource == "" {
		schedulerSource = "manual"
	}
	reportMeta["scheduler_source"] = schedulerSource
	if aggDataURL != "" {
		reportMeta["aggregated_data_sidecar"] = aggDataURL
		reportMeta["aggregated_rows"] = strconv.Itoa(len(aggRows))
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" && s.Value != "" {
				reportMeta["git_revision"] = s.Value
				break
			}
		}
	}
	reportMetaJSON, _ := json.Marshal(reportMeta)
	if reportMetaJSON == nil {
		reportMetaJSON = []byte("{}")
	}

	data := struct {
		JSON                 template.JS
		AggDataURL           string
		ClusterLinksJSON     template.JS
		DiffFlagsJSON        template.JS
		FlakyKeysJSON        template.JS
		PolicyViolationsJSON template.JS
		RunSummaryJSON       template.JS
		HealthTrendsJSON     template.JS
		ArtifactLinksJSON    template.JS
		ReportMetaJSON       template.JS
		Clusters             []struct{ Cluster, HTML, CSV string }
		GeneratedAt          string
		ClusterName          string
		ClusterVersion       string
		NCCVersion           string
	}{
		JSON:                 template.JS(jsonBytes),
		AggDataURL:           aggDataURL,
		ClusterLinksJSON:     template.JS(clusterLinksJSON),
		DiffFlagsJSON:        template.JS(diffFlagsJSON),
		FlakyKeysJSON:        template.JS(flakyKeysJSON),
		PolicyViolationsJSON: template.JS(policyViolationsJSON),
		RunSummaryJSON:       template.JS(runSummaryJSON),
		HealthTrendsJSON:     template.JS(healthTrendsJSON),
		ArtifactLinksJSON:    template.JS(artifactLinksJSON),
		ReportMetaJSON:       template.JS(reportMetaJSON),
		Clusters:             perCluster,
		GeneratedAt:          time.Now().UTC().Format(time.RFC3339),
	}

	f, err := fs.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()
	t := template.Must(template.New("index").Parse(tmpl))
	if err := t.Execute(f, data); err != nil {
		return fmt.Errorf("template execute %s: %w", path, err)
	}
	log.Info().Str("file", abs).Int("rows", len(rows)).Int("clusters", len(perCluster)).Msg("aggregated HTML generated")
	return nil
}
