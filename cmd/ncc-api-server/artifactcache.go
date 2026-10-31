package main

import (
	"container/list"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Parsed-artifact cache for the report endpoints.
//
// A run writes its report files once; the UI then reads them on every page
// load and poll. At fleet scale (7,000 clusters ≈ 200k findings) re-reading
// and re-parsing them per request cost ~30 s per /report/data call and ~1.5 s
// per alerts call. Entries are keyed by path and invalidated by the file's
// size + modification time, so a new run (or another replica writing shared
// storage) is picked up on the next request without any explicit flush.
//
// Values handed out are SHARED: callers must treat them as read-only (copy
// before changing anything; deepFilterClusters copies when it filters).

type fileStamp struct {
	size int64
	mod  int64
}

func statStamp(path string) (fileStamp, bool) {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return fileStamp{}, false
	}
	return fileStamp{size: info.Size(), mod: info.ModTime().UnixNano()}, true
}

type artifactEntry struct {
	key    string
	mu     sync.Mutex // serializes loading of this key (one parse per change)
	stamp  fileStamp
	loaded bool
	value  interface{}
	weight int64
	// expires bounds staleness for values derived from many files (see
	// derived); zero means "valid while the stamp matches".
	expires time.Time
	elem    *list.Element
}

type artifactCache struct {
	mu      sync.Mutex
	entries map[string]*artifactEntry
	lru     *list.List // front = most recently used
	weight  int64
	budget  int64
}

// reportArtifacts holds parsed report files. The budget is in bytes of source
// file; parsed values are a few times larger, so 768 MiB of files is the
// order of 1–2 GiB of heap at the very top end — comfortably more than one
// 7,000-cluster run plus a couple of run-history snapshots.
var reportArtifacts = newArtifactCache(768 << 20)

func newArtifactCache(budget int64) *artifactCache {
	return &artifactCache{entries: map[string]*artifactEntry{}, lru: list.New(), budget: budget}
}

func (c *artifactCache) entry(key string) *artifactEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		e = &artifactEntry{key: key}
		e.elem = c.lru.PushFront(e)
		c.entries[key] = e
	} else {
		c.lru.MoveToFront(e.elem)
	}
	return e
}

// setWeight records a (re)loaded entry's size and evicts least-recently-used
// entries until the cache fits its budget. The entry being set is never
// evicted by its own load.
func (c *artifactCache) setWeight(e *artifactEntry, w int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.weight += w - e.weight
	e.weight = w
	for c.weight > c.budget && c.lru.Len() > 1 {
		back := c.lru.Back()
		victim := back.Value.(*artifactEntry)
		if victim == e {
			break
		}
		c.lru.Remove(back)
		delete(c.entries, victim.key)
		c.weight -= victim.weight
	}
}

func (c *artifactCache) drop(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[key]; ok {
		c.lru.Remove(e.elem)
		delete(c.entries, key)
		c.weight -= e.weight
	}
}

// file returns parse(path's bytes), parsing again only when the file changed.
// ok is false when the file is missing or parse rejects it.
func (c *artifactCache) file(key, path string, parse func([]byte) (interface{}, bool)) (interface{}, bool) {
	stamp, exists := statStamp(path)
	if !exists {
		c.drop(key)
		return nil, false
	}
	e := c.entry(key)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.loaded && e.stamp == stamp {
		return e.value, e.value != nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	// Stamp again after reading: a writer that finished in between gets
	// picked up by the next request rather than cached under the old stamp.
	if after, ok := statStamp(path); ok {
		stamp = after
	}
	v, ok := parse(b)
	if !ok {
		v = nil
	}
	e.stamp, e.value, e.loaded = stamp, v, true
	c.setWeight(e, int64(len(b)))
	return v, ok
}

// derived caches compute() under key until any of the source files changes
// or ttl passes — for values built from a directory of files.
func (c *artifactCache) derived(key string, sources []string, ttl time.Duration, weight int64, compute func() interface{}) interface{} {
	var sig fileStamp
	for _, p := range sources {
		// Directories count too: adding or removing a file changes their time.
		if info, err := os.Stat(p); err == nil {
			sig.size = sig.size*31 + info.Size()
			sig.mod = sig.mod*31 + info.ModTime().UnixNano()
		}
	}
	e := c.entry(key)
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	if e.loaded && e.stamp == sig && now.Before(e.expires) {
		return e.value
	}
	v := compute()
	e.stamp, e.value, e.loaded, e.expires = sig, v, true, now.Add(ttl)
	c.setWeight(e, weight)
	return v
}

func parseJSONValue(b []byte) (interface{}, bool) {
	var out interface{}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, false
	}
	return out, true
}

// readJSONArtifactCached is readJSONArtifact through the cache. The result is
// shared — do not modify it.
func readJSONArtifactCached(path string, fallback interface{}) interface{} {
	if v, ok := reportArtifacts.file("json:"+path, path, parseJSONValue); ok {
		return v
	}
	return fallback
}

// readInlineJSONVarCached is readInlineJSONVar through the cache. The result is
// shared — do not modify it.
func readInlineJSONVarCached(path, varName string, fallback interface{}) interface{} {
	v, ok := reportArtifacts.file("inline:"+varName+":"+path, path, func(b []byte) (interface{}, bool) {
		return parseInlineJSONVar(b, varName)
	})
	if ok {
		return v
	}
	return fallback
}

// readAggRowsCached is readAggRows through the cache. The result is shared —
// do not modify it.
func readAggRowsCached(outDir string) []interface{} {
	if rows, _ := readInlineJSONVarCached(filepath.Join(outDir, "index.html"), "AGG", nil).([]interface{}); len(rows) > 0 {
		return rows
	}
	if side, ok := readJSONArtifactCached(filepath.Join(outDir, "aggregated-data.json"), nil).([]interface{}); ok {
		return side
	}
	return []interface{}{}
}

// readRunSummaryCached returns run-summary.json with the read-side healing
// (error-class reclassification, path scrubbing) applied once at load. The
// result is shared — do not modify it.
func readRunSummaryCached(outDir string) interface{} {
	path := filepath.Join(outDir, "run-summary.json")
	v, ok := reportArtifacts.file("runsummary:"+path, path, func(b []byte) (interface{}, bool) {
		out, ok := parseJSONValue(b)
		if !ok {
			return nil, false
		}
		if m, isMap := out.(map[string]interface{}); isMap {
			reclassifyRunSummaryInPlace(m)
			scrubSummaryPaths(m)
		}
		return out, true
	})
	if ok {
		return v
	}
	return map[string]interface{}{}
}

// slimChecksSnapshotCached returns checks-snapshot.json without the per-check
// lists. When the aggregated rows are present the UI uses the snapshot only to
// map cluster addresses to names; the full per-check lists are ~90% of its
// size (19 MB at 7,000 clusters). The result is shared — do not modify it.
func slimChecksSnapshotCached(outDir string) interface{} {
	path := filepath.Join(outDir, "checks-snapshot.json")
	v, ok := reportArtifacts.file("slimchecks:"+path, path, func(b []byte) (interface{}, bool) {
		out, ok := parseJSONValue(b)
		if !ok {
			return nil, false
		}
		m, isMap := out.(map[string]interface{})
		if !isMap {
			return out, true
		}
		slim := make(map[string]interface{}, len(m))
		for k, val := range m {
			slim[k] = val
		}
		if clusters, isList := m["clusters"].([]interface{}); isList {
			trimmed := make([]interface{}, 0, len(clusters))
			for _, raw := range clusters {
				c, isObj := raw.(map[string]interface{})
				if !isObj {
					trimmed = append(trimmed, raw)
					continue
				}
				cp := make(map[string]interface{}, len(c))
				for k, val := range c {
					if k == "checks" {
						continue
					}
					cp[k] = val
				}
				trimmed = append(trimmed, cp)
			}
			slim["clusters"] = trimmed
		}
		return slim, true
	})
	if ok {
		return v
	}
	return map[string]interface{}{}
}

// reportStampSources are the files whose change marks a new report in outDir.
func reportStampSources(outDir string) []string {
	return []string{
		filepath.Join(outDir, "index.html"),
		filepath.Join(outDir, "run-summary.json"),
		filepath.Join(outDir, "aggregated-data.json"),
	}
}

// warmReportCache parses the current report in the background (from the
// alert-sync loop) so the first page load after a run does not pay for it.
// Each step is a no-op when its files are unchanged.
func (s *apiServer) warmReportCache() {
	outDir := s.selectBestReportOutDir()
	_ = readRunSummaryCached(outDir)
	if len(readAggRowsCached(outDir)) > 0 {
		_ = slimChecksSnapshotCached(outDir)
	}
	_ = loadReportMeta(outDir)
	flags := s.loadFeatureFlags()
	_ = nccLogsForReport(s, flags, outDir)
	_ = nccSummaryForReport(s, flags, outDir)
	_ = nccClusterSummaryForReport(s, flags, clusterAccess{unrestricted: true}, outDir)
}

// cachedLogDirValue caches a value computed from every raw NCC log in logDir.
// It is rebuilt when the current report changes (a run finished) or after a
// minute, whichever comes first.
func cachedLogDirValue(kind, logDir, outDir string, compute func() interface{}) interface{} {
	sources := append(reportStampSources(outDir), logDir)
	return reportArtifacts.derived("logs:"+kind+":"+logDir, sources, time.Minute, 1<<20, compute)
}
