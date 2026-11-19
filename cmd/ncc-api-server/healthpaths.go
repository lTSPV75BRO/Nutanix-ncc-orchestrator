package main

import (
	"sync"
	"time"
)

// healthPathsTTL bounds how long the display paths in /api/v1/health are
// reused. The paths come from startup flags, so they change only if a
// symlink on the way is re-pointed; resolving them on every call cost about
// 18 ms per authenticated request on Windows (every path is symlink-
// evaluated), and the UI polls this endpoint. Only what /health prints is
// cached: file access still goes through normalizeAndConfinePath each time.
const healthPathsTTL = 30 * time.Second

type healthPathSet struct {
	configRel, outputRel                               string
	configAbs, outputAbs, logAbs, tokenAbs, orchBinAbs string
}

type healthPathCache struct {
	mu   sync.Mutex
	at   time.Time
	data healthPathSet
}

// healthPaths returns the resolved display paths, recomputing them at most
// once per healthPathsTTL.
func (s *apiServer) healthPaths(now time.Time) healthPathSet {
	s.healthPathCache.mu.Lock()
	defer s.healthPathCache.mu.Unlock()
	if !s.healthPathCache.at.IsZero() && now.Sub(s.healthPathCache.at) < healthPathsTTL {
		return s.healthPathCache.data
	}
	s.healthPathCache.data = healthPathSet{
		configRel:  s.displayRelPath(s.configPath),
		outputRel:  s.displayRelPath(s.outputDir),
		configAbs:  s.absPath(s.configPath),
		outputAbs:  s.absPath(s.outputDir),
		logAbs:     s.absPath(s.logDir),
		tokenAbs:   s.absPath(s.tokenFilePath),
		orchBinAbs: s.absPath(s.orchestratorBin),
	}
	s.healthPathCache.at = now
	return s.healthPathCache.data
}
