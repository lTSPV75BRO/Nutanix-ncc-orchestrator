package main

import (
	"fmt"
	"runtime/debug"
	"sync/atomic"
)

// backgroundPanicTotal counts panics recovered in background work (exported
// as ncc_api_background_panics_total).
var backgroundPanicTotal atomic.Uint64

// guard runs fn and turns a panic into a logged error instead of a crashed
// API server. Handlers already recover (recoverPanic); the schedulers and
// cache refreshers run outside any request, where one bad file or nil map
// would otherwise take the whole server down, and with it every schedule.
// It reports whether fn panicked, so a caller can release state fn held.
func guard(name string, fn func()) (panicked bool) {
	defer func() {
		if v := recover(); v != nil {
			panicked = true
			backgroundPanicTotal.Add(1)
			serverLog.Error("background task panic", "task", name, "panic", fmt.Sprint(v), "stack", string(debug.Stack()))
		}
	}()
	fn()
	return false
}
