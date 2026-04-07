package kblinks

import "regexp"

// kbNumber matches Nutanix doc references like "KB 5582" or "kb 6153" in NCC text.
var kbNumber = regexp.MustCompile(`(?i)\bKB\s+(\d+)\b`)

// AnnotateMarkdown turns KB number references into markdown links for Cursor/IDE clients.
func AnnotateMarkdown(text string) string {
	return kbNumber.ReplaceAllString(text, `[$0](https://portal.nutanix.com/kb/$1)`)
}
