// Package plural words counts for messages people read: "1 cluster",
// "3 clusters", "2 backups" instead of "3 cluster(s)".
package plural

import "strconv"

// Count returns "<n> <one>" when n is 1 and "<n> <many>" otherwise. many
// defaults to one+"s".
func Count(n int, one string, many ...string) string {
	return strconv.Itoa(n) + " " + Word(n, one, many...)
}

// Word returns one when n is 1 and the plural otherwise, for sentences where
// the number is printed separately.
func Word(n int, one string, many ...string) string {
	if n == 1 {
		return one
	}
	if len(many) > 0 {
		return many[0]
	}
	return one + "s"
}
