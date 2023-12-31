package str

import "strings"

// ShortTitle truncates a title to 45 characters if necesary.
func ShortTitle(title string) string {
	if len(title) < 45 {
		return title
	}
	return title[0:41] + "..."
}

// QuoteString adds single quotes around a string and escapes
// any single quotes.
func QuoteString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
