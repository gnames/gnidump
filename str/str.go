package str

// ShortTitle truncates a title to 45 characters if necesary.
func ShortTitle(title string) string {
	if len(title) < 45 {
		return title
	}
	return title[0:41] + "..."
}
