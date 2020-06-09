package str

func ShortTitle(title string) string {
	if len(title) < 45 {
		return title
	}
	return title[0:41] + "..."
}
