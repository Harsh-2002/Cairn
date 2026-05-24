package render

// SiteConfig is the user-supplied site-wide configuration (typically loaded
// from cairn.toml).
type SiteConfig struct {
	Title       string `toml:"title"`
	Description string `toml:"description"`
	BaseURL     string `toml:"base_url"`
	Author      string `toml:"author"`
	Language    string `toml:"language"`
}

// DefaultSiteConfig returns sane defaults that let an empty cairn.toml produce
// a valid site.
func DefaultSiteConfig() SiteConfig {
	return SiteConfig{
		Title:       "A Cairn",
		Description: "A blog built with Cairn.",
		BaseURL:     "https://example.com",
		Author:      "Unknown",
		Language:    "en",
	}
}

// PostView is the serialisable shape templates consume. Dates are
// pre-formatted strings so templates never touch wall-clock or locale.
type PostView struct {
	Title       string
	Slug        string
	URL         string
	Summary     string
	HasSummary  bool
	Tags        []string
	Date        string
	DateDisplay string
	LastMod     string
	HTML        string
	HasMermaid  bool
	HasMath     bool
}
