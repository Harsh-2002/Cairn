package frontmatter

import "strings"

// Validate enforces the constraints documented in docs/frontmatter.md.
func (f Frontmatter) Validate() error {
	if strings.TrimSpace(f.Title) == "" {
		return &FrontmatterError{Kind: ErrEmptyTitle}
	}
	if f.Date.IsZero() {
		return &FrontmatterError{Kind: ErrMissingDate}
	}
	if f.Slug != nil {
		if err := validateSlug(*f.Slug); err != nil {
			return err
		}
	}
	for _, tag := range f.Tags {
		lowered := strings.ToLower(tag)
		if err := validateSlug(lowered); err != nil {
			return &FrontmatterError{Kind: ErrInvalidTag, Value: tag}
		}
	}
	for _, r := range f.RedirectsFrom {
		if !strings.HasPrefix(r, "/") {
			return &FrontmatterError{Kind: ErrInvalidRedirect, Value: r}
		}
	}
	if f.NotionPageID != nil && !isValidNotionID(*f.NotionPageID) {
		return &FrontmatterError{Kind: ErrInvalidNotionPageID, Value: *f.NotionPageID}
	}
	return nil
}
