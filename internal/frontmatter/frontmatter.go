package frontmatter

import "fmt"

// Frontmatter is the validated YAML header of a Cairn post. The schema is
// defined in docs/frontmatter.md.
type Frontmatter struct {
	Title         string   `yaml:"title"`
	Date          Date     `yaml:"date"`
	Slug          *string  `yaml:"slug,omitempty"`
	Draft         bool     `yaml:"draft,omitempty"`
	Tags          []string `yaml:"tags,omitempty"`
	Summary       *string  `yaml:"summary,omitempty"`
	RedirectsFrom []string `yaml:"redirects_from,omitempty"`
	NotionPageID  *string  `yaml:"notion_page_id,omitempty"`
	Updated       *Date    `yaml:"updated,omitempty"`
}

// ParsedFrontmatter is the result of Parse: the validated struct plus any
// unknown top-level keys encountered (warnings, not errors, for forward
// compatibility).
type ParsedFrontmatter struct {
	Frontmatter Frontmatter
	UnknownKeys []string
}

// ErrorKind discriminates FrontmatterError.
type ErrorKind int

const (
	ErrYAMLParse ErrorKind = iota
	ErrNotAMapping
	ErrEmptyTitle
	ErrInvalidSlug
	ErrEmptyDerivedSlug
	ErrInvalidTag
	ErrInvalidRedirect
	ErrInvalidNotionPageID
	ErrMissingDate
)

// FrontmatterError carries a validation or parse failure with the offending
// value (where applicable).
type FrontmatterError struct {
	Kind    ErrorKind
	Value   string
	Wrapped error
}

func (e *FrontmatterError) Error() string {
	switch e.Kind {
	case ErrYAMLParse:
		return fmt.Sprintf("YAML parse error: %s", e.Wrapped)
	case ErrNotAMapping:
		return "frontmatter must be a YAML mapping"
	case ErrEmptyTitle:
		return "title must not be empty or whitespace only"
	case ErrInvalidSlug:
		return fmt.Sprintf("invalid slug `%s`: must match ^[a-z0-9]+(-[a-z0-9]+)*$ and be 80 characters or fewer", e.Value)
	case ErrEmptyDerivedSlug:
		return "derived slug is empty (title contained no valid characters); set `slug` explicitly"
	case ErrInvalidTag:
		return fmt.Sprintf("invalid tag `%s`: must match ^[a-z0-9]+(-[a-z0-9]+)*$ after lowercasing", e.Value)
	case ErrInvalidRedirect:
		return fmt.Sprintf("redirect path `%s` must begin with `/`", e.Value)
	case ErrInvalidNotionPageID:
		return fmt.Sprintf("notion_page_id `%s` must be 32 lowercase hex characters", e.Value)
	case ErrMissingDate:
		return "date is required"
	}
	return "frontmatter error"
}

func (e *FrontmatterError) Unwrap() error { return e.Wrapped }

// LastMod returns the effective "last meaningfully updated" timestamp: Updated
// when present, else Date.
func (f Frontmatter) LastMod() Date {
	if f.Updated != nil {
		return *f.Updated
	}
	return f.Date
}

// EffectiveSlug returns the explicit Slug field if set, otherwise the derived
// slug from Title.
func (f Frontmatter) EffectiveSlug() (string, error) {
	if f.Slug != nil {
		return *f.Slug, nil
	}
	s, ok := DeriveSlug(f.Title)
	if !ok {
		return "", &FrontmatterError{Kind: ErrEmptyDerivedSlug}
	}
	return s, nil
}
