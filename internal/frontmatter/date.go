package frontmatter

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Date wraps a parsed RFC 3339 timestamp and preserves the original timezone
// offset suffix so re-emission is byte-identical. Go's time.Time formatting
// does not round-trip "Z" vs "+00:00" reliably, which would break Invariant 6
// (determinism) when frontmatter is rewritten by an editor.
type Date struct {
	t   time.Time
	raw string
}

// NewDate parses an RFC 3339 timestamp. The string MUST include a timezone
// (either "Z" or "±HH:MM"); naive timestamps are rejected.
func NewDate(s string) (Date, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return Date{}, fmt.Errorf("invalid RFC 3339 timestamp %q: %w", s, err)
	}
	return Date{t: t, raw: extractOffset(s)}, nil
}

// Time returns the parsed instant.
func (d Date) Time() time.Time { return d.t }

// RawOffset returns the original offset suffix ("Z" or "±HH:MM").
func (d Date) RawOffset() string { return d.raw }

// IsZero reports whether the date is unset.
func (d Date) IsZero() bool { return d.t.IsZero() && d.raw == "" }

// Equal compares two dates for instant + offset equality.
func (d Date) Equal(other Date) bool {
	return d.t.Equal(other.t) && d.raw == other.raw
}

// String renders the date in its original RFC 3339 form (preserving the offset).
func (d Date) String() string {
	if d.IsZero() {
		return ""
	}
	return d.t.Format("2006-01-02T15:04:05") + fractional(d.t) + d.raw
}

// UnmarshalYAML implements yaml.Unmarshaler so YAML maps decode into Date.
func (d *Date) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("date must be a scalar")
	}
	v, err := NewDate(node.Value)
	if err != nil {
		return err
	}
	*d = v
	return nil
}

// MarshalYAML emits the date in its original RFC 3339 form.
func (d Date) MarshalYAML() (any, error) { return d.String(), nil }

// extractOffset pulls the trailing "Z" or "±HH:MM" off an RFC 3339 string.
// Assumes the string already parsed successfully via time.RFC3339, so the
// format is well-formed.
func extractOffset(s string) string {
	if strings.HasSuffix(s, "Z") {
		return "Z"
	}
	if len(s) >= 6 {
		tail := s[len(s)-6:]
		if (tail[0] == '+' || tail[0] == '-') && tail[3] == ':' {
			return tail
		}
	}
	return ""
}

// fractional returns the ".fff" suffix if the time has sub-second precision,
// matching the original RFC 3339 input's precision (Go's Format strips trailing zeros).
func fractional(t time.Time) string {
	ns := t.Nanosecond()
	if ns == 0 {
		return ""
	}
	// Render with up to 9 digits, strip trailing zeros.
	s := fmt.Sprintf(".%09d", ns)
	return strings.TrimRight(s, "0")
}
