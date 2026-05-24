package frontmatter

import (
	"sort"

	"gopkg.in/yaml.v3"
)

var knownKeys = map[string]struct{}{
	"title":          {},
	"date":           {},
	"slug":           {},
	"draft":          {},
	"tags":           {},
	"summary":        {},
	"redirects_from": {},
	"notion_page_id": {},
	"updated":        {},
}

// Parse decodes a YAML frontmatter block (the bytes between the opening and
// closing `---` delimiters; the caller strips those). Unknown top-level keys
// are returned as warnings; structural problems and constraint violations are
// returned as errors.
func Parse(src string) (ParsedFrontmatter, error) {
	var root yaml.Node
	if err := yaml.Unmarshal([]byte(src), &root); err != nil {
		return ParsedFrontmatter{}, &FrontmatterError{Kind: ErrYAMLParse, Wrapped: err}
	}

	var mapping *yaml.Node
	if root.Kind == yaml.DocumentNode && len(root.Content) > 0 {
		mapping = root.Content[0]
	}
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return ParsedFrontmatter{}, &FrontmatterError{Kind: ErrNotAMapping}
	}

	unknown := make(map[string]struct{})
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		k := mapping.Content[i]
		if k.Kind != yaml.ScalarNode {
			continue
		}
		if _, ok := knownKeys[k.Value]; !ok {
			unknown[k.Value] = struct{}{}
		}
	}
	unknownKeys := make([]string, 0, len(unknown))
	for k := range unknown {
		unknownKeys = append(unknownKeys, k)
	}
	sort.Strings(unknownKeys)

	var fm Frontmatter
	if err := mapping.Decode(&fm); err != nil {
		return ParsedFrontmatter{}, &FrontmatterError{Kind: ErrYAMLParse, Wrapped: err}
	}
	if err := fm.Validate(); err != nil {
		return ParsedFrontmatter{}, err
	}

	return ParsedFrontmatter{Frontmatter: fm, UnknownKeys: unknownKeys}, nil
}
