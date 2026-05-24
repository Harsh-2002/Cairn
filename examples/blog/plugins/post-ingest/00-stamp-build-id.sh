#!/bin/sh
# Sample post-ingest plugin. Reads the JSON payload from stdin and emits
# a no-op response. Real plugins would do something useful — modify the
# body, validate frontmatter, etc. See docs/PLUGIN_CONTRACT.md.

set -eu
cat > /dev/null
echo '{}'
