#!/usr/bin/env python3
"""Check changed documentation and incoming local links without executing examples."""
import os
from pathlib import Path
import re
import subprocess
import sys
import urllib.parse

ROOT = Path(__file__).resolve().parents[2]
LINK = re.compile(r'\]\((<[^>]+>|[^\s)]+)(?:\s+"[^"]*")?\)')
FENCE = re.compile(r'^```([^\n]*)\n(.*?)^```\s*$', re.M | re.S)


def slug(text):
    text = re.sub(r'<[^>]*>', '', text)
    return re.sub(r'[^\w\- ]', '', text.lower()).replace(' ', '-')


def anchors(text):
    result, counts = set(), {}
    for title in re.findall(r'^#{1,6}\s+(.+?)\s*#*$', FENCE.sub('', text), re.M):
        key = slug(title)
        count = counts.get(key, 0)
        result.add(f'{key}-{count}' if count else key)
        counts[key] = count + 1
    result.update(re.findall(r'(?:id|name)=["\']([^"\']+)["\']', text))
    return result


def check(root, changed, documents):
    errors = []
    changed = {root / path for path in changed}
    for path in documents:
        text = path.read_text()
        for destination in LINK.findall(FENCE.sub('', text)):
            parsed = urllib.parse.urlsplit(destination.strip('<>'))
            if parsed.scheme or parsed.netloc or parsed.path.startswith('/'):
                continue
            target = (path.parent / urllib.parse.unquote(parsed.path)).resolve() if parsed.path else path
            if path not in changed and target not in changed:
                continue
            label = f'{path.relative_to(root)}: {destination}'
            if not target.exists():
                errors.append(label + ' — missing local target')
            elif parsed.fragment and target.suffix == '.md' and urllib.parse.unquote(parsed.fragment) not in anchors(target.read_text()):
                errors.append(label + ' — missing heading or anchor')
        if path in changed:
            for language, body in FENCE.findall(text):
                if language.strip() in {'sh', 'bash'}:
                    shell = 'bash' if language.strip() == 'bash' else 'sh'
                    result = subprocess.run([shell, '-n'], input=body, text=True, capture_output=True)
                    if result.returncode:
                        errors.append(f'{path.relative_to(root)}: invalid {shell} example: {result.stderr.strip()}')
    return errors


def main():
    base = os.environ.get('BASE_REVISION') or subprocess.check_output(['git', 'rev-parse', 'HEAD^'], cwd=ROOT, text=True).strip()
    if not re.fullmatch(r'[0-9a-f]{40}', base):
        raise ValueError('invalid documentation base revision')
    changed = subprocess.check_output(['git', 'diff', '--name-only', '--no-renames', '-z', base, 'HEAD'], cwd=ROOT).decode().split('\0')
    tracked = subprocess.check_output(['git', 'ls-files', '-z', '*.md'], cwd=ROOT).decode().split('\0')
    documents = [ROOT / path for path in tracked if path and (ROOT / path).is_file() and not (ROOT / path).is_symlink()]
    errors = check(ROOT, [path for path in changed if path], documents)
    if errors:
        print('\n'.join(errors), file=sys.stderr)
        return 1
    print('Documentation links, anchors and shell syntax: PASS')
    return 0


if __name__ == '__main__':
    sys.exit(main())
