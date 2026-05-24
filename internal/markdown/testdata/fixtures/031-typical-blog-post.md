# Moving off Ghost

After four years on Ghost, the operational tax finally outweighed the value. This post describes the replacement.

## What I was paying for

The setup was a Ghost container plus a MySQL container plus a reverse proxy plus a backup job. None of these produces writing.

## What I built

A single Rust binary called Cairn:

- markdown files in git are the source of truth
- the build plane is a pure function from the repo to a static site
- assets are content-addressed and live in object storage

I'll write more about each piece in follow-ups.

> The goal is simple: the blog is a directory of markdown files.

That's it.
